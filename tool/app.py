"""Label Verification & Extraction Training Tool — FastAPI backend.

Serves the web tool locally. On startup, loads all label texts into memory
for instant pattern testing. Provides endpoints for product browsing,
extraction verification, annotation, and pattern learning.

Usage:
    cd nz-catalogue
    python -m uvicorn tool.app:app --reload --port 8000
"""

from __future__ import annotations

import json
import logging
import queue
import re
import threading
import time
import warnings
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

# Suppress pdfplumber/pdfminer color warnings
warnings.filterwarnings("ignore", message="Cannot set non-stroke color")
logging.getLogger("pdfminer").setLevel(logging.ERROR)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
LABELS_DIR = DATA_DIR / "labels"
CORRECTIONS_DIR = DATA_DIR / "corrections"
CATALOGUE_PATH = DATA_DIR / "output" / "2025-2026" / "catalogue.json"
MANIFEST_PATH = LABELS_DIR / "manifest.json"

VERIFIED_PATH = CORRECTIONS_DIR / "verified.json"
CORRECTIONS_PATH = CORRECTIONS_DIR / "corrections.json"
ANNOTATIONS_PATH = CORRECTIONS_DIR / "annotations.json"
LEARNED_PATTERNS_PATH = CORRECTIONS_DIR / "learned_patterns.json"
ACVM_OVERRIDES_PATH = CORRECTIONS_DIR / "acvm_overrides.json"
PRODUCT_SPLITS_PATH = CORRECTIONS_DIR / "product_splits.json"
TEXT_CACHE = CORRECTIONS_DIR / "label_texts_cache.json"
EXTRACTION_CACHE = CORRECTIONS_DIR / "extraction_cache.json"

# ---------------------------------------------------------------------------
# In-memory state
# ---------------------------------------------------------------------------
label_texts: dict[str, str] = {}  # product_id → full label text
extractions: dict[str, dict] = {}  # product_id → extraction data dict
products: list[dict] = []  # product list from catalogue
manifest: dict = {}
verified: dict = {}
corrections: dict = {}
annotations: dict = {}
learned_patterns: dict = {}

# product_id → label PDF path
label_paths: dict[str, Path] = {}
# product_id → reg_no
product_reg_map: dict[str, str] = {}

# Bootstrap (label-text extraction) state. Accessed from the background
# thread + the status endpoint. The module-level dict is mutated in-place;
# the GIL keeps individual field reads/writes atomic, which is enough for
# this single-user tool.
# ACVM register (loaded on startup from cached CSV, used for match suggestions).
# Keyed by trade name and by P-number for O(1) lookups.
acvm_by_name: dict = {}
acvm_by_reg: dict = {}
acvm_overrides: dict = {"block": {}, "force": {}}

# ─── Pipeline runner state ─────────────────────────────────────────────
# Stage outputs go through Python's logging — a QueueLogHandler pushes
# formatted lines into a thread-safe queue, which the SSE endpoint drains.
# Stages never need to know about the GUI.
OUTDATED_THRESHOLD_DAYS = 180  # matches scripts/run_label_check.py

pipeline_state: dict = {
    "phase": "idle",
    "running": False,
    "stages_requested": [],
    "stages_completed": [],
    "stages_errored": [],
    "message": "",
    "error": None,
    "started_at": None,
    "finished_at": None,
}
_pipeline_log_queue: "queue.Queue[str]" = queue.Queue(maxsize=50000)


class _QueueLogHandler(logging.Handler):
    """Formats log records and enqueues them for SSE streaming."""
    def __init__(self, q: "queue.Queue[str]"):
        super().__init__()
        self.q = q
        self.setFormatter(logging.Formatter("[%(levelname)s] %(name)s: %(message)s"))

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self.q.put_nowait(self.format(record))
        except queue.Full:
            pass


def _pipe_log(msg: str) -> None:
    """Push a framing status line into the log queue (in addition to
    whatever the stages emit via logging)."""
    try:
        _pipeline_log_queue.put_nowait(msg)
    except queue.Full:
        pass


rebuild_state: dict = {
    "phase": "idle",           # "idle" | "assembling" | "matching" | "done" | "error"
    "running": False,
    "message": "",
    "error": None,
    "started_at": None,
    "finished_at": None,
}

bootstrap_state: dict = {
    "phase": "idle",           # "idle" | "extracting_text" | "extracting_fields" | "saving" | "done" | "error"
    "running": False,
    "current": 0,
    "total": 0,
    "error": None,
    "started_at": None,
    "finished_at": None,
}
_bootstrap_lock = threading.Lock()

# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------
app = FastAPI(title="Label Verification Tool")
TOOL_DIR = Path(__file__).resolve().parent
app.mount("/static", StaticFiles(directory=str(TOOL_DIR / "static")), name="static")


# ---------------------------------------------------------------------------
# Startup
# ---------------------------------------------------------------------------
@app.on_event("startup")
def startup():
    """Fast startup: load catalogue, manifest, build label paths, load caches
    if present, load correction files. Never runs heavy PDF extraction —
    that's deferred to POST /api/bootstrap/run (see _run_bootstrap)."""
    global products, manifest, verified, corrections, annotations, learned_patterns

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    logger.info("Loading catalogue and manifest (fast startup)...")

    # Load catalogue into in-memory state.
    _load_catalogue_into_memory()

    # Load manifest
    if MANIFEST_PATH.exists():
        manifest.update(json.loads(MANIFEST_PATH.read_text(encoding="utf-8")))

    # Build label paths (lightweight — no PDF parsing).
    _build_label_paths()

    # Load cached texts / extractions if they exist.
    if TEXT_CACHE.exists():
        try:
            label_texts.update(json.loads(TEXT_CACHE.read_text(encoding="utf-8")))
            logger.info("Loaded %d label texts from cache", len(label_texts))
        except Exception as e:
            logger.warning("Failed to load text cache: %s", e)
    if EXTRACTION_CACHE.exists():
        try:
            extractions.update(json.loads(EXTRACTION_CACHE.read_text(encoding="utf-8")))
            logger.info("Loaded %d extractions from cache", len(extractions))
        except Exception as e:
            logger.warning("Failed to load extraction cache: %s", e)

    # Load correction files
    CORRECTIONS_DIR.mkdir(parents=True, exist_ok=True)
    verified.update(_load_json(VERIFIED_PATH))
    corrections.update(_load_json(CORRECTIONS_PATH))
    annotations.update(_load_json(ANNOTATIONS_PATH))
    learned_patterns.update(_load_json(LEARNED_PATTERNS_PATH))

    # Load ACVM register (from 30-day CSV cache) + overrides.
    _load_acvm_register()
    _load_acvm_overrides()

    logger.info("Startup complete. Pending extractions: %d/%d",
                len(label_paths) - len(extractions), len(label_paths))


def _load_acvm_register():
    """Parse the cached ACVM CSV into in-memory dicts for match suggestions.
    No network call — relies on the existing 30-day cache populated by the
    pipeline's `acvm` stage. If the cache is missing, ACVM review is disabled
    and the endpoints return empty results."""
    from src.parsers.acvm_csv import load_acvm_csv
    try:
        products_by_name = load_acvm_csv()
        acvm_by_name.update(products_by_name)
        for p in products_by_name.values():
            acvm_by_reg[p.registration_no] = p
        logger.info("Loaded %d ACVM products into memory", len(acvm_by_name))
    except Exception as e:
        logger.warning("Could not load ACVM register: %s", e)


def _load_acvm_overrides():
    data = _load_json(ACVM_OVERRIDES_PATH)
    acvm_overrides["block"] = data.get("block", {}) if isinstance(data, dict) else {}
    acvm_overrides["force"] = data.get("force", {}) if isinstance(data, dict) else {}


def _save_acvm_overrides():
    payload = {
        "_comment": "Manual overrides for ACVM matching. 'block' prevents false fuzzy matches. 'force' assigns a specific P-number.",
        "block": acvm_overrides["block"],
        "force": acvm_overrides["force"],
    }
    _save_json(ACVM_OVERRIDES_PATH, payload)


def _run_rebuild():
    """Re-run assemble + ACVM match against already-parsed staging data.
    Uses the cached ACVM CSV — no network. Takes ~5 seconds. Reloads the
    in-memory catalogue state so endpoints reflect the new data."""
    import json as _json
    import orjson

    try:
        with _bootstrap_lock:  # reuse the same lock to serialise all heavy work
            if rebuild_state["running"] or bootstrap_state["running"]:
                return
            rebuild_state.update({
                "running": True,
                "phase": "assembling",
                "message": "Loading staging data...",
                "error": None,
                "started_at": _now_iso(),
                "finished_at": None,
            })

        # 1) Load raw_*.json from staging
        from src.assembler import assemble_catalogue
        import src.assembler as _asm
        from src.config import OUTPUT_DIR, STAGING_DIR
        from src.parsers.phi_table import PhiTableResult, PhiTableRow
        from src.parsers.rm_table import RmTableResult, RmTableRow
        from src.parsers.changes_table import ChangesTableResult, ChangesTableRow
        from src.parsers.flagged_table import FlaggedTableResult, FlaggedTableRow

        staging = {
            "phi": STAGING_DIR / "raw_phi_table.json",
            "rm": STAGING_DIR / "raw_rm_table.json",
            "changes": STAGING_DIR / "raw_changes_table.json",
            "flagged": STAGING_DIR / "raw_flagged_table.json",
        }
        if not staging["phi"].exists():
            raise RuntimeError(
                "No parsed schedule found in data/staging/. Run the 'parse' stage from the CLI first."
            )

        phi_rows = [PhiTableRow(**r) for r in _json.loads(staging["phi"].read_text(encoding="utf-8"))]
        phi_result = PhiTableResult(rows=phi_rows, warnings=[], pages_processed=0)
        rm_result = None
        changes_result = None
        flagged_result = None
        if staging["rm"].exists():
            rm_rows = [RmTableRow(**r) for r in _json.loads(staging["rm"].read_text(encoding="utf-8"))]
            rm_result = RmTableResult(rows=rm_rows, warnings=[], pages_processed=0)
        if staging["changes"].exists():
            ch_rows = [ChangesTableRow(**r) for r in _json.loads(staging["changes"].read_text(encoding="utf-8"))]
            changes_result = ChangesTableResult(rows=ch_rows, warnings=[], pages_processed=0)
        if staging["flagged"].exists():
            fl_rows = [FlaggedTableRow(**r) for r in _json.loads(staging["flagged"].read_text(encoding="utf-8"))]
            flagged_result = FlaggedTableResult(rows=fl_rows, warnings=[], pages_processed=0)

        # Bust the assembler's splits cache so freshly-saved overrides apply.
        _asm._product_splits_cache = None

        # 2) Assemble
        rebuild_state["message"] = "Assembling catalogue..."
        # Read the existing catalogue's metadata so we preserve season + source_hash.
        if CATALOGUE_PATH.exists():
            old = orjson.loads(CATALOGUE_PATH.read_bytes())
            season = old.get("season", "unknown")
            source_pdf = old.get("source_pdf", "")
            source_hash = old.get("source_hash", "")
        else:
            season, source_pdf, source_hash = "unknown", "", ""

        catalogue = assemble_catalogue(
            phi_result, rm_result, changes_result, flagged_result,
            season=season, source_pdf=source_pdf, source_hash=source_hash,
        )

        # 3) Match against ACVM + enrich
        rebuild_state["phase"] = "matching"
        rebuild_state["message"] = "Matching against ACVM register..."
        from src.stages.match_acvm import match_products
        from src.stages.enrich_acvm import enrich_catalogue_with_acvm
        match_result = match_products(catalogue.trade_products, acvm_by_name)
        enriched = enrich_catalogue_with_acvm(catalogue, match_result)

        # 4) Write catalogue + extracts
        rebuild_state["message"] = "Writing catalogue..."
        out_dir = OUTPUT_DIR / season if season != "unknown" else CATALOGUE_PATH.parent
        out_dir.mkdir(parents=True, exist_ok=True)
        catalogue_bytes = orjson.dumps(
            enriched.model_dump(),
            option=orjson.OPT_INDENT_2 | orjson.OPT_SORT_KEYS,
        )
        CATALOGUE_PATH.write_bytes(catalogue_bytes)

        # Rewrite the same 4 extracts the CLI writes, to keep them in sync.
        def _write_extract(path: Path, items):
            path.write_bytes(orjson.dumps(
                [i.model_dump() for i in items],
                option=orjson.OPT_INDENT_2 | orjson.OPT_SORT_KEYS,
            ))
        _write_extract(out_dir / "active_ingredients.json", enriched.active_ingredients)
        _write_extract(out_dir / "trade_products.json", enriched.trade_products)
        _write_extract(out_dir / "phi_matrix.json", enriched.phi_entries)
        _write_extract(out_dir / "rm_rules.json", enriched.resistance_management_rules)

        # 5) Refresh the tool's in-memory state
        _load_catalogue_into_memory()

        rebuild_state["phase"] = "done"
        rebuild_state["message"] = (
            f"Rebuilt: {len(enriched.trade_products)} products, "
            f"{len(match_result.matches)} ACVM-matched"
        )
        logger.info("Rebuild complete: %s", rebuild_state["message"])
    except Exception as e:
        logger.exception("Rebuild failed")
        rebuild_state["phase"] = "error"
        rebuild_state["error"] = str(e)
    finally:
        rebuild_state["running"] = False
        rebuild_state["finished_at"] = _now_iso()


@app.get("/api/catalogue/rebuild/status")
def rebuild_status():
    return dict(rebuild_state)


@app.post("/api/catalogue/rebuild")
def rebuild_catalogue():
    """Re-run assemble + ACVM match in the background. Uses cached ACVM
    CSV — no network. Call this after saving product-split or ACVM-override
    changes so they take effect without a CLI run."""
    if rebuild_state["running"]:
        return {"ok": False, "reason": "already_running"}
    if bootstrap_state["running"]:
        return {"ok": False, "reason": "bootstrap_running"}
    thread = threading.Thread(target=_run_rebuild, daemon=True)
    thread.start()
    return {"ok": True}


# ---------------------------------------------------------------------------
# Pipeline runner (full assemble → acvm → labels → diff)
# ---------------------------------------------------------------------------
def _stage_parse(source_pdf_str: str | None, force: bool) -> None:
    from src.parsers.phi_table import parse_phi_table
    from src.parsers.rm_table import parse_rm_table
    from src.parsers.changes_table import parse_changes_table
    from src.parsers.flagged_table import parse_flagged_table
    from src.utils.hashing import hash_file
    import orjson

    pdf_path = Path(source_pdf_str) if source_pdf_str else (DATA_DIR / "input" / "schedule.pdf")
    if not pdf_path.is_absolute():
        pdf_path = PROJECT_ROOT / pdf_path
    if not pdf_path.exists():
        raise FileNotFoundError(
            f"Source PDF not found at {pdf_path}. Place schedule.pdf in data/input/."
        )

    # Idempotency: skip if the PDF hasn't changed since last parse.
    if not force and CATALOGUE_PATH.exists():
        try:
            existing = orjson.loads(CATALOGUE_PATH.read_bytes())
            if existing.get("source_hash") == hash_file(pdf_path):
                _pipe_log("PDF unchanged since last parse — skipping (force=false)")
                return
        except Exception:
            pass  # fall through and re-parse

    _pipe_log(f"Parsing schedule PDF: {pdf_path.name}")
    parse_phi_table(pdf_path, write_staging=True)
    parse_rm_table(pdf_path, write_staging=True)
    parse_changes_table(pdf_path, write_staging=True)
    parse_flagged_table(pdf_path, write_staging=True)


def _stage_assemble_and_match(run_match: bool) -> None:
    """Shared logic: read staging, assemble, optionally ACVM-match, write."""
    import src.assembler as _asm
    from src.assembler import assemble_catalogue
    from src.config import OUTPUT_DIR, STAGING_DIR
    from src.parsers.phi_table import PhiTableResult, PhiTableRow
    from src.parsers.rm_table import RmTableResult, RmTableRow
    from src.parsers.changes_table import ChangesTableResult, ChangesTableRow
    from src.parsers.flagged_table import FlaggedTableResult, FlaggedTableRow
    import orjson

    staging = STAGING_DIR / "raw_phi_table.json"
    if not staging.exists():
        raise RuntimeError("No parsed schedule found. Run the 'parse' stage first.")

    phi_rows = [PhiTableRow(**r) for r in json.loads(staging.read_text(encoding="utf-8"))]
    phi_result = PhiTableResult(rows=phi_rows, warnings=[], pages_processed=0)

    def _load_opt(path: Path, row_cls, result_cls):
        if not path.exists():
            return None
        rows = [row_cls(**r) for r in json.loads(path.read_text(encoding="utf-8"))]
        return result_cls(rows=rows, warnings=[], pages_processed=0)

    rm_result = _load_opt(STAGING_DIR / "raw_rm_table.json", RmTableRow, RmTableResult)
    changes_result = _load_opt(STAGING_DIR / "raw_changes_table.json", ChangesTableRow, ChangesTableResult)
    flagged_result = _load_opt(STAGING_DIR / "raw_flagged_table.json", FlaggedTableRow, FlaggedTableResult)

    # Reset splits cache so freshly-saved overrides apply.
    _asm._product_splits_cache = None

    if CATALOGUE_PATH.exists():
        old = orjson.loads(CATALOGUE_PATH.read_bytes())
        season = old.get("season", "unknown")
        source_pdf = old.get("source_pdf", "")
        source_hash = old.get("source_hash", "")
    else:
        season, source_pdf, source_hash = "unknown", "", ""

    _pipe_log("Assembling catalogue from staging...")
    cat = assemble_catalogue(
        phi_result, rm_result, changes_result, flagged_result,
        season=season, source_pdf=source_pdf, source_hash=source_hash,
    )

    if run_match and acvm_by_name:
        from src.stages.match_acvm import match_products
        from src.stages.enrich_acvm import enrich_catalogue_with_acvm
        _pipe_log(f"Matching {len(cat.trade_products)} products against ACVM register...")
        match_result = match_products(cat.trade_products, acvm_by_name)
        _pipe_log(f"Matched {len(match_result.matches)}/{len(cat.trade_products)} products")
        cat = enrich_catalogue_with_acvm(cat, match_result)

    out_dir = (OUTPUT_DIR / season) if season != "unknown" else CATALOGUE_PATH.parent
    out_dir.mkdir(parents=True, exist_ok=True)
    CATALOGUE_PATH.write_bytes(orjson.dumps(
        cat.model_dump(), option=orjson.OPT_INDENT_2 | orjson.OPT_SORT_KEYS,
    ))
    for name, items in [
        ("active_ingredients.json", cat.active_ingredients),
        ("trade_products.json", cat.trade_products),
        ("phi_matrix.json", cat.phi_entries),
        ("rm_rules.json", cat.resistance_management_rules),
    ]:
        (out_dir / name).write_bytes(orjson.dumps(
            [i.model_dump() for i in items],
            option=orjson.OPT_INDENT_2 | orjson.OPT_SORT_KEYS,
        ))


def _stage_acvm_fetch_labels() -> None:
    """Network: scrape ACVM detail pages, download any new label PDFs.
    Mirrors the acvm sub-flow in scripts/run_pipeline.py."""
    import orjson
    from src.models import SeasonCatalogue
    from src.config import STAGING_DIR, LABELS_DIR as LABELS
    from src.parsers.acvm_detail_scraper import create_session, scrape_details
    from src.stages.fetch_labels import fetch_labels

    if not CATALOGUE_PATH.exists():
        raise RuntimeError("No catalogue to fetch labels against. Run assemble + acvm match first.")
    cat = SeasonCatalogue(**orjson.loads(CATALOGUE_PATH.read_bytes()))

    slug_to_pnum = {
        tp.id: tp.acvm_registration_no
        for tp in cat.trade_products
        if getattr(tp, "acvm_registration_no", None)
    }
    slug_to_name = {tp.id: tp.name for tp in cat.trade_products}
    _pipe_log(f"Scraping ACVM detail pages for {len(slug_to_pnum)} matched products...")

    session = create_session()
    detail_cache = STAGING_DIR / "acvm_detail_cache"
    detail_results = scrape_details(session, slug_to_pnum, cache_dir=detail_cache)
    with_labels = sum(1 for r in detail_results.values() if r.labels)
    _pipe_log(f"Detail pages: {len(detail_results)} scraped, {with_labels} have label URLs")

    if with_labels > 0:
        _pipe_log("Downloading label PDFs...")
        docs = fetch_labels(session, detail_results, slug_to_name)
        _pipe_log(f"Fetched {len(docs)} label documents")


def _stage_labels(force: bool) -> None:
    """Re-extract text + fields from label PDFs. Same code path as the
    one-time bootstrap, but can be triggered via the pipeline runner."""
    from src.parsers.label_text_extractor import extract_label_text
    from src.stages.extract_label_data import extract_single_label

    _pipe_log(f"Extracting text from {len(label_paths)} PDFs (force={force})")
    for i, (pid, pdf_path) in enumerate(label_paths.items(), start=1):
        if i % 25 == 0:
            _pipe_log(f"  {i}/{len(label_paths)} texts...")
        if not force and pid in label_texts:
            continue
        try:
            text = extract_label_text(pdf_path)
            if text:
                label_texts[pid] = text
        except Exception as e:
            logger.warning("Text extract failed for %s: %s", pid, e)

    _pipe_log(f"Running field extractors on {len(label_texts)} labels...")
    for i, pid in enumerate(list(label_texts.keys()), start=1):
        if i % 25 == 0:
            _pipe_log(f"  {i}/{len(label_texts)} fields...")
        if not force and pid in extractions:
            continue
        pdf_path = label_paths.get(pid)
        if not pdf_path:
            continue
        reg_no = product_reg_map.get(pid, pid)
        try:
            data = extract_single_label(pdf_path, pid, reg_no)
            if data:
                extractions[pid] = data.model_dump()
        except Exception as e:
            logger.warning("Field extract failed for %s: %s", pid, e)

    _save_json(TEXT_CACHE, label_texts)
    _save_json(EXTRACTION_CACHE, extractions)


def _stage_diff(previous_catalogue_path: str | None) -> None:
    if not previous_catalogue_path:
        raise ValueError("Diff stage requires a previous-season catalogue path")
    import orjson
    from src.models import SeasonCatalogue
    from src.stages.diff_seasons import diff_seasons
    from src.config import OUTPUT_DIR

    prev_path = Path(previous_catalogue_path)
    if not prev_path.is_absolute():
        prev_path = PROJECT_ROOT / prev_path
    if not prev_path.exists():
        raise FileNotFoundError(f"Previous catalogue not found: {prev_path}")
    if not CATALOGUE_PATH.exists():
        raise RuntimeError("Current catalogue missing — run assemble first")

    prev = SeasonCatalogue(**orjson.loads(prev_path.read_bytes()))
    curr = SeasonCatalogue(**orjson.loads(CATALOGUE_PATH.read_bytes()))
    _pipe_log(f"Comparing {prev.season} → {curr.season}...")
    entries = diff_seasons(prev, curr)
    _pipe_log(f"Found {len(entries)} changelog entries")

    out_dir = OUTPUT_DIR / curr.season
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "changelog.json").write_bytes(orjson.dumps(
        [e.model_dump() for e in entries],
        option=orjson.OPT_INDENT_2 | orjson.OPT_SORT_KEYS,
    ))


def _run_pipeline(
    stages: list[str],
    *,
    force: bool,
    download_labels: bool,
    source_pdf: str | None,
    previous_season_path: str | None,
) -> None:
    # Drain stale log lines from any previous run.
    while not _pipeline_log_queue.empty():
        try: _pipeline_log_queue.get_nowait()
        except queue.Empty: break

    handler = _QueueLogHandler(_pipeline_log_queue)
    root = logging.getLogger()
    root.addHandler(handler)
    prev_level = root.level
    root.setLevel(logging.INFO)

    try:
        with _bootstrap_lock:
            if pipeline_state["running"] or bootstrap_state["running"] or rebuild_state["running"]:
                return
            pipeline_state.update({
                "phase": "starting",
                "running": True,
                "stages_requested": list(stages),
                "stages_completed": [],
                "stages_errored": [],
                "message": "",
                "error": None,
                "started_at": _now_iso(),
                "finished_at": None,
            })
        _pipe_log(f"Running pipeline: {' → '.join(stages)}")

        for stage in stages:
            pipeline_state["phase"] = stage
            _pipe_log(f"── Stage: {stage} ──")
            t0 = time.time()
            try:
                if stage == "parse":
                    _stage_parse(source_pdf, force)
                elif stage == "assemble":
                    _stage_assemble_and_match(run_match=False)
                elif stage == "acvm":
                    _stage_assemble_and_match(run_match=True)
                    if download_labels:
                        _stage_acvm_fetch_labels()
                elif stage == "labels":
                    _stage_labels(force=force)
                elif stage == "diff":
                    _stage_diff(previous_season_path)
                else:
                    raise ValueError(f"Unknown stage: {stage}")
                dt = time.time() - t0
                pipeline_state["stages_completed"].append(stage)
                _pipe_log(f"✓ {stage} ({dt:.1f}s)")
            except Exception as e:
                pipeline_state["stages_errored"].append(stage)
                _pipe_log(f"✗ {stage} failed: {e}")
                raise

        # Refresh anything that might have changed on disk.
        _load_catalogue_into_memory()
        _build_label_paths()

        pipeline_state["phase"] = "done"
        pipeline_state["message"] = f"Completed: {', '.join(stages)}"
    except Exception as e:
        logger.exception("Pipeline failed")
        pipeline_state["phase"] = "error"
        pipeline_state["error"] = str(e)
    finally:
        root.removeHandler(handler)
        root.setLevel(prev_level)
        pipeline_state["running"] = False
        pipeline_state["finished_at"] = _now_iso()
        _pipe_log("[END]")


class PipelineRunRequest(BaseModel):
    stages: list[str]
    force: bool = False
    download_labels: bool = False
    source_pdf: str | None = None
    previous_season_path: str | None = None


@app.post("/api/pipeline/run")
def pipeline_run(req: PipelineRunRequest):
    if pipeline_state["running"] or bootstrap_state["running"] or rebuild_state["running"]:
        return {"ok": False, "reason": "busy"}
    valid = {"parse", "assemble", "acvm", "labels", "diff"}
    bad = [s for s in req.stages if s not in valid]
    if bad:
        raise HTTPException(400, f"Unknown stages: {bad}. Valid: {sorted(valid)}")
    if not req.stages:
        raise HTTPException(400, "No stages requested")
    thread = threading.Thread(
        target=_run_pipeline,
        args=(req.stages,),
        kwargs={
            "force": req.force,
            "download_labels": req.download_labels,
            "source_pdf": req.source_pdf,
            "previous_season_path": req.previous_season_path,
        },
        daemon=True,
    )
    thread.start()
    return {"ok": True}


@app.get("/api/pipeline/status")
def pipeline_status():
    return dict(pipeline_state)


@app.get("/api/pipeline/stream")
def pipeline_stream():
    """Server-sent events: drains the log queue as stages run.
    Browser EventSource closes on 'event: end'."""
    def generate():
        # Opener so the connection fires onopen.
        yield ": stream-open\n\n"
        while True:
            try:
                line = _pipeline_log_queue.get(timeout=0.5)
            except queue.Empty:
                if not pipeline_state["running"]:
                    yield "event: end\ndata: \n\n"
                    return
                yield ": keepalive\n\n"
                continue
            # SSE data must not contain raw newlines.
            safe = line.replace("\r", "").replace("\n", "\\n")
            yield f"data: {safe}\n\n"
            if line == "[END]":
                yield "event: end\ndata: \n\n"
                return
    return StreamingResponse(generate(), media_type="text/event-stream")


# ---------------------------------------------------------------------------
# Label freshness
# ---------------------------------------------------------------------------
@app.get("/api/labels/freshness")
def labels_freshness():
    """Report each label's last-checked age. Anything older than
    OUTDATED_THRESHOLD_DAYS is flagged."""
    now = datetime.now(timezone.utc)
    rows = []
    for reg_no, entry in manifest.items():
        last_checked = entry.get("last_checked")
        age_days = None
        if last_checked:
            try:
                dt = datetime.fromisoformat(last_checked.replace("Z", "+00:00"))
                age_days = (now - dt).days
            except Exception:
                pass
        current = next((v for v in entry.get("versions", []) if v.get("is_current")), None)
        rows.append({
            "p_number": reg_no,
            "trade_name": entry.get("trade_name") or (current.get("filename") if current else reg_no),
            "last_checked": last_checked,
            "age_days": age_days,
            "is_outdated": age_days is not None and age_days > OUTDATED_THRESHOLD_DAYS,
            "has_current": current is not None,
        })
    rows.sort(key=lambda r: (r["age_days"] is None, -(r["age_days"] or 0)))
    outdated = sum(1 for r in rows if r["is_outdated"])
    return {
        "threshold_days": OUTDATED_THRESHOLD_DAYS,
        "total": len(rows),
        "outdated": outdated,
        "labels": rows,
    }


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------
@app.post("/api/validate")
def validate_catalogue():
    """Run referential-integrity checks on the current catalogue.
    Logic lifted from scripts/validate_catalogue.py."""
    import orjson
    if not CATALOGUE_PATH.exists():
        raise HTTPException(404, "No catalogue on disk")
    data = orjson.loads(CATALOGUE_PATH.read_bytes())
    errors: list[str] = []
    warnings_: list[str] = []

    def _norm(code: str) -> str:
        return re.sub(r"[\s\-]+", "", code.lower())

    rm_codes = {r["rule_code"] for r in data.get("resistance_management_rules", [])}
    rm_codes_norm = {_norm(c): c for c in rm_codes}

    tp_rm_codes: set[str] = set()
    for tp in data.get("trade_products", []):
        for code in tp.get("rm_rule_codes", []) or []:
            tp_rm_codes.add(code)
            if code not in rm_codes and _norm(code) not in rm_codes_norm:
                warnings_.append(
                    f"Trade product '{tp['id']}' references RM code '{code}' not in parsed RM rules"
                )
    tp_rm_norms = {_norm(c) for c in tp_rm_codes}
    for code in sorted(rm_codes):
        if code not in tp_rm_codes and _norm(code) not in tp_rm_norms:
            warnings_.append(f"RM rule '{code}' exists but no trade product references it")

    ai_names = {ai["name"].lower() for ai in data.get("active_ingredients", [])}
    for rule in data.get("resistance_management_rules", []):
        for ai_name in rule.get("applicable_active_ingredients", []) or []:
            if ai_name.lower() not in ai_names:
                warnings_.append(
                    f"RM rule '{rule['rule_code']}' references AI '{ai_name}' not in active ingredients"
                )

    for fp in data.get("flagged_products", []):
        ai_lower = fp["active_ingredient"].lower()
        found = any(
            ai_lower in ai["name"].lower() or ai["name"].lower() in ai_lower
            for ai in data.get("active_ingredients", [])
        )
        if not found:
            warnings_.append(
                f"Flagged AI '{fp['active_ingredient']}' not found in active ingredients"
            )

    return {
        "errors": errors,
        "warnings": warnings_,
        "summary": {
            "total_products": len(data.get("trade_products", [])),
            "total_ais": len(data.get("active_ingredients", [])),
            "total_rm_rules": len(rm_codes),
        },
    }


# ---------------------------------------------------------------------------
# Catalogue Explorer — filterable browse of the schedule data
# ---------------------------------------------------------------------------
# Unlike `products` (summary list for label review), this view needs the
# full catalogue: AIs, RM rules, PHI/REI/WHP entries, sections, markets.
# Parsed once per request; catalogue.json is small (~3 MB).

def _read_catalogue() -> dict:
    if not CATALOGUE_PATH.exists():
        raise HTTPException(404, "No catalogue on disk. Run the pipeline first.")
    import orjson
    return orjson.loads(CATALOGUE_PATH.read_bytes())


@app.get("/api/catalogue")
def catalogue_browse():
    """Full catalogue for the Explorer view + filter metadata.
    Client-side filtering is fine for ~260 products."""
    cat = _read_catalogue()

    # Slimmed product list — full detail comes from /products/{id}
    tps = []
    sections: set[str] = set()
    for tp in cat.get("trade_products", []):
        sections.add(tp["section"])
        tps.append({
            "id": tp["id"],
            "name": tp["name"],
            "section": tp["section"],
            "active_ingredient_ids": tp.get("active_ingredient_ids", []),
            "rm_rule_codes": tp.get("rm_rule_codes", []),
            "acvm_registration_no": tp.get("acvm_registration_no"),
            "registrant": tp.get("registrant"),
            "formulation_type": tp.get("formulation_type"),
            "has_label": tp["id"] in label_texts,
        })

    # Active ingredients with MOA info for the MOA filter
    ais = []
    moa_groups: set[str] = set()
    for ai in cat.get("active_ingredients", []):
        moa = ai.get("moa_group_name")
        if moa:
            moa_groups.add(moa)
        ais.append({
            "id": ai["id"],
            "name": ai["name"],
            "section": ai["section"],
            "moa_group_name": moa,
            "frac_code": ai.get("frac_code"),
            "irac_code": ai.get("irac_code"),
            "hrac_code": ai.get("hrac_code"),
            "restriction_level": ai.get("restriction_level"),
        })

    # RM rules (lean)
    rm_rules = []
    for r in cat.get("resistance_management_rules", []):
        rm_rules.append({
            "rule_code": r["rule_code"],
            "moa_group_name": r.get("moa_group_name"),
            "category": r.get("category"),
            "max_applications": r.get("max_applications"),
            "requires_tank_mix": r.get("requires_tank_mix", False),
        })

    # Markets: distinct from PHI entries
    markets = sorted({e["market_code"] for e in cat.get("phi_entries", [])})

    return {
        "products": tps,
        "active_ingredients": ais,
        "rm_rules": rm_rules,
        "markets": markets,
        "sections": sorted(sections),
        "moa_groups": sorted(moa_groups),
        "season": cat.get("season"),
    }


@app.get("/api/catalogue/products/{product_id}")
def catalogue_product_detail(product_id: str):
    """Full per-product detail: AIs, RM rules, PHI/REI/WHP by market,
    ACVM info, label-review status."""
    cat = _read_catalogue()
    tp = next((p for p in cat.get("trade_products", []) if p["id"] == product_id), None)
    if not tp:
        raise HTTPException(404, f"Product {product_id} not in catalogue")

    ai_map = {a["id"]: a for a in cat.get("active_ingredients", [])}
    rule_map = {r["rule_code"]: r for r in cat.get("resistance_management_rules", [])}

    # Join PHI/REI/WHP by product id
    phi_by_market: dict[str, dict] = {}
    for e in cat.get("phi_entries", []):
        if e["trade_product_id"] == product_id:
            phi_by_market[e["market_code"]] = e["phi"]
    rei: dict | None = None
    for e in cat.get("rei_entries", []):
        if e["trade_product_id"] == product_id:
            rei = e["rei"]
            break
    whp: dict[str, dict] = {}
    for e in cat.get("whp_entries", []):
        if e["trade_product_id"] == product_id:
            whp[e["whp_type"]] = e["whp"]

    # Extraction / review status (from the in-memory maps)
    ext = extractions.get(product_id, {})
    ver = verified.get(product_id, {})
    label_status = {
        "has_label": product_id in label_texts,
        "extraction_confidence": ext.get("extraction_confidence", "none"),
        "fields_reviewed": len(ver),
        "total_fields": 17,
    }

    return {
        "product": tp,
        "active_ingredients": [ai_map[aid] for aid in tp.get("active_ingredient_ids", []) if aid in ai_map],
        "rm_rules": [rule_map[c] for c in tp.get("rm_rule_codes", []) or [] if c in rule_map],
        "phi_by_market": phi_by_market,
        "rei": rei,
        "whp": whp,
        "label_status": label_status,
    }


# ---------------------------------------------------------------------------
# Dashboard aggregate
# ---------------------------------------------------------------------------
@app.get("/api/dashboard")
def dashboard():
    """Stat-card data for the Dashboard page. One call, one render."""
    total = len(extractions)
    reviewed = sum(1 for v in verified.values() if v)
    fields_extracted = sum(_count_extracted_fields(ext) for ext in extractions.values())
    max_fields = total * 17 if total else 0
    coverage_pct = round(100 * fields_extracted / max_fields) if max_fields else 0

    # Freshness
    now = datetime.now(timezone.utc)
    outdated = 0
    for entry in manifest.values():
        lc = entry.get("last_checked")
        if not lc:
            outdated += 1
            continue
        try:
            dt = datetime.fromisoformat(lc.replace("Z", "+00:00"))
            if (now - dt).days > OUTDATED_THRESHOLD_DAYS:
                outdated += 1
        except Exception:
            outdated += 1

    unmatched = sum(1 for p in products if not p.get("acvm_registration_no"))

    return {
        "products_total": len(products),
        "products_unmatched_acvm": unmatched,
        "labels_total": len(label_paths),
        "labels_with_text": len(label_texts),
        "labels_outdated": outdated,
        "extraction_coverage_pct": coverage_pct,
        "reviewed": reviewed,
        "learned_patterns": sum(len(v) for v in learned_patterns.values()),
        "last_pipeline_run": pipeline_state.get("finished_at"),
        "last_pipeline_status": pipeline_state.get("phase") if not pipeline_state.get("running") else "running",
    }


def _load_catalogue_into_memory():
    """Parse catalogue.json and populate the `products` list + `product_reg_map`.
    Used at startup and after a rebuild."""
    products.clear()
    product_reg_map.clear()
    if not CATALOGUE_PATH.exists():
        return
    import orjson
    cat = orjson.loads(CATALOGUE_PATH.read_bytes())
    for tp in cat.get("trade_products", []):
        products.append({
            "id": tp["id"],
            "name": tp["name"],
            "section": tp["section"],
            "acvm_registration_no": tp.get("acvm_registration_no"),
        })
        if tp.get("acvm_registration_no"):
            product_reg_map[tp["id"]] = tp["acvm_registration_no"]


def _build_label_paths():
    """Discover current-version label PDFs on disk from the manifest."""
    for reg_no, entry in manifest.items():
        current = next((v for v in entry.get("versions", []) if v.get("is_current")), None)
        if not current:
            continue
        pdf_path = LABELS_DIR / reg_no / current["filename"]
        if not pdf_path.exists():
            continue
        pid = next((k for k, v in product_reg_map.items() if v == reg_no), None)
        if not pid:
            pid = reg_no
        label_paths[pid] = pdf_path


def _run_bootstrap(force: bool = False):
    """Background task: extract text from all label PDFs and run field
    extractors. Updates bootstrap_state as it goes. Called from
    POST /api/bootstrap/run in a daemon thread."""
    from src.parsers.label_text_extractor import extract_label_text
    from src.stages.extract_label_data import extract_single_label

    try:
        with _bootstrap_lock:
            if bootstrap_state["running"]:
                return
            bootstrap_state.update({
                "running": True,
                "phase": "extracting_text",
                "current": 0,
                "total": len(label_paths),
                "error": None,
                "started_at": _now_iso(),
                "finished_at": None,
            })

        # Phase 1: extract text from each PDF (skip if already cached unless force).
        for i, (pid, pdf_path) in enumerate(label_paths.items(), start=1):
            bootstrap_state["current"] = i
            if not force and pid in label_texts:
                continue
            try:
                text = extract_label_text(pdf_path)
                if text:
                    label_texts[pid] = text
            except Exception as e:
                logger.warning("Text extraction failed for %s: %s", pid, e)

        # Phase 2: run field extractors on every label.
        bootstrap_state["phase"] = "extracting_fields"
        bootstrap_state["current"] = 0
        bootstrap_state["total"] = len(label_texts)
        for i, pid in enumerate(list(label_texts.keys()), start=1):
            bootstrap_state["current"] = i
            if not force and pid in extractions:
                continue
            pdf_path = label_paths.get(pid)
            if not pdf_path:
                continue
            reg_no = product_reg_map.get(pid, pid)
            try:
                data = extract_single_label(pdf_path, pid, reg_no)
                if data:
                    extractions[pid] = data.model_dump()
            except Exception as e:
                logger.warning("Field extraction failed for %s: %s", pid, e)

        # Phase 3: persist caches.
        bootstrap_state["phase"] = "saving"
        _save_json(TEXT_CACHE, label_texts)
        _save_json(EXTRACTION_CACHE, extractions)

        bootstrap_state["phase"] = "done"
        logger.info("Bootstrap complete: %d texts, %d extractions",
                    len(label_texts), len(extractions))
    except Exception as e:
        logger.exception("Bootstrap failed")
        bootstrap_state["phase"] = "error"
        bootstrap_state["error"] = str(e)
    finally:
        bootstrap_state["running"] = False
        bootstrap_state["finished_at"] = _now_iso()


@app.get("/api/bootstrap/status")
def bootstrap_status():
    """Current bootstrap state, plus cache counts so the GUI can decide
    whether to prompt the user."""
    return {
        **bootstrap_state,
        "total_labels": len(label_paths),
        "texts_extracted": len(label_texts),
        "fields_extracted": len(extractions),
        "needs_bootstrap": len(label_paths) > 0 and (
            len(label_texts) < len(label_paths)
            or len(extractions) < len(label_texts)
        ),
    }


class BootstrapRunRequest(BaseModel):
    force: bool = False


@app.post("/api/bootstrap/run")
def bootstrap_run(req: BootstrapRunRequest | None = None):
    """Start bootstrap in a background thread. Returns immediately;
    poll /api/bootstrap/status for progress."""
    if bootstrap_state["running"]:
        return {"ok": False, "reason": "already_running"}
    force = bool(req and req.force)
    thread = threading.Thread(target=_run_bootstrap, args=(force,), daemon=True)
    thread.start()
    return {"ok": True}


def _load_json(path: Path) -> dict:
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return {}


def _save_json(path: Path, data: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False, default=str), encoding="utf-8")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# Serve the SPA
# ---------------------------------------------------------------------------
@app.get("/", response_class=HTMLResponse)
def index():
    return (TOOL_DIR / "static" / "index.html").read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# Product endpoints
# ---------------------------------------------------------------------------
class ProductSummary(BaseModel):
    id: str
    name: str
    section: str
    has_label: bool
    confidence: str
    verified_count: int
    total_fields: int
    extracted_count: int


@app.get("/api/products")
def list_products():
    result = []
    for p in products:
        pid = p["id"]
        ext = extractions.get(pid, {})
        ver = verified.get(pid, {})
        field_count = _count_extracted_fields(ext)
        result.append({
            "id": pid,
            "name": p["name"],
            "section": p["section"],
            "has_label": pid in label_texts,
            "confidence": ext.get("extraction_confidence", "none"),
            "verified_count": len(ver),
            "total_fields": 17,
            "extracted_count": field_count,
        })
    # Sort: none/low first, then medium, then high
    order = {"none": 0, "low": 1, "medium": 2, "high": 3}
    result.sort(key=lambda x: (order.get(x["confidence"], 0), x["name"]))
    return result


def _count_extracted_fields(ext: dict) -> int:
    count = 0
    if ext.get("active_ingredients"): count += 1
    if ext.get("target_rates"): count += 1
    if ext.get("container_sizes"): count += 1
    if ext.get("rainfastness_hours") is not None: count += 1
    if ext.get("max_applications_per_season") is not None: count += 1
    if ext.get("growth_stage_earliest") or ext.get("growth_stage_latest"): count += 1
    if ext.get("tank_mix_incompatible") or ext.get("tank_mix_required"): count += 1
    if ext.get("label_buffer_zone_m") is not None: count += 1
    if ext.get("ppe_requirements"): count += 1
    if ext.get("environmental_cautions"): count += 1
    if ext.get("shelf_life_years") is not None: count += 1
    if ext.get("hsr_number"): count += 1
    if ext.get("hsno_classifications"): count += 1
    if ext.get("signal_word"): count += 1
    if ext.get("label_whp_raw"): count += 1
    if ext.get("label_rei_raw"): count += 1
    if ext.get("acvm_number_from_label"): count += 1
    return count


@app.get("/api/products/{product_id}")
def get_product(product_id: str):
    product_info = next((p for p in products if p["id"] == product_id), None)
    if not product_info:
        raise HTTPException(404, f"Product {product_id} not found")
    ext = dict(extractions.get(product_id, {}))  # mutable copy
    has = product_id in label_texts

    # Apply corrections as overlays — these show immediately
    for corr in corrections.get(product_id, []):
        field = corr.get("field")
        value = corr.get("correct_value")
        if field and value is not None:
            ext[field] = value
            ext[field + "_raw"] = corr.get("raw_text", f"[manual correction: {value}]")

    # Apply annotations as overlays for fields that are still empty
    for anno in annotations.get(product_id, []):
        field = anno.get("field")
        value = anno.get("structured_value")
        if field and value is not None and not ext.get(field):
            ext[field] = value
            ext[field + "_raw"] = anno.get("selected_text", "")

    logger.info("GET product %s: has_label=%s, ext_keys=%d", product_id, has, len(ext))
    return {
        "product": product_info,
        "extraction": ext,
        "has_label": has,
        "verified": verified.get(product_id, {}),
        "corrections": corrections.get(product_id, []),
        "annotations": annotations.get(product_id, []),
    }


@app.get("/api/products/{product_id}/label")
def get_label_pdf(product_id: str):
    path = label_paths.get(product_id)
    if not path or not path.exists():
        raise HTTPException(404, "Label PDF not found")
    return FileResponse(str(path), media_type="application/pdf")


@app.get("/api/products/{product_id}/text")
def get_label_text(product_id: str):
    text = label_texts.get(product_id)
    if not text:
        raise HTTPException(404, "Label text not found")
    return {"text": text}


# ---------------------------------------------------------------------------
# Verification / Correction
# ---------------------------------------------------------------------------
class VerifyRequest(BaseModel):
    field: str
    status: str  # "correct", "wrong", "absent"


@app.post("/api/products/{product_id}/verify")
def verify_field(product_id: str, req: VerifyRequest):
    if product_id not in verified:
        verified[product_id] = {}
    verified[product_id][req.field] = {"status": req.status, "at": _now_iso()}
    _save_json(VERIFIED_PATH, verified)
    return {"ok": True}


class BulkVerifyRequest(BaseModel):
    fields: list[str]
    status: str  # "correct", "wrong", "absent"


@app.post("/api/products/{product_id}/verify/bulk")
def verify_bulk(product_id: str, req: BulkVerifyRequest):
    """Mark multiple fields with the same status. Used for Shift+V bulk-verify."""
    if product_id not in verified:
        verified[product_id] = {}
    applied: list[str] = []
    for field in req.fields:
        # Skip fields already verified so Undo only reverses *this* action.
        if field in verified[product_id]:
            continue
        verified[product_id][field] = {"status": req.status, "at": _now_iso()}
        applied.append(field)
    _save_json(VERIFIED_PATH, verified)
    return {"ok": True, "applied": applied}


class UnverifyRequest(BaseModel):
    fields: list[str]


@app.post("/api/products/{product_id}/verify/unverify")
def unverify_fields(product_id: str, req: UnverifyRequest):
    """Remove verification entries. Used for Undo after bulk-verify."""
    if product_id in verified:
        for field in req.fields:
            verified[product_id].pop(field, None)
        if not verified[product_id]:
            del verified[product_id]
    _save_json(VERIFIED_PATH, verified)
    return {"ok": True}


class CorrectRequest(BaseModel):
    field: str
    correct_value: str | int | float | list | dict | None
    raw_text: str | None = None


@app.post("/api/products/{product_id}/correct")
def correct_field(product_id: str, req: CorrectRequest):
    if product_id not in corrections:
        corrections[product_id] = []
    corrections[product_id].append({
        "field": req.field,
        "correct_value": req.correct_value,
        "raw_text": req.raw_text,
        "corrected_at": _now_iso(),
    })
    _save_json(CORRECTIONS_PATH, corrections)
    return {"ok": True}


# ---------------------------------------------------------------------------
# Annotation + Pattern Learning (the core loop)
# ---------------------------------------------------------------------------
class AnnotateRequest(BaseModel):
    field: str
    selected_text: str
    structured_value: str | int | float | list | dict | None = None


@app.post("/api/products/{product_id}/annotate")
def annotate_field(product_id: str, req: AnnotateRequest):
    from tool.pattern_engine import generate_candidates, test_pattern

    # Save annotation
    if product_id not in annotations:
        annotations[product_id] = []
    annotations[product_id].append({
        "field": req.field,
        "selected_text": req.selected_text,
        "structured_value": req.structured_value,
        "annotated_at": _now_iso(),
    })
    _save_json(ANNOTATIONS_PATH, annotations)

    # Generate candidate patterns
    candidates = generate_candidates(req.selected_text, req.field)

    # Determine which products already have this field extracted
    existing_matches = {
        pid for pid, ext in extractions.items()
        if _field_has_value(ext, req.field)
    }

    # Test each candidate against all label texts
    results = []
    for candidate in candidates:
        result = test_pattern(candidate.pattern, req.field, label_texts, existing_matches)
        candidate.test_result = result
        results.append({
            "pattern": candidate.pattern,
            "strategy": candidate.strategy,
            "is_valid": result.is_valid,
            "total_matches": result.total_matches,
            "new_matches": result.new_matches,
            "new_match_count": len(result.new_matches),
            "sample_texts": dict(list(result.matched_texts.items())[:5]),
        })

    return {"annotation_saved": True, "candidates": results}


def _field_has_value(ext: dict, field: str) -> bool:
    val = ext.get(field)
    if val is None:
        return False
    if isinstance(val, list) and len(val) == 0:
        return False
    return True


# ---------------------------------------------------------------------------
# Pattern management
# ---------------------------------------------------------------------------
class PatternTestRequest(BaseModel):
    pattern: str
    field: str


@app.post("/api/patterns/test")
def test_pattern_endpoint(req: PatternTestRequest):
    from tool.pattern_engine import test_pattern

    existing = {
        pid for pid, ext in extractions.items()
        if _field_has_value(ext, req.field)
    }
    result = test_pattern(req.pattern, req.field, label_texts, existing)
    return {
        "pattern": result.pattern,
        "is_valid": result.is_valid,
        "total_matches": result.total_matches,
        "new_matches": result.new_matches,
        "new_match_count": len(result.new_matches),
        "sample_texts": dict(list(result.matched_texts.items())[:10]),
    }


class PatternApproveRequest(BaseModel):
    pattern: str
    field: str
    source_product: str | None = None


@app.post("/api/patterns/approve")
def approve_pattern(req: PatternApproveRequest):
    from tool.pattern_engine import test_pattern

    # Final test
    existing = {
        pid for pid, ext in extractions.items()
        if _field_has_value(ext, req.field)
    }
    result = test_pattern(req.pattern, req.field, label_texts, existing)

    # Save to learned patterns
    if req.field not in learned_patterns:
        learned_patterns[req.field] = []
    learned_patterns[req.field].append({
        "pattern": req.pattern,
        "source": req.source_product,
        "test_results": {
            "new_matches": len(result.new_matches),
            "total_matches": result.total_matches,
        },
        "status": "approved",
        "added_at": _now_iso(),
    })
    _save_json(LEARNED_PATTERNS_PATH, learned_patterns)

    # Re-extract affected products + the source product
    products_to_reextract = set(result.new_matches)
    if req.source_product:
        products_to_reextract.add(req.source_product)

    re_extracted = 0
    from src.stages.extract_label_data import extract_single_label
    for pid in products_to_reextract:
        pdf_path = label_paths.get(pid)
        if pdf_path:
            reg_no = product_reg_map.get(pid, pid)
            data = extract_single_label(pdf_path, pid, reg_no)
            if data:
                extractions[pid] = data.model_dump()
                re_extracted += 1

    # Update the extraction cache so it persists
    _save_json(EXTRACTION_CACHE, extractions)

    return {
        "ok": True,
        "re_extracted": re_extracted,
        "new_matches": len(result.new_matches),
    }


@app.get("/api/patterns")
def list_patterns():
    return learned_patterns


# ---------------------------------------------------------------------------
# ACVM match review
# ---------------------------------------------------------------------------
def _acvm_product_summary(p) -> dict:
    """Shape an AcvmProduct for the GUI."""
    return {
        "p_number": p.registration_no,
        "trade_name": p.trade_name,
        "product_type": p.product_type,
        "registrant": p.registrant,
        "registration_date": p.registration_date,
        "ingredients": [
            {"name": ing.name, "content": ing.content, "unit": ing.unit}
            for ing in p.ingredients
        ],
    }


def _current_override_for(slug: str) -> dict | None:
    if slug in acvm_overrides["force"]:
        p_num = acvm_overrides["force"][slug]
        p = acvm_by_reg.get(p_num)
        return {
            "type": "force",
            "p_number": p_num,
            "trade_name": p.trade_name if p else None,
        }
    if slug in acvm_overrides["block"]:
        return {"type": "block", "reason": acvm_overrides["block"][slug]}
    return None


@app.get("/api/acvm/unmatched")
def acvm_unmatched():
    """Return catalogue products with no ACVM registration, each with top-5
    fuzzy-match suggestions. Also flags which ones are currently blocked
    (so the user can unblock) or forced (so they can clear/change)."""
    from rapidfuzz import fuzz, process as rf_process

    if not acvm_by_name:
        return {"available": False, "unmatched": [], "total_unmatched": 0, "total_products": len(products)}

    acvm_names = list(acvm_by_name.keys())
    unmatched: list[dict] = []

    for p in products:
        slug = p["id"]
        has_match = bool(p.get("acvm_registration_no"))
        override = _current_override_for(slug)
        # Show: (a) products with no match, (b) anything with an override
        # so the user can review and revert.
        if has_match and override is None:
            continue

        # Build fuzzy suggestions against all ACVM trade names.
        name = p["name"]
        clean = re.sub(r"\s*\[.*?\]\s*", " ", name).strip()
        raw_hits = rf_process.extract(
            clean, acvm_names, scorer=fuzz.token_sort_ratio, limit=5
        )
        suggestions = []
        for matched_name, score, _ in raw_hits:
            acvm_p = acvm_by_name[matched_name]
            summary = _acvm_product_summary(acvm_p)
            summary["score"] = int(score)
            suggestions.append(summary)

        unmatched.append({
            "slug": slug,
            "name": name,
            "section": p["section"],
            "current_match": p.get("acvm_registration_no"),
            "override": override,
            "suggestions": suggestions,
        })

    return {
        "available": True,
        "unmatched": unmatched,
        "total_unmatched": sum(1 for u in unmatched if not u["current_match"] and not u["override"]),
        "total_products": len(products),
    }


@app.get("/api/acvm/overrides")
def acvm_overrides_list():
    """Return current override entries so the GUI can render a management tab."""
    entries = []
    for slug, p_num in acvm_overrides["force"].items():
        p = acvm_by_reg.get(p_num)
        entries.append({
            "slug": slug,
            "action": "force",
            "p_number": p_num,
            "trade_name": p.trade_name if p else None,
            "registrant": p.registrant if p else None,
        })
    for slug, reason in acvm_overrides["block"].items():
        entries.append({
            "slug": slug,
            "action": "block",
            "reason": reason,
        })
    return {"overrides": entries}


@app.get("/api/acvm/product/{p_number}")
def acvm_product(p_number: str):
    """Look up a P-number directly — used when the user types one manually."""
    key = p_number.upper().strip()
    p = acvm_by_reg.get(key)
    if not p:
        raise HTTPException(404, f"P-number {key} not in ACVM register")
    return _acvm_product_summary(p)


class AcvmOverrideRequest(BaseModel):
    slug: str
    action: str  # "force" | "block" | "clear"
    p_number: str | None = None
    reason: str | None = None


@app.post("/api/acvm/override")
def acvm_override(req: AcvmOverrideRequest):
    """Apply an override to acvm_overrides.json. Takes effect on the next
    run of the ACVM pipeline stage — this endpoint does not rewrite the
    already-assembled catalogue."""
    slug = req.slug
    if req.action == "force":
        if not req.p_number:
            raise HTTPException(400, "p_number required for 'force'")
        key = req.p_number.upper().strip()
        if key not in acvm_by_reg:
            raise HTTPException(400, f"{key} is not in the ACVM register")
        acvm_overrides["force"][slug] = key
        acvm_overrides["block"].pop(slug, None)
    elif req.action == "block":
        if not req.reason:
            raise HTTPException(400, "reason required for 'block'")
        acvm_overrides["block"][slug] = req.reason
        acvm_overrides["force"].pop(slug, None)
    elif req.action == "clear":
        acvm_overrides["force"].pop(slug, None)
        acvm_overrides["block"].pop(slug, None)
    else:
        raise HTTPException(400, f"Unknown action: {req.action}")

    _save_acvm_overrides()
    return {"ok": True, "override": _current_override_for(slug)}


# ---------------------------------------------------------------------------
# Trade-name split overrides (fixes PDF parser merging two products into one)
# ---------------------------------------------------------------------------
def _load_product_splits_raw() -> dict:
    """Return the full splits file contents (including _comment), for the
    GUI to round-trip when writing."""
    if PRODUCT_SPLITS_PATH.exists():
        try:
            return json.loads(PRODUCT_SPLITS_PATH.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


@app.get("/api/product-splits")
def product_splits_list():
    """Current split overrides. Each entry maps a slug → array of trade names."""
    data = _load_product_splits_raw()
    entries = [
        {"slug": k, "names": v}
        for k, v in data.items()
        if not k.startswith("_") and isinstance(v, list)
    ]
    return {"splits": entries}


class ProductSplitRequest(BaseModel):
    slug: str
    names: list[str] | None = None  # None or [] = delete


@app.post("/api/product-splits")
def product_splits_set(req: ProductSplitRequest):
    """Save (or delete) a split entry. Takes effect next time the assemble
    stage runs."""
    data = _load_product_splits_raw()
    if not req.names:
        data.pop(req.slug, None)
    else:
        cleaned = [n.strip() for n in req.names if n.strip()]
        if len(cleaned) < 2:
            raise HTTPException(400, "A split must produce at least 2 names")
        data[req.slug] = cleaned
    # Preserve the _comment if it exists; add one if not.
    if "_comment" not in data:
        data = {
            "_comment": "Trade-name split overrides. Key = slug of the mashed-together product; value = array of the correct separate trade names.",
            **data,
        }
    _save_json(PRODUCT_SPLITS_PATH, data)
    return {"ok": True}


# ---------------------------------------------------------------------------
# Coverage
# ---------------------------------------------------------------------------
COVERAGE_FIELDS = [
    "hsr_number", "ppe_requirements", "environmental_cautions", "signal_word",
    "container_sizes", "active_ingredients", "acvm_number_from_label",
    "target_rates", "hsno_classifications", "storage_class", "label_whp_raw",
    "shelf_life_years", "tank_mix_incompatible", "rainfastness_hours",
    "max_applications_per_season", "growth_stage_earliest", "label_buffer_zone_m",
]


@app.get("/api/coverage")
def get_coverage():
    total = len(extractions)
    if total == 0:
        return {"total": 0, "fields": {}}
    fields = {}
    for field in COVERAGE_FIELDS:
        count = sum(1 for ext in extractions.values() if _field_has_value(ext, field))
        fields[field] = {"count": count, "total": total, "pct": round(100 * count / total)}
    verified_count = len(verified)
    reviewed_count = sum(1 for pid_fields in verified.values() if pid_fields)
    return {
        "total": total,
        "fields": fields,
        "verified": verified_count,
        "reviewed": reviewed_count,
        "learned_patterns": sum(len(v) for v in learned_patterns.values()),
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    uvicorn.run("tool.app:app", host="127.0.0.1", port=8000, reload=True)
