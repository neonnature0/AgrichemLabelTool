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
import re
import warnings
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, HTMLResponse
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
    global products, manifest, verified, corrections, annotations, learned_patterns

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    logger.info("Loading catalogue and label data...")

    # Load catalogue
    if CATALOGUE_PATH.exists():
        import orjson
        cat = orjson.loads(CATALOGUE_PATH.read_bytes())
        products.clear()
        for tp in cat.get("trade_products", []):
            products.append({
                "id": tp["id"],
                "name": tp["name"],
                "section": tp["section"],
                "acvm_registration_no": tp.get("acvm_registration_no"),
            })
            if tp.get("acvm_registration_no"):
                product_reg_map[tp["id"]] = tp["acvm_registration_no"]

    # Load manifest
    if MANIFEST_PATH.exists():
        manifest.update(json.loads(MANIFEST_PATH.read_text(encoding="utf-8")))

    # Check for cached texts and extractions
    # (constants defined at module level for use in other endpoints)

    if TEXT_CACHE.exists() and EXTRACTION_CACHE.exists():
        logger.info("Loading from cache...")
        cached_texts = json.loads(TEXT_CACHE.read_text(encoding="utf-8"))
        cached_extractions = json.loads(EXTRACTION_CACHE.read_text(encoding="utf-8"))

        # Build label paths
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

        label_texts.update(cached_texts)
        extractions.update(cached_extractions)
        logger.info("Loaded %d texts and %d extractions from cache", len(label_texts), len(extractions))
    else:
        # First run: extract from PDFs and cache
        from src.parsers.label_text_extractor import extract_label_text

        logger.info("First run — extracting text from %d label PDFs...", len(manifest))
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
            text = extract_label_text(pdf_path)
            if text:
                label_texts[pid] = text

        logger.info("Loaded %d label texts into memory", len(label_texts))

        # Run extraction on all labels
        from src.stages.extract_label_data import extract_single_label

        for pid in label_texts:
            pdf_path = label_paths.get(pid)
            if not pdf_path:
                continue
            reg_no = product_reg_map.get(pid, pid)
            data = extract_single_label(pdf_path, pid, reg_no)
            if data:
                extractions[pid] = data.model_dump()

        logger.info("Extracted data for %d labels", len(extractions))

        # Cache for fast restart
        _save_json(TEXT_CACHE, label_texts)
        _save_json(EXTRACTION_CACHE, extractions)
        logger.info("Cached texts and extractions for fast restart")

    # Load correction files
    CORRECTIONS_DIR.mkdir(parents=True, exist_ok=True)
    verified.update(_load_json(VERIFIED_PATH))
    corrections.update(_load_json(CORRECTIONS_PATH))
    annotations.update(_load_json(ANNOTATIONS_PATH))
    learned_patterns.update(_load_json(LEARNED_PATTERNS_PATH))


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
