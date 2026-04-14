"""CLI entry point for the NZ catalogue pipeline.

Usage:
    python -m scripts.run_pipeline --pdf data/input/schedule.pdf --season 2025-2026
    python -m scripts.run_pipeline --pdf data/input/schedule.pdf --season 2025-2026 --stages parse
    python -m scripts.run_pipeline --pdf data/input/schedule.pdf --season 2025-2026 --stages assemble
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import orjson
import typer
from rich.console import Console
from rich.table import Table

# Ensure project root is on path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.assembler import assemble_catalogue
from src.config import OUTPUT_DIR, STAGING_DIR
from src.parsers.changes_table import ChangesTableResult, parse_changes_table
from src.parsers.flagged_table import FlaggedTableResult, parse_flagged_table
from src.parsers.phi_table import PhiTableResult, parse_phi_table
from src.parsers.rm_table import RmTableResult, parse_rm_table
from src.utils.hashing import hash_file

app = typer.Typer(help="NZ Product Catalogue Pipeline")
console = Console()


@app.command()
def run(
    pdf: Path = typer.Option(..., help="Path to the NZW Spray Schedule PDF"),
    season: str = typer.Option(..., help="Season identifier, e.g. '2025-2026'"),
    output_dir: Path | None = typer.Option(None, help="Output directory (default: data/output/{season}/)"),
    stages: str = typer.Option("parse,assemble", help="Comma-separated stages: parse, assemble, acvm, labels, diff"),
    previous_season: str | None = typer.Option(None, help="Previous season for diff (e.g. '2024-2025')"),
    skip_acvm: bool = typer.Option(False, help="Skip ACVM network request (use cache only)"),
    skip_detail_scrape: bool = typer.Option(False, help="Skip ACVM detail page scraping"),
    skip_label_fetch: bool = typer.Option(False, help="Skip label PDF downloads"),
) -> None:
    """Run the pipeline stages."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    stage_list = [s.strip().lower() for s in stages.split(",")]
    out_dir = output_dir or OUTPUT_DIR / season

    if not pdf.exists():
        console.print(f"[red]PDF not found:[/red] {pdf}")
        raise typer.Exit(1)

    source_hash = hash_file(pdf)

    # Check idempotency
    catalogue_path = out_dir / "catalogue.json"
    if catalogue_path.exists():
        existing = orjson.loads(catalogue_path.read_bytes())
        if existing.get("source_hash") == source_hash:
            console.print(
                f"[yellow]Output already exists with same source hash. Skipping.[/yellow]\n"
                f"  {catalogue_path}"
            )
            raise typer.Exit(0)

    phi_result: PhiTableResult | None = None
    rm_result: RmTableResult | None = None
    changes_result: ChangesTableResult | None = None
    flagged_result: FlaggedTableResult | None = None

    # --- Parse stage ---
    if "parse" in stage_list:
        with console.status("[bold green]Parsing PHI table..."):
            phi_result = parse_phi_table(pdf, write_staging=True)
        console.print(f"[green]PHI: {len(phi_result.rows)} rows[/green]")

        with console.status("[bold green]Parsing RM table..."):
            rm_result = parse_rm_table(pdf, write_staging=True)
        console.print(f"[green]RM: {len(rm_result.rows)} rules[/green]")

        with console.status("[bold green]Parsing Changes table..."):
            changes_result = parse_changes_table(pdf, write_staging=True)
        console.print(f"[green]Changes: {len(changes_result.rows)} entries[/green]")

        with console.status("[bold green]Parsing Flagged Products table..."):
            flagged_result = parse_flagged_table(pdf, write_staging=True)
        console.print(f"[green]Flagged: {len(flagged_result.rows)} entries[/green]")

        all_warnings = (
            phi_result.warnings + rm_result.warnings
            + changes_result.warnings + flagged_result.warnings
        )
        if all_warnings:
            for w in all_warnings:
                console.print(f"  [yellow]WARNING: {w}[/yellow]")

    # --- Assemble stage ---
    if "assemble" in stage_list:
        if phi_result is None:
            import json
            from src.parsers.phi_table import PhiTableRow

            staging_path = STAGING_DIR / "raw_phi_table.json"
            if not staging_path.exists():
                console.print("[red]No staging data found. Run 'parse' stage first.[/red]")
                raise typer.Exit(1)

            raw_data = json.loads(staging_path.read_text(encoding="utf-8"))
            rows = [PhiTableRow(**row) for row in raw_data]
            phi_result = PhiTableResult(rows=rows, warnings=[], pages_processed=0)

            # Also load other staging files if available
            rm_result = _load_rm_staging()
            changes_result = _load_changes_staging()
            flagged_result = _load_flagged_staging()

        with console.status("[bold green]Assembling catalogue..."):
            catalogue = assemble_catalogue(
                phi_result,
                rm_result,
                changes_result,
                flagged_result,
                season=season,
                source_pdf=pdf.name,
                source_hash=source_hash,
            )

        # Write output
        out_dir.mkdir(parents=True, exist_ok=True)
        catalogue_bytes = orjson.dumps(
            catalogue.model_dump(),
            option=orjson.OPT_INDENT_2 | orjson.OPT_SORT_KEYS,
        )
        catalogue_path.write_bytes(catalogue_bytes)

        # Convenience extracts
        _write_extract(out_dir / "active_ingredients.json", catalogue.active_ingredients)
        _write_extract(out_dir / "trade_products.json", catalogue.trade_products)
        _write_extract(out_dir / "phi_matrix.json", catalogue.phi_entries)
        _write_extract(out_dir / "rm_rules.json", catalogue.resistance_management_rules)

        _print_summary(catalogue.stats)
        console.print(f"\n[green]Output written to:[/green] {out_dir}")

    # --- ACVM stage ---
    if "acvm" in stage_list:
        from src.models import SeasonCatalogue
        from src.parsers.acvm_csv import load_acvm_csv
        from src.stages.enrich_acvm import enrich_catalogue_with_acvm
        from src.stages.match_acvm import match_products

        if not catalogue_path.exists():
            console.print("[red]No catalogue found. Run 'assemble' stage first.[/red]")
            raise typer.Exit(1)

        cat = SeasonCatalogue(**orjson.loads(catalogue_path.read_bytes()))

        with console.status("[bold green]Loading ACVM register..."):
            acvm_products = load_acvm_csv(force_download=not skip_acvm)

        if acvm_products:
            with console.status("[bold green]Matching products to ACVM register..."):
                match_result = match_products(cat.trade_products, acvm_products)

            matched = len(match_result.matches)
            total = len(cat.trade_products)
            console.print(f"[green]ACVM: {matched}/{total} products matched ({100*matched//total}%)[/green]")

            with console.status("[bold green]Enriching catalogue..."):
                enriched_cat = enrich_catalogue_with_acvm(cat, match_result)

            # Rewrite catalogue with ACVM data
            catalogue_bytes = orjson.dumps(
                enriched_cat.model_dump(),
                option=orjson.OPT_INDENT_2 | orjson.OPT_SORT_KEYS,
            )
            catalogue_path.write_bytes(catalogue_bytes)

            # Rewrite extracts
            _write_extract(out_dir / "trade_products.json", enriched_cat.trade_products)

            if match_result.unmatched:
                console.print(f"  [yellow]{len(match_result.unmatched)} unmatched products[/yellow]")

            # Detail page scraping + label download
            if not skip_detail_scrape:
                from src.parsers.acvm_detail_scraper import create_session as create_acvm_session, scrape_details
                from src.stages.fetch_labels import fetch_labels
                from src.config import LABELS_DIR

                # Build slug→p_number mapping from matched products
                slug_to_pnum = {
                    slug: acvm.registration_no
                    for slug, acvm in match_result.matches.items()
                    if acvm.registration_no
                }
                slug_to_name = {tp.id: tp.name for tp in cat.trade_products}
                detail_cache = STAGING_DIR / "acvm_detail_cache"

                console.print(f"[green]Scraping ACVM detail pages for {len(slug_to_pnum)} products...[/green]")
                acvm_session = create_acvm_session()
                detail_results = scrape_details(
                    acvm_session, slug_to_pnum, cache_dir=detail_cache,
                )
                with_labels = sum(1 for r in detail_results.values() if r.labels)
                console.print(f"[green]Details: {len(detail_results)} scraped, {with_labels} have labels[/green]")

                if not skip_label_fetch and with_labels > 0:
                    console.print("[green]Downloading label PDFs...[/green]")
                    label_docs = fetch_labels(
                        acvm_session, detail_results, slug_to_name,
                    )
                    console.print(f"[green]Labels: {len(label_docs)} documents[/green]")

                    # Add label documents to catalogue
                    if label_docs:
                        enriched_cat_data = enriched_cat.model_dump()
                        enriched_cat_data["label_documents"] = [d.model_dump() for d in label_docs]
                        catalogue_bytes = orjson.dumps(
                            enriched_cat_data,
                            option=orjson.OPT_INDENT_2 | orjson.OPT_SORT_KEYS,
                        )
                        catalogue_path.write_bytes(catalogue_bytes)

        else:
            console.print("[yellow]No ACVM data available. Skipping enrichment.[/yellow]")

    # --- Labels stage ---
    if "labels" in stage_list:
        from src.models import SeasonCatalogue
        from src.stages.extract_label_data import extract_all_labels
        from src.config import LABELS_DIR

        if not catalogue_path.exists():
            console.print("[red]No catalogue found. Run 'assemble' stage first.[/red]")
            raise typer.Exit(1)

        cat = SeasonCatalogue(**orjson.loads(catalogue_path.read_bytes()))
        manifest_path = LABELS_DIR / "manifest.json"

        if manifest_path.exists():
            import json as json_mod
            manifest = json_mod.loads(manifest_path.read_text(encoding="utf-8"))

            console.print(f"[green]Extracting data from {len(manifest)} label PDFs...[/green]")
            extractions = extract_all_labels(LABELS_DIR, manifest, cat)
            console.print(f"[green]Labels: {len(extractions)} extractions[/green]")

            # Write label extractions to catalogue
            cat_data = cat.model_dump()
            cat_data["label_extractions"] = [e.model_dump() for e in extractions]
            cat_data["stats"]["total_label_extractions"] = len(extractions)
            catalogue_path.write_bytes(
                orjson.dumps(cat_data, option=orjson.OPT_INDENT_2 | orjson.OPT_SORT_KEYS)
            )

            # Convenience extract
            _write_extract(out_dir / "label_extractions.json", extractions)

            # Coverage summary
            with_ais = sum(1 for e in extractions if e.active_ingredients)
            with_rates = sum(1 for e in extractions if e.target_rates)
            with_containers = sum(1 for e in extractions if e.container_sizes)
            console.print(f"  AIs: {with_ais}, Rates: {with_rates}, Containers: {with_containers}")
        else:
            console.print("[yellow]No label manifest found. Run 'acvm' stage first.[/yellow]")

    # --- Diff stage ---
    if "diff" in stage_list:
        from src.models import SeasonCatalogue
        from src.stages.diff_seasons import diff_seasons

        # Load current season catalogue
        if not catalogue_path.exists():
            console.print("[red]No current catalogue found. Run 'assemble' stage first.[/red]")
            raise typer.Exit(1)

        curr_cat = SeasonCatalogue(**orjson.loads(catalogue_path.read_bytes()))

        # Find previous season
        prev_season = previous_season
        if not prev_season:
            # Auto-detect: look for adjacent season directory
            parts = season.split("-")
            if len(parts) == 2:
                prev_season = f"{int(parts[0]) - 1}-{int(parts[1]) - 1}"

        if prev_season:
            prev_path = OUTPUT_DIR / prev_season / "catalogue.json"
            if prev_path.exists():
                with console.status(f"[bold green]Diffing {prev_season} -> {season}..."):
                    prev_cat = SeasonCatalogue(**orjson.loads(prev_path.read_bytes()))
                    changelog = diff_seasons(prev_cat, curr_cat)

                console.print(f"[green]Diff: {len(changelog)} changes detected[/green]")

                # Summary by severity
                by_severity: dict[str, int] = {}
                for e in changelog:
                    by_severity[e.severity] = by_severity.get(e.severity, 0) + 1
                for sev in ["critical", "warning", "info"]:
                    if sev in by_severity:
                        color = {"critical": "red", "warning": "yellow", "info": "dim"}[sev]
                        console.print(f"  [{color}]{sev}: {by_severity[sev]}[/{color}]")

                # Write changelog extract
                _write_extract(out_dir / "changelog.json", changelog)
                console.print(f"  Changelog written to: {out_dir / 'changelog.json'}")
            else:
                console.print(f"[yellow]Previous season not found at {prev_path}. Skipping diff.[/yellow]")
        else:
            console.print("[yellow]No previous season specified. Skipping diff.[/yellow]")


def _load_rm_staging() -> RmTableResult | None:
    """Load RM staging data if available."""
    import json
    from src.parsers.rm_table import RmTableRow

    path = STAGING_DIR / "raw_rm_table.json"
    if not path.exists():
        return None
    raw = json.loads(path.read_text(encoding="utf-8"))
    return RmTableResult(rows=[RmTableRow(**r) for r in raw])


def _load_changes_staging() -> ChangesTableResult | None:
    """Load Changes staging data if available."""
    import json
    from src.parsers.changes_table import ChangesTableRow

    path = STAGING_DIR / "raw_changes_table.json"
    if not path.exists():
        return None
    raw = json.loads(path.read_text(encoding="utf-8"))
    return ChangesTableResult(rows=[ChangesTableRow(**r) for r in raw])


def _load_flagged_staging() -> FlaggedTableResult | None:
    """Load Flagged staging data if available."""
    import json
    from src.parsers.flagged_table import FlaggedTableRow

    path = STAGING_DIR / "raw_flagged_table.json"
    if not path.exists():
        return None
    raw = json.loads(path.read_text(encoding="utf-8"))
    return FlaggedTableResult(rows=[FlaggedTableRow(**r) for r in raw])


def _write_extract(path: Path, items: list) -> None:
    """Write a convenience extract JSON file."""
    data = [item.model_dump() for item in items]
    path.write_bytes(orjson.dumps(data, option=orjson.OPT_INDENT_2 | orjson.OPT_SORT_KEYS))


def _print_summary(stats) -> None:
    """Print a Rich summary table."""
    table = Table(title="Catalogue Summary")
    table.add_column("Metric", style="bold")
    table.add_column("Count", justify="right")

    table.add_row("Active Ingredients", str(stats.total_active_ingredients))
    table.add_row("Trade Products", str(stats.total_trade_products))
    table.add_row("PHI Entries", str(stats.total_phi_entries))
    table.add_row("REI Entries", str(stats.total_rei_entries))
    table.add_row("WHP Entries", str(stats.total_whp_entries))
    table.add_row("RM Rules", str(stats.total_rm_rules))
    table.add_row("Schedule Changes", str(stats.total_schedule_changes))
    table.add_row("Flagged Products", str(stats.total_flagged_products))
    table.add_row("", "")
    for section, count in sorted(stats.products_by_section.items()):
        table.add_row(f"  {section}", str(count))
    table.add_row("", "")
    table.add_row("Warnings", str(len(stats.parse_warnings)))

    console.print(table)


if __name__ == "__main__":
    app()
