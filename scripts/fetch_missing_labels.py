"""Fetch labels for products that have ACVM P-numbers but no downloaded label PDF.

Usage:
    python -m scripts.fetch_missing_labels
    python -m scripts.fetch_missing_labels --dry-run
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import orjson
import typer
from rich.console import Console

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.config import LABELS_DIR, OUTPUT_DIR

app = typer.Typer(help="Fetch missing label PDFs")
console = Console()


@app.command()
def fetch(
    dry_run: bool = typer.Option(False, help="Show what would be fetched without downloading"),
    season: str = typer.Option("2025-2026", help="Season to check"),
) -> None:
    # Load catalogue to get product → P-number mapping
    cat_path = OUTPUT_DIR / season / "catalogue.json"
    if not cat_path.exists():
        console.print(f"[red]Catalogue not found: {cat_path}[/red]")
        raise typer.Exit(1)

    cat = orjson.loads(cat_path.read_bytes())
    products_with_pnum: dict[str, str] = {}  # slug → P-number
    product_names: dict[str, str] = {}  # slug → display name

    for tp in cat["trade_products"]:
        pnum = tp.get("acvm_registration_no")
        if pnum:
            products_with_pnum[tp["id"]] = pnum
            product_names[tp["id"]] = tp["name"]

    # Load manifest to see what's already downloaded
    manifest_path = LABELS_DIR / "manifest.json"
    manifest = {}
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    # Find products with P-numbers but no label on disk
    missing: dict[str, str] = {}  # slug → P-number
    for slug, pnum in products_with_pnum.items():
        if pnum not in manifest:
            # Check if PDF directory exists with any file
            pdf_dir = LABELS_DIR / pnum
            if not pdf_dir.exists() or not list(pdf_dir.glob("*.pdf")):
                missing[slug] = pnum

    console.print(f"Products with P-number: {len(products_with_pnum)}")
    console.print(f"Already have labels: {len(manifest)}")
    console.print(f"[yellow]Missing labels: {len(missing)}[/yellow]")

    if not missing:
        console.print("[green]All labels downloaded![/green]")
        return

    for slug, pnum in sorted(missing.items()):
        console.print(f"  {product_names.get(slug, slug)}: {pnum}")

    if dry_run:
        console.print(f"\n[yellow]Dry run — {len(missing)} labels would be fetched.[/yellow]")
        return

    # Scrape detail pages and download labels
    from src.parsers.acvm_detail_scraper import create_session, scrape_details
    from src.stages.fetch_labels import fetch_labels
    from src.config import STAGING_DIR

    console.print(f"\n[green]Scraping ACVM detail pages for {len(missing)} products...[/green]")
    session = create_session()
    detail_cache = STAGING_DIR / "acvm_detail_cache"

    detail_results = scrape_details(
        session, missing, cache_dir=detail_cache,
    )

    with_labels = sum(1 for r in detail_results.values() if r.labels)
    console.print(f"[green]Details: {len(detail_results)} scraped, {with_labels} have labels[/green]")

    if with_labels > 0:
        console.print("[green]Downloading label PDFs...[/green]")
        label_docs = fetch_labels(session, detail_results, product_names)
        console.print(f"[green]Downloaded {len(label_docs)} labels[/green]")

        # Invalidate the tool's cache so it picks up new labels on next start
        tool_cache = Path("data/corrections/label_texts_cache.json")
        if tool_cache.exists():
            tool_cache.unlink()
            console.print("[yellow]Tool cache cleared — restart the tool to see new labels.[/yellow]")
    else:
        console.print("[yellow]No labels found on ACVM detail pages for these products.[/yellow]")


if __name__ == "__main__":
    app()
