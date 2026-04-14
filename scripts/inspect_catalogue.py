"""Catalogue inspector — pretty-print and search the pipeline output.

Usage:
    python -m scripts.inspect_catalogue data/output/2025-2026/catalogue.json
    python -m scripts.inspect_catalogue data/output/2025-2026/catalogue.json --search mancozeb
    python -m scripts.inspect_catalogue data/output/2025-2026/catalogue.json --product kenja-400-sc
    python -m scripts.inspect_catalogue data/output/2025-2026/catalogue.json --section fungicides
"""

from __future__ import annotations

import sys
from pathlib import Path

import orjson
import typer
from rich.console import Console
from rich.table import Table

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

app = typer.Typer(help="Inspect a catalogue JSON file")
console = Console()


def _load_catalogue(path: Path) -> dict:
    if not path.exists():
        console.print(f"[red]File not found:[/red] {path}")
        raise typer.Exit(1)
    return orjson.loads(path.read_bytes())


@app.command()
def inspect(
    catalogue_path: Path = typer.Argument(..., help="Path to catalogue.json"),
    search: str | None = typer.Option(None, help="Search products/ingredients by name"),
    product: str | None = typer.Option(None, help="Show full detail for a product slug"),
    section: str | None = typer.Option(None, help="Filter by section name"),
    changelog: bool = typer.Option(False, help="Show changelog entries"),
    severity: str | None = typer.Option(None, help="Filter changelog by severity (critical/warning/info)"),
) -> None:
    """Inspect the catalogue."""
    data = _load_catalogue(catalogue_path)

    if changelog:
        _show_changelog(data, severity_filter=severity)
    elif product:
        _show_product_detail(data, product)
    elif search:
        _search_products(data, search)
    elif section:
        _list_section(data, section)
    else:
        _show_stats(data)


def _show_stats(data: dict) -> None:
    """Print catalogue statistics."""
    stats = data["stats"]
    table = Table(title=f"Catalogue: {data['season']}")
    table.add_column("Metric", style="bold")
    table.add_column("Value", justify="right")

    table.add_row("Season", data["season"])
    table.add_row("Source PDF", data["source_pdf"])
    table.add_row("Parser Version", data["parser_version"])
    table.add_row("Parsed At", data["parsed_at"])
    table.add_row("", "")
    table.add_row("Active Ingredients", str(stats["total_active_ingredients"]))
    table.add_row("Trade Products", str(stats["total_trade_products"]))
    table.add_row("PHI Entries", str(stats["total_phi_entries"]))
    table.add_row("REI Entries", str(stats["total_rei_entries"]))
    table.add_row("WHP Entries", str(stats["total_whp_entries"]))
    table.add_row("", "")
    for section, count in sorted(stats["products_by_section"].items()):
        table.add_row(f"  {section}", str(count))

    console.print(table)

    if stats["parse_warnings"]:
        console.print(f"\n[yellow]Warnings ({len(stats['parse_warnings'])}):[/yellow]")
        for w in stats["parse_warnings"]:
            console.print(f"  WARNING: {w}")


def _search_products(data: dict, query: str) -> None:
    """Search products and ingredients by name."""
    q = query.lower()

    # Search active ingredients
    ai_matches = [
        ai for ai in data["active_ingredients"] if q in ai["name"].lower()
    ]
    if ai_matches:
        table = Table(title=f"Active Ingredients matching '{query}'")
        table.add_column("ID")
        table.add_column("Name")
        table.add_column("Section")
        for ai in ai_matches:
            table.add_row(ai["id"], ai["name"], ai["section"])
        console.print(table)

    # Search trade products
    tp_matches = [
        tp for tp in data["trade_products"] if q in tp["name"].lower()
    ]
    if tp_matches:
        table = Table(title=f"Trade Products matching '{query}'")
        table.add_column("ID")
        table.add_column("Name")
        table.add_column("Section")
        table.add_column("AIs")
        for tp in tp_matches:
            table.add_row(
                tp["id"],
                tp["name"],
                tp["section"],
                ", ".join(tp["active_ingredient_ids"]),
            )
        console.print(table)

    if not ai_matches and not tp_matches:
        console.print(f"[yellow]No matches for '{query}'[/yellow]")


def _show_product_detail(data: dict, slug: str) -> None:
    """Show full detail for a single product."""
    tp = next((t for t in data["trade_products"] if t["id"] == slug), None)
    if not tp:
        console.print(f"[red]Product not found:[/red] {slug}")
        raise typer.Exit(1)

    console.print(f"\n[bold]{tp['name']}[/bold] ({tp['id']})")
    console.print(f"  Section: {tp['section']}")
    console.print(f"  Active Ingredients: {', '.join(tp['active_ingredient_ids'])}")
    if tp.get("acvm_registration_no"):
        console.print(f"  ACVM Registration: {tp['acvm_registration_no']}")
        console.print(f"  Registrant: {tp.get('registrant', 'N/A')}")
        console.print(f"  Registration Date: {tp.get('acvm_registration_date', 'N/A')}")
    if tp["label_claim"]:
        console.print(f"  Label Claim: {tp['label_claim']}")
    if tp["notes"]:
        console.print(f"  Notes: {tp['notes']}")
    if tp["rm_rule_codes"]:
        console.print(f"  RM Rules: {', '.join(tp['rm_rule_codes'])}")

    # PHI entries
    phi_entries = [p for p in data["phi_entries"] if p["trade_product_id"] == slug]
    if phi_entries:
        table = Table(title="PHI Entries")
        table.add_column("Market")
        table.add_column("Value")
        table.add_column("Unit")
        table.add_column("EL Stage")
        table.add_column("Code")
        table.add_column("Raw")
        for p in sorted(phi_entries, key=lambda x: x["market_code"]):
            phi = p["phi"]
            table.add_row(
                p["market_code"],
                str(phi.get("value", "")),
                phi.get("unit", ""),
                phi.get("el_stage", ""),
                phi.get("code", ""),
                phi["raw"][:30],
            )
        console.print(table)

    # REI
    rei_entries = [r for r in data["rei_entries"] if r["trade_product_id"] == slug]
    if rei_entries:
        rei = rei_entries[0]["rei"]
        console.print(f"\n  REI: {rei['raw']}")

    # WHP
    whp_entries = [w for w in data["whp_entries"] if w["trade_product_id"] == slug]
    if whp_entries:
        for w in whp_entries:
            console.print(f"  WHP ({w['whp_type']}): {w['whp']['raw']}")

    # Labels
    label_docs = [l for l in data.get("label_documents", []) if l["trade_product_id"] == slug]
    if label_docs:
        console.print(f"\n  Labels:")
        for ld in label_docs:
            current = "[green]current[/green]" if ld.get("is_current") else "[dim]superseded[/dim]"
            console.print(
                f"    {ld['filename'][:50]} | "
                f"{ld.get('label_date', 'N/A')} | "
                f"{current}"
            )


def _list_section(data: dict, section: str) -> None:
    """List all products in a section."""
    matches = [tp for tp in data["trade_products"] if tp["section"] == section.lower()]
    if not matches:
        console.print(f"[yellow]No products in section '{section}'[/yellow]")
        return

    table = Table(title=f"{section.title()} ({len(matches)} products)")
    table.add_column("ID")
    table.add_column("Name")
    table.add_column("AIs")
    table.add_column("Label Claim")
    for tp in matches:
        table.add_row(
            tp["id"],
            tp["name"],
            ", ".join(tp["active_ingredient_ids"]),
            (tp.get("label_claim") or "")[:40],
        )
    console.print(table)


def _show_changelog(data: dict, severity_filter: str | None = None) -> None:
    """Display changelog entries with severity colour-coding."""
    entries = data.get("changelog", [])
    if not entries:
        console.print("[yellow]No changelog entries in this catalogue.[/yellow]")
        console.print("Run the pipeline with --stages diff to generate a changelog.")
        return

    if severity_filter:
        entries = [e for e in entries if e["severity"] == severity_filter.lower()]

    if not entries:
        console.print(f"[yellow]No {severity_filter} changelog entries.[/yellow]")
        return

    # Summary counts
    by_severity: dict[str, int] = {}
    for e in entries:
        by_severity[e["severity"]] = by_severity.get(e["severity"], 0) + 1

    summary = Table(title="Changelog Summary")
    summary.add_column("Severity")
    summary.add_column("Count", justify="right")
    for sev in ["critical", "warning", "info"]:
        if sev in by_severity:
            color = {"critical": "red", "warning": "yellow", "info": "dim"}[sev]
            summary.add_row(f"[{color}]{sev}[/{color}]", str(by_severity[sev]))
    console.print(summary)

    # Full listing
    table = Table(title="Changelog Entries")
    table.add_column("Severity", width=8)
    table.add_column("Type")
    table.add_column("Entity")
    table.add_column("Market", width=6)
    table.add_column("Change")
    table.add_column("Description")

    for e in entries:
        sev = e["severity"]
        color = {"critical": "red", "warning": "yellow", "info": "dim"}.get(sev, "")
        change = ""
        if e.get("old_value") and e.get("new_value"):
            change = f"{e['old_value']} -> {e['new_value']}"

        table.add_row(
            f"[{color}]{sev}[/{color}]",
            e["change_type"],
            e.get("entity_id", "")[:25] or "",
            e.get("market", "") or "",
            change[:30],
            e["description"][:50],
        )

    console.print(table)


if __name__ == "__main__":
    app()
