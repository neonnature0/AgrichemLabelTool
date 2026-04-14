"""Cross-validation checks for the assembled catalogue.

Usage:
    python -m scripts.validate_catalogue data/output/2025-2026/catalogue.json
"""

from __future__ import annotations

import sys
from pathlib import Path

import orjson
import typer
from rich.console import Console
from rich.table import Table

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

app = typer.Typer(help="Validate a catalogue JSON file")
console = Console()


@app.command()
def validate(
    catalogue_path: Path = typer.Argument(..., help="Path to catalogue.json"),
) -> None:
    """Run cross-validation checks on the catalogue."""
    if not catalogue_path.exists():
        console.print(f"[red]File not found:[/red] {catalogue_path}")
        raise typer.Exit(1)

    data = orjson.loads(catalogue_path.read_bytes())
    errors: list[str] = []
    warnings: list[str] = []

    # Build lookup sets
    ai_ids = {ai["id"] for ai in data["active_ingredients"]}
    tp_ids = {tp["id"] for tp in data["trade_products"]}
    rm_codes = {r["rule_code"] for r in data["resistance_management_rules"]}

    # Normalized lookup for fuzzy RM code matching (handles PDF formatting inconsistencies)
    import re

    def _norm(code: str) -> str:
        """Normalize RM code for fuzzy comparison: lowercase, strip spaces/hyphens."""
        return re.sub(r"[\s\-]+", "", code.lower())

    rm_codes_norm = {_norm(c): c for c in rm_codes}

    # --- Check 1: RM rule codes on trade products match parsed RM rules ---
    tp_rm_codes: set[str] = set()
    unmatched_tp_codes: list[tuple[str, str]] = []
    for tp in data["trade_products"]:
        for code in tp["rm_rule_codes"]:
            tp_rm_codes.add(code)
            if code not in rm_codes and _norm(code) not in rm_codes_norm:
                unmatched_tp_codes.append((tp["id"], code))

    for tp_id, code in unmatched_tp_codes:
        warnings.append(
            f"Trade product '{tp_id}' references RM code '{code}' "
            f"which is not in the parsed RM rules"
        )

    # Check for RM rules not referenced by any trade product
    tp_rm_norms = {_norm(c) for c in tp_rm_codes}
    for code in sorted(rm_codes):
        if code not in tp_rm_codes and _norm(code) not in tp_rm_norms:
            warnings.append(f"RM rule '{code}' exists but no trade product references it")

    # --- Check 2: RM applicable AIs match PHI table AIs ---
    ai_names = {ai["name"].lower() for ai in data["active_ingredients"]}
    for rule in data["resistance_management_rules"]:
        for ai_name in rule["applicable_active_ingredients"]:
            if ai_name.lower() not in ai_names:
                warnings.append(
                    f"RM rule '{rule['rule_code']}' references AI '{ai_name}' "
                    f"not found in active ingredients"
                )

    # --- Check 3: Flagged products exist in catalogue ---
    for fp in data["flagged_products"]:
        ai_lower = fp["active_ingredient"].lower()
        # Fuzzy match — flagged AI names may not match exactly
        found = any(ai_lower in ai["name"].lower() or ai["name"].lower() in ai_lower
                     for ai in data["active_ingredients"])
        if not found:
            warnings.append(
                f"Flagged AI '{fp['active_ingredient']}' not found in active ingredients"
            )

    # --- Print results ---
    table = Table(title="Validation Results")
    table.add_column("Check", style="bold")
    table.add_column("Status")

    table.add_row(
        "RM codes on products vs parsed rules",
        f"[green]OK[/green]" if not any("references RM code" in w for w in warnings)
        else f"[yellow]{sum(1 for w in warnings if 'references RM code' in w)} mismatches[/yellow]"
    )
    table.add_row(
        "RM AIs vs PHI AIs",
        f"[green]OK[/green]" if not any("references AI" in w for w in warnings)
        else f"[yellow]{sum(1 for w in warnings if 'references AI' in w)} mismatches[/yellow]"
    )
    table.add_row(
        "Flagged products in catalogue",
        f"[green]OK[/green]" if not any("Flagged AI" in w for w in warnings)
        else f"[yellow]{sum(1 for w in warnings if 'Flagged AI' in w)} not found[/yellow]"
    )
    table.add_row("Errors", f"[red]{len(errors)}[/red]" if errors else "[green]0[/green]")
    table.add_row("Warnings", f"[yellow]{len(warnings)}[/yellow]" if warnings else "[green]0[/green]")

    console.print(table)

    if warnings:
        console.print("\n[yellow]Warnings:[/yellow]")
        for w in warnings:
            console.print(f"  {w}")

    if errors:
        console.print("\n[red]Errors:[/red]")
        for e in errors:
            console.print(f"  {e}")
        raise typer.Exit(1)


if __name__ == "__main__":
    app()
