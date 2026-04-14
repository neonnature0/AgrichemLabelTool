"""Label freshness checker — re-checks labels for updated versions.

Usage:
    python -m scripts.run_label_check --max-age-months 6
    python -m scripts.run_label_check --product P007595
    python -m scripts.run_label_check --max-age-months 6 --dry-run
"""

from __future__ import annotations

import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.config import LABELS_DIR

app = typer.Typer(help="Label freshness checker")
console = Console()


@app.command()
def check(
    max_age_months: int = typer.Option(6, help="Re-check labels older than this many months"),
    product: str | None = typer.Option(None, help="Check a specific P-number only"),
    dry_run: bool = typer.Option(False, help="Show what would be checked without downloading"),
) -> None:
    """Check labels for freshness and download new versions if found."""
    manifest_path = LABELS_DIR / "manifest.json"
    if not manifest_path.exists():
        console.print("[yellow]No manifest found. Run the pipeline with --stages acvm first.[/yellow]")
        raise typer.Exit(0)

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    now = datetime.now(timezone.utc)
    stale: list[tuple[str, dict]] = []

    for reg_no, entry in manifest.items():
        if product and reg_no != product:
            continue

        last_checked = entry.get("last_checked")
        if last_checked:
            checked_dt = datetime.fromisoformat(last_checked)
            age_days = (now - checked_dt).days
            age_months = age_days / 30.44
            if age_months < max_age_months and not product:
                continue

        stale.append((reg_no, entry))

    if not stale:
        console.print(f"[green]All {len(manifest)} labels are fresh (checked within {max_age_months} months).[/green]")
        return

    console.print(f"[yellow]{len(stale)} labels need checking (of {len(manifest)} total)[/yellow]")

    if dry_run:
        table = Table(title="Labels to Check (Dry Run)")
        table.add_column("P-Number")
        table.add_column("Trade Name")
        table.add_column("Last Checked")
        table.add_column("Current Label")

        for reg_no, entry in stale:
            current = next((v for v in entry.get("versions", []) if v.get("is_current")), None)
            table.add_row(
                reg_no,
                entry.get("trade_name", "?"),
                entry.get("last_checked", "never")[:10],
                current["filename"][:40] if current else "none",
            )

        console.print(table)
        return

    # Actually re-check
    from src.parsers.acvm_detail_scraper import create_session, search_by_p_number
    from src.stages.fetch_labels import _hash_bytes

    session = create_session()
    updated = 0
    errors = 0

    for reg_no, entry in stale:
        name = entry.get("trade_name", reg_no)
        console.print(f"  Checking {reg_no} ({name})...")

        time.sleep(0.8)
        try:
            detail = search_by_p_number(session, reg_no)
            if not detail or not detail.labels:
                console.print(f"    [yellow]No labels found[/yellow]")
                continue

            current_label = detail.labels[0]

            # Download and compare hash
            time.sleep(0.8)
            resp = session.get(current_label.url, timeout=60)
            resp.raise_for_status()
            new_hash = _hash_bytes(resp.content)

            # Check against existing
            existing_current = next(
                (v for v in entry.get("versions", []) if v.get("is_current")), None
            )

            if existing_current and existing_current.get("hash") == new_hash:
                console.print(f"    [green]No change[/green]")
            else:
                console.print(f"    [yellow]NEW VERSION detected![/yellow]")
                updated += 1

            entry["last_checked"] = now.isoformat()

        except Exception as e:
            console.print(f"    [red]Error: {e}[/red]")
            errors += 1

    # Save updated manifest
    manifest_path.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False, sort_keys=True),
        encoding="utf-8",
    )

    console.print(f"\n[green]Checked: {len(stale)}, Updated: {updated}, Errors: {errors}[/green]")


if __name__ == "__main__":
    app()
