"""Resistance Management table parser — extracts RM rules from the NZW Spray Schedule.

The RM table (Section 4.4) has 5 columns across ~3 pages:
  Code/Group Name | Applicable Active Ingredients | Rule applies to | Resistance Management Rule | Additional notes

Divided into sections: FUNGICIDES, HERBICIDES, INSECTICIDES, WOUND DRESSINGS.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import asdict, dataclass, field
from pathlib import Path

import pdfplumber

from src.config import (
    RM_HEADER_SENTINEL,
    RM_TABLE_COLUMN_COUNT,
    RM_TABLE_PAGE_RANGE,
    SECTIONS,
    STAGING_DIR,
)

logger = logging.getLogger(__name__)


@dataclass
class RmTableRow:
    """One row from the RM table."""

    code_raw: str
    active_ingredients_raw: str | None
    trade_products_raw: str | None
    rule_text: str | None
    additional_notes: str | None
    section: str
    page_number: int


@dataclass
class RmTableResult:
    """Result of parsing the RM table."""

    rows: list[RmTableRow]
    warnings: list[str] = field(default_factory=list)
    pages_processed: int = 0


def _clean_text(cell: str | None) -> str | None:
    """Strip whitespace from a cell, return None if empty."""
    if cell is None:
        return None
    text = cell.strip()
    return text if text else None


def _is_header_row(row: list[str | None]) -> bool:
    """Detect repeated header rows."""
    return (
        row[0] is not None
        and RM_HEADER_SENTINEL in str(row[0])
        and "Group" in str(row[0])
    )


def _is_section_row(row: list[str | None]) -> str | None:
    """Detect section header rows. Returns normalized section name or None."""
    if not row[0]:
        return None
    if not all(c is None for c in row[1:]):
        return None

    text = row[0].strip().upper()
    for section in SECTIONS:
        if text == section:
            return section.lower().replace(" ", "_")
    return None


def parse_rm_table(
    pdf_path: Path,
    *,
    page_range: tuple[int, int] | None = None,
    write_staging: bool = True,
) -> RmTableResult:
    """Parse the Resistance Management table from the NZW Spray Schedule PDF.

    Args:
        pdf_path: Path to the spray schedule PDF.
        page_range: 0-indexed (start, end) inclusive page range.
        write_staging: If True, write raw_rm_table.json to staging dir.

    Returns:
        RmTableResult with parsed rows and any warnings.
    """
    if page_range is None:
        page_range = RM_TABLE_PAGE_RANGE

    start_page, end_page = page_range
    result = RmTableResult(rows=[], warnings=[], pages_processed=0)
    current_section = "unknown"

    with pdfplumber.open(str(pdf_path)) as pdf:
        for page_idx in range(start_page, end_page + 1):
            if page_idx >= len(pdf.pages):
                result.warnings.append(
                    f"Page index {page_idx} out of range (PDF has {len(pdf.pages)} pages)"
                )
                break

            page = pdf.pages[page_idx]
            tables = page.extract_tables()
            result.pages_processed += 1

            for table in tables:
                if not table or len(table[0]) != RM_TABLE_COLUMN_COUNT:
                    continue

                for row in table:
                    # Skip header rows
                    if _is_header_row(row):
                        continue

                    # Detect section headers
                    section = _is_section_row(row)
                    if section is not None:
                        current_section = section
                        continue

                    # Skip empty rows
                    code = _clean_text(row[0])
                    if not code:
                        continue

                    rm_row = RmTableRow(
                        code_raw=code,
                        active_ingredients_raw=_clean_text(row[1]),
                        trade_products_raw=_clean_text(row[2]),
                        rule_text=_clean_text(row[3]),
                        additional_notes=_clean_text(row[4]),
                        section=current_section,
                        page_number=page_idx + 1,
                    )
                    result.rows.append(rm_row)

    logger.info(
        "Parsed %d RM rows from %d pages (%d warnings)",
        len(result.rows),
        result.pages_processed,
        len(result.warnings),
    )

    if write_staging:
        STAGING_DIR.mkdir(parents=True, exist_ok=True)
        staging_path = STAGING_DIR / "raw_rm_table.json"
        staging_data = [asdict(row) for row in result.rows]
        staging_path.write_text(
            json.dumps(staging_data, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        logger.info("RM staging output written to %s", staging_path)

    return result
