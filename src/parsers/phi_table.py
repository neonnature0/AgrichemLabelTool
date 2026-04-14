"""PHI table parser — extracts the master product list from the NZW Spray Schedule PDF.

Handles:
- 26-column table spanning ~11 pages (indices 12-22 for 2025/2026)
- Rotated/reversed column headers (skipped by sentinel detection)
- Section headers (FUNGICIDES, HERBICIDES, INSECTICIDES, WOUND DRESSINGS)
- Semicolon-separated trade names (not split here — done by assembler)
- Hyphenated line breaks in text cells
- One known column misalignment (thiram row on page 18)
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import asdict, dataclass, field
from pathlib import Path

import pdfplumber

from src.config import (
    COL_ACTIVE_INGREDIENT,
    COL_LABEL_CLAIM,
    COL_NOTES,
    COL_REI,
    COL_RM_RULE_CODES,
    COL_TRADE_NAME,
    COL_WHP_GRAZING,
    COL_WHP_SLAUGHTER,
    HEADER_SENTINEL,
    MARKET_COLUMNS,
    PHI_TABLE_COLUMN_COUNT,
    PHI_TABLE_PAGE_RANGE,
    SECTIONS,
    STAGING_DIR,
)

logger = logging.getLogger(__name__)


@dataclass
class PhiTableRow:
    """One row from the PHI table — may represent multiple trade products
    if trade_names_raw contains semicolons."""

    active_ingredient_raw: str
    trade_names_raw: str
    label_claim: str | None
    rei_raw: str | None
    notes: str | None
    rm_rule_codes_raw: str | None
    whp_slaughter_raw: str | None
    whp_grazing_raw: str | None
    phi_values: dict[str, str]  # market_code -> raw cell text
    section: str
    page_number: int


@dataclass
class PhiTableResult:
    """Result of parsing the PHI table."""

    rows: list[PhiTableRow]
    warnings: list[str] = field(default_factory=list)
    pages_processed: int = 0


def _clean_text(cell: str | None) -> str | None:
    """Strip whitespace from a cell, return None if empty."""
    if cell is None:
        return None
    text = cell.strip()
    return text if text else None


def _is_header_row(row: list[str | None]) -> bool:
    """Detect repeated header rows by checking for the reversed sentinel text."""
    col8 = row[8] if len(row) > 8 else None
    return row[0] is None and col8 is not None and HEADER_SENTINEL in str(col8)


def _is_section_row(row: list[str | None]) -> str | None:
    """Detect section header rows. Returns the section name or None.

    Section rows have text in col 0 and all other cols are None.
    Only known section names are treated as sections — the copper note
    row (which also matches the pattern) is excluded.
    """
    if not row[0]:
        return None
    # pdfplumber sometimes gives empty strings instead of None for blank cells
    if not all(c is None or (isinstance(c, str) and c.strip() == "") for c in row[1:]):
        return None

    text = row[0].strip().upper()
    for section in SECTIONS:
        if text == section:
            return section.lower().replace(" ", "_")
    return None


def _repair_misaligned_row(row: list[str | None]) -> list[str | None]:
    """Detect and repair column misalignment in market PHI columns.

    Known case: thiram row where pdfplumber merges cols 15-24 into one cell
    containing space-separated values like "EL18 14 28 28 28 28 28 EL18 28 28",
    with cols 16-24 as None and col 25 having its own value.

    Strategy: find the merged cell, split its tokens, redistribute into the
    None slots. Verify total count matches.
    """
    repaired = list(row)
    market_start = min(MARKET_COLUMNS.keys())
    market_end = max(MARKET_COLUMNS.keys())

    for col_idx in range(market_start, market_end + 1):
        cell = repaired[col_idx]
        if cell is None:
            continue

        # Check if this cell contains space-separated values that look like
        # multiple PHI entries (at least 3 space-separated tokens)
        tokens = cell.split()
        if len(tokens) < 3:
            continue

        # Count how many None cells follow this one (up to end of market cols)
        none_count = 0
        for j in range(col_idx + 1, market_end + 1):
            if repaired[j] is None:
                none_count += 1
            else:
                break

        if none_count == 0:
            continue

        # The tokens should fill this cell + the None cells after it
        # (the cell after the None run may already have its own value)
        slots = 1 + none_count  # this cell + the None cells
        if len(tokens) != slots:
            logger.warning(
                "Misalignment repair: col %d has %d tokens but %d slots. "
                "Skipping repair for row: %s",
                col_idx,
                len(tokens),
                slots,
                row[COL_ACTIVE_INGREDIENT],
            )
            continue

        # Redistribute
        logger.info(
            "Repairing misaligned row: col %d had %d merged values for %s",
            col_idx,
            len(tokens),
            row[COL_ACTIVE_INGREDIENT],
        )
        for i, token in enumerate(tokens):
            repaired[col_idx + i] = token

    return repaired


def parse_phi_table(
    pdf_path: Path,
    *,
    page_range: tuple[int, int] | None = None,
    write_staging: bool = True,
) -> PhiTableResult:
    """Parse the PHI table from the NZW Spray Schedule PDF.

    Args:
        pdf_path: Path to the spray schedule PDF.
        page_range: 0-indexed (start, end) inclusive page range.
                    Defaults to PHI_TABLE_PAGE_RANGE from config.
        write_staging: If True, write raw_phi_table.json to staging dir.

    Returns:
        PhiTableResult with parsed rows and any warnings.
    """
    if page_range is None:
        page_range = PHI_TABLE_PAGE_RANGE

    start_page, end_page = page_range
    result = PhiTableResult(rows=[], warnings=[], pages_processed=0)
    current_section = "unknown"

    with pdfplumber.open(str(pdf_path)) as pdf:
        for page_idx in range(start_page, end_page + 1):
            if page_idx >= len(pdf.pages):
                result.warnings.append(f"Page index {page_idx} out of range (PDF has {len(pdf.pages)} pages)")
                break

            page = pdf.pages[page_idx]
            tables = page.extract_tables()
            result.pages_processed += 1

            for table in tables:
                if not table or len(table[0]) != PHI_TABLE_COLUMN_COUNT:
                    continue  # Skip non-PHI tables (e.g., legend tables on page 13)

                for row in table:
                    # Skip header rows
                    if _is_header_row(row):
                        continue

                    # Detect section headers
                    section = _is_section_row(row)
                    if section is not None:
                        current_section = section
                        continue

                    # Skip rows that look like section notes (e.g. copper note)
                    # These have text in col 0 and all other cols None/empty
                    if row[0] and all(
                        c is None or (isinstance(c, str) and c.strip() == "")
                        for c in row[1:]
                    ):
                        result.warnings.append(
                            f"Skipped non-section note row on page {page_idx + 1}: "
                            f"{row[0][:60]}..."
                        )
                        continue

                    # Repair misaligned rows
                    row = _repair_misaligned_row(row)

                    # Extract fields
                    ai_raw = _clean_text(row[COL_ACTIVE_INGREDIENT])
                    trade_raw = _clean_text(row[COL_TRADE_NAME])

                    if not ai_raw or not trade_raw:
                        result.warnings.append(
                            f"Skipped row with missing AI or trade name on page {page_idx + 1}"
                        )
                        continue

                    # Extract market PHI values
                    phi_values: dict[str, str] = {}
                    for col_idx, market_code in MARKET_COLUMNS.items():
                        cell = _clean_text(row[col_idx])
                        if cell is not None:
                            phi_values[market_code] = cell

                    phi_row = PhiTableRow(
                        active_ingredient_raw=ai_raw,
                        trade_names_raw=trade_raw,
                        label_claim=_clean_text(row[COL_LABEL_CLAIM]),
                        rei_raw=_clean_text(row[COL_REI]),
                        notes=_clean_text(row[COL_NOTES]),
                        rm_rule_codes_raw=_clean_text(row[COL_RM_RULE_CODES]),
                        whp_slaughter_raw=_clean_text(row[COL_WHP_SLAUGHTER]),
                        whp_grazing_raw=_clean_text(row[COL_WHP_GRAZING]),
                        phi_values=phi_values,
                        section=current_section,
                        page_number=page_idx + 1,
                    )
                    result.rows.append(phi_row)

    logger.info(
        "Parsed %d rows from %d pages (%d warnings)",
        len(result.rows),
        result.pages_processed,
        len(result.warnings),
    )

    # Write staging output
    if write_staging:
        STAGING_DIR.mkdir(parents=True, exist_ok=True)
        staging_path = STAGING_DIR / "raw_phi_table.json"
        staging_data = [asdict(row) for row in result.rows]
        staging_path.write_text(
            json.dumps(staging_data, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        logger.info("Staging output written to %s", staging_path)

    return result
