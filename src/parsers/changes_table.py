"""Significant Changes table parser — Section 4.1 of the NZW Spray Schedule.

The changes table has 4 columns on page 8 (index 7), split into 2 sub-tables:
  TOPIC | ACTIVE INGREDIENT / PRODUCT | (trade names) | EXPLANATION

The topic column carries forward from the previous row when it's None.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import asdict, dataclass, field
from pathlib import Path

import pdfplumber

from src.config import (
    CHANGES_TABLE_COLUMN_COUNT,
    CHANGES_TABLE_PAGE_RANGE,
    STAGING_DIR,
)

logger = logging.getLogger(__name__)


@dataclass
class ChangesTableRow:
    """One row from the Significant Changes table."""

    topic: str
    active_ingredient: str | None
    trade_products_raw: str | None  # may contain semicolons + newlines
    explanation: str | None
    page_number: int


@dataclass
class ChangesTableResult:
    """Result of parsing the Significant Changes table."""

    rows: list[ChangesTableRow]
    warnings: list[str] = field(default_factory=list)
    pages_processed: int = 0


def _clean_text(cell: str | None) -> str | None:
    """Clean cell text: rejoin hyphenated wraps, collapse whitespace."""
    if cell is None:
        return None
    # Rejoin hyphenated line breaks
    text = re.sub(r"(\w)-\n(\w)", r"\1\2", cell)
    text = text.replace("\n", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text if text else None


def _is_header_row(row: list[str | None]) -> bool:
    """Detect header rows containing 'TOPIC'."""
    return row[0] is not None and "TOPIC" in str(row[0])


def parse_changes_table(
    pdf_path: Path,
    *,
    page_range: tuple[int, int] | None = None,
    write_staging: bool = True,
) -> ChangesTableResult:
    """Parse the Significant Changes table from the NZW Spray Schedule PDF."""
    if page_range is None:
        page_range = CHANGES_TABLE_PAGE_RANGE

    start_page, end_page = page_range
    result = ChangesTableResult(rows=[], warnings=[], pages_processed=0)

    with pdfplumber.open(str(pdf_path)) as pdf:
        for page_idx in range(start_page, end_page + 1):
            if page_idx >= len(pdf.pages):
                break

            page = pdf.pages[page_idx]
            tables = page.extract_tables()
            result.pages_processed += 1

            for table in tables:
                if not table or len(table[0]) != CHANGES_TABLE_COLUMN_COUNT:
                    continue

                current_topic: str | None = None

                for row in table:
                    if _is_header_row(row):
                        continue

                    # Update topic if present, otherwise carry forward
                    topic_raw = _clean_text(row[0])
                    if topic_raw:
                        current_topic = topic_raw

                    if current_topic is None:
                        result.warnings.append(
                            f"Row with no topic on page {page_idx + 1}"
                        )
                        continue

                    ai = _clean_text(row[1])
                    products = _clean_text(row[2])
                    explanation = _clean_text(row[3])

                    # Skip rows with no data at all (just a topic header with no entries)
                    if ai is None and products is None and explanation is None:
                        continue

                    changes_row = ChangesTableRow(
                        topic=current_topic,
                        active_ingredient=ai,
                        trade_products_raw=products,
                        explanation=explanation,
                        page_number=page_idx + 1,
                    )
                    result.rows.append(changes_row)

    logger.info(
        "Parsed %d changes rows from %d pages",
        len(result.rows),
        result.pages_processed,
    )

    if write_staging:
        STAGING_DIR.mkdir(parents=True, exist_ok=True)
        staging_path = STAGING_DIR / "raw_changes_table.json"
        staging_data = [asdict(row) for row in result.rows]
        staging_path.write_text(
            json.dumps(staging_data, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

    return result
