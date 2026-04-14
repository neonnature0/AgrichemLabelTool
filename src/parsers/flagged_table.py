"""Flagged Products table parser — Section 4.2 of the NZW Spray Schedule.

The flagged table has 4 columns on page 9 (index 8):
  ACTIVE INGREDIENT | TRADE NAME | LABEL CLAIM | ISSUE

Parsing challenges:
- Row 0: header is merged into col 0 (all other cols None)
- Row 1: organophosphates row is also merged into col 0
- Rows 2-9: standard 4-column layout
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import asdict, dataclass, field
from pathlib import Path

import pdfplumber

from src.config import (
    FLAGGED_TABLE_COLUMN_COUNT,
    FLAGGED_TABLE_PAGE_INDEX,
    STAGING_DIR,
)

logger = logging.getLogger(__name__)


@dataclass
class FlaggedTableRow:
    """One row from the Flagged Products table."""

    active_ingredient: str
    trade_products_raw: str | None
    label_claim: str | None
    issue: str
    page_number: int


@dataclass
class FlaggedTableResult:
    """Result of parsing the Flagged Products table."""

    rows: list[FlaggedTableRow]
    warnings: list[str] = field(default_factory=list)
    pages_processed: int = 0


def _clean_text(cell: str | None) -> str | None:
    """Clean cell text."""
    if cell is None:
        return None
    text = re.sub(r"(\w)-\n(\w)", r"\1\2", cell)
    text = text.replace("\n", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text if text else None


# Known flagged product AI names — used to identify the start of merged rows.
# These are hardcoded because pdfplumber merges the 4-column content into a
# single string for some rows, making reliable split-by-position impossible.
# The tradeoff is these need updating per season (but the flagged table barely changes).
_KNOWN_FLAGGED_AIS: dict[str, dict] = {
    "all organophosphates": {
        "active_ingredient": "All organophosphates, carbamates and chlorothalonil",
        "trade_products_raw": "Any product containing these active ingredients",
        "label_claim": None,
        "issue": "SWNZ members must not use any products containing these active ingredients.",
    },
    "dimethomorph": {
        "active_ingredient": "dimethomorph",
        "trade_products_raw": "Sovrin Flo; Sphinx Fungicide",
        "label_claim": "downy mildew",
        "issue": "Dimethomorph has been withdrawn from use in the EU. Our expectation is that the MRL will eventually be withdrawn. Residues in wine are unlikely to be present when used up to EL18. As there is no grandfathering provision in the EU, our advice to EU exporters is to cease use after EL18.",
    },
    "dithiocarbamates": {
        "active_ingredient": "Dithiocarbamates (all products containing mancozeb, metiram and thiram)",
        "trade_products_raw": "Dithane Rainshield Neo Tec; Eurozeb 75WG; Kencozeb 750 WG; Manco 75 WG; Mancostar 750; Manzate Evolution Fungicide; MetiGro; Polyram DF; Penncozeb DF; Ridomil Gold MZ; Thiram 40F; Thiram 80 WDG",
        "label_claim": None,
        "issue": "The situation in EU for dithiocarbamates remains volatile and subject to change. Residues in wine are unlikely to be present when used up to EL18. As there is no grandfathering provision in the EU, our advice to EU exporters is cease use after EL18.",
    },
    "flazasulfuron": {
        "active_ingredient": "flazasulfuron",
        "trade_products_raw": "Katana",
        "label_claim": "pre-emergent weed control",
        "issue": "PRIOR TO USE each season, SWNZ members must apply for written permission to use this product. A maximum of 1 application in non-consecutive seasons is permitted. May only be used on glyphosate or glufosinate resistant weeds from EL41 to EL2.",
    },
    "phosphonic acid": {
        "active_ingredient": "phosphonic acid (previously phosphorus acid)",
        "trade_products_raw": "Agri-Fos; Foschek; Hi PK; Ken-phosphorus Fos; Phosgard; Perk Supa; Phostemic; Tree-Doc, or any other product containing this active ingredient",
        "label_claim": "no grape claims exist",
        "issue": "This active ingredient is not registered for use on grapes. SWNZ members must not use any products containing this active ingredient. Use during the growing season will result in wine residues.",
    },
}


def _try_parse_merged_row(cell_text: str, page_number: int) -> FlaggedTableRow | None:
    """Attempt to parse a row where all content is merged into col 0.

    Uses a lookup table of known flagged AIs because the merged text is
    unreliably formatted (columns bleed into each other).
    """
    if not cell_text:
        return None
    text_lower = cell_text.lower()

    for key, data in _KNOWN_FLAGGED_AIS.items():
        if text_lower.startswith(key):
            return FlaggedTableRow(
                active_ingredient=data["active_ingredient"],
                trade_products_raw=data["trade_products_raw"],
                label_claim=data["label_claim"],
                issue=data["issue"],
                page_number=page_number,
            )

    return None


def parse_flagged_table(
    pdf_path: Path,
    *,
    page_index: int | None = None,
    write_staging: bool = True,
) -> FlaggedTableResult:
    """Parse the Flagged Products table from the NZW Spray Schedule PDF."""
    if page_index is None:
        page_index = FLAGGED_TABLE_PAGE_INDEX

    result = FlaggedTableResult(rows=[], warnings=[], pages_processed=1)

    with pdfplumber.open(str(pdf_path)) as pdf:
        if page_index >= len(pdf.pages):
            result.warnings.append(f"Page index {page_index} out of range")
            return result

        page = pdf.pages[page_index]
        tables = page.extract_tables()

        for table in tables:
            if not table or len(table[0]) != FLAGGED_TABLE_COLUMN_COUNT:
                continue

            for ri, row in enumerate(table):
                # Row 0: merged header — skip
                if ri == 0:
                    continue

                # Check for merged rows (all content in col 0, rest None)
                if row[1] is None and row[2] is None and row[3] is None:
                    merged_row = _try_parse_merged_row(row[0], page_index + 1)
                    if merged_row:
                        result.rows.append(merged_row)
                    else:
                        result.warnings.append(
                            f"Could not parse merged row {ri}: "
                            f"{row[0][:60] if row[0] else 'None'}..."
                        )
                    continue

                ai = _clean_text(row[0])
                issue = _clean_text(row[3])

                if not ai or not issue:
                    result.warnings.append(f"Skipping row {ri} with missing AI or issue")
                    continue

                flagged_row = FlaggedTableRow(
                    active_ingredient=ai,
                    trade_products_raw=_clean_text(row[1]),
                    label_claim=_clean_text(row[2]),
                    issue=issue,
                    page_number=page_index + 1,
                )
                result.rows.append(flagged_row)

    logger.info("Parsed %d flagged product rows", len(result.rows))

    if write_staging:
        STAGING_DIR.mkdir(parents=True, exist_ok=True)
        staging_path = STAGING_DIR / "raw_flagged_table.json"
        staging_data = [asdict(row) for row in result.rows]
        staging_path.write_text(
            json.dumps(staging_data, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

    return result
