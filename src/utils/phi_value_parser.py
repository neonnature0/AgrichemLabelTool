"""Parse raw PHI / REI / WHP cell text into structured PhiValue objects.

Handles all observed patterns from the 2025/2026 NZW Spray Schedule:
- Numeric days: "14", "65", "1"
- Growth stages: "EL18", "EL25"
- Compound: "EL4 + 14 days" (may contain newlines)
- Ranges: "EL1 - EL2" (may contain newlines)
- SFPT combinations: "SFPT EL18"
- Special codes: NS, SNC, NPV, ID, WFD, DNG, N/A, nil
- REI formats: "WFD", "4 hrs", "8 days"
- WHP formats: "2 months", "1 day", "2 months - see notes column", "2 mths SNC"
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

from src.models import PhiValue

if TYPE_CHECKING:
    pass

# Compile regexes once
_RE_SFPT_EL = re.compile(r"^SFPT\s+(EL\d+)$", re.IGNORECASE)
_RE_EL_OFFSET = re.compile(r"^(EL\d+)\s*\+\s*(\d+)\s*(?:days?)?$", re.IGNORECASE)
_RE_EL_RANGE = re.compile(r"^(EL\d+)\s*[-–—]\s*(EL\d+)$", re.IGNORECASE)
_RE_EL_SINGLE = re.compile(r"^(EL\d+)$", re.IGNORECASE)
_RE_NUMERIC = re.compile(r"^(\d+)$")
_RE_VALUE_UNIT = re.compile(r"^(\d+)\s*(hrs?|hours?|days?|months?|mths?)$", re.IGNORECASE)

# Special codes — exact match after normalization (case-insensitive)
_SPECIAL_CODES = {"ns", "snc", "npv", "id", "wfd", "dng", "n/a", "nil", "nc/nc"}

# Canonical form for special codes
_CODE_CANONICAL: dict[str, str] = {
    "ns": "NS",
    "snc": "SNC",
    "npv": "NPV",
    "id": "ID",
    "wfd": "WFD",
    "dng": "DNG",
    "n/a": "N/A",
    "nil": "nil",
    "nc/nc": "NC/NC",
}

# Unit normalization
_UNIT_CANONICAL: dict[str, str] = {
    "hr": "hrs",
    "hrs": "hrs",
    "hour": "hrs",
    "hours": "hrs",
    "day": "days",
    "days": "days",
    "month": "months",
    "months": "months",
    "mth": "months",
    "mths": "months",
}


def _normalize(raw: str) -> str:
    """Replace newlines with spaces, collapse whitespace, strip."""
    return re.sub(r"\s+", " ", raw.replace("\n", " ")).strip()


def parse_phi_value(raw: str | None) -> PhiValue | None:
    """Parse a raw cell value into a structured PhiValue.

    Returns None for empty/blank cells.
    """
    if raw is None:
        return None

    original = raw
    text = _normalize(raw)

    if not text or text == "-" or text == "—" or text == "–":
        return None

    lower = text.lower()

    # --- Special codes (exact match) ---
    if lower in _SPECIAL_CODES:
        return PhiValue(raw=original, code=_CODE_CANONICAL[lower])

    # --- Compound codes with SNC suffix: "2 months - see notes column", "2 mths SNC" ---
    snc_suffix = re.match(
        r"^(\d+)\s*(months?|mths?)\s*[-–—]?\s*(?:see\s+notes?\s+column|SNC)$",
        text,
        re.IGNORECASE,
    )
    if snc_suffix:
        val = int(snc_suffix.group(1))
        return PhiValue(
            raw=original,
            value=val,
            unit="months",
            code="SNC",
        )

    # --- "see notes column" alone ---
    if "see notes column" in lower:
        return PhiValue(raw=original, code="SNC")

    # --- SFPT + growth stage: "SFPT EL18" ---
    m = _RE_SFPT_EL.match(text)
    if m:
        return PhiValue(
            raw=original,
            code="SFPT",
            secondary_el_stage=m.group(1).upper(),
        )

    # --- EL compound: "EL4 + 14 days" ---
    m = _RE_EL_OFFSET.match(text)
    if m:
        return PhiValue(
            raw=original,
            el_stage=m.group(1).upper(),
            el_offset_days=int(m.group(2)),
        )

    # --- EL range: "EL1 - EL2" ---
    m = _RE_EL_RANGE.match(text)
    if m:
        return PhiValue(
            raw=original,
            el_stage=m.group(1).upper(),
            el_stage_end=m.group(2).upper(),
        )

    # --- Single EL: "EL18" ---
    m = _RE_EL_SINGLE.match(text)
    if m:
        return PhiValue(raw=original, el_stage=m.group(1).upper())

    # --- Value + unit: "4 hrs", "8 days", "2 months", "6 months" ---
    m = _RE_VALUE_UNIT.match(text)
    if m:
        val = int(m.group(1))
        unit = _UNIT_CANONICAL.get(m.group(2).lower(), m.group(2).lower())
        return PhiValue(raw=original, value=val, unit=unit)

    # --- Plain numeric: "14", "65", "1" ---
    m = _RE_NUMERIC.match(text)
    if m:
        return PhiValue(raw=original, value=int(m.group(1)), unit="days")

    # --- Fallback: store as code ---
    return PhiValue(raw=original, code=text)
