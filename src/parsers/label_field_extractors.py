"""Field extractors for agchem label PDFs.

Each function takes the full label text and returns structured data + raw text.
Patterns are organised by field, not by manufacturer.
All extractors are fault-tolerant — they return None/empty on failure.
"""

from __future__ import annotations

import json
import logging
import re
from pathlib import Path

from src.models import LabelActiveIngredient, TargetRate

logger = logging.getLogger(__name__)

# Path to learned patterns from the verification tool
_LEARNED_PATTERNS_PATH = Path(__file__).resolve().parent.parent.parent / "data" / "corrections" / "learned_patterns.json"


def _load_learned_patterns(field: str) -> list[re.Pattern]:
    """Load user-approved patterns from the verification tool.

    These persist in data/corrections/learned_patterns.json and are
    loaded alongside built-in patterns on every extraction run.
    """
    if not _LEARNED_PATTERNS_PATH.exists():
        return []
    try:
        data = json.loads(_LEARNED_PATTERNS_PATH.read_text(encoding="utf-8"))
        patterns = []
        for entry in data.get(field, []):
            if entry.get("status") == "approved":
                try:
                    patterns.append(re.compile(entry["pattern"], re.IGNORECASE))
                except re.error:
                    logger.warning("Invalid learned pattern for %s: %s", field, entry["pattern"])
        return patterns
    except Exception:
        return []

# ---------------------------------------------------------------------------
# ACTIVE INGREDIENTS
# ---------------------------------------------------------------------------

_AI_PATTERNS = [
    # "Contains 252 g/kg boscalid and 128 g/kg pyraclostrobin in the form of..."
    # "TOPAS 200 EW contains 200 g/litre PENCONAZOLE in the form of..."
    # "Active ingredient: Contains 400 g/L Isofetamid"
    re.compile(
        r"contains?\s+(.{10,400}?)(?:in the form of|Also contains|$)",
        re.IGNORECASE | re.DOTALL,
    ),
]

_AI_VALUE_PATTERN = re.compile(
    r"(\d+(?:\.\d+)?)\s*(g/kg|g/litre|g/l|g/L|mg/kg|mg/L|mL/L)\s+([A-Za-z][\w\s\-()]+?)(?:\s+(?:and|plus|\+|,)|in the|$)",
    re.IGNORECASE,
)

_UNIT_NORMALIZE = {
    "g/litre": "g/L",
    "g/l": "g/L",
    "mg/l": "mg/L",
    "ml/l": "mL/L",
}


def extract_active_ingredients(text: str) -> list[LabelActiveIngredient]:
    """Extract active ingredient names and concentrations from label text."""
    results: list[LabelActiveIngredient] = []

    all_patterns = list(_AI_PATTERNS) + _load_learned_patterns("active_ingredients")
    for pattern in all_patterns:
        m = pattern.search(text)
        if not m:
            continue

        # Use group(1) if available, otherwise use the full match
        try:
            ai_text = m.group(1)
        except IndexError:
            ai_text = m.group(0)

        for aim in _AI_VALUE_PATTERN.finditer(ai_text):
            value = float(aim.group(1))
            unit = _UNIT_NORMALIZE.get(aim.group(2).lower(), aim.group(2))
            name = aim.group(3).strip().rstrip(",. ")
            basis = "w/v" if "/L" in unit or "/l" in unit else "w/w"

            results.append(LabelActiveIngredient(
                name=name,
                concentration_value=value,
                concentration_unit=unit,
                concentration_basis=basis,
            ))

        if results:
            break

    return results


# ---------------------------------------------------------------------------
# CONTAINER SIZES
# ---------------------------------------------------------------------------

_CONTAINER_PATTERNS = [
    re.compile(
        r"(?:net\s+)?contents?\s*:?\s*([\d.,\s]+(?:m[Ll]|[Ll](?:itre)?|kg|g)(?:\s*,\s*[\d.,\s]+(?:m[Ll]|[Ll](?:itre)?|kg|g))*)",
        re.IGNORECASE,
    ),
]


def extract_container_sizes(text: str) -> list[str]:
    """Extract available container/pack sizes from the label."""
    for pattern in _CONTAINER_PATTERNS:
        m = pattern.search(text)
        if m:
            raw = m.group(1)
            # Split on commas and clean
            sizes = [s.strip() for s in re.split(r",", raw) if s.strip()]
            # Normalize: "1 litre" → "1 L", "500 ml" → "500 mL"
            normalized = []
            for s in sizes:
                s = re.sub(r"\s*litre?s?\b", " L", s, flags=re.IGNORECASE)
                s = re.sub(r"\s*ml\b", " mL", s, flags=re.IGNORECASE)
                s = re.sub(r"\s+", " ", s).strip()
                if s:
                    normalized.append(s)
            return normalized
    return []


# ---------------------------------------------------------------------------
# TARGET RATES (grape-specific)
# ---------------------------------------------------------------------------

# Common crop names that follow the grape section in directions tables
_NEXT_CROP_NAMES = (
    r"Kiwifruit|Apples?|Pears?|Onions?|Beans?|Cherries|Stone\s*fruit|"
    r"Citrus|Potatoes?|Tomatoes?|Strawberries|Cucurbit|Peas?|Turf|"
    r"Ornamentals?|Brassica|Avocado|GENERAL|STORAGE|PERSONAL|Withholding"
)

_GRAPE_SECTION_PATTERN = re.compile(
    r"(?:Grape|Grapevine|Vine)s?\s+(.{20,2000}?)(?=\n(?:" + _NEXT_CROP_NAMES + r")|\Z)",
    re.IGNORECASE | re.DOTALL,
)

_DISEASE_NAMES = [
    "Botrytis", "Powdery mildew", "Downy mildew", "Dead arm", "Black spot",
    "Eutypa", "Phomopsis", "Rust", "leafroller", "mealybug", "mite",
    "thrip", "caterpillar", "codling moth", "grass", "weed", "broadleaf",
]

_RATE_PATTERN = re.compile(
    r"(\d+(?:\.\d+)?)\s*(?:to|-|–)\s*(\d+(?:\.\d+)?)\s*(g|mL|kg|L)/\s*(100\s*(?:L|litres?)|ha)\b"
    r"|(\d+(?:\.\d+)?)\s*(g|mL|kg|L)/\s*(100\s*(?:L|litres?)|ha)\b",
    re.IGNORECASE,
)


def extract_target_rates(text: str) -> list[TargetRate]:
    """Extract application rates for grape targets from the label."""
    results: list[TargetRate] = []

    m = _GRAPE_SECTION_PATTERN.search(text)
    if not m:
        return results

    grape_text = m.group(0)
    lines = grape_text.split("\n")

    current_target = None
    current_block = []

    for line in lines:
        line_stripped = line.strip()
        if not line_stripped:
            continue

        # Detect a target disease name
        found_target = None
        for disease in _DISEASE_NAMES:
            if disease.lower() in line_stripped.lower():
                found_target = disease
                break

        # If we found a new target, flush the previous block
        if found_target and found_target != current_target:
            if current_target and current_block:
                rate = _parse_rate_block(current_target, current_block)
                if rate:
                    results.append(rate)
            current_target = found_target
            current_block = [line_stripped]
        elif current_target:
            current_block.append(line_stripped)

    # Flush last block
    if current_target and current_block:
        rate = _parse_rate_block(current_target, current_block)
        if rate:
            results.append(rate)

    return results


def _parse_rate_block(target: str, lines: list[str]) -> TargetRate | None:
    """Parse a block of text for a single target into a TargetRate."""
    text = " ".join(lines)

    # Find rate values
    rate_m = _RATE_PATTERN.search(text)
    if not rate_m:
        return None

    if rate_m.group(1) and rate_m.group(2):
        # Range: "150 to 200 g/100L"
        rate_min = float(rate_m.group(1))
        rate_max = float(rate_m.group(2))
        unit_val = rate_m.group(3)
        unit_base = rate_m.group(4)
    else:
        rate_min = float(rate_m.group(5))
        rate_max = None
        unit_val = rate_m.group(6)
        unit_base = rate_m.group(7)

    unit_base_norm = re.sub(r"litre", "L", unit_base, flags=re.IGNORECASE)
    rate_unit = f"{unit_val}/{unit_base_norm}"
    rate_value = rate_m.group(0)

    # Look for max applications
    max_apps = None
    max_m = re.search(r"(?:maximum|no more than)\s+(\d+)\s+(?:application|spray|time)", text, re.IGNORECASE)
    if max_m:
        max_apps = int(max_m.group(1))

    # Look for growth stage window
    gs_window = None
    gs_m = re.search(r"(EL\d+\s*(?:to|-|–)\s*EL\d+|budburst|veraison|dormant|flowering|pre-bunch|harvest)", text, re.IGNORECASE)
    if gs_m:
        gs_window = gs_m.group(0)

    # Notes — everything after the rate that isn't another field
    notes = None
    remarks_m = re.search(r"(?:Apply|Use|Begin|DO NOT)\s+.{10,200}", text)
    if remarks_m:
        notes = remarks_m.group(0)[:200]

    return TargetRate(
        target=target,
        rate_value=rate_value,
        rate_min=rate_min,
        rate_max=rate_max if rate_max else rate_min,
        rate_unit=rate_unit,
        growth_stage_window=gs_window,
        max_applications=max_apps,
        notes=notes,
        raw=text[:300],
    )


# ---------------------------------------------------------------------------
# RAINFASTNESS
# ---------------------------------------------------------------------------

_RAINFAST_PATTERNS = [
    (re.compile(r"rainfast\s+(?:within|in)\s+(\d+(?:\.\d+)?)\s*hours?", re.IGNORECASE), "hours"),
    (re.compile(r"rain\s+(?:is\s+)?expected\s+within\s+(\d+(?:\.\d+)?)\s*hours?", re.IGNORECASE), "hours"),
    (re.compile(r"allow\s+(\d+(?:\.\d+)?)\s*hours?\s+.*?(?:dry|rain)", re.IGNORECASE), "hours"),
    (re.compile(r"(\d+(?:\.\d+)?)\s*hours?\s+.*?rain\s*fast", re.IGNORECASE), "hours"),
    (re.compile(r"rainfast\s+(?:once|when)\s+dr(?:y|ied)", re.IGNORECASE), "when_dry"),
]


def extract_rainfastness(text: str) -> tuple[float | None, str | None]:
    """Extract rainfastness time. Returns (hours, raw_text). 0.0 = rainfast when dry."""
    for pattern, ptype in _RAINFAST_PATTERNS:
        m = pattern.search(text)
        if m:
            raw = m.group(0)
            if ptype == "when_dry":
                return 0.0, raw
            return float(m.group(1)), raw
    return None, None


# ---------------------------------------------------------------------------
# MAX APPLICATIONS PER SEASON
# ---------------------------------------------------------------------------

_MAX_APPS_PATTERNS = [
    re.compile(r"(?:maximum|(?:no )?more than)\s+(\d+)\s+(?:application|spray|time)s?\s+(?:per|in\s+a?)\s*(?:season|crop|year)", re.IGNORECASE),
    re.compile(r"(?:DO NOT (?:apply|use)|not apply|not use)\s+more than\s+(\d+)\s+(?:application|spray|time)s?\s+(?:per\s+)?(?:season|crop|year)?", re.IGNORECASE),
    re.compile(r"(?:a\s+)?maximum\s+(?:of\s+)?(\d+)\s+(?:application|spray)s?\s+(?:per|in\s+a?)\s*(?:season|crop|year)", re.IGNORECASE),
    re.compile(r"frequency of (\d+) applications? per (?:season|year)", re.IGNORECASE),
]


def extract_max_applications(text: str) -> tuple[int | None, str | None]:
    """Extract maximum applications per season."""
    all_patterns = list(_MAX_APPS_PATTERNS) + _load_learned_patterns("max_applications")
    for pattern in all_patterns:
        m = pattern.search(text)
        if m:
            try:
                return int(m.group(1)), m.group(0)
            except (IndexError, ValueError):
                continue
    return None, None


# ---------------------------------------------------------------------------
# GROWTH STAGE RESTRICTIONS
# ---------------------------------------------------------------------------

_GS_PATTERNS = [
    re.compile(r"(?:from|after)\s+(EL\d+|budburst|dormant|flowering|veraison).{0,30}(?:to|until|before)\s+(EL\d+|pre-harvest|veraison|harvest|bunch closure)", re.IGNORECASE),
    re.compile(r"(?:DO NOT apply|Do not use)\s+after\s+(EL\d+|veraison|harvest|petal fall|fruit set)", re.IGNORECASE),
    re.compile(r"(?:for use on|apply\s+(?:only\s+)?(?:to|on))\s+(dormant|non[- ]?producing)\s+vines?", re.IGNORECASE),
]


def extract_growth_stage_restrictions(text: str) -> tuple[str | None, str | None, str | None]:
    """Extract growth stage window. Returns (earliest, latest, raw_text)."""
    for pattern in _GS_PATTERNS:
        m = pattern.search(text)
        if m:
            groups = m.groups()
            if len(groups) == 2:
                return groups[0], groups[1], m.group(0)
            elif len(groups) == 1:
                return None, groups[0], m.group(0)
    return None, None, None


# ---------------------------------------------------------------------------
# TANK MIX
# ---------------------------------------------------------------------------


def extract_tank_mix_info(text: str) -> tuple[list[str], list[str], str | None]:
    """Extract tank-mix incompatibilities and requirements. Returns (incompatible, required, raw)."""
    incompatible: list[str] = []
    required: list[str] = []
    raw_parts: list[str] = []

    # Incompatible
    m = re.search(r"(?:DO NOT|do not|Do not)\s+(?:tank[- ]?mix|mix)\s+with\s+(.{10,150}?)(?:\.|$)", text, re.IGNORECASE)
    if m:
        incompatible = [s.strip() for s in re.split(r",|and\b|or\b", m.group(1)) if s.strip()]
        raw_parts.append(m.group(0))

    m2 = re.search(r"(?:incompatible|not compatible)\s+with\s+(.{10,150}?)(?:\.|$)", text, re.IGNORECASE)
    if m2:
        incompatible.extend([s.strip() for s in re.split(r",|and\b|or\b", m2.group(1)) if s.strip()])
        raw_parts.append(m2.group(0))

    # Required
    m3 = re.search(r"(?:must be|should be)\s+(?:applied\s+)?(?:in\s+)?(?:a\s+)?tank[- ]?mix\s+with\s+(.{10,150}?)(?:\.|$)", text, re.IGNORECASE)
    if m3:
        required = [s.strip() for s in re.split(r",|and\b", m3.group(1)) if s.strip()]
        raw_parts.append(m3.group(0))

    raw = " | ".join(raw_parts) if raw_parts else None
    return incompatible, required, raw


# ---------------------------------------------------------------------------
# BUFFER ZONE
# ---------------------------------------------------------------------------


def extract_buffer_zone(text: str) -> tuple[int | None, str | None]:
    """Extract buffer zone distance in metres."""
    m = re.search(r"(\d+)\s*m(?:etre)?s?\s+(?:buffer|from\s+.*?water|of\s+.*?water)", text, re.IGNORECASE)
    if m:
        return int(m.group(1)), m.group(0)
    m2 = re.search(r"buffer\s+zone\s+(?:of\s+)?(\d+)\s*m", text, re.IGNORECASE)
    if m2:
        return int(m2.group(1)), m2.group(0)
    return None, None


# ---------------------------------------------------------------------------
# PPE
# ---------------------------------------------------------------------------

_PPE_ITEMS = [
    r"chemical[- ]?resistant gloves",
    r"(?:rubber|nitrile|latex) gloves",
    r"(?:suitable|protective) gloves",
    r"face shield",
    r"goggles",
    r"(?:cotton )?overalls",
    r"respirator",
    r"dust mask",
    r"footwear",
    r"washable hat",
    r"eye protection",
]


def extract_ppe(text: str) -> list[str]:
    """Extract PPE requirements from the label."""
    found: list[str] = []
    for item in _PPE_ITEMS:
        if re.search(item, text, re.IGNORECASE):
            # Return the canonical form
            found.append(item.replace(r"(?:", "").replace(r")", "").replace(r"|", "/").replace("\\", "").replace(r"?", "").replace(r" ", " "))
    return found


# ---------------------------------------------------------------------------
# ENVIRONMENTAL CAUTIONS
# ---------------------------------------------------------------------------

_ENV_PATTERNS = [
    r"toxic to aquatic (?:organisms|life)",
    r"harmful to (?:bees|birds)",
    r"toxic to (?:fish|bees|birds|terrestrial vertebrates)",
    r"ecotoxic",
    r"hazardous to (?:the )?(?:aquatic )?environment",
]


def extract_environmental_cautions(text: str) -> list[str]:
    """Extract environmental warnings from the label."""
    found: list[str] = []
    for pattern in _ENV_PATTERNS:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            found.append(m.group(0))
    return found


# ---------------------------------------------------------------------------
# SHELF LIFE
# ---------------------------------------------------------------------------


def extract_shelf_life(text: str) -> tuple[float | None, str | None]:
    """Extract product shelf life in years."""
    m = re.search(r"(?:shelf[- ]?life|no significant degradation)\s+.*?(\d+)\s*years?", text, re.IGNORECASE)
    if m:
        return float(m.group(1)), m.group(0)
    m2 = re.search(r"(\d+)\s*years?\s+(?:shelf[- ]?life|from.*?manufacture)", text, re.IGNORECASE)
    if m2:
        return float(m2.group(1)), m2.group(0)
    return None, None


# ---------------------------------------------------------------------------
# WHP / REI (raw text for cross-validation only)
# ---------------------------------------------------------------------------


def extract_whp_raw(text: str) -> str | None:
    """Extract grape withholding period text for cross-validation."""
    m = re.search(r"(?:Grape|Vine).{0,20}(?:DO NOT apply|Nil|withhold).{0,120}", text, re.IGNORECASE)
    return m.group(0) if m else None


def extract_rei_raw(text: str) -> str | None:
    """Extract re-entry interval text for cross-validation."""
    m = re.search(r"(?:re-?entry|enter\s+treated).{0,120}", text, re.IGNORECASE)
    return m.group(0) if m else None


# ---------------------------------------------------------------------------
# REGULATORY DATA (Phase 7)
# ---------------------------------------------------------------------------


def extract_hsr_number(text: str) -> tuple[str | None, str | None]:
    """Extract HSNO approval number (HSR...).

    Patterns:
    - "Approved pursuant to the HSNO Act 1996, Code HSR007853"
    - "Approval Code HSR000592."
    - "No: HSR100838"
    """
    m = re.search(r"(HSR\s*\d{5,6})", text)
    if m:
        # Normalize: remove internal space
        hsr = m.group(1).replace(" ", "")
        return hsr, m.group(0)
    return None, None


def extract_hsno_classifications(text: str) -> tuple[list[str], str | None]:
    """Extract GHS/HSNO hazard classification descriptions from the label.

    Labels use three formats:
    1. Section with "GHS Classification" header followed by classification lines
    2. Compact single-line: "Acute Tox 4 (Oral), Skin Sens. 1, ..."
    3. HSNO numeric: "Hazard classification 9.1A"
    """
    classifications: list[str] = []
    raw = None

    # Strategy 1: Find "GHS Classification" or "Hazard Classification" section
    m = re.search(
        r"(?:GHS|HSNO|Hazard)\s*[Cc]lassification[s]?\s*(?::|\n)(.{10,600}?)(?=\n(?:DANGER|WARNING|CAUTION|PRECAUTION|FIRST AID|HARMFUL|ECOTOXIC)|\Z)",
        text,
        re.DOTALL,
    )
    if m:
        raw = m.group(0)
        section_text = m.group(1)
        # Parse individual classifications from the section
        lines = section_text.strip().split("\n")
        for line in lines:
            line = line.strip()
            if not line:
                continue
            # Skip lines that are just signal words or section headers
            if re.match(r"^(DANGER|WARNING|CAUTION|HARMFUL|ECOTOXIC|PRECAUTION)$", line, re.IGNORECASE):
                break
            # Each non-empty line in a GHS section is typically one classification
            if len(line) > 5:
                classifications.append(line)
        if classifications:
            return classifications, raw

    # Strategy 2: Compact format on a single line after "HAZARD CLASSIFICATION"
    m2 = re.search(
        r"HAZARD CLASSIFICATION[S]?\s*\n(.{20,300}?)(?:\n[A-Z])",
        text,
    )
    if m2:
        raw = m2.group(0)
        # Split on commas for compact format
        parts = m2.group(1).split(",")
        classifications = [p.strip() for p in parts if p.strip() and len(p.strip()) > 3]
        if classifications:
            return classifications, raw

    # Strategy 3: Direct HSNO numeric codes (older/simpler labels)
    m3 = re.search(r"[Hh]azard\s+classification\s+(\d\.\d[A-E]?(?:\s*,\s*\d\.\d[A-E]?)*)", text)
    if m3:
        raw = m3.group(0)
        codes = re.findall(r"\d\.\d[A-E]?", m3.group(1))
        return codes, raw

    return [], None


def extract_signal_word(text: str) -> tuple[str | None, str | None]:
    """Extract the hazard signal word from the label.

    Searches the first ~1500 chars where signal words are prominently displayed.
    """
    # Search in the first portion of text (signal word is near the top)
    header = text[:1500]
    for word in ["DANGER", "WARNING", "CAUTION"]:
        m = re.search(rf"\b({word})\b", header)
        if m:
            return word, m.group(0)
    return None, None


def extract_acvm_number_from_label(text: str) -> tuple[str | None, str | None]:
    """Extract ACVM registration P-number from the label text.

    Pattern: P followed by 5+ digits (e.g. P007595, P008921).
    """
    m = re.search(r"\b(P\d{5,})\b", text)
    if m:
        return m.group(1), m.group(0)
    return None, None
