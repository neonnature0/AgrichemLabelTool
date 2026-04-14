"""Match catalogue trade products to ACVM register entries.

Three-pass matching strategy:
1. Exact name match
2. Case-insensitive match with bracket suffix stripping
3. Fuzzy match (rapidfuzz token_sort_ratio >= threshold)

The spray schedule uses informal names like "Hortcare Glyphosate 360 [Grosafe]"
while the ACVM register uses "Hortcare Glyphosate 360". Bracket stripping and
fuzzy matching bridge this gap, achieving ~91% match rate.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from pathlib import Path

from rapidfuzz import fuzz, process

from src.config import ACVM_FUZZY_THRESHOLD
from src.models import TradeProduct
from src.parsers.acvm_csv import AcvmProduct

logger = logging.getLogger(__name__)

_OVERRIDES_PATH = Path(__file__).resolve().parent.parent.parent / "data" / "corrections" / "acvm_overrides.json"


def _load_overrides() -> tuple[dict[str, str], dict[str, str]]:
    """Load block list and forced mappings from acvm_overrides.json."""
    if not _OVERRIDES_PATH.exists():
        return {}, {}
    try:
        data = json.loads(_OVERRIDES_PATH.read_text(encoding="utf-8"))
        return data.get("block", {}), data.get("force", {})
    except Exception:
        return {}, {}


@dataclass
class MatchResult:
    """Result of matching catalogue products against ACVM register."""

    matches: dict[str, AcvmProduct]  # product slug → matched AcvmProduct
    unmatched: list[str] = field(default_factory=list)  # product slugs with no match
    match_method: dict[str, str] = field(default_factory=dict)  # slug → "exact"/"case"/"fuzzy"


def _strip_brackets(name: str) -> str:
    """Remove bracket suffixes like '[Agpro]' from trade names."""
    return re.sub(r"\s*\[.*?\]\s*", " ", name).strip()


def match_products(
    catalogue_products: list[TradeProduct],
    acvm_products: dict[str, AcvmProduct],
    *,
    fuzzy_threshold: int | None = None,
) -> MatchResult:
    """Match catalogue trade products to ACVM register entries.

    Args:
        catalogue_products: Trade products from the catalogue.
        acvm_products: ACVM products keyed by registered trade name.
        fuzzy_threshold: Minimum fuzzy match score (default from config).

    Returns:
        MatchResult with matched products and unmatched list.
    """
    threshold = fuzzy_threshold if fuzzy_threshold is not None else ACVM_FUZZY_THRESHOLD

    # Load overrides (blocked matches + forced P-numbers)
    blocked, forced = _load_overrides()

    # Build lookup structures for ACVM products
    acvm_exact: dict[str, AcvmProduct] = dict(acvm_products)
    acvm_lower: dict[str, AcvmProduct] = {k.lower(): v for k, v in acvm_products.items()}
    acvm_by_reg: dict[str, AcvmProduct] = {p.registration_no: p for p in acvm_products.values()}
    acvm_names = list(acvm_products.keys())

    result = MatchResult(matches={}, unmatched=[], match_method={})
    stats = {"exact": 0, "case": 0, "fuzzy": 0, "forced": 0, "blocked": 0, "unmatched": 0}

    for tp in catalogue_products:
        name = tp.name
        clean_name = _strip_brackets(name)

        # Check forced override first
        if tp.id in forced:
            forced_pnum = forced[tp.id]
            if forced_pnum in acvm_by_reg:
                result.matches[tp.id] = acvm_by_reg[forced_pnum]
                result.match_method[tp.id] = "forced"
                stats["forced"] += 1
                continue

        # Check block list
        if tp.id in blocked:
            result.unmatched.append(tp.id)
            result.match_method[tp.id] = "blocked"
            stats["blocked"] += 1
            continue

        # Pass 1: Exact match
        if name in acvm_exact:
            result.matches[tp.id] = acvm_exact[name]
            result.match_method[tp.id] = "exact"
            stats["exact"] += 1
            continue

        if clean_name != name and clean_name in acvm_exact:
            result.matches[tp.id] = acvm_exact[clean_name]
            result.match_method[tp.id] = "exact"
            stats["exact"] += 1
            continue

        # Pass 2: Case-insensitive
        lower = clean_name.lower()
        if lower in acvm_lower:
            result.matches[tp.id] = acvm_lower[lower]
            result.match_method[tp.id] = "case"
            stats["case"] += 1
            continue

        if name.lower() in acvm_lower:
            result.matches[tp.id] = acvm_lower[name.lower()]
            result.match_method[tp.id] = "case"
            stats["case"] += 1
            continue

        # Pass 3: Fuzzy match on cleaned name
        fuzzy_result = process.extractOne(
            clean_name, acvm_names, scorer=fuzz.token_sort_ratio
        )
        if fuzzy_result and fuzzy_result[1] >= threshold:
            matched_name = fuzzy_result[0]
            result.matches[tp.id] = acvm_products[matched_name]
            result.match_method[tp.id] = "fuzzy"
            stats["fuzzy"] += 1
            continue

        # No match
        result.unmatched.append(tp.id)
        stats["unmatched"] += 1

    total = len(catalogue_products)
    matched = total - stats["unmatched"]
    logger.info(
        "ACVM matching: %d/%d (%.0f%%) — exact: %d, case: %d, fuzzy: %d, unmatched: %d",
        matched, total, 100 * matched / total if total else 0,
        stats["exact"], stats["case"], stats["fuzzy"], stats["unmatched"],
    )

    return result
