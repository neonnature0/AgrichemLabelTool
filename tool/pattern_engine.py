"""Pattern generation and testing engine for label extraction learning.

Takes annotated text from a user, generates candidate regex patterns,
and tests them against all label texts in memory. Returns match counts
and matched product IDs for user approval.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field


@dataclass
class TestResult:
    pattern: str
    total_matches: int
    new_matches: list[str]  # product IDs with new extractions
    matched_texts: dict[str, str]  # product_id → matched text snippet
    is_valid: bool  # compiled without error


@dataclass
class CandidatePattern:
    pattern: str
    strategy: str  # "literal", "keyword", "user"
    test_result: TestResult | None = None


# Field-specific keywords that should be kept as structure
_FIELD_KEYWORDS: dict[str, list[str]] = {
    "max_applications": ["maximum", "more than", "application", "spray", "times", "per", "season", "crop", "year", "apply", "not"],
    "rainfastness": ["rainfast", "rain", "fast", "dry", "dried", "hours", "hour", "within"],
    "buffer_zone": ["buffer", "zone", "waterway", "water", "metres", "meters"],
    "growth_stage": ["budburst", "veraison", "dormant", "flowering", "harvest", "EL"],
    "tank_mix": ["tank", "mix", "incompatible", "compatible", "not"],
    "shelf_life": ["shelf", "life", "degradation", "years", "year", "manufacture"],
    "container_sizes": ["contents", "net", "available"],
    "active_ingredients": ["contains", "active", "ingredient"],
    "target_rate": ["rate", "apply", "litres", "water", "dilute"],
    "hsno_classifications": ["classification", "hazard", "GHS", "HSNO", "category"],
    "signal_word": ["DANGER", "WARNING", "CAUTION"],
}


def generate_candidates(
    selected_text: str,
    field_name: str,
) -> list[CandidatePattern]:
    """Generate candidate regex patterns from annotated text.

    Returns 2-3 candidates from specific to general.
    """
    candidates: list[CandidatePattern] = []
    text = selected_text.strip()

    # Strategy 1: Literal with number capture
    literal = _make_literal_pattern(text)
    if literal:
        candidates.append(CandidatePattern(pattern=literal, strategy="literal"))

    # Strategy 2: Keyword-based
    keywords = _FIELD_KEYWORDS.get(field_name, [])
    keyword_pat = _make_keyword_pattern(text, keywords)
    if keyword_pat and keyword_pat != literal:
        candidates.append(CandidatePattern(pattern=keyword_pat, strategy="keyword"))

    return candidates


def _make_literal_pattern(text: str) -> str | None:
    """Generate a pattern close to the literal text with numbers/names replaced."""
    # Escape regex special chars in the base text
    escaped = re.escape(text)

    # Replace escaped digit sequences with capture groups
    result = re.sub(r"(?:\\d)+", r"(\\d+)", escaped)

    # Replace product-name-like sequences (capitalized words with numbers) with wildcards
    result = re.sub(
        r"(?:[A-Z][a-z]+(?:\\ )?(?:\\d+)?(?:[A-Z]{1,3})?){1,3}",
        r".{1,60}?",
        result,
    )

    # Collapse excessive whitespace escapes
    result = re.sub(r"(?:\\ )+", r"\\s+", result)

    # Clean up
    result = result.strip()
    if not result or len(result) < 10:
        return None

    return result


def _make_keyword_pattern(text: str, keywords: list[str]) -> str | None:
    """Generate a pattern from field-specific keywords found in the text."""
    text_lower = text.lower()
    found_keywords = [kw for kw in keywords if kw.lower() in text_lower]

    if len(found_keywords) < 2:
        return None

    # Build pattern: keyword1 .{1,80}? keyword2 .{1,80}? keyword3
    parts = []
    for kw in found_keywords[:4]:  # max 4 keywords
        # If keyword looks like it should capture a number after it
        if kw.lower() in ("maximum", "more than", "within"):
            parts.append(re.escape(kw) + r"\s+(\d+)")
        else:
            parts.append(re.escape(kw))

    return r".{0,40}?".join(parts)


def test_pattern(
    pattern_str: str,
    field_name: str,
    label_texts: dict[str, str],
    existing_matches: set[str] | None = None,
) -> TestResult:
    """Test a regex pattern against all label texts.

    Args:
        pattern_str: The regex pattern to test.
        field_name: Which extraction field this is for.
        label_texts: {product_id: full_text} for all labels.
        existing_matches: product IDs already matched by built-in patterns.

    Returns:
        TestResult with match counts and matched product IDs.
    """
    existing = existing_matches or set()

    try:
        compiled = re.compile(pattern_str, re.IGNORECASE)
    except re.error:
        return TestResult(
            pattern=pattern_str,
            total_matches=0,
            new_matches=[],
            matched_texts={},
            is_valid=False,
        )

    total_matches = 0
    new_matches: list[str] = []
    matched_texts: dict[str, str] = {}

    for product_id, text in label_texts.items():
        m = compiled.search(text)
        if m:
            total_matches += 1
            snippet = m.group(0)[:120]
            matched_texts[product_id] = snippet
            if product_id not in existing:
                new_matches.append(product_id)

    return TestResult(
        pattern=pattern_str,
        total_matches=total_matches,
        new_matches=new_matches,
        matched_texts=matched_texts,
        is_valid=True,
    )
