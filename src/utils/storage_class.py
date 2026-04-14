"""Derive HSNO storage class from GHS hazard classifications.

Simplified mapping from GHS classification text to a primary storage group.
The full NZS 8409 storage compatibility matrix has 12+ groups with complex
compatibility rules — this captures the PRIMARY hazard that determines
which storage area the product belongs to.

Storage groups (simplified):
- flammable: Products containing flammable liquids/solids (Class 3, 4)
- toxic: Acutely toxic substances (Class 6.1)
- corrosive: Corrosive substances (Class 8)
- oxidising: Oxidising substances (Class 5)
- ecotoxic: Environmentally hazardous (Class 9.1)
- general: No specific storage requirement beyond standard agrichemical storage
"""

from __future__ import annotations

import re

# Priority order: higher-risk classes take precedence
_CLASSIFICATION_GROUPS = [
    # (pattern, storage_class, priority)
    (re.compile(r"flammable", re.IGNORECASE), "flammable", 10),
    (re.compile(r"corrosive|causes severe.*burn", re.IGNORECASE), "corrosive", 9),
    (re.compile(r"oxidis", re.IGNORECASE), "oxidising", 8),
    (re.compile(r"acute\s+tox.*category\s*[12]|very\s+toxic", re.IGNORECASE), "toxic_high", 7),
    (re.compile(r"acute\s+tox|toxic\s+if", re.IGNORECASE), "toxic", 6),
    (re.compile(r"aquatic|ecotoxic|toxic to aquatic", re.IGNORECASE), "ecotoxic", 5),
]


def derive_storage_class(classifications: list[str]) -> str | None:
    """Derive the primary storage class from GHS classification descriptions.

    Args:
        classifications: List of GHS classification text descriptions from the label.

    Returns:
        Storage class string or None if no relevant classification found.
    """
    if not classifications:
        return None

    combined = " ".join(classifications)
    best_class = None
    best_priority = -1

    for pattern, storage_class, priority in _CLASSIFICATION_GROUPS:
        if pattern.search(combined) and priority > best_priority:
            best_class = storage_class
            best_priority = priority

    return best_class or "general"
