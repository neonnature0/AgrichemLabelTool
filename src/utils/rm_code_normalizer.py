"""Normalize RM (resistance management) rule codes for consistent matching.

RM codes come from two sources that produce inconsistent formatting:
- PHI table: "M04/phthalimide", "29/2,6-dinitro-aniline"
- RM table: "M04/ phthalimide", "29/2,6-dinitro- aniline"

The differences are caused by pdfplumber's newline-to-space conversion
creating extra spaces, and the hyphen-rejoin regex removing intentional hyphens.

This normalizer produces a canonical form for matching.
"""

from __future__ import annotations

import re


def normalize_rm_code(code: str) -> str:
    """Normalize an RM code to a canonical form for matching.

    Rules:
    - Remove spaces immediately after "/"
    - Collapse multiple spaces to one
    - Strip leading/trailing whitespace
    - Rejoin "- " to "-" (PDF artifact from newline rejoin)

    Examples:
        >>> normalize_rm_code("M04/ phthalimide")
        'M04/phthalimide'
        >>> normalize_rm_code("29/2,6-dinitro- aniline")
        '29/2,6-dinitro-aniline'
        >>> normalize_rm_code("3/DMI Wound Dressing (paint-on)")
        '3/DMI Wound Dressing (paint-on)'
    """
    # Remove space after "/"
    result = re.sub(r"/\s+", "/", code)
    # Rejoin "- " to "-" (PDF artifact)
    result = re.sub(r"-\s+", "-", result)
    # Collapse remaining multiple spaces
    result = re.sub(r"\s+", " ", result).strip()
    return result
