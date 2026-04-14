"""Deterministic slug generation for product and ingredient names.

Output must match the existing database constraint:
    ^[a-z0-9]+(?:-[a-z0-9]+)*$
"""

from __future__ import annotations

import re
import unicodedata


def make_slug(name: str) -> str:
    """Generate a deterministic, DB-compatible slug from a product/ingredient name.

    Examples:
        >>> make_slug("Merpan 80 WDG")
        'merpan-80-wdg'
        >>> make_slug("Botry-Zen WP")
        'botry-zen-wp'
        >>> make_slug("Gro-Chem Lime Sulphur")
        'gro-chem-lime-sulphur'
        >>> make_slug("Hort-\\ncare Copper Hydrox-\\nide")
        'hortcare-copper-hydroxide'
    """
    # Rejoin hyphenated line breaks: "Hort-\ncare" → "Hortcare"
    # Must happen before lowercasing to handle mid-word breaks
    text = re.sub(r"(\w)-\n(\w)", r"\1\2", name)

    # Collapse remaining newlines to spaces
    text = text.replace("\n", " ")

    # Normalize unicode (e.g. accented chars)
    text = unicodedata.normalize("NFKD", text)

    # Strip trademark/registered symbols
    text = re.sub(r"[®™©]", "", text)

    # Strip asterisks (restriction markers on active ingredients)
    text = text.replace("*", "")

    # Lowercase
    text = text.lower()

    # Replace non-alphanumeric characters with hyphens
    text = re.sub(r"[^a-z0-9]+", "-", text)

    # Collapse multiple hyphens
    text = re.sub(r"-+", "-", text)

    # Strip leading/trailing hyphens
    text = text.strip("-")

    return text
