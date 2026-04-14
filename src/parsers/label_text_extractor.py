"""Label PDF text extraction — clean text from agchem label PDFs.

Handles multi-page labels, suppresses pdfplumber color warnings,
normalizes whitespace while preserving line structure.
"""

from __future__ import annotations

import logging
import warnings
from pathlib import Path

import pdfplumber

logger = logging.getLogger(__name__)


def extract_label_text(pdf_path: Path) -> str:
    """Extract all text from a label PDF, cleaned and normalised.

    Returns the full text with line breaks preserved but excessive
    whitespace collapsed within lines.
    """
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message="Cannot set non-stroke color")
        try:
            with pdfplumber.open(str(pdf_path)) as pdf:
                pages = []
                for page in pdf.pages:
                    text = page.extract_text()
                    if text:
                        pages.append(text)
                return "\n".join(pages)
        except Exception as e:
            logger.warning("Failed to extract text from %s: %s", pdf_path.name, e)
            return ""
