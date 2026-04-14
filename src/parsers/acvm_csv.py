"""ACVM Register CSV loader — downloads and parses the bulk CSV from MPI.

The ACVM register provides a freely available CSV download of all registered
agricultural compounds. Each registration has multiple rows (one per
condition × ingredient combination). This module groups them into products.

The CSV is cached locally to avoid repeated downloads. Default TTL: 30 days.
"""

from __future__ import annotations

import csv
import io
import logging
import time
from dataclasses import dataclass, field
from pathlib import Path

import httpx

from src.config import ACVM_CACHE_DIR, ACVM_CACHE_TTL_DAYS, ACVM_CSV_URL

logger = logging.getLogger(__name__)

CACHE_FILENAME = "acvm_register.csv"


@dataclass
class AcvmIngredient:
    """One active ingredient entry from the ACVM register."""

    name: str  # UPPERCASE in CSV, e.g. "PYRACLOSTROBIN"
    content: float | None
    unit: str  # g/kg, g/L, etc.


@dataclass
class AcvmProduct:
    """A product from the ACVM register, aggregated from multiple CSV rows."""

    registration_no: str  # P-number, e.g. "P007595"
    trade_name: str
    product_type: str  # "Fungicide", "Herbicide", etc.
    registrant: str
    agent: str | None
    registration_date: str  # DD/MM/YYYY as in CSV
    ingredients: list[AcvmIngredient] = field(default_factory=list)


def load_acvm_csv(
    *,
    cache_dir: Path | None = None,
    max_age_days: int | None = None,
    force_download: bool = False,
) -> dict[str, AcvmProduct]:
    """Download (or load cached) ACVM CSV and parse into products.

    Returns dict keyed by trade name (as-is from ACVM, preserving case).
    """
    cache_dir = cache_dir or ACVM_CACHE_DIR
    max_age_days = max_age_days if max_age_days is not None else ACVM_CACHE_TTL_DAYS
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_path = cache_dir / CACHE_FILENAME

    # Check cache
    if not force_download and cache_path.exists():
        age_seconds = time.time() - cache_path.stat().st_mtime
        age_days = age_seconds / 86400
        if age_days < max_age_days:
            logger.info("Using cached ACVM CSV (%.1f days old)", age_days)
            return _parse_csv(cache_path.read_text(encoding="utf-8"))

    # Download
    logger.info("Downloading ACVM register CSV...")
    try:
        response = httpx.get(ACVM_CSV_URL, follow_redirects=True, timeout=120)
        response.raise_for_status()
    except httpx.HTTPError as e:
        logger.warning("Failed to download ACVM CSV: %s", e)
        # Fall back to cache if available
        if cache_path.exists():
            logger.info("Falling back to stale cache")
            return _parse_csv(cache_path.read_text(encoding="utf-8"))
        return {}

    csv_text = response.text
    cache_path.write_text(csv_text, encoding="utf-8")
    logger.info("ACVM CSV cached at %s (%d bytes)", cache_path, len(csv_text))

    return _parse_csv(csv_text)


def _parse_csv(csv_text: str) -> dict[str, AcvmProduct]:
    """Parse the ACVM CSV text into a dict of products keyed by trade name."""
    reader = csv.DictReader(io.StringIO(csv_text))
    products: dict[str, AcvmProduct] = {}

    for row in reader:
        trade_name = row.get("Trade Name", "").strip()
        if not trade_name:
            continue

        if trade_name not in products:
            products[trade_name] = AcvmProduct(
                registration_no=row.get("Registration No", "").strip(),
                trade_name=trade_name,
                product_type=row.get("Product Type", "").strip(),
                registrant=row.get("Registrant Name", "").strip(),
                agent=row.get("NZ Agent Name", "").strip() or None,
                registration_date=row.get("Date of registration", "").strip(),
            )

        # Add ingredient if not already present
        ingredient_name = row.get("Ingredient", "").strip()
        if ingredient_name:
            content_str = row.get("Content", "").strip()
            try:
                content = float(content_str) if content_str else None
            except ValueError:
                content = None

            ingredient = AcvmIngredient(
                name=ingredient_name,
                content=content,
                unit=row.get("Unit", "").strip(),
            )
            # Deduplicate by name (same ingredient listed per condition)
            if not any(
                i.name == ingredient.name and i.content == ingredient.content
                for i in products[trade_name].ingredients
            ):
                products[trade_name].ingredients.append(ingredient)

    logger.info("Parsed %d unique ACVM products", len(products))
    return products
