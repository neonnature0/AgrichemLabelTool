"""ACVM detail page scraper — searches by P-number and parses label documents.

Ported from the working standalone acvm_scraper.py. Key differences:
- Returns structured dataclasses instead of printing
- Caches detail page HTML (TTL 30 days)
- No argparse — pure functions callable from the pipeline

The ACVM register is fully public — no authentication required.
Session cookies are established via an initial GET request.
"""

from __future__ import annotations

import json
import logging
import re
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path

import requests
from bs4 import BeautifulSoup

from src.config import ACVM_CACHE_TTL_DAYS

logger = logging.getLogger(__name__)

BASE_URL = "https://eatsafe.nzfsa.govt.nz"
REGISTER_URL = f"{BASE_URL}/web/public/acvm-register"
SEARCH_URL = (
    f"{REGISTER_URL}"
    "?p_p_id=searchAcvm_WAR_aaol"
    "&p_p_lifecycle=1"
    "&p_p_state=normal"
    "&p_p_mode=view"
    "&p_p_col_id=column-2"
    "&p_p_col_count=1"
    "&_searchAcvm_WAR_aaol_action=search"
)
USER_AGENT = "Cordyn-Catalogue-Pipeline/1.0"
RATE_LIMIT_SECS = 0.8


@dataclass
class AcvmLabelInfo:
    """A label document found on the ACVM detail page."""

    document_id: str
    filename: str  # original ACVM filename
    url: str  # full download URL
    label_date: str | None = None  # extracted from filename, e.g. "Apr 2025"


@dataclass
class AcvmDetailResult:
    """Result of scraping an ACVM product detail page."""

    registration_no: str
    trade_name: str
    labels: list[AcvmLabelInfo] = field(default_factory=list)


def create_session() -> requests.Session:
    """Create a requests session with cookies from the ACVM register."""
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    resp = session.get(REGISTER_URL, timeout=30)
    resp.raise_for_status()
    return session


def _base_form() -> dict:
    return {
        "regNo": "",
        "tradeName": "",
        "productType": "",
        "registrantName": "",
        "active": "",
        "startDateStr": "",
        "endDateStr": "",
        "submit": "Search",
    }


def _is_detail_page(html: str) -> bool:
    return "Registration details" in html and "id='summary'" in html


def _is_label_row(label_text: str) -> bool:
    """True only for the Labels row — not Decision Summary, SDS, Haznote."""
    lower = label_text.lower()
    return "label" in lower and "decision" not in lower and "delegate" not in lower


def _extract_date(filename: str) -> str | None:
    """Pull the date from the ACVM filename.

    'P005506-14 - Approved Label - Apr 2025.pdf' -> 'Apr 2025'
    """
    stem = re.sub(r"\.pdf$", "", filename, flags=re.IGNORECASE)
    parts = re.split(r"\s+-\s+", stem)
    for candidate in reversed(parts):
        candidate = candidate.strip()
        if re.search(
            r"(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec|\b\d{4}\b)",
            candidate,
            re.IGNORECASE,
        ):
            return candidate
    return None


def _parse_detail_page(html: str) -> AcvmDetailResult | None:
    """Parse an ACVM detail page HTML and extract registration info + label documents."""
    if not _is_detail_page(html):
        return None

    soup = BeautifulSoup(html, "html.parser")
    summary = soup.find(id="summary")
    if not summary:
        return None

    def get_item(label: str) -> str:
        for el in summary.find_all(class_="itemLabel"):
            if label.lower() in el.get_text().lower():
                sib = el.find_next_sibling(class_="item")
                return sib.get_text(strip=True) if sib else ""
        return ""

    labels: list[AcvmLabelInfo] = []
    for row in summary.find_all(class_="itemRow"):
        label_el = row.find(class_="itemLabel")
        if not label_el:
            continue
        label_text = label_el.get_text(strip=True)

        if not _is_label_row(label_text):
            continue

        for anchor in row.find_all("a", href=True):
            href = anchor["href"]
            filename = anchor.get_text(strip=True)
            id_match = re.search(r"documentId=(\d+)", href)
            if not id_match:
                continue
            url = href if href.startswith("http") else f"{BASE_URL}{href}"
            labels.append(AcvmLabelInfo(
                document_id=id_match.group(1),
                filename=filename,
                url=url,
                label_date=_extract_date(filename),
            ))

    return AcvmDetailResult(
        registration_no=get_item("Registration number"),
        trade_name=get_item("Trade name"),
        labels=labels,
    )


def search_by_p_number(
    session: requests.Session,
    p_number: str,
    *,
    cache_dir: Path | None = None,
    cache_ttl_days: int | None = None,
) -> AcvmDetailResult | None:
    """Search the ACVM register by P-number and return the detail page result."""
    ttl = cache_ttl_days if cache_ttl_days is not None else ACVM_CACHE_TTL_DAYS

    # Check cache
    if cache_dir:
        cache_file = cache_dir / f"{p_number}.html"
        if cache_file.exists():
            age_days = (time.time() - cache_file.stat().st_mtime) / 86400
            if age_days < ttl:
                html = cache_file.read_text(encoding="utf-8")
                return _parse_detail_page(html)

    # Search
    form = _base_form()
    form["regNo"] = p_number.strip()
    try:
        resp = session.post(SEARCH_URL, data=form, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        logger.warning("Failed to search ACVM for %s: %s", p_number, e)
        return None

    html = resp.text

    # Cache
    if cache_dir:
        cache_dir.mkdir(parents=True, exist_ok=True)
        cache_file = cache_dir / f"{p_number}.html"
        cache_file.write_text(html, encoding="utf-8")

    return _parse_detail_page(html)


def scrape_details(
    session: requests.Session,
    products: dict[str, str],
    *,
    cache_dir: Path | None = None,
    cache_ttl_days: int | None = None,
) -> dict[str, AcvmDetailResult]:
    """Scrape ACVM detail pages for multiple products.

    Args:
        session: Requests session with cookies.
        products: {product_slug: p_number} mapping.
        cache_dir: Directory for caching HTML responses.
        cache_ttl_days: Cache TTL in days.

    Returns:
        {product_slug: AcvmDetailResult} for successfully scraped products.
    """
    results: dict[str, AcvmDetailResult] = {}
    total = len(products)

    for i, (slug, p_number) in enumerate(products.items()):
        logger.info("Scraping %d/%d: %s (%s)", i + 1, total, slug, p_number)

        # Check if cached (no rate limit needed for cache hits)
        if cache_dir:
            cache_file = cache_dir / f"{p_number}.html"
            ttl = cache_ttl_days if cache_ttl_days is not None else ACVM_CACHE_TTL_DAYS
            if cache_file.exists() and (time.time() - cache_file.stat().st_mtime) / 86400 < ttl:
                html = cache_file.read_text(encoding="utf-8")
                detail = _parse_detail_page(html)
                if detail:
                    results[slug] = detail
                continue

        # Rate limit for non-cached requests
        time.sleep(RATE_LIMIT_SECS)

        detail = search_by_p_number(
            session, p_number, cache_dir=cache_dir, cache_ttl_days=cache_ttl_days
        )
        if detail:
            results[slug] = detail
        else:
            logger.warning("No detail found for %s (%s)", slug, p_number)

    logger.info(
        "Scraped %d/%d products (%d with labels)",
        len(results), total,
        sum(1 for r in results.values() if r.labels),
    )

    # Write staging summary
    if cache_dir:
        summary_path = cache_dir / "detail_summary.json"
        summary = {
            slug: {
                "registration_no": r.registration_no,
                "trade_name": r.trade_name,
                "label_count": len(r.labels),
                "labels": [asdict(l) for l in r.labels],
            }
            for slug, r in results.items()
        }
        summary_path.write_text(
            json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8"
        )

    return results
