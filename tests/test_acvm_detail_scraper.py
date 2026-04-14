"""Tests for the ACVM detail page scraper — HTML parsing and label extraction."""

from pathlib import Path

import pytest

from src.parsers.acvm_detail_scraper import (
    AcvmDetailResult,
    _extract_date,
    _is_label_row,
    _parse_detail_page,
)

FIXTURES_DIR = Path(__file__).parent / "fixtures"


class TestExtractDate:
    @pytest.mark.parametrize("filename,expected", [
        ("P005506-14 - Approved Label - Apr 2025.pdf", "Apr 2025"),
        ("P007595-16 - Approved Label - Feb 2025.pdf", "Feb 2025"),
        ("P008921-10 - Approved Label - Mar 2024.pdf", "Mar 2024"),
        ("P001234 - Label - Jan 2020.pdf", "Jan 2020"),
        ("some-random-file.pdf", None),
    ])
    def test_extract_date(self, filename: str, expected: str | None):
        assert _extract_date(filename) == expected


class TestIsLabelRow:
    @pytest.mark.parametrize("text,expected", [
        ("Labels", True),
        ("Label", True),
        ("Approved Labels", True),
        ("Decision Summary", False),
        ("Delegate's Decision Summary", False),
        ("SDS", False),
        ("Haznote", False),
    ])
    def test_is_label_row(self, text: str, expected: bool):
        assert _is_label_row(text) == expected


class TestParseDetailPage:
    """Test HTML parsing using saved fixture files."""

    @pytest.fixture
    def pristine_html(self) -> str:
        path = FIXTURES_DIR / "P007595.html"
        if not path.exists():
            pytest.skip("Pristine fixture not available")
        return path.read_text(encoding="utf-8")

    @pytest.fixture
    def capetec_html(self) -> str:
        path = FIXTURES_DIR / "P008921.html"
        if not path.exists():
            pytest.skip("Capetec fixture not available")
        return path.read_text(encoding="utf-8")

    def test_pristine_detail(self, pristine_html: str):
        result = _parse_detail_page(pristine_html)
        assert result is not None
        assert result.registration_no == "P007595"
        assert result.trade_name == "Pristine"
        assert len(result.labels) >= 1
        assert result.labels[0].filename  # has a filename
        assert "documentId=" in result.labels[0].url

    def test_capetec_detail(self, capetec_html: str):
        result = _parse_detail_page(capetec_html)
        assert result is not None
        assert result.registration_no == "P008921"
        assert result.trade_name == "Capetec"
        assert len(result.labels) >= 1

    def test_non_detail_page_returns_none(self):
        result = _parse_detail_page("<html><body>Not a detail page</body></html>")
        assert result is None


# ── Network Integration Tests ──────────────────────────────────────────────


@pytest.mark.network
class TestAcvmDetailScraping:
    def test_scrape_pristine(self):
        from src.parsers.acvm_detail_scraper import create_session, search_by_p_number

        session = create_session()
        result = search_by_p_number(session, "P007595")
        assert result is not None
        assert result.registration_no == "P007595"
        assert result.trade_name == "Pristine"
        assert len(result.labels) >= 1
        assert result.labels[0].label_date is not None

    def test_scrape_and_verify_pdf(self):
        """Download one label and verify it's a valid PDF."""
        import time

        from src.parsers.acvm_detail_scraper import create_session, search_by_p_number

        session = create_session()
        result = search_by_p_number(session, "P007595")
        assert result and result.labels

        time.sleep(0.8)
        resp = session.get(result.labels[0].url, timeout=60)
        resp.raise_for_status()

        # Check PDF magic bytes
        assert resp.content[:4] == b"%PDF", "Downloaded file is not a valid PDF"
        assert len(resp.content) > 1000, "PDF suspiciously small"

    def test_scrape_topas(self):
        """Verify Topas 200EW is also scrapable."""
        import time

        from src.parsers.acvm_detail_scraper import create_session, search_by_p_number

        session = create_session()
        time.sleep(0.8)
        result = search_by_p_number(session, "P005374")
        assert result is not None
        assert "Topas" in result.trade_name
