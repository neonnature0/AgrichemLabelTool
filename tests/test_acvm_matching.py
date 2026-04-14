"""Tests for ACVM CSV parsing and product matching."""

import pytest

from src.models import TradeProduct
from src.parsers.acvm_csv import AcvmIngredient, AcvmProduct, _parse_csv
from src.stages.match_acvm import MatchResult, _strip_brackets, match_products


# ── CSV Parsing Tests ──────────────────────────────────────────────────────

SAMPLE_CSV = '''"Registration No","Trade Name","Product Type","Registrant Name","NZ Agent Name","Condition ID","Ingredient","Content","Unit","Date of registration"
"P007595","Pristine","Fungicide","BASF New Zealand Limited","","60","PYRACLOSTROBIN","128.00","g/kg","12/10/2007"
"P007595","Pristine","Fungicide","BASF New Zealand Limited","","60","BOSCALID","252.00","g/kg","12/10/2007"
"P007595","Pristine","Fungicide","BASF New Zealand Limited","","61","PYRACLOSTROBIN","128.00","g/kg","12/10/2007"
"P007595","Pristine","Fungicide","BASF New Zealand Limited","","61","BOSCALID","252.00","g/kg","12/10/2007"
"P008921","Capetec","Fungicide","UPL New Zealand Limited","","60","CAPTAN","800.00","g/kg","11/12/2013"
'''


class TestCsvParsing:
    def test_parse_basic_csv(self):
        products = _parse_csv(SAMPLE_CSV)
        assert len(products) == 2
        assert "Pristine" in products
        assert "Capetec" in products

    def test_pristine_fields(self):
        products = _parse_csv(SAMPLE_CSV)
        p = products["Pristine"]
        assert p.registration_no == "P007595"
        assert p.registrant == "BASF New Zealand Limited"
        assert p.product_type == "Fungicide"
        assert p.registration_date == "12/10/2007"

    def test_pristine_ingredients_deduplicated(self):
        """Same ingredient across conditions should be deduplicated."""
        products = _parse_csv(SAMPLE_CSV)
        p = products["Pristine"]
        names = [i.name for i in p.ingredients]
        assert names == ["PYRACLOSTROBIN", "BOSCALID"]

    def test_ingredient_concentration(self):
        products = _parse_csv(SAMPLE_CSV)
        pyra = next(i for i in products["Pristine"].ingredients if i.name == "PYRACLOSTROBIN")
        assert pyra.content == 128.0
        assert pyra.unit == "g/kg"


# ── Bracket Stripping Tests ────────────────────────────────────────────────


class TestBracketStripping:
    @pytest.mark.parametrize("name,expected", [
        ("Hortcare Glyphosate 360 [Grosafe]", "Hortcare Glyphosate 360"),
        ("Roundup Ultra MAX [Bayer]", "Roundup Ultra MAX"),
        ("AGPRO Glyphosate 510 [Agpro]", "AGPRO Glyphosate 510"),
        ("Pristine", "Pristine"),  # no brackets
        ("GA200 [Rainbow & Brown]", "GA200"),
    ])
    def test_strip_brackets(self, name: str, expected: str):
        assert _strip_brackets(name) == expected


# ── Matching Tests ─────────────────────────────────────────────────────────

def _make_tp(slug: str, name: str) -> TradeProduct:
    return TradeProduct(id=slug, name=name, active_ingredient_ids=[], section="fungicides")


def _make_acvm(name: str, reg_no: str = "P000001") -> AcvmProduct:
    return AcvmProduct(
        registration_no=reg_no, trade_name=name, product_type="Fungicide",
        registrant="Test Co", agent=None, registration_date="01/01/2020",
    )


class TestProductMatching:
    def test_exact_match(self):
        products = [_make_tp("pristine", "Pristine")]
        acvm = {"Pristine": _make_acvm("Pristine")}
        result = match_products(products, acvm)
        assert "pristine" in result.matches
        assert result.match_method["pristine"] == "exact"

    def test_case_insensitive_match(self):
        products = [_make_tp("pristine", "pristine")]
        acvm = {"Pristine": _make_acvm("Pristine")}
        result = match_products(products, acvm)
        assert "pristine" in result.matches
        assert result.match_method["pristine"] == "case"

    def test_bracket_strip_match(self):
        products = [_make_tp("hortcare-360", "Hortcare Glyphosate 360 [Grosafe]")]
        acvm = {"Hortcare Glyphosate 360": _make_acvm("Hortcare Glyphosate 360")}
        result = match_products(products, acvm)
        assert "hortcare-360" in result.matches

    def test_fuzzy_match(self):
        products = [_make_tp("dithane", "Dithane Rainshield Neo Tec")]
        acvm = {"Dithane Rainshield Neo Tec Fungicide": _make_acvm("Dithane Rainshield Neo Tec Fungicide")}
        result = match_products(products, acvm, fuzzy_threshold=80)
        assert "dithane" in result.matches
        assert result.match_method["dithane"] == "fuzzy"

    def test_no_match(self):
        products = [_make_tp("fake-product", "Completely Unknown Product XYZ")]
        acvm = {"Pristine": _make_acvm("Pristine")}
        result = match_products(products, acvm)
        assert len(result.unmatched) == 1
        assert "fake-product" in result.unmatched

    def test_match_preserves_registration_no(self):
        products = [_make_tp("pristine", "Pristine")]
        acvm = {"Pristine": _make_acvm("Pristine", reg_no="P007595")}
        result = match_products(products, acvm)
        assert result.matches["pristine"].registration_no == "P007595"


# ── Network Integration Test ──────────────────────────────────────────────

@pytest.mark.network
class TestAcvmCsvDownload:
    def test_download_and_parse(self):
        """Download real ACVM CSV and verify key products exist."""
        from src.parsers.acvm_csv import load_acvm_csv
        from pathlib import Path
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            products = load_acvm_csv(cache_dir=Path(tmpdir), max_age_days=0)
            assert len(products) > 3000
            assert "Pristine" in products
            assert products["Pristine"].registration_no == "P007595"

    def test_match_against_catalogue(self):
        """Match real ACVM data against real catalogue."""
        from src.parsers.acvm_csv import load_acvm_csv
        from src.config import ACVM_CACHE_DIR
        import orjson
        from pathlib import Path
        from src.models import SeasonCatalogue

        cat_path = Path("D:/cordyn + supamode/cordyn-catalogue/nz-catalogue/data/output/2025-2026/catalogue.json")
        if not cat_path.exists():
            pytest.skip("Catalogue not found")

        cat = SeasonCatalogue(**orjson.loads(cat_path.read_bytes()))
        acvm = load_acvm_csv()

        result = match_products(cat.trade_products, acvm)

        # Should match at least 90%
        match_rate = len(result.matches) / len(cat.trade_products)
        assert match_rate >= 0.90, f"Match rate {match_rate:.0%} below 90%"

        # Verify known products
        assert "pristine" in result.matches
        assert result.matches["pristine"].registration_no == "P007595"
        assert "capetec" in result.matches
        assert result.matches["capetec"].registration_no == "P008921"
        assert "kenja-400-sc" in result.matches
        assert result.matches["kenja-400-sc"].registration_no == "P009740"
