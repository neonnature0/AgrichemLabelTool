"""Tests for the assembler using hand-crafted PhiTableResult fixtures."""

import pytest

from src.assembler import assemble_catalogue, _split_trade_names, _parse_ai_name, _split_rm_codes
from src.parsers.phi_table import PhiTableResult, PhiTableRow


def _make_row(
    ai: str = "captan",
    trade: str = "Capetec",
    section: str = "fungicides",
    phi: dict[str, str] | None = None,
    rei: str = "WFD",
    whp_s: str = "2 months",
    whp_g: str = "NS",
    rm: str = "M04/phthalimide",
    notes: str | None = None,
    label_claim: str = "botrytis",
) -> PhiTableRow:
    return PhiTableRow(
        active_ingredient_raw=ai,
        trade_names_raw=trade,
        label_claim=label_claim,
        rei_raw=rei,
        notes=notes,
        rm_rule_codes_raw=rm,
        whp_slaughter_raw=whp_s,
        whp_grazing_raw=whp_g,
        phi_values=phi or {"NZ": "14", "AU": "14", "EU": "28"},
        section=section,
        page_number=1,
    )


class TestSplitTradeNames:
    def test_single_name(self):
        assert _split_trade_names("Capetec") == ["Capetec"]

    def test_semicolon_separated(self):
        result = _split_trade_names("Capetec; Merpan;\nGroCap")
        assert result == ["Capetec", "Merpan", "GroCap"]

    def test_hyphenated_linebreak(self):
        result = _split_trade_names("Hort-\ncare")
        assert result == ["Hortcare"]

    def test_empty_parts_stripped(self):
        result = _split_trade_names("A; ; B")
        assert result == ["A", "B"]


class TestParseAiName:
    def test_no_asterisks(self):
        name, level = _parse_ai_name("captan")
        assert name == "captan"
        assert level is None

    def test_triple_asterisk(self):
        name, level = _parse_ai_name("mancozeb ***")
        assert name == "mancozeb"
        assert level == 3

    def test_single_asterisk(self):
        name, level = _parse_ai_name("oxyfluorfen *")
        assert name == "oxyfluorfen"
        assert level == 1

    def test_multiline_name(self):
        name, level = _parse_ai_name("fatty acids\n(potassium\nsalts)")
        assert name == "fatty acids (potassium salts)"
        assert level is None


class TestSplitRmCodes:
    def test_single_code(self):
        assert _split_rm_codes("M04/phthalimide") == ["M04/phthalimide"]

    def test_ampersand_split(self):
        assert _split_rm_codes("12/PP & 7/SDHI") == ["12/PP", "7/SDHI"]

    def test_hyphenated_linebreak(self):
        assert _split_rm_codes("M03/dithiocarba-\nmate") == ["M03/dithiocarbamate"]

    def test_none(self):
        assert _split_rm_codes(None) == []


class TestAssembleCatalogue:
    def test_basic_assembly(self):
        result = PhiTableResult(
            rows=[_make_row()],
            warnings=[],
            pages_processed=1,
        )
        cat = assemble_catalogue(
            result, season="2025-2026", source_pdf="test.pdf", source_hash="abc123"
        )
        assert cat.stats.total_active_ingredients == 1
        assert cat.stats.total_trade_products == 1
        assert cat.stats.total_phi_entries == 3  # NZ, AU, EU
        assert cat.stats.total_rei_entries == 1
        assert cat.stats.total_whp_entries == 2  # slaughter + grazing

    def test_semicolon_expansion(self):
        result = PhiTableResult(
            rows=[_make_row(trade="ProductA; ProductB; ProductC")],
            warnings=[],
            pages_processed=1,
        )
        cat = assemble_catalogue(
            result, season="2025-2026", source_pdf="test.pdf", source_hash="abc123"
        )
        assert cat.stats.total_trade_products == 3
        assert cat.stats.total_phi_entries == 9  # 3 products × 3 markets
        product_names = [tp.name for tp in cat.trade_products]
        assert "ProductA" in product_names
        assert "ProductB" in product_names
        assert "ProductC" in product_names

    def test_ai_deduplication(self):
        result = PhiTableResult(
            rows=[
                _make_row(ai="captan", trade="Capetec"),
                _make_row(ai="captan", trade="Merpan"),
            ],
            warnings=[],
            pages_processed=1,
        )
        cat = assemble_catalogue(
            result, season="2025-2026", source_pdf="test.pdf", source_hash="abc123"
        )
        assert cat.stats.total_active_ingredients == 1
        assert cat.stats.total_trade_products == 2

    def test_compound_ai(self):
        """Products with '+' in AI name get multiple AI links."""
        result = PhiTableResult(
            rows=[_make_row(ai="mancozeb + metalaxyl-m", trade="Ridomil Gold")],
            warnings=[],
            pages_processed=1,
        )
        cat = assemble_catalogue(
            result, season="2025-2026", source_pdf="test.pdf", source_hash="abc123"
        )
        ridomil = [tp for tp in cat.trade_products if tp.name == "Ridomil Gold"][0]
        assert len(ridomil.active_ingredient_ids) == 2
        assert "mancozeb" in ridomil.active_ingredient_ids
        assert "metalaxyl-m" in ridomil.active_ingredient_ids

    def test_section_distribution(self):
        result = PhiTableResult(
            rows=[
                _make_row(section="fungicides", trade="FungiProd"),
                _make_row(section="herbicides", trade="HerbProd"),
                _make_row(section="insecticides", trade="InsectProd"),
            ],
            warnings=[],
            pages_processed=1,
        )
        cat = assemble_catalogue(
            result, season="2025-2026", source_pdf="test.pdf", source_hash="abc123"
        )
        assert cat.stats.products_by_section == {
            "fungicides": 1,
            "herbicides": 1,
            "insecticides": 1,
        }

    def test_deterministic_sorting(self):
        """Output lists are sorted by ID for idempotent output."""
        result = PhiTableResult(
            rows=[
                _make_row(ai="zinc", trade="ZincProd"),
                _make_row(ai="alpha", trade="AlphaProd"),
            ],
            warnings=[],
            pages_processed=1,
        )
        cat = assemble_catalogue(
            result, season="2025-2026", source_pdf="test.pdf", source_hash="abc123"
        )
        ai_ids = [ai.id for ai in cat.active_ingredients]
        assert ai_ids == sorted(ai_ids)
        tp_ids = [tp.id for tp in cat.trade_products]
        assert tp_ids == sorted(tp_ids)

    def test_referential_integrity(self):
        """SeasonCatalogue validator catches broken FK references."""
        from src.models import SeasonCatalogue, PhiEntry, PhiValue, CatalogueStats

        with pytest.raises(ValueError, match="Referential integrity"):
            SeasonCatalogue(
                season="2025-2026",
                source_pdf="test.pdf",
                source_hash="abc",
                parsed_at="2025-01-01T00:00:00Z",
                parser_version="1.0.0",
                active_ingredients=[],
                trade_products=[],
                phi_entries=[
                    PhiEntry(
                        trade_product_id="nonexistent",
                        market_code="NZ",
                        phi=PhiValue(raw="14", value=14, unit="days"),
                        season="2025-2026",
                    )
                ],
                rei_entries=[],
                whp_entries=[],
                stats=CatalogueStats(
                    total_active_ingredients=0,
                    total_trade_products=0,
                    total_phi_entries=1,
                    total_rei_entries=0,
                    total_whp_entries=0,
                    products_by_section={},
                    markets=["NZ"],
                ),
            )
