"""Tests for the PHI value parser covering all observed patterns from the 2025/2026 schedule."""

import pytest

from src.utils.phi_value_parser import parse_phi_value


class TestNumericDays:
    """Plain numeric values → value + unit='days'."""

    @pytest.mark.parametrize("raw,expected_days", [
        ("1", 1),
        ("3", 3),
        ("5", 5),
        ("7", 7),
        ("14", 14),
        ("21", 21),
        ("28", 28),
        ("30", 30),
        ("35", 35),
        ("42", 42),
        ("56", 56),
        ("65", 65),
        ("70", 70),
    ])
    def test_numeric_days(self, raw: str, expected_days: int):
        result = parse_phi_value(raw)
        assert result is not None
        assert result.value == expected_days
        assert result.unit == "days"
        assert result.el_stage is None
        assert result.code is None


class TestGrowthStages:
    """Single EL growth stage values."""

    @pytest.mark.parametrize("raw,expected_stage", [
        ("EL2", "EL2"),
        ("EL18", "EL18"),
        ("EL25", "EL25"),
    ])
    def test_single_el(self, raw: str, expected_stage: str):
        result = parse_phi_value(raw)
        assert result is not None
        assert result.el_stage == expected_stage
        assert result.value is None
        assert result.code is None


class TestCompoundValues:
    """Compound EL + offset values, including newline variants from PDF."""

    @pytest.mark.parametrize("raw", [
        "EL4 + 14 days",
        "EL4\n+ 14\ndays",
        "EL4 +\n14 days",
        "EL4\n+ 14 days",
    ])
    def test_el_offset(self, raw: str):
        result = parse_phi_value(raw)
        assert result is not None
        assert result.el_stage == "EL4"
        assert result.el_offset_days == 14
        assert result.raw == raw  # original preserved

    @pytest.mark.parametrize("raw", [
        "EL1 - EL2",
        "EL1 -\nEL2",
        "EL1 – EL2",  # en-dash
        "EL1 — EL2",  # em-dash
    ])
    def test_el_range(self, raw: str):
        result = parse_phi_value(raw)
        assert result is not None
        assert result.el_stage == "EL1"
        assert result.el_stage_end == "EL2"


class TestSfptCombinations:
    """SFPT + growth stage, including newline variants."""

    @pytest.mark.parametrize("raw,expected_el", [
        ("SFPT\nEL18", "EL18"),
        ("SFPT\nEL25", "EL25"),
        ("SFPT EL18", "EL18"),
    ])
    def test_sfpt_el(self, raw: str, expected_el: str):
        result = parse_phi_value(raw)
        assert result is not None
        assert result.code == "SFPT"
        assert result.secondary_el_stage == expected_el


class TestSpecialCodes:
    """Exact-match special codes."""

    @pytest.mark.parametrize("raw,expected_code", [
        ("NS", "NS"),
        ("SNC", "SNC"),
        ("NPV", "NPV"),
        ("ID", "ID"),
        ("WFD", "WFD"),
        ("DNG", "DNG"),
        ("N/A", "N/A"),
        ("nil", "nil"),
        ("NC/NC", "NC/NC"),
    ])
    def test_special_codes(self, raw: str, expected_code: str):
        result = parse_phi_value(raw)
        assert result is not None
        assert result.code == expected_code
        assert result.value is None
        assert result.el_stage is None

    def test_case_insensitive(self):
        result = parse_phi_value("snc")
        assert result is not None
        assert result.code == "SNC"


class TestReiFormats:
    """REI-specific value formats."""

    def test_wfd(self):
        result = parse_phi_value("WFD")
        assert result is not None
        assert result.code == "WFD"

    def test_hours(self):
        result = parse_phi_value("4 hrs")
        assert result is not None
        assert result.value == 4
        assert result.unit == "hrs"

    @pytest.mark.parametrize("raw,days", [
        ("5 days", 5),
        ("6 days", 6),
        ("8 days", 8),
    ])
    def test_days(self, raw: str, days: int):
        result = parse_phi_value(raw)
        assert result is not None
        assert result.value == days
        assert result.unit == "days"


class TestWhpFormats:
    """WHP-specific value formats including months and compound values."""

    @pytest.mark.parametrize("raw,months", [
        ("2 months", 2),
        ("6 months", 6),
    ])
    def test_months(self, raw: str, months: int):
        result = parse_phi_value(raw)
        assert result is not None
        assert result.value == months
        assert result.unit == "months"

    def test_one_day(self):
        result = parse_phi_value("1 day")
        assert result is not None
        assert result.value == 1
        assert result.unit == "days"

    def test_fourteen_days(self):
        result = parse_phi_value("14 days")
        assert result is not None
        assert result.value == 14
        assert result.unit == "days"

    def test_months_see_notes(self):
        result = parse_phi_value("2 months - see notes column")
        assert result is not None
        assert result.value == 2
        assert result.unit == "months"
        assert result.code == "SNC"

    def test_mths_snc(self):
        result = parse_phi_value("2 mths SNC")
        assert result is not None
        assert result.value == 2
        assert result.unit == "months"
        assert result.code == "SNC"

    def test_see_notes_column_alone(self):
        result = parse_phi_value("see notes column")
        assert result is not None
        assert result.code == "SNC"


class TestEmptyValues:
    """None, empty, and dash values return None."""

    @pytest.mark.parametrize("raw", [None, "", "  ", "-", "—", "–"])
    def test_empty_returns_none(self, raw):
        assert parse_phi_value(raw) is None


class TestRawPreservation:
    """The raw field always contains the original input."""

    def test_preserves_newlines(self):
        raw = "EL4\n+ 14\ndays"
        result = parse_phi_value(raw)
        assert result is not None
        assert result.raw == raw

    def test_preserves_whitespace(self):
        raw = "  28  "
        result = parse_phi_value(raw)
        assert result is not None
        assert result.raw == raw
