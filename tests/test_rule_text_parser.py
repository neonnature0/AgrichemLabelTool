"""Tests for the rule text parser — extracts structured data from RM rule descriptions."""

import pytest

from src.utils.rule_text_parser import (
    extract_max_applications,
    extract_non_consecutive,
    extract_tank_mix_requirement,
)


class TestExtractMaxApplications:
    @pytest.mark.parametrize("text,expected", [
        ("A maximum of one application (EL3 - EL47) per season of any Group 3 fungicide in tank mix with sulphur.", 1),
        ("A maximum of 2 non-consecutive applications per season.", 2),
        ("A maximum of 3 applications per season of any Group 29 fungicides.", 3),
        ("A maximum of two non-consecutive applications per season of any Group 50 fungicides in tank mix with sulphur.", 2),
        ("A maximum of 1 application per season.", 1),
    ])
    def test_numeric_and_word(self, text: str, expected: int):
        assert extract_max_applications(text) == expected

    def test_no_max_returns_none(self):
        assert extract_max_applications("Follow label instructions.") is None

    def test_integrated_weed_management(self):
        assert extract_max_applications("Apply integrated weed management practices.") is None

    def test_none_input(self):
        assert extract_max_applications(None) is None


class TestExtractNonConsecutive:
    def test_non_consecutive_present(self):
        assert extract_non_consecutive("A maximum of 2 non-consecutive applications per season.") is True

    def test_non_consecutive_absent(self):
        assert extract_non_consecutive("A maximum of 3 applications per season.") is False

    def test_hyphenated_form(self):
        assert extract_non_consecutive("A maximum of two non-consecutive applications.") is True

    def test_none_input(self):
        assert extract_non_consecutive(None) is False

    def test_follow_label(self):
        assert extract_non_consecutive("Follow label instructions.") is False


class TestExtractTankMixRequirement:
    def test_sulphur(self):
        result = extract_tank_mix_requirement(
            "A maximum of one application per season in tank mix with sulphur."
        )
        assert result == "sulphur"

    def test_effective_botrytis_product(self):
        result = extract_tank_mix_requirement(
            "Must be in a tank mix with an effective botrytis product."
        )
        assert result == "an effective botrytis product"

    def test_no_tank_mix(self):
        assert extract_tank_mix_requirement("A maximum of 3 applications per season.") is None

    def test_none_input(self):
        assert extract_tank_mix_requirement(None) is None

    def test_multiline_rule(self):
        result = extract_tank_mix_requirement(
            "A maximum of one application\n(EL3 - EL47) per season\nof any Group 3 fungicide\nin tank mix with sulphur."
        )
        assert result == "sulphur"
