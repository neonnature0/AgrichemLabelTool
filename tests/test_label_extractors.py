"""Tests for label field extractors using text snippets from real labels."""

import pytest

from src.models import LabelActiveIngredient, TargetRate
from src.parsers.label_field_extractors import (
    extract_acvm_number_from_label,
    extract_active_ingredients,
    extract_buffer_zone,
    extract_container_sizes,
    extract_environmental_cautions,
    extract_growth_stage_restrictions,
    extract_hsr_number,
    extract_hsno_classifications,
    extract_max_applications,
    extract_ppe,
    extract_rainfastness,
    extract_shelf_life,
    extract_signal_word,
    extract_tank_mix_info,
    extract_target_rates,
    extract_whp_raw,
)


class TestActiveIngredients:
    def test_pristine_dual_ai(self):
        text = "Contains 252 g/kg boscalid and 128 g/kg pyraclostrobin in the form of a water dispersible granule"
        result = extract_active_ingredients(text)
        assert len(result) == 2
        boscalid = next(r for r in result if "boscalid" in r.name.lower())
        assert boscalid.concentration_value == 252.0
        assert boscalid.concentration_unit == "g/kg"
        assert boscalid.concentration_basis == "w/w"

    def test_kenja_single_ai(self):
        text = "Active ingredient: Contains 400 g/L Isofetamid in the form of a suspension concentrate."
        result = extract_active_ingredients(text)
        assert len(result) == 1
        assert result[0].name == "Isofetamid"
        assert result[0].concentration_value == 400.0
        assert result[0].concentration_unit == "g/L"
        assert result[0].concentration_basis == "w/v"

    def test_merpan_captan(self):
        text = "Contains 800 g/kg CAPTAN in the form of a water dispersible granule"
        result = extract_active_ingredients(text)
        assert len(result) == 1
        assert "CAPTAN" in result[0].name
        assert result[0].concentration_value == 800.0

    def test_topas_g_per_litre(self):
        text = "TOPAS 200 EW contains 200 g/litre PENCONAZOLE in the form of an oil in water emulsion."
        result = extract_active_ingredients(text)
        assert len(result) == 1
        assert "PENCONAZOLE" in result[0].name
        assert result[0].concentration_value == 200.0
        assert result[0].concentration_unit == "g/L"

    def test_no_ai_returns_empty(self):
        result = extract_active_ingredients("This is a label with no ingredient info.")
        assert result == []


class TestContainerSizes:
    def test_kenja_multiple_sizes(self):
        text = "Net Contents: 500 mL, 1 L, 2 L, 5 L, 10 L, 20 L"
        result = extract_container_sizes(text)
        assert "500 mL" in result
        assert "1 L" in result
        assert "20 L" in result

    def test_pristine_kg(self):
        text = "Contents: 2.5 kg"
        result = extract_container_sizes(text)
        assert "2.5 kg" in result

    def test_merpan_multiple_kg(self):
        text = "NET CONTENTS: 2 kg, 10 kg, 15 kg"
        result = extract_container_sizes(text)
        assert len(result) == 3

    def test_litre_normalised(self):
        text = "Net Contents: 1 litre"
        result = extract_container_sizes(text)
        assert "1 L" in result


class TestRainfastness:
    def test_rainfast_when_dry(self):
        hours, raw = extract_rainfastness("PRISTINE is rainfast once dried on the crop.")
        assert hours == 0.0
        assert raw is not None

    def test_rainfast_within_hours(self):
        hours, raw = extract_rainfastness("Rainfast within 2 hours of application.")
        assert hours == 2.0

    def test_rain_expected(self):
        hours, raw = extract_rainfastness("Do not apply if rain is expected within 6 hours.")
        assert hours == 6.0

    def test_no_rainfastness(self):
        hours, raw = extract_rainfastness("Apply to dry foliage.")
        assert hours is None
        assert raw is None


class TestMaxApplications:
    def test_no_more_than_pattern(self):
        count, raw = extract_max_applications("DO NOT apply more than 3 sprays per season.")
        assert count == 3

    def test_maximum_pattern(self):
        count, raw = extract_max_applications("A maximum of 2 applications per crop.")
        assert count == 2

    def test_frequency_pattern(self):
        count, raw = extract_max_applications("A frequency of 4 applications per year is set.")
        assert count == 4

    def test_not_found(self):
        count, raw = extract_max_applications("Apply as needed.")
        assert count is None


class TestGrowthStageRestrictions:
    def test_from_to_el(self):
        earliest, latest, raw = extract_growth_stage_restrictions(
            "Apply from EL4 to EL27 for best results."
        )
        assert earliest == "EL4"
        assert latest == "EL27"

    def test_do_not_apply_after(self):
        earliest, latest, raw = extract_growth_stage_restrictions(
            "DO NOT apply after veraison."
        )
        assert latest == "veraison"

    def test_dormant_vines(self):
        earliest, latest, raw = extract_growth_stage_restrictions(
            "For use on dormant vines only."
        )
        assert latest == "dormant"

    def test_not_found(self):
        e, l, r = extract_growth_stage_restrictions("Apply when conditions favour disease.")
        assert e is None and l is None


class TestTankMix:
    def test_do_not_mix(self):
        incomp, req, raw = extract_tank_mix_info(
            "Do not tank mix with copper-based products."
        )
        assert len(incomp) >= 1
        assert "copper-based products" in incomp[0]

    def test_must_be_in_tank_mix(self):
        incomp, req, raw = extract_tank_mix_info(
            "Must be applied in a tank mix with a protectant fungicide."
        )
        assert len(req) >= 1

    def test_no_tank_mix_info(self):
        incomp, req, raw = extract_tank_mix_info("Apply in clean water.")
        assert incomp == []
        assert req == []


class TestBufferZone:
    def test_metres_from_waterway(self):
        m, raw = extract_buffer_zone("Do not apply within 15 metres of any waterway.")
        assert m == 15

    def test_buffer_zone_metres(self):
        m, raw = extract_buffer_zone("A buffer zone of 5 m is required.")
        assert m == 5

    def test_not_found(self):
        m, raw = extract_buffer_zone("Apply with care.")
        assert m is None


class TestPPE:
    def test_standard_ppe(self):
        text = "Wear chemical-resistant gloves, goggles, and cotton overalls during application."
        result = extract_ppe(text)
        assert any("gloves" in p for p in result)
        assert any("goggles" in p for p in result)
        assert any("overalls" in p for p in result)


class TestEnvironmental:
    def test_aquatic_toxicity(self):
        result = extract_environmental_cautions("Very toxic to aquatic life.")
        assert any("aquatic" in c.lower() for c in result)

    def test_ecotoxic(self):
        result = extract_environmental_cautions("ECOTOXIC warning applies.")
        assert any("ecotoxic" in c.lower() for c in result)


class TestWhpRaw:
    def test_grape_whp(self):
        result = extract_whp_raw("Grapes: DO NOT apply within 28 days of harvest.")
        assert result is not None
        assert "28 days" in result


class TestShelfLife:
    def test_years(self):
        years, raw = extract_shelf_life(
            "When stored appropriately, no significant degradation for 2 years from date of manufacture."
        )
        assert years == 2.0

    def test_not_found(self):
        years, raw = extract_shelf_life("Store in a cool, dry place.")
        assert years is None


class TestTargetRates:
    def test_grape_botrytis_rate(self):
        text = """Grapes Botrytis 150 g/100 litres Apply in a protectant programme.
Powdery mildew of water
Downy mildew disease.
Kiwifruit something else"""
        rates = extract_target_rates(text)
        assert len(rates) >= 1
        # Should find at least one grape target with a rate
        assert any(r.rate_unit for r in rates)


# ── Regulatory Extractors (Phase 7) ───────────────────────────────────────


class TestHsrNumber:
    @pytest.mark.parametrize("text,expected", [
        ("Approved pursuant to the HSNO Act 1996, Code HSR007853", "HSR007853"),
        ("Approved pursuant to the HSNO Act 1996, Approval Code HSR000592.", "HSR000592"),
        ("Approved pursuant to the HSNO Act 1996, No: HSR100838.", "HSR100838"),
        ("HSR 101512 is the approval code.", "HSR101512"),
    ])
    def test_hsr_patterns(self, text: str, expected: str):
        hsr, raw = extract_hsr_number(text)
        assert hsr == expected
        assert raw is not None

    def test_no_hsr(self):
        hsr, raw = extract_hsr_number("No approval number on this label.")
        assert hsr is None

    def test_normalizes_space(self):
        hsr, _ = extract_hsr_number("Code HSR 007853")
        assert hsr == "HSR007853"  # space removed


class TestHsnoClassifications:
    def test_ghs_section_format(self):
        text = """GHS Classifications
Eye irritation Category 2
Specific target organ toxicity (repeated exposure) Category 1
Hazardous to the aquatic environment chronic Category 2
DANGER
Causes serious eye irritation."""
        classifications, raw = extract_hsno_classifications(text)
        assert len(classifications) >= 2
        assert any("Eye irritation" in c for c in classifications)
        assert any("aquatic" in c.lower() for c in classifications)

    def test_compact_format(self):
        text = """HAZARD CLASSIFICATION
Acute Tox 4 (Oral), Skin Sens. 1, STOT RE1 (Blood, Nervous System), Aquatic Acute 1
HARMFUL"""
        classifications, raw = extract_hsno_classifications(text)
        assert len(classifications) >= 2

    def test_hsno_numeric_format(self):
        text = "Hazard classification 9.1A"
        classifications, raw = extract_hsno_classifications(text)
        assert "9.1A" in classifications

    def test_no_classifications(self):
        classifications, raw = extract_hsno_classifications("Just a normal label.")
        assert classifications == []


class TestSignalWord:
    @pytest.mark.parametrize("text,expected", [
        ("DANGER\nKeep out of reach of children", "DANGER"),
        ("WARNING\nHarmful if swallowed", "WARNING"),
        ("CAUTION\nMay cause irritation", "CAUTION"),
    ])
    def test_signal_words(self, text: str, expected: str):
        word, raw = extract_signal_word(text)
        assert word == expected

    def test_no_signal(self):
        word, raw = extract_signal_word("a" * 1500 + " DANGER")  # DANGER too far in
        # Should still find it if within 1500 chars... actually "a"*1500 + " DANGER" = 1507 chars
        # DANGER is at position 1501, outside the 1500 char window
        assert word is None

    def test_danger_in_header(self):
        word, _ = extract_signal_word("Product Label\nDANGER\nKeep out of reach")
        assert word == "DANGER"


class TestAcvmNumberFromLabel:
    def test_p_number(self):
        p_num, raw = extract_acvm_number_from_label("Registered No P007595")
        assert p_num == "P007595"

    def test_p_number_in_context(self):
        p_num, _ = extract_acvm_number_from_label("ACVM registered P008921 for use in NZ")
        assert p_num == "P008921"

    def test_no_p_number(self):
        p_num, _ = extract_acvm_number_from_label("No registration on this label")
        assert p_num is None
