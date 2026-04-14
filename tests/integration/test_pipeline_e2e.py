"""End-to-end integration test against the actual NZW Spray Schedule PDF.

Requires the PDF to be present at the configured input location.
Run with: pytest tests/integration/ -m integration
"""

from __future__ import annotations

from pathlib import Path

import orjson
import pytest

from src.assembler import assemble_catalogue
from src.models import SeasonCatalogue
from src.parsers.changes_table import parse_changes_table
from src.parsers.flagged_table import parse_flagged_table
from src.parsers.phi_table import parse_phi_table
from src.parsers.rm_table import parse_rm_table
from src.utils.hashing import hash_file

# Path to the actual PDF
PDF_PATH = Path(
    "D:/cordyn + supamode/Feature Ideas/Product Catalogue/Pipeline/"
    "NZW Spray Schedule 2025-2026.pdf"
)

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def catalogue() -> SeasonCatalogue:
    """Parse and assemble the full catalogue with all parsers (once per module)."""
    if not PDF_PATH.exists():
        pytest.skip(f"PDF not found at {PDF_PATH}")

    phi = parse_phi_table(PDF_PATH, write_staging=False)
    rm = parse_rm_table(PDF_PATH, write_staging=False)
    changes = parse_changes_table(PDF_PATH, write_staging=False)
    flagged = parse_flagged_table(PDF_PATH, write_staging=False)

    return assemble_catalogue(
        phi, rm, changes, flagged,
        season="2025-2026",
        source_pdf=PDF_PATH.name,
        source_hash=hash_file(PDF_PATH),
    )


# ── Phase 1 tests (unchanged) ──────────────────────────────────────────────


class TestCatalogueShape:
    def test_has_active_ingredients(self, catalogue: SeasonCatalogue):
        assert catalogue.stats.total_active_ingredients > 50

    def test_has_trade_products(self, catalogue: SeasonCatalogue):
        assert catalogue.stats.total_trade_products > 200

    def test_has_phi_entries(self, catalogue: SeasonCatalogue):
        assert catalogue.stats.total_phi_entries > 3000

    def test_has_all_four_sections(self, catalogue: SeasonCatalogue):
        sections = set(catalogue.stats.products_by_section.keys())
        assert sections == {"fungicides", "herbicides", "insecticides", "wound_dressings"}

    def test_has_rei_entries(self, catalogue: SeasonCatalogue):
        assert catalogue.stats.total_rei_entries > 0

    def test_has_whp_entries(self, catalogue: SeasonCatalogue):
        assert catalogue.stats.total_whp_entries > 0


class TestSpotChecks:
    def test_mancozeb_is_fungicide(self, catalogue: SeasonCatalogue):
        mancozeb = next(
            (ai for ai in catalogue.active_ingredients if ai.name == "mancozeb"), None
        )
        assert mancozeb is not None
        assert mancozeb.section == "fungicides"
        assert mancozeb.restriction_level == 3

    def test_mancozeb_has_multiple_trade_products(self, catalogue: SeasonCatalogue):
        manc_products = [
            tp for tp in catalogue.trade_products
            if "mancozeb" in tp.active_ingredient_ids and tp.section == "fungicides"
        ]
        assert len(manc_products) >= 5

    def test_mancozeb_phi_el18_for_nz(self, catalogue: SeasonCatalogue):
        manc_tp = next(
            (tp for tp in catalogue.trade_products if tp.id == "dithane-rainshield-neo-tec"),
            None,
        )
        assert manc_tp is not None
        nz_phi = next(
            (p for p in catalogue.phi_entries
             if p.trade_product_id == manc_tp.id and p.market_code == "NZ"),
            None,
        )
        assert nz_phi is not None
        assert nz_phi.phi.value == 14

    def test_mancozeb_sfpt_for_eu(self, catalogue: SeasonCatalogue):
        manc_tp = next(
            (tp for tp in catalogue.trade_products if tp.id == "dithane-rainshield-neo-tec"),
            None,
        )
        assert manc_tp is not None
        eu_phi = next(
            (p for p in catalogue.phi_entries
             if p.trade_product_id == manc_tp.id and p.market_code == "EU"),
            None,
        )
        assert eu_phi is not None
        assert eu_phi.phi.code == "SFPT"
        assert eu_phi.phi.secondary_el_stage == "EL18"

    def test_imidacloprid_npv_all_markets(self, catalogue: SeasonCatalogue):
        confidor_phi = [
            p for p in catalogue.phi_entries if p.trade_product_id == "confidor"
        ]
        assert len(confidor_phi) == 18
        assert all(p.phi.code == "NPV" for p in confidor_phi)

    def test_isofetamid_snc_for_nil_and_hk(self, catalogue: SeasonCatalogue):
        kenja_phi = [
            p for p in catalogue.phi_entries if p.trade_product_id == "kenja-400-sc"
        ]
        nil_phi = next((p for p in kenja_phi if p.market_code == "NIL"), None)
        hk_phi = next((p for p in kenja_phi if p.market_code == "HK"), None)
        assert nil_phi is not None and nil_phi.phi.code == "SNC"
        assert hk_phi is not None and hk_phi.phi.code == "SNC"

    def test_botector_value_1_all_markets(self, catalogue: SeasonCatalogue):
        bot_phi = [
            p for p in catalogue.phi_entries if p.trade_product_id == "botector"
        ]
        assert len(bot_phi) == 18
        assert all(p.phi.value == 1 and p.phi.unit == "days" for p in bot_phi)

    def test_fluazinam_in_two_sections(self, catalogue: SeasonCatalogue):
        flu_sections = {
            tp.section for tp in catalogue.trade_products
            if "fluazinam" in tp.active_ingredient_ids
        }
        assert "fungicides" in flu_sections
        assert "wound_dressings" in flu_sections

    def test_glyphosate_is_herbicide(self, catalogue: SeasonCatalogue):
        gly = next(
            (ai for ai in catalogue.active_ingredients if ai.name == "glyphosate"), None
        )
        assert gly is not None
        assert gly.section == "herbicides"


# ── Phase 2 tests ──────────────────────────────────────────────────────────


class TestRmRules:
    """Verify resistance management rules are correctly parsed."""

    def test_rm_rule_count(self, catalogue: SeasonCatalogue):
        assert catalogue.stats.total_rm_rules == 50

    def test_rm_has_all_sections(self, catalogue: SeasonCatalogue):
        sections = {r.category for r in catalogue.resistance_management_rules}
        assert sections == {"fungicides", "herbicides", "insecticides", "wound_dressings"}

    def test_dmi_rule(self, catalogue: SeasonCatalogue):
        """3/DMI should have max 1 application in tank mix with sulphur."""
        dmi = next(
            (r for r in catalogue.resistance_management_rules if r.rule_code == "3/DMI"),
            None,
        )
        assert dmi is not None
        assert dmi.category == "fungicides"
        assert dmi.max_applications == 1
        assert dmi.requires_tank_mix == "sulphur"
        assert dmi.must_be_non_consecutive is False
        assert "difenoconazole" in dmi.applicable_active_ingredients
        assert "penconazole" in dmi.applicable_active_ingredients

    def test_phenylamide_rule(self, catalogue: SeasonCatalogue):
        """4/phenylamide should have max 2 non-consecutive applications."""
        phen = next(
            (r for r in catalogue.resistance_management_rules
             if r.rule_code == "4/phenylamide"),
            None,
        )
        assert phen is not None
        assert phen.max_applications == 2
        assert phen.must_be_non_consecutive is True
        assert phen.requires_tank_mix is None

    def test_nc_nc_rule(self, catalogue: SeasonCatalogue):
        """NC/NC (no classification) should have 'Follow label instructions'."""
        nc = next(
            (r for r in catalogue.resistance_management_rules
             if r.rule_code == "NC/NC"),
            None,
        )
        assert nc is not None
        assert nc.rule_text is not None
        assert "Follow label" in nc.rule_text
        assert nc.max_applications is None

    def test_group_29_max_3(self, catalogue: SeasonCatalogue):
        """29/2,6-dinitro-aniline (fungicides) should have max 3."""
        g29_fungicide = [
            r for r in catalogue.resistance_management_rules
            if "29" in r.rule_code and r.category == "fungicides"
        ]
        assert len(g29_fungicide) >= 1
        assert g29_fungicide[0].max_applications == 3


class TestScheduleChanges:
    """Verify significant changes are correctly parsed."""

    def test_changes_count(self, catalogue: SeasonCatalogue):
        assert catalogue.stats.total_schedule_changes == 41

    def test_has_withdrawn_products_topic(self, catalogue: SeasonCatalogue):
        withdrawn = [
            c for c in catalogue.schedule_changes if "Withdrawn" in c.topic
        ]
        assert len(withdrawn) > 0

    def test_folpet_withdrawn(self, catalogue: SeasonCatalogue):
        folpet_changes = [
            c for c in catalogue.schedule_changes
            if c.active_ingredient and "folpet" in c.active_ingredient.lower()
        ]
        assert len(folpet_changes) >= 1
        assert any("Withdrawn" in c.topic for c in folpet_changes)

    def test_has_rei_changes(self, catalogue: SeasonCatalogue):
        rei = [c for c in catalogue.schedule_changes if "REI" in c.topic]
        assert len(rei) > 0


class TestFlaggedProducts:
    """Verify flagged products are correctly parsed."""

    def test_flagged_count(self, catalogue: SeasonCatalogue):
        assert catalogue.stats.total_flagged_products == 9

    def test_clethodim_requires_approval(self, catalogue: SeasonCatalogue):
        cle = next(
            (f for f in catalogue.flagged_products if f.active_ingredient == "clethodim"),
            None,
        )
        assert cle is not None
        assert "PRIOR TO USE" in cle.issue
        assert "written permission" in cle.issue

    def test_organophosphates_banned(self, catalogue: SeasonCatalogue):
        org = next(
            (f for f in catalogue.flagged_products if "organophosphates" in f.active_ingredient),
            None,
        )
        assert org is not None
        assert "must not use" in org.issue

    def test_dithiocarbamates_flagged(self, catalogue: SeasonCatalogue):
        dith = next(
            (f for f in catalogue.flagged_products if "Dithiocarbamates" in f.active_ingredient),
            None,
        )
        assert dith is not None
        assert "EU" in dith.issue


# ── Idempotency + serialization ────────────────────────────────────────────


class TestIdempotency:
    def test_idempotent_output(self):
        if not PDF_PATH.exists():
            pytest.skip(f"PDF not found at {PDF_PATH}")

        source_hash = hash_file(PDF_PATH)

        def _run():
            phi = parse_phi_table(PDF_PATH, write_staging=False)
            rm = parse_rm_table(PDF_PATH, write_staging=False)
            changes = parse_changes_table(PDF_PATH, write_staging=False)
            flagged = parse_flagged_table(PDF_PATH, write_staging=False)
            return assemble_catalogue(
                phi, rm, changes, flagged,
                season="2025-2026", source_pdf=PDF_PATH.name, source_hash=source_hash,
            )

        cat1 = _run()
        cat2 = _run()

        dump1 = cat1.model_dump()
        dump2 = cat2.model_dump()
        dump1.pop("parsed_at")
        dump2.pop("parsed_at")

        bytes1 = orjson.dumps(dump1, option=orjson.OPT_SORT_KEYS)
        bytes2 = orjson.dumps(dump2, option=orjson.OPT_SORT_KEYS)

        assert bytes1 == bytes2, "Pipeline output is not idempotent"


class TestJsonSerialization:
    def test_round_trip(self, catalogue: SeasonCatalogue):
        data = orjson.dumps(
            catalogue.model_dump(),
            option=orjson.OPT_INDENT_2 | orjson.OPT_SORT_KEYS,
        )
        loaded = orjson.loads(data)
        restored = SeasonCatalogue(**loaded)
        assert restored.stats.total_trade_products == catalogue.stats.total_trade_products
        assert restored.stats.total_rm_rules == catalogue.stats.total_rm_rules
        assert restored.stats.total_schedule_changes == catalogue.stats.total_schedule_changes
        assert restored.stats.total_flagged_products == catalogue.stats.total_flagged_products


# ── Phase 3: Diff Engine Integration Tests ─────────────────────────────────

PREV_PATH = Path("D:/cordyn + supamode/cordyn-catalogue/nz-catalogue/data/output/2024-2025")


class TestDiffEngine:
    """Integration tests for the season diff engine using synthetic previous season."""

    @pytest.fixture(scope="class")
    def diff_result(self):
        from src.stages.diff_seasons import diff_seasons

        prev_path = PREV_PATH / "catalogue.json"
        curr_path = Path("D:/cordyn + supamode/cordyn-catalogue/nz-catalogue/data/output/2025-2026/catalogue.json")
        if not prev_path.exists() or not curr_path.exists():
            pytest.skip("Synthetic previous season not generated")

        prev = SeasonCatalogue(**orjson.loads(prev_path.read_bytes()))
        curr = SeasonCatalogue(**orjson.loads(curr_path.read_bytes()))
        return diff_seasons(prev, curr)

    @pytest.fixture(scope="class")
    def known_changes(self):
        known_path = PREV_PATH / "known_changes.json"
        if not known_path.exists():
            pytest.skip("Known changes oracle not found")
        return orjson.loads(known_path.read_bytes())

    def test_detects_all_known_changes(self, diff_result, known_changes):
        """Every known change in the oracle must be detected by the diff engine."""
        missed = []
        for kc in known_changes:
            found = any(
                e.change_type == kc["change_type"]
                and e.entity_id == kc.get("entity_id")
                and (not kc.get("market") or e.market == kc["market"])
                and (not kc.get("field_changed") or e.field_changed == kc["field_changed"])
                for e in diff_result
            )
            if not found:
                missed.append(kc)
        assert missed == [], f"Missed known changes: {missed}"

    def test_no_false_positives(self, diff_result, known_changes):
        """No unexpected changes beyond the known ones."""
        assert len(diff_result) == len(known_changes), (
            f"Expected {len(known_changes)} changes, got {len(diff_result)}"
        )

    def test_severity_classifications(self, diff_result):
        """Check severity is correct for known change types."""
        for entry in diff_result:
            if entry.change_type in ("product_removed", "active_ingredient_removed", "rm_rule_removed"):
                assert entry.severity == "critical", f"{entry.change_type} should be critical"
            elif entry.change_type == "phi_changed" and entry.new_value == "NPV":
                assert entry.severity == "critical", "PHI→NPV should be critical"
            elif entry.change_type in ("product_added", "rm_rule_added", "product_unflagged",
                                       "rm_rule_relaxed", "rm_rule_wording_changed"):
                assert entry.severity == "info", f"{entry.change_type} should be info"

    def test_product_additions_detected(self, diff_result):
        added = [e for e in diff_result if e.change_type == "product_added"]
        added_ids = {e.entity_id for e in added}
        assert "belanty" in added_ids
        assert "citara-200ew" in added_ids
        assert "digger-ew" in added_ids

    def test_product_removals_detected(self, diff_result):
        removed = [e for e in diff_result if e.change_type == "product_removed"]
        removed_ids = {e.entity_id for e in removed}
        assert "fake-withdrawn-a" in removed_ids
        assert "fake-withdrawn-b" in removed_ids

    def test_dmi_tightened(self, diff_result):
        dmi = [e for e in diff_result if e.entity_id == "3/DMI" and e.change_type == "rm_rule_tightened"]
        assert len(dmi) == 1
        assert dmi[0].field_changed == "max_applications"
        assert dmi[0].old_value == "2"
        assert dmi[0].new_value == "1"

    def test_confidor_npv_critical(self, diff_result):
        confidor = [
            e for e in diff_result
            if e.entity_id == "confidor" and e.market == "NZ" and e.change_type == "phi_changed"
        ]
        assert len(confidor) == 1
        assert confidor[0].severity == "critical"
        assert confidor[0].new_value == "NPV"

    def test_diff_idempotency(self):
        """Running the diff twice produces identical output."""
        from src.stages.diff_seasons import diff_seasons

        prev_path = PREV_PATH / "catalogue.json"
        curr_path = Path("D:/cordyn + supamode/cordyn-catalogue/nz-catalogue/data/output/2025-2026/catalogue.json")
        if not prev_path.exists() or not curr_path.exists():
            pytest.skip("Data not available")

        prev = SeasonCatalogue(**orjson.loads(prev_path.read_bytes()))
        curr = SeasonCatalogue(**orjson.loads(curr_path.read_bytes()))

        result1 = diff_seasons(prev, curr)
        result2 = diff_seasons(prev, curr)

        bytes1 = orjson.dumps([e.model_dump() for e in result1], option=orjson.OPT_SORT_KEYS)
        bytes2 = orjson.dumps([e.model_dump() for e in result2], option=orjson.OPT_SORT_KEYS)
        assert bytes1 == bytes2
