"""Unit tests for the season diff engine using small synthetic catalogues."""

import pytest

from src.models import (
    ActiveIngredient,
    CatalogueStats,
    ChangelogEntry,
    FlaggedProduct,
    PhiEntry,
    PhiValue,
    ReiEntry,
    ResistanceManagementRule,
    SeasonCatalogue,
    TradeProduct,
    WhpEntry,
)
from src.stages.diff_seasons import diff_seasons


def _make_phi_value(days: int) -> PhiValue:
    return PhiValue(raw=str(days), value=days, unit="days")


def _make_phi_value_code(code: str) -> PhiValue:
    return PhiValue(raw=code, code=code)


def _make_phi_value_el(stage: str) -> PhiValue:
    return PhiValue(raw=stage, el_stage=stage)


def _make_catalogue(
    season: str = "2025-2026",
    ais: list[ActiveIngredient] | None = None,
    products: list[TradeProduct] | None = None,
    phi: list[PhiEntry] | None = None,
    rei: list[ReiEntry] | None = None,
    whp: list[WhpEntry] | None = None,
    rm_rules: list[ResistanceManagementRule] | None = None,
    flagged: list[FlaggedProduct] | None = None,
) -> SeasonCatalogue:
    ais = ais or []
    products = products or []
    phi = phi or []
    rei = rei or []
    whp = whp or []
    rm_rules = rm_rules or []
    flagged = flagged or []

    return SeasonCatalogue(
        season=season,
        source_pdf="test.pdf",
        source_hash="abc",
        parsed_at="2025-01-01T00:00:00Z",
        parser_version="1.0.0",
        active_ingredients=ais,
        trade_products=products,
        phi_entries=phi,
        rei_entries=rei,
        whp_entries=whp,
        resistance_management_rules=rm_rules,
        flagged_products=flagged,
        stats=CatalogueStats(
            total_active_ingredients=len(ais),
            total_trade_products=len(products),
            total_phi_entries=len(phi),
            total_rei_entries=len(rei),
            total_whp_entries=len(whp),
            total_rm_rules=len(rm_rules),
            total_flagged_products=len(flagged),
            products_by_section={},
            markets=["NZ"],
        ),
    )


_AI = ActiveIngredient(id="captan", name="captan", section="fungicides")
_TP = TradeProduct(id="capetec", name="Capetec", active_ingredient_ids=["captan"], section="fungicides")


class TestIdenticalCatalogues:
    def test_no_changes(self):
        cat = _make_catalogue(ais=[_AI], products=[_TP])
        result = diff_seasons(cat, cat)
        assert len(result) == 0


class TestDirectionality:
    def test_diff_is_directional(self):
        prev = _make_catalogue(season="2024-2025", ais=[_AI], products=[_TP])
        curr = _make_catalogue(season="2025-2026", ais=[_AI], products=[
            _TP,
            TradeProduct(id="new-prod", name="New Prod", active_ingredient_ids=["captan"], section="fungicides"),
        ])
        result = diff_seasons(prev, curr)
        added = [e for e in result if e.change_type == "product_added"]
        assert len(added) == 1
        assert added[0].entity_id == "new-prod"
        assert added[0].season_from == "2024-2025"
        assert added[0].season_to == "2025-2026"


class TestProductChanges:
    def test_product_added(self):
        prev = _make_catalogue(season="2024-2025", ais=[_AI], products=[_TP])
        new_tp = TradeProduct(id="new-prod", name="New Prod", active_ingredient_ids=["captan"], section="fungicides")
        curr = _make_catalogue(season="2025-2026", ais=[_AI], products=[_TP, new_tp])

        result = diff_seasons(prev, curr)
        added = [e for e in result if e.change_type == "product_added"]
        assert len(added) == 1
        assert added[0].severity == "info"
        assert added[0].entity_type == "product"

    def test_product_removed(self):
        prev = _make_catalogue(season="2024-2025", ais=[_AI], products=[_TP])
        curr = _make_catalogue(season="2025-2026", ais=[_AI], products=[])

        result = diff_seasons(prev, curr)
        removed = [e for e in result if e.change_type == "product_removed"]
        assert len(removed) == 1
        assert removed[0].severity == "critical"
        assert removed[0].entity_id == "capetec"


class TestPhiChanges:
    def test_phi_numeric_change(self):
        phi_old = PhiEntry(trade_product_id="capetec", market_code="NZ", phi=_make_phi_value(28), season="2024-2025")
        phi_new = PhiEntry(trade_product_id="capetec", market_code="NZ", phi=_make_phi_value(42), season="2025-2026")

        prev = _make_catalogue(season="2024-2025", ais=[_AI], products=[_TP], phi=[phi_old])
        curr = _make_catalogue(season="2025-2026", ais=[_AI], products=[_TP], phi=[phi_new])

        result = diff_seasons(prev, curr)
        phi_changes = [e for e in result if e.change_type == "phi_changed"]
        assert len(phi_changes) == 1
        assert phi_changes[0].old_value == "28 days"
        assert phi_changes[0].new_value == "42 days"
        assert phi_changes[0].market == "NZ"
        assert phi_changes[0].severity == "warning"

    def test_phi_to_npv_is_critical(self):
        phi_old = PhiEntry(trade_product_id="capetec", market_code="NZ", phi=_make_phi_value(28), season="2024-2025")
        phi_new = PhiEntry(trade_product_id="capetec", market_code="NZ", phi=_make_phi_value_code("NPV"), season="2025-2026")

        prev = _make_catalogue(season="2024-2025", ais=[_AI], products=[_TP], phi=[phi_old])
        curr = _make_catalogue(season="2025-2026", ais=[_AI], products=[_TP], phi=[phi_new])

        result = diff_seasons(prev, curr)
        phi_changes = [e for e in result if e.change_type == "phi_changed"]
        assert len(phi_changes) == 1
        assert phi_changes[0].severity == "critical"

    def test_phi_el_to_numeric(self):
        phi_old = PhiEntry(trade_product_id="capetec", market_code="NZ", phi=_make_phi_value_el("EL18"), season="prev")
        phi_new = PhiEntry(trade_product_id="capetec", market_code="NZ", phi=_make_phi_value(28), season="curr")

        prev = _make_catalogue(season="prev", ais=[_AI], products=[_TP], phi=[phi_old])
        curr = _make_catalogue(season="curr", ais=[_AI], products=[_TP], phi=[phi_new])

        result = diff_seasons(prev, curr)
        assert len([e for e in result if e.change_type == "phi_changed"]) == 1

    def test_same_phi_no_change(self):
        phi = PhiEntry(trade_product_id="capetec", market_code="NZ", phi=_make_phi_value(28), season="s")
        prev = _make_catalogue(season="prev", ais=[_AI], products=[_TP], phi=[phi])
        curr = _make_catalogue(season="curr", ais=[_AI], products=[_TP], phi=[phi])

        result = diff_seasons(prev, curr)
        assert len([e for e in result if e.change_type == "phi_changed"]) == 0


class TestReiChanges:
    def test_rei_changed(self):
        old_rei = ReiEntry(trade_product_id="capetec", rei=_make_phi_value_code("WFD"))
        new_rei = ReiEntry(trade_product_id="capetec", rei=PhiValue(raw="6 days", value=6, unit="days"))

        prev = _make_catalogue(season="prev", ais=[_AI], products=[_TP], rei=[old_rei])
        curr = _make_catalogue(season="curr", ais=[_AI], products=[_TP], rei=[new_rei])

        result = diff_seasons(prev, curr)
        rei_changes = [e for e in result if e.change_type == "rei_changed"]
        assert len(rei_changes) == 1
        assert rei_changes[0].old_value == "WFD"
        assert rei_changes[0].new_value == "6 days"
        assert rei_changes[0].severity == "warning"


class TestWhpChanges:
    def test_whp_changed(self):
        old_whp = WhpEntry(trade_product_id="capetec", whp_type="sheep_slaughter", whp=_make_phi_value_code("nil"))
        new_whp = WhpEntry(trade_product_id="capetec", whp_type="sheep_slaughter", whp=PhiValue(raw="2 months", value=2, unit="months"))

        prev = _make_catalogue(season="prev", ais=[_AI], products=[_TP], whp=[old_whp])
        curr = _make_catalogue(season="curr", ais=[_AI], products=[_TP], whp=[new_whp])

        result = diff_seasons(prev, curr)
        whp_changes = [e for e in result if e.change_type == "whp_changed"]
        assert len(whp_changes) == 1
        assert whp_changes[0].field_changed == "sheep_slaughter"


class TestRmRuleChanges:
    def _make_rule(self, code="3/DMI", max_apps=2, non_consec=False, tank_mix=None, rule_text="Test rule."):
        return ResistanceManagementRule(
            rule_code=code, moa_group_name="DMI", category="fungicides",
            applicable_active_ingredients=["captan"], applicable_trade_products=["Capetec"],
            rule_text=rule_text, max_applications=max_apps,
            must_be_non_consecutive=non_consec, requires_tank_mix=tank_mix,
            season="s",
        )

    def test_rm_rule_added(self):
        prev = _make_catalogue(season="prev", rm_rules=[])
        curr = _make_catalogue(season="curr", rm_rules=[self._make_rule()])

        result = diff_seasons(prev, curr)
        added = [e for e in result if e.change_type == "rm_rule_added"]
        assert len(added) == 1
        assert added[0].severity == "info"

    def test_rm_rule_removed(self):
        prev = _make_catalogue(season="prev", rm_rules=[self._make_rule()])
        curr = _make_catalogue(season="curr", rm_rules=[])

        result = diff_seasons(prev, curr)
        removed = [e for e in result if e.change_type == "rm_rule_removed"]
        assert len(removed) == 1
        assert removed[0].severity == "critical"

    def test_rm_rule_tightened_max_apps(self):
        prev = _make_catalogue(season="prev", rm_rules=[self._make_rule(max_apps=3)])
        curr = _make_catalogue(season="curr", rm_rules=[self._make_rule(max_apps=2)])

        result = diff_seasons(prev, curr)
        tightened = [e for e in result if e.change_type == "rm_rule_tightened"]
        assert len(tightened) == 1
        assert tightened[0].field_changed == "max_applications"
        assert tightened[0].severity == "warning"

    def test_rm_rule_relaxed_max_apps(self):
        prev = _make_catalogue(season="prev", rm_rules=[self._make_rule(max_apps=1)])
        curr = _make_catalogue(season="curr", rm_rules=[self._make_rule(max_apps=2)])

        result = diff_seasons(prev, curr)
        relaxed = [e for e in result if e.change_type == "rm_rule_relaxed"]
        assert len(relaxed) == 1
        assert relaxed[0].severity == "info"

    def test_rm_rule_tightened_non_consecutive(self):
        prev = _make_catalogue(season="prev", rm_rules=[self._make_rule(non_consec=False)])
        curr = _make_catalogue(season="curr", rm_rules=[self._make_rule(non_consec=True)])

        result = diff_seasons(prev, curr)
        tightened = [e for e in result if e.change_type == "rm_rule_tightened"]
        assert len(tightened) == 1
        assert tightened[0].field_changed == "must_be_non_consecutive"

    def test_rm_rule_wording_only(self):
        prev = _make_catalogue(season="prev", rm_rules=[self._make_rule(rule_text="Old wording.")])
        curr = _make_catalogue(season="curr", rm_rules=[self._make_rule(rule_text="New wording.")])

        result = diff_seasons(prev, curr)
        wording = [e for e in result if e.change_type == "rm_rule_wording_changed"]
        assert len(wording) == 1
        assert wording[0].severity == "info"

    def test_rm_structural_change_not_wording(self):
        """If max_apps changes AND wording changes, only structural change reported."""
        prev = _make_catalogue(season="prev", rm_rules=[self._make_rule(max_apps=3, rule_text="Old.")])
        curr = _make_catalogue(season="curr", rm_rules=[self._make_rule(max_apps=2, rule_text="New.")])

        result = diff_seasons(prev, curr)
        assert len([e for e in result if e.change_type == "rm_rule_wording_changed"]) == 0
        assert len([e for e in result if e.change_type == "rm_rule_tightened"]) == 1


class TestFlaggedChanges:
    def _make_flagged(self, ai="clethodim"):
        return FlaggedProduct(active_ingredient=ai, issue="Flagged", season="s")

    def test_product_flagged(self):
        prev = _make_catalogue(season="prev", flagged=[])
        curr = _make_catalogue(season="curr", flagged=[self._make_flagged()])

        result = diff_seasons(prev, curr)
        flagged = [e for e in result if e.change_type == "product_flagged"]
        assert len(flagged) == 1
        assert flagged[0].severity == "warning"

    def test_product_unflagged(self):
        prev = _make_catalogue(season="prev", flagged=[self._make_flagged()])
        curr = _make_catalogue(season="curr", flagged=[])

        result = diff_seasons(prev, curr)
        unflagged = [e for e in result if e.change_type == "product_unflagged"]
        assert len(unflagged) == 1
        assert unflagged[0].severity == "info"


class TestActiveIngredientChanges:
    def test_ai_added(self):
        prev = _make_catalogue(season="prev", ais=[])
        curr = _make_catalogue(season="curr", ais=[_AI])

        result = diff_seasons(prev, curr)
        added = [e for e in result if e.change_type == "active_ingredient_added"]
        assert len(added) == 1
        assert added[0].severity == "info"

    def test_ai_removed(self):
        prev = _make_catalogue(season="prev", ais=[_AI])
        curr = _make_catalogue(season="curr", ais=[])

        result = diff_seasons(prev, curr)
        removed = [e for e in result if e.change_type == "active_ingredient_removed"]
        assert len(removed) == 1
        assert removed[0].severity == "critical"


class TestSeverityOrdering:
    def test_critical_first(self):
        """Entries are sorted: critical, then warning, then info."""
        prev = _make_catalogue(season="prev", ais=[_AI], products=[_TP])
        new_ai = ActiveIngredient(id="new-ai", name="new-ai", section="fungicides")
        new_tp = TradeProduct(id="new-tp", name="New TP", active_ingredient_ids=["new-ai"], section="fungicides")
        curr = _make_catalogue(season="curr", ais=[new_ai], products=[new_tp])

        result = diff_seasons(prev, curr)
        severities = [e.severity for e in result]
        # All critical entries should come before any warning/info
        assert severities == sorted(severities, key=lambda s: {"critical": 0, "warning": 1, "info": 2}[s])
