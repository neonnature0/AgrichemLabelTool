"""Season diff engine — compares two SeasonCatalogues and produces a structured changelog.

Pure function: two catalogues in, changelog out. No I/O, no side effects.
Performance: O(n) per entity type via dict lookups built upfront.
"""

from __future__ import annotations

from src.models import (
    ChangelogEntry,
    FlaggedProduct,
    PhiEntry,
    PhiValue,
    ReiEntry,
    ResistanceManagementRule,
    SeasonCatalogue,
    WhpEntry,
)


def diff_seasons(
    previous: SeasonCatalogue,
    current: SeasonCatalogue,
) -> list[ChangelogEntry]:
    """Compare two season catalogues and return all detected changes.

    Args:
        previous: The earlier season's catalogue.
        current: The later season's catalogue.

    Returns:
        Sorted list of ChangelogEntry objects describing every difference.
    """
    season_from = previous.season
    season_to = current.season

    entries: list[ChangelogEntry] = []
    entries.extend(_diff_active_ingredients(previous, current, season_from, season_to))
    entries.extend(_diff_trade_products(previous, current, season_from, season_to))
    entries.extend(_diff_phi_entries(previous, current, season_from, season_to))
    entries.extend(_diff_rei_entries(previous, current, season_from, season_to))
    entries.extend(_diff_whp_entries(previous, current, season_from, season_to))
    entries.extend(_diff_rm_rules(previous, current, season_from, season_to))
    entries.extend(_diff_flagged_products(previous, current, season_from, season_to))

    # Sort: critical first, then warning, then info; then by entity_type and entity_id
    severity_order = {"critical": 0, "warning": 1, "info": 2}
    entries.sort(key=lambda e: (
        severity_order.get(e.severity, 3),
        e.entity_type,
        e.entity_id or "",
        e.market or "",
    ))

    return entries


def _phi_value_summary(pv: PhiValue) -> str:
    """Create a concise string summary of a PhiValue for old_value/new_value fields."""
    if pv.code and pv.secondary_el_stage:
        return f"{pv.code} {pv.secondary_el_stage}"
    if pv.code:
        return pv.code
    if pv.el_stage and pv.el_offset_days:
        return f"{pv.el_stage}+{pv.el_offset_days}d"
    if pv.el_stage and pv.el_stage_end:
        return f"{pv.el_stage}-{pv.el_stage_end}"
    if pv.el_stage:
        return pv.el_stage
    if pv.value is not None and pv.unit:
        return f"{pv.value} {pv.unit}"
    if pv.value is not None:
        return str(pv.value)
    return pv.raw


def _phi_values_equal(a: PhiValue, b: PhiValue) -> bool:
    """Compare two PhiValues for equality. Uses raw string first for speed."""
    if a.raw == b.raw:
        return True
    # Structural comparison as fallback
    return (
        a.value == b.value
        and a.unit == b.unit
        and a.el_stage == b.el_stage
        and a.el_stage_end == b.el_stage_end
        and a.el_offset_days == b.el_offset_days
        and a.code == b.code
        and a.secondary_el_stage == b.secondary_el_stage
    )


# ---------------------------------------------------------------------------
# Active ingredients
# ---------------------------------------------------------------------------

def _diff_active_ingredients(
    prev: SeasonCatalogue, curr: SeasonCatalogue,
    season_from: str, season_to: str,
) -> list[ChangelogEntry]:
    entries: list[ChangelogEntry] = []
    prev_ids = {ai.id for ai in prev.active_ingredients}
    curr_ids = {ai.id for ai in curr.active_ingredients}

    for ai_id in curr_ids - prev_ids:
        ai = next(a for a in curr.active_ingredients if a.id == ai_id)
        entries.append(ChangelogEntry(
            change_type="active_ingredient_added",
            severity="info",
            entity_type="active_ingredient",
            entity_id=ai_id,
            description=f"New active ingredient: {ai.name} ({ai.section})",
            season_from=season_from, season_to=season_to,
        ))

    for ai_id in prev_ids - curr_ids:
        ai = next(a for a in prev.active_ingredients if a.id == ai_id)
        entries.append(ChangelogEntry(
            change_type="active_ingredient_removed",
            severity="critical",
            entity_type="active_ingredient",
            entity_id=ai_id,
            description=f"Active ingredient removed: {ai.name} ({ai.section})",
            season_from=season_from, season_to=season_to,
        ))

    return entries


# ---------------------------------------------------------------------------
# Trade products
# ---------------------------------------------------------------------------

def _diff_trade_products(
    prev: SeasonCatalogue, curr: SeasonCatalogue,
    season_from: str, season_to: str,
) -> list[ChangelogEntry]:
    entries: list[ChangelogEntry] = []
    prev_ids = {tp.id for tp in prev.trade_products}
    curr_ids = {tp.id for tp in curr.trade_products}

    for tp_id in curr_ids - prev_ids:
        tp = next(t for t in curr.trade_products if t.id == tp_id)
        entries.append(ChangelogEntry(
            change_type="product_added",
            severity="info",
            entity_type="product",
            entity_id=tp_id,
            description=f"New product: {tp.name} ({tp.section})",
            season_from=season_from, season_to=season_to,
        ))

    for tp_id in prev_ids - curr_ids:
        tp = next(t for t in prev.trade_products if t.id == tp_id)
        entries.append(ChangelogEntry(
            change_type="product_removed",
            severity="critical",
            entity_type="product",
            entity_id=tp_id,
            description=f"Product removed: {tp.name} ({tp.section})",
            season_from=season_from, season_to=season_to,
        ))

    return entries


# ---------------------------------------------------------------------------
# PHI entries
# ---------------------------------------------------------------------------

def _diff_phi_entries(
    prev: SeasonCatalogue, curr: SeasonCatalogue,
    season_from: str, season_to: str,
) -> list[ChangelogEntry]:
    entries: list[ChangelogEntry] = []

    prev_map: dict[tuple[str, str], PhiEntry] = {
        (p.trade_product_id, p.market_code): p for p in prev.phi_entries
    }
    curr_map: dict[tuple[str, str], PhiEntry] = {
        (p.trade_product_id, p.market_code): p for p in curr.phi_entries
    }

    # Only compare entries that exist in both seasons (added/removed products handled separately)
    common_keys = set(prev_map.keys()) & set(curr_map.keys())

    for key in common_keys:
        old_phi = prev_map[key]
        new_phi = curr_map[key]

        if not _phi_values_equal(old_phi.phi, new_phi.phi):
            tp_id, market = key
            old_summary = _phi_value_summary(old_phi.phi)
            new_summary = _phi_value_summary(new_phi.phi)

            # Detect NPV restriction (critical)
            severity = "warning"
            if new_phi.phi.code == "NPV" and old_phi.phi.code != "NPV":
                severity = "critical"

            entries.append(ChangelogEntry(
                change_type="phi_changed",
                severity=severity,
                entity_type="phi",
                entity_id=tp_id,
                market=market,
                field_changed="phi",
                old_value=old_summary,
                new_value=new_summary,
                description=f"PHI changed for {tp_id} in {market}: {old_summary} -> {new_summary}",
                season_from=season_from, season_to=season_to,
            ))

    return entries


# ---------------------------------------------------------------------------
# REI entries
# ---------------------------------------------------------------------------

def _diff_rei_entries(
    prev: SeasonCatalogue, curr: SeasonCatalogue,
    season_from: str, season_to: str,
) -> list[ChangelogEntry]:
    entries: list[ChangelogEntry] = []

    prev_map: dict[str, ReiEntry] = {r.trade_product_id: r for r in prev.rei_entries}
    curr_map: dict[str, ReiEntry] = {r.trade_product_id: r for r in curr.rei_entries}

    for tp_id in set(prev_map.keys()) & set(curr_map.keys()):
        old_rei = prev_map[tp_id]
        new_rei = curr_map[tp_id]

        if not _phi_values_equal(old_rei.rei, new_rei.rei):
            entries.append(ChangelogEntry(
                change_type="rei_changed",
                severity="warning",
                entity_type="rei",
                entity_id=tp_id,
                field_changed="rei",
                old_value=_phi_value_summary(old_rei.rei),
                new_value=_phi_value_summary(new_rei.rei),
                description=(
                    f"REI changed for {tp_id}: "
                    f"{_phi_value_summary(old_rei.rei)} -> {_phi_value_summary(new_rei.rei)}"
                ),
                season_from=season_from, season_to=season_to,
            ))

    return entries


# ---------------------------------------------------------------------------
# WHP entries
# ---------------------------------------------------------------------------

def _diff_whp_entries(
    prev: SeasonCatalogue, curr: SeasonCatalogue,
    season_from: str, season_to: str,
) -> list[ChangelogEntry]:
    entries: list[ChangelogEntry] = []

    prev_map: dict[tuple[str, str], WhpEntry] = {
        (w.trade_product_id, w.whp_type): w for w in prev.whp_entries
    }
    curr_map: dict[tuple[str, str], WhpEntry] = {
        (w.trade_product_id, w.whp_type): w for w in curr.whp_entries
    }

    for key in set(prev_map.keys()) & set(curr_map.keys()):
        old_whp = prev_map[key]
        new_whp = curr_map[key]

        if not _phi_values_equal(old_whp.whp, new_whp.whp):
            tp_id, whp_type = key
            entries.append(ChangelogEntry(
                change_type="whp_changed",
                severity="warning",
                entity_type="whp",
                entity_id=tp_id,
                field_changed=whp_type,
                old_value=_phi_value_summary(old_whp.whp),
                new_value=_phi_value_summary(new_whp.whp),
                description=(
                    f"WHP ({whp_type}) changed for {tp_id}: "
                    f"{_phi_value_summary(old_whp.whp)} -> {_phi_value_summary(new_whp.whp)}"
                ),
                season_from=season_from, season_to=season_to,
            ))

    return entries


# ---------------------------------------------------------------------------
# RM rules
# ---------------------------------------------------------------------------

def _diff_rm_rules(
    prev: SeasonCatalogue, curr: SeasonCatalogue,
    season_from: str, season_to: str,
) -> list[ChangelogEntry]:
    entries: list[ChangelogEntry] = []

    prev_map: dict[str, ResistanceManagementRule] = {
        r.rule_code: r for r in prev.resistance_management_rules
    }
    curr_map: dict[str, ResistanceManagementRule] = {
        r.rule_code: r for r in curr.resistance_management_rules
    }

    prev_codes = set(prev_map.keys())
    curr_codes = set(curr_map.keys())

    # Added rules
    for code in curr_codes - prev_codes:
        rule = curr_map[code]
        entries.append(ChangelogEntry(
            change_type="rm_rule_added",
            severity="info",
            entity_type="rm_rule",
            entity_id=code,
            description=f"New RM rule: {code} ({rule.category})",
            season_from=season_from, season_to=season_to,
        ))

    # Removed rules
    for code in prev_codes - curr_codes:
        rule = prev_map[code]
        entries.append(ChangelogEntry(
            change_type="rm_rule_removed",
            severity="critical",
            entity_type="rm_rule",
            entity_id=code,
            description=f"RM rule removed: {code} ({rule.category})",
            season_from=season_from, season_to=season_to,
        ))

    # Modified rules
    for code in prev_codes & curr_codes:
        old_rule = prev_map[code]
        new_rule = curr_map[code]
        entries.extend(_compare_rm_rule(old_rule, new_rule, season_from, season_to))

    return entries


def _compare_rm_rule(
    old: ResistanceManagementRule,
    new: ResistanceManagementRule,
    season_from: str,
    season_to: str,
) -> list[ChangelogEntry]:
    """Compare two RM rules with the same code, detect structural vs wording changes."""
    entries: list[ChangelogEntry] = []
    code = new.rule_code

    # Check structural fields
    structural_changed = False

    # max_applications: decreased = tightened, increased = relaxed
    if old.max_applications != new.max_applications:
        structural_changed = True
        if (new.max_applications is not None and old.max_applications is not None
                and new.max_applications < old.max_applications):
            change_type = "rm_rule_tightened"
            severity = "warning"
        elif new.max_applications is not None and old.max_applications is None:
            change_type = "rm_rule_tightened"
            severity = "warning"
        else:
            change_type = "rm_rule_relaxed"
            severity = "info"

        entries.append(ChangelogEntry(
            change_type=change_type,
            severity=severity,
            entity_type="rm_rule",
            entity_id=code,
            field_changed="max_applications",
            old_value=str(old.max_applications),
            new_value=str(new.max_applications),
            description=f"RM rule {code}: max applications {old.max_applications} -> {new.max_applications}",
            season_from=season_from, season_to=season_to,
        ))

    # must_be_non_consecutive: false→true = tightened
    if old.must_be_non_consecutive != new.must_be_non_consecutive:
        structural_changed = True
        if new.must_be_non_consecutive:
            change_type = "rm_rule_tightened"
            severity = "warning"
        else:
            change_type = "rm_rule_relaxed"
            severity = "info"

        entries.append(ChangelogEntry(
            change_type=change_type,
            severity=severity,
            entity_type="rm_rule",
            entity_id=code,
            field_changed="must_be_non_consecutive",
            old_value=str(old.must_be_non_consecutive),
            new_value=str(new.must_be_non_consecutive),
            description=f"RM rule {code}: non-consecutive requirement changed to {new.must_be_non_consecutive}",
            season_from=season_from, season_to=season_to,
        ))

    # requires_tank_mix: None→value = tightened
    if old.requires_tank_mix != new.requires_tank_mix:
        structural_changed = True
        if new.requires_tank_mix and not old.requires_tank_mix:
            change_type = "rm_rule_tightened"
            severity = "warning"
        elif not new.requires_tank_mix and old.requires_tank_mix:
            change_type = "rm_rule_relaxed"
            severity = "info"
        else:
            change_type = "rm_rule_tightened"
            severity = "warning"

        entries.append(ChangelogEntry(
            change_type=change_type,
            severity=severity,
            entity_type="rm_rule",
            entity_id=code,
            field_changed="requires_tank_mix",
            old_value=old.requires_tank_mix,
            new_value=new.requires_tank_mix,
            description=f"RM rule {code}: tank mix requirement changed",
            season_from=season_from, season_to=season_to,
        ))

    # Wording-only change: rule_text differs but no structural fields changed
    if not structural_changed and old.rule_text != new.rule_text:
        entries.append(ChangelogEntry(
            change_type="rm_rule_wording_changed",
            severity="info",
            entity_type="rm_rule",
            entity_id=code,
            field_changed="rule_text",
            old_value=(old.rule_text or "")[:200],
            new_value=(new.rule_text or "")[:200],
            description=f"RM rule {code}: rule text wording updated",
            season_from=season_from, season_to=season_to,
        ))

    return entries


# ---------------------------------------------------------------------------
# Flagged products
# ---------------------------------------------------------------------------

def _diff_flagged_products(
    prev: SeasonCatalogue, curr: SeasonCatalogue,
    season_from: str, season_to: str,
) -> list[ChangelogEntry]:
    entries: list[ChangelogEntry] = []

    prev_ais = {fp.active_ingredient for fp in prev.flagged_products}
    curr_ais = {fp.active_ingredient for fp in curr.flagged_products}

    for ai in curr_ais - prev_ais:
        entries.append(ChangelogEntry(
            change_type="product_flagged",
            severity="warning",
            entity_type="flagged",
            entity_id=ai,
            description=f"Product newly flagged: {ai}",
            season_from=season_from, season_to=season_to,
        ))

    for ai in prev_ais - curr_ais:
        entries.append(ChangelogEntry(
            change_type="product_unflagged",
            severity="info",
            entity_type="flagged",
            entity_id=ai,
            description=f"Product no longer flagged: {ai}",
            season_from=season_from, season_to=season_to,
        ))

    return entries
