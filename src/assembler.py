"""Assembler — transforms raw parser results into a validated SeasonCatalogue.

Responsibilities:
1. Deduplicate active ingredients
2. Expand semicolon-separated trade names into individual products
3. Parse all interval values (PHI, REI, WHP) through phi_value_parser
4. Build RM rules from rm_table parser output
5. Build schedule changes and flagged products
6. Validate referential integrity
7. Sort deterministically for idempotent output
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone

from src.config import ALL_MARKET_CODES, PARSER_VERSION
from src.models import (
    ActiveIngredient,
    CatalogueStats,
    FlaggedProduct,
    PhiEntry,
    ReiEntry,
    ResistanceManagementRule,
    ScheduleChange,
    SeasonCatalogue,
    TradeProduct,
    WhpEntry,
)
from src.parsers.phi_table import PhiTableResult, PhiTableRow
from src.parsers.rm_table import RmTableResult
from src.parsers.changes_table import ChangesTableResult
from src.parsers.flagged_table import FlaggedTableResult
from src.utils.phi_value_parser import parse_phi_value
from src.utils.rule_text_parser import (
    extract_max_applications,
    extract_non_consecutive,
    extract_tank_mix_requirement,
)
from src.utils.rm_code_normalizer import normalize_rm_code
from src.utils.slugify import make_slug

logger = logging.getLogger(__name__)


def _clean_trade_name(raw: str) -> str:
    """Clean a single trade name: rejoin hyphenated line breaks, strip whitespace."""
    # Rejoin hyphenated wraps: "Hort-\ncare" → "Hortcare"
    text = re.sub(r"(\w)-\n(\w)", r"\1\2", raw)
    # Collapse newlines to spaces
    text = text.replace("\n", " ")
    # Collapse whitespace
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _split_trade_names(raw: str) -> list[str]:
    """Split semicolon-separated trade names and clean each one."""
    # First rejoin hyphenated wraps across the whole string
    cleaned = re.sub(r"(\w)-\n(\w)", r"\1\2", raw)
    parts = cleaned.split(";")
    result = []
    for part in parts:
        name = part.replace("\n", " ")
        name = re.sub(r"\s+", " ", name).strip()
        if name:
            result.append(name)
    return result


def _parse_ai_name(raw: str) -> tuple[str, int | None]:
    """Parse an active ingredient name, stripping asterisks.

    Returns (cleaned_name, restriction_level).
    restriction_level: ***=3, **=2, *=1, None if no asterisks.
    """
    # Count trailing asterisks
    stripped = raw.rstrip()
    asterisk_count = 0
    while stripped.endswith("*"):
        asterisk_count += 1
        stripped = stripped[:-1]

    # Clean the name: rejoin hyphenated wraps, collapse whitespace
    name = re.sub(r"(\w)-\n(\w)", r"\1\2", stripped)
    name = name.replace("\n", " ")
    name = re.sub(r"\s+", " ", name).strip()

    return name, asterisk_count if asterisk_count > 0 else None


def _split_rm_codes(raw: str | None) -> list[str]:
    """Split RM rule codes from the raw column text.

    Handles: "M03/dithiocarba-\\nmate", "12/PP & 7/SDHI", "NC/NC"
    """
    if not raw:
        return []

    # Rejoin hyphenated line breaks
    text = re.sub(r"(\w)-\n(\w)", r"\1\2", raw)
    text = text.replace("\n", " ")
    text = re.sub(r"\s+", " ", text).strip()

    # Split on " & " (used to separate multiple codes like "12/PP & 7/SDHI")
    parts = re.split(r"\s*&\s*", text)
    return [normalize_rm_code(p.strip()) for p in parts if p.strip()]


def assemble_catalogue(
    phi_result: PhiTableResult,
    rm_result: RmTableResult | None = None,
    changes_result: ChangesTableResult | None = None,
    flagged_result: FlaggedTableResult | None = None,
    *,
    season: str,
    source_pdf: str,
    source_hash: str,
) -> SeasonCatalogue:
    """Transform raw parser output into a validated SeasonCatalogue."""
    warnings: list[str] = list(phi_result.warnings)
    if rm_result:
        warnings.extend(rm_result.warnings)
    if changes_result:
        warnings.extend(changes_result.warnings)
    if flagged_result:
        warnings.extend(flagged_result.warnings)

    # --- 1. Deduplicate active ingredients ---
    ai_map: dict[str, ActiveIngredient] = {}  # slug -> AI
    row_ai_slug: dict[int, str] = {}  # row index -> AI slug

    for i, row in enumerate(phi_result.rows):
        name, restriction = _parse_ai_name(row.active_ingredient_raw)
        slug = make_slug(name)
        if slug not in ai_map:
            ai_map[slug] = ActiveIngredient(
                id=slug,
                name=name,
                section=row.section,
                restriction_level=restriction,
            )
        row_ai_slug[i] = slug

    # --- 2. Expand trade products ---
    trade_products: list[TradeProduct] = []
    tp_slugs: set[str] = set()

    for i, row in enumerate(phi_result.rows):
        ai_slug = row_ai_slug[i]
        names = _split_trade_names(row.trade_names_raw)
        rm_codes = _split_rm_codes(row.rm_rule_codes_raw)

        # Clean notes: rejoin hyphenated wraps, preserve content
        notes = None
        if row.notes:
            notes = re.sub(r"(\w)-\n(\w)", r"\1\2", row.notes)
            notes = notes.replace("\n", " ")
            notes = re.sub(r"\s+", " ", notes).strip()

        label_claim = None
        if row.label_claim:
            label_claim = re.sub(r"(\w)-\n(\w)", r"\1\2", row.label_claim)
            label_claim = label_claim.replace("\n", " ")
            label_claim = re.sub(r"\s+", " ", label_claim).strip()

        for name in names:
            slug = make_slug(name)

            # Handle duplicate slugs (same product appearing in multiple sections)
            if slug in tp_slugs:
                # Append section suffix to disambiguate
                slug = f"{slug}-{row.section}"
                if slug in tp_slugs:
                    warnings.append(f"Duplicate trade product slug even with section suffix: {slug}")
                    continue

            tp_slugs.add(slug)

            # Determine AI links — some rows have compound AIs like "mancozeb + metalaxyl-m"
            ai_ids = [ai_slug]
            if "+" in row.active_ingredient_raw:
                # Parse additional AIs from compound name
                parts = row.active_ingredient_raw.split("+")
                ai_ids = []
                for part in parts:
                    part_name, _ = _parse_ai_name(part.strip())
                    part_slug = make_slug(part_name)
                    if part_slug not in ai_map:
                        ai_map[part_slug] = ActiveIngredient(
                            id=part_slug,
                            name=part_name,
                            section=row.section,
                        )
                    ai_ids.append(part_slug)

            tp = TradeProduct(
                id=slug,
                name=name,
                active_ingredient_ids=sorted(set(ai_ids)),
                label_claim=label_claim,
                notes=notes,
                section=row.section,
                rm_rule_codes=rm_codes,
            )
            trade_products.append(tp)

            # --- 3. Build PHI entries ---
            _build_phi_entries(tp, row, season, warnings)

    # Build all entry lists
    phi_entries: list[PhiEntry] = []
    rei_entries: list[ReiEntry] = []
    whp_entries: list[WhpEntry] = []

    for i, row in enumerate(phi_result.rows):
        names = _split_trade_names(row.trade_names_raw)

        for name in names:
            slug = make_slug(name)
            # Check if we used the section-suffixed slug
            if slug not in tp_slugs and f"{slug}-{row.section}" in tp_slugs:
                slug = f"{slug}-{row.section}"
            if slug not in tp_slugs:
                continue

            # PHI entries
            for market_code, raw_value in row.phi_values.items():
                parsed = parse_phi_value(raw_value)
                if parsed is not None:
                    phi_entries.append(PhiEntry(
                        trade_product_id=slug,
                        market_code=market_code,
                        phi=parsed,
                        season=season,
                    ))

            # REI entry
            rei_parsed = parse_phi_value(row.rei_raw)
            if rei_parsed is not None:
                rei_entries.append(ReiEntry(
                    trade_product_id=slug,
                    rei=rei_parsed,
                ))

            # WHP entries
            whp_slaughter = parse_phi_value(row.whp_slaughter_raw)
            if whp_slaughter is not None:
                whp_entries.append(WhpEntry(
                    trade_product_id=slug,
                    whp_type="sheep_slaughter",
                    whp=whp_slaughter,
                ))

            whp_grazing = parse_phi_value(row.whp_grazing_raw)
            if whp_grazing is not None:
                whp_entries.append(WhpEntry(
                    trade_product_id=slug,
                    whp_type="sheep_grazing",
                    whp=whp_grazing,
                ))

    # --- Build RM rules ---
    rm_rules = _build_rm_rules(rm_result, season) if rm_result else []

    # --- Build schedule changes ---
    schedule_changes = _build_schedule_changes(changes_result, season) if changes_result else []

    # --- Build flagged products ---
    flagged_products = _build_flagged_products(flagged_result, season) if flagged_result else []

    # --- Sort everything deterministically ---
    active_ingredients = sorted(ai_map.values(), key=lambda x: x.id)
    trade_products.sort(key=lambda x: x.id)
    phi_entries.sort(key=lambda x: (x.trade_product_id, x.market_code))
    rei_entries.sort(key=lambda x: x.trade_product_id)
    whp_entries.sort(key=lambda x: (x.trade_product_id, x.whp_type))
    rm_rules.sort(key=lambda x: x.rule_code)
    schedule_changes.sort(key=lambda x: (x.topic, x.active_ingredient or ""))
    flagged_products.sort(key=lambda x: x.active_ingredient)

    # --- Compute stats ---
    products_by_section: dict[str, int] = {}
    for tp in trade_products:
        products_by_section[tp.section] = products_by_section.get(tp.section, 0) + 1

    stats = CatalogueStats(
        total_active_ingredients=len(active_ingredients),
        total_trade_products=len(trade_products),
        total_phi_entries=len(phi_entries),
        total_rei_entries=len(rei_entries),
        total_whp_entries=len(whp_entries),
        total_rm_rules=len(rm_rules),
        total_schedule_changes=len(schedule_changes),
        total_flagged_products=len(flagged_products),
        products_by_section=products_by_section,
        markets=ALL_MARKET_CODES,
        parse_warnings=warnings,
    )

    # --- Assemble catalogue ---
    catalogue = SeasonCatalogue(
        season=season,
        source_pdf=source_pdf,
        source_hash=source_hash,
        parsed_at=datetime.now(timezone.utc).isoformat(),
        parser_version=PARSER_VERSION,
        active_ingredients=active_ingredients,
        trade_products=trade_products,
        phi_entries=phi_entries,
        rei_entries=rei_entries,
        whp_entries=whp_entries,
        resistance_management_rules=rm_rules,
        schedule_changes=schedule_changes,
        flagged_products=flagged_products,
        stats=stats,
    )

    logger.info(
        "Assembled catalogue: %d AIs, %d products, %d PHI, %d REI, %d WHP, "
        "%d RM rules, %d changes, %d flagged",
        len(active_ingredients),
        len(trade_products),
        len(phi_entries),
        len(rei_entries),
        len(whp_entries),
        len(rm_rules),
        len(schedule_changes),
        len(flagged_products),
    )

    return catalogue


def _clean_multiline(text: str | None) -> str | None:
    """Rejoin hyphenated wraps, collapse whitespace."""
    if not text:
        return None
    result = re.sub(r"(\w)-\n(\w)", r"\1\2", text)
    result = result.replace("\n", " ")
    result = re.sub(r"\s+", " ", result).strip()
    return result if result else None


def _split_on_commas(text: str | None) -> list[str]:
    """Split text on commas/semicolons, clean each part."""
    if not text:
        return []
    cleaned = _clean_multiline(text) or ""
    parts = re.split(r"[,;]", cleaned)
    return [p.strip() for p in parts if p.strip()]


def _parse_moa_group(code: str) -> str:
    """Extract MOA group name from RM code. E.g. '3/DMI' → 'DMI', 'M03/dithiocarbamate' → 'dithiocarbamate'."""
    cleaned = _clean_multiline(code) or code
    if "/" in cleaned:
        return cleaned.split("/", 1)[1].strip()
    return cleaned


def _build_rm_rules(rm_result: RmTableResult, season: str) -> list[ResistanceManagementRule]:
    """Build ResistanceManagementRule entries from parsed RM table rows."""
    rules: list[ResistanceManagementRule] = []

    for row in rm_result.rows:
        code = normalize_rm_code(_clean_multiline(row.code_raw) or row.code_raw)
        rule_text = _clean_multiline(row.rule_text)
        additional_notes = _clean_multiline(row.additional_notes)

        # Parse active ingredients and trade products
        ais = _split_on_commas(row.active_ingredients_raw)
        tps_raw = _clean_multiline(row.trade_products_raw) or ""
        trade_products = [p.strip() for p in tps_raw.split(";") if p.strip()]

        rule = ResistanceManagementRule(
            rule_code=code,
            moa_group_name=_parse_moa_group(code),
            category=row.section,
            applicable_active_ingredients=ais,
            applicable_trade_products=trade_products,
            rule_text=rule_text,
            additional_notes=additional_notes,
            max_applications=extract_max_applications(rule_text),
            must_be_non_consecutive=extract_non_consecutive(rule_text),
            requires_tank_mix=extract_tank_mix_requirement(rule_text),
            season=season,
        )
        rules.append(rule)

    return rules


def _build_schedule_changes(
    changes_result: ChangesTableResult, season: str
) -> list[ScheduleChange]:
    """Build ScheduleChange entries from parsed changes table rows."""
    changes: list[ScheduleChange] = []

    for row in changes_result.rows:
        trade_products_raw = _clean_multiline(row.trade_products_raw) or ""
        trade_products = [p.strip() for p in trade_products_raw.split(";") if p.strip()]

        change = ScheduleChange(
            topic=row.topic,
            active_ingredient=_clean_multiline(row.active_ingredient),
            trade_products=trade_products,
            explanation=_clean_multiline(row.explanation),
            season=season,
        )
        changes.append(change)

    return changes


def _build_flagged_products(
    flagged_result: FlaggedTableResult, season: str
) -> list[FlaggedProduct]:
    """Build FlaggedProduct entries from parsed flagged table rows."""
    products: list[FlaggedProduct] = []

    for row in flagged_result.rows:
        trade_products_raw = _clean_multiline(row.trade_products_raw) or ""
        trade_products = [p.strip() for p in trade_products_raw.split(";") if p.strip()]

        product = FlaggedProduct(
            active_ingredient=row.active_ingredient,
            trade_products=trade_products,
            label_claim=_clean_multiline(row.label_claim),
            issue=row.issue,
            season=season,
        )
        products.append(product)

    return products


def _build_phi_entries(
    tp: TradeProduct,
    row: PhiTableRow,
    season: str,
    warnings: list[str],
) -> None:
    """No-op stub — actual PHI building happens in the main assemble loop."""
    pass
