"""ACVM enrichment — merges ACVM register data into the catalogue.

This is additive-only: it fills fields that were previously None.
It never overwrites spray schedule data (PHI, REI, WHP, RM rules).
"""

from __future__ import annotations

import logging
from datetime import datetime

from src.models import (
    CatalogueStats,
    SeasonCatalogue,
    TradeProduct,
    TradeProductActiveIngredient,
)
from src.parsers.acvm_csv import AcvmProduct
from src.stages.match_acvm import MatchResult

logger = logging.getLogger(__name__)


def _parse_acvm_date(date_str: str) -> str | None:
    """Convert DD/MM/YYYY to ISO date string YYYY-MM-DD."""
    if not date_str:
        return None
    try:
        dt = datetime.strptime(date_str, "%d/%m/%Y")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        return None


def enrich_catalogue_with_acvm(
    catalogue: SeasonCatalogue,
    match_result: MatchResult,
) -> SeasonCatalogue:
    """Create a new SeasonCatalogue with ACVM data merged into trade products.

    Since all models are frozen, this creates new TradeProduct instances
    with the ACVM fields populated.
    """
    enriched_products: list[TradeProduct] = []
    ai_ingredients: list[TradeProductActiveIngredient] = []
    matched_count = 0

    for tp in catalogue.trade_products:
        acvm = match_result.matches.get(tp.id)
        if acvm:
            matched_count += 1
            # Create enriched product with ACVM fields
            enriched = TradeProduct(
                id=tp.id,
                name=tp.name,
                active_ingredient_ids=tp.active_ingredient_ids,
                label_claim=tp.label_claim,
                notes=tp.notes,
                section=tp.section,
                rm_rule_codes=tp.rm_rule_codes,
                acvm_registration_no=acvm.registration_no,
                acvm_registration_date=_parse_acvm_date(acvm.registration_date),
                registrant=acvm.registrant,
            )
            enriched_products.append(enriched)

            # Build ingredient concentration data
            for acvm_ing in acvm.ingredients:
                ai_ingredients.append(TradeProductActiveIngredient(
                    active_ingredient_id=acvm_ing.name.lower(),
                    concentration_value=acvm_ing.content,
                    concentration_unit=acvm_ing.unit,
                ))
        else:
            enriched_products.append(tp)

    # Build new stats
    new_stats = CatalogueStats(
        total_active_ingredients=catalogue.stats.total_active_ingredients,
        total_trade_products=catalogue.stats.total_trade_products,
        total_phi_entries=catalogue.stats.total_phi_entries,
        total_rei_entries=catalogue.stats.total_rei_entries,
        total_whp_entries=catalogue.stats.total_whp_entries,
        total_rm_rules=catalogue.stats.total_rm_rules,
        total_schedule_changes=catalogue.stats.total_schedule_changes,
        total_flagged_products=catalogue.stats.total_flagged_products,
        total_changelog_entries=catalogue.stats.total_changelog_entries,
        acvm_match_count=matched_count,
        acvm_total_products=len(catalogue.trade_products),
        products_by_section=catalogue.stats.products_by_section,
        markets=catalogue.stats.markets,
        parse_warnings=catalogue.stats.parse_warnings,
    )

    # Build new catalogue with enriched products
    return SeasonCatalogue(
        season=catalogue.season,
        source_pdf=catalogue.source_pdf,
        source_hash=catalogue.source_hash,
        parsed_at=catalogue.parsed_at,
        parser_version=catalogue.parser_version,
        active_ingredients=catalogue.active_ingredients,
        trade_products=enriched_products,
        phi_entries=catalogue.phi_entries,
        rei_entries=catalogue.rei_entries,
        whp_entries=catalogue.whp_entries,
        resistance_management_rules=catalogue.resistance_management_rules,
        schedule_changes=catalogue.schedule_changes,
        flagged_products=catalogue.flagged_products,
        ppm_entries=catalogue.ppm_entries,
        label_documents=catalogue.label_documents,
        changelog=catalogue.changelog,
        epa_classifications=catalogue.epa_classifications,
        stats=new_stats,
    )
