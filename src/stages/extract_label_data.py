"""Label data extraction orchestrator — processes all downloaded label PDFs.

For each product with a label, runs all field extractors, assesses confidence,
and optionally cross-validates against spray schedule data.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

from src.models import LabelExtractedData, SeasonCatalogue
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
    extract_rei_raw,
    extract_shelf_life,
    extract_signal_word,
    extract_tank_mix_info,
    extract_target_rates,
    extract_whp_raw,
)
from src.parsers.label_text_extractor import extract_label_text
from src.utils.storage_class import derive_storage_class

logger = logging.getLogger(__name__)


def _assess_confidence(data: LabelExtractedData) -> str:
    """Rate extraction confidence based on how many fields were populated."""
    score = 0
    if data.active_ingredients:
        score += 2
    if data.target_rates:
        score += 2
    if data.container_sizes:
        score += 1
    if data.rainfastness_hours is not None:
        score += 1
    if data.max_applications_per_season is not None:
        score += 1
    if data.growth_stage_earliest or data.growth_stage_latest:
        score += 1
    if data.tank_mix_incompatible or data.tank_mix_required:
        score += 1
    if data.label_buffer_zone_m is not None:
        score += 1
    if data.ppe_requirements:
        score += 1
    if data.environmental_cautions:
        score += 1
    if data.hsr_number:
        score += 1
    if data.hsno_classifications:
        score += 1
    if data.signal_word:
        score += 1

    if score >= 8:
        return "high"
    if score >= 4:
        return "medium"
    return "low"


def extract_single_label(
    pdf_path: Path,
    trade_product_id: str,
    registration_no: str,
) -> LabelExtractedData | None:
    """Extract structured data from a single label PDF.

    All extractors are fault-tolerant — individual failures don't block others.
    """
    text = extract_label_text(pdf_path)
    if not text:
        return None

    # Run all extractors with individual error handling
    active_ingredients = _safe(lambda: extract_active_ingredients(text), [], "active_ingredients", pdf_path.name)
    target_rates = _safe(lambda: extract_target_rates(text), [], "target_rates", pdf_path.name)
    container_sizes = _safe(lambda: extract_container_sizes(text), [], "container_sizes", pdf_path.name)

    rainfastness_hours, rainfastness_raw = _safe(lambda: extract_rainfastness(text), (None, None), "rainfastness", pdf_path.name)
    max_apps, max_apps_raw = _safe(lambda: extract_max_applications(text), (None, None), "max_apps", pdf_path.name)
    gs_earliest, gs_latest, gs_raw = _safe(lambda: extract_growth_stage_restrictions(text), (None, None, None), "growth_stages", pdf_path.name)
    incompatible, required, tank_raw = _safe(lambda: extract_tank_mix_info(text), ([], [], None), "tank_mix", pdf_path.name)
    buffer_m, buffer_raw = _safe(lambda: extract_buffer_zone(text), (None, None), "buffer", pdf_path.name)
    shelf_years, shelf_raw = _safe(lambda: extract_shelf_life(text), (None, None), "shelf_life", pdf_path.name)
    ppe = _safe(lambda: extract_ppe(text), [], "ppe", pdf_path.name)
    env = _safe(lambda: extract_environmental_cautions(text), [], "environmental", pdf_path.name)
    whp_raw = _safe(lambda: extract_whp_raw(text), None, "whp_raw", pdf_path.name)
    rei_raw = _safe(lambda: extract_rei_raw(text), None, "rei_raw", pdf_path.name)

    # Regulatory extractors (Phase 7)
    hsr_number, hsr_raw = _safe(lambda: extract_hsr_number(text), (None, None), "hsr_number", pdf_path.name)
    hsno_class_list, hsno_raw = _safe(lambda: extract_hsno_classifications(text), ([], None), "hsno_classifications", pdf_path.name)
    signal_word_val, _ = _safe(lambda: extract_signal_word(text), (None, None), "signal_word", pdf_path.name)
    acvm_from_label, _ = _safe(lambda: extract_acvm_number_from_label(text), (None, None), "acvm_label", pdf_path.name)
    storage = _safe(lambda: derive_storage_class(hsno_class_list), None, "storage_class", pdf_path.name)

    data = LabelExtractedData(
        trade_product_id=trade_product_id,
        registration_no=registration_no,
        source_filename=pdf_path.name,
        active_ingredients=active_ingredients,
        rainfastness_hours=rainfastness_hours,
        rainfastness_raw=rainfastness_raw,
        target_rates=target_rates,
        max_applications_per_season=max_apps,
        max_applications_raw=max_apps_raw,
        growth_stage_earliest=gs_earliest,
        growth_stage_latest=gs_latest,
        growth_stage_restriction_raw=gs_raw,
        tank_mix_incompatible=incompatible,
        tank_mix_required=required,
        tank_mix_raw=tank_raw,
        label_buffer_zone_m=buffer_m,
        label_buffer_zone_raw=buffer_raw,
        ppe_requirements=ppe,
        environmental_cautions=env,
        container_sizes=container_sizes,
        shelf_life_years=shelf_years,
        shelf_life_raw=shelf_raw,
        label_whp_raw=whp_raw,
        label_rei_raw=rei_raw,
        hsr_number=hsr_number,
        hsr_number_raw=hsr_raw,
        hsno_classifications=hsno_class_list,
        hsno_classifications_raw=hsno_raw,
        signal_word=signal_word_val,
        acvm_number_from_label=acvm_from_label,
        storage_class=storage,
    )

    # Set confidence (need to reconstruct since frozen)
    confidence = _assess_confidence(data)
    return LabelExtractedData(**{**data.model_dump(), "extraction_confidence": confidence})


def _safe(fn, default, field_name, filename):
    """Run an extractor safely, returning default on failure."""
    try:
        return fn()
    except Exception as e:
        logger.warning("Extractor '%s' failed for %s: %s", field_name, filename, e)
        return default


def extract_all_labels(
    labels_dir: Path,
    manifest: dict,
    catalogue: SeasonCatalogue,
) -> list[LabelExtractedData]:
    """Extract structured data from all downloaded label PDFs.

    Args:
        labels_dir: Directory containing label PDFs.
        manifest: Label manifest from manifest.json.
        catalogue: Current season catalogue (for product slug mapping).

    Returns:
        List of LabelExtractedData for all successfully processed labels.
    """
    # Build slug → reg_no mapping from catalogue
    slug_to_reg: dict[str, str] = {}
    for tp in catalogue.trade_products:
        if tp.acvm_registration_no:
            slug_to_reg[tp.id] = tp.acvm_registration_no

    # Also build reg_no → slug reverse mapping
    reg_to_slug: dict[str, str] = {v: k for k, v in slug_to_reg.items()}

    results: list[LabelExtractedData] = []
    processed = 0
    failed = 0

    for reg_no, entry in manifest.items():
        current = next((v for v in entry.get("versions", []) if v.get("is_current")), None)
        if not current:
            continue

        pdf_path = labels_dir / reg_no / current["filename"]
        if not pdf_path.exists():
            continue

        slug = reg_to_slug.get(reg_no, reg_no)

        data = extract_single_label(pdf_path, slug, reg_no)
        if data:
            results.append(data)
            processed += 1
        else:
            failed += 1

    # Log coverage
    with_ais = sum(1 for r in results if r.active_ingredients)
    with_rates = sum(1 for r in results if r.target_rates)
    with_containers = sum(1 for r in results if r.container_sizes)
    with_rainfast = sum(1 for r in results if r.rainfastness_hours is not None)
    with_hsr = sum(1 for r in results if r.hsr_number)
    with_classifications = sum(1 for r in results if r.hsno_classifications)
    with_signal = sum(1 for r in results if r.signal_word)

    logger.info(
        "Label extraction: %d processed, %d failed. "
        "AIs: %d, Rates: %d, Containers: %d, HSR: %d, Classifications: %d, Signal: %d",
        processed, failed, with_ais, with_rates, with_containers,
        with_hsr, with_classifications, with_signal,
    )

    # Sort deterministically
    results.sort(key=lambda r: r.trade_product_id)
    return results
