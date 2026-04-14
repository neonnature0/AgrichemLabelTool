"""Pydantic v2 data models for the NZ product catalogue pipeline.

All models are frozen (immutable). The PhiValue decomposition mirrors the
existing pesticide_phi database table structure to ensure a clean Phase 8
migration path.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, model_validator


# ---------------------------------------------------------------------------
# Core value type — used for PHI, REI, and WHP entries
# ---------------------------------------------------------------------------

class PhiValue(BaseModel):
    """Structured representation of a single interval cell value.

    Mirrors the decomposed structure in product_catalogue.pesticide_phi:
    value + unit, el_stage + el_stage_end + el_offset_days, code, secondary_el_stage.
    """

    model_config = ConfigDict(frozen=True)

    raw: str
    value: int | None = None
    unit: str | None = None  # "days", "hrs", "months", "day"
    el_stage: str | None = None  # e.g. "EL18", "EL4"
    el_stage_end: str | None = None  # for ranges: "EL1 - EL2" → end is "EL2"
    el_offset_days: int | None = None  # for compounds: "EL4 + 14 days" → 14
    code: str | None = None  # "NPV", "SNC", "ID", "NS", "WFD", "DNG", etc.
    secondary_el_stage: str | None = None  # for "SFPT EL18" → "EL18"

    @model_validator(mode="after")
    def _must_have_at_least_one_value(self) -> PhiValue:
        if self.value is None and self.el_stage is None and self.code is None:
            raise ValueError(
                f"PhiValue must have at least one of value, el_stage, or code. "
                f"Raw: {self.raw!r}"
            )
        return self


# ---------------------------------------------------------------------------
# Active ingredient
# ---------------------------------------------------------------------------

class ActiveIngredient(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: str  # deterministic slug
    name: str  # cleaned name (no asterisks)
    section: str  # fungicides, herbicides, insecticides, wound_dressings
    restriction_level: int | None = None  # asterisk count: ***=3, **=2, *=1

    # MOA fields — populated in Phase 2 from RM table / ACVM
    frac_code: str | None = None
    irac_code: str | None = None
    hrac_code: str | None = None
    chemical_group: str | None = None
    moa_group_name: str | None = None
    is_multisite: bool = False


# ---------------------------------------------------------------------------
# Trade product
# ---------------------------------------------------------------------------

class TradeProduct(BaseModel):
    model_config = ConfigDict(frozen=True)

    id: str  # deterministic slug
    name: str
    active_ingredient_ids: list[str]
    label_claim: str | None = None
    notes: str | None = None  # preserved verbatim
    section: str
    rm_rule_codes: list[str] = []

    # ACVM Register data — populated in Phase 4
    acvm_registration_no: str | None = None
    acvm_registration_status: str = "unknown"
    acvm_expiry_date: str | None = None
    acvm_registration_date: str | None = None
    registrant: str | None = None
    formulation_type: str | None = None


# ---------------------------------------------------------------------------
# Interval entries — PHI, REI, WHP
# ---------------------------------------------------------------------------

class PhiEntry(BaseModel):
    model_config = ConfigDict(frozen=True)

    trade_product_id: str
    market_code: str
    phi: PhiValue
    season: str


class ReiEntry(BaseModel):
    model_config = ConfigDict(frozen=True)

    trade_product_id: str
    rei: PhiValue


class WhpEntry(BaseModel):
    model_config = ConfigDict(frozen=True)

    trade_product_id: str
    whp_type: Literal["sheep_slaughter", "sheep_grazing"]
    whp: PhiValue


# ---------------------------------------------------------------------------
# Placeholder models for later phases (empty lists in Phase 1 output)
# ---------------------------------------------------------------------------

class TradeProductActiveIngredient(BaseModel):
    """Junction model with concentration data — populated in Phase 4 (ACVM)."""

    model_config = ConfigDict(frozen=True)

    active_ingredient_id: str
    concentration_value: float | None = None
    concentration_unit: str | None = None  # g/kg, g/L
    concentration_basis: str = "w/w"


class ResistanceManagementRule(BaseModel):
    """Full RM rule parsed from the Resistance Management Table (Section 4.4)."""

    model_config = ConfigDict(frozen=True)

    rule_code: str  # e.g. "3/DMI", "4A/neonicotinoids"
    moa_group_name: str  # parsed from code after "/" e.g. "DMI"
    category: str  # fungicides, herbicides, insecticides, wound_dressings
    applicable_active_ingredients: list[str]
    applicable_trade_products: list[str]
    rule_text: str | None = None  # verbatim from table; may be None for some rows
    additional_notes: str | None = None
    max_applications: int | None = None
    must_be_non_consecutive: bool = False
    requires_tank_mix: str | None = None
    season: str


class ScheduleChange(BaseModel):
    """Entry from the Significant Changes Table (Section 4.1)."""

    model_config = ConfigDict(frozen=True)

    topic: str  # e.g. "Withdrawn products", "REI changes", "New products"
    active_ingredient: str | None = None
    trade_products: list[str] = []
    explanation: str | None = None
    season: str


class FlaggedProduct(BaseModel):
    """Entry from the Flagged Products Table (Section 4.2)."""

    model_config = ConfigDict(frozen=True)

    active_ingredient: str
    trade_products: list[str] = []
    label_claim: str | None = None
    issue: str
    season: str


class LabelActiveIngredient(BaseModel):
    """Active ingredient with concentration from the label (definitive source)."""

    model_config = ConfigDict(frozen=True)

    name: str  # "mancozeb", "pyraclostrobin"
    concentration_value: float | None = None
    concentration_unit: str | None = None  # "g/kg", "g/L"
    concentration_basis: str = "w/w"


class TargetRate(BaseModel):
    """Application rate for a specific target pest/disease from the label."""

    model_config = ConfigDict(frozen=True)

    target: str  # "Botrytis", "Powdery mildew"
    rate_value: str  # "150 g/100L", "1.0 kg/ha"
    rate_min: float | None = None
    rate_max: float | None = None
    rate_unit: str | None = None  # "g/100L", "mL/ha"
    water_volume: str | None = None
    growth_stage_window: str | None = None
    max_applications: int | None = None
    notes: str | None = None
    raw: str = ""


class LabelExtractedData(BaseModel):
    """Structured data extracted from a label PDF (Phase 6)."""

    model_config = ConfigDict(frozen=True)

    trade_product_id: str
    registration_no: str
    source_filename: str
    extraction_confidence: str = "low"  # high/medium/low

    active_ingredients: list[LabelActiveIngredient] = []
    rainfastness_hours: float | None = None
    rainfastness_raw: str | None = None
    target_rates: list[TargetRate] = []
    max_applications_per_season: int | None = None
    max_applications_raw: str | None = None
    growth_stage_earliest: str | None = None
    growth_stage_latest: str | None = None
    growth_stage_restriction_raw: str | None = None
    tank_mix_incompatible: list[str] = []
    tank_mix_required: list[str] = []
    tank_mix_raw: str | None = None
    label_buffer_zone_m: int | None = None
    label_buffer_zone_raw: str | None = None
    ppe_requirements: list[str] = []
    environmental_cautions: list[str] = []
    container_sizes: list[str] = []
    shelf_life_years: float | None = None
    shelf_life_raw: str | None = None
    label_whp_raw: str | None = None
    label_rei_raw: str | None = None

    # Regulatory (Phase 7)
    hsr_number: str | None = None  # e.g. "HSR007853"
    hsr_number_raw: str | None = None
    hsno_classifications: list[str] = []  # GHS text descriptions from label
    hsno_classifications_raw: str | None = None
    signal_word: str | None = None  # DANGER / WARNING / CAUTION
    acvm_number_from_label: str | None = None  # P-number for cross-validation
    storage_class: str | None = None  # derived from classifications


class PpmEntry(BaseModel):
    """PPM processing factor — populated in Phase 2."""

    model_config = ConfigDict(frozen=True)

    active_ingredient_id: str
    market: str
    grape_mrl_ppm: float | None = None
    processing_factor_red: float | None = None
    processing_factor_white: float | None = None
    wine_derived_limit_red: float | None = None
    wine_derived_limit_white: float | None = None
    season: str


class LabelDocument(BaseModel):
    """Label PDF reference — populated in Phase 4B."""

    model_config = ConfigDict(frozen=True)

    trade_product_id: str
    registration_no: str  # ACVM P-number
    document_type: str = "label"  # label / sds (future)
    filename: str  # display filename
    local_path: str  # relative to data/labels/
    file_hash_sha256: str
    label_date: str | None = None  # extracted from ACVM filename, e.g. "Apr 2025"
    downloaded_at: str  # ISO datetime
    is_current: bool = True


class ChangelogEntry(BaseModel):
    """Season-over-season change detected by the diff engine (Phase 3).

    change_type values:
        product_added, product_removed,
        phi_changed, rei_changed, whp_changed,
        rm_rule_added, rm_rule_removed, rm_rule_tightened, rm_rule_relaxed, rm_rule_wording_changed,
        active_ingredient_added, active_ingredient_removed,
        product_flagged, product_unflagged

    severity: critical / warning / info
    entity_type: product / phi / rei / whp / rm_rule / active_ingredient / flagged
    """

    model_config = ConfigDict(frozen=True)

    change_type: str
    severity: str = "info"  # critical / warning / info
    entity_type: str  # product / phi / rei / whp / rm_rule / active_ingredient / flagged
    entity_id: str | None = None  # slug of affected product/AI/rule
    market: str | None = None  # for PHI changes
    field_changed: str | None = None
    old_value: str | None = None
    new_value: str | None = None
    description: str
    season_from: str
    season_to: str


class EpaHazardClassification(BaseModel):
    """EPA hazard data — populated in Phase 7."""

    model_config = ConfigDict(frozen=True)

    trade_product_id: str
    epa_approval_no: str
    hsno_class: str
    hazard_description: str | None = None
    storage_class: str | None = None
    buffer_zone_m: int | None = None
    ppe_requirements: str | None = None


# ---------------------------------------------------------------------------
# Statistics
# ---------------------------------------------------------------------------

class CatalogueStats(BaseModel):
    model_config = ConfigDict(frozen=True)

    total_active_ingredients: int
    total_trade_products: int
    total_phi_entries: int
    total_rei_entries: int
    total_whp_entries: int
    total_rm_rules: int = 0
    total_schedule_changes: int = 0
    total_flagged_products: int = 0
    total_changelog_entries: int = 0
    acvm_match_count: int = 0
    acvm_total_products: int = 0
    total_label_extractions: int = 0
    products_by_section: dict[str, int]
    markets: list[str]
    parse_warnings: list[str] = []


# ---------------------------------------------------------------------------
# Top-level catalogue
# ---------------------------------------------------------------------------

class SeasonCatalogue(BaseModel):
    """Complete output of a pipeline run. One JSON file per season."""

    model_config = ConfigDict(frozen=True)

    season: str
    source_pdf: str
    source_hash: str  # SHA-256 of input PDF
    parsed_at: str  # ISO 8601
    parser_version: str

    active_ingredients: list[ActiveIngredient]
    trade_products: list[TradeProduct]
    phi_entries: list[PhiEntry]
    rei_entries: list[ReiEntry]
    whp_entries: list[WhpEntry]

    # Phase 2 entities
    resistance_management_rules: list[ResistanceManagementRule] = []
    schedule_changes: list[ScheduleChange] = []
    flagged_products: list[FlaggedProduct] = []

    # Phase 6: label extractions
    label_extractions: list[LabelExtractedData] = []

    # Placeholder lists — populated in later phases
    ppm_entries: list[PpmEntry] = []
    label_documents: list[LabelDocument] = []
    changelog: list[ChangelogEntry] = []
    epa_classifications: list[EpaHazardClassification] = []

    stats: CatalogueStats

    @model_validator(mode="after")
    def _check_referential_integrity(self) -> SeasonCatalogue:
        errors: list[str] = []
        ai_ids = {ai.id for ai in self.active_ingredients}
        tp_ids = {tp.id for tp in self.trade_products}

        for tp in self.trade_products:
            for ai_id in tp.active_ingredient_ids:
                if ai_id not in ai_ids:
                    errors.append(
                        f"TradeProduct {tp.id!r} references unknown "
                        f"active_ingredient_id {ai_id!r}"
                    )

        for phi in self.phi_entries:
            if phi.trade_product_id not in tp_ids:
                errors.append(
                    f"PhiEntry references unknown trade_product_id "
                    f"{phi.trade_product_id!r}"
                )

        for rei in self.rei_entries:
            if rei.trade_product_id not in tp_ids:
                errors.append(
                    f"ReiEntry references unknown trade_product_id "
                    f"{rei.trade_product_id!r}"
                )

        for whp in self.whp_entries:
            if whp.trade_product_id not in tp_ids:
                errors.append(
                    f"WhpEntry references unknown trade_product_id "
                    f"{whp.trade_product_id!r}"
                )

        if errors:
            raise ValueError(
                f"Referential integrity errors:\n" + "\n".join(f"  - {e}" for e in errors)
            )

        return self
