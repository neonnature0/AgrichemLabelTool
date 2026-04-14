"""Central configuration for the NZ catalogue pipeline."""

from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
INPUT_DIR = DATA_DIR / "input"
STAGING_DIR = DATA_DIR / "staging"
OUTPUT_DIR = DATA_DIR / "output"

# ---------------------------------------------------------------------------
# Parser version — embedded in every catalogue for reproducibility
# ---------------------------------------------------------------------------
PARSER_VERSION = "1.0.0"

# ---------------------------------------------------------------------------
# PHI table layout (2025/2026 schedule)
# ---------------------------------------------------------------------------

# 0-indexed PDF page range containing the PHI table
PHI_TABLE_PAGE_RANGE = (12, 22)

# Expected column count for the PHI table
PHI_TABLE_COLUMN_COUNT = 26

# Metadata columns (0-7)
COL_ACTIVE_INGREDIENT = 0
COL_TRADE_NAME = 1
COL_LABEL_CLAIM = 2
COL_REI = 3
COL_NOTES = 4
COL_RM_RULE_CODES = 5
COL_WHP_SLAUGHTER = 6
COL_WHP_GRAZING = 7

# Market PHI columns (8-25) — index → market code
# Codes match reference.export_markets.code in the Cordyn database
MARKET_COLUMNS: dict[int, str] = {
    8: "NIL",
    9: "NZ",
    10: "AU",
    11: "CA",
    12: "CA-ON",
    13: "CN",
    14: "EU",
    15: "GB",
    16: "HK",
    17: "IL",
    18: "JP",
    19: "KR",
    20: "MY",
    21: "SG",
    22: "CH",
    23: "TW",
    24: "TH",
    25: "US",
}

# All valid market codes (ordered)
ALL_MARKET_CODES = list(MARKET_COLUMNS.values())

# ---------------------------------------------------------------------------
# Sections
# ---------------------------------------------------------------------------
SECTIONS = {"FUNGICIDES", "HERBICIDES", "INSECTICIDES", "WOUND DRESSINGS"}

# ---------------------------------------------------------------------------
# Special PHI / interval codes
# ---------------------------------------------------------------------------
SPECIAL_CODES = {"NS", "SNC", "NPV", "ID", "WFD", "DNG", "SFPT", "N/A", "nil"}

# Header row sentinel — reversed text in col 8 due to rotated PDF headers
HEADER_SENTINEL = "EUDISER"

# ---------------------------------------------------------------------------
# ACVM Register
# ---------------------------------------------------------------------------
ACVM_CSV_URL = (
    "https://eatsafe.nzfsa.govt.nz/web/public/acvm-register"
    "?p_p_id=searchAcvm_WAR_aaol&p_p_lifecycle=0&p_p_state=exclusive"
    "&p_p_mode=view&p_p_col_id=column-2&p_p_col_count=1"
    "&_searchAcvm_WAR_aaol_action=document"
    "&_searchAcvm_WAR_aaol_form-submit=true"
    "&_searchAcvm_WAR_aaol_documentId=71436"
)
ACVM_CACHE_DIR = STAGING_DIR / "acvm_cache"
ACVM_CACHE_TTL_DAYS = 30
ACVM_FUZZY_THRESHOLD = 80
LABELS_DIR = DATA_DIR / "labels"

# ---------------------------------------------------------------------------
# Resistance Management table layout (2025/2026 schedule)
# ---------------------------------------------------------------------------
RM_TABLE_PAGE_RANGE = (9, 12)  # indices 9-12 (pages 10-13)
RM_TABLE_COLUMN_COUNT = 5
RM_HEADER_SENTINEL = "Code"  # col 0 of header rows contains "Code"

# ---------------------------------------------------------------------------
# Significant Changes table layout (2025/2026 schedule)
# ---------------------------------------------------------------------------
CHANGES_TABLE_PAGE_RANGE = (7, 7)  # index 7 (page 8)
CHANGES_TABLE_COLUMN_COUNT = 4

# ---------------------------------------------------------------------------
# Flagged Products table layout (2025/2026 schedule)
# ---------------------------------------------------------------------------
FLAGGED_TABLE_PAGE_INDEX = 8  # index 8 (page 9)
FLAGGED_TABLE_COLUMN_COUNT = 4
