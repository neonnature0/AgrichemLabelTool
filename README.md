# AgrichemLabelTool

Extracts structured product data from New Zealand agrichemical labels and the annual NZW Vineyard Spray Schedule, then enriches it against the ACVM (Agricultural Compounds & Veterinary Medicines) register to produce a single, queryable catalogue of products, active ingredients, intervals, and resistance-management rules.

Built to save hours of manual data entry and to keep a versioned, diffable record of the NZ grape-spray regulatory landscape from season to season.

## What it does

Given the NZW Spray Schedule PDF, the tool will:

1. **Parse** the schedule's master tables (products, PHI/REI/WHP, resistance rules, changes, flagged products)
2. **Assemble** a unified catalogue with de-duplicated active ingredients and structured interval values (e.g. `EL4+14days` becomes a real object you can query)
3. **Look up every product on the ACVM register** (fuzzy match, ~91% hit rate) and pull registration info, registrants, formulation types
4. **Download the actual label PDFs** from ACVM, versioned by file hash
5. **Extract 13+ fields from each label** — active ingredients, rates, rainfastness, container sizes, max applications, growth stages, tank-mix info, buffer zones, shelf life, PPE, HSR/HSNO codes, signal words
6. **Diff season to season** — produces a human-readable changelog of what changed (new products, altered PHIs, added warnings, etc.)

There's also a **web tool** for reviewing extractions, correcting mistakes, and teaching the system new regex patterns — all approved patterns are saved and improve extraction on the next run.

## Quick start

### Requirements
- Python 3.12+ ([download](https://www.python.org/downloads/))
- The NZW Spray Schedule PDF (place at `data/input/schedule.pdf`)

### Launch (Windows — easy mode)
Double-click **`start.bat`**. It creates the venv, installs dependencies on first run, and opens the tool at http://127.0.0.1:8000.

The first time you open the tool, a banner prompts you to click **"Extract labels"** — this parses the ~248 label PDFs (a few minutes) and caches the results so every future launch is instant. Everything runs from the GUI.

### Launch (manual / macOS / Linux)
```bash
python -m venv .venv
source .venv/bin/activate       # or .venv\Scripts\activate on Windows
pip install -e .[dev]
python -m uvicorn tool.app:app --port 8000
```

### Run the full pipeline (CLI, optional)
```bash
python scripts/run_pipeline.py --pdf data/input/schedule.pdf --season 2025-2026
```

Output lands in `data/output/2025-2026/`:
- `catalogue.json` — the full enriched catalogue
- `active_ingredients.json`, `trade_products.json`, `phi_matrix.json`, `rm_rules.json`, `label_extractions.json` — convenience extracts
- `changelog.json` — what changed vs. the previous season (if available)

### Run individual stages
```bash
python scripts/run_pipeline.py --pdf ... --stages parse,assemble
python scripts/run_pipeline.py --stages acvm           # fetch ACVM + labels
python scripts/run_pipeline.py --stages labels         # re-extract label fields
python scripts/run_pipeline.py --stages diff           # regenerate changelog
```

Stages are idempotent — re-running with the same inputs is cheap (source PDF is hashed, ACVM responses are cached 30 days).

## How the verification tool works

The whole point of the tool is a **feedback loop** that improves extraction over time:

1. Pick a product — see the label PDF on the left, extracted fields on the right
2. For each field: mark **Correct**, **Wrong** (enter the right value), or **Not on label**
3. When you notice data the extractor missed, select the text in the PDF and tag it with the correct field name
4. The tool generates 2–3 candidate regex patterns and tests them against all downloaded labels — you see the match count immediately
5. Approve a pattern and it's saved to `data/corrections/learned_patterns.json`
6. The next pipeline run picks up your approved patterns and applies them to every label
7. Coverage stats climb as you review more products

All your corrections, annotations, and learned patterns are stored as plain JSON in `data/corrections/` — version-controlled and persistent across runs.

## Pipeline stages in one diagram

```
 schedule.pdf ─▶ parse ─▶ staging/raw_*.json
                            │
                            ▼
                        assemble ─▶ output/catalogue.json (+ 4 extracts)
                            │
                            ▼
   ACVM CSV + web ◀────── acvm  (match → enrich → scrape → fetch PDFs)
                            │
                            ▼
 labels/*.pdf ────────▶ labels  (pdfplumber → 13 field extractors + learned patterns)
                            │
                            ▼
 previous season ─────▶ diff  ─▶ output/changelog.json
```

## Project layout

```
src/
  parsers/      PDF-table parsers (PHI, RM, changes, flagged) + ACVM + label parsers
  stages/       The 5 pipeline stages
  utils/        slugify, hashing, interval parsing, RM code normaliser, HSNO → storage class
  assembler.py  Builds the final SeasonCatalogue Pydantic model
  models.py     All data models (frozen, validated)
  config.py     Paths and settings

tool/           FastAPI verification + pattern-training web app
scripts/        CLI entry points (pipeline, validators, label checks)
tests/          Pytest suite (unit + integration)

data/
  input/        schedule.pdf (you provide)
  staging/      Intermediate parser output + ACVM caches (git-ignored)
  output/       Final per-season catalogues (git-ignored)
  labels/       Downloaded label PDFs, versioned by P-number
  corrections/  Human feedback — verified extractions, corrections, learned patterns, ACVM overrides
```

## Useful scripts

| Script | Purpose |
|---|---|
| `scripts/run_pipeline.py` | Main orchestrator |
| `scripts/inspect_catalogue.py` | Pretty-print / search the catalogue |
| `scripts/validate_catalogue.py` | Cross-check referential integrity |
| `scripts/run_label_check.py` | Re-download updated label versions |
| `scripts/fetch_missing_labels.py` | Backfill any labels missed by the pipeline |

## Tests

```bash
pytest                          # full suite
pytest -m "not integration"     # skip anything needing the real PDF
pytest -m "not network"         # skip anything needing internet
```

## License

Private — internal tool.
