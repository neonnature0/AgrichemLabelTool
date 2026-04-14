# Label Verification & Extraction Training Tool

Local web tool for reviewing label extractions, correcting mistakes, and teaching the system new patterns.

## Quick Start

```bash
cd nz-catalogue
python -m uvicorn tool.app:app --reload --port 8000
```

Then open http://localhost:8000

## The Core Loop

1. Open a product from the list (sorted by confidence — low first)
2. See the label PDF on the left, extracted data on the right
3. For each field: mark as Correct, Wrong (enter correction), or Not on label
4. When you spot missed data: paste the text in the annotation box, select the field type
5. The system generates regex patterns and tests them against all 244 labels
6. Approve good patterns — they're saved and improve extraction for all products
7. Watch the coverage stats climb as you review more labels

## Data Files

- `data/corrections/verified.json` — which products and fields have been reviewed
- `data/corrections/corrections.json` — manual value corrections
- `data/corrections/annotations.json` — text annotations tagged with field types
- `data/corrections/learned_patterns.json` — approved regex patterns

All files are version controlled and persist across pipeline runs.
