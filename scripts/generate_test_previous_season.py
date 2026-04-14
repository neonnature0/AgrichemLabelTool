"""Generate a synthetic 2024-2025 season catalogue for testing the diff engine.

Loads the real 2025-2026 catalogue and applies deliberate, documented modifications.
Every change is recorded in known_changes.json as the test oracle.

Usage:
    python -m scripts.generate_test_previous_season
"""

from __future__ import annotations

import copy
import sys
from pathlib import Path

import orjson

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.config import OUTPUT_DIR


def main() -> None:
    current_path = OUTPUT_DIR / "2025-2026" / "catalogue.json"
    if not current_path.exists():
        print(f"Current season catalogue not found: {current_path}")
        print("Run the pipeline first: python -m scripts.run_pipeline ...")
        return

    data = orjson.loads(current_path.read_bytes())
    prev = copy.deepcopy(data)
    prev["season"] = "2024-2025"
    prev["source_pdf"] = "NZW Spray Schedule 2024-2025.pdf"
    prev["source_hash"] = "synthetic-previous-season-hash"

    # Update season field on all entries that have it
    for phi in prev["phi_entries"]:
        phi["season"] = "2024-2025"
    for rm in prev["resistance_management_rules"]:
        rm["season"] = "2024-2025"
    for sc in prev["schedule_changes"]:
        sc["season"] = "2024-2025"
    for fp in prev["flagged_products"]:
        fp["season"] = "2024-2025"

    known_changes: list[dict] = []

    # ─────────────────────────────────────────────────────────────────────
    # PRODUCT CHANGES
    # ─────────────────────────────────────────────────────────────────────

    # 1. Remove 3 products from prev (they're "new" in 2025-2026)
    products_to_remove_from_prev = ["belanty", "citara-200ew", "digger-ew"]
    prev["trade_products"] = [
        tp for tp in prev["trade_products"]
        if tp["id"] not in products_to_remove_from_prev
    ]
    # Also remove their PHI/REI/WHP entries
    prev["phi_entries"] = [
        p for p in prev["phi_entries"]
        if p["trade_product_id"] not in products_to_remove_from_prev
    ]
    prev["rei_entries"] = [
        r for r in prev["rei_entries"]
        if r["trade_product_id"] not in products_to_remove_from_prev
    ]
    prev["whp_entries"] = [
        w for w in prev["whp_entries"]
        if w["trade_product_id"] not in products_to_remove_from_prev
    ]

    for tp_id in products_to_remove_from_prev:
        known_changes.append({
            "change_type": "product_added",
            "severity": "info",
            "entity_type": "product",
            "entity_id": tp_id,
        })

    # 2. Add 2 fake products to prev (they're "withdrawn" in 2025-2026)
    fake_ai = {
        "id": "fake-withdrawn-ai",
        "name": "fake withdrawn ai",
        "section": "fungicides",
        "restriction_level": None,
        "frac_code": None, "irac_code": None, "hrac_code": None,
        "chemical_group": None, "moa_group_name": None, "is_multisite": False,
    }
    prev["active_ingredients"].append(fake_ai)

    for fake_id, fake_name in [("fake-withdrawn-a", "Fake Withdrawn A"), ("fake-withdrawn-b", "Fake Withdrawn B")]:
        prev["trade_products"].append({
            "id": fake_id,
            "name": fake_name,
            "active_ingredient_ids": ["fake-withdrawn-ai"],
            "label_claim": "test product",
            "notes": None,
            "section": "fungicides",
            "rm_rule_codes": [],
        })
        # Add PHI entries for fake products
        prev["phi_entries"].append({
            "trade_product_id": fake_id,
            "market_code": "NZ",
            "phi": {"raw": "14", "value": 14, "unit": "days", "el_stage": None,
                    "el_stage_end": None, "el_offset_days": None, "code": None,
                    "secondary_el_stage": None},
            "season": "2024-2025",
        })
        prev["rei_entries"].append({
            "trade_product_id": fake_id,
            "rei": {"raw": "WFD", "value": None, "unit": None, "el_stage": None,
                    "el_stage_end": None, "el_offset_days": None, "code": "WFD",
                    "secondary_el_stage": None},
        })
        known_changes.append({
            "change_type": "product_removed",
            "severity": "critical",
            "entity_type": "product",
            "entity_id": fake_id,
        })

    # AI changes from fake products
    known_changes.append({
        "change_type": "active_ingredient_removed",
        "severity": "critical",
        "entity_type": "active_ingredient",
        "entity_id": "fake-withdrawn-ai",
    })

    # ─────────────────────────────────────────────────────────────────────
    # PHI CHANGES
    # ─────────────────────────────────────────────────────────────────────

    phi_changes = [
        # 1. Kenja NZ: 65 → 42 (increased in prev, so current shows decrease)
        ("kenja-400-sc", "NZ", {"raw": "42", "value": 42, "unit": "days"}, "warning"),
        # 2. Botector AU: 1 → 3
        ("botector", "AU", {"raw": "3", "value": 3, "unit": "days"}, "warning"),
        # 3. Dithane NIL: EL18 → 28 (growth stage to numeric)
        ("dithane-rainshield-neo-tec", "NIL",
         {"raw": "28", "value": 28, "unit": "days", "el_stage": None}, "warning"),
        # 4. Capetec multi-market: change NZ/AU from 21→14, EU from 28→14 in prev
        ("capetec", "NZ", {"raw": "14", "value": 14, "unit": "days"}, "warning"),
        ("capetec", "AU", {"raw": "14", "value": 14, "unit": "days"}, "warning"),
        ("capetec", "EU", {"raw": "14", "value": 14, "unit": "days"}, "warning"),
        # 5. Confidor NZ: NPV → 28 (was unrestricted in prev, restricted to NPV in current)
        ("confidor", "NZ", {"raw": "28", "value": 28, "unit": "days"}, "critical"),
    ]

    for tp_id, market, old_phi_overrides, severity in phi_changes:
        for phi in prev["phi_entries"]:
            if phi["trade_product_id"] == tp_id and phi["market_code"] == market:
                # Build the old value
                old_phi = {
                    "raw": old_phi_overrides["raw"],
                    "value": old_phi_overrides.get("value"),
                    "unit": old_phi_overrides.get("unit"),
                    "el_stage": old_phi_overrides.get("el_stage"),
                    "el_stage_end": None,
                    "el_offset_days": None,
                    "code": old_phi_overrides.get("code"),
                    "secondary_el_stage": None,
                }
                phi["phi"] = old_phi
                break

        known_changes.append({
            "change_type": "phi_changed",
            "severity": severity,
            "entity_type": "phi",
            "entity_id": tp_id,
            "market": market,
        })

    # ─────────────────────────────────────────────────────────────────────
    # REI CHANGES
    # ─────────────────────────────────────────────────────────────────────

    rei_changes = [
        # Kenja: WFD → 6 days (in prev, so current is WFD)
        ("kenja-400-sc", {"raw": "6 days", "value": 6, "unit": "days", "code": None}),
        # Capetec: WFD → 4 hrs
        ("capetec", {"raw": "4 hrs", "value": 4, "unit": "hrs", "code": None}),
    ]

    for tp_id, old_rei_data in rei_changes:
        for rei in prev["rei_entries"]:
            if rei["trade_product_id"] == tp_id:
                rei["rei"] = {
                    "raw": old_rei_data["raw"],
                    "value": old_rei_data.get("value"),
                    "unit": old_rei_data.get("unit"),
                    "el_stage": None, "el_stage_end": None,
                    "el_offset_days": None,
                    "code": old_rei_data.get("code"),
                    "secondary_el_stage": None,
                }
                break

        known_changes.append({
            "change_type": "rei_changed",
            "severity": "warning",
            "entity_type": "rei",
            "entity_id": tp_id,
        })

    # ─────────────────────────────────────────────────────────────────────
    # WHP CHANGE
    # ─────────────────────────────────────────────────────────────────────

    for whp in prev["whp_entries"]:
        if whp["trade_product_id"] == "kenja-400-sc" and whp["whp_type"] == "sheep_grazing":
            whp["whp"] = {
                "raw": "14 days", "value": 14, "unit": "days",
                "el_stage": None, "el_stage_end": None,
                "el_offset_days": None, "code": None, "secondary_el_stage": None,
            }
            break

    known_changes.append({
        "change_type": "whp_changed",
        "severity": "warning",
        "entity_type": "whp",
        "entity_id": "kenja-400-sc",
    })

    # ─────────────────────────────────────────────────────────────────────
    # RM RULE CHANGES
    # ─────────────────────────────────────────────────────────────────────

    for rm in prev["resistance_management_rules"]:
        # 1. 3/DMI: relax max_apps 1→2 in prev (tightened in current)
        if rm["rule_code"] == "3/DMI":
            rm["max_applications"] = 2
            rm["rule_text"] = rm["rule_text"].replace("one application", "2 applications") if rm["rule_text"] else None

        # 2. 7/SDHI: non_consecutive false in prev (tightened to true in current)
        if rm["rule_code"] == "7/SDHI":
            rm["must_be_non_consecutive"] = False
            if rm["rule_text"]:
                rm["rule_text"] = rm["rule_text"].replace("non-consecutive ", "")

        # 3. NC/NC: wording change only
        if rm["rule_code"] == "NC/NC":
            if rm["rule_text"]:
                rm["rule_text"] = rm["rule_text"] + " (Previous season wording.)"

    known_changes.append({
        "change_type": "rm_rule_tightened",
        "severity": "warning",
        "entity_type": "rm_rule",
        "entity_id": "3/DMI",
        "field_changed": "max_applications",
    })
    known_changes.append({
        "change_type": "rm_rule_tightened",
        "severity": "warning",
        "entity_type": "rm_rule",
        "entity_id": "7/SDHI",
        "field_changed": "must_be_non_consecutive",
    })
    known_changes.append({
        "change_type": "rm_rule_wording_changed",
        "severity": "info",
        "entity_type": "rm_rule",
        "entity_id": "NC/NC",
    })

    # 4. Remove UN/UN from prev (new rule in 2025-2026)
    prev["resistance_management_rules"] = [
        r for r in prev["resistance_management_rules"]
        if r["rule_code"] != "UN/UN"
    ]
    known_changes.append({
        "change_type": "rm_rule_added",
        "severity": "info",
        "entity_type": "rm_rule",
        "entity_id": "UN/UN",
    })

    # ─────────────────────────────────────────────────────────────────────
    # FLAGGED PRODUCT CHANGES
    # ─────────────────────────────────────────────────────────────────────

    # Remove clethodim from prev flagged (newly flagged in current)
    prev["flagged_products"] = [
        fp for fp in prev["flagged_products"]
        if fp["active_ingredient"] != "clethodim"
    ]
    known_changes.append({
        "change_type": "product_flagged",
        "severity": "warning",
        "entity_type": "flagged",
        "entity_id": "clethodim",
    })

    # Add a fake unflagged product to prev
    prev["flagged_products"].append({
        "active_ingredient": "old-flagged-product",
        "trade_products": ["OldFlaggedProd"],
        "label_claim": None,
        "issue": "Previously flagged product for testing.",
        "season": "2024-2025",
    })
    known_changes.append({
        "change_type": "product_unflagged",
        "severity": "info",
        "entity_type": "flagged",
        "entity_id": "old-flagged-product",
    })

    # ─────────────────────────────────────────────────────────────────────
    # Update stats
    # ─────────────────────────────────────────────────────────────────────
    prev["stats"]["total_active_ingredients"] = len(prev["active_ingredients"])
    prev["stats"]["total_trade_products"] = len(prev["trade_products"])
    prev["stats"]["total_phi_entries"] = len(prev["phi_entries"])
    prev["stats"]["total_rei_entries"] = len(prev["rei_entries"])
    prev["stats"]["total_whp_entries"] = len(prev["whp_entries"])
    prev["stats"]["total_rm_rules"] = len(prev["resistance_management_rules"])
    prev["stats"]["total_flagged_products"] = len(prev["flagged_products"])

    # ─────────────────────────────────────────────────────────────────────
    # Write output
    # ─────────────────────────────────────────────────────────────────────
    out_dir = OUTPUT_DIR / "2024-2025"
    out_dir.mkdir(parents=True, exist_ok=True)

    catalogue_path = out_dir / "catalogue.json"
    catalogue_path.write_bytes(
        orjson.dumps(prev, option=orjson.OPT_INDENT_2 | orjson.OPT_SORT_KEYS)
    )
    print(f"Synthetic previous season written to: {catalogue_path}")

    changes_path = out_dir / "known_changes.json"
    changes_path.write_bytes(
        orjson.dumps(known_changes, option=orjson.OPT_INDENT_2 | orjson.OPT_SORT_KEYS)
    )
    print(f"Known changes oracle written to: {changes_path}")
    print(f"Total known changes: {len(known_changes)}")

    # Summary
    by_type: dict[str, int] = {}
    for c in known_changes:
        by_type[c["change_type"]] = by_type.get(c["change_type"], 0) + 1
    for ct, count in sorted(by_type.items()):
        print(f"  {ct}: {count}")


if __name__ == "__main__":
    main()
