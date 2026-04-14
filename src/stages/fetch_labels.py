"""Label fetcher — downloads approved label PDFs from the ACVM register.

Downloads the most recent label for each product, tracks versions
via a manifest.json, and detects new label versions by hash comparison.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

from src.config import LABELS_DIR
from src.models import LabelDocument
from src.parsers.acvm_detail_scraper import AcvmDetailResult, AcvmLabelInfo

logger = logging.getLogger(__name__)

RATE_LIMIT_SECS = 0.8


def _hash_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _build_filename(product_name: str, reg_no: str, original_filename: str) -> str:
    """Build a clean filename: ProductName - RegNo - Approved Label - Date.pdf"""
    from src.parsers.acvm_detail_scraper import _extract_date

    date_str = _extract_date(original_filename) or datetime.now().strftime("%b %Y")
    raw = f"{product_name} - {reg_no} - Approved Label - {date_str}.pdf"
    return re.sub(r'[<>:"/\\|?*\x00-\x1f]', "", raw).strip()


def _load_manifest(labels_dir: Path) -> dict:
    manifest_path = labels_dir / "manifest.json"
    if manifest_path.exists():
        return json.loads(manifest_path.read_text(encoding="utf-8"))
    return {}


def _save_manifest(labels_dir: Path, manifest: dict) -> None:
    manifest_path = labels_dir / "manifest.json"
    labels_dir.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False, sort_keys=True),
        encoding="utf-8",
    )


def fetch_labels(
    session: requests.Session,
    detail_results: dict[str, AcvmDetailResult],
    product_names: dict[str, str],
    *,
    labels_dir: Path | None = None,
) -> list[LabelDocument]:
    """Download label PDFs for products with ACVM detail results.

    Args:
        session: Requests session with ACVM cookies.
        detail_results: {product_slug: AcvmDetailResult} from detail scraper.
        product_names: {product_slug: display_name} mapping.
        labels_dir: Directory for label storage (default: config.LABELS_DIR).

    Returns:
        List of LabelDocument entries for the catalogue.
    """
    labels_dir = labels_dir or LABELS_DIR
    manifest = _load_manifest(labels_dir)
    documents: list[LabelDocument] = []
    now_iso = datetime.now(timezone.utc).isoformat()

    downloaded = 0
    skipped = 0
    no_labels = 0

    for slug, detail in detail_results.items():
        reg_no = detail.registration_no
        name = product_names.get(slug, detail.trade_name)

        if not detail.labels:
            no_labels += 1
            continue

        # Take the most recent label (first in list)
        label_info = detail.labels[0]
        filename = _build_filename(name, reg_no, label_info.filename)

        # Check manifest for existing download
        product_dir = labels_dir / reg_no
        existing_entry = manifest.get(reg_no)

        if existing_entry:
            current_version = next(
                (v for v in existing_entry.get("versions", []) if v.get("is_current")),
                None,
            )
            if current_version and current_version.get("filename") == filename:
                # Same filename — skip download, use existing
                skipped += 1
                documents.append(LabelDocument(
                    trade_product_id=slug,
                    registration_no=reg_no,
                    filename=filename,
                    local_path=f"{reg_no}/{filename}",
                    file_hash_sha256=current_version["hash"],
                    label_date=label_info.label_date,
                    downloaded_at=current_version.get("downloaded_at", now_iso),
                    is_current=True,
                ))
                # Update last_checked
                manifest[reg_no]["last_checked"] = now_iso
                continue

        # Download the label
        time.sleep(RATE_LIMIT_SECS)
        try:
            resp = session.get(label_info.url, timeout=60)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.warning("Failed to download label for %s: %s", slug, e)
            continue

        pdf_bytes = resp.content
        pdf_hash = _hash_bytes(pdf_bytes)

        # Check if we already have this exact file (by hash)
        if existing_entry:
            for v in existing_entry.get("versions", []):
                if v.get("hash") == pdf_hash:
                    # Same content, different filename — skip
                    skipped += 1
                    documents.append(LabelDocument(
                        trade_product_id=slug,
                        registration_no=reg_no,
                        filename=v["filename"],
                        local_path=f"{reg_no}/{v['filename']}",
                        file_hash_sha256=pdf_hash,
                        label_date=label_info.label_date,
                        downloaded_at=v.get("downloaded_at", now_iso),
                        is_current=True,
                    ))
                    manifest[reg_no]["last_checked"] = now_iso
                    break
            else:
                # New version — mark old as not current
                for v in existing_entry.get("versions", []):
                    v["is_current"] = False
                # Save new version
                product_dir.mkdir(parents=True, exist_ok=True)
                (product_dir / filename).write_bytes(pdf_bytes)
                downloaded += 1

                manifest[reg_no]["versions"].append({
                    "filename": filename,
                    "hash": pdf_hash,
                    "downloaded_at": now_iso,
                    "is_current": True,
                    "label_date": label_info.label_date,
                })
                manifest[reg_no]["last_checked"] = now_iso

                documents.append(LabelDocument(
                    trade_product_id=slug,
                    registration_no=reg_no,
                    filename=filename,
                    local_path=f"{reg_no}/{filename}",
                    file_hash_sha256=pdf_hash,
                    label_date=label_info.label_date,
                    downloaded_at=now_iso,
                    is_current=True,
                ))
        else:
            # First download for this product
            product_dir.mkdir(parents=True, exist_ok=True)
            (product_dir / filename).write_bytes(pdf_bytes)
            downloaded += 1

            manifest[reg_no] = {
                "trade_name": name,
                "versions": [{
                    "filename": filename,
                    "hash": pdf_hash,
                    "downloaded_at": now_iso,
                    "is_current": True,
                    "label_date": label_info.label_date,
                }],
                "last_checked": now_iso,
            }

            documents.append(LabelDocument(
                trade_product_id=slug,
                registration_no=reg_no,
                filename=filename,
                local_path=f"{reg_no}/{filename}",
                file_hash_sha256=pdf_hash,
                label_date=label_info.label_date,
                downloaded_at=now_iso,
                is_current=True,
            ))

    _save_manifest(labels_dir, manifest)

    logger.info(
        "Labels: %d downloaded, %d skipped (cached), %d no labels found",
        downloaded, skipped, no_labels,
    )

    return documents
