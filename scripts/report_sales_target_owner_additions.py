"""Summarize daily sales owner additions/adjustments for Sales Target."""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any

import csv


ROOT = Path(__file__).resolve().parents[1]
RUNTIME_ROOT = Path(__import__("os").environ.get("DASHBOARD_RUNTIME_ROOT", str(ROOT)))
OUTPUT_DIR = RUNTIME_ROOT / "output"


def _latest_audit_file() -> Path | None:
    candidates = sorted(
        OUTPUT_DIR.glob("current_customer_owner_input_audit_*.csv"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    return candidates[0] if candidates else None


def _find_audit_path(dataset_id: str) -> Path | None:
    exact = OUTPUT_DIR / f"current_customer_owner_input_audit_{dataset_id}.csv"
    if exact.exists():
        return exact

    for path in sorted(OUTPUT_DIR.glob("current_customer_owner_input_audit_*.csv"), reverse=True):
        if dataset_id in path.name:
            return path
    return None


def _previous_dataset_id(current_dataset: str) -> str | None:
    candidates = sorted(
        OUTPUT_DIR.glob("current_customer_owner_input_audit_*.csv"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    current = None
    for path in candidates:
        token = path.stem.replace("current_customer_owner_input_audit_", "")
        if not token.isdigit():
            continue
        if current is None and token == current_dataset:
            current = path
            continue
        if current is not None:
            return token
    return None


def _read_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        return [
            {key: (value or "") for key, value in row.items()}
            for row in reader
        ]


def _load_dataset_rows(dataset_id: str) -> tuple[str, list[dict[str, str]]]:
    path = _find_audit_path(dataset_id)
    if path is None:
        raise FileNotFoundError(f"Missing sales target owner input audit for {dataset_id}: current_customer_owner_input_audit_{dataset_id}.csv")
    return path.name, _read_rows(path)


def _collect_summary(rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    return [
        {
            "target_tab": row.get("target_tab", ""),
            "origin": row.get("target_tab", ""),
            "input_name": row.get("input_name", ""),
            "resolved_sales": row.get("resolved_sales", ""),
            "source_type": row.get("source_type", ""),
            "source_cell": row.get("source_cell", ""),
            "customer_count": int(float(row.get("customer_count", "0") or 0)),
        }
        for row in rows
        if row.get("source_type") == "verified_missing_active_sales"
    ]


def _collect_provisional(rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    return [
        {
            "target_tab": row.get("target_tab", ""),
            "resolved_sales": row.get("resolved_sales", ""),
            "source_cell": row.get("source_cell", ""),
            "customer_count": int(float(row.get("customer_count", "0") or 0)),
        }
        for row in rows
        if row.get("source_type") == "live_booking_provisional"
    ]


def _as_dataset_id(path: Path) -> str:
    token = path.stem.replace("current_customer_owner_input_audit_", "")
    if token.isdigit():
        return token
    return datetime.now().strftime("%Y%m%d")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", default=None, help="Audit date YYYYMMDD")
    parser.add_argument(
        "--emit",
        default="current_customer_owner_addition_check_{dataset}.json",
        help="Output JSON filename pattern (use {dataset})",
    )
    args = parser.parse_args()

    latest = _latest_audit_file()
    if latest is None:
        print("[OwnerCheck] No owner audit CSV found in output/.")
        return 0

    latest_dataset = _as_dataset_id(latest)
    dataset = args.dataset or latest_dataset

    _, current_rows = _load_dataset_rows(dataset)
    current_missing = _collect_summary(current_rows)
    current_provisional = _collect_provisional(current_rows)

    previous_dataset = _previous_dataset_id(dataset)
    previous_missing: set[str] = set()
    if previous_dataset:
        _, previous_rows = _load_dataset_rows(previous_dataset)
        previous_missing = {
            row.get("resolved_sales", "")
            for row in _collect_summary(previous_rows)
            if row.get("resolved_sales")
        }

    current_missing_set = {row["resolved_sales"] for row in current_missing}
    new_missing_set = set(sorted(current_missing_set - previous_missing))
    new_missing = sorted(new_missing_set)

    report = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "dataset": dataset,
        "previous_dataset": previous_dataset,
        "verified_missing_active_sales_count": len(current_missing),
        "new_verified_missing_sales_count": len(new_missing),
        "verified_missing_active_sales": current_missing,
        "provisional_sales_count": len(current_provisional),
        "provisional_sales": current_provisional,
        "new_verified_missing_sales": [
            row for row in current_missing if row["resolved_sales"] in new_missing_set
        ],
    }

    output_name = args.emit.format(dataset=dataset)
    output_path = OUTPUT_DIR / output_name
    output_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[OwnerCheck] Dataset: {dataset}")
    print(f"[OwnerCheck] Missing active owner additions: {len(current_missing):,}")
    print(
        f"[OwnerCheck] New active owner additions since {previous_dataset or 'first available snapshot'}: "
        f"{len(new_missing):,}"
    )
    for row in current_missing:
        tag = "NEW" if row["resolved_sales"] in set(new_missing) else "  "
        print(
            f"[OwnerCheck] {tag} {row['target_tab']} {row['resolved_sales']} "
            f"({row['customer_count']} rows)"
        )
    print(f"[OwnerCheck] Provisional sales rows: {len(current_provisional):,}")
    print(f"[OwnerCheck] Report saved: {output_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
