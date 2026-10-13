"""Block publication unless today's owner roster is present in Sales Target."""

from __future__ import annotations

import argparse
import csv
import json
import os
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RUNTIME = Path(os.environ.get("DASHBOARD_RUNTIME_ROOT", ROOT))


def read_json(path: Path) -> dict:
    if not path.is_file():
        raise FileNotFoundError(path)
    return json.loads(path.read_text(encoding="utf-8-sig"))


def verify(dataset: str) -> int:
    output = RUNTIME / "output"
    target = RUNTIME / "dist" / "sales-target"
    current_index = read_json(target / "index.json")
    if current_index.get("target_basis_policy") == "company_2025_actuals_official_office_attribution":
        from sales_target_release import verify_company_release
        result = verify_company_release(target, dataset)
        print(f"[SalesTargetCheck] {dataset}: verified company-2025 immutable release; "
              f"{result['chunks']} chunks, {result['booking_rows']} booking rows")
        return 0
    report = read_json(output / f"current_customer_owner_target_update_{dataset}.json")
    additions = read_json(target / "current_customer_owner_addition_check.json")
    roster = read_json(output / f"sales_owner_affiliation_{dataset}.json")
    index = read_json(target / "index.json")
    manifest = read_json(target / "manifest.json")
    base = read_json(target / "base2025.json")

    for label, value in (
        ("owner report", report.get("dataset_id")),
        ("owner additions", additions.get("dataset")),
        ("roster", roster.get("dataset_id")),
        ("index", index.get("data_date")),
        ("manifest", manifest.get("data_date")),
        ("base2025", base.get("data_date")),
    ):
        if str(value) != dataset:
            raise ValueError(f"{label} is stale: {value} != {dataset}")
    if report.get("registered_owner_coverage") != "passed":
        raise ValueError("Registered owner coverage did not pass")
    if roster.get("validation") != "passed" or roster.get("dry_run"):
        raise ValueError("Owner roster has not been applied")
    if index.get("office_policy") != roster.get("policy") or manifest.get("office_policy") != roster.get("policy"):
        raise ValueError("Sales Target roster policy differs")

    approved = {tuple(pair) for pair in roster["approved_pairs"]}
    try:
        from sales_target_roster_contract import validate_rosters
    except ModuleNotFoundError:
        from scripts.sales_target_roster_contract import validate_rosters
    actual, performance_only = validate_rosters(index, manifest, approved)

    required = {(str(row["target_tab"]), str(row["resolved_sales"]))
                for row in report.get("registered_owner_additions", [])}
    required.update((str(row["target_tab"]), str(row["resolved_sales"]))
                    for row in additions.get("verified_missing_active_sales", []))
    audit_path = output / f"current_customer_owner_input_audit_{dataset}.csv"
    with audit_path.open(encoding="utf-8-sig", newline="") as handle:
        audit_rows = list(csv.DictReader(handle))
        if not audit_rows:
            raise ValueError("Today's owner input audit is empty")
    active_types = {
        "contact_point", "pinned_sales", "verified_missing_active_sales",
        "booking_verified_active_owner", "live_booking_provisional",
    }
    required.update(
        (row["target_tab"], row["resolved_sales"])
        for row in audit_rows
        if row.get("source_type") in active_types
        and row.get("match_status") != "unmatched"
        and row.get("resolved_sales")
        and int(float(row.get("customer_count") or 0)) > 0
    )
    missing = sorted(required - actual)
    if missing:
        raise ValueError(f"Active owners missing from Sales Target: {missing}")
    print(f"[SalesTargetCheck] {dataset}: {len(approved)} owner rows, {len(required)} required current owners, "
          f"{manifest['chunk_count']} chunks; index, filter and source audit agree")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", required=True)
    args = parser.parse_args()
    return verify(args.dataset)


if __name__ == "__main__":
    raise SystemExit(main())
