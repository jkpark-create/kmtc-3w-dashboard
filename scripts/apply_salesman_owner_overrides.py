"""Audit and apply authoritative customer-owner corrections to salesman.csv.

The dashboard's customer-owner extract is replaced infrequently.  This small,
tracked override layer keeps verified ICC Customer Master changes durable until
the next full extract is supplied.

Preview (default):
    python scripts/apply_salesman_owner_overrides.py --as-of 20260818

Apply:
    python scripts/apply_salesman_owner_overrides.py --as-of 20260818 --apply
"""

from __future__ import annotations

import argparse
import csv
import os
import shutil
import tempfile
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SOURCE = ROOT / "salesman.csv"
DEFAULT_OVERRIDES = ROOT / "config" / "salesman_owner_overrides.csv"
REQUIRED_COLUMNS = (
    "COUNTRY",
    "PORT",
    "CUSTOMER_NO",
    "SALESMAN_NO",
    "SALES_START_DATE",
    "SALES_END_DATE",
)


def clean(value: object) -> str:
    text = "" if value is None else str(value).strip()
    return "" if text.lower() in {"", "nan", "none", "null", "nat"} else text


def date_int(value: object, fallback: int) -> int:
    text = clean(value).replace(".0", "")
    try:
        return int(text)
    except ValueError:
        return fallback


def is_active(row: dict[str, str], as_of: int) -> bool:
    return (
        date_int(row.get("SALES_START_DATE"), 99999999) <= as_of
        <= date_int(row.get("SALES_END_DATE"), 0)
    )


def load_csv(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        fields = list(reader.fieldnames or [])
        missing = [column for column in REQUIRED_COLUMNS if column not in fields]
        if missing:
            raise KeyError(f"{path.name} missing columns: {', '.join(missing)}")
        return fields, [dict(row) for row in reader]


def audit_active(rows: list[dict[str, str]], as_of: int) -> dict[str, object]:
    active = [row for row in rows if is_active(row, as_of)]
    owners: dict[str, set[str]] = defaultdict(set)
    blank_customer = 0
    blank_salesman = 0
    for row in active:
        customer = clean(row.get("CUSTOMER_NO")).upper()
        salesman = clean(row.get("SALESMAN_NO")).upper()
        if not customer:
            blank_customer += 1
            continue
        if not salesman:
            blank_salesman += 1
            continue
        owners[customer].add(salesman)
    conflicts = {
        customer: sorted(values)
        for customer, values in owners.items()
        if len(values) > 1
    }
    return {
        "active_rows": len(active),
        "active_customers": len(owners),
        "blank_customer": blank_customer,
        "blank_salesman": blank_salesman,
        "conflicts": conflicts,
    }


def apply_overrides(
    rows: list[dict[str, str]],
    overrides: list[dict[str, str]],
    as_of: int,
    *, preserve_history: bool = False,
) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    """Replace every active row for an overridden customer.

    ICC Customer Master exposes one current EXPORT PIC per customer.  A verified
    override therefore supersedes stale country/port values as well as the PIC.
    Historical rows that ended before ``as_of`` are retained.
    """
    output = [dict(row) for row in rows]
    changes: list[dict[str, str]] = []
    for override in overrides:
        customer = clean(override.get("CUSTOMER_NO")).upper()
        if not customer:
            raise ValueError("Override CUSTOMER_NO must not be blank")
        if not clean(override.get("SALESMAN_NO")):
            raise ValueError(f"Override SALESMAN_NO must not be blank for {customer}")
        matching = [
            index
            for index, row in enumerate(output)
            if clean(row.get("CUSTOMER_NO")).upper() == customer and is_active(row, as_of)
        ]
        replacement = {
            column: clean(override.get(column))
            for column in REQUIRED_COLUMNS
        }
        if preserve_history:
            from salesman_owner_history import HISTORY_FIELDS, transition_rows
            previous = [output[index] for index in matching]
            incoming = dict(previous[0]) if previous else {}
            incoming.update(replacement)
            incoming.update({key: clean(override.get(key)) for key in HISTORY_FIELDS})
            archived, incoming, event = transition_rows(previous, incoming, as_of)
            for index, row in zip(matching, archived):
                output[index] = row
            output.append(incoming)
            changes.append({"customer": customer, "changed": "True",
                            "matched_rows": str(len(matching)),
                            "evidence": clean(override.get("EVIDENCE")),
                            "history_event": event})
            continue
        if matching:
            first = matching[0]
            before = {column: clean(output[first].get(column)) for column in REQUIRED_COLUMNS}
            after = dict(output[first])
            after.update(replacement)
            output[first] = after
            for index in reversed(matching[1:]):
                del output[index]
            changed = before != replacement or len(matching) > 1
            changes.append(
                {
                    "customer": customer,
                    "before": f"{before['COUNTRY']}/{before['PORT']} -> {before['SALESMAN_NO']}",
                    "after": f"{replacement['COUNTRY']}/{replacement['PORT']} -> {replacement['SALESMAN_NO']}",
                    "matched_rows": str(len(matching)),
                    "changed": str(changed),
                    "evidence": clean(override.get("EVIDENCE")),
                }
            )
        else:
            new_row = {field: "" for field in (rows[0].keys() if rows else REQUIRED_COLUMNS)}
            new_row.update(replacement)
            output.append(new_row)
            changes.append(
                {
                    "customer": customer,
                    "before": "(no active row)",
                    "after": f"{replacement['COUNTRY']}/{replacement['PORT']} -> {replacement['SALESMAN_NO']}",
                    "matched_rows": "0",
                    "changed": "True",
                    "evidence": clean(override.get("EVIDENCE")),
                }
            )
    return output, changes


def write_atomic(path: Path, fields: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    source_stat = path.stat()
    descriptor, temp_name = tempfile.mkstemp(prefix=f"{path.name}.", suffix=".tmp", dir=path.parent)
    os.close(descriptor)
    temp_path = Path(temp_name)
    try:
        with temp_path.open("w", encoding="utf-8-sig", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore", lineterminator="\n")
            writer.writeheader()
            writer.writerows(rows)
        backup = path.with_suffix(path.suffix + ".bak")
        shutil.copy2(path, backup)
        os.replace(temp_path, path)
        # Keep mtime as the extract-freshness signal. Applying a small override
        # must not make an old source extract look newly refreshed.
        os.utime(path, ns=(source_stat.st_atime_ns, source_stat.st_mtime_ns))
    finally:
        temp_path.unlink(missing_ok=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", default=str(DEFAULT_SOURCE))
    parser.add_argument("--overrides", default=str(DEFAULT_OVERRIDES))
    parser.add_argument("--as-of", default=datetime.now().strftime("%Y%m%d"))
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    source = Path(args.source)
    overrides_path = Path(args.overrides)
    as_of = int(args.as_of)
    fields, rows = load_csv(source)
    _, overrides = load_csv(overrides_path)
    before_audit = audit_active(rows, as_of)
    updated, changes = apply_overrides(rows, overrides, as_of)
    after_audit = audit_active(updated, as_of)

    age_days = max(
        0,
        (datetime.now(timezone.utc) - datetime.fromtimestamp(source.stat().st_mtime, timezone.utc)).days,
    )
    print(
        f"Source: {source.name} ({age_days} days old); "
        f"active rows={before_audit['active_rows']:,}, "
        f"customers={before_audit['active_customers']:,}, "
        f"conflicts={len(before_audit['conflicts']):,}."
    )
    for change in changes:
        marker = "CHANGE" if change["changed"] == "True" else "OK"
        print(
            f"[{marker}] {change['customer']}: {change['before']} => {change['after']} "
            f"({change['evidence']})"
        )
    print(
        f"Post-override audit: active rows={after_audit['active_rows']:,}, "
        f"customers={after_audit['active_customers']:,}, "
        f"conflicts={len(after_audit['conflicts']):,}, "
        f"blank customer={after_audit['blank_customer']:,}, "
        f"blank salesperson={after_audit['blank_salesman']:,}."
    )
    if after_audit["conflicts"]:
        for customer, owners in list(after_audit["conflicts"].items())[:20]:
            print(f"[CONFLICT] {customer}: {', '.join(owners)}")
        return 2
    if args.apply and any(change["changed"] == "True" for change in changes):
        write_atomic(source, fields, updated)
        print(f"Applied {sum(change['changed'] == 'True' for change in changes):,} correction(s) to {source}.")
    elif args.apply:
        print("No source changes required.")
    else:
        print("Preview only; use --apply to write corrections.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
