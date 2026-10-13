"""Refresh customer owners from the live Tableau booking source.

The authoritative roster mart (``DM_SALES`` dimensions) is gone from Oracle, so
``refresh_salesman_source.py`` can no longer replace ``salesman.csv``.  The
booking datasource that the daily pipeline already reads still carries the
export salesperson (``EX_SAL_UNO``) that ICC stamps on every booking, so recent
bookings show who owns an account today.

This script queries that live source through a temporary published workbook -
the same mechanism the pipeline uses for View 1/View 2, which needs no VizQL
Data Service permission - and updates only the customers whose owner is proven
to have changed.  Customers without recent bookings keep their existing roster
row, and the manual ICC override layer still runs afterwards so verified
corrections win.

Preview (default):
    python scripts/refresh_salesman_owners_from_bookings.py --as-of 20260915

Apply:
    python scripts/refresh_salesman_owners_from_bookings.py --as-of 20260915 --apply
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(ROOT / "scripts"))
RUNTIME_ROOT = Path(os.environ.get("DASHBOARD_RUNTIME_ROOT", str(ROOT)))
DEFAULT_SOURCE = ROOT / "salesman.csv"

BOOKING_DATASOURCE_CAPTION = os.environ.get(
    "TABLEAU_BOOKING_DATASOURCE_CAPTION", "D_SA002.Booking"
)
BOOKING_DATASOURCE_CONTENT_URL = os.environ.get(
    "TABLEAU_BOOKING_DATASOURCE_CONTENT_URL", "D_SA002_Booking"
)
OWNER_WB_NAME = os.environ.get("TEMP_SALESMAN_OWNER_WB", "temp_salesman_owner_src")
OWNER_SHEET = "owner"
DS_REF = "sqlproxy.salesmanowner"
DIMENSIONS = ("POR_CTR_CD", "POR_PLC_CD", "BKG_SHPR_CST_NO", "EX_SAL_UNO", "BKG_STS_CD")
# ICC stores a single salesperson id; a comma-joined value means shared or
# unresolved ownership and must never overwrite the roster.
SALESMAN_ID_RE = re.compile(r"^[A-Za-z][A-Za-z0-9._-]{1,63}$")
EXCLUDED_STATUS = {"cancel"}
OWNER_WINDOW_MONTHS = max(1, int(os.environ.get("SALESMAN_OWNER_WINDOW_MONTHS", "9")))
OWNER_MIN_BOOKINGS = max(1, int(os.environ.get("SALESMAN_OWNER_MIN_BOOKINGS", "2")))
OWNER_SPARSE_MAX = max(1, int(os.environ.get("SALESMAN_OWNER_SPARSE_MAX", "2")))


# ── Tableau extraction ──────────────────────────────────────────────────────
def build_owner_workbook(since: str) -> bytes:
    """Build a one-sheet workbook listing owners per origin, customer and status.

    Each row also carries the newest booking date and the booking count, so the
    current owner can be chosen without exporting every booking.
    """
    from tableau_published_query import build_workbook, dimension_column

    extra_dependencies = "\n".join([
        dimension_column("BKG_DT", "            "),
        "            <column datatype='integer' name='[BkgDtNum]' role='measure' type='quantitative'>",
        "              <calculation class='tableau' formula='INT([BKG_DT])' />",
        "            </column>",
        "            <column datatype='integer' name='[BkgCount]' role='measure' type='quantitative'>",
        "              <calculation class='tableau' formula='1' />",
        "            </column>",
        "            <column datatype='string' name='[RecentYN]' role='dimension' type='nominal'>",
        "              <calculation class='tableau' formula='IF [BKG_DT] &gt;= "
        f"&quot;{since}&quot; THEN &quot;Y&quot; ELSE &quot;N&quot; END' />",
        "            </column>",
        "            <column-instance column='[BkgDtNum]' derivation='Max' "
        "name='[max:BkgDtNum:qk]' pivot='key' type='quantitative' />",
        "            <column-instance column='[BkgCount]' derivation='Sum' "
        "name='[sum:BkgCount:qk]' pivot='key' type='quantitative' />",
        "            <column-instance column='[RecentYN]' derivation='None' "
        "name='[none:RecentYN:nk]' pivot='key' type='nominal' />",
    ])
    view_extras = "\n".join([
        f"          <filter class='categorical' column='[{DS_REF}].[none:RecentYN:nk]'>",
        "            <groupfilter function='member' level='[none:RecentYN:nk]' "
        "member='&quot;Y&quot;' />",
        "          </filter>",
        "          <slices>",
        f"            <column>[{DS_REF}].[none:RecentYN:nk]</column>",
        "          </slices>",
    ])
    return build_workbook(
        caption=BOOKING_DATASOURCE_CAPTION,
        content_url=BOOKING_DATASOURCE_CONTENT_URL,
        ds_ref=DS_REF,
        sheet=OWNER_SHEET,
        dimensions=DIMENSIONS,
        extra_dependencies=extra_dependencies,
        view_extras=view_extras,
        cols_shelf=f"([{DS_REF}].[max:BkgDtNum:qk] / [{DS_REF}].[sum:BkgCount:qk])",
    )


def window_start(as_of: str, months: int = OWNER_WINDOW_MONTHS) -> str:
    as_of_date = datetime.strptime(as_of, "%Y%m%d")
    start = as_of_date - timedelta(days=int(round(months * 30.4)))
    return start.strftime("%Y%m%d")


def download_owner_export(since: str, save_path: Path) -> int:
    """Publish a temporary workbook, export its rows, then remove the workbook."""
    from tableau_published_query import publish_and_export

    return publish_and_export(build_owner_workbook(since), OWNER_WB_NAME, save_path)


# ── Owner selection ─────────────────────────────────────────────────────────
def _number(value: str) -> int:
    text = re.sub(r"[^0-9-]", "", str(value or ""))
    try:
        return int(text)
    except ValueError:
        return 0


def _column(fieldnames: list[str], *needles: str) -> str:
    for needle in needles:
        for name in fieldnames:
            if needle.lower() in (name or "").strip().lower():
                return name
    raise KeyError(f"Owner export is missing a column for {needles[0]}")


def parse_owner_export(text: str) -> list[dict[str, object]]:
    """Normalize the Tableau export into owner observations."""
    reader = csv.DictReader(io.StringIO(text))
    fields = [name for name in (reader.fieldnames or []) if name]
    if not fields:
        raise ValueError("Owner export has no header row")
    customer_col = _column(fields, "Booking_Shipper_code", "BKG_SHPR_CST_NO")
    salesman_col = _column(fields, "Salesman_POR", "EX_SAL_UNO")
    country_col = _column(fields, "POR_Country", "POR_CTR_CD")
    port_col = _column(fields, "POR_PORT", "POR_PLC_CD")
    status_col = _column(fields, "Booking_status", "BKG_STS_CD")
    date_col = _column(fields, "BkgDtNum")
    count_col = _column(fields, "BkgCount")

    rows: list[dict[str, object]] = []
    # Tableau's summary export repeats each dimension combination once per
    # measure, with the same values on every copy.  Keeping one copy per
    # combination is required: summing the copies would double every booking
    # count and defeat the evidence thresholds below.
    seen: dict[tuple[str, str, str, str, str], tuple[str, str]] = {}
    for row in reader:
        key = (
            (row.get(customer_col) or "").strip().upper(),
            (row.get(country_col) or "").strip().upper(),
            (row.get(port_col) or "").strip().upper(),
            (row.get(salesman_col) or "").strip().upper(),
            (row.get(status_col) or "").strip().upper(),
        )
        measures = (str(row.get(date_col) or ""), str(row.get(count_col) or ""))
        if key in seen:
            if seen[key] != measures:
                raise ValueError(
                    f"Owner export gives conflicting values for {key}: "
                    f"{seen[key]} vs {measures}"
                )
            continue
        seen[key] = measures
        status = (row.get(status_col) or "").strip()
        if status.lower() in EXCLUDED_STATUS:
            continue
        customer = key[0]
        salesman = (row.get(salesman_col) or "").strip()
        if not customer or not SALESMAN_ID_RE.fullmatch(salesman):
            continue
        last_dt = _number(row.get(date_col))
        if last_dt <= 0:
            continue
        rows.append({
            "customer": customer,
            "salesman": salesman,
            "country": key[1],
            "port": key[2],
            "last_dt": last_dt,
            "bookings": max(1, _number(row.get(count_col))),
        })
    return rows


def choose_current_owners(
    observations: list[dict[str, object]],
    min_bookings: int = OWNER_MIN_BOOKINGS,
    sparse_max: int = OWNER_SPARSE_MAX,
) -> tuple[dict[str, dict[str, object]], dict[str, int]]:
    """Pick one current owner per customer from the booking observations.

    The owner of the newest booking wins.  A single stray booking does not
    unseat a busier owner unless the customer barely books at all, and a tie on
    the newest booking date is left alone rather than guessed.
    """
    by_customer: dict[str, list[dict[str, object]]] = {}
    for row in observations:
        by_customer.setdefault(row["customer"], []).append(row)

    chosen: dict[str, dict[str, object]] = {}
    skipped = {"ambiguous_date": 0, "weak_evidence": 0}
    for customer, rows in by_customer.items():
        by_owner: dict[str, dict[str, object]] = {}
        for row in rows:
            owner = by_owner.setdefault(row["salesman"], {
                "salesman": row["salesman"], "last_dt": 0, "bookings": 0,
                "country": row["country"], "port": row["port"],
            })
            owner["bookings"] += int(row["bookings"])
            if int(row["last_dt"]) > int(owner["last_dt"]):
                owner.update({
                    "last_dt": int(row["last_dt"]),
                    "country": row["country"],
                    "port": row["port"],
                })
        newest = max(int(owner["last_dt"]) for owner in by_owner.values())
        leaders = [owner for owner in by_owner.values() if int(owner["last_dt"]) == newest]
        if len(leaders) > 1:
            skipped["ambiguous_date"] += 1
            continue
        leader = leaders[0]
        total = sum(int(owner["bookings"]) for owner in by_owner.values())
        if int(leader["bookings"]) < min_bookings and total > sparse_max:
            skipped["weak_evidence"] += 1
            continue
        chosen[customer] = {
            "customer": customer,
            "salesman": leader["salesman"],
            "country": leader["country"],
            "port": leader["port"],
            "last_dt": int(leader["last_dt"]),
            "bookings": int(leader["bookings"]),
            "total_bookings": total,
        }
    return chosen, skipped


def _parsable_date(value: object) -> bool:
    from apply_salesman_owner_overrides import clean

    return bool(re.fullmatch(r"\d+", clean(value).replace(".0", "")))


def roster_owner_state(
    rows: list[dict[str, str]], as_of: int
) -> tuple[dict[str, str], set[str]]:
    """Return active owners per customer, plus customers whose rows are unreadable.

    A handful of legacy rows carry malformed dates such as ``202103.1``.  The
    override writer cannot tell whether those rows are active, so appending a
    replacement would leave two active rows for one customer.  Those customers
    are reported separately and left untouched.
    """
    from apply_salesman_owner_overrides import clean, is_active

    current: dict[str, str] = {}
    unreadable: set[str] = set()
    for row in rows:
        customer = clean(row.get("CUSTOMER_NO")).upper()
        if not customer:
            continue
        if not (_parsable_date(row.get("SALES_START_DATE"))
                and _parsable_date(row.get("SALES_END_DATE"))):
            unreadable.add(customer)
            continue
        if not is_active(row, as_of):
            continue
        salesman = clean(row.get("SALESMAN_NO"))
        if salesman:
            current.setdefault(customer, salesman)
    return current, unreadable - set(current)


def build_owner_updates(
    chosen: dict[str, dict[str, object]],
    roster_rows: list[dict[str, str]],
    as_of: int,
) -> tuple[list[dict[str, str]], dict[str, int]]:
    """Emit override rows only for customers whose owner actually changed."""
    current, unreadable = roster_owner_state(roster_rows, as_of)
    updates: list[dict[str, str]] = []
    counts = {"unchanged": 0, "changed": 0, "new": 0, "unreadable_row": 0}
    for customer in sorted(chosen):
        pick = chosen[customer]
        existing = current.get(customer, "")
        if existing and existing.upper() == str(pick["salesman"]).upper():
            counts["unchanged"] += 1
            continue
        if not existing and customer in unreadable:
            counts["unreadable_row"] += 1
            continue
        counts["new" if not existing else "changed"] += 1
        updates.append({
            "COUNTRY": str(pick["country"]),
            "PORT": str(pick["port"]),
            "CUSTOMER_NO": customer,
            "SALESMAN_NO": str(pick["salesman"]),
            "SALES_START_DATE": str(as_of),
            "OWNER_OBSERVED_DATE": str(as_of),
            "OWNER_EFFECTIVE_DATE": "",
            "OWNER_EVIDENCE_DATE": str(pick["last_dt"]),
            "OWNER_DATE_BASIS": "observed_only_effective_unknown",
            "OWNER_EFFECTIVE_END_DATE": "",
            "SALES_END_DATE": "99991231",
            "EVIDENCE": (
                f"live booking {BOOKING_DATASOURCE_CAPTION} EX_SAL_UNO; "
                f"last booking {pick['last_dt']}; bookings {pick['bookings']}/"
                f"{pick['total_bookings']}; previous {existing or '(none)'}"
            ),
        })
    return updates, counts


# ── Entry point ─────────────────────────────────────────────────────────────
def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--as-of", default=datetime.now().strftime("%Y%m%d"))
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--export", type=Path, help="Reuse an existing Tableau export CSV.")
    parser.add_argument("--since", help="Booking window start (YYYYMMDD).")
    args = parser.parse_args()

    from salesman_owner_history import HISTORY_FIELDS
    from apply_salesman_owner_overrides import (
        apply_overrides, audit_active, load_csv, write_atomic,
    )

    as_of = int(args.as_of)
    output_dir = RUNTIME_ROOT / "output"
    output_dir.mkdir(parents=True, exist_ok=True)
    export_path = args.export or (output_dir / f"salesman_owner_booking_export_{args.as_of}.csv")
    proposal_path = output_dir / f"salesman_owner_from_bookings_{args.as_of}.csv"
    audit_path = output_dir / f"salesman_owner_from_bookings_{args.as_of}.json"
    since = args.since or window_start(args.as_of)

    audit: dict[str, object] = {
        "as_of": args.as_of,
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "booking_window_start": since,
        "datasource": BOOKING_DATASOURCE_CAPTION,
        "applied": False,
    }
    try:
        if args.export:
            print(f"Reusing owner export: {export_path}")
        else:
            written = download_owner_export(since, export_path)
            print(f"Owner export downloaded from Tableau: {written:,} bytes (since {since})")
        observations = parse_owner_export(export_path.read_text(encoding="utf-8-sig"))
        chosen, skipped = choose_current_owners(observations)
        fields, roster_rows = load_csv(args.source)
        updates, counts = build_owner_updates(chosen, roster_rows, as_of)
    except Exception as exc:
        audit.update({"status": "failed", "error": str(exc)[:1000], "requires_action": True})
        audit_path.write_text(json.dumps(audit, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"WARNING: salesman owner refresh from bookings unavailable ({exc})")
        return 1

    audit.update({
        "observations": len(observations),
        "customers_with_live_owner": len(chosen),
        "skipped_ambiguous": skipped["ambiguous_date"],
        "skipped_weak_evidence": skipped["weak_evidence"],
        "unchanged": counts["unchanged"],
        "changed": counts["changed"],
        "added": counts["new"],
        "skipped_unreadable_roster_row": counts["unreadable_row"],
        "updates": len(updates),
    })
    with proposal_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["COUNTRY", "PORT", "CUSTOMER_NO", "SALESMAN_NO",
                        "SALES_START_DATE", "SALES_END_DATE", "EVIDENCE", *HISTORY_FIELDS],
            lineterminator="\n",
        )
        writer.writeheader()
        writer.writerows(updates)
    print(
        f"Salesman owners from bookings: live customers={len(chosen):,}; "
        f"unchanged={counts['unchanged']:,}; changed={counts['changed']:,}; "
        f"added={counts['new']:,}; skipped ambiguous={skipped['ambiguous_date']:,}; "
        f"skipped weak evidence={skipped['weak_evidence']:,}; "
        f"skipped unreadable roster row={counts['unreadable_row']:,}"
    )

    if not updates:
        audit["status"] = "no_change"
        audit_path.write_text(json.dumps(audit, ensure_ascii=False, indent=2), encoding="utf-8")
        print("Salesman owner refresh from bookings: no owner changes to apply")
        return 0

    updated, changes = apply_overrides(roster_rows, updates, as_of, preserve_history=True)
    audit["owner_history_events"] = [change["history_event"] for change in changes]
    after = audit_active(updated, as_of)
    if after["conflicts"]:
        audit.update({"status": "conflict", "conflicts": len(after["conflicts"]),
                      "requires_action": True})
        audit_path.write_text(json.dumps(audit, ensure_ascii=False, indent=2), encoding="utf-8")
        for customer, owners in list(after["conflicts"].items())[:20]:
            print(f"[CONFLICT] {customer}: {', '.join(owners)}")
        return 2

    applied = sum(change["changed"] == "True" for change in changes)
    if args.apply:
        write_atomic(args.source, list(dict.fromkeys([*fields, *HISTORY_FIELDS])), updated)
        audit.update({"status": "applied", "applied": True, "applied_rows": applied})
        print(f"Applied {applied:,} booking-based owner update(s) to {args.source}")
    else:
        audit["status"] = "preview"
        print(f"Preview only; {applied:,} update(s) ready. Use --apply to write them.")
    audit["proposal_file"] = str(proposal_path)
    audit_path.write_text(json.dumps(audit, ensure_ascii=False, indent=2), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
