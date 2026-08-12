"""Rewrite Salesman_POR in dist/data.json using the active customer-owner mapping
from salesman.csv (rows where today is between SALES_START_DATE and SALES_END_DATE).

The dashboard's bySales table and salesperson filter consume DATA.shipper from
dist/data.json. By post-processing the file after daily_3w_dashboard.py finishes,
both the dashboard view and the sales-target screen end up agreeing on
"who currently owns each shipper account". Provisional new salesperson IDs
detected from recent booking activity are preserved when salesman.csv lags.

Usage:
    py -3 scripts/remap_dashboard_salesman.py
        [--data-json dist/data.json]
        [--salesman-csv salesman.csv]
        [--as-of YYYYMMDD]
        [--missing-label "(미지정)"]
        [--dry-run]
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
RUNTIME_ROOT = Path(os.environ.get("DASHBOARD_RUNTIME_ROOT", str(ROOT)))
DEFAULT_DATA_JSON = RUNTIME_ROOT / "dist" / "data.json"
DEFAULT_SALESMAN_CANDIDATES = ("salesman.csv", "saleman.csv")
NON_OBT_COUNTRIES = {"KR", "JP"}
DEFAULT_MISSING = "(미지정)"


def clean_key(value: object) -> str:
    text = "" if value is None else str(value).strip()
    if text.lower() in {"", "nan", "none", "nat"}:
        return ""
    return text.upper()


def clean_salesman_value(value: object, missing_label: str = DEFAULT_MISSING) -> str:
    text = "" if value is None else str(value).strip()
    if text.lower() in {"", "nan", "none", "nat"} or text == missing_label:
        return ""
    return text


def find_salesman_csv(explicit: str | None) -> Path:
    if explicit:
        p = Path(explicit)
        if not p.exists():
            raise FileNotFoundError(f"salesman.csv not found at {p}")
        return p
    for name in DEFAULT_SALESMAN_CANDIDATES:
        p = ROOT / name
        if p.exists():
            return p
    raise FileNotFoundError("Missing salesman.csv / saleman.csv in project root")


def load_active_mapping(path: Path, as_of: str | None) -> dict[str, object]:
    if as_of is None:
        as_of = datetime.now().strftime("%Y%m%d")
    as_of_int = int(as_of)
    df = pd.read_csv(path, dtype=str, encoding="utf-8-sig", low_memory=False)
    for col in ["COUNTRY", "PORT", "CUSTOMER_NO", "SALESMAN_NO", "SALES_START_DATE", "SALES_END_DATE"]:
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str).str.strip()
        else:
            df[col] = ""
    start = pd.to_numeric(df["SALES_START_DATE"].str.replace(".0", "", regex=False), errors="coerce")
    end = pd.to_numeric(df["SALES_END_DATE"].str.replace(".0", "", regex=False), errors="coerce")
    active = df.loc[(start <= as_of_int) & (end >= as_of_int)].copy()
    active = active.loc[active["CUSTOMER_NO"].ne("") & active["SALESMAN_NO"].ne("")]
    exact: dict[tuple[str, str, str], str] = {}
    by_country: dict[tuple[str, str], str] = {}
    generic: dict[str, str] = {}
    by_customer_sales: dict[str, set[str]] = {}
    salesman_countries: dict[str, list[str]] = {}

    def put_once(bucket, key, value) -> None:
        bucket.setdefault(key, value)

    for _, row in active.iterrows():
        customer = clean_key(row["CUSTOMER_NO"])
        sales = str(row["SALESMAN_NO"]).strip()
        country = clean_key(row["COUNTRY"])
        port = clean_key(row["PORT"])
        if not customer or not sales:
            continue
        if country and port:
            put_once(exact, (country, port, customer), sales)
        if country:
            put_once(by_country, (country, customer), sales)
        if not country and not port:
            put_once(generic, customer, sales)
        by_customer_sales.setdefault(customer, set()).add(sales)
        salesman_countries.setdefault(sales, [])
        if country:
            salesman_countries[sales].append(country)

    unique_customer = {
        customer: next(iter(salespeople))
        for customer, salespeople in by_customer_sales.items()
        if len(salespeople) == 1
    }
    obt_salesmen = []
    for sales, countries in salesman_countries.items():
        home = Counter(countries).most_common(1)[0][0] if countries else ""
        if home not in NON_OBT_COUNTRIES:
            obt_salesmen.append(sales)

    return {
        "exact": exact,
        "by_country": by_country,
        "generic": generic,
        "unique_customer": unique_customer,
        "obt_salesmen": sorted(set(obt_salesmen)),
        "active_rows": int(len(active)),
    }


def lookup_salesman(mapping: dict[str, object], customer_no: object, origin: object = "", ori_port: object = "") -> str:
    customer = clean_key(customer_no)
    if not customer:
        return ""
    country = clean_key(origin)
    port = clean_key(ori_port)
    return (
        mapping["exact"].get((country, port, customer))
        or mapping["by_country"].get((country, customer))
        or mapping["generic"].get(customer)
        or mapping["unique_customer"].get(customer)
        or ""
    )


# Salesman team classification — OBT means home country is NOT KR / JP.
# Unknown (blank-only) salespeople are kept (we don't have signal to exclude).
NON_OBT_COUNTRIES = {"KR", "JP"}


def compute_obt_salesman_set(path: Path, as_of: str | None) -> list[str]:
    """Return SALESMAN_NO list for salespeople whose home country (mode of
    non-empty COUNTRY rows in active assignments) is NOT KR or JP. Their
    bookings on OBT routes should still be excluded from the OBT bySales
    table because the *salesperson* belongs to another team.
    """
    if as_of is None:
        as_of = datetime.now().strftime("%Y%m%d")
    as_of_int = int(as_of)
    df = pd.read_csv(path, dtype=str, encoding="utf-8-sig", low_memory=False)
    for col in ["COUNTRY", "SALESMAN_NO", "SALES_START_DATE", "SALES_END_DATE"]:
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str).str.strip()
    start = pd.to_numeric(df["SALES_START_DATE"].str.replace(".0", "", regex=False), errors="coerce")
    end = pd.to_numeric(df["SALES_END_DATE"].str.replace(".0", "", regex=False), errors="coerce")
    active = df.loc[(start <= as_of_int) & (end >= as_of_int) & df["SALESMAN_NO"].ne("")]
    obt_set: list[str] = []
    for sm, group in active.groupby("SALESMAN_NO"):
        non_empty = group.loc[group["COUNTRY"].ne(""), "COUNTRY"]
        if len(non_empty):
            home = non_empty.mode().iloc[0]
        else:
            home = ""
        if home in NON_OBT_COUNTRIES:
            continue
        obt_set.append(sm)
    return sorted(obt_set)


def _decode_packed_cell(cols: list[str], dicts: dict[str, list], row: list, idx: int | None) -> object:
    if idx is None or idx >= len(row):
        return ""
    value = row[idx]
    col = cols[idx]
    dictionary = dicts.get(col)
    if (
        isinstance(dictionary, list)
        and isinstance(value, int)
        and not isinstance(value, bool)
        and 0 <= value < len(dictionary)
    ):
        return dictionary[value]
    return value


def _remap_shipper_records(
    records: list[dict],
    mapping: dict[str, object],
    missing_label: str,
    provisional_keys: set[str],
) -> tuple[int, int, int, int]:
    matched = 0
    provisional = 0
    raw_fallback = 0
    unmatched = 0
    for row in records:
        if not isinstance(row, dict):
            continue
        raw_sales = clean_salesman_value(row.get("Salesman_POR", ""), missing_label)
        if clean_key(raw_sales) in provisional_keys:
            row["Salesman_POR"] = raw_sales
            provisional += 1
            continue
        new_sales = lookup_salesman(
            mapping,
            row.get("BKG_SHPR_CST_NO", ""),
            row.get("origin") or row.get("POR_CTR_CD", ""),
            row.get("ori_port") or row.get("POR_PLC_CD", ""),
        )
        if new_sales:
            row["Salesman_POR"] = new_sales
            matched += 1
        else:
            if raw_sales:
                row["Salesman_POR"] = raw_sales
                raw_fallback += 1
            else:
                row["Salesman_POR"] = missing_label
                unmatched += 1
    return matched, provisional, raw_fallback, unmatched


def remap_shipper(data: dict, mapping: dict[str, object], missing_label: str) -> tuple[int, int, int, int]:
    provisional_keys = {
        clean_key(value)
        for value in data.get("provisional_salesmen", [])
        if clean_key(value)
    }
    shipper = data.get("shipper")
    if isinstance(shipper, list):
        return _remap_shipper_records(shipper, mapping, missing_label, provisional_keys)
    if not isinstance(shipper, dict):
        return 0, 0, 0, 0
    cols = shipper.get("c", [])
    rows = shipper.get("r", [])
    dicts = shipper.get("d") or shipper.get("dicts") or {}
    if not isinstance(dicts, dict):
        dicts = {}
    if "Salesman_POR" not in cols or "BKG_SHPR_CST_NO" not in cols:
        return 0, 0, 0, 0
    sm_idx = cols.index("Salesman_POR")
    cn_idx = cols.index("BKG_SHPR_CST_NO")
    origin_idx = cols.index("origin") if "origin" in cols else None
    port_idx = cols.index("ori_port") if "ori_port" in cols else None
    matched = 0
    provisional = 0
    raw_fallback = 0
    unmatched = 0
    indexes = [sm_idx, cn_idx]
    if origin_idx is not None:
        indexes.append(origin_idx)
    if port_idx is not None:
        indexes.append(port_idx)
    need_len = max(indexes) + 1
    sales_index: dict[str, int] = {}
    sales_values: list[str] = []

    def encode_sales(value: str) -> int:
        if value not in sales_index:
            sales_index[value] = len(sales_values)
            sales_values.append(value)
        return sales_index[value]

    for row in rows:
        # columns-v1 rows may omit trailing nulls; pad as needed before we can index.
        if len(row) < need_len:
            row.extend([None] * (need_len - len(row)))
        customer_no = _decode_packed_cell(cols, dicts, row, cn_idx)
        origin = _decode_packed_cell(cols, dicts, row, origin_idx)
        ori_port = _decode_packed_cell(cols, dicts, row, port_idx)
        current_sales = clean_salesman_value(_decode_packed_cell(cols, dicts, row, sm_idx), missing_label)
        if clean_key(current_sales) in provisional_keys:
            row[sm_idx] = encode_sales(current_sales)
            provisional += 1
            continue
        new_sales = lookup_salesman(mapping, customer_no, origin, ori_port)
        if new_sales:
            row[sm_idx] = encode_sales(new_sales)
            matched += 1
        elif current_sales:
            row[sm_idx] = encode_sales(current_sales)
            raw_fallback += 1
        else:
            row[sm_idx] = encode_sales(missing_label)
            unmatched += 1
    dicts["Salesman_POR"] = sales_values
    shipper["d"] = dicts
    return matched, provisional, raw_fallback, unmatched


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-json", default=str(DEFAULT_DATA_JSON))
    parser.add_argument("--salesman-csv", default=None)
    parser.add_argument("--as-of", default=None, help="YYYYMMDD (default: today)")
    parser.add_argument("--missing-label", default=DEFAULT_MISSING)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    data_path = Path(args.data_json)
    if not data_path.exists():
        print(f"ERROR: data.json not found at {data_path}", file=sys.stderr)
        return 2
    salesman_path = find_salesman_csv(args.salesman_csv)

    print(f"[1/3] Loading {salesman_path.name} ...", flush=True)
    mapping = load_active_mapping(salesman_path, args.as_of)
    print(
        f"      Active mapping: {mapping['active_rows']:,} active rows; "
        f"{len(mapping['unique_customer']):,} unique CUSTOMER_NO owners.",
        flush=True,
    )

    print(f"[2/3] Reading {data_path.name} ...", flush=True)
    with data_path.open(encoding="utf-8") as fh:
        data = json.load(fh)

    print(f"[3/3] Remapping Salesman_POR in DATA.shipper ...", flush=True)
    matched, provisional, raw_fallback, unmatched = remap_shipper(data, mapping, args.missing_label)
    total = matched + provisional + raw_fallback + unmatched
    print(
        f"      Current-owner matched: {matched:,} / {total:,} rows. "
        f"Provisional new preserved: {provisional:,}. "
        f"Raw fallback: {raw_fallback:,}. "
        f"Unmatched → {args.missing_label!r}: {unmatched:,}.",
        flush=True,
    )

    obt_set = sorted(set(mapping["obt_salesmen"]) | set(data.get("provisional_obt_salesmen", [])))
    data["obt_salesmen"] = obt_set
    print(f"      OBT salesman set: {len(obt_set):,} names (KR/JP-based excluded).", flush=True)

    if args.dry_run:
        print("      Dry run - not writing back.", flush=True)
        return 0

    with data_path.open("w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False, separators=(",", ":"))
    print(f"Done. Wrote {data_path}.", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
