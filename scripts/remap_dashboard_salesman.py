"""Rewrite Salesman_POR in dist/data.json using the active customer-owner mapping
from salesman.csv (rows where today is between SALES_START_DATE and SALES_END_DATE).

The dashboard's bySales table and salesperson filter consume DATA.shipper from
dist/data.json. By post-processing the file after daily_3w_dashboard.py finishes,
both the dashboard view and the sales-target screen end up agreeing on
"who currently owns each shipper account".

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
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DATA_JSON = ROOT / "dist" / "data.json"
DEFAULT_SALESMAN_CANDIDATES = ("salesman.csv", "saleman.csv")
DEFAULT_MISSING = "(미지정)"


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


def load_active_mapping(path: Path, as_of: str | None) -> dict[str, str]:
    if as_of is None:
        as_of = datetime.now().strftime("%Y%m%d")
    as_of_int = int(as_of)
    df = pd.read_csv(path, dtype=str, encoding="utf-8-sig", low_memory=False)
    for col in ["CUSTOMER_NO", "SALESMAN_NO", "SALES_START_DATE", "SALES_END_DATE"]:
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str).str.strip()
    start = pd.to_numeric(df["SALES_START_DATE"].str.replace(".0", "", regex=False), errors="coerce")
    end = pd.to_numeric(df["SALES_END_DATE"].str.replace(".0", "", regex=False), errors="coerce")
    active = df.loc[(start <= as_of_int) & (end >= as_of_int)].copy()
    active = active.loc[active["CUSTOMER_NO"].ne("") & active["SALESMAN_NO"].ne("")]
    active["KEY"] = active["CUSTOMER_NO"].str.upper()
    active = active.drop_duplicates("KEY", keep="first")
    return active.set_index("KEY")["SALESMAN_NO"].to_dict()


def remap_shipper(data: dict, mapping: dict[str, str], missing_label: str) -> tuple[int, int]:
    shipper = data.get("shipper")
    if not isinstance(shipper, dict):
        return 0, 0
    cols = shipper.get("c", [])
    rows = shipper.get("r", [])
    if "Salesman_POR" not in cols or "BKG_SHPR_CST_NO" not in cols:
        return 0, 0
    sm_idx = cols.index("Salesman_POR")
    cn_idx = cols.index("BKG_SHPR_CST_NO")
    matched = 0
    unmatched = 0
    need_len = max(sm_idx, cn_idx) + 1
    for row in rows:
        # columns-v1 rows may omit trailing nulls; pad as needed before we can index.
        if len(row) < need_len:
            row.extend([None] * (need_len - len(row)))
        cn = row[cn_idx]
        cn_str = "" if cn is None else (cn if isinstance(cn, str) else str(cn))
        key = cn_str.strip().upper()
        new_sales = mapping.get(key)
        if new_sales:
            row[sm_idx] = new_sales
            matched += 1
        else:
            row[sm_idx] = missing_label
            unmatched += 1
    return matched, unmatched


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
    print(f"      Active mapping: {len(mapping):,} CUSTOMER_NO → SALESMAN_NO.", flush=True)

    print(f"[2/3] Reading {data_path.name} ...", flush=True)
    with data_path.open(encoding="utf-8") as fh:
        data = json.load(fh)

    print(f"[3/3] Remapping Salesman_POR in DATA.shipper ...", flush=True)
    matched, unmatched = remap_shipper(data, mapping, args.missing_label)
    total = matched + unmatched
    print(
        f"      Matched: {matched:,} / {total:,} rows. "
        f"Unmatched → {args.missing_label!r}: {unmatched:,}.",
        flush=True,
    )

    if args.dry_run:
        print("      Dry run — not writing back.", flush=True)
        return 0

    with data_path.open("w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False, separators=(",", ":"))
    print(f"Done. Wrote {data_path}.", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
