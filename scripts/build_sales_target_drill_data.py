"""Build JSON payloads consumed by the Sales Target & Progress drill-down screen.

Two outputs:

  dist/sales-target/index.json
      Small (a few hundred KB) summary used by the drill-down landing view. Carries
      per-(origin, salesman) Target / Performance / GAP rows for 1Q 2026, 2Q 2026,
      and 3Q 2026
      across the three KPIs (3W Booking Rate vs BSA, Actual Lifting Rate, High-Profit
      Customer Rate), plus A/C counts. Sourced from the Summary_All tab of the target
      workbook so the numbers match the sheet 1:1.

  dist/sales-target/data/<origin>__<salesman>__<YYYYMM>.json
      Per-month, per-salesperson, per-origin drill chunks. Each chunk contains
      shipper-level aggregates and BKG_NO-level rows. Loaded lazily by the UI to
      avoid bundling 100+MB up front. Filename uses a safe variant of the salesman
      key; the manifest carries the human-readable name.

  dist/sales-target/manifest.json
      Catalogue of available chunks plus filter facets (origin list, salesperson
      list per origin, available months).

Usage:
    py -3 scripts/build_sales_target_drill_data.py
        --workbook 1YxZkwvoMaQXIEw07qUDZtCPDFZBf8GOZyr5knkxnLxo
        [--snapshot output/booking_snapshot_result_YYYYMMDD.csv]
        [--out dist/sales-target]

Both arguments are optional: workbook ID defaults to the per-origin Target sheet,
snapshot path defaults to the latest booking_snapshot_result_*.csv in output/.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import math
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build


ROOT = Path(__file__).resolve().parents[1]
RUNTIME_ROOT = Path(os.environ.get("DASHBOARD_RUNTIME_ROOT", str(ROOT)))
DEFAULT_WORKBOOK = "1YxZkwvoMaQXIEw07qUDZtCPDFZBf8GOZyr5knkxnLxo"
SUMMARY_SHEET = "Summary_All"
MISSING_SALES = "(미지정)"
SALESMAN_CSV_CANDIDATES = ("salesman.csv", "saleman.csv")
TEAM_FILTER = "OBT"
JAPAN_TEAM = "JBT"
JAPAN_ORIGIN = "JP"
JAPAN_START_MONTH = "202604"
RAW_NEW_SALESMAN_MIN_ROWS = 3
RAW_NEW_SALESMAN_RECENT_DAYS = 45
RAW_SALESMAN_ID_RE = re.compile(r"^[A-Za-z][A-Za-z0-9._-]{1,63}$")
BSA_ROUTE_KEYS = ["team", "tab", "dest", "dst_port"]
NO_BASIS_SALES = "(no 2025 basis)"
MONTHS_2025 = frozenset(f"2025{m:02d}" for m in range(1, 13))
QUARTER_MONTHS = {
    "q1": frozenset({"202601", "202602", "202603"}),
    "q2": frozenset({"202604", "202605", "202606"}),
    "q3": frozenset({"202607", "202608", "202609"}),
    "q4": frozenset({"202610", "202611", "202612"}),
}
CN_NKG_PORTS = frozenset({
    "AIA", "AQG", "CGD", "CGS", "CKG", "CKQ", "CSX", "CZH",
    "CZX", "FLG", "HFE", "HSI", "JIA", "JIN", "JJG", "LUZ",
    "MSN", "NCH", "NKG", "NTG", "TAZ", "TCG", "TOL", "WHI",
    "WUH", "WUW", "WZH", "YCH", "YYA", "YZH", "YZR", "ZHE", "ZJG",
})


def clean_text(value: Any, fallback: str = "") -> str:
    if value is None:
        return fallback
    try:
        if pd.isna(value):
            return fallback
    except (TypeError, ValueError):
        pass
    text = str(value).strip()
    return fallback if text.lower() in {"", "nan", "none", "nat"} else text


def clean_salesman_series(series: pd.Series) -> pd.Series:
    out = series.fillna("").astype(str).str.strip()
    invalid = out.str.lower().isin({"", "nan", "none", "nat"}) | out.eq(MISSING_SALES)
    return out.mask(invalid, "")


def apply_salesman_map_with_raw_fallback(df: pd.DataFrame, salesman_map: dict[str, str] | None) -> None:
    raw_sales = clean_salesman_series(df["Salesman_POR"])
    if salesman_map:
        keys = df["BKG_SHPR_CST_NO"].str.upper()
        mapped = keys.map(salesman_map).fillna("").astype(str).str.strip()
        active_keys = {
            clean_text(value).upper()
            for value in salesman_map.values()
            if clean_text(value)
        }
        provisional = (
            raw_sales.ne("")
            & ~raw_sales.str.upper().isin(active_keys)
            & raw_sales.map(
                lambda value: bool(re.fullmatch(r"[A-Za-z][A-Za-z0-9._-]{1,63}", value))
            )
        )
        df["Salesman_POR"] = raw_sales.where(
            provisional,
            mapped.where(mapped.ne(""), raw_sales),
        )
    else:
        df["Salesman_POR"] = raw_sales
    df["Salesman_POR"] = df["Salesman_POR"].replace("", MISSING_SALES)


def safe_int(value: Any) -> int:
    text = clean_text(value)
    if not text:
        return 0
    text = text.replace(",", "")
    try:
        return int(float(text))
    except ValueError:
        return 0


def safe_float(value: Any) -> float:
    text = clean_text(value)
    if not text:
        return 0.0
    text = text.replace(",", "")
    try:
        return float(text)
    except ValueError:
        return 0.0


def parse_pct(value: Any) -> float | None:
    """Sheet values like '55.7%' or 0.557 → 0.557 (fraction). Empty → None."""
    if value is None:
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if math.isnan(value) or math.isinf(value):
            return None
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    is_percent = text.endswith("%")
    text = text.replace(",", "").rstrip("%").strip()
    try:
        num = float(text)
    except ValueError:
        return None
    if math.isnan(num) or math.isinf(num):
        return None
    return num / 100.0 if is_percent else num


def tab_key(origin: Any, ori_port: Any) -> str:
    origin_text = clean_text(origin, "UNKNOWN")
    port = clean_text(ori_port, "UNKNOWN")
    if origin_text == "CN":
        if port in CN_NKG_PORTS:
            return "CN_NKG"
        return "CN_SHK_DCB" if port in {"SHK", "DCB"} else f"CN_{port}"
    if origin_text == "VN":
        if port in {"SGN", "CMP"}:
            return "VN_SGN_CMP"
        if port == "HPH":
            return "VN_HPH"
        return f"VN_{port}"
    if origin_text == "ID":
        if port == "JKT":
            return "JKT"
        if port == "SUB":
            return "SUB"
        return "ID-IDO"
    if origin_text == "MY":
        if port in {"PKG", "PKW"}:
            return "PKG+PKW"
        if port == "PEN":
            return "PEN"
        if port == "PGU":
            return "PGU"
        return "MY_out"
    return origin_text


def classify_team(origin: Any, dest: Any) -> str:
    origin_text = clean_text(origin)
    dest_text = clean_text(dest)
    if origin_text not in ("KR", "JP") and dest_text != "KR":
        return "OBT"
    if origin_text == "KR" and dest_text != "JP":
        return "EST"
    if origin_text != "JP" and dest_text == "KR":
        return "IST"
    return "JBT"


def target_team_mask(df: pd.DataFrame) -> pd.Series:
    """OBT rows plus Japan-origin JBT rows requested from 2026 Q2 onward."""
    if "_original_target_team_scope" in df:return df["_original_target_team_scope"].astype(bool)
    team = df["team"].fillna("").astype(str).str.strip()
    tab = df["tab"].fillna("").astype(str).str.strip()
    return team.eq(TEAM_FILTER) | (team.eq(JAPAN_TEAM) & tab.eq(JAPAN_ORIGIN))


def safe_filename_token(name: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", name.strip())
    cleaned = cleaned.strip("_")
    return cleaned or "UNKNOWN"


def get_creds() -> Credentials:
    creds_dir = ROOT.parent / ".gdrive-mcp"
    credentials_path = creds_dir / "credentials.json"
    token_path = creds_dir / "token.json"
    installed = json.loads(credentials_path.read_text(encoding="utf-8-sig"))["installed"]
    token = json.loads(token_path.read_text(encoding="utf-8-sig"))
    creds = Credentials(
        token=token.get("access_token"),
        refresh_token=token.get("refresh_token"),
        token_uri="https://oauth2.googleapis.com/token",
        client_id=installed["client_id"],
        client_secret=installed["client_secret"],
        scopes=token.get("scopes") or [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    if not creds.valid:
        creds.refresh(Request())
    return creds


def find_salesman_csv() -> Path | None:
    for name in SALESMAN_CSV_CANDIDATES:
        p = ROOT / name
        if p.exists():
            return p
    return None


def cached_tab_keys(origins,ports):
    cache={};result=[]
    for origin,port in zip(origins,ports):
        key=(str(origin),str(port))
        if key not in cache:cache[key]=tab_key(origin,port)
        result.append(cache[key])
    return result


def load_active_salesman_mapping(path: Path, as_of: str | None = None) -> dict[str, str]:
    """salesman.csv → {CUSTOMER_NO_UPPER: SALESMAN_NO} for rows active on `as_of` (YYYYMMDD).

    Default `as_of` is today. SALES_END_DATE is treated as 99991231 when missing
    so that "current owner" rows (typical end date 99991231) are kept.
    """
    if as_of is None:
        as_of = datetime.now().strftime("%Y%m%d")
    as_of_int = int(as_of)
    df = pd.read_csv(path, dtype=str, encoding="utf-8-sig", low_memory=False)
    for col in ["COUNTRY", "PORT", "SALESMAN_NO", "CUSTOMER_NO", "SALES_START_DATE", "SALES_END_DATE"]:
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str).str.strip()
    start = pd.to_numeric(df["SALES_START_DATE"].str.replace(".0", "", regex=False), errors="coerce")
    end = pd.to_numeric(df["SALES_END_DATE"].str.replace(".0", "", regex=False), errors="coerce")
    active = df.loc[(start <= as_of_int) & (end >= as_of_int)].copy()
    active = active.loc[active["CUSTOMER_NO"].ne("") & active["SALESMAN_NO"].ne("")]
    active["CUSTOMER_NO_KEY"] = active["CUSTOMER_NO"].str.upper()
    active = active.drop_duplicates("CUSTOMER_NO_KEY", keep="first")
    return active.set_index("CUSTOMER_NO_KEY")["SALESMAN_NO"].to_dict()


def apply_official_history_offices(df: pd.DataFrame, owner_offices: dict[str, Any] | None, owner_map: dict[str,str] | None) -> None:
    if owner_offices is None:return
    original_scope=target_team_mask(df)
    keys=df['BKG_SHPR_CST_NO'].str.upper()
    if owner_map is not None:df['Salesman_POR']=keys.map(owner_map).fillna(MISSING_SALES)
    unique={customer:offices[0] for customer,offices in owner_offices.items() if len(offices)==1}
    homes=keys.map(unique)
    df['physical_tab']=df['tab']
    df.loc[original_scope & homes.notna(),'tab']=homes.loc[original_scope & homes.notna()]
    df['_original_target_team_scope']=original_scope


def load_w3_2025_teu(cache_path: Path, salesman_map: dict[str, str] | None, owner_offices: dict[str, Any] | None = None) -> tuple[dict[tuple[str, str], float], dict[str, float]]:
    """Return ({(tab, Salesman_POR): w3_2025_FST_TEU sum}, {tab: team total}).

    Reads _cache_2025.parquet, filters WOS-3 + OBT, optionally remaps Salesman_POR
    using salesman.csv active mapping (same rule as the 2026 pipeline), groups by
    (tab, Salesman_POR) and sums FST_TEU.
    """
    if not cache_path.exists():
        return {}, {}
    needed_cols = [
        "BKG_SHPR_CST_NO",
        "POR_CTR_CD",
        "POR_PLC_CD",
        "DLY_CTR_CD",
        "Lead_time (BKG_Sche)",
        "team",
        "Salesman_POR",
        "FST_TEU",
    ]
    try:
        import pyarrow.parquet as pq

        available_cols = set(pq.read_schema(cache_path).names)
        read_cols = [c for c in needed_cols if c in available_cols]
    except Exception:
        read_cols = needed_cols
    df = pd.read_parquet(cache_path, columns=read_cols)
    df["POR_CTR_CD"] = df["POR_CTR_CD"].fillna("").astype(str).str.strip()
    df["POR_PLC_CD"] = df["POR_PLC_CD"].fillna("").astype(str).str.strip()
    if "DLY_CTR_CD" not in df.columns:
        df["DLY_CTR_CD"] = ""
    df["DLY_CTR_CD"] = df["DLY_CTR_CD"].fillna("").astype(str).str.strip()
    df["BKG_SHPR_CST_NO"] = df["BKG_SHPR_CST_NO"].fillna("").astype(str).str.strip()
    df["Salesman_POR"] = df["Salesman_POR"].fillna("").astype(str).str.strip()
    apply_salesman_map_with_raw_fallback(df, salesman_map)
    df["tab"] = cached_tab_keys(df["POR_CTR_CD"], df["POR_PLC_CD"])
    if "team" not in df.columns:
        df["team"] = [classify_team(o, d) for o, d in zip(df["POR_CTR_CD"], df["DLY_CTR_CD"])]
    else:
        df["team"] = df["team"].fillna("").astype(str).str.strip()
    apply_official_history_offices(df,owner_offices,salesman_map)
    df["fst_num"] = pd.to_numeric(df["FST_TEU"].astype(str).str.replace(",", "", regex=False), errors="coerce").fillna(0.0)
    scoped = df.loc[
        target_team_mask(df)
        & df["Lead_time (BKG_Sche)"].astype(str).eq("WOS-3")
        & df["fst_num"].gt(0)
        & df["tab"].ne("UNKNOWN")
    ]
    by_pair = scoped.groupby(["tab", "Salesman_POR"], dropna=False)["fst_num"].sum().to_dict()
    by_tab = scoped.groupby("tab", dropna=False)["fst_num"].sum().to_dict()
    return by_pair, by_tab


def load_2025_base_cells(cache_path: Path, salesman_map: dict[str, str] | None, owner_offices: dict[str, Any] | None = None) -> dict[tuple[str, str], list[dict[str, Any]]]:
    """Return {(tab, Salesman_POR): [numerator cells]} for the 2025 base recompute.

    Each cell is a dimensional bucket keyed by (DLY country, DLY port, grade-first,
    route-hi) carrying the raw measures the frontend needs to rebuild the three KPI
    bases and the '25 share for any destination/grade/profit slice:
        nlst = Normal LST_TEU over all lead times (share numerator)
        w3f  = WOS-3 FST_TEU                       (booking-base numerator)
        w3l  = WOS-3 Normal LST_TEU                (lifting-base numerator)
        w3h  = WOS-3 route-hi FST_TEU              (high-profit-base numerator)
    Mirrors load_w3_2025_teu's scoping (OBT team, salesman.csv remap).
    """
    if not cache_path.exists():
        return {}
    needed_cols = [
        "BKG_SHPR_CST_NO",
        "POR_CTR_CD",
        "POR_PLC_CD",
        "DLY_CTR_CD",
        "DLY_PLC_CD",
        "Lead_time (BKG_Sche)",
        "LST_Status",
        "team",
        "Salesman_POR",
        "grade",
        "고/저",
        "FST_TEU",
        "LST_TEU",
    ]
    try:
        import pyarrow.parquet as pq

        available_cols = set(pq.read_schema(cache_path).names)
        read_cols = [c for c in needed_cols if c in available_cols]
    except Exception:
        read_cols = needed_cols
    df = pd.read_parquet(cache_path, columns=read_cols)
    for col in ["POR_CTR_CD", "POR_PLC_CD", "DLY_CTR_CD", "DLY_PLC_CD", "BKG_SHPR_CST_NO", "Salesman_POR", "grade", "고/저", "LST_Status"]:
        if col not in df.columns:
            df[col] = ""
        df[col] = df[col].fillna("").astype(str).str.strip()
    apply_salesman_map_with_raw_fallback(df, salesman_map)
    df["tab"] = cached_tab_keys(df["POR_CTR_CD"], df["POR_PLC_CD"])
    if "team" not in df.columns:
        df["team"] = [classify_team(o, d) for o, d in zip(df["POR_CTR_CD"], df["DLY_CTR_CD"])]
    else:
        df["team"] = df["team"].fillna("").astype(str).str.strip()
    apply_official_history_offices(df,owner_offices,salesman_map)
    df["fst_num"] = pd.to_numeric(df["FST_TEU"].astype(str).str.replace(",", "", regex=False), errors="coerce").fillna(0.0)
    df["lst_num"] = pd.to_numeric(df["LST_TEU"].astype(str).str.replace(",", "", regex=False), errors="coerce").fillna(0.0)
    df = df.loc[target_team_mask(df) & df["tab"].ne("UNKNOWN")].copy()
    if df.empty:
        return {}
    is_w3 = df["Lead_time (BKG_Sche)"].astype(str).eq("WOS-3") if "Lead_time (BKG_Sche)" in df.columns else pd.Series(False, index=df.index)
    is_normal = df["LST_Status"].eq("Normal")
    is_route_hi = df["고/저"].eq("고수익")
    df["g"] = df["grade"].str[:1].str.upper()
    df["hi"] = is_route_hi.astype(int)
    df["nlst"] = df["lst_num"].where(is_normal, 0.0)
    df["w3f"] = df["fst_num"].where(is_w3, 0.0)
    df["w3l"] = df["lst_num"].where(is_w3 & is_normal, 0.0)
    df["w3h"] = df["fst_num"].where(is_w3 & is_route_hi, 0.0)
    grouped = (
        df.groupby(["tab", "Salesman_POR", "DLY_CTR_CD", "DLY_PLC_CD", "g", "hi"], dropna=False)[["nlst", "w3f", "w3l", "w3h"]]
        .sum()
        .reset_index()
    )
    out: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in grouped.itertuples(index=False):
        nlst = round(float(row.nlst), 3)
        w3f = round(float(row.w3f), 3)
        w3l = round(float(row.w3l), 3)
        w3h = round(float(row.w3h), 3)
        if not (nlst or w3f or w3l or w3h):
            continue
        cell = {
            "dlyc": clean_text(row.DLY_CTR_CD),
            "dly": clean_text(row.DLY_PLC_CD),
            "g": clean_text(row.g),
            "hi": int(row.hi),
            "nlst": nlst,
            "w3f": w3f,
            "w3l": w3l,
            "w3h": w3h,
        }
        out.setdefault((clean_text(row.tab), clean_text(row.Salesman_POR)), []).append(cell)
    return out


def load_2025_shipper_cells(cache_path: Path, salesman_map: dict[str, str] | None, owner_offices: dict[str, Any] | None = None) -> tuple[dict[tuple[str, str], list[dict[str, Any]]], dict[tuple[str, str], dict[str, list[str]]]]:
    """Return (cells, meta):
      cells = {(tab, Salesman_POR): [{dlyc, dly, sk, nlst}]} — each shipper's 2025
              Normal LST_TEU per (route, owning salesperson).
      meta  = {(tab, Salesman_POR): {sk: [name, grade]}} — shipper name + grade,
              so the frontend can show churned shippers (2025 BSA but no 2026
              booking) as zero-activity rows (이탈화주 인지 + 합계 목표 = 구간 BSA).

    cells is the per-shipper basis the frontend uses to split a salesperson's
    allocated 2026 BSA among that salesperson's shippers BY THEIR 2025 SHARE
    (목표 = 구간BSA × 화주 2025 nlst / 영업사원 구간 2025 nlst 합). Summing the
    cells of one (tab, salesman, route) reproduces that salesperson's basis_lst in
    build_allocated_bsa, so the per-shipper targets sum back to allocated_bsa.

    Shipper identity stays as the raw BKG_SHPR_CST_NO (= booking.shipper_no), while
    Salesman_POR is remapped to the current owner — same scoping as
    load_allocation_basis / load_2025_base_cells.
    """
    if not cache_path.exists():
        return {}, {}
    needed_cols = [
        "BKG_SHPR_CST_NO",
        "BKG_SHPR_CST_ENM",
        "POR_CTR_CD",
        "POR_PLC_CD",
        "DLY_CTR_CD",
        "DLY_PLC_CD",
        "LST_Status",
        "team",
        "Salesman_POR",
        "grade",
        "LST_TEU",
    ]
    try:
        import pyarrow.parquet as pq

        available_cols = set(pq.read_schema(cache_path).names)
        read_cols = [c for c in needed_cols if c in available_cols]
    except Exception:
        read_cols = needed_cols
    df = pd.read_parquet(cache_path, columns=read_cols)
    for col in ["POR_CTR_CD", "POR_PLC_CD", "DLY_CTR_CD", "DLY_PLC_CD", "BKG_SHPR_CST_NO", "BKG_SHPR_CST_ENM", "Salesman_POR", "LST_Status", "grade"]:
        if col not in df.columns:
            df[col] = ""
        df[col] = df[col].fillna("").astype(str).str.strip()
    apply_salesman_map_with_raw_fallback(df, salesman_map)
    df["tab"] = cached_tab_keys(df["POR_CTR_CD"], df["POR_PLC_CD"])
    if "team" not in df.columns:
        df["team"] = [classify_team(o, d) for o, d in zip(df["POR_CTR_CD"], df["DLY_CTR_CD"])]
    else:
        df["team"] = df["team"].fillna("").astype(str).str.strip()
    apply_official_history_offices(df,owner_offices,salesman_map)
    df["lst_num"] = pd.to_numeric(df["LST_TEU"].astype(str).str.replace(",", "", regex=False), errors="coerce").fillna(0.0)
    df = df.loc[
        target_team_mask(df)
        & df["tab"].ne("UNKNOWN")
        & df["LST_Status"].eq("Normal")
        & df["lst_num"].gt(0)
    ].copy()
    if df.empty:
        return {}, {}
    grouped = (
        df.groupby(["tab", "Salesman_POR", "DLY_CTR_CD", "DLY_PLC_CD", "BKG_SHPR_CST_NO"], dropna=False)["lst_num"]
        .sum()
        .reset_index()
    )
    out: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in grouped.itertuples(index=False):
        nlst = round(float(row.lst_num), 3)
        if nlst <= 0:
            continue
        out.setdefault((clean_text(row.tab), clean_text(row.Salesman_POR)), []).append({
            "dlyc": clean_text(row.DLY_CTR_CD),
            "dly": clean_text(row.DLY_PLC_CD),
            "sk": clean_text(row.BKG_SHPR_CST_NO),
            "nlst": nlst,
        })
    # name/grade per (tab, salesman, shipper): pick the row with the most 2025 LST
    # so the label reflects the shipper's dominant identity that year.
    meta_grp = (
        df.groupby(["tab", "Salesman_POR", "BKG_SHPR_CST_NO", "BKG_SHPR_CST_ENM", "grade"], dropna=False)["lst_num"]
        .sum()
        .reset_index()
        .sort_values("lst_num", ascending=False)
    )
    meta: dict[tuple[str, str], dict[str, list[str]]] = {}
    for row in meta_grp.itertuples(index=False):
        key = (clean_text(row.tab), clean_text(row.Salesman_POR))
        sk = clean_text(row.BKG_SHPR_CST_NO)
        slot = meta.setdefault(key, {})
        if sk not in slot:  # first = highest-LST row for this shipper
            slot[sk] = [clean_text(row.BKG_SHPR_CST_ENM), (clean_text(row.grade)[:1].upper())]
    return out, meta


def build_base2025_payload(
    num_cells: dict[tuple[str, str], list[dict[str, Any]]],
    bsa_df: pd.DataFrame | None,
    data_date: str,
    generated_at: str,
    allowed_sales_by_origin: dict[str, set[str]] | None = None,
    shipper_cells: dict[tuple[str, str], list[dict[str, Any]]] | None = None,
    shipper_meta: dict[tuple[str, str], dict[str, list[str]]] | None = None,
) -> dict[str, Any]:
    """Combine 2025 numerator cells (A1) with 2025 allocated BSA (A2) into the
    base2025.json payload: {tab: {salesman: {"num": [...], "bsa": [...]}}}.

    BSA is a route capacity (no grade/route-hi dimension) so it is carried as a
    separate DLY-keyed list per salesman. Both lists are restricted to the
    workbook's SALES roster (`allowed_sales_by_origin`) so a Team-Total recompute
    (∑ numerators / ∑ BSA over the origin's salesmen) matches the sheet.
    """
    base: dict[str, dict[str, dict[str, list[dict[str, Any]]]]] = {}

    def allowed(tab: str, salesman: str) -> bool:
        if not allowed_sales_by_origin:
            return True
        return salesman in allowed_sales_by_origin.get(tab, set())

    def slot(tab: str, salesman: str) -> dict[str, list[dict[str, Any]]]:
        return base.setdefault(tab, {}).setdefault(salesman, {"num": [], "bsa": [], "shpr": [], "smeta": {}})

    for (tab, salesman), cells in num_cells.items():
        if not allowed(tab, salesman):
            continue
        slot(tab, salesman)["num"] = cells
    if shipper_cells:
        for (tab, salesman), cells in shipper_cells.items():
            if not allowed(tab, salesman):
                continue
            slot(tab, salesman)["shpr"] = cells
    if shipper_meta:
        for (tab, salesman), m in shipper_meta.items():
            if not allowed(tab, salesman):
                continue
            slot(tab, salesman)["smeta"] = m
    if bsa_df is not None and not bsa_df.empty:
        grouped = (
            bsa_df.groupby(["tab", "Salesman_POR", "dest", "dst_port"], dropna=False)["allocated_bsa"]
            .sum()
            .reset_index()
        )
        for row in grouped.itertuples(index=False):
            value = round(float(row.allocated_bsa or 0.0), 3)
            if value <= 0:
                continue
            tab_v = clean_text(row.tab)
            sales_v = clean_text(row.Salesman_POR)
            if not allowed(tab_v, sales_v):
                continue
            slot(tab_v, sales_v)["bsa"].append({
                "dlyc": clean_text(row.dest),
                "dly": clean_text(row.dst_port),
                "bsa": value,
            })
    return {
        "_format": "sales-target-base2025-v1",
        "generated_at": generated_at,
        "data_date": data_date,
        "base": base,
    }


def load_module(module_name: str, path: Path) -> Any:
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load {module_name} from {path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def ensure_2025_cache() -> Path:
    """Create output/_cache_2025.parquet on demand for the BSA allocation basis."""
    out = RUNTIME_ROOT / "output" / "_cache_2025.parquet"
    if out.exists():
        return out
    mod = load_module("build_2025_bsa_shipper_sheet", ROOT / "scripts" / "build_2025_bsa_shipper_sheet.py")
    df = mod.add_dashboard_fields(mod.build_booking_frame())
    if "Lead_time (BKG_Sche)" not in df.columns and "Lead_time_BKG_Sche" in df.columns:
        df["Lead_time (BKG_Sche)"] = df["Lead_time_BKG_Sche"]
    df.to_parquet(out, index=False)
    return out


def find_bsa_csv(as_of: str | None) -> Path | None:
    out_dir = RUNTIME_ROOT / "output"
    if as_of:
        exact = out_dir / f"BSA_raw_monthly3W_{as_of}.csv"
        if exact.exists():
            return exact
    candidates = sorted(out_dir.glob("BSA_raw_monthly3W_*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0] if candidates else None


def load_allocation_basis(cache_path: Path, salesman_map: dict[str, str] | None) -> pd.DataFrame:
    needed_cols = [
        "BKG_SHPR_CST_NO",
        "POR_CTR_CD",
        "POR_PLC_CD",
        "DLY_CTR_CD",
        "DLY_PLC_CD",
        "LST_Status",
        "LST_TEU",
        "YYYYMM",
        "Salesman_POR",
        "team",
    ]
    try:
        import pyarrow.parquet as pq

        available_cols = set(pq.read_schema(cache_path).names)
        read_cols = [c for c in needed_cols if c in available_cols]
    except Exception:
        read_cols = needed_cols
    df = pd.read_parquet(cache_path, columns=read_cols)
    for col in ["BKG_SHPR_CST_NO", "POR_CTR_CD", "POR_PLC_CD", "DLY_CTR_CD", "DLY_PLC_CD", "LST_Status", "YYYYMM", "Salesman_POR"]:
        if col not in df.columns:
            df[col] = ""
        df[col] = df[col].fillna("").astype(str).str.strip()
    apply_salesman_map_with_raw_fallback(df, salesman_map)
    df["tab"] = cached_tab_keys(df["POR_CTR_CD"], df["POR_PLC_CD"])
    if "team" not in df.columns:
        df["team"] = [classify_team(o, d) for o, d in zip(df["POR_CTR_CD"], df["DLY_CTR_CD"])]
    else:
        df["team"] = df["team"].fillna("").astype(str).str.strip()
    df["lst_num"] = pd.to_numeric(df["LST_TEU"].astype(str).str.replace(",", "", regex=False), errors="coerce").fillna(0.0)
    basis = df.loc[
        target_team_mask(df)
        & df["YYYYMM"].astype(str).str.startswith("2025")
        & df["LST_Status"].eq("Normal")
        & df["lst_num"].gt(0)
        & df["tab"].ne("UNKNOWN")
    ].copy()
    if basis.empty:
        return pd.DataFrame(columns=BSA_ROUTE_KEYS + ["Salesman_POR", "basis_lst"])
    basis = basis.rename(
        columns={
            "POR_CTR_CD": "origin",
            "POR_PLC_CD": "ori_port",
            "DLY_CTR_CD": "dest",
            "DLY_PLC_CD": "dst_port",
        }
    )
    return (
        basis.groupby(BSA_ROUTE_KEYS + ["Salesman_POR"], dropna=False)["lst_num"]
        .sum()
        .reset_index()
        .rename(columns={"lst_num": "basis_lst"})
    )


def load_bsa_routes(path: Path, months: set[str]) -> pd.DataFrame:
    bsa = pd.read_csv(path, dtype=str, encoding="utf-8-sig", low_memory=False)
    bsa = bsa.rename(
        columns={
            "POR_Country": "origin",
            "POR_PORT": "ori_port",
            "DLY_Country": "dest",
            "DLY_PORT": "dst_port",
            "TEU_BSA (Actual)": "route_bsa",
        }
    )
    for col in ["origin", "ori_port", "dest", "dst_port", "YYYYMM"]:
        if col not in bsa.columns:
            bsa[col] = ""
        bsa[col] = bsa[col].fillna("").astype(str).str.strip()
    if "team" in bsa.columns:
        team_source = bsa["team"]
    elif "Sales Team" in bsa.columns:
        team_source = bsa["Sales Team"]
    else:
        team_source = pd.Series([""] * len(bsa), index=bsa.index)
    bsa["team"] = team_source.fillna("").astype(str).str.strip().str.upper()
    bsa["route_bsa"] = pd.to_numeric(bsa["route_bsa"].fillna("").astype(str).str.replace(",", "", regex=False), errors="coerce").fillna(0.0)
    bsa["tab"] = [tab_key(o, p) for o, p in zip(bsa["origin"], bsa["ori_port"])]
    bsa = bsa.loc[
        target_team_mask(bsa)
        & bsa["YYYYMM"].isin(months)
        & bsa["route_bsa"].gt(0)
        & bsa["tab"].ne("UNKNOWN")
    ].copy()
    if bsa.empty:
        return pd.DataFrame(columns=["YYYYMM"] + BSA_ROUTE_KEYS + ["route_bsa"])
    return bsa.groupby(["YYYYMM"] + BSA_ROUTE_KEYS, dropna=False)["route_bsa"].sum().reset_index()


def activity_basis_from_snapshot(df: pd.DataFrame, months: set[str]) -> pd.DataFrame:
    """2026 booking-activity basis (FST_TEU) per (team, tab, dest, dst_port, Salesman_POR)
    over `months`. Used to allocate BSA on routes with NO 2025 history (new lanes) so
    their BSA isn't dropped — otherwise the booking rate goes blank for lanes that clearly
    have bookings (e.g. VN→AE port KLF: 2026 BSA + activity but no 2025 basis)."""
    cols = BSA_ROUTE_KEYS + ["Salesman_POR", "act_lst"]
    if df is None or df.empty or not months:
        return pd.DataFrame(columns=cols)
    sub = df.loc[
        target_team_mask(df)
        & df["YYYYMM"].astype(str).isin(months)
        & df["tab"].ne("UNKNOWN")
        & df["fst_teu_num"].gt(0)
    ].copy()
    if sub.empty:
        return pd.DataFrame(columns=cols)
    sub = sub.rename(columns={"DLY_CTR_CD": "dest", "DLY_PLC_CD": "dst_port"})
    return (
        sub.groupby(BSA_ROUTE_KEYS + ["Salesman_POR"], dropna=False)["fst_teu_num"]
        .sum()
        .reset_index()
        .rename(columns={"fst_teu_num": "act_lst"})
    )


def build_allocated_bsa(
    months: set[str],
    salesman_map: dict[str, str] | None,
    as_of: str | None,
    allowed_sales_by_origin: dict[str, set[str]] | None = None,
    fallback_basis: pd.DataFrame | None = None,
) -> pd.DataFrame:
    columns = ["tab", "Salesman_POR", "YYYYMM", "dest", "dst_port", "allocated_bsa"]
    if not months:
        return pd.DataFrame(columns=columns)
    bsa_path = find_bsa_csv(as_of)
    if bsa_path is None:
        print("      WARN: BSA CSV not found; detailed 3W/BSA will be blank.", flush=True)
        return pd.DataFrame(columns=columns)
    try:
        cache_2025 = ensure_2025_cache()
        basis = load_allocation_basis(cache_2025, salesman_map)
        bsa = load_bsa_routes(bsa_path, months)
    except Exception as exc:
        print(f"      WARN: failed to build allocated BSA ({exc}); detailed 3W/BSA will be blank.", flush=True)
        return pd.DataFrame(columns=columns)
    if allowed_sales_by_origin:
        basis = basis.loc[
            [
                clean_text(row.Salesman_POR) in allowed_sales_by_origin.get(clean_text(row.tab), set())
                for row in basis.itertuples(index=False)
            ]
        ].copy()
    if basis.empty or bsa.empty:
        return pd.DataFrame(columns=columns)
    basis_total = (
        basis.groupby(BSA_ROUTE_KEYS, dropna=False)["basis_lst"]
        .sum()
        .reset_index()
        .rename(columns={"basis_lst": "basis_total_lst"})
    )
    alloc = bsa.merge(basis, on=BSA_ROUTE_KEYS, how="left").merge(basis_total, on=BSA_ROUTE_KEYS, how="left")
    alloc["basis_lst"] = pd.to_numeric(alloc["basis_lst"], errors="coerce").fillna(0.0)
    alloc["basis_total_lst"] = pd.to_numeric(alloc["basis_total_lst"], errors="coerce").fillna(0.0)
    with_basis = alloc.loc[alloc["basis_lst"].gt(0) & alloc["basis_total_lst"].gt(0)].copy()
    no_basis = alloc.loc[alloc["basis_total_lst"].le(0)].copy()
    pieces: list[pd.DataFrame] = []
    if not with_basis.empty:
        with_basis["allocated_bsa"] = with_basis["route_bsa"] * with_basis["basis_lst"] / with_basis["basis_total_lst"]
        pieces.append(with_basis)
    if not no_basis.empty:
        # Routes carrying 2026 BSA but with NO 2025 history (new lanes). Allocate by 2026
        # booking activity so the BSA isn't dropped — otherwise the booking rate is blank
        # for lanes that clearly have bookings (e.g. VN→AE/KLF). Legacy NO_BASIS pseudo-
        # salesperson is used only when neither a roster nor an activity basis is available.
        fb = fallback_basis
        if fb is not None and not fb.empty:
            if allowed_sales_by_origin:
                fb = fb.loc[
                    [
                        clean_text(row.Salesman_POR) in allowed_sales_by_origin.get(clean_text(row.tab), set())
                        for row in fb.itertuples(index=False)
                    ]
                ].copy()
            if not fb.empty:
                fb_total = (
                    fb.groupby(BSA_ROUTE_KEYS, dropna=False)["act_lst"]
                    .sum()
                    .reset_index()
                    .rename(columns={"act_lst": "act_total"})
                )
                nb_routes = no_basis[["YYYYMM"] + BSA_ROUTE_KEYS + ["route_bsa"]].drop_duplicates()
                nb = nb_routes.merge(fb, on=BSA_ROUTE_KEYS, how="inner").merge(fb_total, on=BSA_ROUTE_KEYS, how="left")
                nb["act_total"] = pd.to_numeric(nb["act_total"], errors="coerce").fillna(0.0)
                nb = nb.loc[nb["act_total"].gt(0)].copy()
                if not nb.empty:
                    nb["allocated_bsa"] = nb["route_bsa"] * nb["act_lst"] / nb["act_total"]
                    pieces.append(nb)
        elif not allowed_sales_by_origin:
            no_basis = no_basis.copy()
            no_basis["Salesman_POR"] = NO_BASIS_SALES
            no_basis["allocated_bsa"] = no_basis["route_bsa"]
            pieces.append(no_basis)
    if not pieces:
        return pd.DataFrame(columns=columns)
    alloc = pd.concat(pieces, ignore_index=True)
    out = (
        alloc.groupby(["tab", "Salesman_POR", "YYYYMM", "dest", "dst_port"], dropna=False)["allocated_bsa"]
        .sum()
        .reset_index()
    )
    print(
        f"      Allocated BSA from {bsa_path.name}: {len(out):,} origin/sales/month/destination rows.",
        flush=True,
    )
    return out


def latest_snapshot() -> Path:
    candidates: list[tuple[float, Path]] = []
    for path in (RUNTIME_ROOT / "output").glob("booking_snapshot_result_*.csv"):
        if re.fullmatch(r"booking_snapshot_result_\d{8}", path.stem):
            candidates.append((path.stat().st_mtime, path))
    if not candidates:
        raise FileNotFoundError("No booking_snapshot_result_YYYYMMDD.csv found under output/")
    return max(candidates)[1]


def fetch_summary_rows(service: Any, workbook_id: str) -> list[list[Any]]:
    resp = (
        service.spreadsheets()
        .values()
        .get(
            spreadsheetId=workbook_id,
            range=f"{SUMMARY_SHEET}!A1:AC",
            valueRenderOption="UNFORMATTED_VALUE",
        )
        .execute()
    )
    return resp.get("values", [])


def parse_summary(rows: list[list[Any]]) -> list[dict[str, Any]]:
    """Sheet header rows live at A2:AC4; data starts at row 5 (1-based). Columns:
        A Tab, B Name, C 2025_share, D booking_2025_base,
        E booking_q1_target, F booking_q1_perform, G booking_q1_gap,
        H booking_q2_target, I booking_q2_progress, J booking_q2_gap,
        K lifting_q1_target, L lifting_q1_perform, M lifting_q1_gap,
        N lifting_q2_target, O lifting_q2_progress, P lifting_q2_gap,
        Q hp_q1_target, R hp_q1_perform, S hp_q1_gap,
        T hp_q2_target, U hp_q2_progress, V hp_q2_gap,
        W ac_total, X ac_w3, Y ac_pct,
        Z booking_2025_base, AA lifting_2025_base, AB hp_2025_base, AC row_type.

        Summary_All A:AC supplies Q1/Q2 target columns only. Q3/Q4 source
        targets are unavailable. Keep their actual monthly/quarter progress
        without copying an earlier quarter's targets or inventing a gap.
    """
    parsed: list[dict[str, Any]] = []
    for raw in rows[4:]:  # data starts at row 5 (index 4)
        if not raw:
            continue
        tab = clean_text(raw[0] if len(raw) > 0 else "")
        name = clean_text(raw[1] if len(raw) > 1 else "")
        if not tab or not name:
            continue
        sheet_row_type = clean_text(raw[28] if len(raw) > 28 else "").upper()
        row_type = sheet_row_type if sheet_row_type in {"TOTAL", "SALES"} else (
            "TOTAL" if name.lower() in {"team total", "total"} else "SALES"
        )
        parsed.append({
            "tab": tab,
            "name": name,
            "row_type": row_type,
            "share_2025": parse_pct(raw[2] if len(raw) > 2 else None),
            "booking_base_2025": parse_pct(raw[3] if len(raw) > 3 else None),
            "w3_2025_teu": None,
            "kpi": {
                "booking": {
                    "q1": {
                        "target": parse_pct(raw[4] if len(raw) > 4 else None),
                        "perform": parse_pct(raw[5] if len(raw) > 5 else None),
                        "gap": parse_pct(raw[6] if len(raw) > 6 else None),
                    },
                    "q2": {
                        "target": parse_pct(raw[7] if len(raw) > 7 else None),
                        "progress": parse_pct(raw[8] if len(raw) > 8 else None),
                        "gap": parse_pct(raw[9] if len(raw) > 9 else None),
                    },
                    "q3": {
                        "target": None,
                        "progress": None,
                        "gap": None,
                    },
                    "q4": {
                        "target": None,
                        "progress": None,
                        "gap": None,
                    },
                },
                "lifting": {
                    "q1": {
                        "target": parse_pct(raw[10] if len(raw) > 10 else None),
                        "perform": parse_pct(raw[11] if len(raw) > 11 else None),
                        "gap": parse_pct(raw[12] if len(raw) > 12 else None),
                    },
                    "q2": {
                        "target": parse_pct(raw[13] if len(raw) > 13 else None),
                        "progress": parse_pct(raw[14] if len(raw) > 14 else None),
                        "gap": parse_pct(raw[15] if len(raw) > 15 else None),
                    },
                    "q3": {
                        "target": None,
                        "progress": None,
                        "gap": None,
                    },
                    "q4": {
                        "target": None,
                        "progress": None,
                        "gap": None,
                    },
                },
                "high_profit": {
                    "q1": {
                        "target": parse_pct(raw[16] if len(raw) > 16 else None),
                        "perform": parse_pct(raw[17] if len(raw) > 17 else None),
                        "gap": parse_pct(raw[18] if len(raw) > 18 else None),
                    },
                    "q2": {
                        "target": parse_pct(raw[19] if len(raw) > 19 else None),
                        "progress": parse_pct(raw[20] if len(raw) > 20 else None),
                        "gap": parse_pct(raw[21] if len(raw) > 21 else None),
                    },
                    "q3": {
                        "target": None,
                        "progress": None,
                        "gap": None,
                    },
                    "q4": {
                        "target": None,
                        "progress": None,
                        "gap": None,
                    },
                },
            },
            "accounts": {
                "total": safe_int(raw[22] if len(raw) > 22 else 0),
                "w3": safe_int(raw[23] if len(raw) > 23 else 0),
                "pct": parse_pct(raw[24] if len(raw) > 24 else None),
            },
        })
    return parsed


SNAPSHOT_COLUMNS = [
    "BKG_NO",
    "BKG_SHPR_CST_NO",
    "BKG_SHPR_CST_ENM",
    "POR_CTR_CD",
    "POR_PLC_CD",
    "POL_PORT_CD",
    "POD_CTR_CD",
    "POD_PORT_CD",
    "DLY_CTR_CD",
    "DLY_PLC_CD",
    "VSL_CD",
    "VOY_NO",
    "Booking_date",
    "Booking_schedule",
    "Cancel_date",
    "FST_TEU",
    "LST_Status",
    "CM1",
    "LST_TEU",
    "LST_route",
    "LST_VSL",
    "LST_VOY",
    "grade",
    "CM1/TEU",
    "week_start_date",
    "YYYYMM",
    "Lead_time (BKG_Sche)",
    "YYYYMM_BKG_Sche",
    "Salesman_POR",
    "고/저",
    "고수익태그",
]


OPTIONAL_SNAPSHOT_COLUMNS = ["WOS_LST_Status", "WOS_LST_TEU", "WOS_CM1", "BL_NO", "Related_BKG_NO", "Performance_Metric_Kind", "Source_Salesman"]


def load_snapshot(path: Path, salesman_map: dict[str, str] | None = None) -> pd.DataFrame:
    df = pd.read_csv(
        path,
        usecols=lambda name: name in SNAPSHOT_COLUMNS or name in OPTIONAL_SNAPSHOT_COLUMNS,
        dtype=str,
        encoding="utf-8-sig",
        low_memory=False,
    )
    for col in SNAPSHOT_COLUMNS:
        df[col] = df[col].fillna("").astype(str).str.strip()

    # Prefer current owner mapping, but keep the raw snapshot owner when the
    # customer is absent from salesman.csv so real performance rows are not lost.
    apply_salesman_map_with_raw_fallback(df, salesman_map)
    df["team"] = [classify_team(o, d) for o, d in zip(df["POR_CTR_CD"], df["DLY_CTR_CD"])]
    df["tab"] = cached_tab_keys(df["POR_CTR_CD"], df["POR_PLC_CD"])
    df["fst_teu_num"] = pd.to_numeric(df["FST_TEU"].str.replace(",", "", regex=False), errors="coerce").fillna(0.0)
    df["lst_teu_num"] = pd.to_numeric(df["LST_TEU"].str.replace(",", "", regex=False), errors="coerce").fillna(0.0)
    df["cm1_num"] = pd.to_numeric(df["CM1"].str.replace(",", "", regex=False), errors="coerce").fillna(0.0)
    df["cm1_per_teu_num"] = pd.to_numeric(df["CM1/TEU"].str.replace(",", "", regex=False), errors="coerce").fillna(0.0)
    df["is_w3"] = df["Lead_time (BKG_Sche)"].eq("WOS-3")
    status = df["LST_Status"].astype(str).str.strip()
    df["is_normal"] = status.eq("Normal")
    df["is_route_hi"] = df["고/저"].str.contains("고수익", na=False)
    df["is_hi"] = df["is_route_hi"]
    df["norm_lst_teu_num"] = df["lst_teu_num"].where(df["is_normal"], 0.0)
    df["norm_cm1_num"] = df["cm1_num"].where(df["is_normal"] & df["cm1_num"].ne(0), 0.0)
    # WOS is the original booking cohort conversion; physical lifting uses BL rows.
    for optional in OPTIONAL_SNAPSHOT_COLUMNS:
        if optional in df:
            df[optional] = df[optional].fillna("").astype(str).str.strip()
    wos_status = df["WOS_LST_Status"] if "WOS_LST_Status" in df else df["LST_Status"]
    wos_teu = df["WOS_LST_TEU"] if "WOS_LST_TEU" in df else df["LST_TEU"]
    wos_cm1 = df["WOS_CM1"] if "WOS_CM1" in df else df["CM1"]
    wos_normal = wos_status.str.lower().eq("normal")
    df["wos_norm_lst_teu_num"] = pd.to_numeric(wos_teu, errors="coerce").fillna(0).where(wos_normal, 0.0)
    df["wos_norm_cm1_num"] = pd.to_numeric(wos_cm1, errors="coerce").fillna(0).where(wos_normal, 0.0)
    df["is_lifted"] = df["is_normal"]
    df["is_cancel"] = status.str.contains("캔슬|cancel", case=False, na=False) | df["Cancel_date"].ne("")
    return df


def sales_target_scope_mask(df: pd.DataFrame) -> pd.Series:
    """Rows that contribute to Sales Target metrics.

    The main dashboard includes zero-FST rows in the actual lifting numerator
    when they have Normal LST_TEU, so the Sales Target chunks must keep them too.
    """
    if "Original_Target_Scope" in df:
        return df["Original_Target_Scope"].fillna(False).astype(bool)
    customer = df["BKG_SHPR_CST_NO"].fillna("").astype(str).str.strip()
    valid_customer = customer.ne("") & ~customer.str.lower().isin({"nan", "none", "null", "nat"})
    return (
        target_team_mask(df)
        & (df["fst_teu_num"].gt(0) | df["norm_lst_teu_num"].gt(0) | df["wos_norm_lst_teu_num"].gt(0))
        & df["YYYYMM"].ne("")
        & df["tab"].ne("UNKNOWN")
        & (~df["tab"].eq(JAPAN_ORIGIN) | df["YYYYMM"].ge(JAPAN_START_MONTH))
        & valid_customer
    )


def _salesman_activity_dates(df: pd.DataFrame) -> pd.Series:
    for col in ("Booking_schedule", "week_start_date", "Booking_date"):
        if col not in df.columns:
            continue
        text = df[col].fillna("").astype(str).str.strip()
        normalized = (
            text.str.replace("년 ", "-", regex=False)
            .str.replace("월 ", "-", regex=False)
            .str.replace("일", "", regex=False)
        )
        parsed = pd.to_datetime(normalized, errors="coerce")
        if parsed.notna().any():
            return parsed
    return pd.Series(pd.NaT, index=df.index, dtype="datetime64[ns]")


def provisional_salespeople_from_snapshot(
    df: pd.DataFrame,
    salesman_map: dict[str, str] | None,
    as_of: str,
) -> dict[str, list[str]]:
    """Return recent live salesperson IDs absent from the roster, grouped by tab."""
    if not salesman_map or df.empty:
        return {}

    active_keys = {
        clean_text(value).upper()
        for value in salesman_map.values()
        if clean_text(value)
    }
    sales = clean_salesman_series(df["Salesman_POR"])
    sales_keys = sales.str.upper()
    simple = sales.map(lambda value: bool(RAW_SALESMAN_ID_RE.fullmatch(value)))
    unregistered = sales_keys.ne("") & ~sales_keys.isin(active_keys)
    scoped = sales_target_scope_mask(df)

    dates = _salesman_activity_dates(df)
    try:
        as_of_date = pd.Timestamp(datetime.strptime(as_of, "%Y%m%d"))
    except (TypeError, ValueError):
        as_of_date = dates.max()
    if pd.isna(as_of_date):
        recent = pd.Series(True, index=df.index)
    else:
        recent = dates.ge(as_of_date - pd.Timedelta(days=RAW_NEW_SALESMAN_RECENT_DAYS))

    candidates = df.loc[scoped & simple & unregistered & recent, ["tab"]].copy()
    candidates["sales"] = sales[scoped & simple & unregistered & recent].values
    if candidates.empty:
        return {}
    counts = candidates.groupby(["tab", "sales"], dropna=False).size()
    accepted = counts[counts >= RAW_NEW_SALESMAN_MIN_ROWS]
    grouped: dict[str, list[str]] = {}
    for tab, name in accepted.index:
        tab_text = clean_text(tab)
        name_text = clean_text(name)
        if tab_text and name_text:
            grouped.setdefault(tab_text, []).append(name_text)
    return {
        tab: sorted(set(names))
        for tab, names in grouped.items()
    }


def provisional_customer_owner_mapping(
    df: pd.DataFrame,
    salesman_map: dict[str, str] | None,
    as_of: str,
) -> dict[str, str]:
    """Map customers of accepted live-only salespeople to their current owner.

    The augmented mapping is reused for 2025 numerator/BSA cells so the live
    dashboard's '25 share/base columns and target basis follow the same current
    customer ownership as the Target Workbook.
    """
    provisional = provisional_salespeople_from_snapshot(df, salesman_map, as_of)
    accepted_pairs = {
        (clean_text(tab), clean_text(name))
        for tab, names in provisional.items()
        for name in names
    }
    if not accepted_pairs:
        return {}

    try:
        as_of_date = pd.Timestamp(datetime.strptime(as_of, "%Y%m%d"))
    except (TypeError, ValueError):
        as_of_date = _salesman_activity_dates(df).max()
    dates = _salesman_activity_dates(df)
    if pd.isna(as_of_date):
        recent = pd.Series(True, index=df.index)
    else:
        recent = (
            dates.ge(as_of_date - pd.Timedelta(days=RAW_NEW_SALESMAN_RECENT_DAYS))
            & dates.le(as_of_date)
        )

    sales = clean_salesman_series(df["Salesman_POR"])
    customers = df["BKG_SHPR_CST_NO"].fillna("").astype(str).str.strip().str.upper()
    pairs = pd.Series(
        list(zip(df["tab"].map(clean_text), sales.map(clean_text))),
        index=df.index,
        dtype=object,
    )
    mask = sales_target_scope_mask(df) & recent & pairs.isin(accepted_pairs) & customers.ne("")
    if not mask.any():
        return {}

    votes = pd.DataFrame(
        {
            "customer": customers.loc[mask],
            "salesman": sales.loc[mask],
        }
    )
    winners = (
        votes.groupby(["customer", "salesman"], dropna=False)
        .size()
        .reset_index(name="rows")
        .sort_values(["customer", "rows", "salesman"], ascending=[True, False, True])
        .drop_duplicates("customer", keep="first")
    )
    return winners.set_index("customer")["salesman"].to_dict()


def _empty_kpi() -> dict[str, dict[str, dict[str, float | None]]]:
    return {
        key: {
            "q1": {"target": None, "perform": None, "gap": None},
            "q2": {"target": None, "progress": None, "gap": None},
            "q3": {"target": None, "progress": None, "gap": None},
            "q4": {"target": None, "progress": None, "gap": None},
        }
        for key in ("booking", "lifting", "high_profit")
    }


def _provisional_summary_row(tab: str, name: str, df: pd.DataFrame) -> dict[str, Any]:
    q1 = df.loc[
        df["tab"].eq(tab)
        & df["Salesman_POR"].eq(name)
        & df["YYYYMM"].isin(QUARTER_MONTHS["q1"])
    ]
    account_total = int(q1.loc[q1["BKG_SHPR_CST_NO"].ne(""), "BKG_SHPR_CST_NO"].nunique())
    account_w3 = int(
        q1.loc[q1["is_w3"] & q1["BKG_SHPR_CST_NO"].ne(""), "BKG_SHPR_CST_NO"].nunique()
    )
    return {
        "tab": tab,
        "name": name,
        "row_type": "SALES",
        "source": "live_booking_provisional",
        "share_2025": None,
        "booking_base_2025": None,
        "w3_2025_teu": None,
        "kpi": _empty_kpi(),
        "accounts": {
            "total": account_total,
            "w3": account_w3,
            "pct": (account_w3 / account_total) if account_total else None,
        },
    }


def add_provisional_salespeople_to_summary(
    rows: list[dict[str, Any]],
    df: pd.DataFrame,
    salesman_map: dict[str, str] | None,
    as_of: str,
) -> list[tuple[str, str]]:
    """Add live provisional salespeople to target rows before chunk allow-listing."""
    provisional = provisional_salespeople_from_snapshot(df, salesman_map, as_of)
    existing = {
        (clean_text(row.get("tab")), clean_text(row.get("name")))
        for row in rows
        if row.get("row_type") == "SALES"
    }
    additions = {
        tab: [name for name in names if (tab, name) not in existing]
        for tab, names in provisional.items()
    }
    additions = {tab: names for tab, names in additions.items() if names}
    if not additions:
        return []

    merged: list[dict[str, Any]] = []
    inserted_tabs: set[str] = set()
    for index, row in enumerate(rows):
        merged.append(row)
        tab = clean_text(row.get("tab"))
        next_tab = clean_text(rows[index + 1].get("tab")) if index + 1 < len(rows) else ""
        if tab in additions and tab != next_tab:
            merged.extend(_provisional_summary_row(tab, name, df) for name in additions[tab])
            inserted_tabs.add(tab)
    for tab in sorted(set(additions) - inserted_tabs):
        merged.extend(_provisional_summary_row(tab, name, df) for name in additions[tab])
    rows[:] = merged
    return [
        (tab, name)
        for tab in sorted(additions)
        for name in additions[tab]
    ]


def aggregate_chunks(
    df: pd.DataFrame,
    out_dir: Path,
    bsa_allocations: pd.DataFrame | None = None,
    allowed_sales_by_origin: dict[str, set[str]] | None = None,
) -> dict[str, Any]:
    """Filter to OBT scope, drop rows with no origin tab, then write per-(tab, salesman, YYYYMM) JSON chunks."""
    scoped = df.loc[sales_target_scope_mask(df)].copy()
    if allowed_sales_by_origin:
        scoped = scoped.loc[
            [
                clean_text(row.Salesman_POR) in allowed_sales_by_origin.get(clean_text(row.tab), set())
                for row in scoped.itertuples(index=False)
            ]
        ].copy()

    chunk_dir = out_dir / "data"
    chunk_dir.mkdir(parents=True, exist_ok=True)
    for old_chunk in chunk_dir.glob("*.json"):
        old_chunk.unlink()

    manifest: dict[str, Any] = {
        "chunks": [],
        "origins": sorted(scoped["tab"].dropna().unique().tolist()),
        "months": sorted(scoped["YYYYMM"].dropna().unique().tolist()),
        "salespeople_by_origin": {},
        "dest_countries": [],
        "dest_ports_by_country": {},
    }

    grouped_origin_sales = scoped.groupby("tab", dropna=False)["Salesman_POR"].apply(lambda s: sorted(set(s)))
    for origin, names in grouped_origin_sales.items():
        manifest["salespeople_by_origin"][origin] = list(names)

    # Build the destination catalogue used by the new "도착국가 / 도착포트" filters.
    dest = scoped[["POD_CTR_CD", "POD_PORT_CD"]].copy()
    dest = dest.loc[dest["POD_CTR_CD"].ne("")]
    manifest["dest_countries"] = sorted(dest["POD_CTR_CD"].dropna().unique().tolist())
    by_country: dict[str, list[str]] = {}
    for ctr, group in dest.groupby("POD_CTR_CD", dropna=False):
        ports = sorted({p for p in group["POD_PORT_CD"].dropna().unique().tolist() if p})
        by_country[ctr] = ports
    manifest["dest_ports_by_country"] = by_country

    chunk_count = 0
    bkg_total = 0
    # Per-(tab, salesman, YYYYMM) live W3 metrics, collected straight from the chunk
    # totals so the index.json can carry month-level Progress that matches what the
    # Sales Target screen recomputes live (booking=w3f/bsa, lifting=w3l/w3f, hp=w3h/w3f).
    month_metrics: dict[str, dict[str, dict[str, dict[str, float]]]] = {}

    def _record_month_metrics(origin: str, salesman: str, yyyymm: str, totals: dict[str, Any]) -> None:
        month_metrics.setdefault(origin, {}).setdefault(salesman, {})[yyyymm] = {
            "w3f": float(totals.get("w3_fst") or 0.0),
            "goal_w3f": float(totals.get("goal_basis_w3_fst", totals.get("w3_fst")) or 0.0),
            "w3l": float(totals.get("w3_lst") or 0.0),
            "w3h": float(totals.get("w3_hi_fst") or 0.0),
            "bsa": float(totals.get("allocated_bsa") or 0.0),
        }

    bsa_groups: dict[tuple[str, str, str], pd.DataFrame] = {}
    if bsa_allocations is not None and not bsa_allocations.empty:
        for key, group in bsa_allocations.groupby(["tab", "Salesman_POR", "YYYYMM"], dropna=False):
            bsa_groups[key] = group
    written_keys: set[tuple[str, str, str]] = set()
    for (origin, salesman, yyyymm), block in scoped.groupby(["tab", "Salesman_POR", "YYYYMM"], dropna=False):
        if not origin or not salesman or not yyyymm:
            continue
        written_keys.add((origin, salesman, yyyymm))
        chunk = build_chunk(origin, salesman, yyyymm, block, bsa_groups.get((origin, salesman, yyyymm)))
        _record_month_metrics(origin, salesman, yyyymm, chunk["totals"])
        token = "__".join([safe_filename_token(origin), safe_filename_token(salesman), safe_filename_token(yyyymm)])
        path = chunk_dir / f"{token}.json"
        path.write_text(json.dumps(chunk, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
        manifest["chunks"].append({
            "origin": origin,
            "salesman": salesman,
            "yyyymm": yyyymm,
            "file": f"data/{path.name}",
            "rows": len(chunk["bookings"]),
            "shippers": len(chunk["shippers"]),
        })
        chunk_count += 1
        bkg_total += len(chunk["bookings"])

    for (origin, salesman, yyyymm), group in bsa_groups.items():
        if (origin, salesman, yyyymm) in written_keys or not origin or not salesman or not yyyymm:
            continue
        chunk = build_empty_bsa_chunk(origin, salesman, yyyymm, group)
        _record_month_metrics(origin, salesman, yyyymm, chunk["totals"])
        token = "__".join([safe_filename_token(origin), safe_filename_token(salesman), safe_filename_token(yyyymm)])
        path = chunk_dir / f"{token}.json"
        path.write_text(json.dumps(chunk, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
        manifest["chunks"].append({
            "origin": origin,
            "salesman": salesman,
            "yyyymm": yyyymm,
            "file": f"data/{path.name}",
            "rows": 0,
            "shippers": 0,
        })
        chunk_count += 1

    manifest["chunk_count"] = chunk_count
    manifest["bkg_rows"] = bkg_total
    return manifest, month_metrics


def _month_progress_rates(m: dict[str, float]) -> dict[str, float | None]:
    """Live progress fractions for one month bucket, matching the frontend:
    booking = w3_fst / allocated_bsa, lifting = w3_lst / w3_fst, hp = w3_hi_fst / w3_fst."""
    w3f, bsa = m.get("w3f", 0.0), m.get("bsa", 0.0)
    rnd = lambda x: round(x, 4) if x is not None else None
    return {
        "b": rnd(w3f / bsa) if bsa else None,
        "l": rnd(m.get("w3l", 0.0) / w3f) if w3f else None,
        "h": rnd(m.get("w3h", 0.0) / w3f) if w3f else None,
    }


def attach_month_progress(rows: list[dict[str, Any]], month_metrics: dict[str, Any]) -> None:
    """Add row['month_progress'] = {YYYYMM: {b, l, h}} of live W3 progress so the main
    dashboard's target overlay can show the SELECTED MONTH's progress (matching the
    Sales Target screen) instead of the workbook's static quarter snapshot."""
    for row in rows:
        tab = clean_text(row.get("tab"))
        per_sales = month_metrics.get(tab)
        if not per_sales:
            continue
        mp: dict[str, dict[str, float | None]] = {}
        if row.get("row_type") in {"SALES", "PERFORMANCE_ONLY"}:
            for ym, m in (per_sales.get(clean_text(row.get("name"))) or {}).items():
                mp[ym] = _month_progress_rates(m)
        elif row.get("row_type") == "TOTAL":
            agg: dict[str, dict[str, float]] = {}
            for byym in per_sales.values():
                for ym, m in byym.items():
                    a = agg.setdefault(ym, {"w3f": 0.0, "w3l": 0.0, "w3h": 0.0, "bsa": 0.0, "goal_w3f": 0.0})
                    for k in a:
                        a[k] += m.get(k, 0.0)
            for ym, m in agg.items():
                mp[ym] = _month_progress_rates(m)
        if mp:
            row["month_progress"] = mp


def _period_metrics(per_month: dict[str, dict[str, float]], months: frozenset[str]) -> dict[str, float | None]:
    agg = {"w3f": 0.0, "w3l": 0.0, "w3h": 0.0, "bsa": 0.0, "goal_w3f": 0.0}
    for ym, vals in (per_month or {}).items():
        if ym not in months:
            continue
        for key in agg:
            agg[key] += float(vals.get(key, vals.get("w3f") if key=="goal_w3f" else 0.0) or 0.0)
    w3f = agg["w3f"]
    return {
        "booking": (agg["w3f"] / agg["bsa"]) if agg["bsa"] else None,
        "lifting": (agg["w3l"] / w3f) if w3f else None,
        "high_profit": (agg["w3h"] / w3f) if w3f else None,
    }


def _row_month_metrics(row: dict[str, Any], month_metrics: dict[str, Any]) -> dict[str, dict[str, float]]:
    tab = clean_text(row.get("tab"))
    per_sales = month_metrics.get(tab, {})
    if row.get("row_type") in {"SALES", "PERFORMANCE_ONLY"}:
        return per_sales.get(clean_text(row.get("name")), {}) or {}
    if row.get("row_type") != "TOTAL":
        return {}
    agg: dict[str, dict[str, float]] = {}
    for by_month in per_sales.values():
        for ym, vals in by_month.items():
            bucket = agg.setdefault(ym, {"w3f": 0.0, "w3l": 0.0, "w3h": 0.0, "bsa": 0.0, "goal_w3f": 0.0})
            for key in bucket:
                bucket[key] += float(vals.get(key, vals.get("w3f") if key=="goal_w3f" else 0.0) or 0.0)
    return agg


def attach_missing_quarter_progress(rows: list[dict[str, Any]], month_metrics: dict[str, Any]) -> None:
    """Fill missing quarter perform/progress fields from generated monthly metrics."""
    for row in rows:
        per_month = _row_month_metrics(row, month_metrics)
        if not per_month:
            continue
        for quarter, months in QUARTER_MONTHS.items():
            perform_key = "perform" if quarter == "q1" else "progress"
            rates = _period_metrics(per_month, months)
            for kpi, actual in rates.items():
                cell = row["kpi"][kpi][quarter]
                if cell.get(perform_key) is not None:
                    continue
                target = cell.get("target")
                cell[perform_key] = actual
                cell["gap"] = (actual - target) if actual is not None and target is not None else None


def attach_quarter_metrics(rows: list[dict[str, Any]], month_metrics: dict[str, Any]) -> None:
    """Add row['quarter_metrics'] = {q: {w3f, w3l, w3h, bsa}} (raw TEU sums) so the
    frontend can aggregate multiple rows as a ratio-of-sums (∑num / ∑den) instead of
    an account-weighted mean of ratios. mean(x_i/y_i) explodes when one tab has a
    structurally tiny denominator (e.g. ID-IDO outport BSA is booked at the mother
    port), which inflated the multi-origin KPI cards vs the -3W dashboard."""
    for row in rows:
        per_month = _row_month_metrics(row, month_metrics)
        if not per_month:
            continue
        qm: dict[str, dict[str, float]] = {}
        for quarter, months in QUARTER_MONTHS.items():
            agg = {"w3f": 0.0, "w3l": 0.0, "w3h": 0.0, "bsa": 0.0, "goal_w3f": 0.0}
            for ym, vals in per_month.items():
                if ym not in months:
                    continue
                for key in agg:
                    agg[key] += float(vals.get(key, vals.get("w3f") if key=="goal_w3f" else 0.0) or 0.0)
            if any(agg.values()):
                qm[quarter] = {key: round(value, 3) for key, value in agg.items()}
        if qm:
            row["quarter_metrics"] = qm


def serialize_bsa_allocations(bsa_block: pd.DataFrame | None) -> tuple[float, list[dict[str, Any]]]:
    if bsa_block is None or bsa_block.empty:
        return 0.0, []
    keys=["dest","dst_port"]+[key for key in ('physical_tab','shipper_no') if key in bsa_block]
    grouped = (
        bsa_block.groupby(keys, dropna=False)["allocated_bsa"]
        .sum()
        .reset_index()
    )
    rows: list[dict[str, Any]] = []
    total = 0.0
    for item in grouped.itertuples(index=False):
        value = float(item.allocated_bsa or 0.0)
        total += value
        rows.append({
            "pod_country": clean_text(item.dest),
            "pod": clean_text(item.dst_port),
            "allocated_bsa": value,
            **({"physical_origin":clean_text(item.physical_tab)} if 'physical_tab' in keys else {}),
            **({"shipper_no":clean_text(item.shipper_no)} if 'shipper_no' in keys else {}),
        })
    return total, rows


def build_empty_bsa_chunk(origin: str, salesman: str, yyyymm: str, bsa_block: pd.DataFrame) -> dict[str, Any]:
    allocated_bsa, bsa_rows = serialize_bsa_allocations(bsa_block)
    capacity_only = salesman == NO_BASIS_SALES
    if capacity_only:
        for row in bsa_rows:row["goal_eligible"] = False
    return {
        "capacity_only": capacity_only,
        "origin": origin,
        "salesman": salesman,
        "yyyymm": yyyymm,
        "totals": {
            "bkg_count": 0,
            "shipper_count": 0,
            "fst_teu": 0.0,
            "lst_teu": 0.0,
            "cm1": 0.0,
            "w3_fst": 0.0,
            "w3_lst": 0.0,
            "w3_hi_fst": 0.0,
            "allocated_bsa": allocated_bsa,
            "lst_rate_w3": None,
            "hi_share_w3": None,
        },
        "shippers": [],
        "bsa_allocations": bsa_rows,
        "bookings": [],
    }


def build_chunk(origin: str, salesman: str, yyyymm: str, block: pd.DataFrame, bsa_block: pd.DataFrame | None = None) -> dict[str, Any]:
    """Per-(origin, salesman, YYYYMM) chunk: shipper aggregates + BKG_NO list."""
    bookings: list[dict[str, Any]] = []
    for _, r in block.iterrows():
        bookings.append({
            "bkg_no": r["BKG_NO"],
            "shipper_no": r["BKG_SHPR_CST_NO"],
            "shipper_name": r["BKG_SHPR_CST_ENM"],
            "pol": r["POL_PORT_CD"],
            "pod_country": r["POD_CTR_CD"],
            "pod": r["POD_PORT_CD"],
            "dly_country": r["DLY_CTR_CD"],
            "dly_plc": r["DLY_PLC_CD"],
            "vsl": r["VSL_CD"],
            "voy": r["VOY_NO"],
            "lst_vsl": r["LST_VSL"],
            "lst_voy": r["LST_VOY"],
            "lst_route": r["LST_route"],
            "booking_date": r["Booking_date"],
            "booking_schedule": r["Booking_schedule"],
            "cancel_date": r["Cancel_date"],
            "week_start_date": r["week_start_date"],
            "lead_time_bkg_sche": r["Lead_time (BKG_Sche)"],
            "yyyymm_bkg_sche": r["YYYYMM_BKG_Sche"],
            "lst_status": r["LST_Status"],
            "bl_no": r.get("BL_NO", ""),
            "related_booking_no": r.get("Related_BKG_NO", ""),
            "performance_metric_kind": r.get("Performance_Metric_Kind", ""),
            "source_salesman": r.get("Source_Salesman", ""),
            "customer_cohort": r.get("Customer_Cohort", "history_unconfirmed"),
            "target_eligible": bool(r.get("Target_Eligible", True)),
            "goal_exclusion_reason": clean_text(r.get("Goal_Exclusion_Reason", "")),
            "fst_teu": float(r["fst_teu_num"]),
            "lst_teu": float(r["lst_teu_num"]),
            "norm_lst_teu": float(r["norm_lst_teu_num"]),
            "wos_norm_lst_teu": float(r["wos_norm_lst_teu_num"]),
            "cm1": float(r["cm1_num"]),
            "norm_cm1": float(r["norm_cm1_num"]),
            "cm1_per_teu": float(r["cm1_per_teu_num"]),
            "grade": r["grade"],
            "route_profit": r["고/저"],
            "shipper_profit_tag": r["고수익태그"],
            "is_w3": bool(r["is_w3"]),
            "is_hi": bool(r["is_hi"]),
            "is_route_hi": bool(r["is_route_hi"]),
            "is_normal": bool(r["is_normal"]),
            "is_lifted": bool(r["is_lifted"]),
            "is_cancel": bool(r["is_cancel"]),
        })

    shipper_groups = block.groupby(["BKG_SHPR_CST_NO", "BKG_SHPR_CST_ENM"], dropna=False)
    shippers: list[dict[str, Any]] = []
    for (cst_no, cst_nm), g in shipper_groups:
        fst = float(g["fst_teu_num"].sum())
        lst = float(g["norm_lst_teu_num"].sum())
        cm1 = float(g["norm_cm1_num"].sum())
        w3_fst = float(g.loc[g["is_w3"], "fst_teu_num"].sum())
        w3_lst = float(g.loc[g["is_w3"], "wos_norm_lst_teu_num"].sum())
        hi_w3_fst = float(g.loc[g["is_w3"] & g["is_route_hi"], "fst_teu_num"].sum())
        grade_values = sorted(set(g["grade"].dropna().tolist()))
        grade_label = grade_values[0] if len(grade_values) == 1 else (", ".join(grade_values) if grade_values else "")
        shippers.append({
            "shipper_no": clean_text(cst_no),
            "shipper_name": clean_text(cst_nm),
            "target_eligible": bool(g.get("Target_Eligible", pd.Series(True, index=g.index)).all()),
            "grade": grade_label,
            "bkg_count": int(g.get("Performance_Metric_Kind", pd.Series("", index=g.index)).ne("bl").sum()),
            "bl_count": int(g.get("Performance_Metric_Kind", pd.Series("", index=g.index)).eq("bl").sum()),
            "fst_teu": fst,
            "lst_teu": lst,
            "cm1": cm1,
            "w3_fst": w3_fst,
            "goal_basis_w3_fst": float(g.loc[g["is_w3"] & g.get("Target_Eligible", pd.Series(True, index=g.index)), "fst_teu_num"].sum()),
            "w3_lst": w3_lst,
            "hi_w3_fst": hi_w3_fst,
            "cancel_teu": float(g.loc[g["is_cancel"], "fst_teu_num"].sum()),
            "lst_rate_w3": (w3_lst / w3_fst) if w3_fst else None,
            "hi_share_w3": (hi_w3_fst / w3_fst) if w3_fst else None,
        })
    shippers.sort(key=lambda x: x["fst_teu"], reverse=True)

    total_fst = float(block["fst_teu_num"].sum())
    total_lst = float(block["norm_lst_teu_num"].sum())
    total_cm1 = float(block["norm_cm1_num"].sum())
    w3_fst = float(block.loc[block["is_w3"], "fst_teu_num"].sum())
    w3_lst = float(block.loc[block["is_w3"], "wos_norm_lst_teu_num"].sum())
    w3_hi_fst = float(block.loc[block["is_w3"] & block["is_route_hi"], "fst_teu_num"].sum())
    goal_basis_w3_fst = float(block.loc[block["is_w3"] & block.get("Target_Eligible", pd.Series(True, index=block.index)), "fst_teu_num"].sum())
    allocated_bsa, bsa_rows = serialize_bsa_allocations(bsa_block)

    return {
        "origin": origin,
        "salesman": salesman,
        "yyyymm": yyyymm,
        "totals": {
            "bkg_count": int(block.get("Performance_Metric_Kind", pd.Series("", index=block.index)).ne("bl").sum()),
            "bl_count": int(block.get("Performance_Metric_Kind", pd.Series("", index=block.index)).eq("bl").sum()),
            "shipper_count": int(block["BKG_SHPR_CST_NO"].nunique()),
            "fst_teu": total_fst,
            "lst_teu": total_lst,
            "cm1": total_cm1,
            "w3_fst": w3_fst,
            "goal_basis_w3_fst": goal_basis_w3_fst,
            "w3_lst": w3_lst,
            "w3_hi_fst": w3_hi_fst,
            "allocated_bsa": allocated_bsa,
            "lst_rate_w3": (w3_lst / w3_fst) if w3_fst else None,
            "hi_share_w3": (w3_hi_fst / w3_fst) if w3_fst else None,
        },
        "shippers": shippers,
        "bsa_allocations": bsa_rows,
        "bookings": bookings,
    }



def fetch_verified_monthly_target_source(creds: Any, file_id: str) -> dict[str, Any]:
    """Read the reviewed Drive source entirely in memory; no local raw copies."""
    drive = build("drive", "v3", credentials=creds, cache_discovery=False)
    raw = drive.files().get_media(fileId=file_id).execute(num_retries=2)
    source = json.loads(raw.decode("utf-8-sig") if isinstance(raw, bytes) else raw)
    if source.get("format") != "sales-target-monthly-input-v1" or source.get("verification") != "confirmed":
        raise ValueError("Monthly target source is not confirmed")
    return source


def target_period_availability(rows: list[dict[str, Any]]) -> dict[str, bool]:
    return {quarter: any(row.get("kpi", {}).get(key, {}).get(quarter, {}).get("target") is not None
                         for row in rows for key in ("booking", "lifting", "high_profit"))
            for quarter in QUARTER_MONTHS}



def mark_confirmed_new_customers(df: pd.DataFrame, cache_2025: Path, source: dict[str, Any] | None = None) -> None:
    """Registration-backed new cohort has performance, but no target basis."""
    from sales_target_customer_cohorts import classify_customer, fetch_registration_evidence, evidence_day
    classes = (source or {}).get("customer_classes")
    if classes is None:
        baseline_frame = pd.read_parquet(cache_2025, columns=["BKG_SHPR_CST_NO"])
        baseline = set(baseline_frame["BKG_SHPR_CST_NO"].fillna("").astype(str).str.strip().str.upper())
        customers = set(df["BKG_SHPR_CST_NO"].str.upper())
        records, audit = fetch_registration_evidence(customers - baseline)
        date_lookup = {value: evidence_day(value) for value in df["Booking_date"].dropna().unique()}
        observed = df.assign(_trade_day=df["Booking_date"].map(date_lookup).fillna("99999999"))
        first_trade = observed.groupby("BKG_SHPR_CST_NO")["_trade_day"].min().to_dict()
        classes = {customer: classify_customer(customer, baseline, records.get(customer), first_trade.get(customer)) for customer in customers}
        print("      Customer registration audit: " + json.dumps(audit), flush=True)
    keys = df["BKG_SHPR_CST_NO"].str.upper()
    df["Customer_Cohort"] = keys.map(lambda customer: classes.get(customer, {}).get("class", "history_unconfirmed"))
    df["Target_Eligible"] = df["Customer_Cohort"].ne("confirmed_new")
    if (source or {}).get('target_basis_policy')=='company_2025_actuals_official_office_attribution':
        no_history=keys.isin(source.get('no_historical_goal_customers',[]))
        df.loc[no_history,'Target_Eligible']=False
        df['Goal_Exclusion_Reason']=''
        df.loc[no_history,'Goal_Exclusion_Reason']='no_historical_performance'



def ensure_performance_only_rows(rows: list[dict[str, Any]], df: pd.DataFrame) -> set[tuple[str, str]]:
    """Keep confirmed-new actuals even when the target roster has no goal row."""
    if "Target_Eligible" not in df:
        return set()
    actual = df.loc[sales_target_scope_mask(df) & ~df["Target_Eligible"]]
    pairs = {(clean_text(r.tab), clean_text(r.Salesman_POR)) for r in actual.itertuples(index=False)}
    existing = {(r["tab"], r["name"]) for r in rows if r.get("row_type") in {"SALES", "PERFORMANCE_ONLY"}}
    for origin, owner in sorted(pairs - existing):
        group=actual.loc[actual["tab"].eq(origin)&actual["Salesman_POR"].eq(owner)]
        unassigned=group.get("Goal_Exclusion_Reason",pd.Series("",index=group.index)).eq("unassigned_current_owner").any()
        rows.append({"tab": origin, "name": owner, "row_type": "PERFORMANCE_ONLY", "target_eligible": False,
                     "goal_exclusion_reason":"unassigned_current_owner" if unassigned else "no_historical_performance" if group.get("Goal_Exclusion_Reason",pd.Series("",index=group.index)).eq("no_historical_performance").any() else "confirmed_new",
                     "performance_owner_basis": "dated_snapshot_owner; effective-month history not asserted",
                     "accounts": {}, "kpi": _empty_kpi()})
    return pairs



def apply_monthly_owner_maps(df: pd.DataFrame, monthly_maps: dict[str, Any], monthly_target_source: dict[str, Any]) -> None:
    if "Goal_Exclusion_Reason" not in df:
        df["Goal_Exclusion_Reason"] = ""
    else:
        df["Goal_Exclusion_Reason"] = df["Goal_Exclusion_Reason"].fillna("").astype(str)
    company_basis=monthly_target_source.get('target_basis_policy')=='company_2025_actuals_official_office_attribution'
    if company_basis:
        df['Original_Target_Scope']=sales_target_scope_mask(df)
        df['physical_tab']=df['tab']
    customer_keys=df['BKG_SHPR_CST_NO'].str.upper()
    for month, mapping in monthly_maps.items():
        mask = df["YYYYMM"].eq(month)
        excluded_pairs={(origin,customer) for origin,customers in monthly_target_source.get("excluded_unassigned_by_month",{}).get(month,{}).items() for customer in customers}
        if excluded_pairs:
            excluded=mask & pd.MultiIndex.from_arrays([df['tab'],customer_keys]).isin(excluded_pairs)
            df.loc[excluded,'Target_Eligible']=False
            df.loc[excluded,'Goal_Exclusion_Reason']='unassigned_current_owner'
        scope = mask & sales_target_scope_mask(df) & df["Target_Eligible"]
        customers = set(customer_keys.loc[scope])
        if any(customer not in mapping or mapping[customer] == MISSING_SALES for customer in customers):
            raise ValueError("Monthly live customer coverage incomplete; unresolved treatment awaits a decision")
        goal_mask = mask & df["Target_Eligible"]
        df.loc[goal_mask, "Salesman_POR"] = (customer_keys.loc[goal_mask]
                                                .map(mapping).fillna(MISSING_SALES))
        if company_basis:
            office_map=monthly_target_source.get('owner_offices_by_month',{}).get(month,{})
            keys=customer_keys
            homes=keys.map({customer:offices[0] for customer,offices in office_map.items() if len(offices)==1})
            office_mask=mask & df['Original_Target_Scope'] & homes.notna()
            df.loc[office_mask,'tab']=homes.loc[office_mask]
            verified=office_mask & keys.isin(mapping)
            df.loc[verified,'Salesman_POR']=keys.loc[verified].map(mapping)
            multiple={customer:set(offices) for customer,offices in office_map.items() if len(offices)>1}
            multi_mask=mask & df['Original_Target_Scope'] & keys.isin(multiple)
            indices=[index for index in df.index[multi_mask] if df.at[index,'physical_tab'] in multiple[keys.at[index]]]
            df.loc[indices,'tab']=df.loc[indices,'physical_tab']
            df.loc[indices,'Salesman_POR']=keys.loc[indices].map(mapping).fillna(df.loc[indices,'Salesman_POR'])
        for origin, customers in monthly_target_source.get("performance_only_by_month", {}).get(month, {}).items():
            performance_mask = mask & ~df["Target_Eligible"] & df["tab"].eq(origin)
            original = df.loc[performance_mask, "Salesman_POR"]
            df.loc[performance_mask, "Salesman_POR"] = (customer_keys.loc[performance_mask]
                .map(customers).fillna(original))



def monthly_source_bsa_allocations(source: dict[str, Any]) -> pd.DataFrame:
    """Use conserved source route shares; never reweight excluded customers."""
    rows=[]
    for month,ledger in source.get("bsa_route_goal_ledger",{}).items():
        if ledger.get("pending"):
            raise ValueError("Unresolved route ownership still blocks publication")
        assigned=ledger.get("assigned",[]);reserved=ledger.get("reserved",[])
        totals=ledger.get("totals",{})
        raw=float(totals.get("raw",0))
        if abs(raw-sum(float(row["allocated_bsa"]) for row in assigned+reserved))>1e-6:
            raise ValueError("Monthly route ledger cannot lose or reassign capacity")
        rows.extend(dict(row) for row in assigned)
        # This existing technical chunk label holds capacity only. It never
        # creates a salesperson/index goal row or customer target.
        rows.extend({**row,"Salesman_POR":NO_BASIS_SALES} for row in reserved)
    columns=["tab","Salesman_POR","YYYYMM","dest","dst_port"]
    if source.get('target_basis_policy')=='company_2025_actuals_official_office_attribution':columns+=['physical_tab','shipper_no']
    columns+=['allocated_bsa']
    if not rows:return pd.DataFrame(columns=columns)
    return pd.DataFrame(rows).groupby(columns[:-1],dropna=False).allocated_bsa.sum().reset_index()


def main() -> int:
    if os.environ.get("SKIP_SALES_TARGET_BUILD", "").strip() == "1":
        print("Sales Target skipped by SKIP_SALES_TARGET_BUILD=1 before any reads or writes", flush=True)
        return 0
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workbook", default=DEFAULT_WORKBOOK)
    parser.add_argument("--snapshot", default=None, help="Path to booking_snapshot_result_YYYYMMDD.csv (default: latest)")
    parser.add_argument("--salesman-csv", default=None, help="Path to salesman.csv (default: project root)")
    parser.add_argument("--as-of", default=None, help="YYYYMMDD for active-row filter (default: today)")
    parser.add_argument("--no-remap", action="store_true", help="Skip salesman.csv override (use raw Salesman_POR)")
    parser.add_argument("--monthly-target-from-existing-sources", action="store_true",
                        help="Build monthly input in memory from existing Drive/workbook/cache sources")
    parser.add_argument("--monthly-target-verify-only", action="store_true",
                        help="Read and quantify existing monthly sources without generating output files")
    parser.add_argument('--monthly-target-source-local-url',default=None,help='Read verified goal JSON from an existing loopback memory diagnostic; no raw copies')
    parser.add_argument("--monthly-target-source-drive-id", default=None,
                        help="Reviewed monthly owner intervals, roster and target measures on Drive; unresolved inputs are rejected")
    parser.add_argument('--official-roster-stdin',action='store_true',help='Connected official role projection/provenance JSON on stdin; no raw local copy')
    parser.add_argument("--out", default=str(RUNTIME_ROOT / "dist" / "sales-target"))
    args = parser.parse_args()
    official_input=json.load(sys.stdin) if args.official_roster_stdin else None

    if args.monthly_target_verify_only:
        if not args.monthly_target_from_existing_sources:
            raise ValueError("Verification requires --monthly-target-from-existing-sources")
        from build_sales_target_monthly_input import read_existing_evidence, produce_monthly_input
        source, audit = produce_monthly_input(sys.modules[__name__], read_existing_evidence(
            sys.modules[__name__], args.as_of or datetime.now().strftime("%Y%m%d"),
            official_projection=official_input['records'] if official_input else None,
            official_meta=official_input['meta'] if official_input else None))
        print(json.dumps({"verification": source["verification"], "months": audit,
                          "source_references": source["source_references"],
                          "basis_reconciliation_required": source["basis_reconciliation_required"]}, ensure_ascii=False))
        return 0
    if not (args.monthly_target_from_existing_sources or args.monthly_target_source_drive_id or args.monthly_target_source_local_url):
        raise ValueError("Sales Target generation blocked: verified monthly target source is required; no output written")
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    started = time.time()
    print(f"[1/3] Loading target workbook Summary_All ({args.workbook}) ...", flush=True)
    from google_verified_transport import GoogleSession
    from google.auth.transport.requests import Request
    creds = get_creds()
    session=GoogleSession();session.prefer_verified_stdlib=True
    try:
        creds.refresh(Request(session=session));session.headers['Authorization']='Bearer '+creds.token
        response=session.get('https://sheets.googleapis.com/v4/spreadsheets/'+args.workbook+'/values/Summary_All!A1:AC400',
            params={'valueRenderOption':'UNFORMATTED_VALUE'},timeout=60)
        response.raise_for_status();summary_rows=response.json().get('values',[])
    finally:session.close()
    parsed_summary = parse_summary(summary_rows)
    monthly_target_source = (fetch_verified_monthly_target_source(creds, args.monthly_target_source_drive_id)
                             if args.monthly_target_source_drive_id else None)
    if args.monthly_target_source_local_url:
        from urllib.parse import urlparse
        from urllib.request import urlopen
        parsed=urlparse(args.monthly_target_source_local_url)
        if parsed.scheme!='http' or parsed.hostname!='127.0.0.1' or parsed.path!='/monthly-source.json':
            raise ValueError('Memory input is restricted to the read-only local diagnostic endpoint')
        if args.monthly_target_source_drive_id or args.monthly_target_from_existing_sources:
            raise ValueError('Choose exactly one reviewed monthly source')
        with urlopen(args.monthly_target_source_local_url,timeout=30) as response:
            monthly_target_source=json.load(response)
        if monthly_target_source.get('verification')!='confirmed':raise ValueError('Memory source is not confirmed')
    if args.monthly_target_from_existing_sources:
        if args.monthly_target_source_drive_id:
            raise ValueError("Choose one monthly source path")
        from build_sales_target_monthly_input import read_existing_evidence, produce_monthly_input
        monthly_target_source, audit = produce_monthly_input(sys.modules[__name__], read_existing_evidence(
            sys.modules[__name__], args.as_of or datetime.now().strftime("%Y%m%d"),
            official_projection=official_input['records'] if official_input else None,
            official_meta=official_input['meta'] if official_input else None))
        if monthly_target_source["verification"] != "confirmed":
            raise ValueError("Existing monthly sources incomplete; run --monthly-target-verify-only. No fallback selected.")
    if (monthly_target_source or {}).get('target_basis_policy')!='company_2025_actuals_official_office_attribution':
        raise ValueError('Production target generation requires verified company-owned 2025 basis and latest official offices; no legacy fallback')
    company_basis=True
    print(f"      Parsed {len(parsed_summary)} target rows.", flush=True)

    # Existing immutable cache only; never start an extraction here.
    cache_2025=RUNTIME_ROOT/'output'/'_cache_2025.parquet'
    if not cache_2025.exists():cache_2025=Path(__file__).resolve().parents[1]/'output'/'_cache_2025.parquet'
    if not cache_2025.exists():raise FileNotFoundError('Existing immutable 2025 cache is required')
    pre_salesman_map: dict[str, str] = {}
    if not args.no_remap:
        sm_csv = Path(args.salesman_csv) if args.salesman_csv else find_salesman_csv()
        if sm_csv and sm_csv.exists():
            pre_salesman_map = load_active_salesman_mapping(sm_csv, args.as_of)
    reference_month=(monthly_target_source['dataset_id'])[:6]
    current_goal_owner_map=monthly_target_source['owner_maps_by_month'][reference_month]
    current_goal_offices=monthly_target_source['owner_offices_by_month'][reference_month]
    pre_salesman_map=current_goal_owner_map
    w3_2025_by_pair, w3_2025_by_tab = load_w3_2025_teu(cache_2025, pre_salesman_map,current_goal_offices)
    if w3_2025_by_pair:
        for row in parsed_summary:
            if row["row_type"] == "TOTAL":
                row["w3_2025_teu"] = float(w3_2025_by_tab.get(row["tab"], 0.0))
            else:
                row["w3_2025_teu"] = float(w3_2025_by_pair.get((row["tab"], row["name"]), 0.0))
        print(f"      2025 W3 TEU: {len(w3_2025_by_pair)} (tab, salesperson) pairs; {len(w3_2025_by_tab)} tab totals.", flush=True)
    else:
        print("      WARN: _cache_2025.parquet missing - '25 W3 BKG' column will be empty.", flush=True)
        for row in parsed_summary:
            row["w3_2025_teu"] = None

    salesman_map: dict[str, str] | None = None
    salesman_csv_path: Path | None = None
    if not args.no_remap:
        salesman_csv_path = Path(args.salesman_csv) if args.salesman_csv else find_salesman_csv()
        if salesman_csv_path and salesman_csv_path.exists():
            salesman_map = load_active_salesman_mapping(salesman_csv_path, args.as_of)
            print(f"      Salesman map loaded from {salesman_csv_path.name}: {len(salesman_map):,} active CUSTOMER_NO entries.", flush=True)
        else:
            print("      WARN: salesman.csv not found; Salesman_POR will use the raw snapshot values.", flush=True)

    snapshot_path = Path(args.snapshot) if args.snapshot else latest_snapshot()
    data_date_match = re.search(r"booking_snapshot_result_(\d{8})", snapshot_path.stem)
    data_date = data_date_match.group(1) if data_date_match else datetime.now().strftime("%Y%m%d")
    print(f"[2/3] Reading snapshot {snapshot_path.name} ...", flush=True)
    df = load_snapshot(snapshot_path, salesman_map=salesman_map)
    if salesman_map:
        covered = int((df["Salesman_POR"] != MISSING_SALES).sum())
        missing = len(df) - covered
        print(
            f"      Salesman coverage after remap/raw fallback: "
            f"{covered:,} / {len(df):,} rows covered ({missing:,} -> {MISSING_SALES}).",
            flush=True,
        )
    print(f"      Loaded {len(df):,} booking rows.", flush=True)

    if monthly_target_source.get('dataset_id')!=data_date:
        raise ValueError('Reviewed goal input must match the dated actual snapshot')
    # Current Q3/Q4 offices come from the separately verified latest directory.
    # Native Q1/Q2 rows remain frozen, with explicit quarter roster evidence.
    meta=monthly_target_source.get('official_roster_meta',{})
    if meta.get('verification')!='confirmed' or not meta.get('source_id') or not meta.get('modified_time'):
        raise ValueError('Verified latest official office provenance is required')
    native_pairs={(clean_text(r.get('tab')),clean_text(r.get('name'))) for r in parsed_summary if r.get('row_type')=='SALES'}
    current_pairs={tuple(pair) for pair in monthly_target_source.get('approved_goal_roster',[])}
    if not current_pairs:raise ValueError('Verified current goal roster is empty')
    affiliation={'policy':'sales-target-roster-v1','dataset_id':data_date,'validation':'passed','dry_run':False,
                 'approved_pairs':[list(pair) for pair in sorted(current_pairs)],
                 'source':'verified_company_2025_official_offices','official_roster_meta':meta,
                 'native_quarter_roster_evidence':{'q1':[list(pair) for pair in sorted(native_pairs)],'q2':[list(pair) for pair in sorted(native_pairs)]}}
    approved=current_pairs|native_pairs
    print(f"      Official company-2025 office gate passed: {len(current_pairs)} current owners; native Q1/Q2 preserved.",flush=True)
    (out_dir/'office_affiliation.json').write_text(json.dumps(affiliation,ensure_ascii=False,separators=(',',':')),encoding='utf-8')

    provisional_customer_owners = {} if company_basis else provisional_customer_owner_mapping(
        df,
        salesman_map,
        args.as_of or data_date,
    )
    if provisional_customer_owners and not company_basis:
        salesman_map = dict(salesman_map or {})
        salesman_map.update(provisional_customer_owners)
        df = load_snapshot(snapshot_path, salesman_map=salesman_map)
        print(
            "      Current-customer history transferred to provisional salespeople: "
            f"{len(provisional_customer_owners):,} customers.",
            flush=True,
        )
        w3_2025_by_pair, w3_2025_by_tab = load_w3_2025_teu(cache_2025, salesman_map)
        for row in parsed_summary:
            if row["row_type"] == "TOTAL":
                row["w3_2025_teu"] = float(w3_2025_by_tab.get(row["tab"], 0.0))
            else:
                row["w3_2025_teu"] = float(
                    w3_2025_by_pair.get((row["tab"], row["name"]), 0.0)
                )

    mark_confirmed_new_customers(df, cache_2025, monthly_target_source)
    monthly_maps = {}
    monthly_rosters = {}
    if monthly_target_source is not None:
        from sales_target_monthly import attach_verified_target_source
        monthly_maps, monthly_rosters = attach_verified_target_source(parsed_summary, monthly_target_source)
        apply_monthly_owner_maps(df, monthly_maps, monthly_target_source)
        for roster in monthly_rosters.values():
            approved.update((origin, owner) for origin, owners in roster.items() for owner in owners)

    performance_pairs = ensure_performance_only_rows(parsed_summary, df)
    approved.update(performance_pairs)

    print("[3/3] Writing chunk JSONs ...", flush=True)
    alloc_months = set(df.loc[sales_target_scope_mask(df), "YYYYMM"].dropna().astype(str))
    allowed_sales_by_origin: dict[str, set[str]] = {}
    for row in parsed_summary:
        if row.get("row_type") in {"SALES", "PERFORMANCE_ONLY"}:
            allowed_sales_by_origin.setdefault(clean_text(row.get("tab")), set()).add(clean_text(row.get("name")))
    if monthly_target_source and monthly_target_source.get('goal_exclusion_policy') == 'exclude_unassigned_without_redistribution':
        if not monthly_target_source.get('bsa_route_goal_ledger'):
            raise ValueError('Exclusion policy requires a conserved route ledger; no fallback reallocation')
        bsa_allocations = monthly_source_bsa_allocations(monthly_target_source)
        legacy_months = alloc_months-set(monthly_target_source['bsa_route_goal_ledger'])
        if legacy_months:
            legacy_bsa = build_allocated_bsa(legacy_months,salesman_map,args.as_of or data_date,allowed_sales_by_origin,activity_basis_from_snapshot(df.loc[df['Target_Eligible']],legacy_months))
            bsa_allocations = pd.concat([legacy_bsa,bsa_allocations],ignore_index=True)
    elif monthly_maps:
        pieces = [build_allocated_bsa(
            {month}, monthly_maps.get(month, salesman_map), args.as_of or data_date,
            {origin: set(inputs["approved_salespeople"]) for origin, inputs in monthly_target_source["months"].get(month, {}).items()}
                if month in monthly_target_source["months"] else allowed_sales_by_origin,
            activity_basis_from_snapshot(df.loc[df["Target_Eligible"]], {month}),
        ) for month in sorted(alloc_months)]
        bsa_allocations = pd.concat(pieces, ignore_index=True) if pieces else pd.DataFrame()
    else:
        bsa_allocations = build_allocated_bsa(
            alloc_months, salesman_map, args.as_of or data_date, allowed_sales_by_origin,
            activity_basis_from_snapshot(df.loc[df["Target_Eligible"]], alloc_months),
        )
    manifest, month_metrics = aggregate_chunks(df, out_dir, bsa_allocations, allowed_sales_by_origin)
    if monthly_target_source and monthly_target_source.get('goal_exclusion_policy') == 'exclude_unassigned_without_redistribution':
        manifest['capacity_reserve_chunks']=[cell for cell in manifest['chunks'] if cell['salesman']==NO_BASIS_SALES]
        manifest['chunks']=[cell for cell in manifest['chunks'] if cell['salesman']!=NO_BASIS_SALES]
        manifest['chunk_count']=len(manifest['chunks'])
        manifest['bsa_capacity_reconciliation']={month:ledger['totals'] for month,ledger in monthly_target_source['bsa_route_goal_ledger'].items()}
    # The personnel filter includes verified office owners with zero volume.
    manifest["salespeople_by_origin"] = {origin: sorted(names) for origin, names in allowed_sales_by_origin.items()}
    manifest["origins"] = sorted(allowed_sales_by_origin)
    if any((c["origin"], c["salesman"]) not in approved for c in manifest["chunks"]):
        raise ValueError("Cross-office chunk detected before publication")
    # Live month-level Progress for the main dashboard's target overlay (selected-month
    # values that match the Sales Target screen, not the workbook's static quarter snapshot).
    attach_month_progress(parsed_summary, month_metrics)
    attach_missing_quarter_progress(parsed_summary, month_metrics)
    attach_quarter_metrics(parsed_summary, month_metrics)
    for chunk in manifest["chunks"]:
        roster = monthly_rosters.get(chunk["yyyymm"])
        if roster is not None and chunk["salesman"] not in roster.get(chunk["origin"], set()) and (chunk["origin"],chunk["salesman"]) not in performance_pairs:
            raise ValueError("Chunk differs from verified monthly office roster")
    period_support = target_period_availability(parsed_summary)
    current_goal_pairs = {tuple(pair) for pair in affiliation["approved_pairs"]}
    historical_goal_pairs = {(row["tab"], row["name"]) for row in parsed_summary if row.get("row_type") == "SALES"} - current_goal_pairs
    performance_only_pairs = {(row["tab"], row["name"]) for row in parsed_summary if row.get("row_type") == "PERFORMANCE_ONLY"}
    roster_metadata = {
        "current_target_roster_pairs": [list(pair) for pair in sorted(current_goal_pairs)],
        "historical_target_roster_pairs": [list(pair) for pair in sorted(historical_goal_pairs)],
        "performance_only_roster_pairs": [list(pair) for pair in sorted(performance_only_pairs)],
        "monthly_target_rosters": {month: {origin: sorted(inputs["approved_salespeople"]) for origin, inputs in origins.items()}
                                   for month, origins in (monthly_target_source or {}).get("months", {}).items()},
        "monthly_target_source_verification": (monthly_target_source or {}).get("verification"),
        "target_basis_policy": monthly_target_source['target_basis_policy'],
        "official_roster_meta": monthly_target_source['official_roster_meta'],
        "historical_roster_by_quarter": affiliation.get('native_quarter_roster_evidence',{}),
        "historical_roster_source": 'preserved_native_Summary_All_Q1_Q2',

    }

    index_payload = {
        "_format": "sales-target-index-v1",
        **roster_metadata,
        "office_policy": affiliation["policy"],
        "target_period_support": period_support,
        "target_period_basis": ("verified monthly owner intervals and ratio-of-sums targets"
                                if monthly_target_source is not None else "Summary_All supplied targets; monthly history not connected"),
        "monthly_target_source_drive_id": args.monthly_target_source_drive_id,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "data_date": data_date,
        "workbook_id": args.workbook,
        "workbook_url": f"https://docs.google.com/spreadsheets/d/{args.workbook}/edit",
        "rows": parsed_summary,
        "origins": manifest["origins"],
        "months": manifest["months"],
    }
    (out_dir / "index.json").write_text(
        json.dumps(index_payload, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )

    manifest_payload = {
        "_format": "sales-target-manifest-v1",
        **roster_metadata,
        "office_policy": affiliation["policy"],
        "target_period_support": period_support,
        "target_period_basis": ("verified monthly owner intervals and ratio-of-sums targets"
                                if monthly_target_source is not None else "Summary_All supplied targets; monthly history not connected"),
        "monthly_target_source_drive_id": args.monthly_target_source_drive_id,
        "generated_at": index_payload["generated_at"],
        "data_date": data_date,
        "chunk_count": manifest["chunk_count"],
        "bkg_rows": manifest["bkg_rows"],
        "origins": manifest["origins"],
        "months": manifest["months"],
        "salespeople_by_origin": manifest["salespeople_by_origin"],
        "dest_countries": manifest["dest_countries"],
        "dest_ports_by_country": manifest["dest_ports_by_country"],
        "chunks": manifest["chunks"],
        "capacity_reserve_chunks": manifest.get("capacity_reserve_chunks",[]),
        "bsa_capacity_reconciliation": manifest.get("bsa_capacity_reconciliation",{}),
        "target_basis_policy": (monthly_target_source or {}).get("target_basis_policy"),
        "official_roster_meta": (monthly_target_source or {}).get("official_roster_meta"),
    }
    (out_dir / "manifest.json").write_text(
        json.dumps(manifest_payload, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )

    print("      Building 2025 base cells for filter-linked '25/Target columns ...", flush=True)
    base_num_cells = load_2025_base_cells(cache_2025, current_goal_owner_map,current_goal_offices)
    base_shipper_cells, base_shipper_meta = load_2025_shipper_cells(cache_2025,current_goal_owner_map,current_goal_offices)
    bsa_2025 = pd.DataFrame(monthly_target_source['annual_bsa_goal_allocations'],columns=['tab','Salesman_POR','dest','dst_port','allocated_bsa'])
    base2025_payload = build_base2025_payload(base_num_cells, bsa_2025, data_date, index_payload["generated_at"], allowed_sales_by_origin, base_shipper_cells, base_shipper_meta)
    (out_dir / "base2025.json").write_text(
        json.dumps(base2025_payload, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    base2025_bytes = (out_dir / "base2025.json").stat().st_size
    print(
        f"      base2025.json: {len(base_num_cells):,} (tab, salesperson) numerator groups, "
        f"{base2025_bytes / 1024:.0f} KB.",
        flush=True,
    )

    from sales_target_release import prepare_release
    release = prepare_release(out_dir)
    print(f"      Immutable Sales Target release: {release['release_id']}", flush=True)

    duration = time.time() - started
    print(
        f"Done. {manifest['chunk_count']} chunks ({manifest['bkg_rows']:,} BKGs) in {duration:.1f}s. "
        f"Output: {out_dir}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
