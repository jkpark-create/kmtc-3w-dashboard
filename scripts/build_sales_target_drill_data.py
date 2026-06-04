"""Build JSON payloads consumed by the Sales Target & Progress drill-down screen.

Two outputs:

  dist/sales-target/index.json
      Small (a few hundred KB) summary used by the drill-down landing view. Carries
      per-(origin, salesman) Target / Performance / GAP rows for 1Q 2026 and 2Q 2026
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
DEFAULT_WORKBOOK = "1YxZkwvoMaQXIEw07qUDZtCPDFZBf8GOZyr5knkxnLxo"
SUMMARY_SHEET = "Summary_All"
MISSING_SALES = "(미지정)"
SALESMAN_CSV_CANDIDATES = ("salesman.csv", "saleman.csv")
TEAM_FILTER = "OBT"
BSA_ROUTE_KEYS = ["team", "tab", "dest", "dst_port"]
NO_BASIS_SALES = "(no 2025 basis)"
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


def load_w3_2025_teu(cache_path: Path, salesman_map: dict[str, str] | None) -> tuple[dict[tuple[str, str], float], dict[str, float]]:
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
    if salesman_map:
        keys = df["BKG_SHPR_CST_NO"].str.upper()
        df["Salesman_POR"] = keys.map(salesman_map).fillna("").astype(str).str.strip()
    df["Salesman_POR"] = df["Salesman_POR"].replace("", MISSING_SALES)
    df["tab"] = [tab_key(o, p) for o, p in zip(df["POR_CTR_CD"], df["POR_PLC_CD"])]
    if "team" not in df.columns:
        df["team"] = [classify_team(o, d) for o, d in zip(df["POR_CTR_CD"], df["DLY_CTR_CD"])]
    else:
        df["team"] = df["team"].fillna("").astype(str).str.strip()
    df["fst_num"] = pd.to_numeric(df["FST_TEU"].astype(str).str.replace(",", "", regex=False), errors="coerce").fillna(0.0)
    scoped = df.loc[
        df["team"].astype(str).eq("OBT")
        & df["Lead_time (BKG_Sche)"].astype(str).eq("WOS-3")
        & df["fst_num"].gt(0)
        & df["tab"].ne("UNKNOWN")
    ]
    by_pair = scoped.groupby(["tab", "Salesman_POR"], dropna=False)["fst_num"].sum().to_dict()
    by_tab = scoped.groupby("tab", dropna=False)["fst_num"].sum().to_dict()
    return by_pair, by_tab


def load_module(module_name: str, path: Path) -> Any:
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load {module_name} from {path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def ensure_2025_cache() -> Path:
    """Create output/_cache_2025.parquet on demand for the BSA allocation basis."""
    out = ROOT / "output" / "_cache_2025.parquet"
    if out.exists():
        return out
    mod = load_module("build_2025_bsa_shipper_sheet", ROOT / "scripts" / "build_2025_bsa_shipper_sheet.py")
    df = mod.add_dashboard_fields(mod.build_booking_frame())
    if "Lead_time (BKG_Sche)" not in df.columns and "Lead_time_BKG_Sche" in df.columns:
        df["Lead_time (BKG_Sche)"] = df["Lead_time_BKG_Sche"]
    df.to_parquet(out, index=False)
    return out


def find_bsa_csv(as_of: str | None) -> Path | None:
    out_dir = ROOT / "output"
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
    if salesman_map:
        keys = df["BKG_SHPR_CST_NO"].str.upper()
        df["Salesman_POR"] = keys.map(salesman_map).fillna("").astype(str).str.strip()
    df["Salesman_POR"] = df["Salesman_POR"].replace("", MISSING_SALES)
    df["tab"] = [tab_key(o, p) for o, p in zip(df["POR_CTR_CD"], df["POR_PLC_CD"])]
    if "team" not in df.columns:
        df["team"] = [classify_team(o, d) for o, d in zip(df["POR_CTR_CD"], df["DLY_CTR_CD"])]
    else:
        df["team"] = df["team"].fillna("").astype(str).str.strip()
    df["lst_num"] = pd.to_numeric(df["LST_TEU"].astype(str).str.replace(",", "", regex=False), errors="coerce").fillna(0.0)
    basis = df.loc[
        df["team"].eq(TEAM_FILTER)
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
        bsa["team"].eq(TEAM_FILTER)
        & bsa["YYYYMM"].isin(months)
        & bsa["route_bsa"].gt(0)
        & bsa["tab"].ne("UNKNOWN")
    ].copy()
    if bsa.empty:
        return pd.DataFrame(columns=["YYYYMM"] + BSA_ROUTE_KEYS + ["route_bsa"])
    return bsa.groupby(["YYYYMM"] + BSA_ROUTE_KEYS, dropna=False)["route_bsa"].sum().reset_index()


def build_allocated_bsa(
    months: set[str],
    salesman_map: dict[str, str] | None,
    as_of: str | None,
    allowed_sales_by_origin: dict[str, set[str]] | None = None,
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
    if not allowed_sales_by_origin and not no_basis.empty:
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
    for path in (ROOT / "output").glob("booking_snapshot_result_*.csv"):
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
            range=f"{SUMMARY_SHEET}!A1:Z",
            valueRenderOption="UNFORMATTED_VALUE",
        )
        .execute()
    )
    return resp.get("values", [])


def parse_summary(rows: list[list[Any]]) -> list[dict[str, Any]]:
    """Sheet header rows live at A2:Z4; data starts at row 5 (1-based). Columns:
        A Tab, B Name, C 2025_share, D booking_2025_base,
        E booking_q1_target, F booking_q1_perform, G booking_q1_gap,
        H booking_q2_target, I booking_q2_progress, J booking_q2_gap,
        K lifting_q1_target, L lifting_q1_perform, M lifting_q1_gap,
        N lifting_q2_target, O lifting_q2_progress, P lifting_q2_gap,
        Q hp_q1_target, R hp_q1_perform, S hp_q1_gap,
        T hp_q2_target, U hp_q2_progress, V hp_q2_gap,
        W ac_total, X ac_w3, Y ac_pct, Z ac_sort
    """
    parsed: list[dict[str, Any]] = []
    for raw in rows[4:]:  # data starts at row 5 (index 4)
        if not raw:
            continue
        tab = clean_text(raw[0] if len(raw) > 0 else "")
        name = clean_text(raw[1] if len(raw) > 1 else "")
        if not tab or not name:
            continue
        row_type = "TOTAL" if name.lower() in {"team total", "total"} else "SALES"
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


def load_snapshot(path: Path, salesman_map: dict[str, str] | None = None) -> pd.DataFrame:
    df = pd.read_csv(
        path,
        usecols=SNAPSHOT_COLUMNS,
        dtype=str,
        encoding="utf-8-sig",
        low_memory=False,
    )
    for col in SNAPSHOT_COLUMNS:
        df[col] = df[col].fillna("").astype(str).str.strip()

    # Override Salesman_POR using salesman.csv (current customer-owner mapping) when provided.
    if salesman_map:
        keys = df["BKG_SHPR_CST_NO"].str.upper()
        df["Salesman_POR"] = keys.map(salesman_map).fillna("").astype(str).str.strip()
    df["Salesman_POR"] = df["Salesman_POR"].replace("", MISSING_SALES)
    df["team"] = [classify_team(o, d) for o, d in zip(df["POR_CTR_CD"], df["DLY_CTR_CD"])]
    df["tab"] = [tab_key(o, p) for o, p in zip(df["POR_CTR_CD"], df["POR_PLC_CD"])]
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
    df["is_lifted"] = df["is_normal"]
    df["is_cancel"] = status.str.contains("캔슬|cancel", case=False, na=False) | df["Cancel_date"].ne("")
    return df


def sales_target_scope_mask(df: pd.DataFrame) -> pd.Series:
    """Rows that contribute to Sales Target metrics.

    The main dashboard includes zero-FST rows in the actual lifting numerator
    when they have Normal LST_TEU, so the Sales Target chunks must keep them too.
    """
    return (
        df["team"].eq(TEAM_FILTER)
        & (df["fst_teu_num"].gt(0) | df["norm_lst_teu_num"].gt(0))
        & df["YYYYMM"].ne("")
        & df["tab"].ne("UNKNOWN")
    )


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
    return manifest


def serialize_bsa_allocations(bsa_block: pd.DataFrame | None) -> tuple[float, list[dict[str, Any]]]:
    if bsa_block is None or bsa_block.empty:
        return 0.0, []
    grouped = (
        bsa_block.groupby(["dest", "dst_port"], dropna=False)["allocated_bsa"]
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
        })
    return total, rows


def build_empty_bsa_chunk(origin: str, salesman: str, yyyymm: str, bsa_block: pd.DataFrame) -> dict[str, Any]:
    allocated_bsa, bsa_rows = serialize_bsa_allocations(bsa_block)
    return {
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
            "fst_teu": float(r["fst_teu_num"]),
            "lst_teu": float(r["lst_teu_num"]),
            "norm_lst_teu": float(r["norm_lst_teu_num"]),
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
        w3_lst = float(g.loc[g["is_w3"], "norm_lst_teu_num"].sum())
        hi_w3_fst = float(g.loc[g["is_w3"] & g["is_route_hi"], "fst_teu_num"].sum())
        grade_values = sorted(set(g["grade"].dropna().tolist()))
        grade_label = grade_values[0] if len(grade_values) == 1 else (", ".join(grade_values) if grade_values else "")
        shippers.append({
            "shipper_no": clean_text(cst_no),
            "shipper_name": clean_text(cst_nm),
            "grade": grade_label,
            "bkg_count": int(len(g)),
            "fst_teu": fst,
            "lst_teu": lst,
            "cm1": cm1,
            "w3_fst": w3_fst,
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
    w3_lst = float(block.loc[block["is_w3"], "norm_lst_teu_num"].sum())
    w3_hi_fst = float(block.loc[block["is_w3"] & block["is_route_hi"], "fst_teu_num"].sum())
    allocated_bsa, bsa_rows = serialize_bsa_allocations(bsa_block)

    return {
        "origin": origin,
        "salesman": salesman,
        "yyyymm": yyyymm,
        "totals": {
            "bkg_count": int(len(block)),
            "shipper_count": int(block["BKG_SHPR_CST_NO"].nunique()),
            "fst_teu": total_fst,
            "lst_teu": total_lst,
            "cm1": total_cm1,
            "w3_fst": w3_fst,
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


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workbook", default=DEFAULT_WORKBOOK)
    parser.add_argument("--snapshot", default=None, help="Path to booking_snapshot_result_YYYYMMDD.csv (default: latest)")
    parser.add_argument("--salesman-csv", default=None, help="Path to salesman.csv (default: project root)")
    parser.add_argument("--as-of", default=None, help="YYYYMMDD for active-row filter (default: today)")
    parser.add_argument("--no-remap", action="store_true", help="Skip salesman.csv override (use raw Salesman_POR)")
    parser.add_argument("--out", default=str(ROOT / "dist" / "sales-target"))
    args = parser.parse_args()

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    started = time.time()
    print(f"[1/3] Loading target workbook Summary_All ({args.workbook}) ...", flush=True)
    creds = get_creds()
    service = build("sheets", "v4", credentials=creds, cache_discovery=False)
    summary_rows = fetch_summary_rows(service, args.workbook)
    parsed_summary = parse_summary(summary_rows)
    print(f"      Parsed {len(parsed_summary)} target rows.", flush=True)

    # Build the 2025 WOS-3 BKG TEU map up front so we can splice it into the rows.
    cache_2025 = ROOT / "output" / "_cache_2025.parquet"
    pre_salesman_map: dict[str, str] = {}
    if not args.no_remap:
        sm_csv = Path(args.salesman_csv) if args.salesman_csv else find_salesman_csv()
        if sm_csv and sm_csv.exists():
            pre_salesman_map = load_active_salesman_mapping(sm_csv, args.as_of)
    w3_2025_by_pair, w3_2025_by_tab = load_w3_2025_teu(cache_2025, pre_salesman_map)
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
        matched = int((df["Salesman_POR"] != MISSING_SALES).sum())
        print(f"      Remap coverage: {matched:,} / {len(df):,} rows matched (others -> {MISSING_SALES}).", flush=True)
    print(f"      Loaded {len(df):,} booking rows.", flush=True)

    print("[3/3] Writing chunk JSONs ...", flush=True)
    alloc_months = set(df.loc[sales_target_scope_mask(df), "YYYYMM"].dropna().astype(str))
    allowed_sales_by_origin: dict[str, set[str]] = {}
    for row in parsed_summary:
        if row.get("row_type") == "SALES":
            allowed_sales_by_origin.setdefault(clean_text(row.get("tab")), set()).add(clean_text(row.get("name")))
    bsa_allocations = build_allocated_bsa(
        alloc_months,
        salesman_map,
        args.as_of or data_date,
        allowed_sales_by_origin,
    )
    manifest = aggregate_chunks(df, out_dir, bsa_allocations, allowed_sales_by_origin)

    index_payload = {
        "_format": "sales-target-index-v1",
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
    }
    (out_dir / "manifest.json").write_text(
        json.dumps(manifest_payload, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )

    duration = time.time() - started
    print(
        f"Done. {manifest['chunk_count']} chunks ({manifest['bkg_rows']:,} BKGs) in {duration:.1f}s. "
        f"Output: {out_dir}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
