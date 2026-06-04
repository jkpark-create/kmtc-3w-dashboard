"""Create a separate Google Spreadsheet that splits the 2026 sales target view by 선적지.

Structure:
  - README              : overview of the file
  - Target_Input        : single source of truth for 증대율 input
  - Summary_All         : combined view (same as legacy Sales_Target_All)
  - <each origin tab>   : per-선적지 Team Total + salesperson rows

The 증대율 (3W booking / actual lifting / high-profit) lives only on
Target_Input. Every per-origin tab pulls its Target columns via VLOOKUP so
that adjusting one row instantly updates that origin's targets everywhere.
"""

from __future__ import annotations

import json
import math
import re
from pathlib import Path
from typing import Any

import pandas as pd
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build


ROOT = Path(__file__).resolve().parents[1]
SOURCE_SPREADSHEET_ID = "19d_lnB7Qt6H-UJE7ECyDo-i385tjMjAX8aATk0R0N54"
OUTPUT_TITLE = "2026 OBT Sales Target by 선적지"
STATE_PATH = ROOT / "output" / "per_origin_target_workbook.json"

INPUT_SHEET = "Target_Input"
SUMMARY_SHEET = "Summary_All"
README_SHEET = "README"
OWNER_INPUT_SHEET = "Sales_Owner_Input"

NO_BASIS_LABEL = "(no 2025 basis)"
MISSING_SALES = "(미지정)"
Q1_2026 = {"202601", "202602", "202603"}
Q2_2026 = {"202604", "202605", "202606"}
CN_NKG_PORTS = frozenset({
    "AIA", "AQG", "CGD", "CGS", "CKG", "CKQ", "CSX", "CZH",
    "CZX", "FLG", "HFE", "HSI", "JIA", "JIN", "JJG", "LUZ",
    "MSN", "NCH", "NKG", "NTG", "TAZ", "TCG", "TOL", "WHI",
    "WUH", "WUW", "WZH", "YCH", "YYA", "YZH", "YZR", "ZHE", "ZJG",
})

PERIOD_COLS = {
    "2025_Q1": 5,
    "2025_Q2": 9,
    "2025_Total": 18,
    "2026_Q1": 22,
    "2026_Q2_W14_19": 23,
}

BLOCK_PREFIXES = {
    "lst": "1.",
    "bsa": "2.",
    "w3": "3.",
    "w3_norm_lst": "4.",
    "w3_ship_rate": "5.",
    "w3_bsa": "6.",
    "hi_w3": "7.",
    "hi_share": "8.",
}

SUPPORT_TABS = {
    "README",
    "Index",
    "BSA_Backdata",
    "2025_Basis",
    "Validation",
    "Target_Input",
    "Sales_Owner_Input",
    "Sales_Target_All",
}


def rgb(hex_color: str) -> dict[str, float]:
    clean = hex_color.strip().lstrip("#")
    return {
        "red": int(clean[0:2], 16) / 255,
        "green": int(clean[2:4], 16) / 255,
        "blue": int(clean[4:6], 16) / 255,
    }


def col_to_a1(idx: int) -> str:
    idx += 1
    out = ""
    while idx:
        idx, rem = divmod(idx - 1, 26)
        out = chr(65 + rem) + out
    return out


def q(title: str) -> str:
    return "'" + title.replace("'", "''") + "'"


def get_creds() -> Credentials:
    creds_dir = ROOT.parent / ".gdrive-mcp"
    credentials_path = creds_dir / "credentials.json"
    token_path = creds_dir / "token.json"
    installed = json.loads(credentials_path.read_text(encoding="utf-8-sig"))["installed"]
    token = json.loads(token_path.read_text(encoding="utf-8-sig"))
    creds = Credentials(
        token=token.get("access_token"),
        refresh_token=token["refresh_token"],
        token_uri=installed["token_uri"],
        client_id=installed["client_id"],
        client_secret=installed["client_secret"],
    )
    creds.refresh(Request())
    return creds


def clean_number(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        if math.isnan(float(value)) or math.isinf(float(value)):
            return None
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    is_percent = text.endswith("%")
    text = text.rstrip("%").replace(",", "")
    try:
        number = float(text)
    except ValueError:
        return None
    return number / 100 if is_percent else number


def clean_text(value: Any, fallback: str = "") -> str:
    if value is None:
        return fallback
    try:
        if pd.isna(value):
            return fallback
    except TypeError:
        pass
    text = str(value).strip()
    if text.lower() in {"", "nan", "none", "nat"}:
        return fallback
    return text


def safe_ratio(num: Any, den: Any) -> float | None:
    n = clean_number(num)
    d = clean_number(den)
    if n is None or d is None or d == 0:
        return None
    return n / d


def value_at(row: list[Any], idx: int) -> Any:
    return row[idx] if idx < len(row) else ""


def parse_report_tab(values: list[list[Any]]) -> dict[str, dict[str, list[Any]]]:
    blocks: dict[str, dict[str, list[Any]]] = {}
    for row_idx, row in enumerate(values):
        title = str(value_at(row, 0)).strip()
        key = next((name for name, prefix in BLOCK_PREFIXES.items() if title.startswith(prefix)), None)
        if not key:
            continue
        block: dict[str, list[Any]] = {}
        data_idx = row_idx + 2
        while data_idx < len(values):
            data_row = values[data_idx]
            team = clean_text(value_at(data_row, 0))
            sales = clean_text(value_at(data_row, 1))
            if not team and not sales:
                break
            if sales:
                block[sales] = data_row
            data_idx += 1
        blocks[key] = block
    return blocks


def metric(blocks: dict[str, dict[str, list[Any]]], block: str, sales: str, period: str) -> float | None:
    row = blocks.get(block, {}).get(sales)
    if not row:
        return None
    return clean_number(value_at(row, PERIOD_COLS[period]))


RAW_KEYS = (
    "lst_2025",
    "w3_2025",
    "bsa_2025",
    "w3_norm_lst_2025",
    "hi_w3_2025",
    "w3_q1",
    "bsa_q1",
    "w3_norm_lst_q1",
    "hi_w3_q1",
    "w3_q2_progress",
    "bsa_q2_progress",
    "w3_norm_lst_q2_progress",
    "hi_w3_q2_progress",
)
DISPLAY_ZERO_SHARE_CUTOFF = 0.0005
CURRENT_ACTIVITY_KEYS = (
    "w3_q1",
    "w3_norm_lst_q1",
    "hi_w3_q1",
    "w3_q2_progress",
    "w3_norm_lst_q2_progress",
    "hi_w3_q2_progress",
)


def build_raw_metrics(sales: str, blocks: dict[str, dict[str, list[Any]]]) -> dict[str, float]:
    return {
        "lst_2025": metric(blocks, "lst", sales, "2025_Total") or 0.0,
        "w3_2025": metric(blocks, "w3", sales, "2025_Total") or 0.0,
        "bsa_2025": metric(blocks, "bsa", sales, "2025_Total") or 0.0,
        "w3_norm_lst_2025": metric(blocks, "w3_norm_lst", sales, "2025_Total") or 0.0,
        "hi_w3_2025": metric(blocks, "hi_w3", sales, "2025_Total") or 0.0,
        "w3_q1": metric(blocks, "w3", sales, "2026_Q1") or 0.0,
        "bsa_q1": metric(blocks, "bsa", sales, "2026_Q1") or 0.0,
        "w3_norm_lst_q1": metric(blocks, "w3_norm_lst", sales, "2026_Q1") or 0.0,
        "hi_w3_q1": metric(blocks, "hi_w3", sales, "2026_Q1") or 0.0,
        "w3_q2_progress": metric(blocks, "w3", sales, "2026_Q2_W14_19") or 0.0,
        "bsa_q2_progress": metric(blocks, "bsa", sales, "2026_Q2_W14_19") or 0.0,
        "w3_norm_lst_q2_progress": metric(blocks, "w3_norm_lst", sales, "2026_Q2_W14_19") or 0.0,
        "hi_w3_q2_progress": metric(blocks, "hi_w3", sales, "2026_Q2_W14_19") or 0.0,
    }


def has_q1_customers(
    origin: str,
    sales: str,
    raw: dict[str, float],
    account_counts: dict[tuple[str, str], tuple[int, int, float | None]],
) -> bool:
    """A salesperson is in scope only if they have at least one 1Q 2026 customer."""
    if account_counts:
        counts = account_counts.get((origin, sales))
        if counts is None:
            return False
        return (counts[0] or 0) > 0
    # Fallback: no account-count source loaded — keep anyone with any Q1 metric > 0.
    return any(raw[k] > 0 for k in ("w3_q1", "bsa_q1", "w3_norm_lst_q1", "hi_w3_q1"))


def has_any_target_base(raw: dict[str, float]) -> bool:
    """True if any of the three 2025 base ratios are computable.

    Target = 2025_base(%) + Target_Input(%p). When all three bases are uncomputable
    (booking needs bsa_2025>0, lifting/high-profit both need w3_2025>0), every
    Target cell would render empty — these salespersons carry no measurable goal
    and are excluded from the tab.
    """
    return raw["bsa_2025"] > 0 or raw["w3_2025"] > 0


def should_exclude_zero_activity_sales(
    origin: str,
    sales: str,
    raw: dict[str, float],
    team_raw: dict[str, float],
    account_counts: dict[tuple[str, str], tuple[int, int, float | None]],
) -> bool:
    share = safe_ratio(raw["lst_2025"], team_raw["lst_2025"]) or 0.0
    if share >= DISPLAY_ZERO_SHARE_CUTOFF:
        return False
    total_accounts = (account_counts.get((origin, sales)) or (0, 0, None))[0] or 0
    if total_accounts > 0:
        return False
    return not any(raw[key] > 0 for key in CURRENT_ACTIVITY_KEYS)


def build_display_row(
    origin: str,
    sales: str,
    raw: dict[str, float],
    team_raw: dict[str, float],
    is_total: bool,
) -> dict[str, Any]:
    return {
        "tab": origin,
        "sales": "Team Total" if is_total else sales,
        "row_type": "TOTAL" if is_total else "SALES",
        "share_2025": 1.0 if is_total else safe_ratio(raw["lst_2025"], team_raw["lst_2025"]),
        "booking_base_2025": safe_ratio(raw["w3_2025"], raw["bsa_2025"]),
        "booking_q1_perform": safe_ratio(raw["w3_q1"], raw["bsa_q1"]),
        "lifting_base_2025": safe_ratio(raw["w3_norm_lst_2025"], raw["w3_2025"]),
        "lifting_q1_perform": safe_ratio(raw["w3_norm_lst_q1"], raw["w3_q1"]),
        "high_profit_base_2025": safe_ratio(raw["hi_w3_2025"], raw["w3_2025"]),
        "high_profit_q1_perform": safe_ratio(raw["hi_w3_q1"], raw["w3_q1"]),
        "booking_q2_progress": safe_ratio(raw["w3_q2_progress"], raw["bsa_q2_progress"]),
        "lifting_q2_progress": safe_ratio(raw["w3_norm_lst_q2_progress"], raw["w3_q2_progress"]),
        "high_profit_q2_progress": safe_ratio(raw["hi_w3_q2_progress"], raw["w3_q2_progress"]),
        "sort_lift_2025": raw["lst_2025"],
    }


def is_report_values(values: list[list[Any]]) -> bool:
    return bool(values and values[0] and str(values[0][0]).startswith("1."))


def get_report_tab_values(service: Any) -> dict[str, list[list[Any]]]:
    meta = (
        service.spreadsheets()
        .get(spreadsheetId=SOURCE_SPREADSHEET_ID, fields="sheets(properties(title))")
        .execute()
    )
    candidates = [
        s["properties"]["title"]
        for s in meta.get("sheets", [])
        if s["properties"]["title"] not in SUPPORT_TABS
    ]
    ranges = [f"{q(title)}!A1:X260" for title in candidates]
    response = (
        service.spreadsheets()
        .values()
        .batchGet(
            spreadsheetId=SOURCE_SPREADSHEET_ID,
            ranges=ranges,
            valueRenderOption="UNFORMATTED_VALUE",
        )
        .execute()
    )
    out: dict[str, list[list[Any]]] = {}
    for title, value_range in zip(candidates, response.get("valueRanges", [])):
        values = value_range.get("values", [])
        if is_report_values(values):
            out[title] = values
    return out


def read_existing_input(service: Any, spreadsheet_id: str) -> dict[str, tuple[float, float, float, str]]:
    """Read existing Target_Input values. Auto-detects old (B/C/D inputs) vs new (B/D/F inputs) layout."""
    try:
        rows = (
            service.spreadsheets()
            .values()
            .get(
                spreadsheetId=spreadsheet_id,
                range=f"{q(INPUT_SHEET)}!A3:H",
                valueRenderOption="UNFORMATTED_VALUE",
            )
            .execute()
            .get("values", [])
        )
    except Exception:
        return {}
    if not rows:
        return {}
    header = rows[0] if rows else []
    is_new = "제안" in str(value_at(header, 2)) or "제안" in str(value_at(header, 4))
    out: dict[str, tuple[float, float, float, str]] = {}
    for row in rows[1:]:
        tab = clean_text(value_at(row, 0))
        if not tab:
            continue
        if is_new:
            booking = clean_number(value_at(row, 1)) or 0.0
            lifting = clean_number(value_at(row, 3)) or 0.0
            hp = clean_number(value_at(row, 5)) or 0.0
            memo = clean_text(value_at(row, 7))
        else:
            booking = clean_number(value_at(row, 1)) or 0.0
            lifting = clean_number(value_at(row, 2)) or 0.0
            hp = clean_number(value_at(row, 3)) or 0.0
            memo = clean_text(value_at(row, 4))
        out[tab] = (booking, lifting, hp, memo)
    return out


def read_sales_owner_input(service: Any, spreadsheet_id: str) -> dict[str, list[str]]:
    try:
        rows = (
            service.spreadsheets()
            .values()
            .get(
                spreadsheetId=spreadsheet_id,
                range=f"{q(OWNER_INPUT_SHEET)}!A1:O",
                valueRenderOption="UNFORMATTED_VALUE",
            )
            .execute()
            .get("values", [])
        )
    except Exception:
        return {}
    if len(rows) < 2:
        return {}
    header_row_idx = next(
        (
            idx
            for idx, row in enumerate(rows)
            if {"Active", "Target_Tab", "Salesman"}.issubset({clean_text(value) for value in row})
        ),
        None,
    )
    if header_row_idx is None:
        return {}
    header = [clean_text(value) for value in rows[header_row_idx]]
    try:
        active_idx = header.index("Active")
        tab_idx = header.index("Target_Tab")
        sales_idx = header.index("Salesman")
    except ValueError:
        return {}

    owners: dict[str, list[str]] = {}
    seen: dict[str, set[str]] = {}
    for row in rows[header_row_idx + 1 :]:
        active = clean_text(value_at(row, active_idx)).upper()
        tab = clean_text(value_at(row, tab_idx))
        sales = clean_text(value_at(row, sales_idx))
        if active not in {"Y", "YES", "TRUE", "1"} or not tab or not sales:
            continue
        if sales in seen.setdefault(tab, set()):
            continue
        seen[tab].add(sales)
        owners.setdefault(tab, []).append(sales)
    return owners


def latest_current_cache() -> Path | None:
    candidates: list[tuple[float, Path]] = []
    for path in (ROOT / "output").glob("_cache_*.parquet"):
        if re.fullmatch(r"_cache_\d{8}", path.stem):
            candidates.append((path.stat().st_mtime, path))
    if not candidates:
        return None
    return max(candidates)[1]


def latest_booking_snapshot_csv() -> Path | None:
    candidates: list[tuple[float, Path]] = []
    for path in (ROOT / "output").glob("booking_snapshot_result_*.csv"):
        if re.fullmatch(r"booking_snapshot_result_\d{8}", path.stem):
            candidates.append((path.stat().st_mtime, path))
    if not candidates:
        return None
    return max(candidates)[1]


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


def build_account_counts() -> dict[tuple[str, str], tuple[int, int, float | None]]:
    columns = [
        "BKG_SHPR_CST_NO",
        "POR_CTR_CD",
        "POR_PLC_CD",
        "DLY_CTR_CD",
        "FST_TEU",
        "YYYYMM",
        "Lead_time (BKG_Sche)",
        "Salesman_POR",
    ]
    cache = latest_current_cache()
    if cache is not None:
        frame = pd.read_parquet(cache, columns=columns)
    else:
        csv_path = latest_booking_snapshot_csv()
        if csv_path is None:
            return {}
        frame = pd.read_csv(
            csv_path,
            usecols=columns,
            dtype=str,
            encoding="utf-8-sig",
            low_memory=False,
        )
    for col in ["BKG_SHPR_CST_NO", "POR_CTR_CD", "POR_PLC_CD", "DLY_CTR_CD", "YYYYMM", "Lead_time (BKG_Sche)", "Salesman_POR"]:
        frame[col] = frame[col].map(clean_text)
    frame["Salesman_POR"] = frame["Salesman_POR"].replace("", MISSING_SALES)
    frame["fst"] = pd.to_numeric(
        frame["FST_TEU"].fillna("").astype(str).str.replace(",", "", regex=False),
        errors="coerce",
    ).fillna(0.0)
    frame["team"] = [classify_team(o, d) for o, d in zip(frame["POR_CTR_CD"], frame["DLY_CTR_CD"])]
    frame["tab"] = [tab_key(o, p) for o, p in zip(frame["POR_CTR_CD"], frame["POR_PLC_CD"])]
    frame = frame.loc[
        frame["YYYYMM"].isin(Q1_2026)
        & frame["team"].eq("OBT")
        & frame["fst"].gt(0)
        & frame["BKG_SHPR_CST_NO"].ne("")
    ].copy()
    if frame.empty:
        return {}

    total = frame.groupby(["tab", "Salesman_POR"], dropna=False)["BKG_SHPR_CST_NO"].nunique()
    w3 = (
        frame.loc[frame["Lead_time (BKG_Sche)"].eq("WOS-3")]
        .groupby(["tab", "Salesman_POR"], dropna=False)["BKG_SHPR_CST_NO"]
        .nunique()
    )
    total_tab = frame.groupby("tab", dropna=False)["BKG_SHPR_CST_NO"].nunique()
    w3_tab = (
        frame.loc[frame["Lead_time (BKG_Sche)"].eq("WOS-3")]
        .groupby("tab", dropna=False)["BKG_SHPR_CST_NO"]
        .nunique()
    )

    out: dict[tuple[str, str], tuple[int, int, float | None]] = {}
    keys = set(total.index) | set(w3.index)
    for tab, sales in keys:
        total_count = int(total.get((tab, sales), 0))
        w3_count = int(w3.get((tab, sales), 0))
        out[(tab, sales)] = (total_count, w3_count, w3_count / total_count if total_count else None)
    for tab in set(total_tab.index) | set(w3_tab.index):
        total_count = int(total_tab.get(tab, 0))
        w3_count = int(w3_tab.get(tab, 0))
        out[(tab, "Team Total")] = (total_count, w3_count, w3_count / total_count if total_count else None)
    return out


def blank_none(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return ""
    return value


def per_origin_target_formula(row: int, base_col: str, origin_literal: str, input_col: int) -> str:
    # Target = 2025 base (%) + Target_Input increase (%). e.g. 15% base + 10% input = 25%.
    # Target_Input columns: A=Tab, B=Booking입력, C=Booking제안, D=Lifting입력, E=Lifting제안,
    # F=HP입력, G=HP제안, H=Memo. Targets reference the 입력 columns (B/D/F).
    return (
        f'=IF(${base_col}{row}="","",'
        f'${base_col}{row}+IFERROR(VLOOKUP("{origin_literal}",{INPUT_SHEET}!$A:$H,{input_col},FALSE),0))'
    )


def summary_target_formula(row: int, base_col: str, input_col: int) -> str:
    return (
        f'=IF(${base_col}{row}="","",'
        f'${base_col}{row}+IFERROR(VLOOKUP($A{row},{INPUT_SHEET}!$A:$H,{input_col},FALSE),0))'
    )


def diff_formula(row: int, target_col: str, actual_col: str) -> str:
    return f'=IF(OR({target_col}{row}="",{actual_col}{row}=""),"",{actual_col}{row}-{target_col}{row})'


def header_block(include_tab_col: bool) -> list[list[Any]]:
    """Return the 4 header rows used by per-origin and summary tabs."""
    name_header = ["Tab", "Name"] if include_tab_col else ["Name"]
    row2 = name_header + [
        "2025\nShare of\nTotal\nLifting",
        "2025\n3W Before\nBooking\nRate",
        "3W before Booking Rate (vs BSA)", "", "", "", "", "",
        "3W before Actual Lifting Rate", "", "", "", "", "",
        "3W before High-Profit Customer Rate", "", "", "", "", "",
        "No. of A/Cs (1Q 2026)", "", "",
    ]
    pad = [""] * len(name_header)
    row3 = pad + [
        "", "",
        "1Q 2026", "", "", "2Q 2026", "", "",
        "1Q 2026", "", "", "2Q 2026", "", "",
        "1Q 2026", "", "", "2Q 2026", "", "",
        "No. of\nTotal\nA/Cs", "No. of\n3W before\nA/Cs", "%",
    ]
    row4 = pad + [
        "", "",
        "Target\n(%)", "Perform.\n(%)", "'+/-",
        "Target\n(%)", "Progress\n%", "'+/-",
        "Target\n(%)", "Perform.\n(%)", "'+/-",
        "Target\n(%)", "Progress\n%", "'+/-",
        "Target\n(%)", "Perform.\n(%)", "'+/-",
        "Target\n(%)", "Progress\n%", "'+/-",
        "", "", "",
    ]
    return [row2, row3, row4]


def make_data_row(
    item: dict[str, Any],
    sheet_row: int,
    counts: tuple[Any, Any, Any],
    base_cols: dict[str, str],
    origin_literal: str | None,
    target_cols: tuple[str, str, str, str, str, str],
    perform_cols: tuple[str, str, str, str, str, str],
    leading: list[Any],
) -> list[Any]:
    # The 6 target columns correspond to 1Q booking, 2Q booking, 1Q lifting, 2Q lifting, 1Q HP, 2Q HP.
    # Target_Input layout puts input columns at B=2, D=4, F=6 (with suggested values at C/E/G in between).
    if origin_literal is None:
        booking_target_1q = summary_target_formula(sheet_row, base_cols["booking"], 2)
        booking_target_2q = summary_target_formula(sheet_row, base_cols["booking"], 2)
        lifting_target_1q = summary_target_formula(sheet_row, base_cols["lifting"], 4)
        lifting_target_2q = summary_target_formula(sheet_row, base_cols["lifting"], 4)
        hp_target_1q = summary_target_formula(sheet_row, base_cols["hp"], 6)
        hp_target_2q = summary_target_formula(sheet_row, base_cols["hp"], 6)
    else:
        booking_target_1q = per_origin_target_formula(sheet_row, base_cols["booking"], origin_literal, 2)
        booking_target_2q = per_origin_target_formula(sheet_row, base_cols["booking"], origin_literal, 2)
        lifting_target_1q = per_origin_target_formula(sheet_row, base_cols["lifting"], origin_literal, 4)
        lifting_target_2q = per_origin_target_formula(sheet_row, base_cols["lifting"], origin_literal, 4)
        hp_target_1q = per_origin_target_formula(sheet_row, base_cols["hp"], origin_literal, 6)
        hp_target_2q = per_origin_target_formula(sheet_row, base_cols["hp"], origin_literal, 6)

    return [
        *leading,
        item["sales"],
        blank_none(item["share_2025"]),
        blank_none(item["booking_base_2025"]),
        booking_target_1q,
        blank_none(item["booking_q1_perform"]),
        diff_formula(sheet_row, target_cols[0], perform_cols[0]),
        booking_target_2q,
        blank_none(item.get("booking_q2_progress")),
        diff_formula(sheet_row, target_cols[1], perform_cols[1]),
        lifting_target_1q,
        blank_none(item["lifting_q1_perform"]),
        diff_formula(sheet_row, target_cols[2], perform_cols[2]),
        lifting_target_2q,
        blank_none(item.get("lifting_q2_progress")),
        diff_formula(sheet_row, target_cols[3], perform_cols[3]),
        hp_target_1q,
        blank_none(item["high_profit_q1_perform"]),
        diff_formula(sheet_row, target_cols[4], perform_cols[4]),
        hp_target_2q,
        blank_none(item.get("high_profit_q2_progress")),
        diff_formula(sheet_row, target_cols[5], perform_cols[5]),
        "" if counts[0] is None else counts[0],
        "" if counts[1] is None else counts[1],
        blank_none(counts[2]),
        blank_none(item["booking_base_2025"]),
        blank_none(item["lifting_base_2025"]),
        blank_none(item["high_profit_base_2025"]),
        item["row_type"],
    ]


def row_counts(
    origin: str,
    item: dict[str, Any],
    rows: list[dict[str, Any]],
    account_counts: dict[tuple[str, str], tuple[int, int, float | None]],
) -> tuple[Any, Any, Any]:
    if item.get("row_type") != "TOTAL":
        return account_counts.get((origin, item["sales"]), (None, None, None))

    total_count = 0
    w3_count = 0
    found = False
    for row in rows:
        if row.get("row_type") == "TOTAL":
            continue
        counts = account_counts.get((origin, row["sales"]))
        if counts is None:
            continue
        total_count += counts[0] or 0
        w3_count += counts[1] or 0
        found = True
    if not found:
        return account_counts.get((origin, "Team Total"), (None, None, None))
    return total_count, w3_count, (w3_count / total_count if total_count else None)


def select_display_sales_names(
    origin: str,
    blocks: dict[str, dict[str, list[Any]]],
    account_counts: dict[tuple[str, str], tuple[int, int, float | None]] | None = None,
    owner_order: list[str] | None = None,
) -> tuple[list[str], dict[str, dict[str, float]]]:
    counts = account_counts or {}
    metric_sales_names = {
        name
        for block in blocks.values()
        for name in block
        if name not in {"TOTAL", NO_BASIS_LABEL}
    }
    sales_names = set(metric_sales_names)
    if owner_order:
        sales_names.update(owner_order)
    raw_by_sales: dict[str, dict[str, float]] = {
        name: build_raw_metrics(name, blocks) for name in sales_names
    }
    team_raw_all: dict[str, float] = {k: 0.0 for k in RAW_KEYS}
    for raw in raw_by_sales.values():
        for k in RAW_KEYS:
            team_raw_all[k] += raw[k]

    def sort_key(name: str) -> tuple[int, float, str]:
        # Keep the missing-owner bucket at the bottom; sort the rest by 2025 LST volume.
        bucket = 1 if name == MISSING_SALES else 0
        return (bucket, -raw_by_sales[name]["lst_2025"], name)

    def auto_keep(name: str) -> bool:
        return (
            has_q1_customers(origin, name, raw_by_sales[name], counts)
            and has_any_target_base(raw_by_sales[name])
            and not should_exclude_zero_activity_sales(origin, name, raw_by_sales[name], team_raw_all, counts)
        )

    if not owner_order:
        kept = [name for name in sales_names if auto_keep(name)]
        kept.sort(key=sort_key)
        return kept, raw_by_sales

    # Sales_Owner_Input (the org-chart sheet) is the authoritative roster.
    # Honour its order exactly and never append data-derived names — bookings
    # made by a salesperson under a different origin's port should not pull
    # that salesperson into this origin's table.
    kept: list[str] = []
    seen: set[str] = set()
    for name in owner_order:
        if (
            name in raw_by_sales
            and name not in seen
            and not should_exclude_zero_activity_sales(origin, name, raw_by_sales[name], team_raw_all, counts)
        ):
            kept.append(name)
            seen.add(name)
    return kept, raw_by_sales


def collect_rows_for_origin(
    origin: str,
    blocks: dict[str, dict[str, list[Any]]],
    account_counts: dict[tuple[str, str], tuple[int, int, float | None]] | None = None,
    owner_order: list[str] | None = None,
) -> list[dict[str, Any]]:
    """Return display rows for one origin.

    Salespersons with no 1Q 2026 customers are dropped entirely. The Team Total row
    is recomputed from the surviving salespersons so denominators stay consistent.
    `(미지정)` always sorts to the bottom regardless of its 2025 lifting volume.
    """
    kept, raw_by_sales = select_display_sales_names(origin, blocks, account_counts, owner_order)

    team_raw: dict[str, float] = {k: 0.0 for k in RAW_KEYS}
    for name in kept:
        for k in RAW_KEYS:
            team_raw[k] += raw_by_sales[name][k]

    rows = [build_display_row(origin, "Team Total", team_raw, team_raw, is_total=True)]
    for name in kept:
        rows.append(build_display_row(origin, name, raw_by_sales[name], team_raw, is_total=False))
    return rows


def build_origin_sheet_values(
    origin: str,
    rows: list[dict[str, Any]],
    account_counts: dict[tuple[str, str], tuple[int, int, float | None]],
) -> list[list[Any]]:
    title = [f"{origin} — 2026 OBT Sales Target & Performance"]
    values: list[list[Any]] = [title]
    values.extend(header_block(include_tab_col=False))
    # Per-origin tab has Name in col A. Targets occupy D-U, A/Cs V-X, hidden bases Y/Z/AA.
    target_cols = ("D", "G", "J", "M", "P", "S")
    perform_for_diff = ("E", "H", "K", "N", "Q", "T")  # 2Q perform initially blank
    base_cols = {"booking": "Y", "lifting": "Z", "hp": "AA"}
    for item in rows:
        sheet_row = len(values) + 1
        counts = row_counts(origin, item, rows, account_counts)
        values.append(
            make_data_row(
                item=item,
                sheet_row=sheet_row,
                counts=counts,
                base_cols=base_cols,
                origin_literal=origin,
                target_cols=target_cols,
                perform_cols=perform_for_diff,
                leading=[],
            )
        )
    return values


def build_summary_values(
    report_tabs: dict[str, list[list[Any]]],
    account_counts: dict[tuple[str, str], tuple[int, int, float | None]],
    ordered_origins: list[str] | None = None,
    owner_orders: dict[str, list[str]] | None = None,
) -> list[list[Any]]:
    values: list[list[Any]] = [["2026 OBT Sales Target — All 선적지 통합"]]
    values.extend(header_block(include_tab_col=True))
    target_cols = ("E", "H", "K", "N", "Q", "T")
    perform_for_diff = ("F", "I", "L", "O", "R", "U")
    base_cols = {"booking": "Z", "lifting": "AA", "hp": "AB"}
    iter_origins = ordered_origins if ordered_origins is not None else sorted(report_tabs)
    for origin in iter_origins:
        if origin not in report_tabs:
            continue
        blocks = parse_report_tab(report_tabs[origin])
        if not {"lst", "bsa", "w3", "w3_norm_lst", "hi_w3"}.issubset(blocks):
            continue
        rows = collect_rows_for_origin(origin, blocks, account_counts, (owner_orders or {}).get(origin))
        for item in rows:
            sheet_row = len(values) + 1
            counts = row_counts(origin, item, rows, account_counts)
            values.append(
                make_data_row(
                    item=item,
                    sheet_row=sheet_row,
                    counts=counts,
                    base_cols=base_cols,
                    origin_literal=None,
                    target_cols=target_cols,
                    perform_cols=perform_for_diff,
                    leading=[origin],
                )
            )
    return values


DEFAULT_INCREASE = 0.10  # 2025 실적(%)에 그대로 더해지는 목표 증대율 (예: 15% + 10% = 25%)


def _round_half_pp(value: float) -> float:
    """Round a fraction to the nearest 0.5 %p (i.e. 0.005)."""
    return round(value * 200) / 200


def suggest_pp(base: float | None, perform: float | None, *, is_hp: bool) -> tuple[float, str]:
    """Suggest a target +%p and a one-word reason explaining how it was chosen.

    - Booking / Lifting: match the Q1 momentum (Q1 perform − 2025 base), clamped [5%p, 20%p].
    - High-profit: same idea but clamped [2%p, 10%p] (relative metric, conservative).
    - If base + suggestion would exceed 100 %, the suggestion is reduced to fit.
    - If base is None (no 2025 measurement), default to 5%p.

    Returns (suggestion_fraction, reason). Reasons: "no_base", "low_clamp",
    "high_clamp", "cap_100", "match".
    """
    if base is None:
        return 0.05, "no_base"
    delta = (perform - base) if perform is not None else 0.0
    raw = _round_half_pp(delta)
    if is_hp:
        lo, hi = 0.02, 0.10
    else:
        lo, hi = 0.05, 0.20
    if raw < lo:
        suggestion, reason = lo, "low_clamp"
    elif raw > hi:
        suggestion, reason = hi, "high_clamp"
    else:
        suggestion, reason = raw, "match"
    if base + suggestion > 1.0:
        suggestion = max(0.0, _round_half_pp(1.0 - base))
        reason = "cap_100"
    return suggestion, reason


def compute_team_total_raw(
    origin: str,
    blocks: dict[str, dict[str, list[Any]]],
    account_counts: dict[tuple[str, str], tuple[int, int, float | None]] | None,
    owner_order: list[str] | None = None,
) -> dict[str, float]:
    """Replay the filter+aggregation used by collect_rows_for_origin to recover Team Total raw values."""
    kept, raw_by_sales = select_display_sales_names(origin, blocks, account_counts, owner_order)
    team_raw = {k: 0.0 for k in RAW_KEYS}
    for name in kept:
        for k in RAW_KEYS:
            team_raw[k] += raw_by_sales[name][k]
    return team_raw


def compute_suggestions(
    report_tabs: dict[str, list[list[Any]]],
    account_counts: dict[tuple[str, str], tuple[int, int, float | None]] | None,
    origins: list[str],
    owner_orders: dict[str, list[str]] | None = None,
) -> dict[str, dict[str, Any]]:
    """For every origin, recommend a +%p for booking / lifting / high-profit and capture
    the per-metric base, performance and reason so a rationale can be written into Memo.
    """
    out: dict[str, dict[str, Any]] = {}
    for origin in origins:
        if origin not in report_tabs:
            continue
        blocks = parse_report_tab(report_tabs[origin])
        if not {"lst", "bsa", "w3", "w3_norm_lst", "hi_w3"}.issubset(blocks):
            continue
        team = compute_team_total_raw(origin, blocks, account_counts, (owner_orders or {}).get(origin))
        booking_base = safe_ratio(team["w3_2025"], team["bsa_2025"])
        lifting_base = safe_ratio(team["w3_norm_lst_2025"], team["w3_2025"])
        hp_base = safe_ratio(team["hi_w3_2025"], team["w3_2025"])
        booking_perf = safe_ratio(team["w3_q1"], team["bsa_q1"])
        lifting_perf = safe_ratio(team["w3_norm_lst_q1"], team["w3_q1"])
        hp_perf = safe_ratio(team["hi_w3_q1"], team["w3_q1"])
        booking_pp, booking_why = suggest_pp(booking_base, booking_perf, is_hp=False)
        lifting_pp, lifting_why = suggest_pp(lifting_base, lifting_perf, is_hp=False)
        hp_pp, hp_why = suggest_pp(hp_base, hp_perf, is_hp=True)
        out[origin] = {
            "booking_pp": booking_pp,
            "lifting_pp": lifting_pp,
            "hp_pp": hp_pp,
            "metrics": {
                "booking": {"base": booking_base, "perf": booking_perf, "pp": booking_pp, "why": booking_why},
                "lifting": {"base": lifting_base, "perf": lifting_perf, "pp": lifting_pp, "why": lifting_why},
                "hp": {"base": hp_base, "perf": hp_perf, "pp": hp_pp, "why": hp_why},
            },
        }
    return out


def _format_rationale(label: str, metric: dict[str, Any]) -> str:
    base = metric["base"]
    perf = metric["perf"]
    pp = metric["pp"]
    why = metric["why"]
    if base is None:
        return f"{label}: 2025 측정 불가 → 기본 {pp*100:.0f}%p"
    base_s = f"{base*100:.1f}%"
    if perf is None:
        return f"{label}: 2025 {base_s}, 1Q 측정 불가 → 기본 {pp*100:.0f}%p"
    perf_s = f"{perf*100:.1f}%"
    delta = (perf - base) * 100
    delta_s = f"{delta:+.1f}%p"
    suffix_map = {
        "low_clamp": f"하한 {pp*100:.0f}%p",
        "high_clamp": f"상한 {pp*100:.0f}%p",
        "cap_100": f"100% 한도 {pp*100:.1f}%p",
        "match": f"{pp*100:.1f}%p",
    }
    return f"{label}: {base_s}→{perf_s} ({delta_s}) → {suffix_map.get(why, f'{pp*100:.1f}%p')}"


def build_rationale_memo(suggestion: dict[str, Any]) -> str:
    metrics = suggestion.get("metrics") if isinstance(suggestion, dict) else None
    if not metrics:
        return ""
    return "\n".join(
        [
            _format_rationale("부킹", metrics["booking"]),
            _format_rationale("실선적", metrics["lifting"]),
            _format_rationale("고수익", metrics["hp"]),
        ]
    )


def build_input_values(
    origins: list[str],
    existing: dict[str, tuple[float, float, float, str]],
    suggestions: dict[str, dict[str, float | None]] | None = None,
) -> list[list[Any]]:
    suggestions = suggestions or {}
    values: list[list[Any]] = [
        ["선적지별 2026 목표 증대율 입력 + 추천 목표"],
        [
            "입력값(%p)은 2025 실적(%)에 그대로 더해집니다. "
            "'제안' 열은 2025 base 대비 1Q 2026 진척 모멘텀을 0.5%p 단위로 반올림한 값입니다. "
            "부킹/실선적은 [5%p, 20%p], 고수익은 [2%p, 10%p] 범위 클램프."
        ],
        [
            "Tab",
            "3W Booking\n입력 (%p)",
            "3W Booking\n제안 (%p)",
            "Actual Lifting\n입력 (%p)",
            "Actual Lifting\n제안 (%p)",
            "High-Profit\n입력 (%p)",
            "High-Profit\n제안 (%p)",
            "Memo",
        ],
    ]
    for origin in origins:
        booking, lifting, hp, memo = existing.get(
            origin, (DEFAULT_INCREASE, DEFAULT_INCREASE, DEFAULT_INCREASE, "")
        )
        s = suggestions.get(origin, {})
        # Memo gets the analytical rationale for each metric's suggestion. It rebuilds
        # on every run, so user-typed memo content is intentionally not preserved here.
        memo_text = build_rationale_memo(s) or memo
        values.append(
            [
                origin,
                booking,
                s.get("booking_pp", DEFAULT_INCREASE),
                lifting,
                s.get("lifting_pp", DEFAULT_INCREASE),
                hp,
                s.get("hp_pp", DEFAULT_INCREASE),
                memo_text,
            ]
        )
    return values


def build_readme_values() -> list[list[Any]]:
    return [
        ["2026 OBT Sales Target by 선적지"],
        [""],
        ["■ 구조"],
        ["• Target_Input : 선적지별 2026 증대율 입력 + 추천(B/D/F 입력, C/E/G 제안값 살구색)"],
        ["• Summary_All  : 전체 선적지 통합 view (참고용)"],
        ["• <선적지>     : 선적지별 Team Total + 영업사원 행 (실제 작업 탭)"],
        [""],
        ["■ 추천 목표 산식 (Target_Input의 C/E/G 열)"],
        ["• Q1 momentum = 1Q 2026 perform − 2025 base, 0.5%p 단위 반올림"],
        ["• 부킹 / 실선적: [5%p, 20%p] 범위 클램프"],
        ["• 고수익화주: [2%p, 10%p] (상대평가 지표라 보수적)"],
        ["• base + 추천 > 100% 면 100% 이내로 자동 축소"],
        ["• Memo(H)에 각 항목별 base→perform (Δ%p) + 적용 룰이 자동 기재됨"],
        [""],
        ["■ Target 산식"],
        ["• 1Q Target = 2Q Target = 2025 전체 실적(%) + Target_Input 증대율(%p)"],
        ["• 예: 2025 실적 15% + Target_Input 10% → 목표 25% (덧셈으로 그대로 더해짐)"],
        ["• 1Q와 2Q 목표가 동일한 % (전월 실적/상황에 따라 Target_Input에서 조정 가능)"],
        ["• 3W 부킹률   = W3 부킹량 / BSA"],
        ["• 실선적률    = W3 정상선적량 / W3 부킹량"],
        ["• 고수익화주  = 3W 부킹 중 고수익 화주 비중 (상대평가 지표라 합계가 100% 가 아님)"],
        [""],
        ["■ 실적"],
        ["• 1Q 2026 = 확정 실적"],
        ["• 2Q 2026 = 14~19주차 진척률 (14주차 시작 후 데이터 채워짐)"],
        [""],
        ["■ 화주수 비중"],
        ["• 전체 화주수 대비 3W 부킹 화주수 비중은 목표설정 없이 1Q 실적만 표시"],
        ["• 향후 목표 설정 검토 예정"],
        [""],
        ["■ 데이터 출처"],
        [f"• 원본 보고서 스프레드시트: https://docs.google.com/spreadsheets/d/{SOURCE_SPREADSHEET_ID}/edit"],
        ["• 재생성 스크립트: scripts/create_per_origin_target_workbook.py"],
    ]


def origin_format_requests(sheet_id: int, row_count: int) -> list[dict[str, Any]]:
    last_data_row = max(row_count, 5)
    header_blue = rgb("CFE2F3")
    title_fill = rgb("D9EAD3")
    total_fill = rgb("D9D9D9")
    border = {"style": "SOLID", "color": rgb("000000")}
    thin_gray = {"style": "SOLID", "color": rgb("B7B7B7")}
    requests: list[dict[str, Any]] = [
        {
            "updateSheetProperties": {
                "properties": {
                    "sheetId": sheet_id,
                    "gridProperties": {
                        "rowCount": last_data_row + 20,
                        "columnCount": 28,
                        "frozenRowCount": 4,
                        "frozenColumnCount": 0,
                    },
                },
                "fields": "gridProperties(rowCount,columnCount,frozenRowCount,frozenColumnCount)",
            }
        },
        {
            "mergeCells": {
                "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1, "startColumnIndex": 0, "endColumnIndex": 24},
                "mergeType": "MERGE_ALL",
            }
        },
    ]
    # Header merges (per-origin: Name col A, base cols B/C, target groups D:W, A/C W:Y)
    for start, end in [(0, 1), (1, 2), (2, 3)]:
        requests.append(
            {
                "mergeCells": {
                    "range": {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": 4, "startColumnIndex": start, "endColumnIndex": end},
                    "mergeType": "MERGE_ALL",
                }
            }
        )
    for start, end in [(3, 9), (9, 15), (15, 21), (21, 24)]:
        requests.append(
            {
                "mergeCells": {
                    "range": {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": 2, "startColumnIndex": start, "endColumnIndex": end},
                    "mergeType": "MERGE_ALL",
                }
            }
        )
    for start in [3, 6, 9, 12, 15, 18]:
        requests.append(
            {
                "mergeCells": {
                    "range": {"sheetId": sheet_id, "startRowIndex": 2, "endRowIndex": 3, "startColumnIndex": start, "endColumnIndex": start + 3},
                    "mergeType": "MERGE_ALL",
                }
            }
        )
    for start, end in [(21, 22), (22, 23), (23, 24)]:
        requests.append(
            {
                "mergeCells": {
                    "range": {"sheetId": sheet_id, "startRowIndex": 2, "endRowIndex": 4, "startColumnIndex": start, "endColumnIndex": end},
                    "mergeType": "MERGE_ALL",
                }
            }
        )

    requests.extend(
        [
            {
                "repeatCell": {
                    "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1, "startColumnIndex": 0, "endColumnIndex": 24},
                    "cell": {
                        "userEnteredFormat": {
                            "backgroundColor": title_fill,
                            "textFormat": {"bold": True, "fontSize": 12},
                            "horizontalAlignment": "LEFT",
                        }
                    },
                    "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)",
                }
            },
            {
                "repeatCell": {
                    "range": {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": 4, "startColumnIndex": 0, "endColumnIndex": 24},
                    "cell": {
                        "userEnteredFormat": {
                            "backgroundColor": header_blue,
                            "textFormat": {"bold": True},
                            "horizontalAlignment": "CENTER",
                            "verticalAlignment": "MIDDLE",
                            "wrapStrategy": "WRAP",
                        }
                    },
                    "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment,verticalAlignment,wrapStrategy)",
                }
            },
            {
                "repeatCell": {
                    "range": {"sheetId": sheet_id, "startRowIndex": 4, "endRowIndex": last_data_row, "startColumnIndex": 0, "endColumnIndex": 24},
                    "cell": {
                        "userEnteredFormat": {
                            "horizontalAlignment": "CENTER",
                            "verticalAlignment": "MIDDLE",
                        }
                    },
                    "fields": "userEnteredFormat(horizontalAlignment,verticalAlignment)",
                }
            },
            # Percent columns: C-V (col idx 1..20 are within booking/lifting/hp data and A/C %)
            {
                "repeatCell": {
                    "range": {"sheetId": sheet_id, "startRowIndex": 4, "endRowIndex": last_data_row, "startColumnIndex": 1, "endColumnIndex": 21},
                    "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.0%"}}},
                    "fields": "userEnteredFormat.numberFormat",
                }
            },
            # A/C count columns (V, W = col idx 21, 22) integer; X (idx 23) percent
            {
                "repeatCell": {
                    "range": {"sheetId": sheet_id, "startRowIndex": 4, "endRowIndex": last_data_row, "startColumnIndex": 21, "endColumnIndex": 23},
                    "cell": {"userEnteredFormat": {"numberFormat": {"type": "NUMBER", "pattern": "#,##0"}}},
                    "fields": "userEnteredFormat.numberFormat",
                }
            },
            {
                "repeatCell": {
                    "range": {"sheetId": sheet_id, "startRowIndex": 4, "endRowIndex": last_data_row, "startColumnIndex": 23, "endColumnIndex": 24},
                    "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.0%"}}},
                    "fields": "userEnteredFormat.numberFormat",
                }
            },
            {
                "updateBorders": {
                    "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": last_data_row, "startColumnIndex": 0, "endColumnIndex": 24},
                    "top": thin_gray,
                    "bottom": thin_gray,
                    "left": thin_gray,
                    "right": thin_gray,
                    "innerHorizontal": thin_gray,
                    "innerVertical": thin_gray,
                }
            },
            {
                "updateBorders": {
                    "range": {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": 4, "startColumnIndex": 0, "endColumnIndex": 24},
                    "top": border,
                    "bottom": border,
                    "left": border,
                    "right": border,
                    "innerHorizontal": border,
                    "innerVertical": border,
                }
            },
            {
                "updateDimensionProperties": {
                    "range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": 0, "endIndex": 1},
                    "properties": {"pixelSize": 160},
                    "fields": "pixelSize",
                }
            },
            {
                "updateDimensionProperties": {
                    "range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": 1, "endIndex": 24},
                    "properties": {"pixelSize": 80},
                    "fields": "pixelSize",
                }
            },
            {
                "updateDimensionProperties": {
                    "range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": 24, "endIndex": 28},
                    "properties": {"hiddenByUser": True},
                    "fields": "hiddenByUser",
                }
            },
            {
                "updateDimensionProperties": {
                    "range": {"sheetId": sheet_id, "dimension": "ROWS", "startIndex": 1, "endIndex": 4},
                    "properties": {"pixelSize": 36},
                    "fields": "pixelSize",
                }
            },
        ]
    )

    # Team Total highlight (col A = "Team Total")
    requests.append(
        {
            "addConditionalFormatRule": {
                "rule": {
                    "ranges": [
                        {"sheetId": sheet_id, "startRowIndex": 4, "endRowIndex": last_data_row, "startColumnIndex": 0, "endColumnIndex": 24}
                    ],
                    "booleanRule": {
                        "condition": {"type": "CUSTOM_FORMULA", "values": [{"userEnteredValue": '=$A5="Team Total"'}]},
                        "format": {"backgroundColor": total_fill, "textFormat": {"bold": True}},
                    },
                },
                "index": 0,
            }
        }
    )
    # +/- columns (idx 5, 8, 11, 14, 17, 20) colored green/red
    for col in [5, 8, 11, 14, 17, 20]:
        requests.extend(
            [
                {
                    "addConditionalFormatRule": {
                        "rule": {
                            "ranges": [
                                {"sheetId": sheet_id, "startRowIndex": 4, "endRowIndex": last_data_row, "startColumnIndex": col, "endColumnIndex": col + 1}
                            ],
                            "booleanRule": {
                                "condition": {"type": "NUMBER_GREATER", "values": [{"userEnteredValue": "0"}]},
                                "format": {"textFormat": {"foregroundColor": rgb("008000"), "bold": True}},
                            },
                        },
                        "index": 0,
                    }
                },
                {
                    "addConditionalFormatRule": {
                        "rule": {
                            "ranges": [
                                {"sheetId": sheet_id, "startRowIndex": 4, "endRowIndex": last_data_row, "startColumnIndex": col, "endColumnIndex": col + 1}
                            ],
                            "booleanRule": {
                                "condition": {"type": "NUMBER_LESS", "values": [{"userEnteredValue": "0"}]},
                                "format": {"textFormat": {"foregroundColor": rgb("CC0000"), "bold": True}},
                            },
                        },
                        "index": 0,
                    }
                },
            ]
        )
    return requests


def summary_format_requests(sheet_id: int, row_count: int) -> list[dict[str, Any]]:
    last_data_row = max(row_count, 5)
    header_blue = rgb("CFE2F3")
    title_fill = rgb("D9EAD3")
    total_fill = rgb("D9D9D9")
    border = {"style": "SOLID", "color": rgb("000000")}
    thin_gray = {"style": "SOLID", "color": rgb("B7B7B7")}

    requests: list[dict[str, Any]] = [
        {
            "updateSheetProperties": {
                "properties": {
                    "sheetId": sheet_id,
                    "gridProperties": {
                        "rowCount": last_data_row + 20,
                        "columnCount": 29,
                        "frozenRowCount": 4,
                        "frozenColumnCount": 0,
                    },
                },
                "fields": "gridProperties(rowCount,columnCount,frozenRowCount,frozenColumnCount)",
            }
        },
        {
            "mergeCells": {
                "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1, "startColumnIndex": 0, "endColumnIndex": 25},
                "mergeType": "MERGE_ALL",
            }
        },
    ]
    for start, end in [(0, 1), (1, 2), (2, 3), (3, 4)]:
        requests.append(
            {
                "mergeCells": {
                    "range": {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": 4, "startColumnIndex": start, "endColumnIndex": end},
                    "mergeType": "MERGE_ALL",
                }
            }
        )
    for start, end in [(4, 10), (10, 16), (16, 22), (22, 25)]:
        requests.append(
            {
                "mergeCells": {
                    "range": {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": 2, "startColumnIndex": start, "endColumnIndex": end},
                    "mergeType": "MERGE_ALL",
                }
            }
        )
    for start in [4, 7, 10, 13, 16, 19]:
        requests.append(
            {
                "mergeCells": {
                    "range": {"sheetId": sheet_id, "startRowIndex": 2, "endRowIndex": 3, "startColumnIndex": start, "endColumnIndex": start + 3},
                    "mergeType": "MERGE_ALL",
                }
            }
        )
    for start, end in [(22, 23), (23, 24), (24, 25)]:
        requests.append(
            {
                "mergeCells": {
                    "range": {"sheetId": sheet_id, "startRowIndex": 2, "endRowIndex": 4, "startColumnIndex": start, "endColumnIndex": end},
                    "mergeType": "MERGE_ALL",
                }
            }
        )

    requests.extend(
        [
            {
                "repeatCell": {
                    "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1, "startColumnIndex": 0, "endColumnIndex": 25},
                    "cell": {
                        "userEnteredFormat": {
                            "backgroundColor": title_fill,
                            "textFormat": {"bold": True, "fontSize": 12},
                            "horizontalAlignment": "LEFT",
                        }
                    },
                    "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)",
                }
            },
            {
                "repeatCell": {
                    "range": {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": 4, "startColumnIndex": 0, "endColumnIndex": 25},
                    "cell": {
                        "userEnteredFormat": {
                            "backgroundColor": header_blue,
                            "textFormat": {"bold": True},
                            "horizontalAlignment": "CENTER",
                            "verticalAlignment": "MIDDLE",
                            "wrapStrategy": "WRAP",
                        }
                    },
                    "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment,verticalAlignment,wrapStrategy)",
                }
            },
            {
                "repeatCell": {
                    "range": {"sheetId": sheet_id, "startRowIndex": 4, "endRowIndex": last_data_row, "startColumnIndex": 0, "endColumnIndex": 25},
                    "cell": {
                        "userEnteredFormat": {
                            "horizontalAlignment": "CENTER",
                            "verticalAlignment": "MIDDLE",
                        }
                    },
                    "fields": "userEnteredFormat(horizontalAlignment,verticalAlignment)",
                }
            },
            {
                "repeatCell": {
                    "range": {"sheetId": sheet_id, "startRowIndex": 4, "endRowIndex": last_data_row, "startColumnIndex": 2, "endColumnIndex": 22},
                    "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.0%"}}},
                    "fields": "userEnteredFormat.numberFormat",
                }
            },
            {
                "repeatCell": {
                    "range": {"sheetId": sheet_id, "startRowIndex": 4, "endRowIndex": last_data_row, "startColumnIndex": 22, "endColumnIndex": 24},
                    "cell": {"userEnteredFormat": {"numberFormat": {"type": "NUMBER", "pattern": "#,##0"}}},
                    "fields": "userEnteredFormat.numberFormat",
                }
            },
            {
                "repeatCell": {
                    "range": {"sheetId": sheet_id, "startRowIndex": 4, "endRowIndex": last_data_row, "startColumnIndex": 24, "endColumnIndex": 25},
                    "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.0%"}}},
                    "fields": "userEnteredFormat.numberFormat",
                }
            },
            {
                "updateBorders": {
                    "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": last_data_row, "startColumnIndex": 0, "endColumnIndex": 25},
                    "top": thin_gray,
                    "bottom": thin_gray,
                    "left": thin_gray,
                    "right": thin_gray,
                    "innerHorizontal": thin_gray,
                    "innerVertical": thin_gray,
                }
            },
            {
                "updateBorders": {
                    "range": {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": 4, "startColumnIndex": 0, "endColumnIndex": 25},
                    "top": border,
                    "bottom": border,
                    "left": border,
                    "right": border,
                    "innerHorizontal": border,
                    "innerVertical": border,
                }
            },
            {
                "updateDimensionProperties": {
                    "range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": 0, "endIndex": 1},
                    "properties": {"pixelSize": 105},
                    "fields": "pixelSize",
                }
            },
            {
                "updateDimensionProperties": {
                    "range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": 1, "endIndex": 2},
                    "properties": {"pixelSize": 145},
                    "fields": "pixelSize",
                }
            },
            {
                "updateDimensionProperties": {
                    "range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": 2, "endIndex": 25},
                    "properties": {"pixelSize": 78},
                    "fields": "pixelSize",
                }
            },
            {
                "updateDimensionProperties": {
                    "range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": 25, "endIndex": 29},
                    "properties": {"hiddenByUser": True},
                    "fields": "hiddenByUser",
                }
            },
            {
                "updateDimensionProperties": {
                    "range": {"sheetId": sheet_id, "dimension": "ROWS", "startIndex": 1, "endIndex": 4},
                    "properties": {"pixelSize": 36},
                    "fields": "pixelSize",
                }
            },
        ]
    )

    requests.append(
        {
            "addConditionalFormatRule": {
                "rule": {
                    "ranges": [
                        {"sheetId": sheet_id, "startRowIndex": 4, "endRowIndex": last_data_row, "startColumnIndex": 0, "endColumnIndex": 25}
                    ],
                    "booleanRule": {
                        "condition": {"type": "CUSTOM_FORMULA", "values": [{"userEnteredValue": '=$B5="Team Total"'}]},
                        "format": {"backgroundColor": total_fill, "textFormat": {"bold": True}},
                    },
                },
                "index": 0,
            }
        }
    )
    for col in [6, 9, 12, 15, 18, 21]:
        requests.extend(
            [
                {
                    "addConditionalFormatRule": {
                        "rule": {
                            "ranges": [
                                {"sheetId": sheet_id, "startRowIndex": 4, "endRowIndex": last_data_row, "startColumnIndex": col, "endColumnIndex": col + 1}
                            ],
                            "booleanRule": {
                                "condition": {"type": "NUMBER_GREATER", "values": [{"userEnteredValue": "0"}]},
                                "format": {"textFormat": {"foregroundColor": rgb("008000"), "bold": True}},
                            },
                        },
                        "index": 0,
                    }
                },
                {
                    "addConditionalFormatRule": {
                        "rule": {
                            "ranges": [
                                {"sheetId": sheet_id, "startRowIndex": 4, "endRowIndex": last_data_row, "startColumnIndex": col, "endColumnIndex": col + 1}
                            ],
                            "booleanRule": {
                                "condition": {"type": "NUMBER_LESS", "values": [{"userEnteredValue": "0"}]},
                                "format": {"textFormat": {"foregroundColor": rgb("CC0000"), "bold": True}},
                            },
                        },
                        "index": 0,
                    }
                },
            ]
        )
    return requests


def input_format_requests(sheet_id: int, row_count: int) -> list[dict[str, Any]]:
    last_row = max(row_count, 4)
    suggestion_fill = rgb("FFF2CC")  # 살구색: 입력이 아닌 분석 추천 셀
    requests: list[dict[str, Any]] = [
        {
            "updateSheetProperties": {
                "properties": {
                    "sheetId": sheet_id,
                    "gridProperties": {
                        "rowCount": last_row + 20,
                        "columnCount": 8,
                        "frozenRowCount": 3,
                    },
                },
                "fields": "gridProperties(rowCount,columnCount,frozenRowCount)",
            }
        },
        {
            "mergeCells": {
                "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1, "startColumnIndex": 0, "endColumnIndex": 8},
                "mergeType": "MERGE_ALL",
            }
        },
        {
            "mergeCells": {
                "range": {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": 2, "startColumnIndex": 0, "endColumnIndex": 8},
                "mergeType": "MERGE_ALL",
            }
        },
        {
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1, "startColumnIndex": 0, "endColumnIndex": 8},
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": rgb("D9EAD3"),
                        "textFormat": {"bold": True, "fontSize": 12},
                    }
                },
                "fields": "userEnteredFormat(backgroundColor,textFormat)",
            }
        },
        {
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 2, "endRowIndex": 3, "startColumnIndex": 0, "endColumnIndex": 8},
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": rgb("CFE2F3"),
                        "textFormat": {"bold": True},
                        "horizontalAlignment": "CENTER",
                        "verticalAlignment": "MIDDLE",
                        "wrapStrategy": "WRAP",
                    }
                },
                "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment,verticalAlignment,wrapStrategy)",
            }
        },
        {
            # Percent format on all 6 numeric columns (B-G).
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 3, "endRowIndex": last_row, "startColumnIndex": 1, "endColumnIndex": 7},
                "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.0%"}}},
                "fields": "userEnteredFormat.numberFormat",
            }
        },
        {
            # Highlight suggested columns (C, E, G) so users see at a glance which cells
            # are read-only analysis output vs editable input.
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 3, "endRowIndex": last_row, "startColumnIndex": 2, "endColumnIndex": 3},
                "cell": {"userEnteredFormat": {"backgroundColor": suggestion_fill}},
                "fields": "userEnteredFormat.backgroundColor",
            }
        },
        {
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 3, "endRowIndex": last_row, "startColumnIndex": 4, "endColumnIndex": 5},
                "cell": {"userEnteredFormat": {"backgroundColor": suggestion_fill}},
                "fields": "userEnteredFormat.backgroundColor",
            }
        },
        {
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 3, "endRowIndex": last_row, "startColumnIndex": 6, "endColumnIndex": 7},
                "cell": {"userEnteredFormat": {"backgroundColor": suggestion_fill}},
                "fields": "userEnteredFormat.backgroundColor",
            }
        },
        {
            # Memo column (H) wraps and top-aligns so the multi-line rationale is readable.
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 3, "endRowIndex": last_row, "startColumnIndex": 7, "endColumnIndex": 8},
                "cell": {"userEnteredFormat": {"wrapStrategy": "WRAP", "verticalAlignment": "TOP"}},
                "fields": "userEnteredFormat(wrapStrategy,verticalAlignment)",
            }
        },
        {
            "setBasicFilter": {
                "filter": {
                    "range": {"sheetId": sheet_id, "startRowIndex": 2, "endRowIndex": last_row, "startColumnIndex": 0, "endColumnIndex": 8}
                }
            }
        },
    ]
    widths = [110, 115, 115, 115, 115, 115, 115, 360]
    for idx, width in enumerate(widths):
        requests.append(
            {
                "updateDimensionProperties": {
                    "range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": idx, "endIndex": idx + 1},
                    "properties": {"pixelSize": width},
                    "fields": "pixelSize",
                }
            }
        )
    return requests


def readme_format_requests(sheet_id: int, row_count: int) -> list[dict[str, Any]]:
    return [
        {
            "updateSheetProperties": {
                "properties": {
                    "sheetId": sheet_id,
                    "gridProperties": {"rowCount": max(row_count, 4) + 10, "columnCount": 2},
                },
                "fields": "gridProperties(rowCount,columnCount)",
            }
        },
        {
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1, "startColumnIndex": 0, "endColumnIndex": 1},
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": rgb("D9EAD3"),
                        "textFormat": {"bold": True, "fontSize": 14},
                    }
                },
                "fields": "userEnteredFormat(backgroundColor,textFormat)",
            }
        },
        {
            "updateDimensionProperties": {
                "range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": 0, "endIndex": 1},
                "properties": {"pixelSize": 760},
                "fields": "pixelSize",
            }
        },
    ]


def existing_sheet_format_requests(requests: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return format requests that are safe to reapply on an existing sheet.

    In-place runs keep the current header merges, but data row counts can grow.
    Reapply number formats, borders, dimensions, and conditional formatting so
    newly appended rows do not inherit Google Sheets' default "automatic" format.
    """
    return [request for request in requests if "mergeCells" not in request]


def delete_conditional_format_requests(
    service: Any,
    spreadsheet_id: str,
    sheet_ids: set[int],
) -> list[dict[str, Any]]:
    if not sheet_ids:
        return []
    meta = (
        service.spreadsheets()
        .get(spreadsheetId=spreadsheet_id, fields="sheets(properties(sheetId),conditionalFormats)")
        .execute()
    )
    requests: list[dict[str, Any]] = []
    for sheet in meta.get("sheets", []):
        sheet_id = sheet.get("properties", {}).get("sheetId")
        if sheet_id not in sheet_ids:
            continue
        rule_count = len(sheet.get("conditionalFormats", []))
        for idx in range(rule_count - 1, -1, -1):
            requests.append({"deleteConditionalFormatRule": {"sheetId": sheet_id, "index": idx}})
    return requests


def _with_backoff(call) -> Any:
    import time
    attempt = 0
    while True:
        try:
            return call.execute()
        except Exception as exc:  # noqa: BLE001
            if "429" in str(exc) and attempt < 6:
                time.sleep(2 ** attempt * 5)
                attempt += 1
                continue
            raise


def batch_write_values(
    service: Any,
    spreadsheet_id: str,
    payloads: list[tuple[str, list[list[Any]]]],
) -> None:
    """Bulk-write multiple tabs in a single batchUpdate to stay under the 60/min cap."""
    data = []
    for sheet, values in payloads:
        if not values:
            continue
        end_col = col_to_a1(max(len(row) for row in values) - 1)
        data.append(
            {
                "range": f"{q(sheet)}!A1:{end_col}{len(values)}",
                "values": values,
            }
        )
    if not data:
        return
    # Sheets API caps batchUpdate at ~10 MB; split into chunks of 20 tabs to be safe.
    for start in range(0, len(data), 20):
        chunk = data[start : start + 20]
        call = service.spreadsheets().values().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"valueInputOption": "USER_ENTERED", "data": chunk},
        )
        _with_backoff(call)


def clear_tab_values(service: Any, spreadsheet_id: str, sheets: list[str]) -> None:
    """Wipe all cell values on the given tabs in one batched request (rate-limit friendly).

    Only clears values — number formats, borders, merges, conditional formatting all stay.
    """
    if not sheets:
        return
    ranges = [f"{q(s)}!A1:AC2000" for s in sheets]
    call = service.spreadsheets().values().batchClear(
        spreadsheetId=spreadsheet_id,
        body={"ranges": ranges},
    )
    try:
        _with_backoff(call)
    except Exception:
        pass


def create_spreadsheet(service: Any, title: str, sheet_titles: list[str]) -> tuple[str, dict[str, int]]:
    # Provision each sheet generously so subsequent merges and value writes never hit
    # the implicit 26-column default created by Sheets API when gridProperties is absent.
    sheets_body = []
    for idx, name in enumerate(sheet_titles):
        sheets_body.append(
            {
                "properties": {
                    "title": name,
                    "index": idx,
                    "gridProperties": {
                        "rowCount": 1000,
                        "columnCount": 30,
                    },
                }
            }
        )
    body = {
        "properties": {"title": title, "locale": "ko_KR", "timeZone": "Asia/Seoul"},
        "sheets": sheets_body,
    }
    created = service.spreadsheets().create(body=body, fields="spreadsheetId,sheets(properties(title,sheetId,gridProperties))").execute()
    sid = created["spreadsheetId"]
    sheet_ids = {
        s["properties"]["title"]: s["properties"]["sheetId"]
        for s in created.get("sheets", [])
    }
    return sid, sheet_ids


def batch_apply(service: Any, spreadsheet_id: str, requests: list[dict[str, Any]]) -> None:
    """Apply requests with rate-limit backoff. Resize first, then merges/formats."""
    if not requests:
        return
    import time

    resize_phase = [r for r in requests if "updateSheetProperties" in r]
    other_phase = [r for r in requests if "updateSheetProperties" not in r]

    def send(chunk: list[dict[str, Any]]) -> None:
        attempt = 0
        while True:
            try:
                service.spreadsheets().batchUpdate(
                    spreadsheetId=spreadsheet_id,
                    body={"requests": chunk},
                ).execute()
                return
            except Exception as exc:  # noqa: BLE001
                if "429" in str(exc) and attempt < 6:
                    time.sleep(2 ** attempt * 5)
                    attempt += 1
                    continue
                raise

    chunk_size = 80
    for phase in (resize_phase, other_phase):
        for start in range(0, len(phase), chunk_size):
            send(phase[start : start + chunk_size])
            time.sleep(1.1)


def trash_previous(creds: Credentials, spreadsheet_id: str | None) -> None:
    if not spreadsheet_id:
        return
    try:
        drive = build("drive", "v3", credentials=creds)
        drive.files().update(fileId=spreadsheet_id, body={"trashed": True}).execute()
    except Exception:
        pass


def list_existing_origin_tabs(service: Any, spreadsheet_id: str) -> tuple[list[str], dict[str, int]]:
    """Return (origin tab titles in current tab-bar order, sheet_id_by_title)."""
    meta = (
        service.spreadsheets()
        .get(spreadsheetId=spreadsheet_id, fields="sheets(properties(sheetId,title,index))")
        .execute()
    )
    sheets = sorted(
        meta.get("sheets", []),
        key=lambda s: s["properties"].get("index", 0),
    )
    ordered_origins: list[str] = []
    sheet_ids: dict[str, int] = {}
    for s in sheets:
        title = s["properties"]["title"]
        sheet_ids[title] = s["properties"]["sheetId"]
        if title in SUPPORT_TABS or title in {README_SHEET, INPUT_SHEET, SUMMARY_SHEET, OWNER_INPUT_SHEET}:
            continue
        ordered_origins.append(title)
    return ordered_origins, sheet_ids


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--inplace",
        action="store_true",
        help="Update the existing spreadsheet from state instead of creating a new one.",
    )
    parser.add_argument(
        "--reset-input",
        action="store_true",
        help="In --inplace mode, reset Target_Input increases to DEFAULT_INCREASE for every origin.",
    )
    args = parser.parse_args()

    creds = get_creds()
    service = build("sheets", "v4", credentials=creds)

    report_tabs = get_report_tab_values(service)
    if not report_tabs:
        raise RuntimeError("No report tabs found in source spreadsheet")
    account_counts = build_account_counts()

    existing_state: dict[str, Any] = {}
    if STATE_PATH.exists():
        try:
            existing_state = json.loads(STATE_PATH.read_text(encoding="utf-8"))
        except Exception:
            existing_state = {}
    existing_id: str | None = existing_state.get("spreadsheet_id")
    existing_input: dict[str, tuple[float, float, float, str]] = {}
    if existing_id:
        existing_input = read_existing_input(service, existing_id)
    if not existing_input:
        existing_input = read_existing_input(service, SOURCE_SPREADSHEET_ID)
    owner_orders: dict[str, list[str]] = read_sales_owner_input(service, existing_id) if existing_id else {}

    valid_origins = {
        origin
        for origin in report_tabs
        if {"lst", "bsa", "w3", "w3_norm_lst", "hi_w3"}.issubset(parse_report_tab(report_tabs[origin]))
    }

    if args.inplace:
        if not existing_id:
            raise RuntimeError("--inplace requires a prior run state at " + str(STATE_PATH))
        spreadsheet_id = existing_id
        # Use the current spreadsheet's tab order so Summary_All matches what the user sees.
        ordered_origins, sheet_ids = list_existing_origin_tabs(service, spreadsheet_id)
        # Keep only origins that still have a tab AND have valid report data.
        origins = [o for o in ordered_origins if o in valid_origins]
        # Reset inputs to default if requested (e.g. when changing the default to 10%).
        if args.reset_input:
            existing_input = {
                o: (DEFAULT_INCREASE, DEFAULT_INCREASE, DEFAULT_INCREASE, existing_input.get(o, (0, 0, 0, ""))[3])
                for o in origins
            }
    else:
        origins = sorted(valid_origins)
        sheet_titles = [README_SHEET, INPUT_SHEET, SUMMARY_SHEET, *origins]
        trash_previous(creds, existing_id)
        spreadsheet_id, sheet_ids = create_spreadsheet(service, OUTPUT_TITLE, sheet_titles)
        STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
        STATE_PATH.write_text(
            json.dumps({"spreadsheet_id": spreadsheet_id, "url": f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit"}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    # In-place rewrites can produce fewer rows than the previous run; wipe cell values
    # first so stale rows (e.g. salespersons filtered for having no Q1 customers) disappear.
    if args.inplace:
        clear_tab_values(service, spreadsheet_id, [README_SHEET, INPUT_SHEET, SUMMARY_SHEET, *origins])

    # Compose all writes, then ship them in a single batchUpdate to avoid 429s.
    summary_values = build_summary_values(
        report_tabs,
        account_counts,
        ordered_origins=origins,
        owner_orders=owner_orders,
    )
    suggestions = compute_suggestions(report_tabs, account_counts, origins, owner_orders)
    payloads: list[tuple[str, list[list[Any]]]] = [
        (README_SHEET, build_readme_values()),
        (INPUT_SHEET, build_input_values(origins, existing_input, suggestions)),
        (SUMMARY_SHEET, summary_values),
    ]
    per_origin_rowcounts: dict[str, int] = {}
    for origin in origins:
        blocks = parse_report_tab(report_tabs[origin])
        rows = collect_rows_for_origin(origin, blocks, account_counts, owner_orders.get(origin))
        values = build_origin_sheet_values(origin, rows, account_counts)
        payloads.append((origin, values))
        per_origin_rowcounts[origin] = len(values)
    batch_write_values(service, spreadsheet_id, payloads)

    # Format pass only when creating a fresh workbook; in-place leaves existing formats untouched.
    if not args.inplace:
        requests: list[dict[str, Any]] = []
        requests.extend(readme_format_requests(sheet_ids[README_SHEET], len(build_readme_values())))
        requests.extend(input_format_requests(sheet_ids[INPUT_SHEET], len(origins) + 3))
        requests.extend(summary_format_requests(sheet_ids[SUMMARY_SHEET], len(summary_values)))
        for origin in origins:
            requests.extend(origin_format_requests(sheet_ids[origin], per_origin_rowcounts[origin]))
        batch_apply(service, spreadsheet_id, requests)
    else:
        # In-place runs keep existing header merges, but row counts can grow when
        # stale Sales_Owner_Input entries are supplemented by data-derived owners.
        # Refresh visible data formats and conditional-format ranges for every
        # target sheet so newly appended rows render as percentages/counts.
        target_sheet_ids = {sheet_ids[SUMMARY_SHEET], *[sheet_ids[origin] for origin in origins]}
        format_reqs: list[dict[str, Any]] = []
        format_reqs.extend(delete_conditional_format_requests(service, spreadsheet_id, target_sheet_ids))
        format_reqs.extend(existing_sheet_format_requests(summary_format_requests(sheet_ids[SUMMARY_SHEET], len(summary_values))))
        for origin in origins:
            format_reqs.extend(existing_sheet_format_requests(origin_format_requests(sheet_ids[origin], per_origin_rowcounts[origin])))
        batch_apply(service, spreadsheet_id, format_reqs)

        # In-place runs reassert Target_Input formats. The new layout is wider (8 cols, with
        # suggestion columns C/E/G interleaved), so we also resize the grid and reapply the
        # suggestion-highlight + column widths. We avoid touching merges/filter to keep the
        # user's existing visual state intact.
        input_last_row = len(origins) + 3
        suggestion_fill = rgb("FFF2CC")
        reqs: list[dict[str, Any]] = [
            {
                "updateSheetProperties": {
                    "properties": {
                        "sheetId": sheet_ids[INPUT_SHEET],
                        "gridProperties": {"columnCount": 8, "frozenRowCount": 3},
                    },
                    "fields": "gridProperties(columnCount,frozenRowCount)",
                }
            },
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_ids[INPUT_SHEET],
                        "startRowIndex": 3,
                        "endRowIndex": input_last_row,
                        "startColumnIndex": 1,
                        "endColumnIndex": 7,
                    },
                    "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.0%"}}},
                    "fields": "userEnteredFormat.numberFormat",
                }
            },
        ]
        for col_start in (2, 4, 6):
            reqs.append(
                {
                    "repeatCell": {
                        "range": {
                            "sheetId": sheet_ids[INPUT_SHEET],
                            "startRowIndex": 3,
                            "endRowIndex": input_last_row,
                            "startColumnIndex": col_start,
                            "endColumnIndex": col_start + 1,
                        },
                        "cell": {"userEnteredFormat": {"backgroundColor": suggestion_fill}},
                        "fields": "userEnteredFormat.backgroundColor",
                    }
                }
            )
        # Memo wrap + top-align so multi-line rationale text is readable in place.
        reqs.append(
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_ids[INPUT_SHEET],
                        "startRowIndex": 3,
                        "endRowIndex": input_last_row,
                        "startColumnIndex": 7,
                        "endColumnIndex": 8,
                    },
                    "cell": {"userEnteredFormat": {"wrapStrategy": "WRAP", "verticalAlignment": "TOP"}},
                    "fields": "userEnteredFormat(wrapStrategy,verticalAlignment)",
                }
            }
        )
        widths = [110, 115, 115, 115, 115, 115, 115, 360]
        for idx, width in enumerate(widths):
            reqs.append(
                {
                    "updateDimensionProperties": {
                        "range": {"sheetId": sheet_ids[INPUT_SHEET], "dimension": "COLUMNS", "startIndex": idx, "endIndex": idx + 1},
                        "properties": {"pixelSize": width},
                        "fields": "pixelSize",
                    }
                }
            )
        batch_apply(service, spreadsheet_id, reqs)

    url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit"
    report = {
        "spreadsheet_id": spreadsheet_id,
        "url": url,
        "title": OUTPUT_TITLE,
        "origin_tabs": len(origins),
        "summary_rows": len(summary_values) - 4,
        "account_counts_loaded": bool(account_counts),
        "mode": "inplace" if args.inplace else "fresh",
    }
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
