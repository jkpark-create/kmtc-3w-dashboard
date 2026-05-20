from __future__ import annotations

import importlib.util
import json
import math
from datetime import datetime
from pathlib import Path
from typing import Iterable

import pandas as pd
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build


ROOT = Path(__file__).resolve().parents[1]
SPREADSHEET_ID = "19d_lnB7Qt6H-UJE7ECyDo-i385tjMjAX8aATk0R0N54"
SUPPORT_TABS = ["BSA_Backdata", "2025_Basis", "Validation"]
NO_BASIS_LABEL = "(no 2025 basis)"
NO_BASIS_LEVEL = "no 2025 basis"
NO_BASIS_NOTE = (
    "2025년 같은 POR_port + DLY_port route에 Normal LST_TEU 배분 기준이 없어 "
    "해당 route BSA를 담당자별로 나누지 못하고 예외로 남긴 값입니다."
)


def rgb(hex_color: str) -> dict[str, float]:
    clean = hex_color.strip().lstrip("#")
    return {
        "red": int(clean[0:2], 16) / 255,
        "green": int(clean[2:4], 16) / 255,
        "blue": int(clean[4:6], 16) / 255,
    }


QUARTER_HEADER_COLOR = rgb("C6E0B4")
QUARTER_VALUE_COLOR = rgb("E2F0D9")
YEAR_HEADER_COLOR = rgb("F4B183")
YEAR_VALUE_COLOR = rgb("FCE4D6")
NO_BASIS_HEADER_COLOR = rgb("F4B183")
NO_BASIS_VALUE_COLOR = rgb("FCE4D6")


def period_kind(period_key: str) -> str:
    if period_key.endswith("_Total"):
        return "year"
    if "_Q" in period_key:
        return "quarter"
    return "month"


def load_builder():
    path = ROOT / "scripts" / "build_salesperson_bsa_action_sheet.py"
    spec = importlib.util.spec_from_file_location("salesperson_bsa_builder", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


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


def clean_value(value: object) -> object:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except TypeError:
        pass
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return ""
        return int(value) if value.is_integer() else value
    return value


def q(title: str) -> str:
    return "'" + title.replace("'", "''") + "'"


def col_to_a1(idx: int) -> str:
    idx += 1
    out = ""
    while idx:
        idx, rem = divmod(idx - 1, 26)
        out = chr(65 + rem) + out
    return out


def dataframe_values(df: pd.DataFrame) -> list[list[object]]:
    values = [list(df.columns)]
    for row in df.itertuples(index=False, name=None):
        values.append([clean_value(v) for v in row])
    return values


def chunked(values: list[list[object]], size: int = 2000) -> Iterable[tuple[int, list[list[object]]]]:
    for start in range(0, len(values), size):
        yield start, values[start : start + size]


def build_report_values(mod, tab: str, rows: pd.DataFrame, metrics: dict[str, object]) -> tuple[list[list[object]], list[dict[str, object]]]:
    table_specs = [
        ("1. 전체 실적 LST_TEU (Normal 기준)", "lst", False),
        ("2. 2025 실적비중 기준 배분 BSA", "bsa", False),
        ("3. 3주전 부킹 FST_TEU (대시보드 WOS-3 기준)", "w3", False),
        ("4. 3주전 부킹 실선적 LST_TEU (Normal 기준)", "w3_norm_lst", False),
        ("5. 3주전 부킹 실선적률", "w3_ship_rate", True),
        ("6. 3주전 부킹 / BSA", "w3_bsa", True),
        ("7. 3주전 고수익 부킹 FST_TEU", "hi_w3", False),
        ("8. 3주전 고수익 부킹 비중", "hi_share", True),
    ]
    width = 2 + len(mod.PERIODS)
    values: list[list[object]] = []
    blocks: list[dict[str, object]] = []
    headers = ["Team", "Salesman"] + [label for _, label, _ in mod.PERIODS]

    for title, key, is_ratio in table_specs:
        start_row = len(values)
        source = mod.restrict_source_to_rows(metrics[key], rows)
        if is_ratio:
            numerator, denominator = source
            table = mod.pivot_ratio(numerator, denominator, tab, rows)
            table = mod.add_total_row(table, (numerator, denominator), tab, ratio=True)
        else:
            table = mod.pivot_sum(source, tab, rows)
            table = mod.add_total_row(table, source, tab, ratio=False)

        values.append([title] + [""] * (width - 1))
        values.append(headers)
        for row in table.itertuples(index=False, name=None):
            values.append([clean_value(v) for v in row])
        values.append([""] * width)
        blocks.append(
            {
                "title_row": start_row,
                "header_row": start_row + 1,
                "total_row": start_row + 2,
                "data_start": start_row + 2,
                "data_end": start_row + 2 + len(table),
                "is_ratio": is_ratio,
            }
        )
    return values, blocks


def build_index_values(validation: pd.DataFrame, allocation: pd.DataFrame, tab_names: list[str], gids: dict[str, int]) -> list[list[object]]:
    summary = (
        validation.loc[validation["tab"].isin(tab_names)]
        .groupby("tab", dropna=False)
        .agg(
            Source_Route_BSA=("route_bsa", "sum"),
            Allocated_BSA=("allocated_bsa", "sum"),
            BSA_Diff=("bsa_diff", "sum"),
            LST_TEU=("lst", "sum"),
            WOS3_FST_TEU=("w3", "sum"),
            WOS3_LST_TEU=("w3_norm_lst", "sum"),
            High_Profit_WOS3_FST_TEU=("hi_w3", "sum"),
        )
        .reset_index()
        .rename(columns={"tab": "Tab"})
    )
    no_basis = (
        allocation.loc[allocation["allocation_level"].eq(NO_BASIS_LEVEL)]
        .groupby("tab", dropna=False)
        .agg(No_Basis_Rows=("allocated_bsa", "size"), No_Basis_BSA=("allocated_bsa", "sum"))
        .reset_index()
        .rename(columns={"tab": "Tab"})
    )
    summary = summary.merge(no_basis, on="Tab", how="left")
    summary[["No_Basis_Rows", "No_Basis_BSA"]] = summary[["No_Basis_Rows", "No_Basis_BSA"]].fillna(0)
    summary["WOS3/BSA"] = summary["WOS3_FST_TEU"] / summary["Allocated_BSA"].replace(0, pd.NA)
    summary["WOS3_실선적률"] = summary["WOS3_LST_TEU"] / summary["WOS3_FST_TEU"].replace(0, pd.NA)
    summary["고수익_WOS3_비중"] = summary["High_Profit_WOS3_FST_TEU"] / summary["WOS3_FST_TEU"].replace(0, pd.NA)
    summary = summary[
        [
            "Tab",
            "Source_Route_BSA",
            "Allocated_BSA",
            "BSA_Diff",
            "LST_TEU",
            "WOS3_FST_TEU",
            "WOS3/BSA",
            "WOS3_LST_TEU",
            "WOS3_실선적률",
            "High_Profit_WOS3_FST_TEU",
            "고수익_WOS3_비중",
            "No_Basis_Rows",
            "No_Basis_BSA",
        ]
    ].sort_values(["Source_Route_BSA", "LST_TEU", "Tab"], ascending=[False, False, True])
    summary = summary.rename(columns={"High_Profit_WOS3_FST_TEU": "고수익_WOS3_FST_TEU"})

    values: list[list[object]] = [
        ["3W Booking Action BSA Salesperson Index"],
        [f"Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"],
        list(summary.columns),
    ]
    for row in summary.itertuples(index=False, name=None):
        row_values = [clean_value(v) for v in row]
        tab = str(row_values[0])
        if tab in gids:
            label = tab.replace('"', '""')
            row_values[0] = f'=HYPERLINK("#gid={gids[tab]}","{label}")'
        values.append(row_values)
    return values


def build_readme_values(tab_count: int, dataset_id: str) -> list[list[object]]:
    return [
        ["3W Booking Action BSA Salesperson Sheet"],
        [],
        ["Workbook", "3W Booking Action BSA Salesperson OBT 2025-2026Q1"],
        ["Updated at", datetime.now().strftime("%Y-%m-%d %H:%M:%S")],
        ["Source booking cache", f"_cache_2025.parquet + _cache_{dataset_id}.parquet"],
        ["Source BSA", f"BSA_raw_monthly3W_{dataset_id}.csv"],
        ["Team filter", "OBT only"],
        ["Report months", "2025-01 to 2026-03"],
        ["Report tabs", tab_count],
        ["Tab rule", "CN by POR_port with SHK+DCB combined; VN as SGN+CMP and HPH; ID as JKT/SUB/ID_out; MY as PKG+PKW/PEN/PGU"],
        ["Route basis", "Default route is POR_port + DLY_port"],
        ["BSA allocation", "Monthly route BSA allocated only by exact POR_port + DLY_port 2025 Normal LST_TEU salesperson share"],
        [
            "No-basis handling",
            "No CN/origin-wide fallback; routes without exact 2025 basis stay in (no 2025 basis)",
        ],
        ["3W booking", "Lead_time (BKG_Sche)=WOS-3, FST_TEU"],
        ["3W shipment", "Lead_time (BKG_Sche)=WOS-3 and LST_Status=Normal, LST_TEU"],
        ["High-profit 3W booking", "Dashboard profit type 고/저=고수익 within WOS-3 FST_TEU"],
        ["Ratio totals", "Quarter/year ratios use summed numerator divided by summed denominator"],
    ]


def execute_with_backoff(call, max_attempts: int = 7):
    import time

    for attempt in range(max_attempts):
        try:
            return call.execute()
        except Exception as exc:  # noqa: BLE001
            if "429" not in str(exc) or attempt == max_attempts - 1:
                raise
            time.sleep((2 ** attempt) * 5)


def ensure_sheets(service, required_titles: list[str]) -> dict[str, int]:
    meta = service.spreadsheets().get(
        spreadsheetId=SPREADSHEET_ID,
        fields="sheets(properties(sheetId,title,index,gridProperties))",
    ).execute()
    existing = {s["properties"]["title"]: s["properties"]["sheetId"] for s in meta.get("sheets", [])}
    requests = []
    for title, sheet_id in existing.items():
        if title not in required_titles:
            requests.append({"deleteSheet": {"sheetId": sheet_id}})
    for title in required_titles:
        if title not in existing:
            requests.append({"addSheet": {"properties": {"title": title}}})
    if requests:
        execute_with_backoff(
            service.spreadsheets().batchUpdate(spreadsheetId=SPREADSHEET_ID, body={"requests": requests})
        )
    meta = service.spreadsheets().get(
        spreadsheetId=SPREADSHEET_ID,
        fields="sheets(properties(sheetId,title,index,gridProperties))",
    ).execute()
    return {s["properties"]["title"]: s["properties"]["sheetId"] for s in meta.get("sheets", [])}


def batch_update_requests(service, requests: list[dict[str, object]], chunk_size: int = 400) -> None:
    for start in range(0, len(requests), chunk_size):
        execute_with_backoff(
            service.spreadsheets().batchUpdate(
                spreadsheetId=SPREADSHEET_ID,
                body={"requests": requests[start : start + chunk_size]},
            )
        )


def format_report_sheet_requests(
    sheet_id: int,
    blocks: list[dict[str, object]],
    row_count: int,
    col_count: int,
    periods: list[tuple[str, str, list[str]]] | None = None,
) -> list[dict[str, object]]:
    dark_blue = {"red": 0.12, "green": 0.31, "blue": 0.47}
    light_blue = {"red": 0.85, "green": 0.92, "blue": 0.97}
    pale_yellow = {"red": 1.0, "green": 0.95, "blue": 0.8}
    border_gray = rgb("A6A6A6")
    highlighted_periods = [
        (col_idx, kind)
        for col_idx, (key, _, _) in enumerate(periods or [], start=2)
        if (kind := period_kind(key)) != "month"
    ]
    grid_rows = max(row_count + 20, 200)
    requests: list[dict[str, object]] = [
        {
            "updateSheetProperties": {
                "properties": {
                    "sheetId": sheet_id,
                    "gridProperties": {
                        "rowCount": grid_rows,
                        "columnCount": col_count,
                        "frozenRowCount": 2,
                        "frozenColumnCount": 0,
                    },
                },
                "fields": "gridProperties(rowCount,columnCount,frozenRowCount,frozenColumnCount)",
            }
        },
        {
            "updateDimensionProperties": {
                "range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": 0, "endIndex": 1},
                "properties": {"pixelSize": 70},
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
                "range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": 2, "endIndex": col_count},
                "properties": {"pixelSize": 84},
                "fields": "pixelSize",
            }
        },
        {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 0,
                    "endRowIndex": grid_rows,
                    "startColumnIndex": 0,
                    "endColumnIndex": col_count,
                },
                "cell": {"userEnteredFormat": {}},
                "fields": "userEnteredFormat",
            }
        },
    ]
    for block in blocks:
        title_row = int(block["title_row"])
        header_row = int(block["header_row"])
        total_row = int(block["total_row"])
        data_start = int(block["data_start"])
        data_end = int(block["data_end"])
        is_ratio = bool(block["is_ratio"])
        requests.extend(
            [
                {
                    "mergeCells": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": title_row,
                            "endRowIndex": title_row + 1,
                            "startColumnIndex": 0,
                            "endColumnIndex": col_count,
                        },
                        "mergeType": "MERGE_ALL",
                    }
                },
                {
                    "repeatCell": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": title_row,
                            "endRowIndex": title_row + 1,
                            "startColumnIndex": 0,
                            "endColumnIndex": col_count,
                        },
                        "cell": {
                            "userEnteredFormat": {
                                "backgroundColor": dark_blue,
                                "textFormat": {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
                                "horizontalAlignment": "LEFT",
                            }
                        },
                        "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)",
                    }
                },
                {
                    "repeatCell": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": header_row,
                            "endRowIndex": header_row + 1,
                            "startColumnIndex": 0,
                            "endColumnIndex": col_count,
                        },
                        "cell": {
                            "userEnteredFormat": {
                                "backgroundColor": light_blue,
                                "textFormat": {"bold": True},
                                "horizontalAlignment": "CENTER",
                            }
                        },
                        "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)",
                    }
                },
                {
                    "repeatCell": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": total_row,
                            "endRowIndex": total_row + 1,
                            "startColumnIndex": 0,
                            "endColumnIndex": col_count,
                        },
                        "cell": {
                            "userEnteredFormat": {
                                "backgroundColor": pale_yellow,
                                "textFormat": {"bold": True},
                            }
                        },
                        "fields": "userEnteredFormat(backgroundColor,textFormat)",
                    }
                },
                {
                    "repeatCell": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": data_start,
                            "endRowIndex": data_end,
                            "startColumnIndex": 2,
                            "endColumnIndex": col_count,
                        },
                        "cell": {
                            "userEnteredFormat": {
                                "numberFormat": {
                                    "type": "PERCENT" if is_ratio else "NUMBER",
                                    "pattern": "#,##0%" if is_ratio else "#,##0",
                                },
                                "horizontalAlignment": "RIGHT",
                            }
                        },
                        "fields": "userEnteredFormat(numberFormat,horizontalAlignment)",
                    }
                },
            ]
        )
        requests.append(
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": header_row,
                        "endRowIndex": header_row + 1,
                        "startColumnIndex": 1,
                        "endColumnIndex": 2,
                    },
                    "cell": {"note": NO_BASIS_NOTE},
                    "fields": "note",
                }
            }
        )
        for col_idx, kind in highlighted_periods:
            header_color = QUARTER_HEADER_COLOR if kind == "quarter" else YEAR_HEADER_COLOR
            value_color = QUARTER_VALUE_COLOR if kind == "quarter" else YEAR_VALUE_COLOR
            requests.extend(
                [
                    {
                        "repeatCell": {
                            "range": {
                                "sheetId": sheet_id,
                                "startRowIndex": header_row,
                                "endRowIndex": header_row + 1,
                                "startColumnIndex": col_idx,
                                "endColumnIndex": col_idx + 1,
                            },
                            "cell": {
                                "userEnteredFormat": {
                                    "backgroundColor": header_color,
                                    "textFormat": {"bold": True},
                                    "horizontalAlignment": "CENTER",
                                }
                            },
                            "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)",
                        }
                    },
                    {
                        "repeatCell": {
                            "range": {
                                "sheetId": sheet_id,
                                "startRowIndex": data_start,
                                "endRowIndex": data_end,
                                "startColumnIndex": col_idx,
                                "endColumnIndex": col_idx + 1,
                            },
                            "cell": {"userEnteredFormat": {"backgroundColor": value_color}},
                            "fields": "userEnteredFormat.backgroundColor",
                        }
                    },
                    {
                        "updateBorders": {
                            "range": {
                                "sheetId": sheet_id,
                                "startRowIndex": header_row,
                                "endRowIndex": data_end,
                                "startColumnIndex": col_idx,
                                "endColumnIndex": col_idx + 1,
                            },
                            "left": {"style": "SOLID", "color": border_gray},
                            "right": {"style": "SOLID", "color": border_gray},
                        }
                    },
                ]
            )
    return requests


def format_support_requests(sheet_id: int, df: pd.DataFrame, row_count: int) -> list[dict[str, object]]:
    col_count = len(df.columns)
    grid_rows = max(row_count + 20, 100)
    requests: list[dict[str, object]] = [
        {
            "updateSheetProperties": {
                "properties": {
                    "sheetId": sheet_id,
                    "gridProperties": {
                        "rowCount": grid_rows,
                        "columnCount": max(col_count, 1),
                        "frozenRowCount": 1,
                    },
                },
                "fields": "gridProperties(rowCount,columnCount,frozenRowCount)",
            }
        },
        {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 0,
                    "endRowIndex": grid_rows,
                    "startColumnIndex": 0,
                    "endColumnIndex": max(col_count, 1),
                },
                "cell": {"userEnteredFormat": {}},
                "fields": "userEnteredFormat",
            }
        },
        {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 0,
                    "endRowIndex": 1,
                    "startColumnIndex": 0,
                    "endColumnIndex": col_count,
                },
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": {"red": 0.85, "green": 0.92, "blue": 0.97},
                        "textFormat": {"bold": True},
                        "horizontalAlignment": "CENTER",
                    }
                },
                "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)",
            }
        },
    ]
    for idx, name in enumerate(df.columns):
        if name in {"Salesman", "Allocation_Level"}:
            fmt = {
                "backgroundColor": {"red": 0.85, "green": 0.92, "blue": 0.97},
                "textFormat": {"bold": True},
                "horizontalAlignment": "CENTER",
            }
            if name == "Allocation_Level":
                fmt["backgroundColor"] = NO_BASIS_HEADER_COLOR
            requests.append(
                {
                    "repeatCell": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": 0,
                            "endRowIndex": 1,
                            "startColumnIndex": idx,
                            "endColumnIndex": idx + 1,
                        },
                        "cell": {"note": NO_BASIS_NOTE, "userEnteredFormat": fmt},
                        "fields": "note,userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)",
                    }
                }
            )
        if name == "Allocation_Level":
            requests.append(
                {
                    "repeatCell": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": 1,
                            "endRowIndex": max(row_count, 2),
                            "startColumnIndex": idx,
                            "endColumnIndex": idx + 1,
                        },
                        "cell": {"userEnteredFormat": {"backgroundColor": NO_BASIS_VALUE_COLOR}},
                        "fields": "userEnteredFormat.backgroundColor",
                    }
                }
            )
    for idx, name in enumerate(df.columns):
        if any(token in name for token in ["Share", "Rate", "BSA", "비중", "실선적률"]) and name not in {
            "Route_BSA",
            "Allocated_BSA",
            "Source_Route_BSA",
            "BSA_Diff",
            "No_Basis_BSA",
        }:
            pattern = "#,##0%"
            fmt_type = "PERCENT"
        elif pd.api.types.is_numeric_dtype(df[name]):
            pattern = "#,##0"
            fmt_type = "NUMBER"
        else:
            continue
        requests.append(
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 1,
                        "endRowIndex": max(row_count, 2),
                        "startColumnIndex": idx,
                        "endColumnIndex": idx + 1,
                    },
                    "cell": {"userEnteredFormat": {"numberFormat": {"type": fmt_type, "pattern": pattern}}},
                    "fields": "userEnteredFormat.numberFormat",
                }
            }
        )
    return requests


def main() -> None:
    mod = load_builder()
    booking = mod.load_booking()
    bsa = mod.load_bsa()
    basis_detail, lookups = mod.build_basis(booking)
    allocation = mod.allocate_bsa(bsa, lookups)

    lst = mod.metric_sum(booking, booking["status"].eq("Normal") & booking["lst"].gt(0), "lst")
    w3_mask = booking["lead_time"].eq("WOS-3") & booking["fst"].gt(0)
    w3 = mod.metric_sum(booking, w3_mask, "fst")
    w3_norm_source = booking.assign(_w3_norm_lst=booking["lst"].where(w3_mask & booking["status"].eq("Normal"), 0.0))
    w3_norm_lst = mod.metric_sum(w3_norm_source, w3_norm_source["_w3_norm_lst"].gt(0), "_w3_norm_lst")
    w3_high_source = booking.assign(
        _hi_w3=booking["fst"].where(w3_mask & booking["profit_type"].eq("고수익"), 0.0)
    )
    hi_w3 = mod.metric_sum(w3_high_source, w3_high_source["_hi_w3"].gt(0), "_hi_w3")
    bsa_sales = mod.sales_metric_from_allocation(allocation)

    tab_names = sorted(set(bsa["tab"].unique()) | set(lst["tab"].unique()) | set(w3["tab"].unique()))
    bsa_tabs = set(bsa["tab"].unique())
    tab_names = [tab for tab in tab_names if tab in bsa_tabs]
    metrics: dict[str, object] = {
        "lst": lst,
        "bsa": bsa_sales,
        "w3": w3,
        "w3_norm_lst": w3_norm_lst,
        "w3_ship_rate": (w3_norm_lst, w3),
        "w3_bsa": (w3, bsa_sales),
        "hi_w3": hi_w3,
        "hi_share": (hi_w3, w3),
    }
    validation = mod.build_validation(bsa, allocation, lst, w3, w3_norm_lst, hi_w3)
    validation = validation.loc[validation["tab"].isin(tab_names)].copy()

    report_values: dict[str, list[list[object]]] = {}
    report_blocks: dict[str, list[dict[str, object]]] = {}
    for tab in tab_names:
        rows = mod.build_rows_for_tab(tab, [lst, bsa_sales, w3, w3_norm_lst, hi_w3], lst)
        values, blocks = build_report_values(mod, tab, rows, metrics)
        report_values[tab] = values
        report_blocks[tab] = blocks

    allocation_out = allocation.rename(
        columns={
            "yyyymm": "YYYYMM",
            "tab": "Tab",
            "team": "Team",
            "origin": "POR_Country",
            "ori_port": "POR_Port",
            "dest": "DLY_Country",
            "dst_port": "DLY_Port",
            "sales": "Salesman",
            "basis_lST_TEU": "2025_Basis_LST_TEU",
            "basis_total_lST_TEU": "2025_Basis_Total_LST_TEU",
            "allocation_share": "Allocation_Share",
            "route_bsa": "Route_BSA",
            "allocated_bsa": "Allocated_BSA",
            "allocation_level": "Allocation_Level",
            "allocation_key": "Allocation_Key",
        }
    )
    basis_out = basis_detail.rename(
        columns={
            "tab": "Tab",
            "team": "Team",
            "origin": "POR_Country",
            "ori_port": "POR_Port",
            "dest": "DLY_Country",
            "dst_port": "DLY_Port",
            "shipper_code": "Shipper_Code",
            "shipper_name": "Shipper_Name",
            "sales": "Salesman",
        }
    )
    validation_out = validation.rename(
        columns={
            "tab": "Tab",
            "yyyymm": "YYYYMM",
            "route_bsa": "Source_Route_BSA",
            "allocated_bsa": "Allocated_BSA",
            "bsa_diff": "BSA_Diff",
            "lst": "LST_TEU",
            "w3": "WOS3_FST_TEU",
            "w3_norm_lst": "WOS3_LST_TEU",
            "hi_w3": "High_Profit_WOS3_FST_TEU",
            "w3_bsa": "WOS3_BSA",
            "w3_ship_rate": "WOS3_Shipment_Rate",
            "hi_w3_share": "High_Profit_WOS3_Share",
        }
    )
    support_frames = {
        "BSA_Backdata": allocation_out,
        "2025_Basis": basis_out,
        "Validation": validation_out,
    }

    creds = get_creds()
    service = build("sheets", "v4", credentials=creds)
    required_titles = ["README", "Index"] + tab_names + SUPPORT_TABS
    gids = ensure_sheets(service, required_titles)

    unmerge_requests = []
    for title, sheet_id in gids.items():
        unmerge_requests.append(
            {
                "unmergeCells": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 0,
                        "endRowIndex": 2000,
                        "startColumnIndex": 0,
                        "endColumnIndex": 60,
                    }
                }
            }
        )
    batch_update_requests(service, unmerge_requests)

    clear_ranges = [f"{q(title)}!A:AZ" for title in required_titles]
    execute_with_backoff(
        service.spreadsheets().values().batchClear(
            spreadsheetId=SPREADSHEET_ID,
            body={"ranges": clear_ranges},
        )
    )

    readme_values = build_readme_values(len(tab_names), mod.CURRENT_DATASET_ID)
    execute_with_backoff(
        service.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{q('README')}!A1",
            valueInputOption="RAW",
            body={"values": readme_values},
        )
    )

    index_values = build_index_values(validation, allocation, tab_names, gids)
    execute_with_backoff(
        service.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{q('Index')}!A1",
            valueInputOption="USER_ENTERED",
            body={"values": index_values},
        )
    )

    value_data = []
    for tab, values in report_values.items():
        value_data.append({"range": f"{q(tab)}!A1", "values": values})
    execute_with_backoff(
        service.spreadsheets().values().batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body={"valueInputOption": "RAW", "data": value_data},
        )
    )

    support_resize_requests: list[dict[str, object]] = []
    for title, df in support_frames.items():
        support_resize_requests.append(
            {
                "updateSheetProperties": {
                    "properties": {
                        "sheetId": gids[title],
                        "gridProperties": {
                            "rowCount": max(len(df) + 21, 100),
                            "columnCount": max(len(df.columns), 1),
                            "frozenRowCount": 1,
                        },
                    },
                    "fields": "gridProperties(rowCount,columnCount,frozenRowCount)",
                }
            }
        )
    batch_update_requests(service, support_resize_requests)

    for title, df in support_frames.items():
        values = dataframe_values(df)
        for start, part in chunked(values):
            execute_with_backoff(
                service.spreadsheets().values().update(
                    spreadsheetId=SPREADSHEET_ID,
                    range=f"{q(title)}!A{start + 1}",
                    valueInputOption="RAW",
                    body={"values": part},
                )
            )

    fmt_requests: list[dict[str, object]] = []
    for tab in tab_names:
        fmt_requests.extend(
            format_report_sheet_requests(
                gids[tab],
                report_blocks[tab],
                len(report_values[tab]),
                2 + len(mod.PERIODS),
                mod.PERIODS,
            )
        )

    index_row_count = len(index_values)
    index_grid_rows = max(index_row_count + 20, 100)
    fmt_requests.extend(
        [
            {
                "updateSheetProperties": {
                    "properties": {
                        "sheetId": gids["Index"],
                        "gridProperties": {
                            "rowCount": index_grid_rows,
                            "columnCount": len(index_values[2]),
                            "frozenRowCount": 3,
                        },
                    },
                    "fields": "gridProperties(rowCount,columnCount,frozenRowCount)",
                }
            },
            {
                "repeatCell": {
                    "range": {
                        "sheetId": gids["Index"],
                        "startRowIndex": 0,
                        "endRowIndex": index_grid_rows,
                        "startColumnIndex": 0,
                        "endColumnIndex": len(index_values[2]),
                    },
                    "cell": {"userEnteredFormat": {}},
                    "fields": "userEnteredFormat",
                }
            },
            {
                "repeatCell": {
                    "range": {
                        "sheetId": gids["Index"],
                        "startRowIndex": 2,
                        "endRowIndex": 3,
                        "startColumnIndex": 0,
                        "endColumnIndex": len(index_values[2]),
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "backgroundColor": {"red": 0.85, "green": 0.92, "blue": 0.97},
                            "textFormat": {"bold": True},
                            "horizontalAlignment": "CENTER",
                        }
                    },
                    "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)",
                }
            },
            {
                "repeatCell": {
                    "range": {
                        "sheetId": gids["Index"],
                        "startRowIndex": 3,
                        "endRowIndex": max(index_row_count, 4),
                        "startColumnIndex": 1,
                        "endColumnIndex": len(index_values[2]),
                    },
                    "cell": {"userEnteredFormat": {"numberFormat": {"type": "NUMBER", "pattern": "#,##0"}}},
                    "fields": "userEnteredFormat.numberFormat",
                }
            },
        ]
    )
    for idx in [6, 8, 10]:
        fmt_requests.append(
            {
                "repeatCell": {
                    "range": {
                        "sheetId": gids["Index"],
                        "startRowIndex": 3,
                        "endRowIndex": max(index_row_count, 4),
                        "startColumnIndex": idx,
                        "endColumnIndex": idx + 1,
                    },
                    "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "#,##0%"}}},
                    "fields": "userEnteredFormat.numberFormat",
                }
            }
        )
    for idx, header in enumerate(index_values[2]):
        if header not in {"No_Basis_Rows", "No_Basis_BSA"}:
            continue
        fmt_requests.extend(
            [
                {
                    "repeatCell": {
                        "range": {
                            "sheetId": gids["Index"],
                            "startRowIndex": 2,
                            "endRowIndex": 3,
                            "startColumnIndex": idx,
                            "endColumnIndex": idx + 1,
                        },
                        "cell": {
                            "note": NO_BASIS_NOTE,
                            "userEnteredFormat": {
                                "backgroundColor": NO_BASIS_HEADER_COLOR,
                                "textFormat": {"bold": True},
                                "horizontalAlignment": "CENTER",
                            },
                        },
                        "fields": "note,userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)",
                    }
                },
                {
                    "repeatCell": {
                        "range": {
                            "sheetId": gids["Index"],
                            "startRowIndex": 3,
                            "endRowIndex": max(index_row_count, 4),
                            "startColumnIndex": idx,
                            "endColumnIndex": idx + 1,
                        },
                        "cell": {"userEnteredFormat": {"backgroundColor": NO_BASIS_VALUE_COLOR}},
                        "fields": "userEnteredFormat.backgroundColor",
                    }
                },
            ]
        )
    fmt_requests.extend(
        [
            {
                "updateSheetProperties": {
                    "properties": {
                        "sheetId": gids["README"],
                        "gridProperties": {"rowCount": 100, "columnCount": 4, "frozenRowCount": 0},
                    },
                    "fields": "gridProperties(rowCount,columnCount,frozenRowCount)",
                }
            },
            {
                "repeatCell": {
                    "range": {
                        "sheetId": gids["README"],
                        "startRowIndex": 0,
                        "endRowIndex": 100,
                        "startColumnIndex": 0,
                        "endColumnIndex": 4,
                    },
                    "cell": {"userEnteredFormat": {}},
                    "fields": "userEnteredFormat",
                }
            },
            {
                "repeatCell": {
                    "range": {"sheetId": gids["README"], "startRowIndex": 0, "endRowIndex": 1},
                    "cell": {"userEnteredFormat": {"textFormat": {"bold": True, "fontSize": 14}}},
                    "fields": "userEnteredFormat.textFormat",
                }
            },
        ]
    )
    readme_no_basis_row = next(
        (idx for idx, row in enumerate(readme_values) if row and row[0] == "No-basis handling"),
        None,
    )
    if readme_no_basis_row is not None:
        fmt_requests.extend(
            [
                {
                    "repeatCell": {
                        "range": {
                            "sheetId": gids["README"],
                            "startRowIndex": readme_no_basis_row,
                            "endRowIndex": readme_no_basis_row + 1,
                            "startColumnIndex": 0,
                            "endColumnIndex": 2,
                        },
                        "cell": {"userEnteredFormat": {"backgroundColor": NO_BASIS_VALUE_COLOR}},
                        "fields": "userEnteredFormat.backgroundColor",
                    }
                },
                {
                    "repeatCell": {
                        "range": {
                            "sheetId": gids["README"],
                            "startRowIndex": readme_no_basis_row,
                            "endRowIndex": readme_no_basis_row + 1,
                            "startColumnIndex": 1,
                            "endColumnIndex": 2,
                        },
                        "cell": {"note": NO_BASIS_NOTE},
                        "fields": "note",
                    }
                },
            ]
        )
    for title, df in support_frames.items():
        fmt_requests.extend(format_support_requests(gids[title], df, len(df) + 1))
    batch_update_requests(service, fmt_requests)

    result = {
        "spreadsheet_id": SPREADSHEET_ID,
        "report_tabs": len(tab_names),
        "tabs": tab_names,
        "source_bsa": float(validation["route_bsa"].sum()),
        "allocated_bsa": float(validation["allocated_bsa"].sum()),
        "bsa_diff": float(validation["allocated_bsa"].sum() - validation["route_bsa"].sum()),
        "wos3_fst_teu": float(validation["w3"].sum()),
        "high_profit_wos3_fst_teu": float(validation["hi_w3"].sum()),
        "allocation_levels": {
            str(k): float(v) for k, v in allocation.groupby("allocation_level")["allocated_bsa"].sum().to_dict().items()
        },
        "no_2025_basis_rows": int((allocation["allocation_level"] == NO_BASIS_LEVEL).sum()),
        "no_2025_basis_bsa": float(
            allocation.loc[allocation["allocation_level"].eq(NO_BASIS_LEVEL), "allocated_bsa"].sum()
        ),
    }
    report_path = ROOT / "output" / "3W_Booking_Action_BSA_Salesperson_google_update.json"
    report_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
