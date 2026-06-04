"""Create a brand-new Google Spreadsheet used for entering yearly OBT sales targets.

Architecture: "Eternal Workbook" — one URL serves every year.
  - README       : how to use, who edits, rollover instructions
  - Settings     : Active_Year cell (drives downstream lookups)
  - Targets      : input table keyed by (Year, Tab) — Booking %p / Lifting %p / High-Profit %p / Memo
  - Audit        : append-only log (Year, Tab, field, old, new, who, when) — written by Apps Script later

Only the OBT origin codes that the dashboard accepts are pre-populated as rows.

Usage:
    py -3 scripts/create_target_input_workbook.py
        [--title "OBT Sales Target Input (Eternal)"]
        [--year 2026]
        [--include-suggested]     # also pre-fill the Suggested columns

The resulting spreadsheet ID + URL is printed and saved to
`output/target_input_workbook.json` so other scripts can pick it up.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build


ROOT = Path(__file__).resolve().parents[1]
STATE_PATH = ROOT / "output" / "target_input_workbook.json"
DEFAULT_TITLE = "OBT Sales Target Input (Eternal)"

# Same whitelist the dashboard uses. Keep in sync with dist/sales-target/app.js.
WHITELIST = [
    ("CN", "중국 / China", ["CN_SHA", "CN_NKG", "CN_NBO", "CN_TAO", "CN_XGG", "CN_DLC", "CN_LYG", "CN_SHK_DCB", "CN_XMN", "CN_NNS"]),
    ("HK", "홍콩 / Hong Kong", ["HK"]),
    ("TW", "대만 / Taiwan", ["TW"]),
    ("TH", "태국 / Thailand", ["TH"]),
    ("VN", "베트남 / Vietnam", ["VN_SGN_CMP", "VN_HPH"]),
    ("PH", "필리핀 / Philippines", ["PH"]),
    ("MY", "말레이시아 / Malaysia", ["PKG+PKW", "PEN", "PGU"]),
    ("SG", "싱가포르 / Singapore", ["SG"]),
    ("ID", "인도네시아 / Indonesia", ["JKT", "SUB", "ID-IDO"]),
    ("IN", "인도 / India", ["IN"]),
    ("AE", "UAE", ["AE"]),
]


def get_creds() -> Credentials:
    creds_dir = ROOT.parent / ".gdrive-mcp"
    installed = json.loads((creds_dir / "credentials.json").read_text(encoding="utf-8-sig"))["installed"]
    token = json.loads((creds_dir / "token.json").read_text(encoding="utf-8-sig"))
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


def rgb(hex_color: str) -> dict[str, float]:
    h = hex_color.strip().lstrip("#")
    return {
        "red": int(h[0:2], 16) / 255,
        "green": int(h[2:4], 16) / 255,
        "blue": int(h[4:6], 16) / 255,
    }


def create_spreadsheet(service, title: str) -> dict[str, Any]:
    body = {
        "properties": {"title": title, "locale": "ko_KR"},
        "sheets": [
            {"properties": {"sheetId": 100, "title": "README", "gridProperties": {"rowCount": 60, "columnCount": 6}}},
            {"properties": {"sheetId": 110, "title": "Settings", "gridProperties": {"rowCount": 20, "columnCount": 6, "frozenRowCount": 1}}},
            {"properties": {"sheetId": 120, "title": "Targets", "gridProperties": {"rowCount": 400, "columnCount": 10, "frozenRowCount": 1}}},
            {"properties": {"sheetId": 130, "title": "Audit", "gridProperties": {"rowCount": 1000, "columnCount": 8, "frozenRowCount": 1}}},
        ],
    }
    return service.spreadsheets().create(body=body).execute()


def col_letter(idx0: int) -> str:
    out = ""
    n = idx0 + 1
    while n:
        n, r = divmod(n - 1, 26)
        out = chr(65 + r) + out
    return out


def build_requests(active_year: int, include_suggested: bool) -> tuple[list[dict], list[dict]]:
    """Return (batchUpdate metadata requests, values batchUpdate updates)."""
    requests: list[dict] = []
    value_updates: list[dict] = []

    # ── README content ────────────────────────────────────────────────
    readme = [
        ["OBT Sales Target Input (Eternal Workbook)"],
        [""],
        ["Purpose:"],
        ["  Single source of truth for entering annual OBT sales targets."],
        ["  Lives at this URL forever — do NOT replace each year. Add new Year rows in Targets."],
        [""],
        ["Tabs:"],
        ["  README   - this page"],
        ["  Settings - Active_Year drives downstream consumers (set to current year)"],
        ["  Targets  - input table; one row per (Year, Tab)"],
        ["  Audit    - append-only change log"],
        [""],
        ["Editing rules:"],
        ["  - Only the columns labelled 'Input %p' are user-editable. Memo is free-text."],
        ["  - 'Suggested %p' is a hint computed by the analyst team; leave editing to the team."],
        ["  - Performance / Gap views live in the 36-tab reporting workbook; this one is INPUT only."],
        [""],
        ["Annual rollover (next year):"],
        ["  1. Append new rows in Targets with the next Year."],
        ["  2. Update Settings!B2 (Active_Year) to the new year."],
        ["  3. Adjust permissions if the admin list changes."],
        [""],
        ["Permissions (configure manually in Share → Advanced):"],
        ["  - Editor access for the admin list only."],
        ["  - Comment / View for the wider team if needed."],
        ["  - Protect ranges: Targets!A:B (Year + Tab) and Targets!header (row 1)."],
        [""],
        ["Whitelist of origin codes (must match dashboard filters):"],
    ]
    for country, label, ports in WHITELIST:
        readme.append([f"  {country} — {label}: {', '.join(ports)}"])

    value_updates.append({
        "range": "README!A1",
        "majorDimension": "ROWS",
        "values": readme,
    })

    # ── Settings content ──────────────────────────────────────────────
    settings_rows = [
        ["Key", "Value", "Description"],
        ["Active_Year", active_year, "Targets!Year matching this value drives downstream consumers"],
        ["Admins", "", "Comma-separated emails authorised to edit Targets"],
        ["Last_Reviewed", "", "YYYY-MM-DD of last sign-off (free text)"],
        ["Notes", "", ""],
    ]
    value_updates.append({
        "range": "Settings!A1",
        "majorDimension": "ROWS",
        "values": settings_rows,
    })

    # ── Targets header + rows ─────────────────────────────────────────
    target_header = [
        "Year",
        "Tab",
        "Country",
        "Booking Input (%p)",
        "Booking Suggested (%p)",
        "Lifting Input (%p)",
        "Lifting Suggested (%p)",
        "High-Profit Input (%p)",
        "High-Profit Suggested (%p)",
        "Memo",
    ]
    target_rows: list[list[Any]] = [target_header]
    for country, _label, ports in WHITELIST:
        for port in ports:
            row = [
                active_year,
                port,
                country,
                10.0 if port.startswith("CN") else 5.0,                      # Booking Input default
                10.0 if include_suggested else "",
                5.0,                                                         # Lifting Input default
                5.0 if include_suggested else "",
                3.0,                                                         # HP Input default
                3.0 if include_suggested else "",
                "",
            ]
            target_rows.append(row)
    value_updates.append({
        "range": "Targets!A1",
        "majorDimension": "ROWS",
        "values": target_rows,
    })

    # ── Audit header ──────────────────────────────────────────────────
    value_updates.append({
        "range": "Audit!A1",
        "majorDimension": "ROWS",
        "values": [["Timestamp", "Year", "Tab", "Field", "Old", "New", "Editor", "Comment"]],
    })

    # ── Formatting requests ──────────────────────────────────────────
    bold_header = lambda sheet_id, end_col: {
        "repeatCell": {
            "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1, "startColumnIndex": 0, "endColumnIndex": end_col},
            "cell": {"userEnteredFormat": {
                "backgroundColor": rgb("#1a73e8"),
                "textFormat": {"foregroundColor": rgb("#ffffff"), "bold": True},
                "horizontalAlignment": "CENTER",
            }},
            "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)",
        }
    }
    requests.append(bold_header(110, 6))
    requests.append(bold_header(120, 10))
    requests.append(bold_header(130, 8))
    # Style Settings key column
    requests.append({
        "repeatCell": {
            "range": {"sheetId": 110, "startRowIndex": 1, "endRowIndex": 10, "startColumnIndex": 0, "endColumnIndex": 1},
            "cell": {"userEnteredFormat": {"backgroundColor": rgb("#e8f0fe"), "textFormat": {"bold": True}}},
            "fields": "userEnteredFormat(backgroundColor,textFormat)",
        }
    })
    # Highlight Input columns in Targets (D, F, H, J = indexes 3,5,7,9)
    for col_idx in (3, 5, 7):
        requests.append({
            "repeatCell": {
                "range": {"sheetId": 120, "startRowIndex": 1, "startColumnIndex": col_idx, "endColumnIndex": col_idx + 1},
                "cell": {"userEnteredFormat": {"backgroundColor": rgb("#fff4ce")}},
                "fields": "userEnteredFormat(backgroundColor)",
            }
        })
    # Suggested columns greyed
    for col_idx in (4, 6, 8):
        requests.append({
            "repeatCell": {
                "range": {"sheetId": 120, "startRowIndex": 1, "startColumnIndex": col_idx, "endColumnIndex": col_idx + 1},
                "cell": {"userEnteredFormat": {"backgroundColor": rgb("#f1f3f4")}},
                "fields": "userEnteredFormat(backgroundColor)",
            }
        })
    # Auto-resize columns
    for sheet_id, end in [(100, 6), (110, 6), (120, 10), (130, 8)]:
        requests.append({
            "autoResizeDimensions": {
                "dimensions": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": 0, "endIndex": end}
            }
        })
    # Protect Targets header row (read-only)
    requests.append({
        "addProtectedRange": {
            "protectedRange": {
                "range": {"sheetId": 120, "startRowIndex": 0, "endRowIndex": 1, "startColumnIndex": 0, "endColumnIndex": 10},
                "description": "Targets header — read-only",
                "warningOnly": True,
            }
        }
    })
    # Protect Audit entirely (warning) so users don't edit logs by hand
    requests.append({
        "addProtectedRange": {
            "protectedRange": {
                "range": {"sheetId": 130},
                "description": "Audit log — do not edit",
                "warningOnly": True,
            }
        }
    })

    return requests, value_updates


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--title", default=DEFAULT_TITLE)
    parser.add_argument("--year", type=int, default=2026)
    parser.add_argument("--include-suggested", action="store_true")
    args = parser.parse_args()

    creds = get_creds()
    service = build("sheets", "v4", credentials=creds, cache_discovery=False)
    drive = build("drive", "v3", credentials=creds, cache_discovery=False)

    print(f"[1/3] Creating spreadsheet titled '{args.title}' ...", flush=True)
    sheet = create_spreadsheet(service, args.title)
    spreadsheet_id = sheet["spreadsheetId"]
    url = sheet["spreadsheetUrl"]

    print(f"[2/3] Writing initial content + formatting ...", flush=True)
    requests, value_updates = build_requests(args.year, args.include_suggested)
    service.spreadsheets().values().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={"valueInputOption": "USER_ENTERED", "data": value_updates},
    ).execute()
    service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={"requests": requests},
    ).execute()

    print(f"[3/3] Marking the file public-readable to project members (manual share still recommended) ...", flush=True)
    try:
        drive.permissions().create(
            fileId=spreadsheet_id,
            body={"type": "anyone", "role": "reader"},
            fields="id",
        ).execute()
    except Exception as err:
        print(f"      WARN: could not set public-read permission ({err}); share manually.", flush=True)

    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(
        json.dumps(
            {
                "spreadsheet_id": spreadsheet_id,
                "url": url,
                "title": args.title,
                "active_year": args.year,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    print(f"Done.\n  URL: {url}\n  State: {STATE_PATH}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
