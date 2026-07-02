"""Backfill Salesman_POR in historical dashboard summary JSON files on Drive.

Default mode is a dry run. Pass --apply to upload corrected JSON back to the
same Google Drive files.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(ROOT / "scripts"))

from remap_dashboard_salesman import (  # noqa: E402
    DEFAULT_MISSING,
    find_salesman_csv,
    load_active_mapping,
    remap_shipper,
)


GDRIVE_FOLDER_ID = "1JIxg6Y-_gRfI1HueXZ1Q9j4-Z5bxvNgv"
GDRIVE_CREDS_DIR = ROOT.parent / ".gdrive-mcp"
SUMMARY_NAME_RE = re.compile(r"^dashboard_summary(?:_(?:\d{4}|\d{8}))?\.json$")


def drive_request(method: str, url: str, *, retry_label: str, **kwargs: Any) -> requests.Response:
    retries = 4
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            resp = requests.request(method, url, timeout=90, **kwargs)
            resp.raise_for_status()
            return resp
        except requests.RequestException as exc:
            last_error = exc
            if attempt == retries:
                break
            wait = min(30, 2**attempt)
            print(f"  WARN: {retry_label} failed on attempt {attempt}/{retries}; retrying in {wait}s")
            time.sleep(wait)
    raise RuntimeError(f"{retry_label} failed after {retries} attempts: {last_error}") from last_error


def drive_headers() -> dict[str, str]:
    creds = json.loads((GDRIVE_CREDS_DIR / "credentials.json").read_text(encoding="utf-8-sig"))["installed"]
    token = json.loads((GDRIVE_CREDS_DIR / "token.json").read_text(encoding="utf-8-sig"))
    resp = drive_request(
        "POST",
        "https://oauth2.googleapis.com/token",
        retry_label="Google OAuth token refresh",
        data={
            "client_id": creds["client_id"],
            "client_secret": creds["client_secret"],
            "refresh_token": token["refresh_token"],
            "grant_type": "refresh_token",
        },
    )
    return {"Authorization": f"Bearer {resp.json()['access_token']}"}


def list_summary_files(headers: dict[str, str], folder_id: str) -> list[dict[str, Any]]:
    files: list[dict[str, Any]] = []
    page_token = None
    query = f"name contains 'dashboard_summary' and '{folder_id}' in parents and trashed=false"
    while True:
        params = {
            "q": query,
            "fields": "nextPageToken,files(id,name,size,modifiedTime)",
            "orderBy": "name",
            "pageSize": 100,
        }
        if page_token:
            params["pageToken"] = page_token
        resp = drive_request(
            "GET",
            "https://www.googleapis.com/drive/v3/files",
            retry_label="Drive summary file listing",
            headers=headers,
            params=params,
        )
        payload = resp.json()
        files.extend(f for f in payload.get("files", []) if SUMMARY_NAME_RE.match(f.get("name", "")))
        page_token = payload.get("nextPageToken")
        if not page_token:
            return sorted(files, key=lambda item: item["name"])


def download_file(headers: dict[str, str], file_id: str, name: str) -> bytes:
    resp = drive_request(
        "GET",
        f"https://www.googleapis.com/drive/v3/files/{file_id}",
        retry_label=f"Drive download {name}",
        headers=headers,
        params={"alt": "media"},
    )
    return resp.content


def upload_file(headers: dict[str, str], file_id: str, name: str, content: bytes) -> None:
    drive_request(
        "PATCH",
        f"https://www.googleapis.com/upload/drive/v3/files/{file_id}",
        retry_label=f"Drive upload {name}",
        headers={**headers, "Content-Type": "application/json; charset=UTF-8"},
        params={"uploadType": "media"},
        data=content,
    )


def shipper_row_count(data: dict[str, Any]) -> int:
    shipper = data.get("shipper")
    if isinstance(shipper, list):
        return len(shipper)
    if isinstance(shipper, dict):
        rows = shipper.get("r") or shipper.get("rows") or []
        return len(rows) if isinstance(rows, list) else 0
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Upload corrected files to Drive")
    parser.add_argument("--as-of", default=datetime.now().strftime("%Y%m%d"))
    parser.add_argument("--folder-id", default=GDRIVE_FOLDER_ID)
    parser.add_argument("--salesman-csv", default=None)
    parser.add_argument("--missing-label", default=DEFAULT_MISSING)
    parser.add_argument("--limit", type=int, default=0, help="Optional max files for testing")
    parser.add_argument("--name", action="append", default=[], help="Only process matching Drive filename")
    args = parser.parse_args()

    salesman_path = find_salesman_csv(args.salesman_csv)
    mapping = load_active_mapping(salesman_path, args.as_of)
    headers = drive_headers()
    files = list_summary_files(headers, args.folder_id)
    if args.name:
        wanted = set(args.name)
        files = [f for f in files if f["name"] in wanted]
    if args.limit:
        files = files[: args.limit]

    print(f"Salesman map: {len(mapping['unique_customer']):,} unique CUSTOMER_NO owners as of {args.as_of}")
    print(f"Drive summaries found: {len(files):,}; mode: {'APPLY' if args.apply else 'DRY-RUN'}")

    total_rows = total_matched = total_raw_fallback = total_unmatched = total_changed = total_uploaded = 0
    for idx, meta in enumerate(files, 1):
        name = meta["name"]
        raw = download_file(headers, meta["id"], name)
        data = json.loads(raw.decode("utf-8-sig"))
        rows = shipper_row_count(data)
        matched, raw_fallback, unmatched = remap_shipper(data, mapping, args.missing_label)
        data["obt_salesmen"] = mapping["obt_salesmen"]
        fixed = json.dumps(data, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        changed = fixed != raw
        if args.apply and changed:
            upload_file(headers, meta["id"], name, fixed)
            total_uploaded += 1
        total_rows += rows
        total_matched += matched
        total_raw_fallback += raw_fallback
        total_unmatched += unmatched
        total_changed += int(changed)
        status = "uploaded" if args.apply and changed else "would-update" if changed else "unchanged"
        print(
            f"[{idx:03d}/{len(files):03d}] {name}: rows={rows:,}, "
            f"matched={matched:,}, raw_fallback={raw_fallback:,}, unmatched={unmatched:,}, {status}"
        )

    print(
        "TOTAL: "
        f"files={len(files):,}, changed={total_changed:,}, uploaded={total_uploaded:,}, "
        f"rows={total_rows:,}, matched={total_matched:,}, raw_fallback={total_raw_fallback:,}, "
        f"unmatched={total_unmatched:,}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
