#!/usr/bin/env python3
"""Synchronize the dashboard runtime data set to its Google Drive folders.

The dashboards authenticate users with Google OAuth in the browser and read the
uploaded files through Drive API ``files.get?alt=media``.  This script preserves
file IDs by updating existing files in place, so the deployed JavaScript config
does not need to change after every daily refresh.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import mimetypes
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import requests


ROOT = Path(__file__).resolve().parents[1]
RUNTIME_ROOT = Path(os.environ.get("DASHBOARD_RUNTIME_ROOT", str(ROOT)))
CREDS_DIR = ROOT.parent / ".gdrive-mcp"

DRIVE_FOLDERS = {
    "root": "1qwcpaBcfwSpT9UTTLukajuSrxtjTRqYC",
    "main": "1lDyEaYHhvkMw6BoTJxSNgkWG0-BbMtpY",
    "main_history": "1q8XdzDbYHGL6aObw0cGik8xOQgNkNgmg",
    "main_source": "1zUm2P3VbwkRR6Up_zK111PV5moApKT65",
    "obt": "1ChAP3LiukrM3_GFDBL-kfgC8S4ejJ66P",
    "sales": "1pfGPPjtbQN7daHfeJ2vPY2blJYPTw0jl",
    "sales_data": "1vwXvhaxItQ00E3u5pxFebuX1d3v6t31-",
    "documentation": "1GkZkU9MiiTOEOWT91Y__GDfRkK0hmj1D",
    "analysis": "1Jq7ZESyVJaKOJ4RI5Mq6scaGZBYgyph6",
}

DRIVE_API = "https://www.googleapis.com/drive/v3"
DRIVE_UPLOAD_API = "https://www.googleapis.com/upload/drive/v3"
LOCAL_STATE_ROOT = Path(
    os.environ.get("DASHBOARD_LOCAL_STATE_DIR")
    or Path(os.environ.get("LOCALAPPDATA") or (Path.home() / "AppData" / "Local"))
    / "KMTC"
    / "3w-dashboard"
)
STATE_PATH = LOCAL_STATE_ROOT / "gdrive_runtime_manifest.json"
THREAD_LOCAL = threading.local()
LEGACY_FOLDER_ID = "1JIxg6Y-_gRfI1HueXZ1Q9j4-Z5bxvNgv"
HISTORICAL_CACHE_PATH = RUNTIME_ROOT / "output" / "_cache_2025.parquet"
OBT_HISTORY_PATH = RUNTIME_ROOT / "obt-exception-monitor" / "history.json"


def refresh_access_token(*, retries: int = 6) -> str:
    credentials = json.loads(
        (CREDS_DIR / "credentials.json").read_text(encoding="utf-8-sig")
    )["installed"]
    token = json.loads(
        (CREDS_DIR / "token.json").read_text(encoding="utf-8-sig")
    )
    payload = {
        "client_id": credentials["client_id"],
        "client_secret": credentials["client_secret"],
        "refresh_token": token["refresh_token"],
        "grant_type": "refresh_token",
    }
    last_error: Exception | None = None
    for attempt in range(1, max(1, retries) + 1):
        try:
            response = requests.post(
                "https://oauth2.googleapis.com/token",
                data=payload,
                timeout=30,
            )
            if response.status_code in {408, 425, 429} or response.status_code >= 500:
                response.raise_for_status()
            if not response.ok:
                detail = response.text[:500].replace("\n", " ")
                raise RuntimeError(
                    f"Google OAuth token refresh: HTTP {response.status_code}: {detail}"
                )
            return response.json()["access_token"]
        except (requests.RequestException, RuntimeError, KeyError) as exc:
            last_error = exc
            if attempt >= max(1, retries):
                break
            delay = min(60, 2 ** attempt)
            print(
                f"[auth] token refresh attempt {attempt}/{retries} failed: {exc}; "
                f"retrying in {delay}s"
            )
            time.sleep(delay)
    raise RuntimeError(
        f"Google OAuth token refresh failed after {max(1, retries)} attempts: {last_error}"
    )


def session(access_token: str) -> requests.Session:
    current = getattr(THREAD_LOCAL, "session", None)
    if current is None:
        current = requests.Session()
        current.headers.update({"Authorization": f"Bearer {access_token}"})
        THREAD_LOCAL.session = current
    return current


def request(
    access_token: str,
    method: str,
    url: str,
    *,
    label: str,
    retries: int = 6,
    **kwargs: Any,
) -> requests.Response:
    kwargs.setdefault("timeout", 180)
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            response = session(access_token).request(method, url, **kwargs)
            if response.status_code in {408, 425, 429} or response.status_code >= 500:
                response.raise_for_status()
            if not response.ok:
                detail = response.text[:500].replace("\n", " ")
                raise RuntimeError(
                    f"{label}: HTTP {response.status_code}: {detail}"
                )
            return response
        except (requests.RequestException, RuntimeError) as exc:
            last_error = exc
            if attempt >= retries:
                break
            delay = min(60, 2 ** attempt)
            time.sleep(delay)
    raise RuntimeError(f"{label} failed after {retries} attempts: {last_error}")


def list_files(access_token: str, folder_id: str) -> list[dict[str, Any]]:
    files: list[dict[str, Any]] = []
    page_token: str | None = None
    while True:
        params = {
            "q": f"'{folder_id}' in parents and trashed=false",
            "pageSize": 1000,
            "fields": "nextPageToken,files(id,name,size,md5Checksum,mimeType,parents)",
        }
        if page_token:
            params["pageToken"] = page_token
        response = request(
            access_token,
            "GET",
            f"{DRIVE_API}/files",
            label=f"list folder {folder_id}",
            params=params,
        )
        payload = response.json()
        files.extend(payload.get("files") or [])
        page_token = payload.get("nextPageToken")
        if not page_token:
            return files


def ensure_company_reader(access_token: str) -> None:
    response = request(
        access_token,
        "GET",
        f"{DRIVE_API}/files/{DRIVE_FOLDERS['root']}/permissions",
        label="read root folder permissions",
        params={"fields": "permissions(id,type,role,domain,allowFileDiscovery)"},
    )
    permissions = response.json().get("permissions") or []
    if any(
        item.get("type") == "domain"
        and item.get("domain") == "ekmtc.com"
        and item.get("role") == "reader"
        for item in permissions
    ):
        print("[permissions] ekmtc.com reader permission already present")
        return
    request(
        access_token,
        "POST",
        f"{DRIVE_API}/files/{DRIVE_FOLDERS['root']}/permissions",
        label="create ekmtc.com reader permission",
        params={"fields": "id,type,role,domain,allowFileDiscovery"},
        headers={"Content-Type": "application/json; charset=UTF-8"},
        json={
            "type": "domain",
            "role": "reader",
            "domain": "ekmtc.com",
            "allowFileDiscovery": False,
        },
    )
    print("[permissions] added ekmtc.com reader permission (not discoverable)")


def md5_file(path: Path) -> str:
    digest = hashlib.md5()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def download_verified_file(
    access_token: str,
    remote: dict[str, Any],
    target_path: Path,
) -> Path:
    """Download one Drive file atomically and verify its advertised metadata."""
    remote_name = str(remote.get("name") or target_path.name)
    expected_md5 = str(remote.get("md5Checksum") or "")
    expected_size = int(remote.get("size") or 0)
    target_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = target_path.with_name(f".{target_path.name}.download")
    temp_path.unlink(missing_ok=True)
    try:
        response = request(
            access_token,
            "GET",
            f"{DRIVE_API}/files/{remote['id']}",
            label=f"download {remote_name}",
            params={"alt": "media"},
            stream=True,
            timeout=600,
        )
        digest = hashlib.md5()
        downloaded_size = 0
        with temp_path.open("wb") as handle:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if not chunk:
                    continue
                handle.write(chunk)
                digest.update(chunk)
                downloaded_size += len(chunk)
        downloaded_md5 = digest.hexdigest()
        if expected_size and downloaded_size != expected_size:
            raise RuntimeError(
                f"Downloaded {remote_name} size mismatch: "
                f"expected {expected_size:,}, got {downloaded_size:,}"
            )
        if expected_md5 and downloaded_md5 != expected_md5:
            raise RuntimeError(
                f"Downloaded {remote_name} MD5 mismatch: "
                f"expected {expected_md5}, got {downloaded_md5}"
            )
        temp_path.replace(target_path)
    except Exception:
        temp_path.unlink(missing_ok=True)
        raise
    print(
        f"[restore] downloaded {remote_name} -> {target_path} "
        f"({downloaded_size:,} bytes, md5={downloaded_md5})"
    )
    return target_path


def ensure_historical_cache(access_token: str) -> Path:
    """Restore the immutable 2025 booking cache from Drive when needed.

    The daily pipeline consumes this cache before the runtime-data sync step. It
    therefore has to survive local cleanup, and a missing/stale local copy must
    be recoverable at the start of a later run.
    """
    remote_name = HISTORICAL_CACHE_PATH.name
    remote_files = {
        item["name"]: item
        for item in list_files(access_token, DRIVE_FOLDERS["main_source"])
    }
    remote = remote_files.get(remote_name)
    if remote is None:
        raise FileNotFoundError(
            f"Drive source artifact is missing: {remote_name} "
            f"(folder {DRIVE_FOLDERS['main_source']})"
        )

    expected_md5 = str(remote.get("md5Checksum") or "")
    expected_size = int(remote.get("size") or 0)
    if HISTORICAL_CACHE_PATH.exists():
        local_size = HISTORICAL_CACHE_PATH.stat().st_size
        local_md5 = md5_file(HISTORICAL_CACHE_PATH)
        size_matches = not expected_size or local_size == expected_size
        md5_matches = not expected_md5 or local_md5 == expected_md5
        if local_size > 0 and size_matches and md5_matches:
            print(
                f"[restore] {remote_name} already current "
                f"({local_size:,} bytes, md5={local_md5})"
            )
            return HISTORICAL_CACHE_PATH

    return download_verified_file(access_token, remote, HISTORICAL_CACHE_PATH)


def restore_obt_history(
    access_token: str,
    target_path: Path = OBT_HISTORY_PATH,
) -> Path:
    """Restore the canonical OBT history before appending today's snapshot."""
    matches = [
        item
        for item in list_files(access_token, DRIVE_FOLDERS["obt"])
        if item.get("name") == "obt_exception_history.json"
    ]
    if len(matches) != 1:
        raise RuntimeError(
            "Expected exactly one Drive obt_exception_history.json, "
            f"found {len(matches)} in folder {DRIVE_FOLDERS['obt']}"
        )
    return download_verified_file(access_token, matches[0], target_path)


def restore_latest_booking_snapshot(access_token: str) -> Path:
    """Rebuild the latest Booking CSV from its verified Drive parquet cache."""
    remote_files = list_files(access_token, DRIVE_FOLDERS["main_source"])
    cache_files = []
    for item in remote_files:
        name = str(item.get("name") or "")
        if not (name.startswith("_cache_") and name.endswith(".parquet")):
            continue
        date_token = name.removeprefix("_cache_").removesuffix(".parquet")
        if len(date_token) == 8 and date_token.isdigit() and date_token != "2025":
            cache_files.append((date_token, item))
    if not cache_files:
        raise FileNotFoundError("No dated Booking cache found in the Drive source folder")

    data_date, remote = max(cache_files, key=lambda pair: pair[0])
    cache_path = RUNTIME_ROOT / "output" / f"_cache_{data_date}.parquet"
    expected_md5 = str(remote.get("md5Checksum") or "")
    expected_size = int(remote.get("size") or 0)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    temp_cache_path = cache_path.with_name(f".{cache_path.name}.download")
    temp_cache_path.unlink(missing_ok=True)
    try:
        response = request(
            access_token,
            "GET",
            f"{DRIVE_API}/files/{remote['id']}",
            label=f"download {cache_path.name}",
            params={"alt": "media"},
            stream=True,
            timeout=600,
        )
        digest = hashlib.md5()
        downloaded_size = 0
        with temp_cache_path.open("wb") as handle:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if not chunk:
                    continue
                handle.write(chunk)
                digest.update(chunk)
                downloaded_size += len(chunk)
        downloaded_md5 = digest.hexdigest()
        if expected_size and downloaded_size != expected_size:
            raise RuntimeError(
                f"Downloaded {cache_path.name} size mismatch: "
                f"expected {expected_size:,}, got {downloaded_size:,}"
            )
        if expected_md5 and downloaded_md5 != expected_md5:
            raise RuntimeError(
                f"Downloaded {cache_path.name} MD5 mismatch: "
                f"expected {expected_md5}, got {downloaded_md5}"
            )
        temp_cache_path.replace(cache_path)
    except Exception:
        temp_cache_path.unlink(missing_ok=True)
        raise

    import pandas as pd

    snapshot_path = RUNTIME_ROOT / "output" / f"booking_snapshot_result_{data_date}.csv"
    temp_snapshot_path = snapshot_path.with_name(f".{snapshot_path.name}.writing")
    temp_snapshot_path.unlink(missing_ok=True)
    try:
        frame = pd.read_parquet(cache_path)
        if frame.empty:
            raise RuntimeError(f"Restored Booking cache is empty: {cache_path}")
        frame.to_csv(temp_snapshot_path, index=False, encoding="utf-8-sig")
        temp_snapshot_path.replace(snapshot_path)
    except Exception:
        temp_snapshot_path.unlink(missing_ok=True)
        raise
    print(
        f"[restore] rebuilt {snapshot_path.name} from verified Drive cache "
        f"({snapshot_path.stat().st_size:,} bytes, {len(frame):,} rows)"
    )
    return snapshot_path


def mime_type(path: Path) -> str:
    if path.name.endswith(".json.gz"):
        return "application/gzip"
    if path.suffix.lower() == ".json":
        return "application/json"
    return mimetypes.guess_type(path.name)[0] or "application/octet-stream"


def upload_media(
    access_token: str,
    *,
    file_id: str,
    path: Path,
    content_type: str,
) -> dict[str, Any]:
    size = path.stat().st_size
    # Keep retries replayable. A file handle may be left at EOF after a failed
    # request, causing the next attempt to advertise the original Content-Length
    # while sending no bytes and then wait until timeout.
    payload = path.read_bytes()
    return request(
        access_token,
        "PATCH",
        f"{DRIVE_UPLOAD_API}/files/{file_id}",
        label=f"upload {path.name}",
        params={"uploadType": "media", "fields": "id,name,size,md5Checksum,parents"},
        headers={
            "Content-Type": content_type,
            "Content-Length": str(size),
        },
        data=payload,
        timeout=600,
    ).json()


def sync_one(
    access_token: str,
    *,
    folder_id: str,
    path: Path,
    remote_name: str,
    existing: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    path = path.resolve()
    local_md5 = md5_file(path)
    found = existing.get(remote_name)
    if found and found.get("md5Checksum") == local_md5:
        return {
            "id": found["id"],
            "name": remote_name,
            "size": int(found.get("size") or path.stat().st_size),
            "md5": local_md5,
            "action": "unchanged",
        }

    if found:
        file_id = found["id"]
        action = "updated"
    else:
        response = request(
            access_token,
            "POST",
            f"{DRIVE_API}/files",
            label=f"create {remote_name}",
            params={"fields": "id,name,parents"},
            headers={"Content-Type": "application/json; charset=UTF-8"},
            json={"name": remote_name, "parents": [folder_id]},
        )
        file_id = response.json()["id"]
        action = "created"

    uploaded = upload_media(
        access_token,
        file_id=file_id,
        path=path,
        content_type=mime_type(path),
    )
    uploaded_size = int(uploaded.get("size") or 0)
    uploaded_md5 = str(uploaded.get("md5Checksum") or "")
    if uploaded_size and uploaded_size != path.stat().st_size:
        raise RuntimeError(
            f"Drive size mismatch after uploading {remote_name}: "
            f"local={path.stat().st_size:,}, remote={uploaded_size:,}"
        )
    if uploaded_md5 and uploaded_md5 != local_md5:
        raise RuntimeError(
            f"Drive MD5 mismatch after uploading {remote_name}: "
            f"local={local_md5}, remote={uploaded_md5}"
        )
    return {
        "id": uploaded["id"],
        "name": remote_name,
        "size": uploaded_size or path.stat().st_size,
        "md5": uploaded_md5 or local_md5,
        "action": action,
    }


def latest_summary() -> Path:
    candidates = sorted(
        (RUNTIME_ROOT / "output").glob("dashboard_summary_*.json"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    if not candidates:
        raise FileNotFoundError("No output/dashboard_summary_*.json file found")
    return candidates[0]


def latest_output_file(pattern: str, *, exclude_names: set[str] | None = None) -> Path:
    excluded = exclude_names or set()
    candidates = sorted(
        (
            path
            for path in (RUNTIME_ROOT / "output").glob(pattern)
            if path.name not in excluded
        ),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    if not candidates:
        raise FileNotFoundError(f"No output/{pattern} file found")
    return candidates[0]


def runtime_groups() -> dict[str, list[tuple[Path, str]]]:
    summary = latest_summary()
    cache_2025 = RUNTIME_ROOT / "output" / "_cache_2025.parquet"
    current_cache = latest_output_file(
        "_cache_*.parquet",
        exclude_names={"_cache_2025.parquet"},
    )
    current_bsa = latest_output_file("BSA_raw_monthly3W_*.csv")
    dataset_ids = {
        "summary": re.fullmatch(r"dashboard_summary_(\d{8})\.json", summary.name),
        "cache": re.fullmatch(r"_cache_(\d{8})\.parquet", current_cache.name),
        "bsa": re.fullmatch(r"BSA_raw_monthly3W_(\d{8})\.csv", current_bsa.name),
    }
    invalid = [label for label, match in dataset_ids.items() if match is None]
    if invalid:
        raise RuntimeError(
            "Cannot determine runtime dataset date for: " + ", ".join(invalid)
        )
    resolved_dates = {
        label: match.group(1)
        for label, match in dataset_ids.items()
        if match is not None
    }
    if len(set(resolved_dates.values())) != 1:
        raise RuntimeError(
            "Refusing to sync mixed runtime dataset dates: "
            + ", ".join(f"{label}={value}" for label, value in resolved_dates.items())
        )
    obt_history_json = RUNTIME_ROOT / "obt-exception-monitor" / "history.json"
    groups: dict[str, list[tuple[Path, str]]] = {
        "main": [
            (RUNTIME_ROOT / "dist" / "data.json.gz", "data.json.gz"),
            (summary, "dashboard_summary.json"),
        ],
        "main_history": [(summary, summary.name)],
        "main_source": [
            (cache_2025, cache_2025.name),
            (current_cache, current_cache.name),
            (current_bsa, current_bsa.name),
        ],
        "obt": [
            (
                RUNTIME_ROOT / "dist" / "obt-exception-monitor" / "history.json.gz",
                "history.json.gz",
            ),
            (obt_history_json, "obt_exception_history.json"),
        ],
        "sales": [
            (RUNTIME_ROOT / "dist" / "sales-target" / "index.json", "index.json"),
            (RUNTIME_ROOT / "dist" / "sales-target" / "manifest.json", "manifest.json"),
            (RUNTIME_ROOT / "dist" / "sales-target" / "base2025.json", "base2025.json"),
        ],
        "sales_data": [],
    }
    groups["sales_data"] = [
        (path, path.name)
        for path in sorted((RUNTIME_ROOT / "dist" / "sales-target" / "data").glob("*.json"))
    ]
    missing = [
        str(path)
        for entries in groups.values()
        for path, _ in entries
        if not path.exists()
    ]
    if missing:
        raise FileNotFoundError("Missing runtime data:\n" + "\n".join(missing))
    return groups


def cleanup_local_data(groups: dict[str, list[tuple[Path, str]]]) -> None:
    """Remove local pipeline data only after every Drive sync has succeeded."""
    if RUNTIME_ROOT.resolve() == ROOT.resolve():
        raise RuntimeError(
            "Refusing cleanup because DASHBOARD_RUNTIME_ROOT is the project root. "
            "Set it to the dedicated LocalAppData runtime directory."
        )
    # All generated runtime artifacts are temporary staging files. The immutable
    # 2025 cache is restored from Drive at the beginning of the next run, so it
    # must not remain inside the OneDrive-backed project after a verified sync.
    keep: set[Path] = set()
    targets = {
        path.resolve()
        for entries in groups.values()
        for path, _ in entries
    }
    targets.update(
        {
            (RUNTIME_ROOT / "dist" / "data.json").resolve(),
            (RUNTIME_ROOT / "dist" / "obt-exception-monitor" / "history.json").resolve(),
            (RUNTIME_ROOT / "1.csv").resolve(),
            (RUNTIME_ROOT / "2.csv").resolve(),
        }
    )
    for pattern in ("1_*.csv", "2_*.csv"):
        targets.update(path.resolve() for path in RUNTIME_ROOT.glob(pattern) if path.is_file())
    output_dir = (RUNTIME_ROOT / "output").resolve()
    if output_dir.exists():
        targets.update(path.resolve() for path in output_dir.rglob("*") if path.is_file())

    removed_files = 0
    removed_bytes = 0
    for path in sorted(targets):
        if path in keep or not path.exists():
            continue
        try:
            path.relative_to(RUNTIME_ROOT.resolve())
        except ValueError as exc:
            raise RuntimeError(f"Refusing to remove path outside runtime root: {path}") from exc
        if not path.is_file() and not path.is_symlink():
            continue
        removed_bytes += path.stat().st_size
        path.unlink()
        removed_files += 1

    removable_dirs = [
        RUNTIME_ROOT / "dist" / "sales-target" / "data",
        RUNTIME_ROOT / "output",
    ]
    for directory in removable_dirs:
        if directory.exists():
            try:
                directory.rmdir()
            except OSError:
                pass

    print(
        f"[cleanup] removed {removed_files:,} local data files "
        f"({removed_bytes / (1024 ** 2):,.1f} MiB) after verified Drive sync"
    )


def sync_group(
    access_token: str,
    *,
    group_name: str,
    entries: list[tuple[Path, str]],
    workers: int,
) -> list[dict[str, Any]]:
    folder_id = DRIVE_FOLDERS[group_name]
    existing_list = list_files(access_token, folder_id)
    existing = {item["name"]: item for item in existing_list}
    print(
        f"[{group_name}] {len(entries):,} local files; "
        f"{len(existing_list):,} existing Drive files"
    )

    results: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(
                sync_one,
                access_token,
                folder_id=folder_id,
                path=path,
                remote_name=remote_name,
                existing=existing,
            ): remote_name
            for path, remote_name in entries
        }
        completed = 0
        for future in as_completed(futures):
            result = future.result()
            results.append(result)
            completed += 1
            if completed == len(entries) or completed % 100 == 0:
                print(f"  {completed:,}/{len(entries):,} complete")
    return sorted(results, key=lambda item: item["name"])


def write_state(groups: dict[str, list[dict[str, Any]]]) -> None:
    files = {
        group: {item["name"]: item for item in entries}
        for group, entries in groups.items()
    }
    state = {
        "format": "dashboard-gdrive-runtime-v1",
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "folders": DRIVE_FOLDERS,
        "files": files,
    }
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(
        json.dumps(state, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"State: {STATE_PATH}")


def verify_remote_groups(
    access_token: str,
    groups: dict[str, list[dict[str, Any]]],
) -> int:
    """Read Drive metadata back and verify every expected runtime artifact."""
    checked = 0
    problems: list[str] = []
    for group_name, expected_items in groups.items():
        if group_name not in DRIVE_FOLDERS:
            problems.append(f"unknown Drive group in manifest: {group_name}")
            continue
        remote_list = list_files(access_token, DRIVE_FOLDERS[group_name])
        remote_by_id = {
            str(item.get("id") or ""): item
            for item in remote_list
        }
        remote_name_counts: dict[str, int] = {}
        for item in remote_list:
            remote_name = str(item.get("name") or "")
            remote_name_counts[remote_name] = remote_name_counts.get(remote_name, 0) + 1
        for expected in expected_items:
            file_id = str(expected.get("id") or "")
            name = str(expected.get("name") or "")
            remote = remote_by_id.get(file_id)
            if remote is None:
                problems.append(f"{group_name}/{name}: Drive file id {file_id} not found")
                continue
            if remote_name_counts.get(name, 0) != 1:
                problems.append(
                    f"{group_name}/{name}: expected one Drive file, found "
                    f"{remote_name_counts.get(name, 0)}"
                )
            if str(remote.get("name") or "") != name:
                problems.append(
                    f"{group_name}/{name}: remote name is {remote.get('name')!r}"
                )
            expected_size = int(expected.get("size") or 0)
            remote_size = int(remote.get("size") or 0)
            if expected_size and remote_size != expected_size:
                problems.append(
                    f"{group_name}/{name}: size {remote_size:,}, expected {expected_size:,}"
                )
            expected_md5 = str(expected.get("md5") or "")
            remote_md5 = str(remote.get("md5Checksum") or "")
            if expected_md5 and remote_md5 != expected_md5:
                problems.append(
                    f"{group_name}/{name}: MD5 {remote_md5}, expected {expected_md5}"
                )
            checked += 1
    if problems:
        raise RuntimeError("Drive verification failed:\n" + "\n".join(problems[:100]))
    print(f"[verify] {checked:,} Google Drive runtime files matched by id, size, and MD5")
    return checked


def verify_saved_state(access_token: str) -> int:
    if not STATE_PATH.exists():
        raise FileNotFoundError(f"Drive runtime manifest not found: {STATE_PATH}")
    state = json.loads(STATE_PATH.read_text(encoding="utf-8"))
    print(
        "[verify] checking saved manifest generated at "
        f"{state.get('generated_at') or 'unknown'}; this does not inspect unsynced local staging"
    )
    raw_groups = state.get("files") or {}
    groups = {
        group_name: list((items or {}).values())
        for group_name, items in raw_groups.items()
    }
    return verify_remote_groups(access_token, groups)


def migrate_legacy_files(access_token: str, *, workers: int) -> None:
    legacy_items = list_files(access_token, LEGACY_FOLDER_ID)
    destinations = {
        key: {
            item["name"]
            for item in list_files(access_token, DRIVE_FOLDERS[key])
        }
        for key in (
            "main_history",
            "main_source",
            "obt",
            "documentation",
            "analysis",
        )
    }
    print(f"[legacy] {len(legacy_items):,} items to organize")

    moves: list[tuple[dict[str, Any], str, str]] = []
    for item in legacy_items:
        name = item["name"]
        mime = item.get("mimeType") or ""
        if mime == "application/vnd.google-apps.folder":
            group = "analysis"
        elif mime.startswith("application/vnd.google-apps."):
            group = "documentation"
        elif name == "obt_exception_history.json":
            group = "obt"
        elif name.startswith("dashboard_summary"):
            group = "main_history"
        else:
            group = "main_source"

        new_name = name
        if new_name in destinations[group]:
            stem, suffix = os.path.splitext(name)
            new_name = f"{stem}_legacy_{item['id'][:6]}{suffix}"
        destinations[group].add(new_name)
        moves.append((item, group, new_name))

    def move_one(move: tuple[dict[str, Any], str, str]) -> dict[str, Any]:
        item, group, new_name = move
        name = item["name"]
        body = {"name": new_name} if new_name != name else {}
        request(
            access_token,
            "PATCH",
            f"{DRIVE_API}/files/{item['id']}",
            label=f"move legacy {name}",
            params={
                "addParents": DRIVE_FOLDERS[group],
                "removeParents": LEGACY_FOLDER_ID,
                "fields": "id,name,parents,size,md5Checksum",
            },
            headers={"Content-Type": "application/json; charset=UTF-8"},
            json=body,
        )
        return {"id": item["id"], "name": new_name, "group": group}

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(move_one, move) for move in moves]
        for index, future in enumerate(as_completed(futures), start=1):
            future.result()
            if index == len(futures) or index % 25 == 0:
                print(f"  {index:,}/{len(futures):,} legacy items moved")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--workers",
        type=int,
        default=max(1, min(8, (os.cpu_count() or 4))),
        help="Parallel Drive uploads per folder (default: up to 8).",
    )
    parser.add_argument(
        "--migrate-legacy",
        action="store_true",
        help="Move files from the former shared folder into the new dashboard folders.",
    )
    parser.add_argument(
        "--cleanup-local",
        action="store_true",
        help="Delete local pipeline data only after all Drive groups sync successfully.",
    )
    parser.add_argument(
        "--ensure-cache-2025",
        action="store_true",
        help="Restore and verify output/_cache_2025.parquet from Drive, then exit.",
    )
    parser.add_argument(
        "--ensure-runtime-baseline",
        action="store_true",
        help="Restore the 2025 cache and canonical OBT history from Drive, then exit.",
    )
    parser.add_argument(
        "--backup-obt-history",
        type=Path,
        help="Download and verify the canonical Drive OBT history to this path, then exit.",
    )
    parser.add_argument(
        "--restore-latest-booking-snapshot",
        action="store_true",
        help="Restore the latest dated cache and rebuild its Booking snapshot CSV, then exit.",
    )
    parser.add_argument(
        "--verify-drive",
        action="store_true",
        help="Verify the last synced runtime manifest against current Drive metadata, then exit.",
    )
    args = parser.parse_args()

    access_token = refresh_access_token()
    if args.ensure_cache_2025:
        ensure_historical_cache(access_token)
        return 0
    if args.ensure_runtime_baseline:
        ensure_historical_cache(access_token)
        restore_obt_history(access_token)
        return 0
    if args.backup_obt_history:
        restore_obt_history(access_token, args.backup_obt_history.resolve())
        return 0
    if args.restore_latest_booking_snapshot:
        restore_latest_booking_snapshot(access_token)
        return 0
    if args.verify_drive:
        verify_saved_state(access_token)
        return 0
    ensure_company_reader(access_token)
    local_groups = runtime_groups()
    synced: dict[str, list[dict[str, Any]]] = {}
    for group_name in (
        "main",
        "main_history",
        "main_source",
        "obt",
        "sales",
        "sales_data",
    ):
        synced[group_name] = sync_group(
            access_token,
            group_name=group_name,
            entries=local_groups[group_name],
            workers=args.workers,
        )
    verify_remote_groups(access_token, synced)
    write_state(synced)
    if args.migrate_legacy:
        migrate_legacy_files(access_token, workers=args.workers)
    if args.cleanup_local:
        cleanup_local_data(local_groups)

    actions: dict[str, int] = {}
    for entries in synced.values():
        for item in entries:
            actions[item["action"]] = actions.get(item["action"], 0) + 1
    print("Done: " + ", ".join(f"{key}={value:,}" for key, value in sorted(actions.items())))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
