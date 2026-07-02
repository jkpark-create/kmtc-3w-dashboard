#!/usr/bin/env python3
"""Build compact OBT booking pace history for the exception monitor.

The monitor's same-weekday pace logic needs a durable daily ledger, not only
whatever recent git commits happen to be in the latest rebuild window. This
script merges existing history, dist/data.json git history, and the current
working-tree dist/data.json snapshot, then keeps enough days for the 100-day
"3-month average" same-weekday window used by the frontend.

History rows are intentionally limited to W+1 through W+4 relative to each
snapshot date. W+1..W+3 feed the same-weekday benchmark, and W+4 lets the
frontend compare today's W+3 target against the same target captured 3-7 days
earlier.
"""

from __future__ import annotations

import gzip
import json
import math
import os
import re
import subprocess
from collections import defaultdict
from contextlib import suppress
from datetime import date, datetime, timedelta
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DIST = ROOT / "dist"
SOURCE_OUT = ROOT / "obt-exception-monitor" / "history.json"
DEPLOY_OUT = DIST / "obt-exception-monitor" / "history.json"
DATA_PATH = DIST / "data.json"
DATA_GZIP_PATH = DIST / "data.json.gz"
GIT_DATA_PATHS = ("data.json", "data.json.gz")
CURRENT_DATA_PATHS = (DATA_PATH, DATA_GZIP_PATH)
HISTORY_SCHEMA = 2
GIT_COMMIT_LIMIT = int(os.environ.get("OBT_HISTORY_GIT_COMMIT_LIMIT", "240"))
HISTORY_RETENTION_DAYS = int(os.environ.get("OBT_HISTORY_RETENTION_DAYS", "120"))
HISTORY_MAX_LEAD_OFFSET = int(os.environ.get("OBT_HISTORY_MAX_LEAD_OFFSET", "4"))
DEPLOY_HISTORY_MAX_BYTES = int(os.environ.get("OBT_HISTORY_DEPLOY_MAX_BYTES", "90000000"))
REBUILD_EXISTING_DATES = os.environ.get("OBT_HISTORY_REBUILD_EXISTING_DATES", "0") == "1"
DATA_DATE_RE = re.compile(rb'"data_date"\s*:\s*"(\d{8})"')


def git(args: list[str]) -> bytes:
    return subprocess.check_output(["git", "-C", str(DIST), *args], stderr=subprocess.PIPE)


def git_blob_exists(commit: str, path: str) -> bool:
    return subprocess.run(
        ["git", "-C", str(DIST), "cat-file", "-e", f"{commit}:{path}"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    ).returncode == 0


def git_blob_prefix(commit: str, path: str, size: int = 8192) -> bytes:
    try:
        process = subprocess.Popen(
            ["git", "-C", str(DIST), "show", f"{commit}:{path}"],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        )
    except OSError as exc:
        print(f"WARNING: cannot read git blob prefix for {commit[:7]}: {exc}")
        return b""
    try:
        return process.stdout.read(size) if process.stdout else b""
    finally:
        with suppress(ProcessLookupError):
            process.kill()
        process.communicate()


def git_gzip_blob_prefix(commit: str, path: str, size: int = 8192) -> bytes:
    try:
        process = subprocess.Popen(
            ["git", "-C", str(DIST), "show", f"{commit}:{path}"],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        )
    except OSError as exc:
        print(f"WARNING: cannot read git blob prefix for {commit[:7]}: {exc}")
        return b""
    try:
        if not process.stdout:
            return b""
        with gzip.GzipFile(fileobj=process.stdout) as gz:
            return gz.read(size)
    except (OSError, EOFError):
        return b""
    finally:
        with suppress(ProcessLookupError):
            process.kill()
        process.communicate()


def git_data_blob_prefix(commit: str, size: int = 8192) -> bytes:
    for path in GIT_DATA_PATHS:
        if not git_blob_exists(commit, path):
            continue
        if path.endswith(".gz"):
            return git_gzip_blob_prefix(commit, path, size)
        return git_blob_prefix(commit, path, size)
    return b""


def read_git_data_blob(commit: str) -> tuple[str, bytes] | None:
    for path in GIT_DATA_PATHS:
        if not git_blob_exists(commit, path):
            continue
        payload = git(["show", f"{commit}:{path}"])
        if path.endswith(".gz"):
            payload = gzip.decompress(payload)
        return path, payload
    return None


def commits(limit: int = GIT_COMMIT_LIMIT) -> list[str]:
    try:
        raw = git(["log", f"--max-count={limit}", "--format=%H", "--", *GIT_DATA_PATHS])
    except (OSError, subprocess.CalledProcessError) as exc:
        print(f"WARNING: cannot read dist/data.json(.gz) git history: {exc}")
        return []
    seen = set()
    out = []
    for line in raw.decode("utf-8").splitlines():
        commit = line.strip()
        if not commit or commit in seen:
            continue
        seen.add(commit)
        out.append(commit)
    return out


def parse_week_date(value: str):
    match = re.search(r"(\d{4})\D+(\d{1,2})\D+(\d{1,2})", str(value or ""))
    if not match:
        return None
    year, month, day = (int(v) for v in match.groups())
    return date(year, month, day)


def business_week(day: date) -> int:
    return ((day - date(day.year, 1, 1)).days // 7) + 1


def number(value) -> float:
    try:
        found = float(str(value).replace(",", "")) if value not in (None, "") else 0.0
    except (TypeError, ValueError):
        return 0.0
    return found if math.isfinite(found) else 0.0


def iter_section(data: dict, section: str):
    """Yield dict rows, supporting legacy list-of-dicts and compact columns-v1 (`{c, r}`) layouts."""
    raw = data.get(section)
    if isinstance(raw, list):
        for row in raw:
            if isinstance(row, dict):
                yield row
        return
    if isinstance(raw, dict):
        columns = raw.get("c") or raw.get("columns") or []
        dicts = raw.get("d") or raw.get("dicts") or {}
        for row in raw.get("r") or []:
            if isinstance(row, list):
                out = {}
                for idx, column in enumerate(columns):
                    if idx >= len(row):
                        continue
                    value = row[idx]
                    dictionary = dicts.get(column)
                    if isinstance(dictionary, list) and isinstance(value, int) and 0 <= value < len(dictionary):
                        value = dictionary[value]
                    out[column] = value
                yield out
            elif isinstance(row, dict):
                yield row


def route_snapshot(data: dict, target_weeks: set[str] | None = None) -> list[list]:
    routes = {}
    bsa_map = defaultdict(float)
    week_label_by_no = {}
    week_filter = set(target_weeks or [])

    for section in ("weekly", "shipper"):
        for row in iter_section(data, section):
            week = str(row.get("week_start_date") or row.get("week") or "").strip()
            week_day = parse_week_date(week)
            if week_day and week not in week_label_by_no.values():
                week_label_by_no.setdefault(business_week(week_day), week)

    for row in iter_section(data, "bsa"):
        if row.get("team") != "OBT":
            continue

        origin = str(row.get("origin") or "").strip()
        pol = str(row.get("POR_PORT") or row.get("ori_port") or "").strip()
        dest = str(row.get("dest") or "").strip()
        dst = str(row.get("DLY_PORT") or row.get("dst_port") or "").strip()
        ww = str(row.get("WW") or "").strip()
        yyyymm = str(row.get("YYYYMM") or "").strip()
        if not (origin and pol and dest and dst and ww.isdigit() and len(yyyymm) >= 4):
            continue

        week = week_label_by_no.get(int(ww))
        if not week:
            year = int(yyyymm[:4])
            week_start = datetime.strptime(f"{year}-01-01", "%Y-%m-%d").date() + timedelta(days=(int(ww) - 1) * 7)
            week = f"{week_start.year}년 {week_start.month:02d}월 {week_start.day:02d}일"
        if week_filter and week not in week_filter:
            continue
        bsa_map[(f"{origin}|{pol}|{dest}|{dst}", week)] += number(row.get("teu_bsa"))

    for row in iter_section(data, "shipper"):
        if row.get("team") != "OBT":
            continue

        origin = str(row.get("origin") or "").strip()
        pol = str(row.get("ori_port") or "").strip()
        dest = str(row.get("dest") or "").strip()
        dst = str(row.get("dst_port") or "").strip()
        week = str(row.get("week_start_date") or row.get("week") or "").strip()
        if not (origin and pol and dest and dst and week):
            continue
        if week_filter and week not in week_filter:
            continue

        key = (f"{origin}|{pol}|{dest}|{dst}", week)
        found = routes.setdefault(key, {"teu": 0.0, "w3": 0.0, "active": set(), "w3_active": set()})
        teu = number(row.get("fst"))
        w3 = number(row.get("w3_fst"))
        shipper = str(row.get("BKG_SHPR_CST_NO") or row.get("BKG_SHPR_CST_ENM") or "").strip()

        found["teu"] += teu
        found["w3"] += w3
        if shipper and teu > 0:
            found["active"].add(shipper)
        if shipper and w3 > 0:
            found["w3_active"].add(shipper)

    rows = []
    route_week_keys = set(routes) | set(bsa_map)
    for (route_key, week) in route_week_keys:
        values = routes.get((route_key, week), {"teu": 0.0, "w3": 0.0, "active": set(), "w3_active": set()})
        teu = round(values["teu"], 2)
        w3 = round(values["w3"], 2)
        bsa = round(bsa_map.get((route_key, week), 0.0), 2)
        if teu <= 0 and w3 <= 0 and bsa <= 0:
            continue
        rows.append([route_key, week, teu, w3, len(values["active"]), len(values["w3_active"]), bsa])
    rows.sort(key=lambda item: (item[1], item[0]))
    return rows


def target_history_weeks(data_date: str, max_offset: int = HISTORY_MAX_LEAD_OFFSET) -> set[str]:
    if not data_date or not re.fullmatch(r"\d{8}", data_date):
        return set()
    current = datetime.strptime(data_date, "%Y%m%d").date()
    week_start = current - timedelta(days=(current.weekday() + 1) % 7)
    weeks = set()
    for offset in range(1, max(1, max_offset) + 1):
        target = week_start + timedelta(days=offset * 7)
        weeks.add(f"{target.year}년 {target.month:02d}월 {target.day:02d}일")
    return weeks


def shipper_snapshot(data: dict, target_weeks: set[str]) -> list[list]:
    shippers = {}

    for row in iter_section(data, "shipper"):
        if row.get("team") != "OBT":
            continue

        origin = str(row.get("origin") or "").strip()
        pol = str(row.get("ori_port") or "").strip()
        dest = str(row.get("dest") or "").strip()
        dst = str(row.get("dst_port") or "").strip()
        week = str(row.get("week_start_date") or row.get("week") or "").strip()
        if target_weeks and week not in target_weeks:
            continue
        sales = str(row.get("Salesman_POR") or "미지정").strip()
        shipper = str(row.get("BKG_SHPR_CST_NO") or row.get("BKG_SHPR_CST_ENM") or "").strip()
        if not (origin and pol and dest and dst and week and shipper):
            continue

        route_key = f"{origin}|{pol}|{dest}|{dst}"
        key = (shipper, route_key, week, sales)
        found = shippers.setdefault(key, {"teu": 0.0, "w3": 0.0})
        found["teu"] += number(row.get("fst"))
        found["w3"] += number(row.get("w3_fst"))

    rows = []
    for (shipper, route_key, week, sales), values in shippers.items():
        teu = round(values["teu"], 2)
        w3 = round(values["w3"], 2)
        if teu <= 0 and w3 <= 0:
            continue
        rows.append([shipper, route_key, week, sales, teu, w3])
    rows.sort(key=lambda item: (item[2], item[1], item[0], item[3]))
    return rows


def existing_generated_at(snapshots: list[dict]) -> str | None:
    for path in (DEPLOY_OUT, SOURCE_OUT):
        if not path.exists():
            continue
        try:
            existing = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if existing.get("snapshots") == snapshots:
            return existing.get("generated_at")
    return None


def snapshot_from_data(data: dict, commit: str, source: str) -> dict | None:
    data_date = str(data.get("data_date") or "")
    if not data_date:
        return None
    target_weeks = target_history_weeks(data_date)
    routes = route_snapshot(data, target_weeks)
    shippers = shipper_snapshot(data, target_weeks)
    if not routes and not shippers:
        return None
    return {
        "schema": HISTORY_SCHEMA,
        "data_date": data_date,
        "commit": commit,
        "source": source,
        "lead_offsets": list(range(1, max(1, HISTORY_MAX_LEAD_OFFSET) + 1)),
        "routes": routes,
        "shippers": shippers,
    }


def read_json(path: Path) -> dict | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def read_dashboard_data(path: Path) -> dict | None:
    try:
        payload = path.read_bytes()
        if path.name.endswith(".gz"):
            payload = gzip.decompress(payload)
        return json.loads(payload)
    except (OSError, EOFError, json.JSONDecodeError):
        return None


def read_current_dashboard_data() -> tuple[dict | None, str]:
    for path in CURRENT_DATA_PATHS:
        data = read_dashboard_data(path)
        if data:
            return data, f"dist/{path.name}"
    return None, "dist/data.json"


def valid_snapshot(snapshot: object) -> bool:
    return (
        isinstance(snapshot, dict)
        and snapshot.get("schema") == HISTORY_SCHEMA
        and bool(str(snapshot.get("data_date") or ""))
        and isinstance(snapshot.get("routes"), list)
        and isinstance(snapshot.get("shippers"), list)
        and bool(snapshot.get("routes") or snapshot.get("shippers"))
    )


def load_existing_snapshots() -> list[dict]:
    snapshots: list[dict] = []
    seen = set()
    for path in (DEPLOY_OUT, SOURCE_OUT):
        history = read_json(path)
        if not history:
            continue
        for snapshot in history.get("snapshots") or []:
            if not valid_snapshot(snapshot):
                continue
            data_date = str(snapshot.get("data_date") or "")
            if data_date in seen:
                continue
            seen.add(data_date)
            snapshots.append(snapshot)
    return snapshots


def current_dist_commit() -> str:
    try:
        return git(["rev-parse", "--short=12", "HEAD"]).decode("utf-8").strip()
    except (OSError, subprocess.CalledProcessError):
        return "working-tree"


def parse_data_date(value: str) -> date | None:
    text = str(value or "")
    if len(text) != 8 or not text.isdigit():
        return None
    return date(int(text[:4]), int(text[4:6]), int(text[6:8]))


def data_date_from_bytes(payload: bytes) -> str:
    match = DATA_DATE_RE.search(payload)
    return match.group(1).decode("ascii") if match else ""


def prune_snapshots(snapshots: list[dict], retention_days: int = HISTORY_RETENTION_DAYS) -> list[dict]:
    dated = [(parse_data_date(str(snapshot.get("data_date") or "")), snapshot) for snapshot in snapshots]
    valid_dates = [day for day, _snapshot in dated if day]
    if not valid_dates:
        return sorted(snapshots, key=lambda item: str(item.get("data_date") or ""))
    latest = max(valid_dates)
    cutoff = latest - timedelta(days=retention_days)
    kept = [
        snapshot
        for day, snapshot in dated
        if day is None or day >= cutoff
    ]
    return sorted(kept, key=lambda item: str(item.get("data_date") or ""))


def merge_snapshot(by_date: dict[str, dict], priorities: dict[str, int], snapshot: dict | None, priority: int) -> None:
    if not snapshot or not valid_snapshot(snapshot):
        return
    data_date = str(snapshot.get("data_date") or "")
    if priority > priorities.get(data_date, -1):
        by_date[data_date] = snapshot
        priorities[data_date] = priority


def snapshot_content_equal(left: dict | None, right: dict | None) -> bool:
    if not left or not right:
        return False
    keys = ("schema", "data_date", "lead_offsets", "routes", "shippers")
    return all(left.get(key) == right.get(key) for key in keys)


def history_payload(snapshots: list[dict], generated_at: str | None = None) -> str:
    history = {
        "schema": HISTORY_SCHEMA,
        "generated_at": generated_at or existing_generated_at(snapshots) or datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source": f"existing history + dist/data.json(.gz) git/current; retention {HISTORY_RETENTION_DAYS} days; W+1..W+{HISTORY_MAX_LEAD_OFFSET}",
        "snapshots": snapshots,
    }
    return json.dumps(history, ensure_ascii=False, separators=(",", ":"))


def fit_snapshots_to_bytes(snapshots: list[dict], max_bytes: int, generated_at: str) -> tuple[list[dict], int, int]:
    if max_bytes <= 0:
        payload_size = len(history_payload(snapshots, generated_at).encode("utf-8"))
        return snapshots, 0, payload_size

    kept = list(snapshots)
    trimmed = 0
    while kept:
        payload_size = len(history_payload(kept, generated_at).encode("utf-8"))
        if payload_size <= max_bytes:
            return kept, trimmed, payload_size
        kept = kept[1:]
        trimmed += 1
    payload_size = len(history_payload(kept, generated_at).encode("utf-8"))
    return kept, trimmed, payload_size


def write_text_atomic(path: Path, payload: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f"{path.name}.tmp")
    tmp_path.write_text(payload, encoding="utf-8")
    tmp_path.replace(path)


def main() -> None:
    by_date: dict[str, dict] = {}
    priorities: dict[str, int] = {}

    for snapshot in load_existing_snapshots():
        merge_snapshot(by_date, priorities, snapshot, 10)
    print(f"Loaded {len(by_date)} existing snapshots")

    current_data, current_source = read_current_dashboard_data()
    current_day = parse_data_date(str(current_data.get("data_date") or "")) if current_data else None
    cutoff_day = current_day - timedelta(days=HISTORY_RETENTION_DAYS) if current_day else None

    backfilled = 0
    seen_git_dates = set()
    for commit in commits():
        try:
            data_date = data_date_from_bytes(git_data_blob_prefix(commit))
            if data_date and data_date in seen_git_dates:
                continue
            day = parse_data_date(data_date) if data_date else None
            if day and cutoff_day and day < cutoff_day:
                continue
            if data_date and data_date in by_date and not REBUILD_EXISTING_DATES:
                seen_git_dates.add(data_date)
                continue
            blob = read_git_data_blob(commit)
            if blob is None:
                continue
            blob_path, payload = blob
            if not data_date:
                data_date = data_date_from_bytes(payload)
            if data_date and data_date in seen_git_dates:
                continue
            day = parse_data_date(data_date) if data_date else None
            if day and cutoff_day and day < cutoff_day:
                continue
            if data_date and data_date in by_date and not REBUILD_EXISTING_DATES:
                seen_git_dates.add(data_date)
                continue
            if data_date:
                seen_git_dates.add(data_date)
            data = json.loads(payload)
            snapshot = snapshot_from_data(data, commit[:7], f"dist/{blob_path} git")
        except (OSError, EOFError, subprocess.CalledProcessError, json.JSONDecodeError, UnicodeDecodeError, ValueError) as exc:
            print(f"WARNING: skipping git snapshot {commit[:7]}: {exc}")
            continue
        merge_snapshot(by_date, priorities, snapshot, 20)
        if snapshot:
            backfilled += 1
            print(f"Backfilled {snapshot['data_date']} from git {commit[:7]}")

    if current_data:
        snapshot = snapshot_from_data(current_data, current_dist_commit(), f"{current_source} current")
        if snapshot:
            existing = by_date.get(str(snapshot.get("data_date") or ""))
            if snapshot_content_equal(existing, snapshot):
                print(f"Current snapshot {snapshot['data_date']} unchanged; kept existing metadata")
            else:
                merge_snapshot(by_date, priorities, snapshot, 30)
                print(f"Added current snapshot {snapshot['data_date']}")

    snapshots = prune_snapshots(list(by_date.values()))
    if not snapshots:
        print("WARNING: no usable OBT history snapshots; existing history files were not changed")
        return

    generated_at = existing_generated_at(snapshots) or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    source_payload = history_payload(snapshots, generated_at)
    write_text_atomic(SOURCE_OUT, source_payload)
    print(
        f"Wrote {SOURCE_OUT} with {len(snapshots)} snapshots "
        f"({len(source_payload.encode('utf-8')):,} bytes, backfilled {backfilled})"
    )

    deploy_generated_at = existing_generated_at(snapshots) or generated_at
    deploy_snapshots, trimmed, deploy_size = fit_snapshots_to_bytes(
        snapshots,
        DEPLOY_HISTORY_MAX_BYTES,
        deploy_generated_at,
    )
    deploy_payload = history_payload(deploy_snapshots, existing_generated_at(deploy_snapshots) or deploy_generated_at)
    write_text_atomic(DEPLOY_OUT, deploy_payload)
    trim_note = f", trimmed {trimmed} old snapshots for deploy cap" if trimmed else ""
    print(
        f"Wrote {DEPLOY_OUT} with {len(deploy_snapshots)} snapshots "
        f"({deploy_size:,} bytes{trim_note}, backfilled {backfilled})"
    )


if __name__ == "__main__":
    main()
