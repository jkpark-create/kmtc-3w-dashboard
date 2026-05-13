#!/usr/bin/env python3
"""Build compact OBT booking pace history from dist/data.json git commits."""

from __future__ import annotations

import json
import re
import subprocess
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DIST = ROOT / "dist"
SOURCE_OUT = ROOT / "obt-exception-monitor" / "history.json"
DEPLOY_OUT = DIST / "obt-exception-monitor" / "history.json"


def git(args: list[str]) -> bytes:
    return subprocess.check_output(["git", "-C", str(DIST), *args])


def commits(limit: int = 12) -> list[str]:
    raw = git(["log", f"--max-count={limit}", "--format=%H", "--", "data.json"])
    return [line.strip() for line in raw.decode("utf-8").splitlines() if line.strip()]


def parse_week_date(value: str):
    match = re.search(r"(\d{4})\D+(\d{1,2})\D+(\d{1,2})", str(value or ""))
    if not match:
        return None
    year, month, day = (int(v) for v in match.groups())
    return date(year, month, day)


def business_week(day: date) -> int:
    return ((day - date(day.year, 1, 1)).days // 7) + 1


def iter_section(data: dict, section: str):
    """Yield dict rows, supporting legacy list-of-dicts and compact columns-v1 (`{c, r}`) layouts."""
    raw = data.get(section)
    if isinstance(raw, list):
        for row in raw:
            if isinstance(row, dict):
                yield row
        return
    if isinstance(raw, dict):
        columns = raw.get("c") or []
        for row in raw.get("r") or []:
            if isinstance(row, list):
                yield dict(zip(columns, row))
            elif isinstance(row, dict):
                yield row


def route_snapshot(data: dict) -> list[list]:
    routes = {}
    bsa_map = defaultdict(float)
    week_label_by_no = {}

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
        bsa_map[(f"{origin}|{pol}|{dest}|{dst}", week)] += float(row.get("teu_bsa") or 0)

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

        key = (f"{origin}|{pol}|{dest}|{dst}", week)
        found = routes.setdefault(key, {"teu": 0.0, "w3": 0.0, "active": set(), "w3_active": set()})
        teu = float(row.get("fst") or 0)
        w3 = float(row.get("w3_fst") or 0)
        shipper = str(row.get("BKG_SHPR_CST_NO") or row.get("BKG_SHPR_CST_ENM") or "").strip()

        found["teu"] += teu
        found["w3"] += w3
        if shipper and teu > 0:
            found["active"].add(shipper)
        if shipper and w3 > 0:
            found["w3_active"].add(shipper)

    rows = []
    for (route_key, week), values in routes.items():
        teu = round(values["teu"], 2)
        w3 = round(values["w3"], 2)
        bsa = round(bsa_map.get((route_key, week), 0.0), 2)
        if teu <= 0 and w3 <= 0:
            continue
        rows.append([route_key, week, teu, w3, len(values["active"]), len(values["w3_active"]), bsa])
    rows.sort(key=lambda item: (item[1], item[0]))
    return rows


def target_action_weeks(data_date: str) -> set[str]:
    if not data_date or len(data_date) != 8:
        return set()
    current = datetime.strptime(data_date, "%Y%m%d").date()
    week_start = current - timedelta(days=(current.weekday() + 1) % 7)
    weeks = set()
    for offset in (1, 2, 3):
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
        found["teu"] += float(row.get("fst") or 0)
        found["w3"] += float(row.get("w3_fst") or 0)

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


def main() -> None:
    seen_dates = set()
    snapshots = []

    for commit in commits():
        payload = git(["show", f"{commit}:data.json"])
        data = json.loads(payload)
        data_date = str(data.get("data_date") or "")
        if not data_date or data_date in seen_dates:
            continue
        seen_dates.add(data_date)
        snapshots.append({
            "data_date": data_date,
            "commit": commit[:7],
            "routes": route_snapshot(data),
            "shippers": shipper_snapshot(data, target_action_weeks(data_date)),
        })

    snapshots.sort(key=lambda item: item["data_date"])
    history = {
        "generated_at": existing_generated_at(snapshots) or datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source": "dist/data.json git history",
        "snapshots": snapshots,
    }
    payload = json.dumps(history, ensure_ascii=False, separators=(",", ":"))
    for out in (SOURCE_OUT, DEPLOY_OUT):
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(payload, encoding="utf-8")
        print(f"Wrote {out} with {len(snapshots)} snapshots")


if __name__ == "__main__":
    main()
