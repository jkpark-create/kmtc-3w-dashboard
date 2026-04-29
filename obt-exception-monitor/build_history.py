#!/usr/bin/env python3
"""Build compact OBT booking pace history from dist/data.json git commits."""

from __future__ import annotations

import json
import subprocess
from collections import defaultdict
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DIST = ROOT / "dist"
OUT = ROOT / "obt-exception-monitor" / "history.json"


def git(args: list[str]) -> bytes:
    return subprocess.check_output(["git", "-C", str(DIST), *args])


def commits(limit: int = 12) -> list[str]:
    raw = git(["log", f"--max-count={limit}", "--format=%H", "--", "data.json"])
    return [line.strip() for line in raw.decode("utf-8").splitlines() if line.strip()]


def route_snapshot(data: dict) -> list[list]:
    routes = {}

    for row in data.get("shipper", []):
        if row.get("team") != "OBT":
            continue

        origin = str(row.get("origin") or "").strip()
        pol = str(row.get("ori_port") or "").strip()
        dest = str(row.get("dest") or "").strip()
        dst = str(row.get("dst_port") or "").strip()
        week = str(row.get("week_start_date") or "").strip()
        if not (origin and pol and dest and dst and week):
            continue

        key = (f"{origin}|{pol}|{dest}|{dst}", week)
        found = routes.setdefault(key, {"teu": 0.0, "w3": 0.0, "active": set(), "w3_active": set()})
        teu = float(row.get("norm_lst") or row.get("fst") or 0)
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
        if teu <= 0 and w3 <= 0:
            continue
        rows.append([route_key, week, teu, w3, len(values["active"]), len(values["w3_active"])])
    rows.sort(key=lambda item: (item[1], item[0]))
    return rows


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
        })

    snapshots.sort(key=lambda item: item["data_date"])
    history = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source": "dist/data.json git history",
        "snapshots": snapshots,
    }
    OUT.write_text(json.dumps(history, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    print(f"Wrote {OUT} with {len(snapshots)} snapshots")


if __name__ == "__main__":
    main()
