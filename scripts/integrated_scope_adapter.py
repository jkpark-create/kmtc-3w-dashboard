"""Bridge the -3W dashboard to the Integrated dashboard extraction contract.

The Integrated dashboard owns the Oracle reconciliation for BSA, Booking and
B/L performance.  This adapter intentionally consumes its published local
snapshot instead of re-implementing the SQL rules in a second project.

Only the performance axis changes at the July 2026 cutover.  The -3W booking
schedule axis is left to ``daily_3w_dashboard.py`` so WOS-3 calculations keep
their existing meaning.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import lru_cache
import gzip
import json
import os
from pathlib import Path
from typing import Any

import pandas as pd


DEFAULT_CUTOVER_MONTH = "202607"
# Fiscal 2026 July starts on Sunday 5 July in the dashboard's 4-4-5 calendar.
DEFAULT_CUTOVER_DATE = "20260705"
DEFAULT_INTEGRATED_ROOT = Path(
    r"C:\Users\JKPARK\OneDrive\Documents\Claude\Integrated dashboard project"
)
SPACE_MIN_PRIOR_UNUSED_BSA_TEU = 30.0
SPACE_MIN_PHYSICAL_UNUSED_TEU = 20.0
SPACE_MIN_REUSABLE_TEU = 20.0

REQUIRED_BSA_SCOPE_MARKERS = (
    "Single LOCAL first-vessel",
    "CGO_PFM_CAT_CD='L'",
    "BSA_CD='03' SPOT replaces BSA_CD='01' Original",
)
REQUIRED_ACTUAL_SCOPE_MARKERS = (
    "B/L / SINGLE(TEAM) / individual",
    "Booking and actual B/L remain separate",
)
REQUIRED_ACTUAL_SCOPE_SEGMENT_ALTERNATIVES = (
    "first BSA-bearing load segment",
    "valid BSA-bearing vessel",
)


def _clean(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    return "" if text.lower() in {"nan", "none", "nat"} else text


def _number(value: Any) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _metric_text(value: Any) -> str:
    number = _number(value)
    if number.is_integer():
        return str(int(number))
    return f"{number:.6f}".rstrip("0").rstrip(".")


def _read_json(path: Path) -> dict[str, Any]:
    if path.suffix.lower() == ".gz":
        with gzip.open(path, "rt", encoding="utf-8") as stream:
            return json.load(stream)
    return json.loads(path.read_text(encoding="utf-8"))


def integrated_root() -> Path:
    return Path(os.environ.get("KMTC_INTEGRATED_DASHBOARD_ROOT", str(DEFAULT_INTEGRATED_ROOT)))


def integrated_data_root() -> Path:
    override = os.environ.get("KMTC_INTEGRATED_DASHBOARD_DATA_ROOT", "").strip()
    if override:
        return Path(override)
    return integrated_root() / "public" / "data" / "oracle-dashboard"


def _validate_source_contract(source_meta: dict[str, Any]) -> None:
    bsa_scope = _clean(source_meta.get("bsaScope"))
    actual_scope = _clean(source_meta.get("iccActualScope"))
    missing = [marker for marker in REQUIRED_BSA_SCOPE_MARKERS if marker not in bsa_scope]
    missing.extend(marker for marker in REQUIRED_ACTUAL_SCOPE_MARKERS if marker not in actual_scope)
    if not any(marker in actual_scope for marker in REQUIRED_ACTUAL_SCOPE_SEGMENT_ALTERNATIVES):
        missing.append("valid BSA-bearing load segment/vessel contract")
    if missing:
        raise RuntimeError(
            "Integrated dashboard source contract is not the expected Single/individual contract; "
            f"missing markers: {', '.join(missing)}"
        )


def _validate_freshness(source_date: str, reference_date: str | None) -> None:
    if not reference_date:
        return
    max_age_days = max(0, int(os.environ.get("KMTC_INTEGRATED_SCOPE_MAX_AGE_DAYS", "1")))
    source_dt = datetime.strptime(source_date, "%Y%m%d")
    reference_dt = datetime.strptime(reference_date, "%Y%m%d")
    age_days = (reference_dt.date() - source_dt.date()).days
    if age_days < 0 or age_days > max_age_days:
        raise RuntimeError(
            "Integrated dashboard snapshot freshness check failed: "
            f"source={source_date}, reference={reference_date}, allowed_age_days={max_age_days}"
        )


def _week_start_by_key(year: int) -> dict[str, datetime]:
    anchors = {
        2025: (datetime(2024, 12, 29), [4, 4, 5, 4, 4, 5, 4, 4, 5, 4, 4, 6]),
        2026: (datetime(2026, 1, 4), [4, 4, 5, 4, 4, 5, 4, 4, 5, 4, 4, 5]),
        2027: (datetime(2027, 1, 3), [4, 4, 5, 4, 4, 5, 4, 4, 5, 4, 4, 5]),
    }
    first_sunday, pattern = anchors[year]
    result: dict[str, datetime] = {}
    week_no = 1
    for month_index, week_count in enumerate(pattern, start=1):
        month = f"{year}{month_index:02d}"
        for _ in range(week_count):
            result[f"{month}{week_no:02d}"] = first_sunday + timedelta(weeks=week_no - 1)
            week_no += 1
    return result


def _month_file_path(data_root: Path, relative: str) -> Path:
    path = data_root / relative
    if path.exists():
        return path
    if path.suffix.lower() != ".gz" and Path(str(path) + ".gz").exists():
        return Path(str(path) + ".gz")
    raise FileNotFoundError(f"Integrated dashboard month payload missing: {path}")


def _choose_booking_scope_rows(rows: list[dict[str, Any]]) -> pd.DataFrame:
    candidates: list[dict[str, Any]] = []
    for row in rows:
        booking_no = _clean(row.get("bookingNo"))
        month = _clean(row.get("month"))
        week = _clean(row.get("week"))
        departure_date = _clean(row.get("routeVesselDepartureDate"))
        if not departure_date and len(week) == 8 and week[:4].isdigit():
            try:
                week_start = _week_start_by_key(int(week[:4])).get(week)
            except KeyError:
                week_start = None
            if week_start:
                departure_date = week_start.strftime("%Y%m%d")
        if not booking_no or month < DEFAULT_CUTOVER_MONTH:
            continue
        candidates.append({
            "BKG_NO": booking_no,
            "route": _clean(row.get("route")),
            "performance_month": month,
            "performance_week": week,
            "performance_departure_date": departure_date,
            "performance_vessel": _clean(row.get("routeVesselCode") or row.get("vesselCode")),
            "performance_voyage": _clean(row.get("routeVoyageNo") or row.get("voyageNo")),
            "booking_teu": _number(row.get("bookingTeu")),
            "bl_teu": _number(row.get("blTeu")),
            "booking_cm1": _number(row.get("bookingCm1")),
            "bl_cm1": _number(row.get("blCm1")),
            "origin": _clean(row.get("origin")),
            "por": _clean(row.get("por")),
            "pol_country": _clean(row.get("polCountry")),
            "pol": _clean(row.get("pol")),
            "pod_country": _clean(row.get("podCountry")),
            "pod": _clean(row.get("pod")),
            "dest": _clean(row.get("dest")),
            "dly": _clean(row.get("dly")),
            "shipper_code": _clean(row.get("shipperCode")),
            "shipper_name": _clean(row.get("shipperName") or row.get("bookingShipperName")),
            "salesman": _clean(row.get("salesman")),
            "booking_status": _clean(row.get("bookingStatusLabel")),
        })
    if not candidates:
        return pd.DataFrame(columns=[
            "BKG_NO", "route", "performance_month", "performance_week",
            "performance_departure_date", "performance_vessel", "performance_voyage",
            "booking_teu", "bl_teu", "booking_cm1", "bl_cm1",
        ])

    frame = pd.DataFrame(candidates)
    frame["_volume"] = frame["booking_teu"] + frame["bl_teu"]
    # The Integrated payload can contain one row per B/L for a small number of
    # bookings.  Keep the dominant BKG+route row for the performance axis.
    dominant = (
        frame.sort_values(
            ["BKG_NO", "route", "_volume", "performance_departure_date"],
            ascending=[True, True, False, False],
        )
        .drop_duplicates(["BKG_NO", "route"], keep="first")
        .drop(columns=["_volume", "booking_teu", "bl_teu", "booking_cm1", "bl_cm1"])
    )
    totals = (
        frame.groupby(["BKG_NO", "route"], as_index=False, dropna=False)[
            ["booking_teu", "bl_teu", "booking_cm1", "bl_cm1"]
        ]
        .sum()
    )
    return dominant.merge(totals, on=["BKG_NO", "route"], how="left").reset_index(drop=True)


def _canonical_bsa_rows(weekly_rows: list[dict[str, Any]]) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    month_weeks: dict[str, set[str]] = {}
    for row in weekly_rows:
        month = _clean(row.get("month"))
        week = _clean(row.get("week"))
        if month >= DEFAULT_CUTOVER_MONTH and week:
            month_weeks.setdefault(month, set()).add(week)
    ordinals = {
        (month, week): index
        for month, weeks in month_weeks.items()
        for index, week in enumerate(sorted(weeks), start=1)
    }

    for row in weekly_rows:
        month = _clean(row.get("month"))
        week = _clean(row.get("week"))
        bsa_teu = _number(row.get("bsaTeu"))
        if month < DEFAULT_CUTOVER_MONTH or not week or bsa_teu == 0:
            continue
        team = _clean(row.get("team")).upper()
        records.append({
            "DLY_Country": _clean(row.get("dest")),
            "DLY_PORT": _clean(row.get("dly")),
            "POR_Country": _clean(row.get("origin")),
            "POR_PORT": _clean(row.get("por")),
            "Sales Team": team,
            "WW": str(ordinals[(month, week)]),
            "YYYYMM": month,
            "TEU_BSA (Actual)": bsa_teu,
            "team": team,
            "Scope_Source": "Integrated Single/individual",
        })
    if not records:
        return pd.DataFrame(columns=[
            "DLY_Country", "DLY_PORT", "POR_Country", "POR_PORT", "Sales Team",
            "WW", "YYYYMM", "TEU_BSA (Actual)", "team", "Scope_Source",
        ])
    frame = pd.DataFrame(records)
    keys = [
        "DLY_Country", "DLY_PORT", "POR_Country", "POR_PORT", "Sales Team",
        "WW", "YYYYMM", "team", "Scope_Source",
    ]
    return frame.groupby(keys, as_index=False, dropna=False)["TEU_BSA (Actual)"].sum()


def _space_reuse_opportunities(
    weekly_rows: list[dict[str, Any]],
    rob_max_rows: list[dict[str, Any]],
    source_meta: dict[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Build conservative previous-POL BSA reuse candidates.

    Booking/BSA is first aggregated at OBT + voyage + POL grain.  A physical
    ROB fact is accepted only on an exact operational key or, for vessel
    substitutions, when Route+Voyage+Bound+POL resolves to one ROB fact.  The
    output keeps one strongest candidate per voyage so the dashboard does not
    repeat the same carry-over opportunity at several later calls.
    """

    exact: dict[tuple[str, ...], dict[str, Any]] = {}
    unique_voyage: dict[tuple[str, ...], dict[str, Any] | None] = {}
    for row in rob_max_rows:
        exact_key = tuple(_clean(row.get(key)).upper() for key in (
            "week", "route", "vesselCode", "voyageNo", "bound", "pol",
        ))
        previous = exact.get(exact_key)
        if previous is None or _number(row.get("polSequence")) > _number(previous.get("polSequence")):
            exact[exact_key] = row

        voyage_key = tuple(_clean(row.get(key)).upper() for key in (
            "week", "route", "voyageNo", "bound", "pol",
        ))
        if voyage_key not in unique_voyage:
            unique_voyage[voyage_key] = row
        else:
            prior = unique_voyage[voyage_key]
            if prior is not None and _clean(prior.get("key")) != _clean(row.get("key")):
                unique_voyage[voyage_key] = None

    source_groups: dict[tuple[str, ...], dict[str, Any]] = {}
    for row in weekly_rows:
        if _clean(row.get("team")).upper() != "OBT":
            continue
        key = tuple(_clean(row.get(field)).upper() for field in (
            "week", "route", "vesselCode", "voyageNo", "bound", "pol",
        ))
        if not key[0] or key[0][:6] < DEFAULT_CUTOVER_MONTH:
            continue
        item = source_groups.setdefault(key, {
            "bsa_teu": 0.0,
            "booking_teu": 0.0,
            "origins": set(),
            "destinations": set(),
            "destination_ports": set(),
        })
        item["bsa_teu"] += _number(row.get("bsaTeu"))
        booking_value = row.get("referencePerformanceTeu")
        if booking_value is None:
            booking_value = row.get("bookingTeu")
        item["booking_teu"] += _number(booking_value)
        origin = _clean(row.get("origin")).upper()
        destination = _clean(row.get("dest")).upper()
        destination_port = _clean(row.get("dly")).upper()
        if origin:
            item["origins"].add(origin)
        if destination:
            item["destinations"].add(destination)
        if destination_port:
            item["destination_ports"].add(destination_port)

    eligible_groups = {
        key: item for key, item in source_groups.items()
        if item["bsa_teu"] > 0 or item["booking_teu"] > 0
    }
    fact_groups: dict[str, dict[str, Any]] = {}
    exact_matches = 0
    unique_matches = 0
    eligible_by_week: dict[str, int] = {}
    matched_by_week: dict[str, int] = {}
    exact_by_week: dict[str, int] = {}
    unique_by_week: dict[str, int] = {}
    for key in eligible_groups:
        eligible_by_week[key[0]] = eligible_by_week.get(key[0], 0) + 1
    for key, item in eligible_groups.items():
        fact = exact.get(key)
        match_basis = "exact"
        if fact is None:
            fact = unique_voyage.get((key[0], key[1], key[3], key[4], key[5]))
            match_basis = "unique_route_voyage"
        if fact is None:
            continue
        if match_basis == "exact":
            exact_matches += 1
            exact_by_week[key[0]] = exact_by_week.get(key[0], 0) + 1
        else:
            unique_matches += 1
            unique_by_week[key[0]] = unique_by_week.get(key[0], 0) + 1
        matched_by_week[key[0]] = matched_by_week.get(key[0], 0) + 1
        fact_key = _clean(fact.get("key")) or "|".join(key)
        target = fact_groups.setdefault(fact_key, {
            "fact": fact,
            "match_basis": match_basis,
            "bsa_teu": 0.0,
            "booking_teu": 0.0,
            "origins": set(),
            "destinations": set(),
            "destination_ports": set(),
        })
        target["bsa_teu"] += item["bsa_teu"]
        target["booking_teu"] += item["booking_teu"]
        target["origins"].update(item["origins"])
        target["destinations"].update(item["destinations"])
        target["destination_ports"].update(item["destination_ports"])
        if match_basis == "exact":
            target["match_basis"] = "exact"

    voyages: dict[tuple[str, ...], list[dict[str, Any]]] = {}
    for item in fact_groups.values():
        fact = item["fact"]
        voyage_key = tuple(_clean(fact.get(field)).upper() for field in (
            "week", "route", "vesselCode", "voyageNo", "bound",
        ))
        voyages.setdefault(voyage_key, []).append(item)

    opportunities: list[dict[str, Any]] = []
    for voyage_key, calls in voyages.items():
        cumulative_bsa = 0.0
        cumulative_booking = 0.0
        previous_port = ""
        candidates: list[dict[str, Any]] = []
        for item in sorted(calls, key=lambda value: (
            _number(value["fact"].get("polSequence")),
            _clean(value["fact"].get("pol")),
        )):
            fact = item["fact"]
            current_port = _clean(fact.get("pol")).upper()
            prior_unused = max(cumulative_bsa - cumulative_booking, 0.0)
            physical_unused = max(_number(fact.get("unusedTeu")), 0.0)
            reusable_teu = min(prior_unused, physical_unused)
            own_gap = max(item["bsa_teu"] - item["booking_teu"], 0.0)
            if (
                previous_port
                and current_port
                and current_port != previous_port
                and prior_unused >= SPACE_MIN_PRIOR_UNUSED_BSA_TEU
                and physical_unused >= SPACE_MIN_PHYSICAL_UNUSED_TEU
                and reusable_teu >= SPACE_MIN_REUSABLE_TEU
            ):
                week = _clean(fact.get("week"))
                week_start = ""
                if len(week) == 8 and week[:4].isdigit():
                    start = _week_start_by_key(int(week[:4])).get(week)
                    if start:
                        week_start = f"{start.year}년 {start.month:02d}월 {start.day:02d}일"
                candidates.append({
                    "month": _clean(fact.get("month")) or week[:6],
                    "week": week,
                    "week_start": week_start,
                    "week_label": _clean(fact.get("weekLabel")),
                    "route": _clean(fact.get("route")),
                    "vessel_code": _clean(fact.get("vesselCode")),
                    "voyage_no": _clean(fact.get("voyageNo")),
                    "bound": _clean(fact.get("bound")),
                    "head_back": _clean(fact.get("headBack")),
                    "previous_port": previous_port,
                    "current_port": current_port,
                    "departure_date": _clean(fact.get("departureDate")),
                    "bsa_teu": item["bsa_teu"],
                    "booking_teu": item["booking_teu"],
                    "own_gap_teu": own_gap,
                    "prior_unused_bsa_teu": prior_unused,
                    "physical_unused_teu": physical_unused,
                    "reusable_teu": reusable_teu,
                    "rob_occupancy": _number(fact.get("occupancy")),
                    "origins": sorted(item["origins"]),
                    "destinations": sorted(item["destinations"]),
                    "destination_ports": sorted(item["destination_ports"]),
                    "match_basis": item["match_basis"],
                    "source_snapshot_date": _clean(fact.get("sourceSnapshotDate")),
                })
            cumulative_bsa += item["bsa_teu"]
            cumulative_booking += item["booking_teu"]
            previous_port = current_port

        if candidates:
            # One action row per voyage: later calls carrying the same unused
            # amount are alternate selling points, not additional capacity.
            opportunities.append(max(candidates, key=lambda row: (
                row["reusable_teu"], row["physical_unused_teu"], row["departure_date"],
            )))

    opportunities.sort(key=lambda row: (
        row["week"], -row["reusable_teu"], row["route"], row["vessel_code"], row["voyage_no"],
    ))
    matched_groups = exact_matches + unique_matches
    coverage_by_week = {}
    for week, eligible_count in sorted(eligible_by_week.items()):
        start_text = ""
        if len(week) == 8 and week[:4].isdigit():
            start = _week_start_by_key(int(week[:4])).get(week)
            if start:
                start_text = f"{start.year}년 {start.month:02d}월 {start.day:02d}일"
        matched_count = matched_by_week.get(week, 0)
        coverage_by_week[week] = {
            "weekStart": start_text,
            "eligibleGroups": eligible_count,
            "matchedGroups": matched_count,
            "exactMatchedGroups": exact_by_week.get(week, 0),
            "uniqueVoyageMatchedGroups": unique_by_week.get(week, 0),
            "matchCoverage": matched_count / eligible_count if eligible_count else 0.0,
        }
    meta = {
        "basis": "OBT BSA/Booking by voyage POL + MAX ROB physical unused; one candidate per voyage",
        "bookingBasis": "referencePerformanceTeu (fallback bookingTeu); B/L is not substituted",
        "physicalSource": _clean(source_meta.get("robMaxSource")),
        "physicalSourceMode": _clean(source_meta.get("robMaxSourceMode")),
        "physicalBasis": _clean(source_meta.get("robMaxUnusedBasis")),
        "eligibleGroups": len(eligible_groups),
        "matchedGroups": matched_groups,
        "exactMatchedGroups": exact_matches,
        "uniqueVoyageMatchedGroups": unique_matches,
        "matchedFactCalls": len(fact_groups),
        "matchCoverage": matched_groups / len(eligible_groups) if eligible_groups else 0.0,
        "coverageByWeek": coverage_by_week,
        "candidateVoyages": len(opportunities),
        "minPriorUnusedBsaTeu": SPACE_MIN_PRIOR_UNUSED_BSA_TEU,
        "minPhysicalUnusedTeu": SPACE_MIN_PHYSICAL_UNUSED_TEU,
        "minReusableTeu": SPACE_MIN_REUSABLE_TEU,
        "deduplication": "strongest later-port candidate per week/route/vessel/voyage/bound",
    }
    return opportunities, meta


@dataclass(frozen=True)
class IntegratedScopeSnapshot:
    source_date: str
    booking_scope: pd.DataFrame
    bsa: pd.DataFrame
    source_meta: dict[str, Any]
    space_opportunities: list[dict[str, Any]]
    space_opportunity_meta: dict[str, Any]


@lru_cache(maxsize=4)
def load_integrated_scope_snapshot(
    reference_date: str | None = None,
    dataset_year: int = 2026,
) -> IntegratedScopeSnapshot:
    data_root = integrated_data_root()
    manifest_path = data_root / "manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(f"Integrated dashboard manifest missing: {manifest_path}")
    manifest = _read_json(manifest_path)
    source_meta = dict(manifest.get("sourceMeta") or {})
    _validate_source_contract(source_meta)
    source_date = _clean(source_meta.get("sourceAsOfDate"))
    if len(source_date) != 8:
        raise RuntimeError("Integrated dashboard manifest has no valid sourceAsOfDate")
    _validate_freshness(source_date, reference_date)

    month_files = manifest.get("monthFiles") or {}
    booking_rows: list[dict[str, Any]] = []
    weekly_rows: list[dict[str, Any]] = []
    for month, relative in sorted(month_files.items()):
        if not str(month).startswith(str(dataset_year)) or str(month) < DEFAULT_CUTOVER_MONTH:
            continue
        payload = _read_json(_month_file_path(data_root, str(relative)))
        booking_rows.extend(payload.get("bookingDetailRows") or [])
        weekly_rows.extend(payload.get("weeklyLaneRows") or [])

    booking_scope = _choose_booking_scope_rows(booking_rows)
    bsa = _canonical_bsa_rows(weekly_rows)
    space_opportunities, space_opportunity_meta = _space_reuse_opportunities(
        weekly_rows,
        list(manifest.get("robMaxRows") or []),
        source_meta,
    )
    if bsa.empty:
        raise RuntimeError(
            f"Integrated dashboard snapshot {source_date} contains no BSA from {DEFAULT_CUTOVER_MONTH}"
        )
    return IntegratedScopeSnapshot(
        source_date,
        booking_scope,
        bsa,
        source_meta,
        space_opportunities,
        space_opportunity_meta,
    )


def blend_bsa_cutover(
    legacy_bsa: pd.DataFrame,
    integrated_bsa: pd.DataFrame,
    cutover_month: str = DEFAULT_CUTOVER_MONTH,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    legacy = legacy_bsa.copy()
    legacy_month = legacy.get("YYYYMM", pd.Series("", index=legacy.index)).astype(str).str.strip()
    legacy = legacy.loc[legacy_month < cutover_month].copy()
    legacy["Scope_Source"] = legacy.get("Scope_Source", "Tableau legacy")
    integrated = integrated_bsa.loc[
        integrated_bsa["YYYYMM"].astype(str).str.strip() >= cutover_month
    ].copy()
    combined = pd.concat([legacy, integrated], ignore_index=True, sort=False)
    stats = {
        "cutoverMonth": cutover_month,
        "legacyRows": int(len(legacy)),
        "integratedRows": int(len(integrated)),
        "integratedBsaTeu": float(pd.to_numeric(
            integrated["TEU_BSA (Actual)"], errors="coerce"
        ).fillna(0).sum()),
    }
    return combined, stats


def apply_booking_performance_scope(
    output: pd.DataFrame,
    booking_scope: pd.DataFrame,
    cutover_month: str = DEFAULT_CUTOVER_MONTH,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    result = output.copy()
    if booking_scope.empty or result.empty:
        return result, {"matchedRows": 0, "eligibleRows": 0, "cutoverMonth": cutover_month}

    mutable_columns = (
        "Actual_Departure_schedule", "LST_route", "LST_VSL", "LST_VOY",
        "LST_Status", "LST_TEU", "CM1", "Performance_Scope_Source",
        "Performance_Week", "Performance_Booking_TEU", "Performance_BL_TEU",
    )
    for column in mutable_columns:
        if column not in result.columns:
            result[column] = ""
        result[column] = result[column].astype(object)

    route_series = result.get("LST_route", pd.Series("", index=result.index)).map(_clean)
    booking_series = result["BKG_NO"].map(_clean)
    exact = {
        (_clean(row.BKG_NO), _clean(row.route)): row
        for row in booking_scope.itertuples(index=False)
    }
    by_booking: dict[str, list[Any]] = {}
    for row in booking_scope.itertuples(index=False):
        by_booking.setdefault(_clean(row.BKG_NO), []).append(row)

    selected: list[Any | None] = []
    for booking_no, route in zip(booking_series, route_series):
        row = exact.get((booking_no, route))
        if row is None:
            candidates = by_booking.get(booking_no, [])
            row = candidates[0] if len(candidates) == 1 else None
        selected.append(row)

    original_indexes = list(result.index)
    matched_scope_keys: set[tuple[str, str]] = set()
    matched = 0
    eligible = 0
    for index, row in zip(original_indexes, selected):
        if row is None or _clean(row.performance_month) < cutover_month:
            continue
        eligible += 1
        departure = _clean(row.performance_departure_date)
        if len(departure) != 8:
            continue
        departure_dt = datetime.strptime(departure, "%Y%m%d")
        result.at[index, "Actual_Departure_schedule"] = (
            f"{departure_dt.year}년 {departure_dt.month}월 {departure_dt.day}일"
        )
        if _clean(row.route):
            result.at[index, "LST_route"] = _clean(row.route)
        if _clean(row.performance_vessel):
            result.at[index, "LST_VSL"] = _clean(row.performance_vessel)
        if _clean(row.performance_voyage):
            result.at[index, "LST_VOY"] = _clean(row.performance_voyage)
        result.at[index, "Performance_Scope_Source"] = "Integrated Single/individual"
        result.at[index, "Performance_Week"] = _clean(row.performance_week)
        result.at[index, "Performance_Booking_TEU"] = _metric_text(row.booking_teu)
        result.at[index, "Performance_BL_TEU"] = _metric_text(row.bl_teu)
        result.at[index, "LST_TEU"] = _metric_text(row.bl_teu)
        result.at[index, "CM1"] = _metric_text(row.bl_cm1)
        if _clean(result.at[index, "LST_Status"]) != "Cancel":
            result.at[index, "LST_Status"] = "Normal" if _number(row.bl_teu) > 0 else "Confirm"
        matched_scope_keys.add((_clean(row.BKG_NO), _clean(row.route)))
        matched += 1

    # Rows in the post-cutover performance window that are absent from the
    # canonical scope must not continue contributing legacy LST/CM1.  Keep the
    # row and its Booking_schedule so historical WOS-3/cancellation logic is
    # still available.
    excluded = 0
    date_pattern = pd.Series(
        result.loc[original_indexes, "Actual_Departure_schedule"], index=original_indexes
    ).astype(str).str.extract(r"(\d{4})\D+(\d{1,2})\D+(\d{1,2})")
    date_keys = (
        date_pattern[0].fillna("")
        + date_pattern[1].fillna("").str.zfill(2)
        + date_pattern[2].fillna("").str.zfill(2)
    )
    for index, row, date_key in zip(original_indexes, selected, date_keys):
        if row is not None or date_key < DEFAULT_CUTOVER_DATE:
            continue
        result.at[index, "LST_TEU"] = "0"
        result.at[index, "CM1"] = "0"
        if _clean(result.at[index, "LST_Status"]) == "Normal":
            result.at[index, "LST_Status"] = "Confirm"
        result.at[index, "Performance_Scope_Source"] = "Integrated excluded"
        result.at[index, "Performance_Booking_TEU"] = "0"
        result.at[index, "Performance_BL_TEU"] = "0"
        excluded += 1

    # Add canonical performance-only bookings that are missing from the
    # Tableau -3W snapshot.  Blank booking dates/schedules ensure these rows
    # never enter a WOS bucket; they exist only to conserve July+ Booking/B/L
    # performance and salesperson attribution.
    synthetic_rows: list[dict[str, Any]] = []
    for row in booking_scope.itertuples(index=False):
        scope_key = (_clean(row.BKG_NO), _clean(row.route))
        if scope_key in matched_scope_keys or _clean(row.performance_month) < cutover_month:
            continue
        departure = _clean(row.performance_departure_date)
        if len(departure) != 8:
            continue
        departure_dt = datetime.strptime(departure, "%Y%m%d")
        schedule_text = f"{departure_dt.year}년 {departure_dt.month}월 {departure_dt.day}일"
        synthetic = {column: "" for column in result.columns}
        synthetic.update({
            "BKG_NO": _clean(row.BKG_NO),
            "BKG_SHPR_CST_NO": _clean(row.shipper_code),
            "BKG_SHPR_CST_ENM": _clean(row.shipper_name),
            "POR_CTR_CD": _clean(row.origin),
            "POR_PLC_CD": _clean(row.por),
            "POL_CTR_CD": _clean(row.pol_country),
            "POL_PORT_CD": _clean(row.pol),
            "POD_CTR_CD": _clean(row.pod_country),
            "POD_PORT_CD": _clean(row.pod),
            "DLY_CTR_CD": _clean(row.dest),
            "DLY_PLC_CD": _clean(row.dly),
            "FST_TEU": "0",
            "Actual_Departure_schedule": schedule_text,
            "LST_Status": "Normal" if _number(row.bl_teu) > 0 else "Confirm",
            "CM1": _metric_text(row.bl_cm1),
            "LST_TEU": _metric_text(row.bl_teu),
            "LST_route": _clean(row.route),
            "LST_VSL": _clean(row.performance_vessel),
            "LST_VOY": _clean(row.performance_voyage),
            "Salesman_POR": _clean(row.salesman),
            "Performance_Scope_Source": "Integrated Single/individual synthetic",
            "Performance_Week": _clean(row.performance_week),
            "Performance_Booking_TEU": _metric_text(row.booking_teu),
            "Performance_BL_TEU": _metric_text(row.bl_teu),
        })
        synthetic_rows.append(synthetic)
    if synthetic_rows:
        result = pd.concat([result, pd.DataFrame(synthetic_rows)], ignore_index=True, sort=False)

    stats = {
        "cutoverMonth": cutover_month,
        "scopeRows": int(len(booking_scope)),
        "eligibleRows": eligible,
        "matchedRows": matched,
        "excludedLegacyRows": excluded,
        "syntheticRows": int(len(synthetic_rows)),
        "unmatchedRows": int(len(original_indexes) - matched),
    }
    return result, stats
