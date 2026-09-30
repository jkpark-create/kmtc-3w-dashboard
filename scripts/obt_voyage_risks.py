"""OBT risk facts at origin/receipt port, service, vessel, voyage and week grain."""
from collections import defaultdict

def build_voyage_calls(rows, week_starts):
    groups = {}
    for row in rows:
        if str(row.get("team", "")).upper() != "OBT":
            continue
        week = str(row.get("week") or "")
        if week not in week_starts:
            continue
        origin, port, route, vessel, voyage, bound = [str(row.get(k) or "").strip() for k in ("origin", "por", "route", "vesselCode", "voyageNo", "bound")]
        if not (origin and port and route and vessel and voyage):
            continue
        key = (week, origin, port, route, vessel, voyage, bound)
        item = groups.setdefault(key, dict(week=week, week_start=week_starts[week], month=week[:6], origin=origin,
            port=port, route=route, vessel_code=vessel, vessel_name=str(row.get("vesselName") or ""),
            voyage_no=voyage, bound=bound, bsa_teu=0.0, booking_teu=0.0, departure_date=""))
        item["bsa_teu"] += float(row.get("bsaTeu") or 0)
        booking = row.get("referencePerformanceTeu")
        item["booking_teu"] += float(row.get("bookingTeu") or 0) if booking is None else float(booking)
        date = str(row.get("revisedDepartureDate") or row.get("departureDate") or "")
        if date and (not item["departure_date"] or date < item["departure_date"]):
            item["departure_date"] = date
    return sorted((row for row in groups.values() if row["bsa_teu"] > 0),
                  key=lambda r: (r["week"], r["origin"], r["port"], r["route"], r["vessel_code"], r["voyage_no"]))
