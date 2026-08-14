import gzip
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))

import integrated_scope_adapter as adapter


class IntegratedScopeAdapterTests(unittest.TestCase):
    def tearDown(self):
        adapter.load_integrated_scope_snapshot.cache_clear()

    def write_fixture(self, root: Path):
        month_dir = root / "months"
        month_dir.mkdir(parents=True)
        payload = {
            "bookingDetailRows": [
                {
                    "bookingNo": "BKG1", "month": "202607", "week": "20260727",
                    "route": "KCV", "routeVesselDepartureDate": "20260705",
                    "routeVesselCode": "JAAA", "routeVoyageNo": "001E",
                    "bookingTeu": 10, "blTeu": 8,
                }
            ],
            "weeklyLaneRows": [
                {
                    "month": "202607", "week": "20260727", "team": "OBT",
                    "origin": "CN", "por": "SHA", "dest": "TH", "dly": "BKK",
                    "bsaTeu": 30,
                },
                {
                    "month": "202607", "week": "20260728", "team": "OBT",
                    "origin": "CN", "por": "SHA", "dest": "TH", "dly": "BKK",
                    "bsaTeu": 40,
                },
            ],
        }
        with gzip.open(month_dir / "202607.json.gz", "wt", encoding="utf-8") as stream:
            json.dump(payload, stream)
        manifest = {
            "sourceMeta": {
                "sourceAsOfDate": "20260814",
                "bsaScope": (
                    "Single LOCAL first-vessel CGO_PFM_CAT_CD='L'; "
                    "BSA_CD='03' SPOT replaces BSA_CD='01' Original"
                ),
                "iccActualScope": (
                    "B/L / SINGLE(TEAM) / individual; first BSA-bearing load segment; "
                    "Booking and actual B/L remain separate"
                ),
            },
            "monthFiles": {"202607": "months/202607.json.gz"},
        }
        (root / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    def test_snapshot_builds_bsa_and_booking_scope(self):
        with tempfile.TemporaryDirectory() as tmp:
            data_root = Path(tmp)
            self.write_fixture(data_root)
            with patch.dict(os.environ, {
                "KMTC_INTEGRATED_DASHBOARD_DATA_ROOT": str(data_root),
                "KMTC_INTEGRATED_SCOPE_MAX_AGE_DAYS": "1",
            }):
                snapshot = adapter.load_integrated_scope_snapshot("20260814", 2026)
        self.assertEqual(snapshot.source_date, "20260814")
        self.assertEqual(snapshot.bsa["WW"].tolist(), ["1", "2"])
        self.assertEqual(snapshot.bsa["TEU_BSA (Actual)"].sum(), 70)
        self.assertEqual(snapshot.booking_scope.iloc[0]["performance_vessel"], "JAAA")

    def test_cutover_preserves_legacy_and_replaces_july(self):
        legacy = pd.DataFrame([
            {"YYYYMM": "202606", "TEU_BSA (Actual)": 10},
            {"YYYYMM": "202607", "TEU_BSA (Actual)": 999},
        ])
        integrated = pd.DataFrame([
            {"YYYYMM": "202607", "TEU_BSA (Actual)": 30},
        ])
        combined, stats = adapter.blend_bsa_cutover(legacy, integrated)
        self.assertEqual(combined["YYYYMM"].tolist(), ["202606", "202607"])
        self.assertEqual(combined["TEU_BSA (Actual)"].tolist(), [10, 30])
        self.assertEqual(stats["integratedBsaTeu"], 30)

    def test_performance_overlay_does_not_touch_booking_schedule(self):
        output = pd.DataFrame([{
            "BKG_NO": "BKG1", "LST_route": "KCV", "LST_VSL": "OLD",
            "LST_VOY": "OLDV", "Actual_Departure_schedule": "2026년 7월 1일",
            "Booking_schedule": "2026년 6월 28일", "LST_Status": "Normal",
            "LST_TEU": 7, "CM1": 70,
        }])
        scope = pd.DataFrame([{
            "BKG_NO": "BKG1", "route": "KCV", "performance_month": "202607",
            "performance_week": "20260727", "performance_departure_date": "20260705",
            "performance_vessel": "JAAA", "performance_voyage": "001E",
            "booking_teu": 10, "bl_teu": 8, "booking_cm1": 100, "bl_cm1": 80,
            "origin": "CN", "por": "SHA", "pol_country": "CN", "pol": "SHA",
            "pod_country": "TH", "pod": "BKK", "dest": "TH", "dly": "BKK",
            "shipper_code": "C1", "shipper_name": "SHIPPER", "salesman": "S1",
            "booking_status": "Normal",
        }])
        result, stats = adapter.apply_booking_performance_scope(output, scope)
        self.assertEqual(result.loc[0, "Actual_Departure_schedule"], "2026년 7월 5일")
        self.assertEqual(result.loc[0, "LST_VSL"], "JAAA")
        self.assertEqual(result.loc[0, "Booking_schedule"], "2026년 6월 28일")
        self.assertEqual(result.loc[0, "LST_TEU"], "8")
        self.assertEqual(stats["matchedRows"], 1)

    def test_missing_canonical_booking_is_added_without_wos_schedule(self):
        output = pd.DataFrame([{
            "BKG_NO": "LEGACY", "LST_route": "OLD", "LST_Status": "Normal",
            "LST_TEU": 5, "CM1": 50,
            "Actual_Departure_schedule": "2026년 7월 12일",
            "Booking_schedule": "2026년 6월 20일",
        }])
        scope = pd.DataFrame([{
            "BKG_NO": "NEW", "route": "KCV", "performance_month": "202607",
            "performance_week": "20260727", "performance_departure_date": "20260705",
            "performance_vessel": "JAAA", "performance_voyage": "001E",
            "booking_teu": 10, "bl_teu": 8, "booking_cm1": 100, "bl_cm1": 80,
            "origin": "CN", "por": "SHA", "pol_country": "CN", "pol": "SHA",
            "pod_country": "TH", "pod": "BKK", "dest": "TH", "dly": "BKK",
            "shipper_code": "C1", "shipper_name": "SHIPPER", "salesman": "S1",
            "booking_status": "Normal",
        }])
        result, stats = adapter.apply_booking_performance_scope(output, scope)
        legacy = result.loc[result["BKG_NO"].eq("LEGACY")].iloc[0]
        synthetic = result.loc[result["BKG_NO"].eq("NEW")].iloc[0]
        self.assertEqual(legacy["LST_TEU"], "0")
        self.assertEqual(legacy["Booking_schedule"], "2026년 6월 20일")
        self.assertEqual(synthetic["Booking_schedule"], "")
        self.assertEqual(synthetic["LST_TEU"], "8")
        self.assertEqual(stats["syntheticRows"], 1)


if __name__ == "__main__":
    unittest.main()
