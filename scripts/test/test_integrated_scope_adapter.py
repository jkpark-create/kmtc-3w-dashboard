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

    def test_default_data_root_is_outside_the_integrated_onedrive_project(self):
        with patch.dict(os.environ, {
            "KMTC_INTEGRATED_DASHBOARD_DATA_ROOT": "",
            "KMTC_INTEGRATED_DASHBOARD_RUNTIME_ROOT": "",
        }):
            data_root = adapter.integrated_data_root()
        self.assertIn("integrated-dashboard-runtime", str(data_root))
        self.assertNotIn("Integrated dashboard project", str(data_root))

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
                    "route": "KCV", "vesselCode": "JAAA", "voyageNo": "001E",
                    "bound": "E", "pol": "SHA", "bsaTeu": 100,
                    "referencePerformanceTeu": 40,
                },
                {
                    "month": "202607", "week": "20260728", "team": "OBT",
                    "origin": "CN", "por": "SHA", "dest": "TH", "dly": "BKK",
                    "bsaTeu": 40,
                },
                {
                    "month": "202607", "week": "20260727", "team": "OBT",
                    "origin": "CN", "por": "NBO", "dest": "TH", "dly": "BKK",
                    "route": "KCV", "vesselCode": "JAAA", "voyageNo": "001E",
                    "bound": "E", "pol": "NBO", "bsaTeu": 80,
                    "referencePerformanceTeu": 30,
                },
                {
                    "month": "202607", "week": "20260727", "team": "OBT",
                    "origin": "CN", "por": "HKG", "dest": "TH", "dly": "BKK",
                    "route": "KCV", "vesselCode": "JAAA", "voyageNo": "001E",
                    "bound": "E", "pol": "HKG", "bsaTeu": 60,
                    "referencePerformanceTeu": 20,
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
            "robMaxRows": [
                {
                    "key": "SHA", "month": "202607", "week": "20260727",
                    "weekLabel": "W27", "route": "KCV", "vesselCode": "JAAA",
                    "voyageNo": "001E", "bound": "E", "headBack": "head",
                    "pol": "SHA", "polSequence": 1, "unusedTeu": 90,
                    "occupancy": 0.5, "departureDate": "20260705",
                    "sourceSnapshotDate": "20260814",
                },
                {
                    "key": "NBO", "month": "202607", "week": "20260727",
                    "weekLabel": "W27", "route": "KCV", "vesselCode": "JAAA",
                    "voyageNo": "001E", "bound": "E", "headBack": "head",
                    "pol": "NBO", "polSequence": 2, "unusedTeu": 50,
                    "occupancy": 0.7, "departureDate": "20260706",
                    "sourceSnapshotDate": "20260814",
                },
                {
                    "key": "HKG", "month": "202607", "week": "20260727",
                    "weekLabel": "W27", "route": "KCV", "vesselCode": "JAAA",
                    "voyageNo": "001E", "bound": "E", "headBack": "head",
                    "pol": "HKG", "polSequence": 3, "unusedTeu": 45,
                    "occupancy": 0.75, "departureDate": "20260707",
                    "sourceSnapshotDate": "20260814",
                },
            ],
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
        self.assertEqual(sorted(snapshot.bsa["WW"].unique().tolist()), ["1", "2"])
        self.assertEqual(snapshot.bsa["TEU_BSA (Actual)"].sum(), 280)
        self.assertEqual(snapshot.booking_scope.iloc[0]["performance_vessel"], "JAAA")
        self.assertEqual(len(snapshot.space_opportunities), 1)
        opportunity = snapshot.space_opportunities[0]
        self.assertEqual(opportunity["previous_port"], "SHA")
        self.assertEqual(opportunity["current_port"], "NBO")
        self.assertEqual(opportunity["prior_unused_bsa_teu"], 60)
        self.assertEqual(opportunity["physical_unused_teu"], 50)
        self.assertEqual(opportunity["reusable_teu"], 50)
        self.assertEqual(snapshot.space_opportunity_meta["candidateVoyages"], 1)
        self.assertEqual(snapshot.space_opportunity_meta["matchedGroups"], 3)

    def test_current_prepared_local_contract_is_accepted(self):
        adapter._validate_source_contract({
            "bsaScope": (
                "Single LOCAL first-vessel CGO_PFM_CAT_CD='L'; "
                "BSA_CD='03' SPOT replaces BSA_CD='01' Original"
            ),
            "iccActualResolvedSourceMode": "sandbox_first_ocean",
            "iccActualSource": (
                "DMSAE_201DS historical weekly snapshots + "
                "DMSAE_201DS_CUR latest prepared LOCAL facts"
            ),
            "iccActualScope": (
                "B/L / SINGLE(TEAM) / individual prepared CGO_PFM_CAT_CD='L' rows; "
                "domestic T/S duplicates are excluded; "
                "Booking and actual B/L remain separate"
            ),
        })

    def test_prepared_local_contract_requires_authoritative_source_mode(self):
        with self.assertRaisesRegex(RuntimeError, "load segment/vessel contract"):
            adapter._validate_source_contract({
                "bsaScope": (
                    "Single LOCAL first-vessel CGO_PFM_CAT_CD='L'; "
                    "BSA_CD='03' SPOT replaces BSA_CD='01' Original"
                ),
                "iccActualResolvedSourceMode": "unknown",
                "iccActualSource": (
                    "DMSAE_201DS historical weekly snapshots + "
                    "DMSAE_201DS_CUR latest prepared LOCAL facts"
                ),
                "iccActualScope": (
                    "B/L / SINGLE(TEAM) / individual prepared CGO_PFM_CAT_CD='L' rows; "
                    "domestic T/S duplicates are excluded; "
                    "Booking and actual B/L remain separate"
                ),
            })

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

    def test_integrated_route_aggregate_uses_explicit_non_customer_sentinel(self):
        output = pd.DataFrame([{
            "BKG_NO": "LEGACY", "LST_route": "OLD", "LST_Status": "Confirm",
            "LST_TEU": 0, "CM1": 0, "Actual_Departure_schedule": "",
            "Booking_schedule": "",
        }])
        scope = pd.DataFrame([{
            "BKG_NO": "DMSAE-AIM-1", "route": "AIM", "performance_month": "202607",
            "performance_week": "20260727", "performance_departure_date": "20260705",
            "performance_vessel": "JAAA", "performance_voyage": "001E",
            "booking_teu": 0, "bl_teu": 39, "booking_cm1": 0, "bl_cm1": 390,
            "origin": "KR", "por": "PNC", "pol_country": "KR", "pol": "PNC",
            "pod_country": "OM", "pod": "SOH", "dest": "OM", "dly": "SOH",
            "shipper_code": "", "shipper_name": "", "salesman": "",
            "booking_status": "Normal",
        }])
        result, _stats = adapter.apply_booking_performance_scope(output, scope)
        aggregate = result.loc[result["BKG_NO"].eq("DMSAE-AIM-1")].iloc[0]
        self.assertEqual(aggregate["BKG_SHPR_CST_NO"], "__INTEGRATED_AGGREGATE__")
        self.assertEqual(aggregate["BKG_SHPR_CST_ENM"], "Integrated route aggregate")
        self.assertEqual(aggregate["Salesman_POR"], "UNASSIGNED")
        self.assertEqual(aggregate["LST_TEU"], "39")
        self.assertEqual(aggregate["Booking_schedule"], "")


if __name__ == "__main__":
    unittest.main()
