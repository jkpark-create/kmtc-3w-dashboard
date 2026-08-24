import importlib.util
import os
import sys
import unittest
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
os.environ.setdefault("WORK_DIR", str(ROOT))

spec = importlib.util.spec_from_file_location("daily_3w_dashboard", ROOT / "daily_3w_dashboard.py")
dashboard = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(dashboard)

remap_spec = importlib.util.spec_from_file_location(
    "remap_dashboard_salesman",
    ROOT / "scripts" / "remap_dashboard_salesman.py",
)
remap = importlib.util.module_from_spec(remap_spec)
assert remap_spec.loader is not None
remap_spec.loader.exec_module(remap)

target_spec = importlib.util.spec_from_file_location(
    "build_sales_target_drill_data",
    ROOT / "scripts" / "build_sales_target_drill_data.py",
)
target_builder = importlib.util.module_from_spec(target_spec)
assert target_spec.loader is not None
target_spec.loader.exec_module(target_builder)

updater_spec = importlib.util.spec_from_file_location(
    "update_target_workbook_from_current_customer_owners",
    ROOT / "scripts" / "update_target_workbook_from_current_customer_owners.py",
)
target_updater = importlib.util.module_from_spec(updater_spec)
assert updater_spec.loader is not None
sys.modules[updater_spec.name] = target_updater
updater_spec.loader.exec_module(target_updater)


class NewSalesmanDetectionTest(unittest.TestCase):
    def lookup(self):
        return {
            "exact": {("CN", "SHA", "C1"): "OLDOWNER"},
            "by_country": {},
            "generic": {},
            "unique_customer": {},
            "obt_salesmen": ["OLDOWNER"],
            "active_salesman_keys": {"OLDOWNER"},
        }

    def test_recent_unregistered_salesman_is_preserved(self):
        df = pd.DataFrame(
            {
                "BKG_SHPR_CST_NO": ["C1", "C1", "C1"],
                "POR_CTR_CD": ["CN", "CN", "CN"],
                "POR_PLC_CD": ["SHA", "SHA", "SHA"],
                "Salesman_POR": ["TOMHUANG", "TOMHUANG", "TOMHUANG"],
                "Actual_Departure_schedule": [
                    "2026년 7월 29일",
                    "2026년 8월 2일",
                    "2026년 8월 9일",
                ],
            }
        )
        lookup = self.lookup()

        matched, provisional, raw_fallback, unmatched = dashboard.apply_salesman_mapping(
            df,
            lookup,
            as_of="20260730",
        )

        self.assertEqual(df["Salesman_POR"].tolist(), ["TOMHUANG"] * 3)
        self.assertEqual((matched, provisional, raw_fallback, unmatched), (0, 3, 0, 0))
        self.assertEqual(lookup["provisional_salesmen"], ["TOMHUANG"])
        self.assertEqual(lookup["provisional_obt_salesmen"], ["TOMHUANG"])

    def test_known_or_multi_owner_values_do_not_bypass_roster_mapping(self):
        df = pd.DataFrame(
            {
                "BKG_SHPR_CST_NO": ["C1"] * 6,
                "POR_CTR_CD": ["CN"] * 6,
                "POR_PLC_CD": ["SHA"] * 6,
                "Salesman_POR": ["OLDOWNER"] * 3 + ["TOMHUANG,WENJIE"] * 3,
                "Actual_Departure_schedule": ["2026년 8월 2일"] * 6,
            }
        )
        lookup = self.lookup()

        matched, provisional, raw_fallback, unmatched = dashboard.apply_salesman_mapping(
            df,
            lookup,
            as_of="20260730",
        )

        self.assertEqual(df["Salesman_POR"].tolist(), ["OLDOWNER"] * 6)
        self.assertEqual((matched, provisional, raw_fallback, unmatched), (6, 0, 0, 0))
        self.assertEqual(lookup["provisional_salesmen"], [])

    def test_unassigned_is_not_detected_as_a_provisional_salesperson(self):
        df = pd.DataFrame(
            {
                "BKG_SHPR_CST_NO": ["__INTEGRATED_AGGREGATE__"] * 3,
                "POR_CTR_CD": ["CN"] * 3,
                "POR_PLC_CD": ["SHA"] * 3,
                "Salesman_POR": ["UNASSIGNED"] * 3,
                "Actual_Departure_schedule": ["2026년 8월 2일"] * 3,
            }
        )
        lookup = self.lookup()

        dashboard.apply_salesman_mapping(df, lookup, as_of="20260824")

        self.assertEqual(df["Salesman_POR"].tolist(), [dashboard.MISSING_SALES] * 3)
        self.assertEqual(lookup["provisional_salesmen"], [])

    def test_old_unregistered_value_is_not_treated_as_new(self):
        df = pd.DataFrame(
            {
                "BKG_SHPR_CST_NO": ["C1", "C1", "C1"],
                "POR_CTR_CD": ["CN", "CN", "CN"],
                "POR_PLC_CD": ["SHA", "SHA", "SHA"],
                "Salesman_POR": ["FORMER"] * 3,
                "Actual_Departure_schedule": ["2026년 1월 4일"] * 3,
            }
        )
        lookup = self.lookup()

        dashboard.apply_salesman_mapping(df, lookup, as_of="20260730")

        self.assertEqual(df["Salesman_POR"].tolist(), ["OLDOWNER"] * 3)
        self.assertEqual(lookup["provisional_salesmen"], [])

    def test_postprocess_preserves_provisional_salesman_metadata(self):
        data = {
            "provisional_salesmen": ["TOMHUANG"],
            "shipper": [
                {
                    "BKG_SHPR_CST_NO": "C1",
                    "origin": "CN",
                    "ori_port": "SHA",
                    "Salesman_POR": "TOMHUANG",
                }
            ],
        }

        result = remap.remap_shipper(data, self.lookup(), "(미지정)")

        self.assertEqual(result, (0, 1, 0, 0))
        self.assertEqual(data["shipper"][0]["Salesman_POR"], "TOMHUANG")

    def test_sales_target_summary_adds_recent_provisional_salesman(self):
        df = pd.DataFrame(
            {
                "tab": ["CN_SHA"] * 3,
                "team": ["OBT"] * 3,
                "Salesman_POR": ["TOMHUANG"] * 3,
                "Booking_schedule": ["2026년 8월 2일"] * 3,
                "YYYYMM": ["202608"] * 3,
                "BKG_SHPR_CST_NO": ["A", "B", "C"],
                "fst_teu_num": [1.0, 1.0, 1.0],
                "norm_lst_teu_num": [0.0, 0.0, 0.0],
                "is_w3": [True, True, False],
            }
        )
        rows = [
            {
                "tab": "CN_SHA",
                "name": "Team Total",
                "row_type": "TOTAL",
            },
            {
                "tab": "CN_SHA",
                "name": "OLDOWNER",
                "row_type": "SALES",
            },
        ]

        added = target_builder.add_provisional_salespeople_to_summary(
            rows,
            df,
            {"C1": "OLDOWNER"},
            "20260730",
        )

        self.assertEqual(added, [("CN_SHA", "TOMHUANG")])
        self.assertEqual(rows[-1]["name"], "TOMHUANG")
        self.assertEqual(rows[-1]["source"], "live_booking_provisional")
        self.assertEqual(rows[-1]["accounts"], {"total": 0, "w3": 0, "pct": None})

    def test_sales_target_transfers_provisional_customers_to_2025_basis(self):
        df = pd.DataFrame(
            {
                "tab": ["CN_SHA"] * 5,
                "team": ["OBT"] * 5,
                "Salesman_POR": ["TOMHUANG"] * 4 + ["OLDOWNER"],
                "Booking_schedule": ["2026년 7월 25일"] * 5,
                "YYYYMM": ["202607"] * 5,
                "BKG_SHPR_CST_NO": ["C001", "C001", "C002", "C002", "C003"],
                "fst_teu_num": [1.0] * 5,
                "norm_lst_teu_num": [0.0] * 5,
                "is_w3": [True] * 5,
            }
        )

        mapping = target_builder.provisional_customer_owner_mapping(
            df,
            {"C003": "OLDOWNER"},
            "20260730",
        )

        self.assertEqual(mapping, {"C001": "TOMHUANG", "C002": "TOMHUANG"})

    def test_target_workbook_maps_new_salesperson_customers_for_historical_targets(self):
        frame = pd.DataFrame(
            {
                "Salesman_POR": [
                    "TOMHUANG",
                    "TOMHUANG",
                    "TOMHUANG",
                    "TOMHUANG",
                    "KNOWNREP",
                    "A,B",
                ],
                "BKG_SHPR_CST_NO": ["C001", "C001", "C002", "C002", "C003", "C004"],
                "Booking_date": [
                    "2026년 7월 20일",
                    "2026년 7월 21일",
                    "2026년 7월 22일",
                    "2026년 7월 23일",
                    "2026년 7월 24일",
                    "2026년 7월 25일",
                ],
                "POR_CTR_CD": ["CN"] * 6,
                "POR_PLC_CD": ["SHA"] * 6,
                "DLY_CTR_CD": ["US"] * 6,
            }
        )

        customer_map, by_origin, evidence = target_updater.infer_provisional_customer_owners(
            frame,
            {"KNOWNREP"},
            20260729,
        )

        self.assertEqual(customer_map, {"C001": "TOMHUANG", "C002": "TOMHUANG"})
        self.assertEqual(by_origin, {"CN_SHA": ["TOMHUANG"]})
        self.assertEqual(evidence, {"TOMHUANG": 4})

    def test_target_workbook_appends_provisional_salesperson_to_explicit_roster(self):
        explicit = {
            "CN_SHA": [
                target_updater.OwnerEntry(
                    target_tab="CN_SHA",
                    source_sheet="N.CN",
                    source_cell="R6",
                    input_name="Alex Wu",
                    sales_key="ALEXWU",
                    resolved_sales="WUXIAOCHEN",
                    match_status="exact:WUXIAOCHEN",
                    source_type="contact_point",
                    customer_count=10,
                )
            ]
        }

        result = target_updater.complete_owner_entries(
            ["CN_SHA"],
            explicit,
            {},
            {"WUXIAOCHEN": 10, "TOMHUANG": 2},
            {"CN_SHA": ["TOMHUANG"]},
        )

        self.assertEqual(
            [entry.resolved_sales for entry in result["CN_SHA"]],
            ["WUXIAOCHEN", "TOMHUANG"],
        )
        self.assertEqual(result["CN_SHA"][-1].source_type, "live_booking_provisional")


if __name__ == "__main__":
    unittest.main()
