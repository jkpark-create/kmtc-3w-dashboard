import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts import verify_sales_target_daily as check


class VerifySalesTargetDailyTest(unittest.TestCase):
    def test_current_owner_must_reach_index_and_filter(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            out = root / "output"
            target = root / "dist" / "sales-target"
            out.mkdir(parents=True)
            target.mkdir(parents=True)
            date = "20260928"
            def write(path, payload):
                path.write_text(json.dumps(payload), encoding="utf-8")

            write(out / f"current_customer_owner_target_update_{date}.json", {
                "dataset_id": date, "registered_owner_coverage": "passed",
                "registered_owner_additions": [{"target_tab": "JKT", "resolved_sales": "NINA"}],
            })
            write(out / f"sales_owner_affiliation_{date}.json", {
                "dataset_id": date, "policy": "sales-target-roster-v1",
                "validation": "passed", "dry_run": False,
                "approved_pairs": [["JKT", "NINA"], ["JKT", "JACK"]],
            })
            write(target / "current_customer_owner_addition_check.json", {
                "dataset": date, "verified_missing_active_sales": [],
            })
            write(target / "index.json", {
                "data_date": date, "office_policy": "sales-target-roster-v1",
                "rows": [{"tab": "JKT", "name": name, "row_type": "SALES"}
                         for name in ("NINA", "JACK")],
            })
            write(target / "manifest.json", {
                "data_date": date, "office_policy": "sales-target-roster-v1",
                "salespeople_by_origin": {"JKT": ["NINA", "JACK"]}, "chunk_count": 1,
            })
            write(target / "base2025.json", {"data_date": date})
            (out / f"current_customer_owner_input_audit_{date}.csv").write_text(
                "target_tab,resolved_sales,source_type,match_status,customer_count\n"
                "JKT,NINA,booking_verified_active_owner,verified_booking_owner,1\n"
                "JKT,JACK,contact_point,exact:JACK,2\n", encoding="utf-8")

            with patch.object(check, "RUNTIME", root):
                self.assertEqual(check.verify(date), 0)
                write(target / "index.json", {
                    "data_date": date, "office_policy": "sales-target-roster-v1", "rows": [],
                })
                with self.assertRaisesRegex(ValueError, "does not match"):
                    check.verify(date)
                write(out / f"sales_owner_affiliation_{date}.json", {
                    "dataset_id": date, "policy": "sales-target-roster-v1",
                    "validation": "passed", "dry_run": False,
                    "approved_pairs": [["JKT", "NINA"]],
                })
                write(target / "index.json", {
                    "data_date": date, "office_policy": "sales-target-roster-v1",
                    "rows": [{"tab": "JKT", "name": "NINA", "row_type": "SALES"}],
                })
                write(target / "manifest.json", {
                    "data_date": date, "office_policy": "sales-target-roster-v1",
                    "salespeople_by_origin": {"JKT": ["NINA"]}, "chunk_count": 1,
                })
                with self.assertRaisesRegex(ValueError, "Active owners missing"):
                    check.verify(date)


if __name__ == "__main__":
    unittest.main()
