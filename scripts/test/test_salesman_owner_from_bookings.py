from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
for path in (str(ROOT), str(ROOT / "scripts")):
    if path not in sys.path:
        sys.path.insert(0, path)


def load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


owners = load("owners_from_bookings", ROOT / "scripts" / "refresh_salesman_owners_from_bookings.py")
published = load("tableau_published_query", ROOT / "scripts" / "tableau_published_query.py")


EXPORT_HEADER = (
    "Booking_Shipper_code,Booking_status,POR_Country,POR_PORT,Salesman_POR,"
    "최대값 BkgDtNum,합계 BkgCount\n"
)


def observations(*rows: str) -> list[dict[str, object]]:
    return owners.parse_owner_export(EXPORT_HEADER + "".join(rows))


class OwnerExportParsingTests(unittest.TestCase):
    def test_thousands_separators_and_captions_are_handled(self) -> None:
        parsed = observations('C001,Normal,TH,BKK,SALINA,"20,260,914","12"\n')
        self.assertEqual(parsed, [{
            "customer": "C001", "salesman": "SALINA", "country": "TH", "port": "BKK",
            "last_dt": 20260914, "bookings": 12,
        }])

    def test_repeated_dimension_rows_do_not_inflate_counts(self) -> None:
        """Tableau repeats each combination once per measure; counts must not double."""
        parsed = observations(
            "C001,Normal,TH,BKK,SALINA,20260914,1\n",
            "C001,Normal,TH,BKK,SALINA,20260914,1\n",
        )
        self.assertEqual(len(parsed), 1)
        self.assertEqual(parsed[0]["bookings"], 1)
        chosen, skipped = owners.choose_current_owners(observations(
            "C001,Normal,TH,BKK,BUSY,20260913,40\n",
            "C001,Normal,TH,BKK,BUSY,20260913,40\n",
            "C001,Normal,TH,BKK,STRAY,20260914,1\n",
            "C001,Normal,TH,BKK,STRAY,20260914,1\n",
        ))
        self.assertEqual(chosen, {})
        self.assertEqual(skipped["weak_evidence"], 1)

    def test_conflicting_duplicate_values_are_rejected(self) -> None:
        with self.assertRaises(ValueError):
            observations(
                "C001,Normal,TH,BKK,SALINA,20260914,3\n",
                "C001,Normal,TH,BKK,SALINA,20260901,9\n",
            )

    def test_cancelled_shared_and_blank_owners_are_ignored(self) -> None:
        parsed = observations(
            "C001,Cancel,TH,BKK,SALINA,20260914,3\n",
            "C002,Normal,TH,BKK,,20260914,3\n",
            "C003,Normal,VN,SGN,'DQDINH,NVTOAN',20260914,3\n",
            "C004,Normal,TH,LCH,PHANARINN,,3\n",
        )
        self.assertEqual(parsed, [])


class OwnerSelectionTests(unittest.TestCase):
    def test_newest_booking_decides_the_owner(self) -> None:
        chosen, skipped = owners.choose_current_owners(observations(
            "C001,Normal,TH,BKK,OLDOWNER,20260301,40\n",
            "C001,Normal,TH,BKK,NEWOWNER,20260914,4\n",
        ))
        self.assertEqual(chosen["C001"]["salesman"], "NEWOWNER")
        self.assertEqual(chosen["C001"]["total_bookings"], 44)
        self.assertEqual(skipped, {"ambiguous_date": 0, "weak_evidence": 0})

    def test_single_stray_booking_does_not_unseat_a_busy_owner(self) -> None:
        chosen, skipped = owners.choose_current_owners(observations(
            "C001,Normal,TH,BKK,BUSY,20260913,40\n",
            "C001,Normal,TH,BKK,STRAY,20260914,1\n",
        ))
        self.assertNotIn("C001", chosen)
        self.assertEqual(skipped["weak_evidence"], 1)

    def test_sparse_customer_keeps_its_only_evidence(self) -> None:
        chosen, _ = owners.choose_current_owners(observations(
            "C001,Normal,TH,BKK,ONLYONE,20260914,1\n",
        ))
        self.assertEqual(chosen["C001"]["salesman"], "ONLYONE")

    def test_same_day_tie_is_left_alone(self) -> None:
        chosen, skipped = owners.choose_current_owners(observations(
            "C001,Normal,TH,BKK,ONE,20260914,5\n",
            "C001,Normal,TH,LCH,TWO,20260914,5\n",
        ))
        self.assertEqual(chosen, {})
        self.assertEqual(skipped["ambiguous_date"], 1)


def roster(*rows: tuple[str, str, str, str, str]) -> list[dict[str, str]]:
    return [
        {"COUNTRY": c, "PORT": p, "CUSTOMER_NO": cust, "SALESMAN_NO": s,
         "SALES_START_DATE": start, "SALES_END_DATE": "99991231",
         "CUSTOMER_CATEGORY": "01", "EXPORT_IMPORT_TYPE": "A"}
        for c, p, cust, s, start in rows
    ]


class OwnerUpdateTests(unittest.TestCase):
    def setUp(self) -> None:
        self.chosen, _ = owners.choose_current_owners(observations(
            "SAME01,Normal,TH,BKK,KEEPER,20260914,6\n",
            "MOVED1,Normal,TH,BKK,NEWOWNER,20260914,6\n",
            "FRESH1,Normal,PH,MIP,MARIANNE,20260914,6\n",
        ))

    def test_only_changed_and_new_customers_are_written(self) -> None:
        updates, counts = owners.build_owner_updates(
            self.chosen,
            roster(("TH", "BKK", "SAME01", "KEEPER", "20240101"),
                   ("TH", "BKK", "MOVED1", "OLDOWNER", "20240101")),
            20260915,
        )
        self.assertEqual(counts, {"unchanged": 1, "changed": 1, "new": 1, "unreadable_row": 0})
        self.assertEqual({row["CUSTOMER_NO"] for row in updates}, {"MOVED1", "FRESH1"})
        moved = next(row for row in updates if row["CUSTOMER_NO"] == "MOVED1")
        self.assertEqual(moved["SALESMAN_NO"], "NEWOWNER")
        self.assertEqual(moved["SALES_START_DATE"], "20260915")
        self.assertEqual(moved["OWNER_OBSERVED_DATE"], "20260915")
        self.assertEqual(moved["OWNER_EVIDENCE_DATE"], "20260914")
        self.assertEqual(moved["OWNER_EFFECTIVE_DATE"], "")
        self.assertEqual(moved["SALES_END_DATE"], "99991231")
        self.assertIn("previous OLDOWNER", moved["EVIDENCE"])

    def test_expired_assignment_is_replaced_not_skipped(self) -> None:
        rows = roster(("TH", "BKK", "MOVED1", "OLDOWNER", "20200101"))
        rows[0]["SALES_END_DATE"] = "20250101"
        updates, counts = owners.build_owner_updates({"MOVED1": self.chosen["MOVED1"]}, rows, 20260915)
        self.assertEqual(counts["new"], 1)
        self.assertEqual(updates[0]["SALESMAN_NO"], "NEWOWNER")

    def test_unreadable_roster_dates_are_left_untouched(self) -> None:
        rows = roster(("TH", "BKK", "MOVED1", "OLDOWNER", "202103.1"))
        updates, counts = owners.build_owner_updates({"MOVED1": self.chosen["MOVED1"]}, rows, 20260915)
        self.assertEqual(updates, [])
        self.assertEqual(counts["unreadable_row"], 1)


class WorkbookTests(unittest.TestCase):
    def test_owner_workbook_keeps_one_row_per_origin_customer_owner(self) -> None:
        twb = owners.build_owner_workbook("20260101").decode("utf-8")
        for field in owners.DIMENSIONS:
            self.assertEqual(twb.count(f"[none:{field}:nk]'"), 1, field)
            self.assertIn(f"[{owners.DS_REF}].[none:{field}:nk]", twb)
        rows = twb.split("<rows>", 1)[1].split("</rows>", 1)[0]
        cols = twb.split("<cols>", 1)[1].split("</cols>", 1)[0]
        self.assertNotIn("BkgDtNum", rows)
        self.assertIn("[max:BkgDtNum:qk]", cols)
        self.assertIn("[sum:BkgCount:qk]", cols)
        self.assertIn('IF [BKG_DT] &gt;= &quot;20260101&quot;', twb)
        self.assertIn(owners.BOOKING_DATASOURCE_CONTENT_URL, twb)

    def test_window_start_uses_the_configured_months(self) -> None:
        self.assertEqual(owners.window_start("20260915", months=1), "20260816")


class ExportErrorTests(unittest.TestCase):
    def test_missing_oracle_table_is_not_reported_as_a_permission_problem(self) -> None:
        error = published.classify_export_error(
            'ORA-00942: table or view does not exist The table '
            '"[DM_SALES].[N2024_DM_SALES_MAN_DIMENSION]" does not exist.'
        )
        self.assertIsInstance(error, published.SourceTableMissing)
        self.assertIn("[DM_SALES].[N2024_DM_SALES_MAN_DIMENSION]", str(error))

    def test_permission_denial_is_kept_distinct(self) -> None:
        self.assertIsInstance(
            published.classify_export_error("HTTP 403 access is unauthorized"),
            PermissionError,
        )


if __name__ == "__main__":
    unittest.main()
