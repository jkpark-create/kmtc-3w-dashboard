import importlib.util
import sys
import unittest
from datetime import datetime, timedelta
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "daily_3w_dashboard_chunk_test",
    ROOT / "daily_3w_dashboard.py",
)
dashboard = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = dashboard
SPEC.loader.exec_module(dashboard)


class TableauView1ChunkTest(unittest.TestCase):
    def test_five_week_chunks_cover_window_without_gaps_or_overlaps(self):
        chunks = dashboard.window_week_chunks(
            "2025-12-28 00:00:00",
            "2026-09-12 00:00:00",
            5,
        )

        self.assertEqual(len(chunks), 8)
        self.assertEqual(chunks[0][1:], (
            "2025-12-28 00:00:00",
            "2026-01-31 00:00:00",
        ))
        self.assertEqual(chunks[-1][1:], (
            "2026-08-30 00:00:00",
            "2026-09-12 00:00:00",
        ))

        for index, (_number, start_text, end_text) in enumerate(chunks):
            start = datetime.strptime(start_text[:10], "%Y-%m-%d")
            end = datetime.strptime(end_text[:10], "%Y-%m-%d")
            self.assertLessEqual((end - start).days + 1, 35)
            if index:
                previous_end = datetime.strptime(chunks[index - 1][2][:10], "%Y-%m-%d")
                self.assertEqual(start, previous_end + timedelta(days=1))

    def test_partial_final_chunk_keeps_requested_end_date(self):
        chunks = dashboard.window_week_chunks(
            "2026-01-01 00:00:00",
            "2026-02-10 00:00:00",
            5,
        )

        self.assertEqual(chunks, [
            (1, "2026-01-01 00:00:00", "2026-02-04 00:00:00"),
            (2, "2026-02-05 00:00:00", "2026-02-10 00:00:00"),
        ])

    def test_reversed_window_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "Invalid date window"):
            dashboard.window_week_chunks(
                "2026-02-10 00:00:00",
                "2026-02-01 00:00:00",
                5,
            )


if __name__ == "__main__":
    unittest.main()
