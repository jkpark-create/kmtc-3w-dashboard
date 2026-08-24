import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

import daily_3w_dashboard as dashboard


class TableauDownloadDefaultsTest(unittest.TestCase):
    def test_two_week_timeout_falls_back_to_one_week_files(self):
        class Session:
            def post(self, *_args, **_kwargs):
                return None

        calls = []

        def fake_download(_workbook, _view, save_path, **kwargs):
            calls.append((Path(save_path).name, kwargs.get("max_attempts")))
            if Path(save_path).name.endswith("_c01_20260802_20260815.csv"):
                raise TimeoutError("Tableau render timeout")
            value = Path(save_path).stem
            Path(save_path).write_text(f"value\n{value}\n", encoding="utf-8")
            return Path(save_path).stat().st_size

        with tempfile.TemporaryDirectory() as tmp:
            runtime = Path(tmp)
            output = runtime / "1.csv"
            with (
                patch.object(dashboard, "RUNTIME_DIR", runtime),
                patch.object(dashboard, "BKG_SCHEDULE_START", "2026-08-02 00:00:00"),
                patch.object(dashboard, "BKG_SCHEDULE_END", "2026-08-15 00:00:00"),
                patch.object(dashboard, "TABLEAU_VIEW1_CHUNK_WEEKS", 2),
                patch.object(dashboard, "DASHBOARD_REUSE_SAME_DAY_CHUNKS", False),
                patch.object(dashboard, "tableau_rest_api", return_value=(Session(), "3.29", "site")),
                patch.object(dashboard, "ensure_temp_workbook", side_effect=lambda *_a, workbook_name, **_k: workbook_name),
                patch.object(dashboard, "download_csv_from_tableau", side_effect=fake_download),
            ):
                dashboard.download_view1_daily(output)

            self.assertTrue(output.exists())
            self.assertEqual(dashboard.count_csv_rows(output), 14)
            self.assertEqual(calls[0], ("1_20260824_c01_20260802_20260815.csv", 1))
            self.assertEqual(len(calls), 15)
            self.assertEqual(calls[1], ("1_20260824_c01d01_20260802_20260802.csv", 1))
            self.assertEqual(calls[-1], ("1_20260824_c01d14_20260815_20260815.csv", 1))

    def test_slow_two_week_window_splits_into_daily_windows(self):
        windows = dashboard.split_view1_chunk_window(
            "2026-08-02 00:00:00", "2026-08-15 00:00:00"
        )
        self.assertEqual(len(windows), 14)
        self.assertEqual(windows[0], ("2026-08-02 00:00:00", "2026-08-02 00:00:00"))
        self.assertEqual(windows[-1], ("2026-08-15 00:00:00", "2026-08-15 00:00:00"))

    def test_one_week_window_splits_into_seven_days(self):
        windows = dashboard.split_view1_chunk_window(
            "2026-08-02 00:00:00", "2026-08-08 00:00:00"
        )
        self.assertEqual(len(windows), 7)

    def test_one_day_window_is_the_minimum(self):
        self.assertEqual(
            dashboard.split_view1_chunk_window(
                "2026-08-02 00:00:00", "2026-08-02 00:00:00"
            ),
            [],
        )

    def test_view1_uses_browser_while_large_exports_keep_http(self):
        env = os.environ.copy()
        for name in (
            "TABLEAU_USE_HTTP_CSV_DOWNLOAD",
            "TABLEAU_VIEW1_USE_HTTP_CSV_DOWNLOAD",
        ):
            env.pop(name, None)
        command = (
            "import daily_3w_dashboard as d; "
            "print(int(d.TABLEAU_USE_HTTP_CSV_DOWNLOAD), "
            "int(d.TABLEAU_VIEW1_USE_HTTP_CSV_DOWNLOAD), "
            "d.TABLEAU_VIEW1_CHUNK_WEEKS)"
        )
        result = subprocess.run(
            [sys.executable, "-c", command],
            cwd=ROOT,
            env=env,
            check=True,
            capture_output=True,
            text=True,
        )
        self.assertEqual(result.stdout.strip(), "1 0 2")

    def test_operational_batch_uses_reliable_browser_strategy(self):
        batch = (ROOT / "run_daily.bat").read_text(encoding="utf-8")
        self.assertIn('TABLEAU_USE_HTTP_CSV_DOWNLOAD=1', batch)
        self.assertIn('set "TABLEAU_VIEW1_USE_HTTP_CSV_DOWNLOAD=0"', batch)
        self.assertIn('set "TABLEAU_VIEW1_CHUNK_WEEKS=2"', batch)


if __name__ == "__main__":
    unittest.main()
