import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "sync_dashboard_data_to_gdrive_test",
    ROOT / "scripts" / "sync_dashboard_data_to_gdrive.py",
)
sync = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = sync
SPEC.loader.exec_module(sync)


class RuntimeSyncSafetyTest(unittest.TestCase):
    def test_mixed_dataset_dates_are_rejected(self):
        original_runtime_root = sync.RUNTIME_ROOT
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                runtime_root = Path(temp_dir)
                output = runtime_root / "output"
                output.mkdir(parents=True)
                (output / "dashboard_summary_20260812.json").write_text("{}", encoding="utf-8")
                (output / "_cache_2025.parquet").write_bytes(b"basis")
                (output / "_cache_20260812.parquet").write_bytes(b"current")
                (output / "BSA_raw_monthly3W_20260811.csv").write_text("x\n", encoding="utf-8")
                sync.RUNTIME_ROOT = runtime_root
                with self.assertRaisesRegex(RuntimeError, "mixed runtime dataset dates"):
                    sync.runtime_groups()
        finally:
            sync.RUNTIME_ROOT = original_runtime_root

    def test_cleanup_refuses_project_root(self):
        original_runtime_root = sync.RUNTIME_ROOT
        try:
            sync.RUNTIME_ROOT = sync.ROOT
            with self.assertRaisesRegex(RuntimeError, "project root"):
                sync.cleanup_local_data({})
        finally:
            sync.RUNTIME_ROOT = original_runtime_root

    def test_duplicate_drive_names_are_rejected(self):
        original_list_files = sync.list_files
        try:
            sync.list_files = lambda _token, _folder: [
                {"id": "one", "name": "data.json.gz", "size": "10", "md5Checksum": "abc"},
                {"id": "two", "name": "data.json.gz", "size": "10", "md5Checksum": "abc"},
            ]
            expected = {
                "main": [
                    {"id": "one", "name": "data.json.gz", "size": 10, "md5": "abc"}
                ]
            }
            with self.assertRaisesRegex(RuntimeError, "expected one Drive file, found 2"):
                sync.verify_remote_groups("token", expected)
        finally:
            sync.list_files = original_list_files


if __name__ == "__main__":
    unittest.main()
