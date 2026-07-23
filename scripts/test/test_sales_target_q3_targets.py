import importlib.util
import sys
import types
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = ROOT / "scripts" / "build_sales_target_drill_data.py"

# parse_summary is dependency-free, but the production module imports its
# pipeline libraries at module load time. Stub only those imports so this
# focused regression test also runs in a lightweight Python environment.
pandas_stub = sys.modules.setdefault("pandas", types.ModuleType("pandas"))
pandas_stub.isna = lambda value: False
for module_name in [
    "google",
    "google.auth",
    "google.auth.transport",
    "google.auth.transport.requests",
    "google.oauth2",
    "google.oauth2.credentials",
    "googleapiclient",
    "googleapiclient.discovery",
]:
    sys.modules.setdefault(module_name, types.ModuleType(module_name))
sys.modules["google.auth.transport.requests"].Request = object
sys.modules["google.oauth2.credentials"].Credentials = object
sys.modules["googleapiclient.discovery"].build = lambda *args, **kwargs: None

SPEC = importlib.util.spec_from_file_location("build_sales_target_drill_data", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
SPEC.loader.exec_module(MODULE)


class ParseSummaryQ3TargetTest(unittest.TestCase):
    def test_q3_reuses_target_input_adjusted_q2_targets(self) -> None:
        headers = [[], [], [], []]
        row = [
            "CN_SHA",
            "Team Total",
            1.0,
            0.55,
            0.65,
            0.50,
            -0.15,
            0.65,
            0.52,
            -0.13,
            0.70,
            0.60,
            -0.10,
            0.70,
            0.62,
            -0.08,
            0.45,
            0.40,
            -0.05,
            0.45,
            0.42,
            -0.03,
            10,
            8,
            0.8,
            0.55,
            0.60,
            0.40,
            "TOTAL",
        ]

        parsed = MODULE.parse_summary(headers + [row])
        self.assertEqual(len(parsed), 1)
        kpi = parsed[0]["kpi"]
        self.assertEqual(kpi["booking"]["q3"]["target"], 0.65)
        self.assertEqual(kpi["lifting"]["q3"]["target"], 0.70)
        self.assertEqual(kpi["high_profit"]["q3"]["target"], 0.45)
        self.assertNotEqual(kpi["booking"]["q3"]["target"], row[25])
        self.assertNotEqual(kpi["lifting"]["q3"]["target"], row[26])
        self.assertNotEqual(kpi["high_profit"]["q3"]["target"], row[27])


if __name__ == "__main__":
    unittest.main()
