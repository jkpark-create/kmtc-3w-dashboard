import importlib.util
from pathlib import Path
import subprocess
import tempfile
from types import SimpleNamespace
import unittest

spec = importlib.util.spec_from_file_location('daily_target', Path(__file__).resolve().parents[1] / 'refresh_sales_target_daily.py')
daily = importlib.util.module_from_spec(spec)
spec.loader.exec_module(daily)


class DailyTargetTest(unittest.TestCase):
    def setup_runtime(self, root):
        for name in ('booking_snapshot_result_20261021.csv', '_cache_20261021.parquet', 'BSA_raw_monthly3W_20261021.csv'):
            path = root / 'output' / name
            path.parent.mkdir(exist_ok=True)
            path.write_text('source')
        site = root / 'dist/sales-target'
        site.mkdir(parents=True)
        (site / 'index.json').write_text('previous')
        return site

    def test_failed_build_keeps_previous_generation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); site = self.setup_runtime(root)
            with self.assertRaises(subprocess.CalledProcessError):
                daily.refresh(root, root, '20261021', runner=lambda *a, **k: SimpleNamespace(returncode=9))
            self.assertEqual((site / 'index.json').read_text(), 'previous')

    def test_failed_verification_keeps_previous_generation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); site = self.setup_runtime(root)
            def reject(*args): raise ValueError('incomplete source')
            with self.assertRaisesRegex(ValueError, 'incomplete'):
                daily.refresh(root, root, '20261021', runner=lambda *a, **k: SimpleNamespace(returncode=0), verifier=reject)
            self.assertEqual((site / 'index.json').read_text(), 'previous')

    def test_verified_build_activates_and_retains_previous(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); site = self.setup_runtime(root)
            def run(command, **kwargs):
                self.assertIn('--monthly-target-from-existing-sources', command)
                self.assertEqual(kwargs['env']['SKIP_SALES_TARGET_BUILD'], '0')
                stage = Path(command[command.index('--out') + 1])
                (stage / 'index.json').write_text('today')
                return SimpleNamespace(returncode=0)
            result = daily.refresh(root, root, '20261021', runner=run, verifier=lambda path, day: {'dataset': day})
            self.assertEqual((site / 'index.json').read_text(), 'today')
            self.assertEqual((Path(result['previous']) / 'index.json').read_text(), 'previous')

    def test_missing_current_inputs_never_starts_builder(self):
        with tempfile.TemporaryDirectory() as directory:
            def run(*args, **kwargs): self.fail('Builder started without inputs')
            with self.assertRaises(FileNotFoundError):
                daily.refresh(directory, directory, '20261021', runner=run)


if __name__ == '__main__':
    unittest.main()
