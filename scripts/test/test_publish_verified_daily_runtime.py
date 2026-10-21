import importlib.util
from pathlib import Path
import tempfile
from types import SimpleNamespace
from unittest.mock import Mock
import unittest

spec = importlib.util.spec_from_file_location('publish_daily', Path(__file__).resolve().parents[1] / 'publish_verified_daily_runtime.py')
daily = importlib.util.module_from_spec(spec)
spec.loader.exec_module(daily)


class PublicationTest(unittest.TestCase):
    def fixture(self, runtime):
        events = []
        output = runtime / 'output'; output.mkdir()
        for name in ('customer_code_quality_20261021.json', 'customer_code_errors_20261021.csv'):
            (output / name).write_text('quality')
        groups = {name: [] for name in ('main', 'main_source', 'obt')}
        groups['main_history'] = [(output / 'dashboard_summary_20261021.json', 'summary')]
        sync = SimpleNamespace(runtime_groups=Mock(return_value=groups), refresh_access_token=Mock(return_value='token'),
            ensure_company_reader=Mock(), sync_group=Mock(side_effect=lambda *a, **kw: events.append(kw['group_name']) or []),
            verify_remote_groups=Mock(side_effect=lambda *a: events.append('verified') or 10), merge_state_group=Mock(),
            DRIVE_FOLDERS={'sales': 'folder'}, list_files=Mock(), sync_one=Mock(), request=Mock(), DRIVE_API='api')
        release = SimpleNamespace(verify_company_release=Mock(), release_entries=Mock(return_value={'sales': [], 'sales_data': []}),
            publish_release_pointer=Mock(side_effect=lambda *a: events.append('pointer') or {'id': 'pointer'}))
        return events, sync, release

    def test_pointer_is_published_after_dependencies_and_verified_before_state(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); events, sync, release = self.fixture(root)
            self.assertEqual(daily.publish(root, '20261021', sync, release), 10)
            self.assertLess(events.index('sales_data'), events.index('sales'))
            self.assertEqual(events[-3:], ['verified', 'pointer', 'verified'])
            sync.runtime_groups.assert_called_once_with(skip_sales_target=True)
            self.assertEqual(sync.merge_state_group.call_count, 6)

    def test_failed_dependencies_cannot_move_pointer(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); events, sync, release = self.fixture(root)
            sync.verify_remote_groups.side_effect = ValueError('remote checksum differs')
            with self.assertRaises(ValueError): daily.publish(root, '20261021', sync, release)
            release.publish_release_pointer.assert_not_called()
            sync.merge_state_group.assert_not_called()

    def test_mixed_dates_fail_before_remote_changes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); events, sync, release = self.fixture(root)
            with self.assertRaisesRegex(ValueError, 'dates differ'): daily.publish(root, '20261022', sync, release)
            sync.refresh_access_token.assert_not_called()


if __name__ == '__main__':
    unittest.main()
