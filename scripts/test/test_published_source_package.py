"""Immutable consumer packages must match the independently published hashes."""
import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location('source_package', Path(__file__).resolve().parents[1] / 'published_source_package.py')
package = importlib.util.module_from_spec(spec)
spec.loader.exec_module(package)


class SourcePackageTests(unittest.TestCase):
    def fixture(self, root):
        integrated, runtime = root / 'producer', root / 'consumer'
        integrated.mkdir()
        manifest = {'sourceMeta': {'sourceAsOfDate': '20261021', 'generatedAt': '2026-10-21T17:00:00+09:00', 'refreshState': 'complete'},
                    'monthFiles': {'202607': '202607.json'}, 'detailFiles': {'202607': '202607-detail.json'}}
        bodies = {'manifest.json': json.dumps(manifest).encode(), '202607.json': b'{"rows":[1]}', '202607-detail.json': b'{"rows":[2]}'}
        success = {'status': 'success', 'productionVerified': True, 'outcome': 'updated', 'sourceAsOfDate': '20261021',
                   'generatedAt': manifest['sourceMeta']['generatedAt'], 'manifestSha256': package.sha(bodies['manifest.json'])}
        (integrated / 'integrated-dashboard-last-success.json').write_text(json.dumps(success), encoding='utf-8')
        index = {'driveFolderId': '1lccizv8st6zJ-1mFSAvHZlx5kF8dYH7r', 'files': {
            package.PREFIX + name: {'size': len(body), 'md5': hashlib.md5(body).hexdigest(), 'fileId': name}
            for name, body in bodies.items()}}
        return integrated, runtime, runtime / 'pin.json', bodies, json.dumps(index).encode()

    def prepare(self, fixture, **kwargs):
        integrated, runtime, pin, bodies, index = fixture
        return package.prepare(integrated, runtime, '20261021', pin, index_reader=lambda: index,
                               member_reader=kwargs.get('reader', lambda name, entry: bodies[name]), report=lambda *args: None)

    def test_consumer_reads_complete_package_while_producer_is_busy(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = self.fixture(Path(directory))
            result = self.prepare(fixture)
            self.assertEqual(package.from_pin(fixture[2], '20261021'), result)
            self.assertEqual(self.prepare(fixture), result)

    def test_corrupt_member_cannot_create_pin(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = self.fixture(Path(directory))
            with self.assertRaisesRegex(ValueError, 'hash mismatch'):
                self.prepare(fixture, reader=lambda name, entry: b'corrupt')
            self.assertFalse(fixture[2].exists())

    def test_changed_member_invalidates_pin(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = self.fixture(Path(directory))
            result = self.prepare(fixture)
            (Path(result['packageRoot']) / 'oracle-dashboard/202607.json').write_bytes(b'corrupt')
            with self.assertRaisesRegex(ValueError, 'hash mismatch'):
                package.from_pin(fixture[2], '20261021')

    def test_previous_day_cannot_satisfy_current_request(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = self.fixture(Path(directory))
            result = self.prepare(fixture)
            with self.assertRaisesRegex(ValueError, 'source_date_pending'):
                package.validate(result['packageRoot'], '20261022')


if __name__ == '__main__':
    unittest.main()
