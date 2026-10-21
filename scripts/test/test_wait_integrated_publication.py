"""Readiness retries must neither bypass publication proof nor take writer locks."""
import importlib.util
from pathlib import Path
import unittest
from unittest.mock import Mock, patch
from types import SimpleNamespace

spec = importlib.util.spec_from_file_location('wait_publication', Path(__file__).resolve().parents[1] / 'wait_integrated_publication.py')
wait = importlib.util.module_from_spec(spec)
spec.loader.exec_module(wait)


class PublicationWaitTests(unittest.TestCase):
    def run_wait(self, results, budget=120):
        now = [0]
        sleeps = []
        values = iter(results)
        def sleep(seconds):
            sleeps.append(seconds)
            now[0] += seconds
        rc = wait.wait_for_publication(lambda: next(values), budget, 60,
                                      clock=lambda: now[0], sleep=sleep,
                                      report=lambda *args, **kwargs: None)
        return rc, sleeps

    def test_pending_source_eventually_advances(self):
        rc, sleeps = self.run_wait([{'ready': False, 'reason': 'source_date_pending'},
                                   {'ready': True, 'reason': 'published_immutable_source_ready'}])
        self.assertEqual((rc, sleeps), (0, [60]))

    def test_transient_network_failure_is_retried(self):
        rc, sleeps = self.run_wait([{'ready': False, 'reason': 'TLS EOF'}, {'ready': True}])
        self.assertEqual((rc, sleeps), (0, [60]))

    def test_budget_expires_without_false_success(self):
        rc, sleeps = self.run_wait([{'ready': False}] * 3, budget=61)
        self.assertEqual((rc, sleeps), (3, [60, 1]))

    def test_permissions_fail_immediately(self):
        rc, sleeps = self.run_wait([{'ready': False, 'terminal': True, 'winError': 5}])
        self.assertEqual((rc, sleeps), (77, []))

    def test_zero_budget_still_checks_once(self):
        self.assertEqual(self.run_wait([{'ready': True}], budget=0), (0, []))

    def test_consumer_path_passes_retries_to_wait_loop(self):
        prepare = Mock(side_effect=[ValueError('source_date_pending'), {'sourceDate': '20261021'}])
        argv = ['wait', '--root', 'producer', '--consumer-runtime', 'consumer',
                '--source-date', '20261021', '--wait-seconds', '120', '--poll-seconds', '60']
        def exercise(inspect, seconds, poll):
            self.assertEqual((seconds, poll), (120, 60))
            self.assertFalse(inspect()['ready'])
            self.assertTrue(inspect()['ready'])
            return 0
        with patch.object(wait.sys, 'argv', argv), patch.dict(wait.sys.modules, {'published_source_package': SimpleNamespace(prepare=prepare)}), patch.dict(wait.os.environ, {'KMTC_THREEW_SOURCE_PIN': 'pin.json'}), patch.object(wait, 'wait_for_publication', side_effect=exercise):
            self.assertEqual(wait.main(), 0)
        self.assertEqual(prepare.call_count, 2)


if __name__ == '__main__':
    unittest.main()
