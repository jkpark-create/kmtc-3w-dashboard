import ast
import sys
import unittest
from pathlib import Path
from collections import Counter
from types import SimpleNamespace

HERE = Path(__file__).resolve().parent
SCRIPTS = HERE.parent if HERE.name == 'test' else HERE
sys.path.insert(0, str(SCRIPTS))
from sales_target_monthly import *


class MonthlyTargetsTest(unittest.TestCase):
    def test_observation_date_is_rejected_as_effective_history(self):
        with self.assertRaisesRegex(ValueError, 'not confirmed'):
            month_owner_mapping([dict(CUSTOMER_NO='C',SALESMAN_NO='S',SALES_START_DATE='20260801',SALES_END_DATE='99991231',OWNER_DATE_BASIS='observed_only_effective_unknown')], '202608')
    def test_change_month_and_old_months(self):
        records = [dict(CUSTOMER_NO='C1', SALESMAN_NO='OLD', SALES_START_DATE='20250101', SALES_END_DATE='20260814'),
                   dict(CUSTOMER_NO='C1', SALESMAN_NO='NEW', SALES_START_DATE='20260815', SALES_END_DATE='')]
        self.assertEqual(month_owner_mapping(records, '202607')['C1'], 'OLD')
        self.assertEqual(month_owner_mapping(records, '202608')['C1'], 'NEW')
        self.assertEqual(month_owner_mapping(records, '202609')['C1'], 'NEW')

    def test_multiple_changes_fail(self):
        records = [dict(CUSTOMER_NO='C', SALESMAN_NO='A', SALES_START_DATE='20260801', SALES_END_DATE='20260814'),
                   dict(CUSTOMER_NO='C', SALESMAN_NO='B', SALES_START_DATE='20260815', SALES_END_DATE='99991231')]
        with self.assertRaisesRegex(ValueError, 'Multiple owner changes'):
            month_owner_mapping(records, '202608')

    def test_zero_blank_and_total_preservation(self):
        measures = [dict(customer='A', denominator=100), dict(customer='B', denominator=300)]
        allocated = allocate_month_targets(measures, {'A': 'S1', 'B': 'S2'}, 200, {'S1': 0, 'S2': ''})
        self.assertEqual(allocated['S1']['numerator'], 0)
        self.assertTrue(allocated['S1']['explicit'])
        self.assertEqual(allocated['S2']['numerator'], 200)
        self.assertEqual(sum(x['numerator'] for x in allocated.values()), 200)

    def test_quarter_ratio_of_sums_and_transition_conserves_goal(self):
        measures = [dict(customer='A', denominator=100), dict(customer='B', denominator=200)]
        july = allocate_month_targets(measures, {'A': 'OLD', 'B': 'OTHER'}, 150)
        august = allocate_month_targets(measures, {'A': 'NEW', 'B': 'OTHER'}, 210)
        self.assertNotIn('OLD', august)
        self.assertEqual(sum(x['numerator'] for x in july.values()), 150)
        self.assertEqual(sum(x['numerator'] for x in august.values()), 210)
        q = quarter_target([july['OTHER'], august['OTHER']])
        self.assertAlmostEqual(q['target'], .6)
        self.assertNotEqual(q['target'], july['OTHER']['target'] + august['OTHER']['target'])

    def test_no_duplicate_or_dropped_unassigned(self):
        row = dict(customer='A', denominator=100)
        with self.assertRaisesRegex(ValueError, 'Duplicate'):
            allocate_month_targets([row, row], {}, 50)
        self.assertEqual(allocate_month_targets([row], {}, 50)[UNASSIGNED]['numerator'], 50)

    def test_conflicting_override_fails(self):
        with self.assertRaisesRegex(ValueError, 'exceeds'):
            allocate_month_targets([dict(customer='A', denominator=100)], {'A': 'S'}, 50, {'S': 1})

    def test_refresh_keeps_past_and_explicit_zero(self):
        july = {'numerator': 50, 'denominator': 100}
        august = {'numerator': 0, 'denominator': 100, 'explicit': True}
        original = {'202607': july, '202608': august}
        revised = {'202607': {'numerator': 99}, '202608': {'numerator': 99}, '202609': {'numerator': 70}}
        merged = merge_month_history(original, revised, '202608')
        self.assertEqual(merged['202607'], july)
        self.assertEqual(merged['202608'], august)
        self.assertEqual(merge_month_history(merged, revised, '202608'), merged)

    def test_missing_frozen_history_fails(self):
        with self.assertRaisesRegex(ValueError, 'Missing frozen'):
            merge_month_history({}, {'202607': {}}, '202608')


class WorkbookInputTest(unittest.TestCase):
    def test_input_workbook_is_closed_on_missing_sheet_and_errors(self):
        source = (SCRIPTS / 'update_target_workbook_from_current_customer_owners.py').read_text(encoding='utf-8')
        tree = ast.parse(source)
        selected = ast.Module(body=[node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == 'read_existing_target_input'], type_ignores=[])
        class Workbook:
            sheetnames = []
            closed = False
            def close(self): self.closed = True
        workbook = Workbook()
        namespace = {'Path': Path, 'Any': object, 'openpyxl': SimpleNamespace(load_workbook=lambda *args, **kwargs: workbook)}
        exec(compile(selected, str(SCRIPTS), 'exec'), namespace)
        self.assertEqual(namespace['read_existing_target_input'](Path('fixture.xlsx'), SimpleNamespace(INPUT_SHEET='Target_Input')), {})
        self.assertTrue(workbook.closed)

    def test_blank_input_defaults_but_zero_is_preserved(self):
        # Compile the actual pure workbook functions without pipeline imports.
        source = (SCRIPTS / 'update_target_workbook_from_current_customer_owners.py').read_text(encoding='utf-8')
        names = {'most_applied_target_inputs', 'build_input_values'}
        tree = ast.parse(source)
        selected = ast.Module(body=[node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name in names], type_ignores=[])
        namespace = {'Any': object, 'Counter': Counter, 'MODE_INPUT_ORIGINS': set(), 'rationale': lambda suggestion: ''}
        exec(compile(selected, str(SCRIPTS), 'exec'), namespace)
        existing = {'PH': (0.0, None, 0.05, 'keep')}
        values = namespace['build_input_values'](['PH'], existing, {'PH': {'lifting_pp': 0.1}})
        self.assertEqual(values[3][1], 0)
        self.assertEqual(values[3][3], .1)
        self.assertEqual(values[3][5], .05)
        self.assertEqual(values[3][7], 'keep')
        self.assertEqual(namespace['most_applied_target_inputs'](existing), (0.0, .05, .05))


if __name__ == '__main__':
    unittest.main()
