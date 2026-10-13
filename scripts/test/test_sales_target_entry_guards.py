import ast,sys,unittest,os
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
HERE=Path(__file__).resolve().parent
SCRIPTS=HERE.parent if HERE.name=='test' else HERE


class EntryGuardTest(unittest.TestCase):
    def test_skip_precedes_any_updater_or_builder_operation(self):
        for name in ('build_sales_target_drill_data.py','update_target_workbook_from_current_customer_owners.py'):
            source=(SCRIPTS/name).read_text(encoding='utf-8')
            main=next(n for n in ast.parse(source).body if isinstance(n,ast.FunctionDef) and n.name=='main')
            # Execute the actual leading guard with no services/IO dependencies.
            main.body=[main.body[0],ast.Raise(exc=ast.Call(func=ast.Name(id='AssertionError',ctx=ast.Load()),args=[ast.Constant('IO after skip')],keywords=[]),cause=None)]
            tree=ast.fix_missing_locations(ast.Module(body=[main],type_ignores=[]))
            namespace={'os':os}
            exec(compile(tree,name,'exec'),namespace)
            with patch.dict(os.environ,{'SKIP_SALES_TARGET_BUILD':'1'}):
                self.assertIn(namespace['main'](),(None,0))


if __name__=='__main__':unittest.main()
