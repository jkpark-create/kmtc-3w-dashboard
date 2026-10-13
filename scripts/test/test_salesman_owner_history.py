import unittest
import sys
from pathlib import Path
HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE.parent if HERE.name=='test' else HERE))
from salesman_owner_history import transition_rows


class OwnerHistoryTest(unittest.TestCase):
    def old(self):
        return dict(CUSTOMER_NO='C',SALESMAN_NO='OLD',SALES_START_DATE='20250101',SALES_END_DATE='99991231')

    def test_booking_date_is_evidence_not_effective_date(self):
        original=self.old()
        archived,new,event=transition_rows([original],dict(CUSTOMER_NO='C',SALESMAN_NO='NEW',OWNER_EVIDENCE_DATE='20260815'),20260901)
        self.assertEqual(original['SALES_END_DATE'],'99991231')
        self.assertEqual(archived[0]['SALES_END_DATE'],'20260831')
        self.assertEqual(archived[0]['OWNER_EFFECTIVE_END_DATE'],'')
        self.assertEqual(new['SALES_START_DATE'],'20260901')
        self.assertEqual(new['OWNER_EFFECTIVE_DATE'],'')
        self.assertEqual(event['effective_date_status'],'unknown')
        self.assertEqual(event['previous_rows'][0],original)

    def test_confirmed_effective_date_keeps_old_month(self):
        archived,new,event=transition_rows([self.old()],dict(SALESMAN_NO='NEW',OWNER_EFFECTIVE_DATE='20260815'),20260901)
        self.assertEqual(archived[0]['SALES_END_DATE'],'20260814')
        self.assertEqual(archived[0]['OWNER_EFFECTIVE_END_DATE'],'20260814')
        self.assertEqual(new['SALES_START_DATE'],'20260815')
        self.assertEqual(event['observed_date'],'20260901')
        self.assertEqual(event['effective_date'],'20260815')

    def test_same_day_and_future_changes_need_review(self):
        old=self.old(); old['SALES_START_DATE']='20260901'
        with self.assertRaisesRegex(ValueError,'Same-day'):
            transition_rows([old],dict(SALESMAN_NO='NEW'),20260901)
        with self.assertRaisesRegex(ValueError,'Future'):
            transition_rows([self.old()],dict(SALESMAN_NO='NEW',OWNER_EFFECTIVE_DATE='20261001'),20260901)

    def test_override_keeps_closed_prior_owner_and_one_active_current_owner(self):
        from apply_salesman_owner_overrides import apply_overrides, audit_active
        old=self.old()
        updates=[dict(CUSTOMER_NO='C',SALESMAN_NO='NEW',COUNTRY='TH',PORT='BKK',SALES_START_DATE='20260815',SALES_END_DATE='99991231',OWNER_EVIDENCE_DATE='20260815')]
        rows,events=apply_overrides([old],updates,20260901,preserve_history=True)
        self.assertEqual(len(rows),2)
        self.assertEqual(rows[0]['SALESMAN_NO'],'OLD')
        self.assertEqual(rows[1]['SALESMAN_NO'],'NEW')
        self.assertEqual(audit_active(rows,20260901)['active_customers'],1)
        self.assertEqual(audit_active(rows,20260901)['conflicts'],{})
        self.assertEqual(events[0]['history_event']['effective_date_status'],'unknown')


if __name__=='__main__': unittest.main()
