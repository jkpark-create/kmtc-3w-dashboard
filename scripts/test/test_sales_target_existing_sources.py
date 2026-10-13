import sys,unittest
from pathlib import Path
HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE.parent if HERE.name=='test' else HERE))
from build_sales_target_monthly_input import recorded_period_agreement,input_values,produce_monthly_input


class ExistingSourceEvidenceTest(unittest.TestCase):
    def record(self,owner='OLD'):
        return dict(CUSTOMER_NO='C',COUNTRY='TH',PORT='BKK',SALESMAN_NO=owner,SALES_START_DATE='20250101',SALES_END_DATE='99991231')
    def test_unchanged_record_requires_both_rosters(self):
        pairs={('TH','OLD')}
        approved,blocked=recorded_period_agreement([self.record()],[self.record()],'202607',pairs,pairs,lambda country,port:country)
        self.assertEqual(approved['C'][0],('TH','OLD'))
        self.assertEqual(blocked,{})
        approved,blocked=recorded_period_agreement([self.record()],[self.record()],'202607',set(),pairs,lambda country,port:country)
        self.assertFalse(approved)
        self.assertEqual(blocked['C'],'archived_or_current_office_roster_missing')
    def test_changed_owner_is_not_backdated(self):
        approved,blocked=recorded_period_agreement([self.record()],[self.record('NEW')],'202607',{('TH','OLD')},{('TH','NEW')},lambda country,port:country)
        self.assertFalse(approved)
        self.assertEqual(blocked['C'],'changed_record_effective_month_unconfirmed')
    def test_multiple_or_observed_intervals_are_not_assumed_verified(self):
        current=self.record();current['OWNER_DATE_BASIS']='observed_only_effective_unknown'
        approved,blocked=recorded_period_agreement([self.record()],[current],'202607',{('TH','OLD')},{('TH','OLD')},lambda country,port:country)
        self.assertEqual(blocked['C'],'observed_date_only')
        approved,blocked=recorded_period_agreement([self.record()],[self.record(),self.record()],'202607',set(),set(),lambda country,port:country)
        self.assertEqual(blocked['C'],'missing_or_multiple_periods')
    def test_input_zero_stays_zero_and_blank_stays_missing(self):
        self.assertEqual(input_values([[],[],[],['TH',0,'',None,'',.05]],lambda v:None if v is None else float(v)),{'TH':(0.,None,.05)})

    def test_existing_source_producer_feeds_generator_without_new_input_file(self):
        import pandas as pd
        from types import SimpleNamespace
        from sales_target_monthly import attach_verified_target_source
        months=('202607','202608','202609')
        row=self.record();pairs={('TH','OLD')}
        deltas={'TH':(.1,.05,.05)}
        base=pd.DataFrame([dict(tab='TH',customer='C',dest='SG',dst_port='SIN',nlst=100.,w3f=50.,w3l=35.,w3h=20.)])
        live=pd.DataFrame([dict(tab='TH',customer='C',dest='SG',dst_port='SIN',YYYYMM=month,w3f=30.) for month in months])
        bsa=pd.DataFrame([dict(tab='TH',dest='SG',dst_port='SIN',YYYYMM='202501',route_bsa=100.)]+[dict(tab='TH',dest='SG',dst_port='SIN',YYYYMM=month,route_bsa=200.) for month in months])
        evidence=dict(base=base,live=live,bsa=bsa,months=months,dataset_id='20261008',baseline=[row],current=[row],
                      current_roster=pairs,archives={'20260818':pairs,'20260930':pairs},archive_inputs={'20260818':deltas,'20260930':deltas},
                      deltas=deltas,totals={'TH':{'booking':.5,'lifting':.7,'high_profit':.4}})
        source,report=produce_monthly_input(SimpleNamespace(tab_key=lambda c,p:c,DEFAULT_WORKBOOK='fixture'),evidence)
        self.assertEqual(source['verification'],'confirmed')
        self.assertFalse(source['basis_reconciliation_required'])
        rows=[dict(tab='TH',name='Team Total',row_type='TOTAL',kpi={}),dict(tab='TH',name='OLD',row_type='SALES',kpi={})]
        attach_verified_target_source(rows,source)
        self.assertAlmostEqual(rows[0]['kpi']['booking']['q3']['target'],.6)
        self.assertAlmostEqual(rows[1]['kpi']['lifting']['q3']['target'],.75)
        self.assertAlmostEqual(rows[1]['kpi']['high_profit']['q3']['target'],.45)


if __name__=='__main__':unittest.main()
