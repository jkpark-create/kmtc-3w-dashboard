import unittest,sys
from pathlib import Path
HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE.parent if HERE.name=='test' else HERE))
from sales_target_customer_cohorts import classify_customer


class CustomerCohortTest(unittest.TestCase):
    def test_august_snapshot_absence_does_not_define_new(self):
        self.assertEqual(classify_customer('C',set())['class'],'history_unconfirmed')
    def test_verified_registration_with_no_2025_history_is_new(self):
        result=classify_customer('C',set(),{'registered_date':'20260701103000','verification':'confirmed'},'2026-07-03')
        self.assertEqual(result['class'],'confirmed_new')
    def test_baseline_or_older_business_evidence_prevents_false_new(self):
        evidence={'registered_date':'20260101','older_business_date':'20170101','verification':'confirmed'}
        self.assertEqual(classify_customer('C',{'C'},evidence)['class'],'existing')
        self.assertEqual(classify_customer('C',set(),evidence)['class'],'existing')
    def test_late_master_creation_does_not_backdate_new_status(self):
        self.assertEqual(classify_customer('C',set(),{'registered_date':'20260901','verification':'confirmed'},'20260701')['class'],'history_unconfirmed')

    def test_existing_older_registration_is_not_unclassified_new(self):
        self.assertEqual(classify_customer('C',set(),{'registered_date':'20170101','verification':'confirmed'})['class'],'existing')

    def test_new_customer_addition_preserves_existing_goals_and_includes_performance(self):
        import pandas as pd
        from types import SimpleNamespace
        from build_sales_target_monthly_input import produce_monthly_input
        from sales_target_monthly import attach_verified_target_source
        months=('202607','202608','202609');pairs={('TH','OLD')}
        owner=dict(CUSTOMER_NO='C',COUNTRY='TH',PORT='BKK',SALESMAN_NO='OLD',SALES_START_DATE='20250101',SALES_END_DATE='99991231')
        base=pd.DataFrame([dict(tab='TH',customer='C',dest='SG',dst_port='SIN',nlst=100.,w3f=50.,w3l=35.,w3h=20.)])
        live=pd.DataFrame([dict(tab='TH',customer='C',dest='SG',dst_port='SIN',YYYYMM=m,w3f=30.,Salesman_POR='OLD') for m in months])
        bsa=pd.DataFrame([dict(tab='TH',dest='SG',dst_port='SIN',YYYYMM='202501',route_bsa=100.)]+[dict(tab='TH',dest='SG',dst_port='SIN',YYYYMM=m,route_bsa=200.) for m in months])
        inputs={'TH':(.1,.05,.05)}
        evidence=dict(base=base,live=live,bsa=bsa,months=months,dataset_id='20261008',baseline=[owner],current=[owner],current_roster=pairs,
                      archives={'20260818':pairs,'20260930':pairs},archive_inputs={'20260818':inputs,'20260930':inputs},deltas=inputs,
                      totals={'TH':{'booking':.5,'lifting':.7,'high_profit':.4}})
        builder=SimpleNamespace(tab_key=lambda c,p:c,DEFAULT_WORKBOOK='fixture')
        before,_=produce_monthly_input(builder,evidence)
        additions=pd.DataFrame([dict(tab='TH',customer='N',dest='SG',dst_port='SIN',YYYYMM=m,w3f=100.,Salesman_POR='OLD' if m=='202607' else 'NEW') for m in months])
        evidence['live']=pd.concat([live,additions],ignore_index=True)
        evidence['customer_classes']={'N':{'class':'confirmed_new'}}
        observed_only,_=produce_monthly_input(builder,evidence)
        self.assertEqual(observed_only['verification'],'pending_history')
        self.assertEqual(observed_only['performance_only_by_month']['202607'],{})
        self.assertEqual(observed_only['performance_observations_by_month']['202607']['TH']['N'],'OLD')
        self.assertEqual(len(observed_only['unverified_new_performance_owner_groups']),3)
        evidence['verified_performance_owners']={m:{'TH':{'N':'OLD' if m=='202607' else 'NEW'}} for m in months}
        after,report=produce_monthly_input(builder,evidence)
        self.assertEqual(before['months'],after['months'])
        self.assertEqual(after['unresolved_customers'],[])
        self.assertEqual(report[0]['performance_only_w3_fst_teu'],100)
        rows=[dict(tab='TH',name='Team Total',row_type='TOTAL',kpi={}),dict(tab='TH',name='OLD',row_type='SALES',kpi={})]
        maps,rosters=attach_verified_target_source(rows,after)
        self.assertEqual(after['performance_only_by_month']['202607']['TH']['N'],'OLD')
        self.assertEqual(after['performance_only_by_month']['202608']['TH']['N'],'NEW')
        new_row=next(row for row in rows if row['name']=='NEW')
        self.assertEqual(new_row['row_type'],'PERFORMANCE_ONLY')
        self.assertIsNone(new_row['kpi']['booking']['q3']['target'])
        self.assertAlmostEqual(rows[0]['kpi']['booking']['q3']['target'],.6)
        import build_sales_target_drill_data as generator
        frame=pd.DataFrame([dict(tab='TH',team='OBT',BKG_SHPR_CST_NO='N',YYYYMM=m,Salesman_POR='OLD',Target_Eligible=False,
                                fst_teu_num=100,norm_lst_teu_num=80,wos_norm_lst_teu_num=80) for m in months])
        generator.apply_monthly_owner_maps(frame,maps,after)
        self.assertEqual(frame.Salesman_POR.tolist(),['OLD','NEW','NEW'])

    def test_chunk_ratios_include_new_actuals_but_goal_weights_do_not(self):
        import pandas as pd
        import build_sales_target_drill_data as generator
        records=[]
        for customer,fst,lst,hi,eligible in [('OLD',40,28,True,True),('OLD',60,42,False,True),('NEW',100,80,True,False)]:
            row={name:'' for name in generator.SNAPSHOT_COLUMNS}
            row.update(BKG_SHPR_CST_NO=customer,BKG_SHPR_CST_ENM=customer,grade='A',fst_teu_num=fst,lst_teu_num=lst,
                       norm_lst_teu_num=lst,wos_norm_lst_teu_num=lst,cm1_num=0,norm_cm1_num=0,cm1_per_teu_num=0,
                       is_w3=True,is_hi=hi,is_route_hi=hi,is_normal=True,is_lifted=True,is_cancel=False,Target_Eligible=eligible)
            records.append(row)
        chunk=generator.build_chunk('TH','S','202607',pd.DataFrame(records),None)
        self.assertEqual(chunk['totals']['w3_fst'],200)
        self.assertEqual(chunk['totals']['goal_basis_w3_fst'],100)
        self.assertEqual(chunk['totals']['lst_rate_w3'],.75)
        self.assertEqual(chunk['totals']['hi_share_w3'],.7)
        self.assertFalse(next(s for s in chunk['shippers'] if s['shipper_no']=='NEW')['target_eligible'])


if __name__=='__main__':unittest.main()
