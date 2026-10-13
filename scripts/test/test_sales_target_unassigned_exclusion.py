import unittest,sys,copy
from pathlib import Path
import pandas as pd
HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE.parent if HERE.name=='test' else Path(r'C:\Users\JKPARK\OneDrive\Documents\Claude\-3W bkg dashboard\scripts')))
import build_sales_target_drill_data as generator
from build_sales_target_monthly_input import produce_monthly_input,resolve_month_owners,monthly_bsa_ledger
from sales_target_monthly import attach_verified_target_source
from types import SimpleNamespace

class UnassignedExclusionTest(unittest.TestCase):
    def evidence(self,missing_actuals=True):
        months=('202607','202608','202609');pairs={('TH','OLD')};pp={'TH':(.1,.05,.05)}
        owner=dict(CUSTOMER_NO='C',COUNTRY='TH',PORT='BKK',SALESMAN_NO='OLD',SALES_START_DATE='20250101',SALES_END_DATE='99991231')
        base=pd.DataFrame([dict(tab='TH',customer=c,dest='SG',dst_port='SIN',nlst=100.,w3f=50.,w3l=35.,w3h=20.) for c in ('C','U')])
        rows=[dict(tab='TH',customer='C',dest='SG',dst_port='SIN',YYYYMM=m,nlst=30.,w3f=30.,Salesman_POR='OLD') for m in months]
        if missing_actuals:rows.extend(dict(tab='TH',customer='U',dest='SG',dst_port='SIN',YYYYMM=m,nlst=100.,w3f=100.,Salesman_POR='') for m in months)
        bsa=pd.DataFrame([dict(tab='TH',dest='SG',dst_port='SIN',YYYYMM='202501',route_bsa=200.)]+[dict(tab='TH',dest='SG',dst_port='SIN',YYYYMM=m,route_bsa=200.) for m in months])
        return dict(base=base,live=pd.DataFrame(rows),bsa=bsa,months=months,dataset_id='20261012',baseline=[owner],current=[owner],current_roster=pairs,
                    archives={'20260818':pairs,'20260930':pairs},archive_inputs={'20260818':pp,'20260930':pp},deltas=pp,
                    totals={'TH':{'booking':.5,'lifting':.7,'high_profit':.4}})
    def produce(self,e):return produce_monthly_input(SimpleNamespace(tab_key=lambda c,p:c,DEFAULT_WORKBOOK='fixture'),e)[0]
    def test_missing_customer_actuals_never_inflate_assigned_goals(self):
        before=self.produce(self.evidence(False));after=self.produce(self.evidence(True))
        self.assertEqual(after['verification'],'confirmed');self.assertFalse(after['unresolved_customers'])
        self.assertEqual(before['months'],after['months'])
        self.assertEqual(after['excluded_unassigned_by_month']['202607']['TH'],['U'])
        for audit in after['goal_exclusion_audit']:
            self.assertAlmostEqual(audit['reference_before_exclusion_numerator'],audit['excluded_reference_numerator']+audit['assigned_numerator'])
            self.assertEqual(audit['redistributed_excluded_quantity'],0)
        booking=next(a for a in after['goal_exclusion_audit'] if a['month']=='202607' and a['kpi']=='booking')
        self.assertAlmostEqual(booking['reference_before_exclusion_numerator'],120)
        self.assertAlmostEqual(booking['assigned_numerator'],60);self.assertAlmostEqual(booking['excluded_reference_numerator'],60)
    def test_route_shares_and_raw_capacity_are_preserved_not_redistributed(self):
        source=self.produce(self.evidence())
        frame=generator.monthly_source_bsa_allocations(source)
        for month in ('202607','202608','202609'):
            selected=frame.loc[frame.YYYYMM.eq(month)]
            self.assertEqual(selected.allocated_bsa.sum(),200)
            self.assertEqual(selected.loc[selected.Salesman_POR.eq('OLD'),'allocated_bsa'].sum(),100)
            self.assertEqual(selected.loc[selected.Salesman_POR.eq(generator.NO_BASIS_SALES),'allocated_bsa'].sum(),100)
        source['bsa_route_goal_ledger']['202607']['pending']=[{'allocated_bsa':1}]
        with self.assertRaises(ValueError):generator.monthly_source_bsa_allocations(source)
    def test_actuals_remain_no_goal_or_fabricated_owner_and_explicit_zero_survives(self):
        source=self.produce(self.evidence())
        rows=[dict(tab='TH',name='Team Total',row_type='TOTAL',kpi={}),dict(tab='TH',name='OLD',row_type='SALES',kpi={'booking':{'q3':{'target':0}}})]
        maps,rosters=attach_verified_target_source(rows,source)
        frame=pd.DataFrame([dict(tab='TH',team='OBT',YYYYMM='202607',BKG_SHPR_CST_NO=c,Salesman_POR=owner,Target_Eligible=True,fst_teu_num=10,norm_lst_teu_num=10,wos_norm_lst_teu_num=10) for c,owner in [('C','OLD'),('U',generator.MISSING_SALES)]])
        generator.apply_monthly_owner_maps(frame,maps,source)
        self.assertEqual(frame.Salesman_POR.tolist(),['OLD',generator.MISSING_SALES])
        self.assertEqual(frame.Target_Eligible.tolist(),[True,False]);self.assertEqual(len(frame),2)
        self.assertEqual(frame.Goal_Exclusion_Reason.tolist(),['','unassigned_current_owner'])
        import json
        json.dumps(frame.Goal_Exclusion_Reason.tolist(),allow_nan=False)
        self.assertEqual(rows[1]['kpi']['booking']['q3']['target'],0)
        generator.ensure_performance_only_rows(rows,frame)
        excluded=next(r for r in rows if r['name']==generator.MISSING_SALES)
        self.assertEqual(excluded['goal_exclusion_reason'],'unassigned_current_owner')
        self.assertTrue(all(cell['target'] is None for kpi in excluded['kpi'].values() for cell in kpi.values()))
    def test_excluded_customer_cannot_reenter_a_verified_goal_source(self):
        source=self.produce(self.evidence())
        source['months']['202607']['TH']['kpis']['booking']['shipper_measures'].append(dict(customer='U',denominator=100,basis_numerator=60))
        with self.assertRaisesRegex(ValueError,'Excluded unassigned'):
            attach_verified_target_source([dict(tab='TH',name='Team Total',row_type='TOTAL',kpi={})],source)
    def test_conflicting_owner_still_blocks_and_missing_base_guard_is_unchanged(self):
        e=self.evidence();conflict=dict(e['current'][0]);conflict['SALESMAN_NO']='OTHER';e['current'].append(conflict)
        source=self.produce(e)
        self.assertEqual(source['verification'],'pending_history')
        self.assertTrue(any(r['customer']=='C' for r in source['unresolved_customers']))
        e=self.evidence();e['totals']['TH']['lifting']=None
        source=self.produce(e)
        self.assertEqual(source['verification'],'pending_history');self.assertEqual(source['months']['202607'],{})
    def test_ambiguous_history_cannot_use_a_removed_office_roster(self):
        from build_sales_target_monthly_input import owner_matches_origin
        row=self.evidence()['current'][0];row=dict(row);row.update(COUNTRY='',PORT='',SALESMAN_NO='CURRENT')
        old={('OLD_OFFICE','CURRENT')};current={('TH','CURRENT')}
        approved,blocked,audit=resolve_month_owners([], [row], '202607','20261012',old,current,generator.tab_key)
        self.assertTrue(owner_matches_origin(approved['C'],'TH',old|current))
        self.assertFalse(owner_matches_origin(approved['C'],'OLD_OFFICE',old|current))
    def test_pending_owner_quantities_are_not_reported_as_assigned(self):
        e=self.evidence();e['current'][0]['SALESMAN_NO']='NOT_IN_ROSTER'
        source=self.produce(e)
        for row in source['goal_exclusion_audit']:
            self.assertEqual(row['assigned_numerator'],0)
            self.assertAlmostEqual(row['reference_before_exclusion_numerator'],row['excluded_reference_numerator']+row['pending_reference_numerator'])
    def test_booking_por_in_customer_source_is_not_employee_affiliation(self):
        from build_sales_target_monthly_input import owner_matches_origin
        row=self.evidence()['current'][0];row=dict(row);row.update(COUNTRY='VN',PORT='HPH')
        approved,blocked,audit=resolve_month_owners([], [row], '202607','20261012',set(),{('TH','OLD')},generator.tab_key)
        self.assertFalse(blocked);self.assertEqual(approved['C'][0],('TH','OLD'))
        self.assertTrue(owner_matches_origin(approved['C'],'TH',{('TH','OLD')}))
        self.assertFalse(owner_matches_origin(approved['C'],'VN_HPH',{('TH','OLD')}))
    def test_confirmed_new_remains_excluded_from_goals(self):
        e=self.evidence();e['customer_classes']={'U':{'class':'confirmed_new'}}
        source=self.produce(e)
        self.assertTrue(all(c['customer']!='U' for origins in source['months'].values() for inputs in origins.values() for spec in inputs['kpis'].values() for c in spec['shipper_measures']))

if __name__=='__main__':unittest.main()
