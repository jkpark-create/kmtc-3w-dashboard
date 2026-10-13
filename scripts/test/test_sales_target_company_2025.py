import sys,unittest,copy
from pathlib import Path
import pandas as pd
from types import SimpleNamespace
HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE.parent if HERE.name=='test' else Path(r'C:\Users\JKPARK\OneDrive\Documents\Claude\-3W bkg dashboard\scripts')))
from sales_target_company_2025 import build_company_2025_source,official_priority_roster

class Company2025Test(unittest.TestCase):
    def evidence(self):
        row=dict(CUSTOMER_NO='C',COUNTRY='PORT_COUNTRY',PORT='PORT',SALESMAN_NO='OWNER',SALES_START_DATE='20250101',SALES_END_DATE='99991231')
        months=('202607','202608','202609','202610','202611','202612')
        base=pd.DataFrame([dict(tab='PHYSICAL',customer='C',dest='SG',dst_port='SIN',YYYYMM='2025'+m[-2:],nlst=100.,w3f=50.,w3l=0.,w3h=20.) for m in months])
        live=pd.DataFrame([dict(tab='PHYSICAL',customer='C',YYYYMM=m,w3f=99999.,Salesman_POR='OWNER') for m in months])
        bsa=pd.DataFrame([dict(tab='PHYSICAL',dest='SG',dst_port='SIN',YYYYMM=y+m[-2:],route_bsa=100.) for y in ('2025','2026') for m in months])
        pairs={('WRONG_OLD_OFFICE','OWNER')}
        return dict(base=base,live=live,bsa=bsa,months=months,dataset_id='20261012',baseline=[row],current=[row],current_roster=pairs,
            official_roster={('OFFICIAL','OWNER')},official_roster_meta=dict(verification='confirmed',modified_time='2026-10-12'),
            archives={'20260818':pairs,'20260930':pairs},deltas={'PHYSICAL':(.1,.05,.05),'OFFICIAL':(.1,.05,.05)},
            totals={'PHYSICAL':{'booking':None,'lifting':None,'high_profit':None},'OFFICIAL':{'booking':None,'lifting':None,'high_profit':None}})
    def source(self,e):return build_company_2025_source(SimpleNamespace(tab_key=lambda c,p:c,DEFAULT_WORKBOOK='fixture'),e)[0]
    def test_latest_official_office_wins_without_changing_physical_bsa(self):
        s=self.source(self.evidence());self.assertEqual(s['verification'],'confirmed')
        for month in s['months']:
            ledger=s['bsa_route_goal_ledger'][month]
            self.assertEqual(ledger['totals']['raw'],100)
            self.assertEqual(ledger['assigned'][0]['tab'],'OFFICIAL')
            self.assertEqual(ledger['assigned'][0]['physical_tab'],'PHYSICAL')
    def test_2026_actuals_cannot_change_any_goals_including_december(self):
        e=self.evidence();before=self.source(e)
        e['live']['w3f']=0.
        self.assertEqual(before['months'],self.source(e)['months'])
        lifting=before['months']['202612']['OFFICIAL']['kpis']['lifting']
        self.assertAlmostEqual(lifting['team_numerator'],2.5)
        self.assertEqual(lifting['shipper_measures'][0]['company_2025_numerator'],0)
    def test_zero_quantum_and_zero_past_numerator_stay_valid_zero(self):
        e=self.evidence();e['deltas']['OFFICIAL']=(0.,0.,0.)
        spec=self.source(e)['months']['202612']['OFFICIAL']['kpis']['lifting']
        self.assertEqual(spec['state'],'known');self.assertEqual(spec['team_numerator'],0)
    def test_missing_quantum_is_a_blocker_not_authorized_no_history(self):
        e=self.evidence();e['deltas']['OFFICIAL']=(.1,None,.05)
        s=self.source(e);self.assertEqual(s['verification'],'pending_history')
    def test_missing_month_source_differs_from_zero_month_activity(self):
        e=self.evidence();e['base']=e['base'].loc[e['base'].YYYYMM.ne('202512')]
        self.assertEqual(self.source(e)['verification'],'pending_history')
        e=self.evidence();e['base'].loc[e['base'].YYYYMM.eq('202512'),'w3f']=0.
        spec=self.source(e)['months']['202612']['OFFICIAL']['kpis']['lifting']
        self.assertEqual(spec['state'],'no_historical_month_weight')
    def test_unassigned_share_is_reserved_without_redistribution(self):
        e=self.evidence();extra=e['base'].copy();extra['customer']='U';e['base']=pd.concat([e['base'],extra])
        s=self.source(e);ledger=s['bsa_route_goal_ledger']['202612']
        self.assertEqual(ledger['totals']['assigned'],50);self.assertEqual(ledger['totals']['reserved'],50)
        self.assertEqual(ledger['totals']['raw'],100)
        self.assertAlmostEqual(s['months']['202612']['OFFICIAL']['kpis']['booking']['team_numerator'],55)
    def test_source_requires_verified_official_membership(self):
        e=self.evidence();e['official_roster_meta']['verification']='unknown'
        with self.assertRaises(ValueError):self.source(e)
    def test_attach_preserves_manual_zero_and_typed_blank_reasons(self):
        from sales_target_monthly import attach_verified_target_source,quarter_target,blank_target_cell
        e=self.evidence();e['base']['w3f']=0.
        s=self.source(e)
        rows=[dict(tab=o,name='Total',row_type='TOTAL',kpi={}) for o in e['totals']]
        rows.append(dict(tab='OFFICIAL',name='OWNER',row_type='SALES',kpi={'booking':{'q4':{'target':0}}}))
        attach_verified_target_source(rows,s)
        owner=rows[-1]
        self.assertEqual(owner['kpi']['booking']['q4']['target'],0)
        self.assertIsNone(owner['kpi']['lifting']['q4']['target'])
        self.assertEqual(owner['kpi']['lifting']['q4']['basis_state'],'zero_baseline_denominator')
        self.assertIsNone(owner['kpi']['lifting']['q4']['denominator'])
        mixed=quarter_target([dict(numerator=0.,denominator=50.),blank_target_cell('no_historical_performance')])
        self.assertEqual(mixed['target'],0.)
        self.assertEqual(mixed['coverage'],'known_with_verified_blanks')
        with self.assertRaises(ValueError):quarter_target([dict(numerator=None,denominator=None,basis_state='source_missing')])
    def test_live_and_history_offices_keep_physical_scope_and_quantities(self):
        import build_sales_target_drill_data as b
        from sales_target_monthly import attach_verified_target_source
        e=self.evidence();s=self.source(e)
        rows=[dict(tab=o,name='Total',row_type='TOTAL',kpi={}) for o in e['totals']]
        maps,rosters=attach_verified_target_source(rows,s)
        frame=pd.DataFrame([dict(tab='PHYSICAL',team='OBT',YYYYMM='202612',BKG_SHPR_CST_NO='C',Salesman_POR='OWNER',Target_Eligible=True,
            fst_teu_num=10.,norm_lst_teu_num=10.,wos_norm_lst_teu_num=10.)])
        b.apply_monthly_owner_maps(frame,maps,s)
        self.assertEqual(frame.tab.tolist(),['OFFICIAL']);self.assertEqual(frame.physical_tab.tolist(),['PHYSICAL'])
        self.assertEqual(frame.fst_teu_num.sum(),10);self.assertTrue(b.sales_target_scope_mask(frame).all())
        history=pd.DataFrame([dict(tab='JP',team='JBT',BKG_SHPR_CST_NO='C',Salesman_POR='OWNER')])
        b.apply_official_history_offices(history,{'C':['OFFICIAL']},{'C':'OWNER'})
        self.assertEqual(history.tab.tolist(),['OFFICIAL']);self.assertTrue(b.target_team_mask(history).all())
        allocations=b.monthly_source_bsa_allocations(s)
        quantity,serialized=b.serialize_bsa_allocations(allocations.loc[allocations.YYYYMM.eq('202612')])
        self.assertEqual(quantity,100);self.assertEqual(serialized[0]['shipper_no'],'C')
        self.assertEqual(serialized[0]['physical_origin'],'PHYSICAL')

if __name__=='__main__':unittest.main()
