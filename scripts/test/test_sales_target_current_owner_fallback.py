import sys,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from build_sales_target_monthly_input import resolve_month_owners
from sales_target_monthly import attach_verified_target_source

class CurrentOwnerFallbackTest(unittest.TestCase):
    def record(self,owner,start='20250101',end='99991231',**extra):
        return dict(CUSTOMER_NO='C',COUNTRY='TH',PORT='BKK',SALESMAN_NO=owner,SALES_START_DATE=start,SALES_END_DATE=end,**extra)
    def resolve(self,old,new,month='202607'):
        return resolve_month_owners(old,new,month,'20261008',{('TH','OLD')},{('TH','NEW')},lambda c,p:c)
    def test_unknown_change_uses_current_source_without_backdated_event(self):
        approved,blocked,audit=self.resolve([self.record('OLD')],[self.record('NEW',OWNER_DATE_BASIS='observed_only_effective_unknown')])
        self.assertEqual(approved['C'][0],('TH','NEW'));self.assertFalse(blocked)
        self.assertEqual(audit[0]['basis'],'current_owner_fallback');self.assertIsNone(audit[0]['effective_date'])
    def test_confirmed_change_preserves_earlier_month(self):
        incoming=self.record('NEW','20260815',OWNER_DATE_BASIS='confirmed_effective',OWNER_EFFECTIVE_DATE='20260815')
        for month,expected in [('202607','OLD'),('202608','NEW'),('202609','NEW')]:
            approved,blocked,audit=self.resolve([self.record('OLD')],[incoming],month)
            self.assertEqual(approved['C'][0][1],expected);self.assertFalse(blocked)
            self.assertEqual(audit[0]['basis'],'confirmed_effective_change')
    def test_ambiguous_current_source_is_not_invented(self):
        approved,blocked,audit=self.resolve([self.record('OLD')],[self.record('OLD'),self.record('NEW')])
        self.assertFalse(approved);self.assertEqual(blocked['C'],'current_source_owner_missing_or_ambiguous')
    def test_expired_source_does_not_become_current_owner(self):
        approved,blocked,audit=self.resolve([self.record('OLD')],[self.record('NEW',end='20260831')])
        self.assertFalse(approved);self.assertFalse(audit)
    def test_blank_office_resolves_with_origin_roster(self):
        from build_sales_target_monthly_input import owner_matches_origin
        row=self.record('NEW');row.update(COUNTRY='',PORT='')
        import build_sales_target_drill_data as generator
        approved,blocked,audit=resolve_month_owners([self.record('OLD')],[row],'202607','20261012',{('TH','OLD')},{('TH','NEW')},generator.tab_key)
        self.assertFalse(blocked)
        self.assertTrue(owner_matches_origin(approved['C'],'TH',{('TH','NEW')}))
        self.assertFalse(owner_matches_origin(approved['C'],'VN',{('TH','NEW')}))
    def test_confirmed_history_with_blank_office_keeps_previous_owner(self):
        import build_sales_target_drill_data as generator
        prior=self.record('OLD');prior.update(COUNTRY='',PORT='')
        incoming=self.record('NEW','20260815',OWNER_DATE_BASIS='confirmed_effective',OWNER_EFFECTIVE_DATE='20260815');incoming.update(COUNTRY='',PORT='')
        for month,owner in [('202607','OLD'),('202608','NEW')]:
            approved,blocked,audit=resolve_month_owners([prior],[incoming],month,'20261012',{('TH','OLD')},{('TH','NEW')},generator.tab_key)
            self.assertEqual(approved['C'][0][1],owner);self.assertFalse(blocked)
            self.assertEqual(audit[0]['basis'],'confirmed_effective_change')
    def test_unknown_start_date_uses_unique_current_owner_without_invented_date(self):
        approved,blocked,audit=self.resolve([self.record('OLD')],[self.record('NEW','2017060')])
        self.assertFalse(blocked);self.assertEqual(approved['C'][1]['SALES_START_DATE'],'2017060')
        self.assertIsNone(audit[0]['effective_date'])
    def test_existing_activity_lane_fallback_conserves_capacity_and_excludes_new(self):
        import pandas as pd
        from build_sales_target_monthly_input import add_existing_activity_lane_basis
        routes=pd.DataFrame(columns=['tab','dest','dst_port','customer','nlst','share'])
        live=pd.DataFrame([dict(tab='TH',dest='AE',dst_port='KLF',customer=c,nlst=n) for c,n in [('C',30),('D',70),('NEW',1000)]])
        shares=add_existing_activity_lane_basis(routes,live,{'NEW'})
        self.assertEqual(set(shares.customer),{'C','D'});self.assertAlmostEqual(shares.share.sum(),1)
        before=add_existing_activity_lane_basis(routes,live.loc[live.customer.ne('NEW')],set())
        self.assertEqual(before.to_dict('records'),shares.to_dict('records'))

    def test_confirmed_change_uses_period_roster_not_customer_booking_port(self):
        from build_sales_target_monthly_input import owner_matches_origin
        prior=self.record('OLD');prior.update(COUNTRY='OTHER')
        incoming=self.record('NEW','20260815',OWNER_DATE_BASIS='confirmed_effective',OWNER_EFFECTIVE_DATE='20260815')
        incoming.update(COUNTRY='OTHER')
        for month,origin,owner in [('202607','OLD_OFFICE','OLD'),('202608','NEW_OFFICE','NEW')]:
            approved,blocked,audit=resolve_month_owners([prior],[incoming],month,'20261012',{('OLD_OFFICE','OLD')},{('NEW_OFFICE','NEW')},lambda c,p:c)
            self.assertFalse(blocked);self.assertEqual(approved['C'][0],(origin,owner))
            self.assertTrue(owner_matches_origin(approved['C'],origin,{('OLD_OFFICE','OLD'),('NEW_OFFICE','NEW')}))
            self.assertFalse(owner_matches_origin(approved['C'],'OTHER',{('OTHER',owner)}))

    def test_monthly_map_reaches_quarter_and_keeps_zero(self):
        months=('202607','202608','202609')
        specs={key:{'team_numerator':60,'shipper_measures':[dict(customer='C',denominator=100,basis_numerator=60)],'explicit_rates':{}} for key in ('booking','lifting','high_profit')}
        source=dict(format='sales-target-monthly-input-v1',verification='confirmed',owner_intervals=[],unresolved_customers=[],
                    owner_maps_by_month={m:{'C':'OLD' if m=='202607' else 'NEW'} for m in months},
                    months={m:{'TH':dict(approved_salespeople=['OLD','NEW'],kpis=specs)} for m in months})
        rows=[dict(tab='TH',name='Total',row_type='TOTAL',kpi={}),dict(tab='TH',name='NEW',row_type='SALES',kpi={'booking':{'q3':{'target':0}}})]
        maps,rosters=attach_verified_target_source(rows,source)
        self.assertEqual(rows[1]['kpi']['booking']['q3']['target'],0)
        self.assertEqual(rows[0]['kpi']['booking']['q3']['numerator'],180)
        self.assertEqual(rows[0]['kpi']['booking']['q3']['denominator'],300)
        self.assertEqual(maps['202607']['C'],'OLD')

if __name__=='__main__':unittest.main()
