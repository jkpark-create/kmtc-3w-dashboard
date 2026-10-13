import copy,sys,unittest,json
from pathlib import Path
HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE.parent if HERE.name=='test' else HERE))
from sales_target_monthly import attach_verified_target_source


class MonthlySourceIntegrationTest(unittest.TestCase):
    def source(self):
        source={'format':'sales-target-monthly-input-v1','verification':'confirmed','unresolved_customers':[],
                'owner_intervals':[dict(CUSTOMER_NO='C',SALESMAN_NO='OLD',SALES_START_DATE='20250101',SALES_END_DATE='20260814'),
                                   dict(CUSTOMER_NO='C',SALESMAN_NO='NEW',SALES_START_DATE='20260815',SALES_END_DATE='99991231')], 'months':{}}
        for month,num in [('202607',50),('202608',60),('202609',90)]:
            source['months'][month]={'TH':{'approved_salespeople':['OLD','NEW'],
                'kpis':{key:{'team_numerator':num,'shipper_measures':[{'customer':'C','denominator':100}]} for key in ('booking','lifting','high_profit')}}}
        return source

    def rows(self):
        return [{'tab':'TH','name':'Team Total','row_type':'TOTAL','kpi':{}},
                {'tab':'TH','name':'OLD','row_type':'SALES','kpi':{}},
                {'tab':'TH','name':'NEW','row_type':'SALES','kpi':{}}]

    def test_history_allocation_index_and_quarter_measures(self):
        rows=self.rows(); owners,rosters=attach_verified_target_source(rows,self.source())
        self.assertEqual(owners['202607']['C'],'OLD')
        self.assertEqual(owners['202608']['C'],'NEW')
        old=rows[1]['month_targets'];new=rows[2]['month_targets']
        self.assertEqual(old['202607']['booking']['numerator'],50)
        self.assertEqual(new['202607']['booking']['numerator'],0)
        self.assertEqual(new['202608']['booking']['numerator'],60)
        self.assertAlmostEqual(rows[0]['kpi']['booking']['q3']['target'],2/3)
        self.assertEqual(rows[0]['kpi']['booking']['q3']['numerator'],200)
        self.assertEqual(rows[0]['kpi']['booking']['q3']['denominator'],300)
        self.assertEqual(rows[2]['kpi']['booking']['q3']['target'],.75)
        again=copy.deepcopy(rows);attach_verified_target_source(again,self.source())
        self.assertEqual(again,rows)

    def test_explicit_zero_not_overwritten(self):
        rows=self.rows();rows[2]['kpi']={'booking':{'q3':{'target':0}}}
        attach_verified_target_source(rows,self.source())
        self.assertEqual(rows[2]['kpi']['booking']['q3']['target'],0)
        self.assertEqual(rows[2]['kpi']['booking']['q3']['target_source'],'explicit')

    def test_unresolved_source_has_no_fallback(self):
        source=self.source();source['unresolved_customers']=['C']
        rows=self.rows()
        with self.assertRaisesRegex(ValueError,'awaits a decision'):
            attach_verified_target_source(rows,source)
        self.assertNotIn('month_targets',rows[0])

    def test_recalculation_keeps_frozen_earlier_month_targets(self):
        rows=self.rows();source=self.source()
        attach_verified_target_source(rows,source)
        july=copy.deepcopy(rows[0]['month_targets']['202607'])
        source['recalculate_from_month']='202608'
        for spec in source['months']['202607']['TH']['kpis'].values(): spec['team_numerator']=99
        attach_verified_target_source(rows,source)
        self.assertEqual(rows[0]['month_targets']['202607'],july)

    def test_real_generator_reads_drive_in_memory_and_attaches_rates(self):
        import build_sales_target_drill_data as generator
        from unittest.mock import MagicMock,patch
        service=MagicMock()
        service.files.return_value.get_media.return_value.execute.return_value=json.dumps(self.source()).encode('utf-8')
        with patch.object(generator,'build',return_value=service):
            source=generator.fetch_verified_monthly_target_source(object(),'verified-source-id')
        service.files.return_value.get_media.assert_called_once_with(fileId='verified-source-id')
        rows=self.rows();attach_verified_target_source(rows,source)
        month_metrics={'TH':{'OLD':{'202607':{'w3f':40,'w3l':30,'w3h':20,'bsa':100}},
                             'NEW':{'202608':{'w3f':50,'w3l':30,'w3h':20,'bsa':100},'202609':{'w3f':60,'w3l':30,'w3h':20,'bsa':100}}}}
        # Existing workbook rows normally contain Q1/Q2; fill fixture shells.
        for row in rows:
            for key in ('booking','lifting','high_profit'):
                for q in ('q1','q2'):
                    row['kpi'][key][q]={'target':None,'perform' if q=='q1' else 'progress':None,'gap':None}
        generator.attach_missing_quarter_progress(rows,month_metrics)
        generator.attach_quarter_metrics(rows,month_metrics)
        self.assertAlmostEqual(rows[0]['kpi']['booking']['q3']['target'],2/3)
        self.assertEqual(rows[0]['kpi']['booking']['q3']['progress'],.5)
        self.assertTrue(generator.target_period_availability(rows)['q3'])
        self.assertFalse(generator.target_period_availability(rows)['q4'])


if __name__=='__main__':unittest.main()
