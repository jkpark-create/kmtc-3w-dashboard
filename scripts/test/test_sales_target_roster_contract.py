import sys,unittest,copy
from pathlib import Path
HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE.parent if HERE.name=='test' else HERE))
from sales_target_roster_contract import validate_rosters


class SplitRosterTest(unittest.TestCase):
    def payload(self):
        index={'rows':[{'tab':'TH','name':'GOAL','row_type':'SALES'},
                        {'tab':'TH','name':'ACTUAL','row_type':'PERFORMANCE_ONLY','target_eligible':False,'kpi':{'booking':{'q3':{'target':None}}}}],
               'current_target_roster_pairs':[['TH','GOAL']], 'historical_target_roster_pairs':[],
               'performance_only_roster_pairs':[['TH','ACTUAL']]}
        manifest={key:value for key,value in index.items() if key!='rows'}
        manifest['salespeople_by_origin']={'TH':['GOAL','ACTUAL']}
        return index,manifest
    def test_performance_only_person_does_not_replace_goal_roster(self):
        index,manifest=self.payload()
        goals,actuals=validate_rosters(index,manifest,{('TH','GOAL')})
        self.assertEqual(goals,{('TH','GOAL')});self.assertEqual(actuals,{('TH','ACTUAL')})
    def test_performance_only_target_even_zero_is_rejected(self):
        index,manifest=self.payload();index['rows'][1]['kpi']['booking']['q3']['target']=0
        with self.assertRaisesRegex(ValueError,'received a target'):validate_rosters(index,manifest,{('TH','GOAL')})
    def test_missing_goal_cannot_be_replaced_by_performance_row(self):
        index,manifest=self.payload();index['rows']=index['rows'][1:]
        with self.assertRaisesRegex(ValueError,'does not match'):validate_rosters(index,manifest,{('TH','GOAL')})
    def test_unlisted_performance_row_or_filter_is_rejected(self):
        index,manifest=self.payload();index['performance_only_roster_pairs']=[]
        with self.assertRaisesRegex(ValueError,'evidence missing'):validate_rosters(index,manifest,{('TH','GOAL')})
    def test_unverified_historical_goal_is_rejected(self):
        index,manifest=self.payload();index['historical_target_roster_pairs']=[['TH','OLD']]
        with self.assertRaisesRegex(ValueError,'not verified'):validate_rosters(index,manifest,{('TH','GOAL')})


if __name__=='__main__':unittest.main()
