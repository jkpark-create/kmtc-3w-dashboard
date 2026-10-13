import json,tempfile,unittest,sys,os
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parent))
import sales_target_release as release

class AtomicReleaseTest(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(); self.site=Path(self.temp.name)
        (self.site/'data').mkdir()
        index={'data_date':'20261012','target_basis_policy':'company_2025_actuals_official_office_attribution','monthly_target_source_verification':'confirmed','rows':[{'goal':0}]}
        manifest={'data_date':'20261012','chunks':[{'file':'data/TH__A__202610.json'}],'capacity_reserve_chunks':[{'file':'data/TH__reserve__202610.json'}]}
        for key,value in [('index',index),('manifest',manifest),('base2025',{'data_date':'20261012','base':{}})]:
            (self.site/(key+'.json')).write_text(json.dumps(value),encoding='utf-8')
        for name in ('TH__A__202610.json','TH__reserve__202610.json'):
            (self.site/'data'/name).write_text('{"target":0}',encoding='utf-8')
    def tearDown(self):self.temp.cleanup()
    def test_daily_generation_references_only_complete_immutable_files(self):
        pointer=release.prepare_release(self.site)
        entries=release.release_entries(self.site)
        self.assertEqual(len(entries['sales_data']),2)
        self.assertTrue(all(pointer['release_id'] in name for _,name in entries['sales_data']))
        self.assertEqual(release.read(entries['sales'][0][0])['rows'][0]['goal'],0)
    def test_changed_owner_goal_creates_a_new_generation(self):
        first=release.prepare_release(self.site)
        self.setUpNewGeneration({'goal':0,'owner':'B'})
        second=release.prepare_release(self.site)
        self.assertNotEqual(first['release_id'],second['release_id'])
    def setUpNewGeneration(self,row):
        index=release.read(self.site/'index.json');index.pop('release_id');index['rows']=[row]
        manifest={'data_date':'20261012','chunks':[{'file':'data/new.json'}]}
        (self.site/'data'/'new.json').write_text('{"target":0}',encoding='utf-8')
        for key,value in [('index',index),('manifest',manifest),('base2025',{'data_date':'20261012','base':{}})]:
            (self.site/(key+'.json')).write_text(json.dumps(value),encoding='utf-8')
    def test_mutated_chunk_blocks_pointer_update(self):
        release.prepare_release(self.site)
        entries=release.release_entries(self.site)
        entries['sales_data'][0][0].write_text('{"target":9}',encoding='utf-8')
        calls=[]
        with self.assertRaisesRegex(ValueError,'content changed'):
            release.publish_release_pointer('test','folder',self.site,lambda *a:[],lambda *a,**k:calls.append(k),lambda *a,**k:None,'api')
        self.assertEqual(calls,[])
    def test_missing_chunk_cannot_publish(self):
        (self.site/'data'/'TH__A__202610.json').unlink()
        with self.assertRaises(FileNotFoundError):release.prepare_release(self.site)
        self.assertFalse((self.site/'sales-target-release.json').exists())
    def test_pointer_readback_verified(self):
        pointer=release.prepare_release(self.site);calls=[]
        class Response:
            def json(self):return pointer
        result=release.publish_release_pointer('test','folder',self.site,lambda *a:[],lambda *a,**k: (calls.append(k) or {'id':'pointer'}),lambda *a,**k:Response(),'api')
        self.assertEqual(result['id'],'pointer');self.assertEqual(calls[0]['remote_name'],'sales-target-release.json')
    def run_daily_publisher(self,fail=False):
        import importlib.util
        pointer=release.prepare_release(self.site)
        sync_path=Path(__file__).with_name('sync_sales_target_release_only.py')
        if not sync_path.exists():sync_path=Path(__file__).resolve().parents[1]/'sync_dashboard_data_to_gdrive.py'
        spec=importlib.util.spec_from_file_location('scoped_daily_sync',sync_path)
        sync=importlib.util.module_from_spec(spec);spec.loader.exec_module(sync)
        # The hook's runtime path is virtualized to this fixture through the module path value.
        class Runtime:
            def __truediv__(self,part):return self if part=='dist' else self.site
        runtime=Runtime();runtime.site=self.site
        sync.RUNTIME_ROOT=runtime
        groups={name:[] for name in ('main','main_history','main_source','obt','sales_data','sales')}
        groups.update(release.release_entries(self.site));events=[]
        class Response:
            def json(self):return pointer
        def verify(*args):
            events.append('verified')
            if fail:raise ValueError('Remote checksum differs')
        with patch.dict(os.environ,{'SKIP_SALES_TARGET_BUILD':'0'}),patch.object(sync,'refresh_access_token',return_value='test'),patch.object(sync,'ensure_company_reader'),patch.object(sync,'runtime_groups',return_value=groups),patch.object(sync,'sync_group',side_effect=lambda *a,**k:[]),patch.object(sync,'verify_remote_groups',side_effect=verify),patch.object(sync,'list_files',return_value=[]),patch.object(sync,'sync_one',side_effect=lambda *a,**k:(events.append('pointer') or {'id':'pointer','name':'sales-target-release.json','action':'created'})),patch.object(sync,'request',return_value=Response()),patch.object(sync,'write_state',side_effect=lambda *a:events.append('state')),patch.object(sys,'argv',['sync']):
            if fail:
                with self.assertRaisesRegex(ValueError,'checksum'):sync.main()
            else:self.assertEqual(sync.main(),0)
        return events
    def test_daily_sync_updates_pointer_only_after_remote_verification(self):
        self.assertEqual(self.run_daily_publisher(),['verified','pointer','state'])
    def test_daily_sync_failure_preserves_previous_pointer(self):
        self.assertEqual(self.run_daily_publisher(fail=True),['verified'])
    def modern_fixture(self,target=0,state='known'):
        index=release.read(self.site/'index.json')
        index['official_roster_meta']={'verification':'confirmed'}
        index['rows']=[{'row_type':'SALES','kpi':{key:{quarter:{'target':target,'basis_state':state} for quarter in ('q3','q4')} for key in ('booking','lifting','high_profit')}}]
        manifest=release.read(self.site/'manifest.json');manifest['bkg_rows']=0
        manifest['bsa_capacity_reconciliation']={'202610':{'raw':2,'assigned':1,'reserved':1,'pending':0}}
        for name in ('TH__A__202610.json','TH__reserve__202610.json'):
            (self.site/'data'/name).write_text(json.dumps({'yyyymm':'202610','totals':{'allocated_bsa':1},'bookings':[]}),encoding='utf-8')
        (self.site/'office_affiliation.json').write_text('{"dataset_id":"20261012","approved_pairs":[]}',encoding='utf-8')
        for key,value in [('index',index),('manifest',manifest)]:
            (self.site/(key+'.json')).write_text(json.dumps(value),encoding='utf-8')
        release.prepare_release(self.site)
    def test_modern_verifier_preserves_zero_without_legacy_source_audit(self):
        self.modern_fixture()
        with patch('sales_target_roster_contract.validate_rosters'):
            self.assertEqual(release.verify_company_release(self.site,'20261012')['chunks'],2)
    def test_modern_verifier_rejects_unknown_source_blank(self):
        self.modern_fixture(None,'source_missing')
        with patch('sales_target_roster_contract.validate_rosters'):
            with self.assertRaisesRegex(ValueError,'Unknown target gap'):release.verify_company_release(self.site,'20261012')

if __name__=='__main__':unittest.main()
