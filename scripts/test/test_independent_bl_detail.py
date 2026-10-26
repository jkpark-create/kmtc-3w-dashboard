"""Latest own-B/L detail must reconcile before it can replace existing tables."""
import copy
import gzip
import json
import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import contextlib, io, tempfile, os
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from independent_component_bl import stage as bl_stage
MONTHS=["202607","202608","202609","202610","202611","202612"]

class OfflineBase(unittest.TestCase):
 def setUp(self):
  self.quiet=contextlib.redirect_stdout(io.StringIO());self.quiet.__enter__()
  base=pathlib.Path(os.environ.get('KMTC_COMPONENT_TEST_TEMP') or tempfile.gettempdir())/'kmtc-independent-component-tests';base.mkdir(exist_ok=True)
  self.temp=tempfile.TemporaryDirectory(dir=base);self.root=pathlib.Path(self.temp.name);self.source=self.root/'producer/oracle-dashboard';self.source.mkdir(parents=True);self.runtime=self.root/'consumer';self.store=self.runtime/'independent-components'
  self.meta={'sourceAsOfDate':'20261024','bsaSourceAsOfDate':'20261024','blSourceAsOfDate':'20261024','canonicalBlLoadDtm':'20261024074658','generatedAt':'2026-10-24T10:37:43+09:00','sourceUsable':True,'sandboxBsaSourceFresh':True,'bsaSourceModeResolved':'sandbox_only','bsaScope':'official BSA scope','canonicalIdentifierSourceMode':'icc_direct','canonicalIdentifierValidation':{'passed':True,'mismatchCount':0,'multiMonthIdentifierCount':0,'blIdentifiers':123},'iccClosedWeekReconciliationApplied':True,'iccClosedWeekReconciliationSource':'DM_SALES.DMSAB_102DS','iccClosedWeekReconciliationMaxAbsDiff':0,'singleIndividualRouteGrainReconciled':True,'singleIndividualRouteGrainMonths':MONTHS,'bsaRenderConsistency':{'passed':True,'months':MONTHS,'maxExactDiff':0,'maxSourceTotalDiff':0},'pendingSources':['Booking'],'refreshState':'partial'}
  self.manifest={'sourceMeta':self.meta,'monthFiles':{},'kpiByMonth':{}}
  for month,week in zip(MONTHS,[27,31,35,40,44,48]):
   row={'month':month,'week':month+str(week),'team':'OBT','origin':'CN','por':'TAO','dest':'IN','dly':'MAA','bsaTeu':10.,'blTeu':4.,'blCm1':16.,'blFreight':20.,'blCm1BasisTeu':4.}
   meta=copy.deepcopy(self.meta);meta.pop('singleIndividualRouteGrainMonths')
   relative='months/'+month+'.json.gz';path=self.source/relative;path.parent.mkdir(exist_ok=True);path.write_bytes(gzip.compress(json.dumps({'sourceMeta':meta,'weeklyLaneRows':[row]}).encode(),mtime=0))
   self.manifest['monthFiles'][month]=relative;self.manifest['kpiByMonth'][month]={k:row[k] for k in ['bsaTeu','blTeu','blCm1','blFreight','blCm1BasisTeu']}
  self.write_manifest()
 def tearDown(self):self.temp.cleanup();self.quiet.__exit__(None,None,None)
 def write_manifest(self):(self.source/"manifest.json").write_text(json.dumps(self.manifest),encoding="utf-8")



class DetailTests(OfflineBase):
    def setUp(self):
        super().setUp()
        self.manifest['detailFiles'] = {}
        for month, week in zip(MONTHS, [27,31,35,40,44,48]):
            relative = 'details/' + month + '.json.gz'
            path = self.source / relative
            path.parent.mkdir(exist_ok=True)
            row = dict(month=month, week=month+str(week), team='OBT',origin='CN',por='TAO',dest='IN',dly='MAA',
                       blNo='BL-'+month, sourceType='icc_identifier_direct', shipperCode='A',shipperName='Shipper',salesman='Owner',
                       blTeu=4.,blCm1=16.,blFreight=20.,blCm1BasisTeu=4.)
            path.write_bytes(gzip.compress(json.dumps({'sourceMeta':self.meta,'bookingDetailRows':[row]}).encode()))
            self.manifest['detailFiles'][month] = relative
        self.write_manifest()

    def change(self, transform):
        path = self.source / self.manifest['detailFiles']['202607']
        payload = json.loads(gzip.decompress(path.read_bytes()))
        transform(payload)
        path.write_bytes(gzip.compress(json.dumps(payload).encode()))

    def stage(self):
        return bl_stage(self.source, self.root/'current-bl.json')

    def test_reconciled_details_preserve_count_week_and_dictionary(self):
        result = self.stage()
        table = result['dashboardDetail']['table']
        self.assertEqual(result['dashboardDetail']['identifierCount'],6)
        self.assertEqual(len(table['r']),6)
        self.assertEqual(table['d']['week_start_date'][0],'2026년 07월 05일')
        self.assertEqual(sum(row[11] for row in table['r']),24)
        self.assertEqual(len(result['sourceFiles']),12)

    def test_detail_month_total_mismatch_is_not_publishable(self):
        self.change(lambda p:p['bookingDetailRows'][0].update(blTeu=5.))
        with self.assertRaisesRegex(ValueError,'reconcile'):self.stage()

    def test_duplicate_identifier_is_not_publishable(self):
        self.change(lambda p:p['bookingDetailRows'].append(copy.deepcopy(p['bookingDetailRows'][0])))
        with self.assertRaisesRegex(ValueError,'Duplicate BL'):self.stage()

    def test_unknown_cm1_is_not_coerced_to_zero(self):
        self.change(lambda p:p['bookingDetailRows'][0].update(blCm1=None))
        with self.assertRaisesRegex(ValueError,'Unknown BL'):self.stage()

    def test_same_month_but_wrong_lane_is_not_publishable(self):
        self.change(lambda p:p['bookingDetailRows'][0].update(por='SHA'))
        with self.assertRaisesRegex(ValueError,'reconcile'):self.stage()


if __name__ == '__main__':unittest.main()
