import unittest
from scripts.repair_sales_target_wos_cm1 import repair_booking


class WosCm1RepairTest(unittest.TestCase):
    def evidence(self):
        return {'CN1': dict(BKG_SHPR_CST_NO='C', POL_PORT_CD='SHA', POD_PORT_CD='SIN',
                            WOS_LST_Status='Normal', WOS_LST_TEU='2', WOS_CM1='1,250.50')}

    def booking(self):
        return dict(bkg_no='CN1', shipper_no='C', pol='SHA', pod='SIN',
                    performance_metric_kind='booking', is_w3=True, wos_norm_lst_teu=2, norm_cm1=0)

    def test_restore_from_original_normal_booking_without_touching_bl_actual(self):
        row = self.booking(); self.assertTrue(repair_booking(row, self.evidence()))
        self.assertEqual(row['wos_norm_cm1'], 1250.5); self.assertEqual(row['norm_cm1'], 0)
        bl = dict(performance_metric_kind='bl', norm_cm1=500)
        self.assertFalse(repair_booking(bl, {})); self.assertEqual(bl['norm_cm1'], 500)

    def test_mismatched_lineage_or_snapshot_is_rejected(self):
        for change in [dict(pod='JKT'), dict(bkg_no='MISSING'), dict(wos_norm_lst_teu=9)]:
            with self.subTest(change=change), self.assertRaises(ValueError):
                repair_booking(self.booking() | change, self.evidence())

    def test_cancel_has_no_normal_cohort_profit(self):
        row = self.booking() | dict(wos_norm_lst_teu=0)
        evidence = self.evidence(); evidence['CN1']['WOS_LST_Status'] = 'Cancel'
        repair_booking(row, evidence); self.assertEqual(row['wos_norm_cm1'], 0)


if __name__ == '__main__':
    unittest.main()
