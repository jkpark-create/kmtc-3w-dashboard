import unittest
from scripts.obt_voyage_risks import build_voyage_calls

class VoyageRiskTests(unittest.TestCase):
    def test_destination_rows_merge_but_voyages_stay_separate(self):
        base=dict(team="OBT",week="20261042",origin="CN",por="SHA",route="R",vesselCode="V",voyageNo="001",bound="E",bsaTeu=50,referencePerformanceTeu=10,revisedDepartureDate="20261020")
        rows=[{**base,"dly":"BKK"},{**base,"dly":"SIN"},{**base,"voyageNo":"002","dly":"BKK"},{**base,"team":"IBT"}]
        facts=build_voyage_calls(rows,{"20261042":"2026년 10월 18일"})
        self.assertEqual(len(facts),2)
        self.assertEqual(facts[0]["bsa_teu"],100)
        self.assertEqual(facts[0]["booking_teu"],20)
        self.assertNotIn("dly",facts[0])
        self.assertEqual(facts[1]["voyage_no"],"002")
    def test_missing_identity_is_not_invented(self):
        self.assertEqual(build_voyage_calls([dict(team="OBT",week="20261042",origin="CN",por="SHA",bsaTeu=50)],{"20261042":"week"}),[])
