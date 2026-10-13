import sys,io,unittest
from pathlib import Path
import pandas as pd
import openpyxl
HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE.parent if HERE.name=='test' else Path(r'C:\Users\JKPARK\OneDrive\Documents\Claude\-3W bkg dashboard\scripts')))
import update_target_workbook_from_current_customer_owners as u

class OfficialContactCoverageTest(unittest.TestCase):
    def test_current_official_japan_egypt_jordan_are_read_without_disk(self):
        workbook=openpyxl.Workbook();jp=workbook.active;jp.title='JP'
        header=['Area','Department','Name','ICC ID','Role']
        jp.append(header);jp.append(['TYO','Field Sales Team','Employee Japan','EMPJ','Sales'])
        middle=workbook.create_sheet('JED,SKN,AQJ');middle.append(header)
        middle.append(['SKN','Sales & Marketing','Employee Egypt','EMPE','Sales'])
        middle.append(['AQJ','Sales & Marketing','Employee Jordan','EMPJO','Sales'])
        memory=io.BytesIO();workbook.save(memory);workbook.close();memory.seek(0)
        active=pd.DataFrame({'SALESMAN_NO':['EMPJ','EMPE','EMPJO']})
        result=u.load_owner_entries(memory,active,{name:1 for name in active.SALESMAN_NO})
        self.assertEqual({(origin,e.resolved_sales) for origin,entries in result.items() for e in entries},{('JP','EMPJ'),('EG','EMPE'),('JO','EMPJO')})

if __name__=='__main__':unittest.main()
