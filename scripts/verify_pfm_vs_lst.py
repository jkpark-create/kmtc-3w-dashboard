# -*- coding: utf-8 -*-
"""스냅샷 안에서 B/L 존재(lifted)를 구분하는 프록시가 있는지 탐색."""
import pandas as pd, re
from datetime import datetime
from pathlib import Path
OUT = Path(r"c:\Users\JKPARK\OneDrive\Documents\Claude\-3W bkg dashboard\output")

bl = pd.read_csv(OUT/"shk_dcb_sp030s01_revised_bl_biz_o_202603_202605.csv", dtype=str, encoding='utf-8-sig')
bl['PFM_TEU']=pd.to_numeric(bl['PFM_TEU'],errors='coerce').fillna(0)
bl_set=set(bl['BKG_NO'])

sn = pd.read_csv(OUT/"booking_snapshot_result_20260620.csv", dtype=str, low_memory=False, encoding='utf-8-sig')
sn['LST_TEU']=pd.to_numeric(sn['LST_TEU'].astype(str).str.replace(',',''),errors='coerce').fillna(0)
sn['CM1n']=pd.to_numeric(sn['CM1'].astype(str).str.replace(',',''),errors='coerce').fillna(0)
def pdate(s):
    m=re.match(r'(\d{4})\D+(\d{1,2})\D+(\d{1,2})',str(s)) if pd.notna(s) else None
    return datetime(int(m.group(1)),int(m.group(2)),int(m.group(3))) if m else None
sn['adep']=sn['Actual_Departure_schedule'].apply(pdate)

# SHK+DCB, adep 3/1~5/30 (KMTC 완료기간)
shk = sn[(sn['POR_PLC_CD'].isin(['SHK','DCB'])) &
         (sn['adep'].apply(lambda d: d is not None and datetime(2026,3,1)<=d<=datetime(2026,5,30)))].copy()
shk['in_bl']=shk['BKG_NO'].isin(bl_set)
truth_teu = shk[shk['in_bl']]['LST_TEU'].sum()
print(f"대조군: SHK+DCB adep3~5월  {len(shk)} BKG")
print(f"  B/L 정답: {shk['in_bl'].sum()} BKG, LST_TEU {round(truth_teu)}\n")

def nonempty(col):
    s=shk[col].astype(str).str.strip()
    return ~s.isin(['','nan','NaN','None','0'])

candidates = {
 "LST_Status=='Normal'": shk['LST_Status']=='Normal',
 "CM1 != 0":             shk['CM1n']!=0,
 "LST_VSL 존재":          nonempty('LST_VSL'),
 "LST_VOY 존재":          nonempty('LST_VOY'),
 "LST_route 존재":        nonempty('LST_route'),
 "Lead_time(Actual) 존재": nonempty('Lead_time(Actual)'),
 "Normal & CM1!=0":      (shk['LST_Status']=='Normal') & (shk['CM1n']!=0),
}
print(f"{'프록시':28} {'예측BKG':>8} {'예측TEU':>9} {'∩정답BKG':>9} {'정밀도':>7} {'재현율':>7}")
for name,mask in candidates.items():
    pred=shk[mask]; pteu=pred['LST_TEU'].sum()
    inter=(mask & shk['in_bl']).sum()
    prec=inter/mask.sum()*100 if mask.sum() else 0
    rec=inter/shk['in_bl'].sum()*100
    print(f"{name:28} {mask.sum():>8} {round(pteu):>9,} {inter:>9} {prec:>6.1f}% {rec:>6.1f}%")
