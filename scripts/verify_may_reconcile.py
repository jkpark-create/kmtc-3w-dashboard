# -*- coding: utf-8 -*-
"""5월(마감) 대시보드 norm_lst vs KMTC Lifting(86,850) 정합 — 출발지/도착지 차원 확인."""
import pandas as pd, re
from datetime import datetime, timedelta
from pathlib import Path
OUT=Path(r"c:\Users\JKPARK\OneDrive\Documents\Claude\-3W bkg dashboard\output")

df=pd.read_csv(OUT/"booking_snapshot_result_20260620.csv",dtype=str,low_memory=False,encoding='utf-8-sig')
df['lst']=pd.to_numeric(df['LST_TEU'].astype(str).str.replace(',',''),errors='coerce').fillna(0)
FISCAL_445={2026:(datetime(2026,1,4),[4,4,5,4,4,5,4,4,5,4,4,5])}
wm={}
for y,(fs,pat) in FISCAL_445.items():
    wk=0
    for mi,c in enumerate(pat):
        ym=f'{y}{mi+1:02d}'
        for _ in range(c):
            wm[(fs+timedelta(weeks=wk)).strftime('%Y-%m-%d')]=ym; wk+=1
def pkd(s):
    m=re.match(r'(\d{4})\D+(\d{1,2})\D+(\d{1,2})',str(s)) if pd.notna(s) else None
    return f'{int(m.group(1))}-{int(m.group(2)):02d}-{int(m.group(3)):02d}' if m else None
df['_ws']=df['week_start_date'].apply(pkd)
df['YM445']=df['_ws'].map(wm).fillna('')
def team(o,d):
    o=str(o).strip();d=str(d).strip()
    if o not in('KR','JP') and d!='KR':return 'OBT'
    if o=='KR' and d!='JP':return 'EST'
    if o!='JP' and d=='KR':return 'IST'
    return 'JBT'
df['team']=[team(o,d) for o,d in zip(df['POR_CTR_CD'],df['DLY_CTR_CD'])]
norm=df['LST_Status']=='Normal'

o=df[(df['YM445']=='202605')&(df['team']=='OBT')&norm]
print("5월 OBT Normal norm_lst 총계:", round(o['lst'].sum()), "  (KMTC 86,850 / 대시보드 86,819)")
print("  week_start_date 분포:", sorted(o['_ws'].unique()))

print("\n=== 출발지(POR_CTR_CD)별 norm_lst vs KMTC(POR CN) ===")
kmtc_por={'CN':39517,'IN':7836,'ID':7612,'MY':4014,'HK':3951,'AE':552,'EG':116,'JO':25,'KH':89,'MM':6,'MX':10,'BH':11,'BD':3}
g=o.groupby('POR_CTR_CD')['lst'].sum().round(0).sort_values(ascending=False)
print(f"{'POR':5}{'대시보드':>10}{'KMTC':>10}{'차이':>8}")
for k,v in g.head(18).items():
    km=kmtc_por.get(k,'')
    diff=round(v-km) if km!='' else ''
    print(f"{k:5}{round(v):>10,}{(str(km) if km!='' else '-'):>10}{(str(diff) if diff!='' else '-'):>8}")

print("\n=== 도착지(DLY_CTR_CD)별 norm_lst (대시보드 화면과 동일) ===")
g2=o.groupby('DLY_CTR_CD')['lst'].sum().round(0).sort_values(ascending=False).head(12)
print(g2.to_string())
