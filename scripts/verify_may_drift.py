# -*- coding: utf-8 -*-
"""5월 마감월 잔차 진단: 연속 스냅샷(06-19 vs 06-20) 간 5월 norm_lst 변동 + 팀스코프."""
import pandas as pd, re, json
from datetime import datetime, timedelta
from pathlib import Path
OUT=Path(r"c:\Users\JKPARK\OneDrive\Documents\Claude\-3W bkg dashboard\output")
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
def team(o,d):
    o=str(o).strip();d=str(d).strip()
    if o not in('KR','JP') and d!='KR':return 'OBT'
    if o=='KR' and d!='JP':return 'EST'
    if o!='JP' and d=='KR':return 'IST'
    return 'JBT'
obt_sm=set(json.load(open(OUT.parent/'dist/data.json',encoding='utf-8'))['obt_salesmen'])

def load(path):
    df=pd.read_csv(path,dtype=str,low_memory=False,encoding='utf-8-sig')
    df['lst']=pd.to_numeric(df['LST_TEU'].astype(str).str.replace(',',''),errors='coerce').fillna(0)
    df['YM']=df['week_start_date'].apply(pkd).map(wm).fillna('')
    df['team']=[team(o,d) for o,d in zip(df['POR_CTR_CD'],df['DLY_CTR_CD'])]
    m=df[(df['YM']=='202605')&(df['team']=='OBT')&(df['LST_Status']=='Normal')].copy()
    m['sm']=df['Salesman_POR'].astype(str).str.strip()
    return m

for tag,fn in [('06-19','booking_snapshot_result_20260619.csv'),('06-20','booking_snapshot_result_20260620.csv')]:
    p=OUT/fn
    if not p.exists():
        print(tag,'없음'); continue
    m=load(p)
    cn=m[m['POR_CTR_CD']=='CN']['lst'].sum()
    print(f"[{tag}] 5월 OBT Normal norm_lst 총계: {round(m['lst'].sum()):>8,}  | CN출발: {round(cn):>8,}")

# 팀스코프: 5월 CN출발 中 obt_salesmen 여부 (KMTC OBT2와의 차이 후보)
m=load(OUT/'booking_snapshot_result_20260620.csv')
cn=m[m['POR_CTR_CD']=='CN']
print("\n[06-20] 5월 CN출발 norm_lst:", round(cn['lst'].sum()))
print("  obt_salesmen 소속:", round(cn[cn['sm'].isin(obt_sm)]['lst'].sum()),
      " / 비OBT:", round(cn[~cn['sm'].isin(obt_sm)]['lst'].sum()))
print("\nKMTC 5월 CN(출발)=39,517, 대시보드=39,426 (차이 -91; 대시보드가 낮음)")
