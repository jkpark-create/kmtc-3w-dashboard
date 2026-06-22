# -*- coding: utf-8 -*-
import pandas as pd, re, json
from datetime import datetime, timedelta
from pathlib import Path

ROOT=Path(r"c:\Users\JKPARK\OneDrive\Documents\Claude\-3W bkg dashboard")
df = pd.read_csv(ROOT/"output/booking_snapshot_result_20260620.csv", dtype=str, low_memory=False, encoding='utf-8-sig')
df = df.rename(columns={'고/저':'profit_type'})
for c in ['FST_TEU','LST_TEU']:
    df[c]=pd.to_numeric(df[c].astype(str).str.replace(',',''),errors='coerce').fillna(0)
df['lst']=df['LST_TEU']
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

obt=set(json.load(open(ROOT/'dist/data.json',encoding='utf-8'))['obt_salesmen'])
o=df[(df['YM445']=='202606')&(df['team']=='OBT')&(df['LST_Status']=='Normal')].copy()
o['sm']=o['Salesman_POR'].astype(str).str.strip()
print("Salesman_POR 샘플:", o['sm'].dropna().unique()[:8])
inobt=o['sm'].isin(obt)
print(f"\nOBT June Normal 전체 norm_lst : {round(o['lst'].sum()):>10,}  ({len(o)} rows)")
print(f"  ├ obt_salesmen 소속        : {round(o[inobt]['lst'].sum()):>10,}  ({inobt.sum()} rows)")
print(f"  └ 비OBT salesman          : {round(o[~inobt]['lst'].sum()):>10,}  ({(~inobt).sum()} rows)")

KMTC4={'2026-05-31','2026-06-07','2026-06-14','2026-06-21'}
o4=o[o['_ws'].isin(KMTC4)]; in4=o4['sm'].isin(obt)
print(f"\nOBT June(4주) Normal norm_lst : {round(o4['lst'].sum()):>10,}")
print(f"  └ obt_salesmen 소속        : {round(o4[in4]['lst'].sum()):>10,}   <- KMTC 81,453 대조")

# group by origin (obt_salesmen restricted, 4-week) top 15
print("\n=== obt_salesmen·4주 origin별 norm_lst (KMTC origin CN=35,268 대조) ===")
g=o4[in4].groupby('POR_CTR_CD')['lst'].sum().round(0).sort_values(ascending=False).head(15)
print(g)
