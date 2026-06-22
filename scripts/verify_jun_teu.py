# -*- coding: utf-8 -*-
import pandas as pd, re
from datetime import datetime, timedelta
from pathlib import Path

snap = Path(r"c:\Users\JKPARK\OneDrive\Documents\Claude\-3W bkg dashboard\output\booking_snapshot_result_20260620.csv")
df = pd.read_csv(snap, dtype=str, low_memory=False, encoding='utf-8-sig')
df = df.rename(columns={'고/저':'profit_type'})
for c in ['FST_TEU','LST_TEU']:
    df[c] = pd.to_numeric(df[c].astype(str).str.replace(',',''), errors='coerce').fillna(0)
df['fst']=df['FST_TEU']; df['lst']=df['LST_TEU']

FISCAL_445 = {2026: (datetime(2026,1,4), [4,4,5,4,4,5,4,4,5,4,4,5])}
wm={}
for year,(fs,pat) in FISCAL_445.items():
    wk=0
    for mi,cnt in enumerate(pat):
        ym=f'{year}{mi+1:02d}'
        for _ in range(cnt):
            wm[(fs+timedelta(weeks=wk)).strftime('%Y-%m-%d')]=ym; wk+=1
def pkd(s):
    m=re.match(r'(\d{4})\D+(\d{1,2})\D+(\d{1,2})', str(s)) if pd.notna(s) else None
    return f'{int(m.group(1))}-{int(m.group(2)):02d}-{int(m.group(3)):02d}' if m else None
df['YM445']=df['week_start_date'].apply(pkd).map(wm).fillna('')
def team(o,d):
    o=str(o).strip(); d=str(d).strip()
    if o not in ('KR','JP') and d!='KR': return 'OBT'
    if o=='KR' and d!='JP': return 'EST'
    if o!='JP' and d=='KR': return 'IST'
    return 'JBT'
df['team']=[team(o,d) for o,d in zip(df['POR_CTR_CD'], df['DLY_CTR_CD'])]
norm=(df['LST_Status']=='Normal').astype(int)
lt=df['Lead_time (BKG_Sche)']
w3=(lt=='WOS-3').astype(int)
rhi=df['profit_type'].astype(str).str.contains('고수익',na=False).astype(int)

o=df[(df['YM445']=='202606')&(df['team']=='OBT')].copy()
o['norm']=norm[o.index]; o['w3']=w3[o.index]; o['rhi']=rhi[o.index]

w3_fst            = (o['fst']*o['w3']).sum()
w3_route_hi_fst   = (o['fst']*o['w3']*o['rhi']).sum()
w3_norm_lst       = (o['lst']*o['w3']*o['norm']).sum()
w3_rh_norm_lst    = (o['lst']*o['w3']*o['rhi']*o['norm']).sum()
norm_lst          = (o['lst']*o['norm']).sum()
rh_norm_lst       = (o['lst']*o['rhi']*o['norm']).sum()

print("===== ⑥ 탭 신규 KPI (OBT, 6월 445) =====")
print(f"WOS-3 TEU (부킹 FST)            : {w3_fst:>10,.0f}   (기존과 동일)")
print(f"WOS-3 고수익 TEU (부킹 FST)     : {w3_route_hi_fst:>10,.0f}   (기존과 동일)")
print(f"WOS-3 고수익비중 (부킹)         : {w3_route_hi_fst/w3_fst*100:>9.1f}%   (기존과 동일)")
print(f"고수익 실선적 TEU (B/L)         : {w3_rh_norm_lst:>10,.0f}   (기존과 동일)")
print(f"실선적률 = 고실/고부킹          : {w3_rh_norm_lst/w3_route_hi_fst*100:>9.1f}%   (기존과 동일)")
print(f"[신규] 고수익 실선적 비중       : {w3_rh_norm_lst/w3_norm_lst*100:>9.1f}%   (= 고수익실선적 {w3_rh_norm_lst:,.0f} / WOS-3 실선적B/L {w3_norm_lst:,.0f})")
print(f"전체 TEU [변경: 부킹176,261→B/L]: {norm_lst:>10,.0f}   (전체 실선적 B/L, Normal)")
print(f"전체 고수익 TEU [변경→B/L]      : {rh_norm_lst:>10,.0f}   (전체 고수익 실선적 B/L)")
print(f"전체 고수익비중 [변경→B/L]      : {rh_norm_lst/norm_lst*100:>9.1f}%   (기존 부킹 46.4%)")
