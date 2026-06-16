# -*- coding: utf-8 -*-
"""실선적률(= Σ WOS-3 Normal LST ÷ Σ WOS-3 부킹FST, Feb-May, 총합기준) GAP 랭킹.
모든 OBT 구간(grp 선적지 → DLY 국가) 대상. 목표 = 영업사원 sliced lifting 목표의
부킹량 가중평균. 저조구간 재선정용.
"""
import sys
from pathlib import Path
import pandas as pd
sys.stdout.reconfigure(encoding='utf-8')
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'scripts'))
from laggard_q3_fix_salesman import target_sliced   # (grp,name,dest,kpi)->(target,tab)

SNAP = ROOT / 'output/booking_snapshot_result_20260612.csv'
BSA = ROOT / 'output/BSA_raw_monthly3W_20260612.csv'
PERIOD = ['202602', '202603', '202604', '202605']
WEEKS = 17
MIN_WBSA = 150   # 주간 BSA(TEU) 최소


def grp_of(plc, ctr):
    if plc in ('SHK', 'DCB'):
        return 'SZP'
    if plc in ('NBO', 'TAO', 'SHA'):
        return plc
    if ctr in ('ID', 'VN', 'TH', 'MY'):
        return ctr
    return None


df = pd.read_csv(SNAP, low_memory=False)
df = df[(~df['POR_CTR_CD'].isin(['KR', 'JP'])) & (df['DLY_CTR_CD'] != 'KR')].copy()
for c in ('FST_TEU', 'LST_TEU'):
    df[c] = pd.to_numeric(df[c], errors='coerce')
df['fst'] = df['FST_TEU'].fillna(df['LST_TEU'])
df['ym'] = df['YYYYMM'].astype(str)
df = df[df['ym'].isin(PERIOD)]
df['is_w3'] = df['Lead_time (BKG_Sche)'] == 'WOS-3'
df['is_norm'] = df['LST_Status'] == 'Normal'
df['grp'] = [grp_of(p, c) for p, c in zip(df['POR_PLC_CD'], df['POR_CTR_CD'])]
df = df[df['grp'].notna()]

# ---- 주간 BSA per (grp, DLY_country) ----
bsa = pd.read_csv(BSA, low_memory=False)
bsa['bsa'] = pd.to_numeric(bsa['TEU_BSA (Actual)'], errors='coerce')
bsa = bsa[(bsa['team'] == 'OBT') & (bsa['YYYYMM'].astype(str).isin(PERIOD))].copy()
bsa['grp'] = [grp_of(p, c) for p, c in zip(bsa['POR_PORT'], bsa['POR_Country'])]
wbsa = (bsa.groupby(['grp', 'DLY_Country'])['bsa'].sum() / WEEKS).to_dict()

rows = []
for (grp, dly), sub in df.groupby(['grp', 'DLY_CTR_CD']):
    wb = wbsa.get((grp, dly), 0.0)
    if wb < MIN_WBSA:
        continue
    w3 = sub[sub['is_w3']]
    fst = w3['fst'].sum()
    lst = w3.loc[w3['is_norm'], 'LST_TEU'].sum()
    if fst <= 0:
        continue
    act = lst / fst
    tw = bw = 0.0
    nsp = 0
    for sp, g in w3.groupby('Salesman_POR'):
        b = g['fst'].sum()
        if b <= 0:
            continue
        t, _tab = target_sliced(grp, sp, dly, 'lifting')
        if t is not None:
            tw += t * b; bw += b
        nsp += 1
    tgt = (tw / bw) if bw > 0 else None
    gap = (act - tgt) if tgt is not None else None
    rows.append((grp, dly, wb, fst, act, tgt, gap, nsp))

rows.sort(key=lambda r: (r[6] is None, r[6]))   # GAP 오름차순(저조 먼저)
print(f"주간BSA>{MIN_WBSA} 구간만\n{'구간':12} {'주간BSA':>6} {'WOS3부킹':>8} {'실적':>5} {'목표':>5} {'GAP':>6} {'영업사원':>5}")
for i, (grp, dly, wb, fst, act, tgt, gap, nsp) in enumerate(rows):
    tp = '  -' if tgt is None else f'{tgt*100:3.0f}%'
    gp = '   -' if gap is None else f'{gap*100:+4.0f}%'
    mark = '★' if i < 4 else ' '
    print(f"{mark}{grp+'→'+dly:12} {wb:6.0f} {fst:8.0f} {act*100:4.0f}% {tp:>5} {gp:>6}  {nsp:>4}")
