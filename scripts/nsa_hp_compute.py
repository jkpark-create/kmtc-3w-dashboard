# -*- coding: utf-8 -*-
"""중국발(SHA/NBO/TAO/SZP=SHK+DCB) → NSA(Nhava Sheva, India) 고수익비중
목표/실적/GAP, 영업사원까지. 실적=Feb-May WOS-3 고수익부킹/전체부킹(도착항 NSA).
목표=대시보드 슬라이스(도착항 NSA): filtBase(w3h/w3f) + (sheetTarget − unfBase).

build_nsa_block_rows() → laggard_q3_rebuild_filtered 의 블록 ②(고수익비중) 행 생성에 사용.
표시 필터: GAP<0 전원  OR  (목표≤35% & 0≤GAP≤10%);  목표 없는 영업사원 제외.
"""
import sys, json
from pathlib import Path
import pandas as pd
sys.stdout.reconfigure(encoding='utf-8')
ROOT = Path(__file__).resolve().parents[1]
SNAP = ROOT / 'output/booking_snapshot_result_20260612.csv'
BSA = ROOT / 'output/BSA_raw_monthly3W_20260612.csv'
BASE = json.load(open(ROOT / 'dist/sales-target/base2025.json', encoding='utf-8'))
BASE = BASE.get('base', BASE)
IDX = json.load(open(ROOT / 'dist/sales-target/index.json', encoding='utf-8'))
PERIOD = ['202602', '202603', '202604', '202605']
WEEKS = 17
DEST = 'NSA'
GRP_PORTS = {'SHA': ['SHA'], 'NBO': ['NBO'], 'TAO': ['TAO'], 'SZP': ['SHK', 'DCB']}
GRP_TAB = {'SHA': 'CN_SHA', 'NBO': 'CN_NBO', 'TAO': 'CN_TAO', 'SZP': 'CN_SHK_DCB'}
LOW_TGT, SMALL_GAP = 0.35, 0.10   # 목표≤35% & 0≤GAP≤10% → 양수라도 함께 표시

IDX_TN = {}
for r in IDX.get('rows', []):
    if r.get('row_type') == 'SALES':
        IDX_TN[(r['tab'], r['name'])] = r['kpi']


def _safe(a, b):
    return (a / b) if b else None


def _sliced(tab, name, port):
    slot = BASE.get(tab, {}).get(name)
    m = {'w3f': 0.0, 'w3h': 0.0}
    if not slot:
        return m
    for c in slot.get('num', []):
        if port is not None and c.get('dly') != port:
            continue
        m['w3f'] += c.get('w3f', 0) or 0
        m['w3h'] += c.get('w3h', 0) or 0
    return m


def _hp_target(tab, name):
    m = _sliced(tab, name, DEST)
    um = _sliced(tab, name, None)
    filt, unf = _safe(m['w3h'], m['w3f']), _safe(um['w3h'], um['w3f'])
    k = IDX_TN.get((tab, name))
    st = k['high_profit']['q1']['target'] if k else None
    if st is None:
        return None
    if filt is not None and unf is not None:
        return filt + (st - unf)
    return st


def compute_nsa():
    """Return list per origin: {grp, act, tgt, gap, wbsa, sps:[{sp,plc,act,tgt,gap,bkg}]}."""
    df = pd.read_csv(SNAP, low_memory=False)
    d = df[(~df['POR_CTR_CD'].isin(['KR', 'JP'])) & (df['DLY_CTR_CD'] != 'KR')].copy()
    for c in ('FST_TEU', 'LST_TEU'):
        d[c] = pd.to_numeric(d[c], errors='coerce')
    d['fst'] = d['FST_TEU'].fillna(d['LST_TEU'])
    d['ym'] = d['YYYYMM'].astype(str)
    d = d[d['ym'].isin(PERIOD) & (d['DLY_PLC_CD'] == DEST)]
    d['is_w3'] = d['Lead_time (BKG_Sche)'] == 'WOS-3'
    d['is_hi'] = d['고/저'] == '고수익'
    p2g = {p: g for g, ps in GRP_PORTS.items() for p in ps}
    d['grp'] = d['POR_PLC_CD'].map(p2g)
    d = d[d['grp'].notna()]

    b = pd.read_csv(BSA, low_memory=False)
    b['bsa'] = pd.to_numeric(b['TEU_BSA (Actual)'], errors='coerce')
    b = b[(b['team'] == 'OBT') & (b['YYYYMM'].astype(str).isin(PERIOD)) & (b['DLY_PORT'] == DEST)].copy()
    b['grp'] = b['POR_PORT'].map(p2g)

    def hs(sub):
        bkg = sub.loc[sub['is_w3'], 'fst'].sum()
        hi = sub.loc[sub['is_w3'] & sub['is_hi'], 'fst'].sum()
        return float(bkg), float(hi), _safe(hi, bkg)

    out = []
    for grp in ['SHA', 'NBO', 'TAO', 'SZP']:
        tab = GRP_TAB[grp]
        sub = d[d['grp'] == grp]
        bkg, hi, act = hs(sub)
        tw = bw = 0.0
        sps = []
        for sp, g in sub.groupby('Salesman_POR'):
            b2, h2, a2 = hs(g)
            if b2 <= 0:
                continue
            t = _hp_target(tab, sp)
            gap = (a2 - t) if (a2 is not None and t is not None) else None
            plc = g['POR_PLC_CD'].mode().iat[0] if len(g) else grp
            sps.append({'sp': sp, 'plc': plc, 'act': a2, 'tgt': t, 'gap': gap, 'bkg': b2})
            if t is not None:
                tw += t * b2; bw += b2
        seg_tgt = (tw / bw) if bw > 0 else None
        seg_gap = (act - seg_tgt) if (act is not None and seg_tgt is not None) else None
        wbsa = round(b[b['grp'] == grp]['bsa'].sum() / WEEKS)
        out.append({'grp': grp, 'act': act, 'tgt': seg_tgt, 'gap': seg_gap, 'wbsa': wbsa, 'sps': sps})
    return out


def _keep(s):
    g, t = s['gap'], s['tgt']
    if t is None or g is None:
        return False                      # 목표 없는 영업사원 제외
    if g < 0:
        return True                       # GAP 음수 전원
    return t <= LOW_TGT and 0 <= g <= SMALL_GAP   # 목표 낮고 GAP 소폭 양수


def build_nsa_block_rows():
    """Rows for block ② (고수익비중) in the rebuild grid: [{kind, cells:[8]}]."""
    rows = []
    for o in compute_nsa():
        grp = o['grp']
        rows.append({'kind': 'seg',
                     'cells': ['구간', f'{grp}→NSA', grp, 'NSA',
                               o['tgt'], o['act'], o['gap'], o['wbsa']]})
        kept = [s for s in o['sps'] if _keep(s)]
        kept.sort(key=lambda s: s['gap'])
        for s in kept:
            rows.append({'kind': 'sales',
                         'cells': ['└ 영업사원', s['sp'], s['plc'], '',
                                   s['tgt'], s['act'], s['gap'], None]})
    return rows


if __name__ == '__main__':
    def pct(x):
        return '   -' if x is None else f'{x*100:4.0f}%'
    for o in compute_nsa():
        print(f"\n[{o['grp']}→NSA] 목표 {pct(o['tgt'])} 실적 {pct(o['act'])} GAP {pct(o['gap'])}  주간BSA {o['wbsa']}")
        for s in sorted(o['sps'], key=lambda x: (x['gap'] is None, x['gap'])):
            mark = '  ✓' if _keep(s) else '  ·'
            gp = '   -' if s['gap'] is None else f"{s['gap']*100:+.0f}%"
            print(f"  {mark} {s['sp']:13} 목표 {pct(s['tgt'])} 실적 {pct(s['act'])} GAP {gp:>6}  (bkg {s['bkg']:.0f})")
