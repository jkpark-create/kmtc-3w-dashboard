# -*- coding: utf-8 -*-
"""Compute Q3-target + performance data for the dashboard underperformance widget.

Reproduces the dashboard 저조(laggard) widget basis (validated against
output/extend_rankings_20260605.json and the source image):
  - OBT filter: POR_CTR_CD not in {KR,JP} AND DLY_CTR_CD != KR
  - Period: Feb-May 2026 (2~5월) by YYYYMM (actual sailing month)
  - grp(선적지): SZP=SHK/DCB, NBO/TAO/SHA=that port, ID/VN/TH/MY=country
  - ① 3주전부킹률  = WOS-3 FST_TEU(fallback LST) / BSA
  - ② 고수익비중    = WOS-3 고수익 FST / WOS-3 FST
  - ③ 실선적률(3주전) = WOS-3 Normal LST / BSA   (저조 트리거 = image)
       소석률(전체)   = 전체 Normal LST / BSA   (목표 비교 기준)
  - 목표율 = origin(선적지) KPI 목표율 (target workbook, index.json), 분기 불변 = Q3 목표

Outputs: output/laggard_q3_data.json
"""
import json, sys
from pathlib import Path
import pandas as pd
import numpy as np

sys.stdout.reconfigure(encoding='utf-8')
ROOT = Path(__file__).resolve().parents[1]
SNAP = ROOT / 'output/booking_snapshot_result_20260610.csv'
BSA  = ROOT / 'output/BSA_raw_monthly3W_20260610.csv'
IDX  = ROOT / 'dist/sales-target/index.json'
OUT  = ROOT / 'output/laggard_q3_data.json'
PERIOD = ['202602', '202603', '202604', '202605']  # 2~5월

# grp(선적지) -> index.json origin tabs (for target rates)
GRP_TABS = {
    'ID':  ['ID-IDO'],
    'SZP': ['CN_SHK_DCB'],
    'TAO': ['CN_TAO'],
    'NBO': ['CN_NBO'],
    'SHA': ['CN_SHA'],
    'TH':  ['TH'],
    'VN':  ['VN_HPH', 'VN_SGN_CMP'],
    'MY':  ['PKG+PKW', 'PEN'],
}

# image laggard routes per KPI (validated exact)
LAGGARD_ROUTES = {
    'w3': [('ID', 'AE'), ('NBO', 'JP'), ('SZP', 'SG'), ('VN', 'TH')],
    'hi': [('SZP', 'SG'), ('VN', 'HK'), ('TH', 'CN'), ('ID', 'HK')],
    'lf': [('ID', 'AE'), ('SZP', 'SG'), ('NBO', 'JP'), ('ID', 'HK')],
}
# image laggard countries(grp) per KPI
LAGGARD_GRPS = {
    'w3': ['ID', 'SZP', 'TAO', 'NBO'],
    'hi': ['TH', 'SZP', 'VN', 'MY'],
    'lf': ['SZP', 'ID', 'VN', 'NBO'],
}


def grp_of(plc, ctr):
    if plc in ('SHK', 'DCB'):
        return 'SZP'
    if plc in ('NBO', 'TAO', 'SHA'):
        return plc
    if ctr in ('ID', 'VN', 'TH', 'MY'):
        return ctr
    return None


def load_snapshot():
    df = pd.read_csv(SNAP, low_memory=False)
    df = df[(~df['POR_CTR_CD'].isin(['KR', 'JP'])) & (df['DLY_CTR_CD'] != 'KR')].copy()
    df['grp'] = [grp_of(p, c) for p, c in zip(df['POR_PLC_CD'], df['POR_CTR_CD'])]
    for c in ('FST_TEU', 'LST_TEU'):
        df[c] = pd.to_numeric(df[c], errors='coerce')
    df['fst'] = df['FST_TEU'].fillna(df['LST_TEU'])
    df['ym'] = df['YYYYMM'].astype(str)
    df = df[df['ym'].isin(PERIOD)]
    df['is_w3'] = df['Lead_time (BKG_Sche)'] == 'WOS-3'
    df['is_norm'] = df['LST_Status'] == 'Normal'
    df['is_hi'] = df['고/저'] == '고수익'
    return df


def load_bsa():
    b = pd.read_csv(BSA, low_memory=False)
    b['bsa'] = pd.to_numeric(b['TEU_BSA (Actual)'], errors='coerce')
    b = b[b['team'] == 'OBT'].copy()
    b['grp'] = [grp_of(p, c) for p, c in zip(b['POR_PORT'], b['POR_Country'])]
    b['ym'] = b['YYYYMM'].astype(str)
    b = b[b['ym'].isin(PERIOD)]
    return b


def load_targets():
    """grp -> {booking, lifting, high_profit} annual target rate (w3-teu weighted)."""
    d = json.load(open(IDX, encoding='utf-8'))
    tot = {r['tab']: r for r in d['rows'] if r['row_type'] == 'TOTAL'}
    out = {}
    for grp, tabs in GRP_TABS.items():
        wsum = 0.0
        acc = {'booking': 0.0, 'lifting': 0.0, 'high_profit': 0.0}
        for tab in tabs:
            r = tot.get(tab)
            if not r:
                continue
            w = r.get('w3_2025_teu') or 0.0
            wsum += w
            for k in acc:
                acc[k] += r['kpi'][k]['q1']['target'] * w
        if wsum > 0:
            out[grp] = {k: acc[k] / wsum for k in acc}
        else:
            out[grp] = {k: None for k in acc}
    return out


# grp -> set of index.json tabs (for salesman target lookup)
GRP_TAB_SET = {g: set(t) for g, t in GRP_TABS.items()}


def load_salesman_targets():
    """salesman name -> per-grp {booking/lifting/high_profit: {target,perform,progress,gap}, accounts}.
    Keyed (name, tab) so we can pick the tab matching the salesman's origin grp."""
    d = json.load(open(IDX, encoding='utf-8'))
    out = {}
    for r in d['rows']:
        if r['row_type'] != 'SALES':
            continue
        out[(r['name'], r['tab'])] = {
            'tab': r['tab'],
            'kpi': r['kpi'],
            'accounts': r.get('accounts', {}),
            'share_2025': r.get('share_2025'),
        }
    return out


def metrics(sub, bsa_sub):
    bkg = sub.loc[sub['is_w3'], 'fst'].sum()
    hi = sub.loc[sub['is_w3'] & sub['is_hi'], 'fst'].sum()
    w3norm = sub.loc[sub['is_w3'] & sub['is_norm'], 'LST_TEU'].sum()
    norm_all = sub.loc[sub['is_norm'], 'LST_TEU'].sum()
    bsa = bsa_sub['bsa'].sum()
    return {
        'bkg': float(bkg), 'hi': float(hi), 'w3norm': float(w3norm),
        'norm_all': float(norm_all), 'bsa': float(bsa),
        'w3bsa': float(bkg / bsa) if bsa else None,     # ① 3주전부킹률
        'hishare': float(hi / bkg) if bkg else None,    # ② 고수익비중
        'lft3w': float(w3norm / bsa) if bsa else None,  # ③ 실선적률(3주전)
        'occ': float(norm_all / bsa) if bsa else None,  # 소석률(전체)
    }


def main():
    df = load_snapshot()
    bsa = load_bsa()
    tgt = load_targets()

    result = {
        'period': '2026-02~05 (2~5월)', 'data_date': '20260610',
        'targets': tgt,
        'country': {}, 'route': {}, 'route_salesman': {},
    }

    # ---- country(grp) summary ----
    for grp in ['ID', 'SZP', 'TAO', 'NBO', 'TH', 'VN', 'MY', 'SHA']:
        sub = df[df['grp'] == grp]
        bsub = bsa[bsa['grp'] == grp]
        result['country'][grp] = metrics(sub, bsub)

    # ---- routes (union of all laggard routes) ----
    all_routes = sorted({r for v in LAGGARD_ROUTES.values() for r in v})
    for (grp, dly) in all_routes:
        sub = df[(df['grp'] == grp) & (df['DLY_CTR_CD'] == dly)]
        bsub = bsa[(bsa['grp'] == grp) & (bsa['DLY_Country'] == dly)]
        key = f"{grp}->{dly}"
        result['route'][key] = metrics(sub, bsub)
        # salesman breakdown within route
        route_bkg = result['route'][key]['bkg']
        sps = []
        for sp, g in sub.groupby('Salesman_POR'):
            bkg = g.loc[g['is_w3'], 'fst'].sum()
            hi = g.loc[g['is_w3'] & g['is_hi'], 'fst'].sum()
            w3norm = g.loc[g['is_w3'] & g['is_norm'], 'LST_TEU'].sum()
            norm_all = g.loc[g['is_norm'], 'LST_TEU'].sum()
            sps.append({
                'sp': sp, 'bkg': float(bkg), 'hi': float(hi),
                'w3norm': float(w3norm), 'norm_all': float(norm_all),
                'bkg_share': float(bkg / route_bkg) if route_bkg else None,
                'hishare': float(hi / bkg) if bkg else None,
                'conv': float(w3norm / bkg) if bkg else None,  # 실선적전환율 = WOS-3 실선적/부킹
                'plc': g['POR_PLC_CD'].mode().iat[0] if len(g) else '',
            })
        sps = [s for s in sps if s['bkg'] > 0 or s['norm_all'] > 0]
        sps.sort(key=lambda s: -s['bkg'])
        result['route_salesman'][key] = sps

    # ---- salesman KPI 목표/실적/GAP (선적지 단위, index.json) for laggard grps ----
    sptg = load_salesman_targets()
    grp_tab = {t: g for g, ts in GRP_TABS.items() for t in ts}
    sales_by_grp = {}
    for (name, tab), rec in sptg.items():
        grp = grp_tab.get(tab)
        if grp is None:
            continue
        k = rec['kpi']
        def pick(kpi):
            q1 = k[kpi]['q1']; q2 = k[kpi]['q2']
            return {
                'target': q1['target'],
                'q1': q1.get('perform'),
                'q2': q2.get('progress'),
                'gap': q2.get('gap'),
            }
        sales_by_grp.setdefault(grp, []).append({
            'sp': name, 'tab': tab,
            'accounts': rec['accounts'],
            'booking': pick('booking'),
            'lifting': pick('lifting'),
            'high_profit': pick('high_profit'),
        })
    result['salesman_targets'] = sales_by_grp

    OUT.write_text(json.dumps(result, ensure_ascii=False, indent=1), encoding='utf-8')
    # quick verification print
    print('=== COUNTRY 3WBSA (img ①: ID51 SZP54 TAO61 NBO62) ===')
    for g in ['ID', 'SZP', 'TAO', 'NBO']:
        print(f"  {g}: w3bsa={result['country'][g]['w3bsa']*100:.0f}%")
    print('=== COUNTRY hishare (img ②: TH37 SZP38 VN38 MY38) ===')
    for g in ['TH', 'SZP', 'VN', 'MY']:
        print(f"  {g}: hishare={result['country'][g]['hishare']*100:.0f}%")
    print('=== COUNTRY lft3w (img ③: SZP21 ID30 VN33 NBO35) ===')
    for g in ['SZP', 'ID', 'VN', 'NBO']:
        print(f"  {g}: lft3w={result['country'][g]['lft3w']*100:.0f}%")
    print('=== ROUTE check ===')
    for key in ['ID->AE', 'NBO->JP', 'SZP->SG', 'VN->TH', 'VN->HK', 'TH->CN', 'ID->HK']:
        m = result['route'][key]
        print(f"  {key}: w3bsa={m['w3bsa']*100:.0f}% hishare={m['hishare']*100:.0f}% lft3w={m['lft3w']*100:.0f}% occ={m['occ']*100:.0f}% (bkg={m['bkg']:.0f} bsa={m['bsa']:.0f})")
    print('=== TARGETS (grp) ===')
    for g in ['ID', 'SZP', 'TAO', 'NBO', 'TH', 'VN', 'MY']:
        t = tgt[g]
        print(f"  {g}: booking={t['booking']*100:.0f}% lifting={t['lifting']*100:.0f}% highprofit={t['high_profit']*100:.0f}%")
    print('\nwrote', OUT)


if __name__ == '__main__':
    main()
