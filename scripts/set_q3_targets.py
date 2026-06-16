# -*- coding: utf-8 -*-
"""Part1: 메인표 '3분기 목표' 열(I/R/AA) 채우기.
3분기목표 = clip( (Q3예측 + 2~5월목표) / 2 ).
Q3예측 = Feb~May 월별 실적률 선형회귀를 Q3(8월,t=8)로 외삽, clip.
KPI: ①부킹률=ΣWOS3부킹/배분BSA, ②고수익=Σ고수익부킹/Σ부킹, ③실선적=ΣWOS3 LST/Σ부킹.
현 레이아웃(빈A열+3분기열): ①B~I ②K~R ③T~AA, 데이터 4행~.

Usage: runpy.cmd scripts/set_q3_targets.py            # preview
       runpy.cmd scripts/set_q3_targets.py --apply
"""
import sys, re
from pathlib import Path
import pandas as pd
sys.stdout.reconfigure(encoding='utf-8')
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'scripts'))
from laggard_q3_fix_salesman import (get_service, SPREADSHEET_ID, SHEET_NAME, num,
                                     sliced_2025, resolve_tab)

SNAP = ROOT / 'output/booking_snapshot_result_20260612.csv'
BSA = ROOT / 'output/BSA_raw_monthly3W_20260612.csv'
PERIOD = ['202602', '202603', '202604', '202605']
SHEET_ID = 0
# (kind, gubun, label, 목표, 3분기목표, dest)  0-indexed cols
BLOCKS = [('w3', 1, 2, 5, 8, 'CTR'), ('hi', 10, 11, 14, 17, 'PLC'), ('lf', 19, 20, 23, 26, 'CTR')]
CN = {'SHA': ['SHA'], 'NBO': ['NBO'], 'TAO': ['TAO'], 'SZP': ['SHK', 'DCB']}
Q3T = 8.0   # 8월 (Q3 중간)


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
df['mon'] = df['ym'].str[-2:].astype(int)
df['is_w3'] = df['Lead_time (BKG_Sche)'] == 'WOS-3'
df['is_norm'] = df['LST_Status'] == 'Normal'
df['is_hi'] = df['고/저'] == '고수익'
df['grp_ctr'] = [grp_of(p, c) for p, c in zip(df['POR_PLC_CD'], df['POR_CTR_CD'])]
p2cn = {p: g for g, ps in CN.items() for p in ps}
df['grp_cn'] = df['POR_PLC_CD'].map(p2cn)

bsa = pd.read_csv(BSA, low_memory=False)
bsa['bsa'] = pd.to_numeric(bsa['TEU_BSA (Actual)'], errors='coerce')
bsa = bsa[(bsa['team'] == 'OBT') & (bsa['YYYYMM'].astype(str).isin(PERIOD))].copy()
bsa['grp'] = [grp_of(p, c) for p, c in zip(bsa['POR_PORT'], bsa['POR_Country'])]
bsa['mon'] = bsa['YYYYMM'].astype(str).str[-2:].astype(int)
ROUTEBSA = bsa.groupby(['grp', 'DLY_Country', 'mon'])['bsa'].sum().to_dict()

_share_cache = {}


def share_sp(kind, grp, dly, sp):
    if kind != 'w3':
        return None
    key = (grp, dly)
    if key not in _share_cache:
        seg = df[(df['grp_ctr'] == grp) & (df['DLY_CTR_CD'] == dly)]
        tot = {}
        for s in seg['Salesman_POR'].dropna().unique():
            tab = resolve_tab(grp, s)
            m, _ = sliced_2025(tab, s, dly) if tab else ({'nlst': 0}, 0)
            tot[s] = m['nlst']
        ssum = sum(tot.values())
        _share_cache[key] = (tot, ssum)
    tot, ssum = _share_cache[key]
    if ssum <= 0:
        return None
    return tot.get(sp, 0.0) / ssum


def monthly_rates(kind, grp, dly, sp):
    if kind == 'hi':
        sub = df[(df['grp_cn'] == grp) & (df['DLY_PLC_CD'] == dly)]
    else:
        sub = df[(df['grp_ctr'] == grp) & (df['DLY_CTR_CD'] == dly)]
    if sp:
        sub = sub[sub['Salesman_POR'] == sp]
    sub = sub[sub['is_w3']]
    rates = {}
    for m in (2, 3, 4, 5):
        g = sub[sub['mon'] == m]
        fst = g['fst'].sum()
        if kind == 'hi':
            num, den = g.loc[g['is_hi'], 'fst'].sum(), fst
        elif kind == 'lf':
            num, den = g.loc[g['is_norm'], 'LST_TEU'].sum(), fst
        else:  # w3
            num = fst
            rb = ROUTEBSA.get((grp, dly, m), 0.0)
            sh = share_sp('w3', grp, dly, sp) if sp else 1.0
            den = rb * (sh if sh is not None else 0.0)
        if den and den > 0:
            rates[m] = num / den
    return rates


def forecast(rates, pooled_act, cap):
    """실적에 앵커 + 월추세 보정(±0.20 한도). 외삽 폭주 방지.
    Q3전망 = 2~5월실적 + slope×(8 − 3.5월). slope는 월별 실적률 선형회귀."""
    if pooled_act is None:
        return None
    pts = sorted(rates.items())
    b = 0.0
    if len(pts) >= 3:
        xs = [p[0] for p in pts]; ys = [p[1] for p in pts]
        n = len(xs); sx = sum(xs); sy = sum(ys)
        sxx = sum(x * x for x in xs); sxy = sum(x * y for x, y in zip(xs, ys))
        d = n * sxx - sx * sx
        if d != 0:
            b = (n * sxy - sx * sy) / d
    adj = max(-0.20, min(0.20, b * (Q3T - 3.5)))   # 추세 보정 한도 ±20%p
    return max(0.0, min(cap, pooled_act + adj))


def parse_seg(label):
    m = re.match(r'\s*(.+?)\s*[→/]\s*([A-Za-z]+)', label)
    return (m.group(1).strip(), m.group(2).strip()) if m else (None, None)


def pctval(s):
    s = (s or '').strip().replace('%', '')
    try:
        return float(s) / 100.0
    except ValueError:
        return None


def colL(i):
    s = ''; i += 1
    while i:
        i, r = divmod(i - 1, 26); s = chr(65 + r) + s
    return s


def main():
    apply = '--apply' in sys.argv
    svc = get_service()
    grid = svc.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID, range=f"'{SHEET_NAME}'!A1:AA45").execute().get('values', [])

    def cell(row, c):
        return row[c].strip() if c < len(row) and row[c] is not None else ''

    updates, preview = [], []
    for kind, gc, lc, tc, q3c, dest in BLOCKS:
        cap = 1.0 if kind == 'hi' else 1.5
        cur = (None, None)
        for ri, row in enumerate(grid):
            if ri < 3:
                continue
            gub, label = cell(row, gc), cell(row, lc)
            if gub == '구간':
                cur = parse_seg(label)
                sp = None
                who = label
            elif label and gub != '구분':
                sp = label
                who = '  └ ' + label
            else:
                continue
            grp, dly = cur
            if grp is None:
                continue
            tgt = pctval(cell(row, tc))
            if tgt is None:
                continue
            act = pctval(cell(row, tc + 1))   # 2~5월 실적 (G/P/Y)
            f = forecast(monthly_rates(kind, grp, dly, sp), act, cap)
            if f is None or act is None:
                q3 = tgt
            else:
                if act >= tgt:                    # 달성/초과 → 실적 위로 상향(감소 금지)
                    stretch = min(0.10, max(0.02, 0.5 * (act - tgt)))
                    q3 = max(act + stretch, f)
                else:                              # 미달 → 갭 50% 축소(현재~목표), 추세 상향시 더
                    q3 = max((act + tgt) / 2, (f + tgt) / 2)
                    q3 = min(tgt, max(act, q3))
                q3 = max(0.0, min(cap, q3))
            r1 = ri + 1
            updates.append((f"{colL(q3c)}{r1}", round(q3, 4)))
            preview.append((kind, who, tgt, f, q3))

    for kind, who, tgt, f, q3 in preview:
        fp = '  -' if f is None else f'{f*100:.0f}%'
        print(f"{kind:3} {who:20} 목표{tgt*100:4.0f}%  예측{fp:>5}  →3Q목표 {q3*100:.0f}%")
    print(f"\n{len(updates)} cells.")

    if apply:
        body = {'valueInputOption': 'USER_ENTERED',
                'data': [{'range': f"'{SHEET_NAME}'!{a}", 'values': [[v]]} for a, v in updates]}
        svc.spreadsheets().values().batchUpdate(spreadsheetId=SPREADSHEET_ID, body=body).execute()
        # 3분기목표 % 서식(0%, +기호 없음) — I/R/AA
        freq = [{'repeatCell': {
            'range': {'sheetId': SHEET_ID, 'startRowIndex': 3, 'endRowIndex': 44,
                      'startColumnIndex': c, 'endColumnIndex': c + 1},
            'cell': {'userEnteredFormat': {'numberFormat': {'type': 'PERCENT', 'pattern': '0%'}}},
            'fields': 'userEnteredFormat.numberFormat'}} for c in (8, 17, 26)]
        svc.spreadsheets().batchUpdate(spreadsheetId=SPREADSHEET_ID, body={'requests': freq}).execute()
        print('APPLIED (3분기 목표 I/R/AA + % 서식).')
    else:
        print('(preview only; --apply to write)')


if __name__ == '__main__':
    main()
