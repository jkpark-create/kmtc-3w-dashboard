# -*- coding: utf-8 -*-
"""③ 실선적률 블록(현 레이아웃 T~AA열)만 새 저조구간으로 교체.
구간: 주간BSA>150 중 실선적률 GAP 최저 4 = NBO→TH, NBO→ID, NBO→JP, ID→PH.
실선적률 = Σ WOS-3 Normal LST ÷ Σ WOS-3 부킹(FST)  (Feb-May, 총합기준).
목표 = 영업사원 sliced lifting 목표의 부킹가중평균.
영업사원 필터: GAP<0  OR  (목표≤35% & 0≤GAP≤10%);  목표 없는 사람 제외.
화주수 = distinct BKG_SHPR_CST_NO (fst>0): 전체 / 3주전(WOS-3).
열: T구분 U라벨 V전체화주 W3주전화주 X목표 Y실적 Z GAP  (AA 3분기목표는 불변).

Usage: runpy.cmd scripts/rebuild_block3.py            # preview
       runpy.cmd scripts/rebuild_block3.py --apply
"""
import sys
from pathlib import Path
import pandas as pd
sys.stdout.reconfigure(encoding='utf-8')
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'scripts'))
from laggard_q3_fix_salesman import target_sliced, get_service, SPREADSHEET_ID, SHEET_NAME

SNAP = ROOT / 'output/booking_snapshot_result_20260612.csv'
BSA = ROOT / 'output/BSA_raw_monthly3W_20260612.csv'
PERIOD = ['202602', '202603', '202604', '202605']
WEEKS = 17
CST = 'BKG_SHPR_CST_NO'
SEGMENTS = [('NBO', 'TH'), ('NBO', 'ID'), ('NBO', 'JP'), ('ID', 'PH')]
SHEET_ID = 0
COL0 = 19          # T : 구분
SEG_BG = {'red': 0.85, 'green': 0.92, 'blue': 0.99}
PCT = {'type': 'PERCENT', 'pattern': '0%'}
GAPNF = {'type': 'PERCENT', 'pattern': '+0%;-0%'}
INTNF = {'type': 'NUMBER', 'pattern': '#,##0'}
LOW_TGT, SMALL_GAP = 0.35, 0.10


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

bsa = pd.read_csv(BSA, low_memory=False)
bsa['bsa'] = pd.to_numeric(bsa['TEU_BSA (Actual)'], errors='coerce')
bsa = bsa[(bsa['team'] == 'OBT') & (bsa['YYYYMM'].astype(str).isin(PERIOD))].copy()
bsa['grp'] = [grp_of(p, c) for p, c in zip(bsa['POR_PORT'], bsa['POR_Country'])]
WBSA = (bsa.groupby(['grp', 'DLY_Country'])['bsa'].sum() / WEEKS).to_dict()


def hwaju(sub):
    s = sub[sub['fst'] > 0]
    return int(s[CST].nunique()), int(s.loc[s['is_w3'], CST].nunique())


def conv(sub):
    w3 = sub[sub['is_w3']]
    fst = w3['fst'].sum()
    lst = w3.loc[w3['is_norm'], 'LST_TEU'].sum()
    return (lst / fst) if fst else None, float(fst)


def build_rows():
    rows = []
    for grp, dly in SEGMENTS:
        seg = df[(df['grp'] == grp) & (df['DLY_CTR_CD'] == dly)]
        act, _ = conv(seg)
        tot, w3 = hwaju(seg)
        wb = round(WBSA.get((grp, dly), 0))
        # salesmen
        tw = bw = 0.0
        sps = []
        for sp, g in seg[seg['is_w3']].groupby('Salesman_POR'):
            a, fst = conv(g)
            if fst <= 0:
                continue
            t, _tab = target_sliced(grp, sp, dly, 'lifting')
            gap = (a - t) if (a is not None and t is not None) else None
            st, sw = hwaju(seg[seg['Salesman_POR'] == sp])
            sps.append({'sp': sp, 'act': a, 'tgt': t, 'gap': gap, 'tot': st, 'w3': sw, 'fst': fst})
            if t is not None:
                tw += t * fst; bw += fst
        seg_tgt = (tw / bw) if bw > 0 else None
        seg_gap = (act - seg_tgt) if (act is not None and seg_tgt is not None) else None
        kept = [s for s in sps if s['tgt'] is not None and s['gap'] is not None
                and (s['gap'] < 0 or (s['tgt'] <= LOW_TGT and 0 <= s['gap'] <= SMALL_GAP))]
        kept.sort(key=lambda s: s['gap'])
        rows.append({'kind': 'seg', 'label': f'{grp}→{dly} ({wb})', 'tot': tot, 'w3': w3,
                     'tgt': seg_tgt, 'act': act, 'gap': seg_gap})
        for s in kept:
            rows.append({'kind': 'sales', 'label': s['sp'], 'tot': s['tot'], 'w3': s['w3'],
                         'tgt': s['tgt'], 'act': s['act'], 'gap': s['gap']})
    return rows


def cellnum(v, nf=None, bg=None, bold=False):
    cd = {}
    if v is not None and v != '':
        cd['userEnteredValue'] = {'numberValue': v} if isinstance(v, (int, float)) else {'stringValue': str(v)}
    fmt = {}
    if bg:
        fmt['backgroundColor'] = bg
    if bold:
        fmt['textFormat'] = {'bold': True}
    if nf:
        fmt['numberFormat'] = nf
    cd['userEnteredFormat'] = fmt
    return cd


def main():
    apply = '--apply' in sys.argv
    rows = build_rows()
    print(f"{'구분':9} {'구간/영업사원':16} {'전체화주':>5}{'3주전':>5} {'목표':>5}{'실적':>5}{'GAP':>6}")
    for r in rows:
        tp = '' if r['tgt'] is None else f"{r['tgt']*100:.0f}%"
        ap = '' if r['act'] is None else f"{r['act']*100:.0f}%"
        gp = '' if r['gap'] is None else f"{r['gap']*100:+.0f}%"
        k = '구간' if r['kind'] == 'seg' else '└영업사원'
        print(f"{k:9} {r['label']:16} {r['tot']:>5}{r['w3']:>5} {tp:>5}{ap:>5}{gp:>6}")
    print(f"\n{len(rows)} rows (구간 {sum(1 for r in rows if r['kind']=='seg')}).")
    if not apply:
        print('(preview only; --apply to write)')
        return

    grid_rows = []
    for r in rows:
        seg = r['kind'] == 'seg'
        bg = SEG_BG if seg else None
        b = seg
        grid_rows.append({'values': [
            cellnum('구간' if seg else '└ 영업사원', bg=bg, bold=b),
            cellnum(r['label'], bg=bg, bold=b),
            cellnum(r['tot'], nf=INTNF, bg=bg, bold=b),
            cellnum(r['w3'], nf=INTNF, bg=bg, bold=b),
            cellnum(r['tgt'], nf=PCT, bg=bg, bold=b),
            cellnum(r['act'], nf=PCT, bg=bg, bold=b),
            cellnum(r['gap'], nf=GAPNF, bg=bg, bold=b),
        ]})
    # 데이터는 4행(rowIndex=3)부터. 1~3행은 헤더.
    start_row = 3
    end_row = start_row + len(grid_rows)
    blank = {'values': [cellnum('') for _ in range(7)]}
    for _ in range(end_row, 45):
        grid_rows.append(blank)

    # 3행 헤더(V3:AA3 = 전체화주수/3주전화주수/목표/실적/GAP/목표) 복원:
    # 직전 잘못된 쓰기로 덮어써졌으므로 블록②의 동일 헤더(M3:R3)를 복사
    req = [{'copyPaste': {
        'source': {'sheetId': SHEET_ID, 'startRowIndex': 2, 'endRowIndex': 3,
                   'startColumnIndex': 12, 'endColumnIndex': 18},
        'destination': {'sheetId': SHEET_ID, 'startRowIndex': 2, 'endRowIndex': 3,
                        'startColumnIndex': 21, 'endColumnIndex': 27},
        'pasteType': 'PASTE_NORMAL'}}]
    req.append({'updateCells': {
        'rows': grid_rows,
        'fields': 'userEnteredValue,userEnteredFormat',
        'start': {'sheetId': SHEET_ID, 'rowIndex': start_row, 'columnIndex': COL0}}})
    # title
    req.append({'updateCells': {
        'rows': [{'values': [cellnum('③ 실선적률 저조 (실선적률 = WOS-3 LST ÷ WOS-3 부킹)')]}],
        'fields': 'userEnteredValue',
        'start': {'sheetId': SHEET_ID, 'rowIndex': 0, 'columnIndex': COL0}}})
    get_service().spreadsheets().batchUpdate(
        spreadsheetId=SPREADSHEET_ID, body={'requests': req}).execute()
    print('APPLIED (③블록 T~Z 교체).')


if __name__ == '__main__':
    main()
