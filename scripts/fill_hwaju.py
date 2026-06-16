# -*- coding: utf-8 -*-
"""기존 "①②③ 저조구간·영업사원" 탭의 전체화주수/3주전화주수 컬럼만 채운다.
다른 셀은 건드리지 않음. 행(구간/영업사원)은 시트를 읽어 그대로 매칭.

화주수 = distinct BKG_SHPR_CST_NO (fst>0, OBT scope, Feb-May).
  전체화주수 = 모든 lead-time / 3주전화주수 = WOS-3 만.
블록 ①③: 도착 = DLY_CTR_CD,  grp_of(POR_PLC,POR_CTR)
블록 ②  : 도착 = DLY_PLC_CD(NSA),  중국 grp(SHA/NBO/TAO/SZP=SHK+DCB)

Usage: runpy.cmd scripts/fill_hwaju.py            # preview
       runpy.cmd scripts/fill_hwaju.py --apply
"""
import sys, re
from pathlib import Path
import pandas as pd
sys.stdout.reconfigure(encoding='utf-8')
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'scripts'))
from laggard_q3_fix_salesman import get_service, SPREADSHEET_ID, SHEET_NAME

SNAP = ROOT / 'output/booking_snapshot_result_20260612.csv'
PERIOD = ['202602', '202603', '202604', '202605']
CST = 'BKG_SHPR_CST_NO'
# block -> (gubun_col, label_col, 전체화주_col, 3주전화주_col, dest_kind)
BLOCKS = [
    ('①', 0, 1, 2, 3, 'CTR'),
    ('②', 8, 9, 10, 11, 'PLC'),
    ('③', 16, 17, 18, 19, 'CTR'),
]
CN_GRP = {'SHA': ['SHA'], 'NBO': ['NBO'], 'TAO': ['TAO'], 'SZP': ['SHK', 'DCB']}


def grp_of(plc, ctr):
    if plc in ('SHK', 'DCB'):
        return 'SZP'
    if plc in ('NBO', 'TAO', 'SHA'):
        return plc
    if ctr in ('ID', 'VN', 'TH', 'MY'):
        return ctr
    return None


def load():
    df = pd.read_csv(SNAP, low_memory=False)
    df = df[(~df['POR_CTR_CD'].isin(['KR', 'JP'])) & (df['DLY_CTR_CD'] != 'KR')].copy()
    for c in ('FST_TEU', 'LST_TEU'):
        df[c] = pd.to_numeric(df[c], errors='coerce')
    df['fst'] = df['FST_TEU'].fillna(df['LST_TEU'])
    df['ym'] = df['YYYYMM'].astype(str)
    df = df[df['ym'].isin(PERIOD) & (df['fst'] > 0)].copy()
    df['is_w3'] = df['Lead_time (BKG_Sche)'] == 'WOS-3'
    df['grp_ctr'] = [grp_of(p, c) for p, c in zip(df['POR_PLC_CD'], df['POR_CTR_CD'])]
    p2cn = {p: g for g, ps in CN_GRP.items() for p in ps}
    df['grp_cn'] = df['POR_PLC_CD'].map(p2cn)
    return df


DF = load()


def counts(kind, grp, dly, sp=None):
    if kind == 'PLC':           # block ② : china grp -> DLY_PLC
        sub = DF[(DF['grp_cn'] == grp) & (DF['DLY_PLC_CD'] == dly)]
    else:                        # block ①③ : grp_of -> DLY_CTR
        sub = DF[(DF['grp_ctr'] == grp) & (DF['DLY_CTR_CD'] == dly)]
    if sp is not None:
        sub = sub[sub['Salesman_POR'] == sp]
    total = sub[CST].nunique()
    w3 = sub.loc[sub['is_w3'], CST].nunique()
    return int(total), int(w3)


def parse_seg(label):
    # "ID→AE (68)" -> ('ID','AE')
    m = re.match(r'\s*(.+?)\s*→\s*([A-Za-z]+)', label)
    if not m:
        return None, None
    return m.group(1).strip(), m.group(2).strip()


def col_letter(i):
    s = ''
    i += 1
    while i:
        i, r = divmod(i - 1, 26)
        s = chr(65 + r) + s
    return s


def main():
    apply = '--apply' in sys.argv
    svc = get_service()
    grid = svc.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID, range=f"'{SHEET_NAME}'!A1:W60").execute().get('values', [])

    def cell(row, c):
        return row[c].strip() if c < len(row) and row[c] is not None else ''

    updates = []
    preview = []
    for tag, gc, lc, totc, w3c, kind in BLOCKS:
        cur_grp = cur_dly = None
        for ri, row in enumerate(grid):
            if ri < 2:
                continue
            gub = cell(row, gc)
            label = cell(row, lc)
            if gub == '구간':
                cur_grp, cur_dly = parse_seg(label)
                if cur_grp is None:
                    continue
                tot, w3 = counts(kind, cur_grp, cur_dly)
            elif label and gub != '구분':
                if cur_grp is None:
                    continue
                tot, w3 = counts(kind, cur_grp, cur_dly, sp=label)
            else:
                continue
            r1 = ri + 1
            updates.append((f"'{SHEET_NAME}'!{col_letter(totc)}{r1}", tot))
            updates.append((f"'{SHEET_NAME}'!{col_letter(w3c)}{r1}", w3))
            preview.append((tag, r1, gub or '·', label, cur_grp, cur_dly, tot, w3))

    for tag, r1, gub, label, g, d, tot, w3 in preview:
        print(f"{tag} r{r1:<2} {gub:8} {label:16} [{g}→{d}]  전체화주 {tot:>3}  3주전화주 {w3:>3}")
    print(f"\n{len(updates)} cells across {len(preview)} rows.")

    if apply:
        body = {'valueInputOption': 'USER_ENTERED',
                'data': [{'range': a, 'values': [[v]]} for a, v in updates]}
        svc.spreadsheets().values().batchUpdate(spreadsheetId=SPREADSHEET_ID, body=body).execute()
        print('APPLIED.')
    else:
        print('(preview only; pass --apply to write)')


if __name__ == '__main__':
    main()
