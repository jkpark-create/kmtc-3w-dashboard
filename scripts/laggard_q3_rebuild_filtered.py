# -*- coding: utf-8 -*-
"""Rebuild the "①②③ 저조구간·영업사원" tab keeping ONLY negative-GAP (저조) salesmen
under each segment, and add a 주간BSA column (segment weekly-avg BSA = route BSA over
Feb-May / 17 weeks). Same horizontal 3-block look, now 8 data cols per block.

Reuses compute() from laggard_q3_fix_salesman (segment-sliced target / segment actual / gap).

Usage:  runpy.cmd scripts/laggard_q3_rebuild_filtered.py            # preview
        runpy.cmd scripts/laggard_q3_rebuild_filtered.py --apply    # rebuild sheet
"""
import sys
from pathlib import Path
sys.stdout.reconfigure(encoding='utf-8')
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'scripts'))
from laggard_q3_fix_salesman import compute, DATA, get_service, SPREADSHEET_ID, SHEET_NAME
from nsa_hp_compute import build_nsa_block_rows

SHEET_ID = 0
WEEKS = 17  # distinct (YYYYMM,WW) weeks in Feb-May 2026 (OBT)
LAGGARD_ROUTES = {
    'w3': [('ID', 'AE'), ('NBO', 'JP'), ('SZP', 'SG'), ('VN', 'TH')],
    'hi': [('SZP', 'SG'), ('VN', 'HK'), ('TH', 'CN'), ('ID', 'HK')],
    'lf': [('ID', 'AE'), ('SZP', 'SG'), ('NBO', 'JP'), ('ID', 'HK')],
}
TITLES = {
    'w3': '① 3주전부킹 저조 (3주전부킹률 = WOS-3 부킹(FST) ÷ BSA)',
    'hi': '② 고수익비중 — 중국발→NSA (고수익 부킹 ÷ 전체 부킹, WOS-3)',
    'lf': '③ 실선적률 저조 (실선적률(3주전) = WOS-3 실선적(Normal) ÷ BSA)',
}
HEADERS = ['구분', '구간/영업사원', '선적항', '도착', '목표', '실적', 'GAP(%p)', '주간BSA']
# (block, grp, dly) segments that bypass the neg-GAP filter and show ALL salesmen
# ① VN→TH: 목표 자체가 낮았고 1차 목표 달성 → 전원 표시
SHOW_ALL = {('w3', 'VN', 'TH')}
BLOCK_ORDER = ['w3', 'hi', 'lf']
BLOCK_START = {'w3': 0, 'hi': 9, 'lf': 18}   # start col index; 8 data cols + 1 spacer
NCOLS = 26                                    # A..Z

# colors / formats
HDR_BG = {'red': 0.12, 'green': 0.29, 'blue': 0.49}
SEG_BG = {'red': 0.85, 'green': 0.92, 'blue': 0.99}
WHITE = {'red': 1, 'green': 1, 'blue': 1}
PCT = {'type': 'PERCENT', 'pattern': '0%'}
GAPNF = {'type': 'PERCENT', 'pattern': '+0%;-0%'}
BSANF = {'type': 'NUMBER', 'pattern': '#,##0'}


def build_block_rows(block):
    """Return ordered list of row dicts for one block: each {kind, cells:[8]}."""
    if block == 'hi':                       # ② 고수익비중 → 중국발→NSA (별도 산출)
        return build_nsa_block_rows()
    vals = COMPUTED
    rows = []
    for grp, dly in LAGGARD_ROUTES[block]:
        key = f'{grp}->{dly}'
        seg = vals.get((block, grp, dly, '__SEG__'), {})
        wk_bsa = round(DATA['route'][key]['bsa'] / WEEKS) if DATA['route'].get(key) else None
        rows.append({'kind': 'seg',
                     'cells': ['구간', f'{grp}→{dly}', grp, dly,
                               seg.get('target'), seg.get('actual'), seg.get('gap'), wk_bsa]})
        sps = [s for s in DATA['route_salesman'].get(key, []) if s['bkg'] and s['bkg'] > 0]
        show_all = (block, grp, dly) in SHOW_ALL
        kept = []
        for s in sps:
            rec = vals.get((block, grp, dly, s['sp']))
            if rec and rec['gap'] is not None and (show_all or rec['gap'] < 0):
                kept.append((s, rec))
        kept.sort(key=lambda x: x[1]['gap'])      # worst (most negative) first
        for s, rec in kept:
            rows.append({'kind': 'sales',
                         'cells': ['└ 영업사원', s['sp'], s['plc'], '',
                                   rec['target'], rec['actual'], rec['gap'], None]})
    return rows


def cell(value=None, *, bg=None, bold=False, white=False, nf=None, align=None):
    cd = {}
    if value is not None and value != '':
        cd['userEnteredValue'] = ({'numberValue': value} if isinstance(value, (int, float))
                                  else {'stringValue': str(value)})
    fmt = {}
    if bg:
        fmt['backgroundColor'] = bg
    tf = {}
    if bold:
        tf['bold'] = True
    if white:
        tf['foregroundColor'] = WHITE
    if tf:
        fmt['textFormat'] = tf
    if nf:
        fmt['numberFormat'] = nf
    if align:
        fmt['horizontalAlignment'] = align
    cd['userEnteredFormat'] = fmt
    return cd


def main():
    apply = '--apply' in sys.argv
    global COMPUTED
    COMPUTED = compute()
    block_rows = {b: build_block_rows(b) for b in BLOCK_ORDER}
    body_h = max(len(v) for v in block_rows.values())
    total_rows = 2 + body_h                      # title + header + body

    # ---- preview ----
    for b in BLOCK_ORDER:
        print(f"\n=== {TITLES[b]} ===")
        for r in block_rows[b]:
            c = r['cells']
            def p(x):
                return '' if x is None or x == '' else (f'{round(x*100):+d}%' if isinstance(x, float) and r['kind']=='x' else (f'{round(x*100)}%' if isinstance(x, float) else str(x)))
            mark = '  └' if r['kind'] == 'sales' else '구간'
            print(f"  {mark} {c[1]:12} {c[2]:4} {c[3]:3} 목표 {p(c[4]):>5} 실적 {p(c[5]):>5} GAP {p(c[6]):>6} 주간BSA {c[7] if c[7] is not None else ''}")
    print(f"\nGrid: {total_rows} rows x {NCOLS} cols. Salesman kept (neg-GAP only): "
          f"{sum(1 for b in BLOCK_ORDER for r in block_rows[b] if r['kind']=='sales')}")

    if not apply:
        print('\n(preview only; pass --apply to rebuild the sheet)')
        return

    # ---- build full grid rowData ----
    grid = []
    # row 0: titles
    row0 = [cell() for _ in range(NCOLS)]
    for b in BLOCK_ORDER:
        s = BLOCK_START[b]
        for j in range(8):
            row0[s + j] = cell(TITLES[b] if j == 0 else None, bg=HDR_BG, bold=True, white=True)
    grid.append(row0)
    # row 1: headers
    row1 = [cell() for _ in range(NCOLS)]
    for b in BLOCK_ORDER:
        s = BLOCK_START[b]
        for j, h in enumerate(HEADERS):
            row1[s + j] = cell(h, bg=HDR_BG, bold=True, white=True, align='CENTER')
    grid.append(row1)
    # body rows
    for i in range(body_h):
        row = [cell() for _ in range(NCOLS)]
        for b in BLOCK_ORDER:
            s = BLOCK_START[b]
            seq = block_rows[b]
            if i >= len(seq):
                continue
            r = seq[i]
            cc = r['cells']
            is_seg = r['kind'] == 'seg'
            bg = SEG_BG if is_seg else None
            bold = is_seg
            # 구분, label, 선적항, 도착
            for j in range(4):
                row[s + j] = cell(cc[j], bg=bg, bold=bold)
            # 목표, 실적, GAP, 주간BSA
            row[s + 4] = cell(cc[4], bg=bg, bold=bold, nf=PCT)
            row[s + 5] = cell(cc[5], bg=bg, bold=bold, nf=PCT)
            row[s + 6] = cell(cc[6], bg=bg, bold=bold, nf=GAPNF)
            row[s + 7] = cell(cc[7], bg=bg, bold=bold, nf=BSANF, align='CENTER')
        grid.append(row)

    requests = [{
        'updateSheetProperties': {
            'properties': {'sheetId': SHEET_ID, 'gridProperties': {'columnCount': NCOLS}},
            'fields': 'gridProperties.columnCount',
        }
    }, {
        # drop any leftover row-1 title merges (old 7-col layout: A1:G1/I1:O1/Q1:W1)
        # — they block the new block titles/backgrounds if left in place
        'unmergeCells': {'range': {'sheetId': SHEET_ID, 'startRowIndex': 0, 'endRowIndex': 1,
                                   'startColumnIndex': 0, 'endColumnIndex': NCOLS}}
    }, {
        'updateCells': {
            'rows': [{'values': r} for r in grid],
            'fields': 'userEnteredValue,userEnteredFormat',
            'start': {'sheetId': SHEET_ID, 'rowIndex': 0, 'columnIndex': 0},
        }
    }]
    # clear any leftover rows below the new grid (old sheet had ~43 rows)
    if total_rows < 60:
        requests.append({'updateCells': {
            'range': {'sheetId': SHEET_ID, 'startRowIndex': total_rows, 'endRowIndex': 60,
                      'startColumnIndex': 0, 'endColumnIndex': NCOLS},
            'fields': 'userEnteredValue,userEnteredFormat'}})
    # column widths: per block [구분79,라벨93,선적항47,도착34,목표37,실적37,GAP56,BSA52], spacer 18
    widths = [79, 93, 47, 34, 37, 37, 56, 52]
    for b in BLOCK_ORDER:
        s = BLOCK_START[b]
        for j, w in enumerate(widths):
            requests.append({'updateDimensionProperties': {
                'range': {'sheetId': SHEET_ID, 'dimension': 'COLUMNS',
                          'startIndex': s + j, 'endIndex': s + j + 1},
                'properties': {'pixelSize': w}, 'fields': 'pixelSize'}})
        if s + 8 < NCOLS:   # spacer column (last block has none)
            requests.append({'updateDimensionProperties': {
                'range': {'sheetId': SHEET_ID, 'dimension': 'COLUMNS',
                          'startIndex': s + 8, 'endIndex': s + 9},
                'properties': {'pixelSize': 18}, 'fields': 'pixelSize'}})

    svc = get_service()
    svc.spreadsheets().batchUpdate(spreadsheetId=SPREADSHEET_ID, body={'requests': requests}).execute()
    print(f'\nAPPLIED: rebuilt {total_rows} rows.')


if __name__ == '__main__':
    main()
