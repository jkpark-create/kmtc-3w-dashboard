# -*- coding: utf-8 -*-
"""Fix per-salesperson 목표/실적/GAP in the existing "①②③ 저조구간·영업사원" sheet so
they reflect the salesperson's value WITHIN the segment (구간 = 선적지→도착국가),
instead of the segment-agnostic whole-origin sheet target / Q2 progress.

목표 (target): dashboard destination-sliced formula
    target = filtBase + (sheetTarget - unfBase)
    booking filtBase = w3f/bsa, lifting = w3l/w3f, high_profit = w3h/w3f   (2025 base2025 cells, sliced to dest country)
실적 (actual): segment Feb-May WOS-3 raw (same basis as the segment row)
    ① booking   = sp_w3fst / sp_bsa_alloc
    ② high_profit = sp_w3hi_fst / sp_w3fst                (= route_salesman hishare)
    ③ lifting   = sp_w3norm_lst / sp_bsa_alloc
    sp_bsa_alloc = route_bsa * (sp 2025 nlst share within segment)   (project BSA-by-2025-share convention)

Reads: output/laggard_q3_data.json (segment rows + route_salesman, same basis as the
       current sheet), dist/sales-target/base2025.json, dist/sales-target/index.json
Writes: the existing spreadsheet's Tab "①②③ 저조구간·영업사원" 목표/실적/GAP columns
        for 영업사원 rows only (segment rows untouched).

Usage:  runpy.cmd scripts/laggard_q3_fix_salesman.py            # preview only
        runpy.cmd scripts/laggard_q3_fix_salesman.py --apply    # write to sheet
"""
import json, sys
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')
ROOT = Path(__file__).resolve().parents[1]
DATA = json.load(open(ROOT / 'output/laggard_q3_data.json', encoding='utf-8'))
BASE = json.load(open(ROOT / 'dist/sales-target/base2025.json', encoding='utf-8'))
BASE = BASE.get('base', BASE)
IDX = json.load(open(ROOT / 'dist/sales-target/index.json', encoding='utf-8'))

SPREADSHEET_ID = '1alkFHfYvBF6pBss10SvmeU5RVEY5GbQQzF2U5kll6ZQ'
SHEET_NAME = '①②③ 저조구간·영업사원'

GRP_TABS = {
    'ID':  ['ID-IDO', 'JKT', 'SUB'],
    'SZP': ['CN_SHK_DCB'],
    'TAO': ['CN_TAO'],
    'NBO': ['CN_NBO'],
    'SHA': ['CN_SHA'],
    'TH':  ['TH'],
    'VN':  ['VN_HPH', 'VN_SGN_CMP'],
    'MY':  ['PKG+PKW', 'PEN'],
}
# block -> (kpi key in index, column letters for 목표/실적/GAP)
BLOCKS = {
    'w3': {'kpi': 'booking',     'cols': ('E', 'F', 'G'), 'name_col': 1,  'gubun_col': 0,  'dest_col': 3},
    'hi': {'kpi': 'high_profit', 'cols': ('M', 'N', 'O'), 'name_col': 9,  'gubun_col': 8,  'dest_col': 11},
    'lf': {'kpi': 'lifting',     'cols': ('U', 'V', 'W'), 'name_col': 17, 'gubun_col': 16, 'dest_col': 19},
}


def safe(a, b):
    return (a / b) if b else None


# ---- index.json SALES targets: (tab,name)->kpi, name->[(tab,kpi)] ----
IDX_BY_TAB_NAME = {}
IDX_BY_NAME = {}
for r in IDX.get('rows', []):
    if r.get('row_type') != 'SALES':
        continue
    IDX_BY_TAB_NAME[(r['tab'], r['name'])] = r['kpi']
    IDX_BY_NAME.setdefault(r['name'], []).append((r['tab'], r['kpi']))


def resolve_tab(grp, name):
    """Pick the base2025 tab (within grp) that holds this salesman; prefer max total nlst."""
    cands = [t for t in GRP_TABS.get(grp, []) if name in BASE.get(t, {})]
    if not cands:
        return None
    if len(cands) == 1:
        return cands[0]
    def tot_nlst(t):
        return sum(c.get('nlst', 0) for c in BASE[t][name].get('num', []))
    return max(cands, key=tot_nlst)


def sliced_2025(tab, name, dest):
    """Return (num measures dict, bsa) for cells matching dest country; dest=None => all."""
    slot = BASE.get(tab, {}).get(name)
    m = {'nlst': 0.0, 'w3f': 0.0, 'w3l': 0.0, 'w3h': 0.0}
    bsa = 0.0
    if not slot:
        return m, bsa
    for c in slot.get('num', []):
        if dest is not None and c.get('dlyc') != dest:
            continue
        m['nlst'] += c.get('nlst', 0) or 0
        m['w3f'] += c.get('w3f', 0) or 0
        m['w3l'] += c.get('w3l', 0) or 0
        m['w3h'] += c.get('w3h', 0) or 0
    for c in slot.get('bsa', []):
        if dest is not None and c.get('dlyc') != dest:
            continue
        bsa += c.get('bsa', 0) or 0
    return m, bsa


def sheet_target(tab, name, kpi):
    k = IDX_BY_TAB_NAME.get((tab, name))
    if not k:
        recs = IDX_BY_NAME.get(name, [])
        if len(recs) == 1:
            k = recs[0][1]
    if not k or kpi not in k:
        return None
    t = k[kpi].get('q1', {}).get('target')
    return t


def target_sliced(grp, name, dest, kpi):
    tab = resolve_tab(grp, name)
    if tab is None:
        return None, None
    m, bsa = sliced_2025(tab, name, dest)
    um, ubsa = sliced_2025(tab, name, None)
    if kpi == 'booking':
        filt, unf = safe(m['w3f'], bsa), safe(um['w3f'], ubsa)
    elif kpi == 'lifting':
        filt, unf = safe(m['w3l'], m['w3f']), safe(um['w3l'], um['w3f'])
    else:  # high_profit
        filt, unf = safe(m['w3h'], m['w3f']), safe(um['w3h'], um['w3f'])
    st = sheet_target(tab, name, kpi)
    if st is None:
        return None, tab
    if filt is not None and unf is not None:
        return filt + (st - unf), tab
    return st, tab


def alloc_bsa(grp, dly, sps):
    """Allocate route BSA to each salesman by 2025 nlst share within the segment dest.
    Returns {name: bsa_alloc}. nlst-equivalent fallback for salesmen w/o 2025 nlst."""
    route_bsa = DATA['route'][f'{grp}->{dly}']['bsa']
    nlst = {}
    for s in sps:
        tab = resolve_tab(grp, s['sp'])
        m, _ = sliced_2025(tab, s['sp'], dly) if tab else ({'nlst': 0.0}, 0)
        nlst[s['sp']] = m['nlst']
    sum_nlst = sum(nlst.values())
    sum_bkg_with_nlst = sum(s['bkg'] for s in sps if nlst[s['sp']] > 0)
    if sum_nlst <= 0:
        weights = {s['sp']: s['bkg'] for s in sps}        # no 2025 basis at all -> booking share
    else:
        ratio = (sum_nlst / sum_bkg_with_nlst) if sum_bkg_with_nlst > 0 else 0.0
        weights = {}
        for s in sps:
            w = nlst[s['sp']]
            if w <= 0:                                     # present but no 2025 nlst -> nlst-equivalent of bookings
                w = s['bkg'] * ratio
            weights[s['sp']] = w
    tw = sum(weights.values())
    if tw <= 0:
        return {s['sp']: 0.0 for s in sps}
    return {sp: route_bsa * w / tw for sp, w in weights.items()}


def compute():
    """Return {(block, grp, dly, name|'__SEG__'): {'target':, 'actual':, 'gap':}}.
    Salesman rows: actual = segment Feb-May raw. GAP = actual − target for ALL
    blocks. ③ 실선적률 = WOS-3 LST ÷ WOS-3 부킹 (conversion, NOT ÷BSA).
    Segment rows: target = booking-volume-weighted avg of member salesman sliced
    targets (keeps header coherent with the salesmen below it); 실적 untouched."""
    LAGGARD_ROUTES = {
        'w3': [('ID', 'AE'), ('NBO', 'JP'), ('SZP', 'SG'), ('VN', 'TH')],
        'hi': [('SZP', 'SG'), ('VN', 'HK'), ('TH', 'CN'), ('ID', 'HK')],
        'lf': [('ID', 'AE'), ('SZP', 'SG'), ('NBO', 'JP'), ('ID', 'HK')],
    }
    out = {}
    for block, routes in LAGGARD_ROUTES.items():
        kpi = BLOCKS[block]['kpi']
        for grp, dly in routes:
            key = f'{grp}->{dly}'
            route = DATA['route'][key]
            sps = [s for s in DATA['route_salesman'].get(key, []) if s['bkg'] and s['bkg'] > 0]
            bsa_alloc = alloc_bsa(grp, dly, sps)
            tw = bw = 0.0           # for segment-target volume weighting
            for s in sps:
                name = s['sp']
                tgt, _tab = target_sliced(grp, name, dly, kpi)
                ba = bsa_alloc.get(name, 0.0)
                if block == 'w3':
                    actual = safe(s['bkg'], ba)
                    gap = (actual - tgt) if (actual is not None and tgt is not None) else None
                elif block == 'lf':
                    actual = safe(s['w3norm'], s['bkg'])  # 실선적률 = WOS-3 LST ÷ WOS-3 부킹
                    gap = (actual - tgt) if (actual is not None and tgt is not None) else None
                else:
                    actual = s['hishare']
                    gap = (actual - tgt) if (actual is not None and tgt is not None) else None
                out[(block, grp, dly, name)] = {'target': tgt, 'actual': actual, 'gap': gap}
                if tgt is not None and s['bkg']:
                    tw += tgt * s['bkg']; bw += s['bkg']
            # ---- segment header: coherent sliced target ----
            seg_tgt = (tw / bw) if bw > 0 else None
            if block == 'w3':
                seg_act = route['w3bsa']
                seg_gap = (seg_act - seg_tgt) if (seg_act is not None and seg_tgt is not None) else None
            elif block == 'lf':
                seg_act = safe(route['w3norm'], route['bkg'])   # WOS-3 LST ÷ WOS-3 부킹
                seg_gap = (seg_act - seg_tgt) if (seg_act is not None and seg_tgt is not None) else None
            else:
                seg_act = route['hishare']
                seg_gap = (seg_act - seg_tgt) if (seg_act is not None and seg_tgt is not None) else None
            out[(block, grp, dly, '__SEG__')] = {'target': seg_tgt, 'actual': seg_act, 'gap': seg_gap}
    return out


def get_service():
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
    from googleapiclient.discovery import build
    cd = ROOT.parent / '.gdrive-mcp'
    installed = json.loads((cd / 'credentials.json').read_text(encoding='utf-8-sig'))['installed']
    token = json.loads((cd / 'token.json').read_text(encoding='utf-8-sig'))
    creds = Credentials(
        token=token.get('access_token'), refresh_token=token.get('refresh_token'),
        token_uri='https://oauth2.googleapis.com/token',
        client_id=installed['client_id'], client_secret=installed['client_secret'],
        scopes=token.get('scopes') or ['https://www.googleapis.com/auth/spreadsheets'])
    if not creds.valid:
        creds.refresh(Request())
    return build('sheets', 'v4', credentials=creds)


def pct(x):
    return '' if x is None else f'{round(x*100):.0f}%'


def num(x):
    """Decimal value for writing (existing cells are numbers w/ percent format)."""
    return '' if x is None else round(x, 4)


def main():
    apply = '--apply' in sys.argv
    vals = compute()
    svc = get_service()
    grid = svc.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{SHEET_NAME}'!A1:W60").execute().get('values', [])

    def cell(row, c):
        return row[c] if c < len(row) else ''

    updates = []       # (a1, value)
    preview = []
    for block, cfg in BLOCKS.items():
        gc, nc, dc = cfg['gubun_col'], cfg['name_col'], cfg['dest_col']
        tcol, acol, gcol = cfg['cols']
        cur_grp = cur_dly = None
        for ri, row in enumerate(grid):
            gub = cell(row, gc).strip()
            nm = cell(row, nc).strip()
            sheet_row = ri + 1
            if gub == '구간':
                # 선적항 column = gubun_col+2 (grp), 도착 = dest_col (dly)
                cur_grp = cell(row, gc + 2).strip()
                cur_dly = cell(row, dc).strip()
                rec = vals.get((block, cur_grp, cur_dly, '__SEG__'))
                if rec:  # segment header: update 목표 + GAP only (실적 = raw, untouched)
                    updates.append((f"'{SHEET_NAME}'!{tcol}{sheet_row}", num(rec['target'])))
                    updates.append((f"'{SHEET_NAME}'!{gcol}{sheet_row}", num(rec['gap'])))
                    preview.append((block, '['+cur_grp+'→'+cur_dly+']', '(구간)', sheet_row,
                                    cell(row, _col_idx(tcol)), pct(rec['target']),
                                    cell(row, _col_idx(acol)), '(유지)',
                                    cell(row, _col_idx(gcol)), pct(rec['gap'])))
                continue
            if not nm or gub.startswith('기간') or nm in ('구간/영업사원',):
                continue
            if cur_grp is None or cur_dly is None:
                continue
            rec = vals.get((block, cur_grp, cur_dly, nm))
            if not rec:
                continue
            for col, v in ((tcol, rec['target']), (acol, rec['actual']), (gcol, rec['gap'])):
                a1 = f"'{SHEET_NAME}'!{col}{sheet_row}"
                updates.append((a1, num(v)))
            preview.append((block, cur_grp+'→'+cur_dly, nm, sheet_row,
                            cell(row, _col_idx(tcol)), pct(rec['target']),
                            cell(row, _col_idx(acol)), pct(rec['actual']),
                            cell(row, _col_idx(gcol)), pct(rec['gap'])))

    print(f"{'blk':3} {'seg':12} {'name':12} {'row':>3}  {'목표(old→new)':>16} {'실적(old→new)':>16} {'GAP(old→new)':>16}")
    for b, seg, nm, sr, ot, nt, oa, na, og, ng in preview:
        print(f"{b:3} {seg:12} {nm:12} {sr:>3}  {ot:>7}→{nt:>7}  {oa:>7}→{na:>7}  {og:>7}→{ng:>7}")
    print(f"\n{len(updates)} cells to update across {len(preview)} rows (incl. segment headers).")

    if apply:
        data = [{'range': a1, 'values': [[v]]} for a1, v in updates if v != '']
        body = {'valueInputOption': 'USER_ENTERED', 'data': data}
        svc.spreadsheets().values().batchUpdate(spreadsheetId=SPREADSHEET_ID, body=body).execute()
        print(f'APPLIED {len(data)} cells to sheet.')
    else:
        print('(preview only; pass --apply to write)')


def _col_idx(letter):
    idx = 0
    for ch in letter:
        idx = idx * 26 + (ord(ch) - ord('A') + 1)
    return idx - 1


if __name__ == '__main__':
    main()
