# -*- coding: utf-8 -*-
"""Create a new Google Sheet for the 저조구간 Q3목표·영업사원 실적 analysis.

Reads output/laggard_q3_data.json (produced by laggard_q3_compute.py) and writes:
  Tab 1 ①②③ 저조구간·영업사원   — laggard routes + within-route salesman breakdown
  Tab 2 영업사원 KPI 목표·실적     — salesman-level target/perform/gap (index.json)
  Tab 3 선적지(국가) 요약          — origin 3-KPI target/perform/gap
  Tab 4 기준·정의                  — methodology & caveats
"""
import json
from pathlib import Path
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

ROOT = Path(__file__).resolve().parents[1]
DATA = json.load(open(ROOT / 'output/laggard_q3_data.json', encoding='utf-8'))
SALES_TARGET_INDEX = ROOT / 'dist/sales-target/index.json'

KPIS = [
    ('w3', '① 3주전부킹 저조', 'booking',
     '3주전부킹률 = WOS-3 부킹(FST) ÷ BSA', 'w3bsa'),
    ('hi', '② 고수익비중 저조', 'high_profit',
     '고수익비중 = 고수익 부킹 ÷ 전체 부킹 (WOS-3)', 'hishare'),
    ('lf', '③ 실선적률 저조', 'lifting',
     '실선적률(3주전) = WOS-3 실선적(Normal) ÷ BSA', 'lft3w'),
]
LAGGARD_ROUTES = {
    'w3': [('ID', 'AE'), ('NBO', 'JP'), ('SZP', 'SG'), ('VN', 'TH')],
    'hi': [('SZP', 'SG'), ('VN', 'HK'), ('TH', 'CN'), ('ID', 'HK')],
    'lf': [('ID', 'AE'), ('SZP', 'SG'), ('NBO', 'JP'), ('ID', 'HK')],
}
LAGGARD_GRPS = {
    'w3': ['ID', 'SZP', 'TAO', 'NBO'],
    'hi': ['TH', 'SZP', 'VN', 'MY'],
    'lf': ['SZP', 'ID', 'VN', 'NBO'],
}
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


def get_creds():
    cd = ROOT.parent / '.gdrive-mcp'
    installed = json.loads((cd / 'credentials.json').read_text(encoding='utf-8-sig'))['installed']
    token = json.loads((cd / 'token.json').read_text(encoding='utf-8-sig'))
    creds = Credentials(
        token=token.get('access_token'), refresh_token=token.get('refresh_token'),
        token_uri='https://oauth2.googleapis.com/token',
        client_id=installed['client_id'], client_secret=installed['client_secret'],
        scopes=token.get('scopes') or ['https://www.googleapis.com/auth/spreadsheets',
                                       'https://www.googleapis.com/auth/drive'])
    if not creds.valid:
        creds.refresh(Request())
    return creds


def pct(x):
    return None if x is None else round(x, 4)


def i(x):
    return None if x is None else round(x)


def load_sales_target_index_kpis():
    if not SALES_TARGET_INDEX.exists():
        return {}, {}
    idx = json.load(open(SALES_TARGET_INDEX, encoding='utf-8'))
    by_tab_name = {}
    by_name = {}
    for row in idx.get('rows', []):
        if row.get('row_type') != 'SALES':
            continue
        tab = row.get('tab', '')
        name = row.get('name', '')
        if not tab or not name:
            continue

        def pick(kpi):
            q1 = row['kpi'][kpi]['q1']
            q2 = row['kpi'][kpi]['q2']
            return {
                'target': q1.get('target'),
                'q2': q2.get('progress'),
                'gap': q2.get('gap'),
            }

        rec = {
            'booking': pick('booking'),
            'lifting': pick('lifting'),
            'high_profit': pick('high_profit'),
        }
        by_tab_name[(tab, name)] = rec
        by_name.setdefault(name, []).append((tab, rec))
    return by_tab_name, by_name


def sales_target_tab_candidates(grp, plc):
    tabs = []
    for tab in (plc, f'CN_{plc}'):
        if tab:
            tabs.append(tab)
    if grp == 'ID':
        tabs.extend([plc, 'ID-IDO'])
    elif grp == 'SZP':
        tabs.append('CN_SHK_DCB')
    elif grp in ('NBO', 'TAO', 'SHA'):
        tabs.append(f'CN_{grp}')
    elif grp == 'VN':
        tabs.extend(['VN_HPH'] if plc == 'HPH' else ['VN_SGN_CMP'] if plc in ('SGN', 'CMP') else ['VN_HPH', 'VN_SGN_CMP'])
    elif grp == 'MY':
        tabs.extend(['PEN'] if plc == 'PEN' else ['PKG+PKW'] if plc in ('PKG', 'PKW') else ['PKG+PKW', 'PEN'])
    else:
        tabs.extend(GRP_TABS.get(grp, [grp]))

    out = []
    seen = set()
    for tab in tabs + GRP_TABS.get(grp, []):
        if tab and tab not in seen:
            out.append(tab)
            seen.add(tab)
    return out


def lookup_salesperson_kpi(sales_kpi, index_by_tab_name, index_by_name, grp, plc, name, tkey):
    if (grp, name) in sales_kpi and tkey in sales_kpi[(grp, name)]:
        return sales_kpi[(grp, name)][tkey]
    for tab in sales_target_tab_candidates(grp, plc):
        rec = index_by_tab_name.get((tab, name))
        if rec and tkey in rec:
            return rec[tkey]
    name_recs = index_by_name.get(name, [])
    if len(name_recs) == 1:
        return name_recs[0][1].get(tkey, {})
    return {}

# ---------------- Tab 1: 저조구간·영업사원 ----------------
HDR1 = ['구분', '구간/영업사원', '선적항', '도착', '목표율', '실적', 'GAP(%p)',
        '3주전부킹(TEU)', '부킹비중', '고수익비중', '실선적전환율', '실선적(WOS-3)', '소석률(전체)']


def build_tab1():
    rows = [HDR1[:]]
    meta = {'section': [], 'route': [], 'sales': [], 'note': []}
    rows.append([f"기간: {DATA['period']}  ·  기준일: {DATA['data_date']}  ·  OBT 기준  ·  구간행=선적지 KPI 목표/실적, 영업사원행=영업사원 KPI 목표/2Q진척"]
                + ['']*12)
    meta['note'].append(len(rows)-1)
    sales_kpi = {
        (grp, s['sp']): s
        for grp, sales in DATA.get('salesman_targets', {}).items()
        for s in sales
    }
    index_by_tab_name, index_by_name = load_sales_target_index_kpis()
    for kpi, title, tkey, formula, mkey in KPIS:
        rows.append([f"{title}    ({formula})"] + ['']*12)
        meta['section'].append(len(rows)-1)
        for (grp, dly) in LAGGARD_ROUTES[kpi]:
            key = f"{grp}->{dly}"
            m = DATA['route'][key]
            tgt = DATA['targets'][grp][tkey]
            if kpi == 'lf':
                actual = m['lft3w']; gap = (m['occ'] - tgt) if (m['occ'] is not None and tgt is not None) else None
            else:
                actual = m[mkey]; gap = (actual - tgt) if (actual is not None and tgt is not None) else None
            rows.append(['구간', f"{grp}→{dly}", grp, dly, pct(tgt), pct(actual), pct(gap),
                         i(m['bkg']), pct(1.0), pct(m['hishare']), pct(m['lft3w']),
                         i(m['w3norm']), pct(m['occ'])])
            meta['route'].append(len(rows)-1)
            sps = [s for s in DATA['route_salesman'][key] if s['bkg'] and s['bkg'] > 0]
            for s in sps:
                sk = lookup_salesperson_kpi(sales_kpi, index_by_tab_name, index_by_name, grp, s['plc'], s['sp'], tkey)
                rows.append(['└ 영업사원', s['sp'], s['plc'], '',
                             pct(sk.get('target')), pct(sk.get('q2')), pct(sk.get('gap')),
                             i(s['bkg']), pct(s['bkg_share']), pct(s['hishare']),
                             pct(s['conv']), i(s['w3norm']), pct(s['norm_all'] / m['bsa']) if m['bsa'] else None])
                meta['sales'].append(len(rows)-1)
        rows.append(['']*13)
    return rows, meta

# ---------------- Tab 2: 영업사원 KPI 목표·실적 ----------------
HDR2 = ['선적지', '영업사원', 'KPI', '목표율', '1Q 실적', '2Q 진척', 'GAP(2Q)', '계정(전체)', '계정(3주전)']
KPI_LABEL = {'booking': '3주전부킹률', 'high_profit': '고수익비중', 'lifting': '실선적률'}


def build_tab2():
    rows = [HDR2[:]]
    meta = {'section': [], 'grp': []}
    rows.append(['index.json(목표 워크북) 기준 · 1Q/2Q 분기 실적 · 목표율은 분기불변(=Q3 목표)'] + ['']*8)
    meta['section'].append(len(rows)-1)
    sbg = DATA['salesman_targets']
    seen_grps = []
    for kpi, _t, _f, _fm, _m in KPIS:
        seen_grps += LAGGARD_GRPS[kpi]
    order = []
    for g in ['ID', 'SZP', 'TAO', 'NBO', 'TH', 'VN', 'MY']:
        if g in seen_grps and g not in order:
            order.append(g)
    for grp in order:
        sps = sbg.get(grp, [])
        # sort by booking gap ascending (worst first)
        sps = sorted(sps, key=lambda s: (s['booking']['gap'] if s['booking']['gap'] is not None else 0))
        rows.append([f"━ {grp} 선적지 ({len(sps)}명)"] + ['']*8)
        meta['grp'].append(len(rows)-1)
        for s in sps:
            acc = s['accounts']
            for kpi in ['booking', 'high_profit', 'lifting']:
                k = s[kpi]
                rows.append([grp if kpi == 'booking' else '', s['sp'] if kpi == 'booking' else '',
                             KPI_LABEL[kpi], pct(k['target']), pct(k['q1']), pct(k['q2']), pct(k['gap']),
                             acc.get('total') if kpi == 'booking' else '',
                             acc.get('w3') if kpi == 'booking' else ''])
    return rows, meta

# ---------------- Tab 3: 선적지 요약 ----------------
HDR3 = ['선적지', '3주전부킹률 실적', '목표', 'GAP',
        '고수익비중 실적', '목표', 'GAP',
        '실선적률(3주전)', '소석률(전체)', '소석목표', 'GAP',
        '3주전부킹(TEU)', 'BSA(TEU)']


def build_tab3():
    rows = [HDR3[:]]
    meta = {'lag': {}}
    rows.append(['저조(빨강)=대시보드 위젯에 표기된 선적지 · GAP 음수=목표 미달'] + ['']*12)
    grps = ['ID', 'SZP', 'TAO', 'NBO', 'TH', 'VN', 'MY']
    lagset = {}
    for kpi, gl in LAGGARD_GRPS.items():
        for g in gl:
            lagset.setdefault(g, set()).add(kpi)
    for grp in grps:
        c = DATA['country'][grp]; t = DATA['targets'][grp]
        gw = (c['w3bsa'] - t['booking']) if c['w3bsa'] is not None else None
        gh = (c['hishare'] - t['high_profit']) if c['hishare'] is not None else None
        gl = (c['occ'] - t['lifting']) if c['occ'] is not None else None
        rows.append([grp, pct(c['w3bsa']), pct(t['booking']), pct(gw),
                     pct(c['hishare']), pct(t['high_profit']), pct(gh),
                     pct(c['lft3w']), pct(c['occ']), pct(t['lifting']), pct(gl),
                     i(c['bkg']), i(c['bsa'])])
        meta['lag'][grp] = lagset.get(grp, set())
    return rows, meta

# ---------------- Tab 4: 기준·정의 ----------------
def build_tab4():
    L = [
        ['항목', '정의 / 비고'],
        ['데이터 기준일', DATA['data_date'] + ' 스냅샷 (booking_snapshot / BSA_raw)'],
        ['분석 기간', DATA['period'] + ' — 마지막 완료월(5월)까지. 6월은 선행부킹 왜곡으로 제외'],
        ['OBT 팀필터', '선적국가 ∉ {KR,JP} AND 도착국가 ≠ KR'],
        ['선적지(국가) 그룹', 'SZP=SHK+DCB, NBO/TAO/SHA=해당 포트, ID/VN/TH/MY=국가코드'],
        ['① 3주전부킹률', 'WOS-3 부킹(FST_TEU, 공란시 LST) ÷ BSA. 부킹 진도 지표'],
        ['② 고수익비중', 'WOS-3 고수익 부킹 ÷ WOS-3 전체 부킹. 수익성 지표'],
        ['③ 실선적률(3주전)', 'WOS-3 실선적(Normal LST) ÷ BSA. 대시보드 저조 위젯 트리거 지표'],
        ['  소석률(전체)', '전체 실선적(Normal LST) ÷ BSA. 목표(lifting)와 동일 기준 → ③ GAP은 소석률(전체) vs 목표'],
        ['실선적전환율(영업사원)', 'WOS-3 실선적 ÷ WOS-3 부킹. 부킹 대비 실제 선적 전환율(영업사원별)'],
        ['목표율 (3분기 목표)', '목표 워크북(2026 OBT Sales Target) 선적지 KPI 목표율. 분기 불변 → Q3 목표와 동일'],
        ['  VN / MY 목표', 'VN=HPH+SGN, MY=PKG/PKW+PEN 탭을 2025 3주전물량 가중평균으로 결합'],
        ['저조 구간/국가 선정', '대시보드 저조 위젯(이미지)과 동일 — 각 KPI 하위 구간/선적지'],
        ['영업사원 목표/실적', 'Tab1 영업사원 E:G는 index.json(목표워크북) 영업사원 KPI 목표/2Q진척/GAP. H:M은 2~5월 구간 raw 집계'],
        ['주의', '영업사원 BSA는 구간에 직접 배분되지 않음 → 구간내 영업사원은 절대 TEU·비중·전환율로 비교'],
    ]
    return L


def col_letter(n):
    s = ''
    while n >= 0:
        s = chr(n % 26 + 65) + s
        n = n // 26 - 1
    return s


def main():
    creds = get_creds()
    sh = build('sheets', 'v4', credentials=creds)

    t1, m1 = build_tab1()
    t2, m2 = build_tab2()
    t3, m3 = build_tab3()
    t4 = build_tab4()

    titles = ['①②③ 저조구간·영업사원', '영업사원 KPI 목표·실적', '선적지(국가) 요약', '기준·정의']
    created = sh.spreadsheets().create(body={
        'properties': {'title': '저조구간 Q3목표·영업사원 실적 분석 (2~5월)'},
        'sheets': [{'properties': {'title': t, 'sheetId': idx, 'gridProperties': {'frozenRowCount': 1}}}
                   for idx, t in enumerate(titles)],
    }).execute()
    sid = created['spreadsheetId']
    url = created['spreadsheetUrl']
    sheet_ids = {s['properties']['title']: s['properties']['sheetId'] for s in created['sheets']}

    # write values
    data = []
    for title, vals in [(titles[0], t1), (titles[1], t2), (titles[2], t3), (titles[3], t4)]:
        ncol = max(len(r) for r in vals)
        rng = f"'{title}'!A1:{col_letter(ncol-1)}{len(vals)}"
        norm = [[('' if v is None else v) for v in r] + ['']*(ncol-len(r)) for r in vals]
        data.append({'range': rng, 'values': norm})
    sh.spreadsheets().values().batchUpdate(spreadsheetId=sid, body={
        'valueInputOption': 'RAW', 'data': data}).execute()

    # ---- formatting ----
    reqs = []
    def fmt_range(sheet, r0, r1, c0, c1, cell):
        return {'repeatCell': {'range': {'sheetId': sheet, 'startRowIndex': r0, 'endRowIndex': r1,
                'startColumnIndex': c0, 'endColumnIndex': c1}, 'cell': cell, 'fields': 'userEnteredFormat'}}
    def numfmt(sheet, rows_idx, cols, ntype, pattern):
        out = []
        for r in rows_idx:
            out.append({'repeatCell': {'range': {'sheetId': sheet, 'startRowIndex': r, 'endRowIndex': r+1,
                'startColumnIndex': cols[0], 'endColumnIndex': cols[1]},
                'cell': {'userEnteredFormat': {'numberFormat': {'type': ntype, 'pattern': pattern}}},
                'fields': 'userEnteredFormat.numberFormat'}})
        return out

    BLUE = {'red': 0.12, 'green': 0.29, 'blue': 0.49}
    LBLUE = {'red': 0.85, 'green': 0.92, 'blue': 0.99}
    GREYH = {'red': 0.20, 'green': 0.25, 'blue': 0.33}
    RED = {'red': 0.99, 'green': 0.90, 'blue': 0.90}
    YEL = {'red': 1.0, 'green': 0.97, 'blue': 0.85}

    def header_row(sheet, ncol):
        return fmt_range(sheet, 0, 1, 0, ncol, {'userEnteredFormat': {
            'backgroundColor': GREYH, 'textFormat': {'foregroundColor': {'red': 1, 'green': 1, 'blue': 1}, 'bold': True, 'fontSize': 10},
            'horizontalAlignment': 'CENTER', 'verticalAlignment': 'MIDDLE', 'wrapStrategy': 'WRAP'}})

    # Tab1
    s1 = sheet_ids[titles[0]]; n1 = len(HDR1)
    reqs.append(header_row(s1, n1))
    reqs.append(fmt_range(s1, m1['note'][0], m1['note'][0]+1, 0, n1, {'userEnteredFormat': {
        'backgroundColor': YEL, 'textFormat': {'italic': True, 'fontSize': 9}}}))
    reqs.append({'mergeCells': {'range': {'sheetId': s1, 'startRowIndex': m1['note'][0], 'endRowIndex': m1['note'][0]+1, 'startColumnIndex': 0, 'endColumnIndex': n1}, 'mergeType': 'MERGE_ALL'}})
    for r in m1['section']:
        reqs.append(fmt_range(s1, r, r+1, 0, n1, {'userEnteredFormat': {
            'backgroundColor': BLUE, 'textFormat': {'foregroundColor': {'red': 1, 'green': 1, 'blue': 1}, 'bold': True, 'fontSize': 11}}}))
        reqs.append({'mergeCells': {'range': {'sheetId': s1, 'startRowIndex': r, 'endRowIndex': r+1, 'startColumnIndex': 0, 'endColumnIndex': n1}, 'mergeType': 'MERGE_ALL'}})
    for r in m1['route']:
        reqs.append(fmt_range(s1, r, r+1, 0, n1, {'userEnteredFormat': {
            'backgroundColor': LBLUE, 'textFormat': {'bold': True}}}))
    # number formats tab1: percent cols 4,5,6,8,9,10,12 ; TEU cols 7,11
    pcols = [(4, 7), (8, 11), (12, 13)]
    allrows = m1['route'] + m1['sales']
    for r in allrows:
        for c0, c1 in pcols:
            reqs.append({'repeatCell': {'range': {'sheetId': s1, 'startRowIndex': r, 'endRowIndex': r+1, 'startColumnIndex': c0, 'endColumnIndex': c1},
                'cell': {'userEnteredFormat': {'numberFormat': {'type': 'PERCENT', 'pattern': '0%'}}}, 'fields': 'userEnteredFormat.numberFormat'}})
        reqs.append({'repeatCell': {'range': {'sheetId': s1, 'startRowIndex': r, 'endRowIndex': r+1, 'startColumnIndex': 7, 'endColumnIndex': 8},
            'cell': {'userEnteredFormat': {'numberFormat': {'type': 'NUMBER', 'pattern': '#,##0'}}}, 'fields': 'userEnteredFormat.numberFormat'}})
        reqs.append({'repeatCell': {'range': {'sheetId': s1, 'startRowIndex': r, 'endRowIndex': r+1, 'startColumnIndex': 11, 'endColumnIndex': 12},
            'cell': {'userEnteredFormat': {'numberFormat': {'type': 'NUMBER', 'pattern': '#,##0'}}}, 'fields': 'userEnteredFormat.numberFormat'}})
    # widths tab1
    for c, w in [(0, 90), (1, 130), (2, 60), (3, 50)]:
        reqs.append({'updateDimensionProperties': {'range': {'sheetId': s1, 'dimension': 'COLUMNS', 'startIndex': c, 'endIndex': c+1}, 'properties': {'pixelSize': w}, 'fields': 'pixelSize'}})

    # Tab2
    s2 = sheet_ids[titles[1]]; n2 = len(HDR2)
    reqs.append(header_row(s2, n2))
    reqs.append(fmt_range(s2, m2['section'][0], m2['section'][0]+1, 0, n2, {'userEnteredFormat': {'backgroundColor': YEL, 'textFormat': {'italic': True, 'fontSize': 9}}}))
    reqs.append({'mergeCells': {'range': {'sheetId': s2, 'startRowIndex': m2['section'][0], 'endRowIndex': m2['section'][0]+1, 'startColumnIndex': 0, 'endColumnIndex': n2}, 'mergeType': 'MERGE_ALL'}})
    for r in m2['grp']:
        reqs.append(fmt_range(s2, r, r+1, 0, n2, {'userEnteredFormat': {'backgroundColor': BLUE, 'textFormat': {'foregroundColor': {'red': 1, 'green': 1, 'blue': 1}, 'bold': True}}}))
        reqs.append({'mergeCells': {'range': {'sheetId': s2, 'startRowIndex': r, 'endRowIndex': r+1, 'startColumnIndex': 0, 'endColumnIndex': n2}, 'mergeType': 'MERGE_ALL'}})
    # percent cols 3,4,5,6 for all data rows
    grpset = set(m2['grp']); noteset = set(m2['section'])
    for r in range(2, len(t2)):
        if r in grpset or r in noteset:
            continue
        reqs.append({'repeatCell': {'range': {'sheetId': s2, 'startRowIndex': r, 'endRowIndex': r+1, 'startColumnIndex': 3, 'endColumnIndex': 7},
            'cell': {'userEnteredFormat': {'numberFormat': {'type': 'PERCENT', 'pattern': '0%'}}}, 'fields': 'userEnteredFormat.numberFormat'}})
    for c, w in [(0, 60), (1, 110), (2, 95)]:
        reqs.append({'updateDimensionProperties': {'range': {'sheetId': s2, 'dimension': 'COLUMNS', 'startIndex': c, 'endIndex': c+1}, 'properties': {'pixelSize': w}, 'fields': 'pixelSize'}})

    # Tab3
    s3 = sheet_ids[titles[2]]; n3 = len(HDR3)
    reqs.append(header_row(s3, n3))
    reqs.append(fmt_range(s3, 1, 2, 0, n3, {'userEnteredFormat': {'backgroundColor': YEL, 'textFormat': {'italic': True, 'fontSize': 9}}}))
    reqs.append({'mergeCells': {'range': {'sheetId': s3, 'startRowIndex': 1, 'endRowIndex': 2, 'startColumnIndex': 0, 'endColumnIndex': n3}, 'mergeType': 'MERGE_ALL'}})
    pcols3 = [(1, 11)]
    for r in range(2, len(t3)):
        reqs.append({'repeatCell': {'range': {'sheetId': s3, 'startRowIndex': r, 'endRowIndex': r+1, 'startColumnIndex': 1, 'endColumnIndex': 11},
            'cell': {'userEnteredFormat': {'numberFormat': {'type': 'PERCENT', 'pattern': '0%'}}}, 'fields': 'userEnteredFormat.numberFormat'}})
        reqs.append({'repeatCell': {'range': {'sheetId': s3, 'startRowIndex': r, 'endRowIndex': r+1, 'startColumnIndex': 11, 'endColumnIndex': 13},
            'cell': {'userEnteredFormat': {'numberFormat': {'type': 'NUMBER', 'pattern': '#,##0'}}}, 'fields': 'userEnteredFormat.numberFormat'}})
    # red highlight for laggard cells
    LAGCOL = {'w3': [(1, 4)], 'hi': [(4, 7)], 'lf': [(7, 11)]}
    for ri, grp in enumerate(['ID', 'SZP', 'TAO', 'NBO', 'TH', 'VN', 'MY']):
        r = ri + 2
        for kpi in m3['lag'][grp]:
            for c0, c1 in LAGCOL[kpi]:
                reqs.append({'repeatCell': {'range': {'sheetId': s3, 'startRowIndex': r, 'endRowIndex': r+1,
                    'startColumnIndex': c0, 'endColumnIndex': c1},
                    'cell': {'userEnteredFormat': {'backgroundColor': RED}},
                    'fields': 'userEnteredFormat.backgroundColor'}})
    reqs.append({'updateDimensionProperties': {'range': {'sheetId': s3, 'dimension': 'COLUMNS', 'startIndex': 0, 'endIndex': 1}, 'properties': {'pixelSize': 70}, 'fields': 'pixelSize'}})

    # Tab4
    s4 = sheet_ids[titles[3]]
    reqs.append(header_row(s4, 2))
    reqs.append({'updateDimensionProperties': {'range': {'sheetId': s4, 'dimension': 'COLUMNS', 'startIndex': 0, 'endIndex': 1}, 'properties': {'pixelSize': 170}, 'fields': 'pixelSize'}})
    reqs.append({'updateDimensionProperties': {'range': {'sheetId': s4, 'dimension': 'COLUMNS', 'startIndex': 1, 'endIndex': 2}, 'properties': {'pixelSize': 650}, 'fields': 'pixelSize'}})
    reqs.append(fmt_range(s4, 1, len(t4), 1, 2, {'userEnteredFormat': {'wrapStrategy': 'WRAP'}}))

    sh.spreadsheets().batchUpdate(spreadsheetId=sid, body={'requests': reqs}).execute()

    print('SHEET_URL:', url)
    print('SHEET_ID:', sid)


if __name__ == '__main__':
    main()
