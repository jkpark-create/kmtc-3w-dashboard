# -*- coding: utf-8 -*-
"""Part2: '개선대상 화주' 탭 — 지정 영업사원의 화주별 개선대상.
섹션: ① VN/TH·HTCONG(3주전부킹률 %)  ② SHA/NSA·JACKJIANG(고수익 TEU)  ③ NBO/TH·ALEXHAN(실선적률 %)
컬럼: 구간 | 화주(업체) | commodity | 영업사원 | 목표 | 실적 | GAP | 3분기목표
- ②는 비중%가 아니라 고수익 TEU 목표: 목표=비중목표×전체부킹, 실적=현재 고수익TEU,
  3분기목표=Q3비중(현재~목표 갭절반)×전체부킹 (물량↑→비중↑ 흐름).
- ①③은 %(부킹률/실선적률), 목표=영업사원 슬라이스 목표.
- commodity = tableau_raw_*.csv 의 (화주코드,영업사원) main ITEM(물량 최대).
- 화주 필터: GAP<0(개선대상), 임팩트순 상위 7.

Usage: runpy.cmd scripts/improve_targets_tab.py [--apply]
"""
import sys, json
from pathlib import Path
import pandas as pd
sys.stdout.reconfigure(encoding='utf-8')
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'scripts'))
from laggard_q3_fix_salesman import (get_service, SPREADSHEET_ID, target_sliced, resolve_tab,
                                     sliced_2025)
from nsa_hp_compute import _hp_target, GRP_TAB

SNAP = ROOT / 'output/booking_snapshot_result_20260612.csv'
BSA = ROOT / 'output/BSA_raw_monthly3W_20260612.csv'
CACHE25 = ROOT / 'output/_cache_2025.parquet'
BASE = json.load(open(ROOT / 'dist/sales-target/base2025.json', encoding='utf-8'))
BASE = BASE.get('base', BASE)
PERIOD = ['202602', '202603', '202604', '202605']
NEW_TAB = '개선대상 화주'
CST, ENM = 'BKG_SHPR_CST_NO', 'BKG_SHPR_CST_ENM'
CN = {'SHA': ['SHA'], 'NBO': ['NBO'], 'TAO': ['TAO'], 'SZP': ['SHK', 'DCB']}
# (kind, grp, dly, salesman, title, unit)
SECTIONS = [
    ('w3', 'VN', 'TH', 'HTCONG', '① 3주전부킹 저조 — VN/TH · HTCONG 개선대상 화주 (단위 %)', 'pct'),
    ('hi', 'SHA', 'NSA', 'JACKJIANG', '② 고수익 물량(TEU) — SHA/NSA · JACKJIANG 개선대상 화주 (단위 TEU)', 'teu'),
    ('lf', 'NBO', 'TH', 'ALEXHAN', '③ 실선적률 저조 — NBO/TH · ALEXHAN 개선대상 화주 (단위 %)', 'pct'),
]
Q3T = 8.0
TOPN = 7


def grp_of(plc, ctr):
    if plc in ('SHK', 'DCB'):
        return 'SZP'
    if plc in ('NBO', 'TAO', 'SHA'):
        return plc
    if ctr in ('ID', 'VN', 'TH', 'MY'):
        return ctr
    return None


# ---- commodity map: (shipper_code, salesman) -> main ITEM ----
def load_commodity():
    out = {}
    by_code = {}
    frames = []
    for fn in ('tableau_raw_202603.csv', 'tableau_raw_202605.csv'):
        p = ROOT / 'output' / fn
        if p.exists():
            frames.append(pd.read_csv(p))
    if not frames:
        return out, by_code
    t = pd.concat(frames, ignore_index=True)
    t = t[t['측정값 이름'] == '물량'].copy()
    t['v'] = pd.to_numeric(t['측정값'].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
    g = t.groupby(['Booking_Shipper_code', 'Salesman_POR', 'ITEM'])['v'].sum().reset_index()
    for (code, sp), sub in g.groupby(['Booking_Shipper_code', 'Salesman_POR']):
        out[(code, sp)] = sub.loc[sub['v'].idxmax(), 'ITEM']
    for code, sub in g.groupby('Booking_Shipper_code'):
        by_code[code] = sub.groupby('ITEM')['v'].sum().idxmax()
    return out, by_code


COMM, COMM_BY_CODE = load_commodity()

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


def route_bsa_total(grp, dly):
    return sum(v for (g, d, m), v in ROUTEBSA.items() if g == grp and d == dly)


# ---- 2025 cache (화주 단위 2025 기준 rate; 고수익 불필요한 ①③용) ----
_c25 = pd.read_parquet(CACHE25)
_c25 = _c25[_c25['team'] == 'OBT'].copy()
for c in ('fst', 'lst'):
    _c25[c] = pd.to_numeric(_c25[c], errors='coerce').fillna(0.0)
_c25['grp_ctr'] = [grp_of(p, c) for p, c in zip(_c25['POR_PLC_CD'], _c25['POR_CTR_CD'])]


def base2025_rate_by_shipper(kind, grp, dly, sp):
    """화주코드 -> 2025 기준값. ③: lifting rate(=WOS3 NormalLST/WOS3부킹). ①: WOS3 부킹TEU(분자, 배분BSA로 나눔)."""
    c = _c25[(_c25['grp_ctr'] == grp) & (_c25['DLY_CTR_CD'] == dly) & (_c25['Salesman_POR'] == sp)]
    c = c[c['is_w3'] == True]
    out = {}
    for sk, g in c.groupby('BKG_SHPR_CST_NO'):
        fst = g['fst'].sum()
        if fst <= 0:
            continue
        if kind == 'lf':
            out[sk] = g[g['is_normal'] == True]['lst'].sum() / fst
        else:  # w3 booking: store 2025 WOS3 부킹 TEU
            out[sk] = fst
    return out


def salesman_filtbase(kind, grp, sp, dly):
    """영업사원 2025 기준 rate(base2025). booking=w3f/bsa, lifting=w3l/w3f."""
    tab = resolve_tab(grp, sp)
    if not tab:
        return None
    m, bsa = sliced_2025(tab, sp, dly)
    if kind == 'w3':
        return (m['w3f'] / bsa) if bsa else None
    return (m['w3l'] / m['w3f']) if m['w3f'] else None


GRP_TABS_CTR = {'SZP': ['CN_SHK_DCB'], 'NBO': ['CN_NBO'], 'TAO': ['CN_TAO'], 'SHA': ['CN_SHA'],
                'TH': ['TH'], 'ID': ['ID-IDO', 'JKT', 'SUB'], 'VN': ['VN_HPH', 'VN_SGN_CMP'],
                'MY': ['PKG+PKW', 'PEN']}


def seg_shpr_total_nlst(grp, dly):
    tot = 0.0
    for tab in GRP_TABS_CTR.get(grp, []):
        for sp, s in BASE.get(tab, {}).items():
            for c in s.get('shpr', []):
                if c.get('dlyc') == dly:
                    tot += c.get('nlst', 0) or 0
    return tot


def shpr_nlst(tab, sp, dly, sk):
    s = BASE.get(tab, {}).get(sp, {})
    return sum(c.get('nlst', 0) or 0 for c in s.get('shpr', [])
               if c.get('sk') == sk and c.get('dlyc') == dly)


def slope(rates):
    pts = sorted(rates.items())
    if len(pts) < 3:
        return 0.0
    xs = [p[0] for p in pts]; ys = [p[1] for p in pts]
    n = len(xs); sx = sum(xs); sy = sum(ys)
    sxx = sum(x * x for x in xs); sxy = sum(x * y for x, y in zip(xs, ys))
    d = n * sxx - sx * sx
    return ((n * sxy - sx * sy) / d) if d else 0.0


def q3_rate(act, tgt, rates, cap):
    """미달(act<tgt) 화주: 갭 절반 축소(현재~목표), 추세 상향 시 더."""
    f = max(0.0, min(cap, act + max(-0.20, min(0.20, slope(rates) * (Q3T - 3.5)))))
    q = max((act + tgt) / 2, (f + tgt) / 2)
    return max(0.0, min(cap, min(tgt, max(act, q))))


def commodity(cst, sp):
    return COMM.get((cst, sp)) or COMM_BY_CODE.get(cst) or '-'


def collect(kind, grp, dly, sp, unit):
    if kind == 'hi':
        seg = df[(df['grp_cn'] == grp) & (df['DLY_PLC_CD'] == dly) & (df['Salesman_POR'] == sp)]
        tab = GRP_TAB.get(grp)
        ratio_tgt = _hp_target(tab, sp)
    else:
        seg = df[(df['grp_ctr'] == grp) & (df['DLY_CTR_CD'] == dly) & (df['Salesman_POR'] == sp)]
        tab = resolve_tab(grp, sp)
        ratio_tgt, _ = target_sliced(grp, sp, dly, 'booking' if kind == 'w3' else 'lifting')
    rbsa_tot = route_bsa_total(grp, dly) if kind == 'w3' else None
    seg_nlst = seg_shpr_total_nlst(grp, dly) if kind == 'w3' else None
    # 화주별 목표 차등화: company 2025 base + 영업사원 델타 (①③)
    b25 = base2025_rate_by_shipper(kind, grp, dly, sp) if kind != 'hi' else {}
    sfb = salesman_filtbase(kind, grp, sp, dly) if kind != 'hi' else None
    delta = (ratio_tgt - sfb) if (ratio_tgt is not None and sfb is not None) else None
    cap = 1.5
    rows = []
    for sk, g in seg.groupby(CST):
        w3 = g[g['is_w3']]
        fst = w3['fst'].sum()
        hi = w3.loc[w3['is_hi'], 'fst'].sum()
        if fst <= 0 or ratio_tgt is None:
            continue
        name = g[ENM].mode().iat[0] if len(g) else sk
        comm = commodity(sk, sp)
        rates = {}
        for m in (2, 3, 4, 5):
            gm = w3[w3['mon'] == m]
            fm = gm['fst'].sum()
            if kind == 'hi':
                num, den = gm.loc[gm['is_hi'], 'fst'].sum(), fm
            elif kind == 'lf':
                num, den = gm.loc[gm['is_norm'], 'LST_TEU'].sum(), fm
            else:
                nl = shpr_nlst(tab, sp, dly, sk) if tab else 0.0
                den = ROUTEBSA.get((grp, dly, m), 0.0) * (nl / seg_nlst if seg_nlst else 0)
                num = fm
            if den and den > 0:
                rates[m] = num / den
        if kind == 'hi':                     # ── 고수익 TEU: 물량 성장 목표 ──
            # 0고수익 화주 → 비중목표 물량으로 전환 / 기존 고수익 화주 → +30% 물량 성장
            tgt_teu = max(ratio_tgt * fst, hi * 1.3)
            act_teu = hi
            gap = act_teu - tgt_teu
            if gap >= 0:
                continue
            q3 = (act_teu + tgt_teu) / 2        # 절반만큼 물량 증대
            rows.append({'name': name, 'comm': comm, 'sp': sp, 'tot': fst,
                         'tgt': tgt_teu, 'act': act_teu, 'gap': gap, 'q3': q3,
                         'imp': tgt_teu - act_teu, 'haspos': act_teu > 0})
        else:                                 # ── % (부킹률/실선적률) : 화주별 목표 ──
            if kind == 'lf':
                act = w3.loc[w3['is_norm'], 'LST_TEU'].sum() / fst
                base = b25.get(sk)
            else:
                nl = shpr_nlst(tab, sp, dly, sk) if tab else 0.0
                share = (nl / seg_nlst) if (seg_nlst and nl > 0) else None
                if share is None:
                    continue
                hbsa = rbsa_tot * share
                act = fst / hbsa if hbsa > 0 else None
                base = (b25.get(sk) / hbsa) if (sk in b25 and hbsa > 0) else None
            if act is None:
                continue
            ctgt = ratio_tgt
            if base is not None and sfb is not None:        # 화주 목표 = 영업사원목표 + 화주 2025편차(±30%p 제한)
                dev = max(-0.30, min(0.30, base - sfb))
                ctgt = max(0.0, min(1.0, ratio_tgt + dev))
            if act >= ctgt:
                continue
            gap = act - ctgt
            q3 = q3_rate(act, ctgt, rates, cap)
            rows.append({'name': name, 'comm': comm, 'sp': sp, 'tot': fst,
                         'tgt': ctgt, 'act': act, 'gap': gap, 'q3': q3, 'imp': fst * (-gap)})
    if kind == 'hi':       # 고수익 실적(TEU) 큰 업체 대상, 2~5월 고수익 실적 0 업체 제외
        return sorted([r for r in rows if r['act'] > 0], key=lambda r: -r['act'])[:TOPN]
    rows.sort(key=lambda r: -r['imp'])
    return rows[:TOPN]


def main():
    apply = '--apply' in sys.argv
    sections = []
    for kind, grp, dly, sp, title, unit in SECTIONS:
        rows = collect(kind, grp, dly, sp, unit)
        sections.append((title, unit, grp, dly, sp, rows))
        print(f"\n=== {title} ===")
        for r in rows:
            if unit == 'teu':
                print(f"  {grp}/{dly} {r['name'][:30]:32}{r['comm'][:16]:18}{r['sp']:10}"
                      f" 목표{r['tgt']:5.0f} 실적{r['act']:5.0f} GAP{r['gap']:+6.0f} 3Q{r['q3']:5.0f}")
            else:
                print(f"  {grp}/{dly} {r['name'][:30]:32}{r['comm'][:16]:18}{r['sp']:10}"
                      f" 목표{r['tgt']*100:4.0f}% 실적{r['act']*100:4.0f}% GAP{r['gap']*100:+5.0f}% 3Q{r['q3']*100:4.0f}%")
    if not apply:
        print('\n(preview only; --apply to write)')
        return
    write_tab(sections)


def write_tab(sections):
    svc = get_service()
    meta = svc.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
    sid = None
    for sh in meta['sheets']:
        if sh['properties']['title'] == NEW_TAB:
            sid = sh['properties']['sheetId']
    if sid is None:
        r = svc.spreadsheets().batchUpdate(spreadsheetId=SPREADSHEET_ID, body={'requests': [
            {'addSheet': {'properties': {'title': NEW_TAB, 'gridProperties': {'rowCount': 60, 'columnCount': 9}}}}]}).execute()
        sid = r['replies'][0]['addSheet']['properties']['sheetId']
    HDR = {'red': 0.12, 'green': 0.29, 'blue': 0.49}
    SEC = {'red': 0.85, 'green': 0.92, 'blue': 0.99}
    WHITE = {'red': 1, 'green': 1, 'blue': 1}
    PCT = {'type': 'PERCENT', 'pattern': '0%'}
    GPCT = {'type': 'PERCENT', 'pattern': '+0%;-0%'}
    INT = {'type': 'NUMBER', 'pattern': '#,##0'}
    GINT = {'type': 'NUMBER', 'pattern': '+#,##0;-#,##0'}
    HEADERS = ['구간', '화주(업체)', 'Commodity', '영업사원', '목표', '실적', 'GAP', '3분기목표']

    def cell(v, nf=None, bg=None, bold=False, white=False):
        cd = {}
        if v is not None and v != '':
            cd['userEnteredValue'] = {'numberValue': v} if isinstance(v, (int, float)) else {'stringValue': str(v)}
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
        cd['userEnteredFormat'] = fmt
        return cd

    grid = [{'values': [cell('개선대상 화주 — 2~5월 목표/실적/GAP + 3분기목표 (지정 영업사원, AI 월추세예측)', bold=True)]},
            {'values': [cell('')]}]
    for title, unit, grp, dly, sp, rows in sections:
        grid.append({'values': [cell(title, bg=HDR, bold=True, white=True)] + [cell('', bg=HDR) for _ in range(7)]})
        grid.append({'values': [cell(h, bg=SEC, bold=True) for h in HEADERS]})
        tnf, gnf = (INT, GINT) if unit == 'teu' else (PCT, GPCT)
        for r in rows:
            tv = round(r['tgt']) if unit == 'teu' else round(r['tgt'], 4)
            av = round(r['act']) if unit == 'teu' else round(r['act'], 4)
            gv = round(r['gap']) if unit == 'teu' else round(r['gap'], 4)
            qv = round(r['q3']) if unit == 'teu' else round(r['q3'], 4)
            grid.append({'values': [cell(f'{grp}/{dly}'), cell(r['name']), cell(r['comm']), cell(r['sp']),
                                    cell(tv, nf=tnf), cell(av, nf=tnf), cell(gv, nf=gnf), cell(qv, nf=tnf)]})
        grid.append({'values': [cell('')]})

    req = [{'updateCells': {   # 옛 버전 잔여(I~L열, 하단행) 전체 클리어
        'range': {'sheetId': sid, 'startRowIndex': 0, 'endRowIndex': 60,
                  'startColumnIndex': 0, 'endColumnIndex': 12},
        'fields': 'userEnteredValue,userEnteredFormat'}},
        {'updateCells': {'rows': grid, 'fields': 'userEnteredValue,userEnteredFormat',
                         'start': {'sheetId': sid, 'rowIndex': 0, 'columnIndex': 0}}}]
    for i, w in enumerate([72, 240, 150, 95, 60, 60, 60, 75]):
        req.append({'updateDimensionProperties': {
            'range': {'sheetId': sid, 'dimension': 'COLUMNS', 'startIndex': i, 'endIndex': i + 1},
            'properties': {'pixelSize': w}, 'fields': 'pixelSize'}})
    svc.spreadsheets().batchUpdate(spreadsheetId=SPREADSHEET_ID, body={'requests': req}).execute()
    print(f"APPLIED: '{NEW_TAB}' 탭 재작성.")


if __name__ == '__main__':
    main()
