"""
-3W Booking Dashboard
Global filters: 팀 → 선적지 국가 → 선적지 포트 → 도착지 국가 → 도착지 포트
Team: OBT / EST / IST / JBT
Table: multi-level headers (Image 8 format)
"""
import os, re, sys, time
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime

import dash
from dash import dcc, html, dash_table, Input, Output, State
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
import plotly.express as px

WORK_DIR = Path(r'C:\Users\JKPARK\OneDrive\Documents\Claude\-3W bkg dashboard')
OUTPUT_DIR = WORK_DIR / 'output'
DEST_GROUP_MAP = {}  # No grouping — IN and PK shown separately
MAIN_DESTS = ['IN', 'PK', 'MY', 'SG', 'TH', 'ID', 'VN']

WOS_ORDER_ACTUAL = ['WOS-3', 'WOS-2', 'WOS-1', 'Week of Sailing (WOS)']
WOS_LBL_ACTUAL = {'WOS-3': 'WOS-3', 'WOS-2': 'WOS-2', 'WOS-1': 'WOS-1', 'Week of Sailing (WOS)': 'WOS'}
# Booking-schedule based WOS (used in Tab2 progression table)
# WOS progression: 3W+4W combined as WOS-3
WOS_STEPS = ['WOS-3', 'WOS-2', 'WOS-1']
WOS_STEP_MAP = {'4W': 'WOS-3', '3W': 'WOS-3', '2W': 'WOS-2', '1W': 'WOS-1'}
ML = {f'2026{m:02d}': f'{m}월' for m in range(1, 13)}
ML.update({f'2025{m:02d}': f'{m}월(25)' for m in range(1, 13)})
C = {'pri': '#1a73e8', 'ok': '#34a853', 'ng': '#ea4335', 'warn': '#f9ab00',
     'bg': '#f0f2f5', 'txt': '#202124', 'mt': '#5f6368', 'bdr': '#dadce0'}
PAL = px.colors.qualitative.Set2


# ═══════════════════════════════════════════════════════════
# Team classification
# ═══════════════════════════════════════════════════════════
def classify_team(origin, dly_raw):
    """OBT/EST/IST/JBT based on raw origin & destination country codes."""
    if origin not in ('KR', 'JP') and dly_raw != 'KR':
        return 'OBT'
    elif origin == 'KR' and dly_raw != 'JP':
        return 'EST'
    elif origin != 'JP' and dly_raw == 'KR':
        return 'IST'
    else:
        return 'JBT'


# ═══════════════════════════════════════════════════════════
# Data Loading (Google Drive → local cache)
# ═══════════════════════════════════════════════════════════
GDRIVE_FOLDER_ID = '1JIxg6Y-_gRfI1HueXZ1Q9j4-Z5bxvNgv'
GDRIVE_CLIENT_ID = os.environ.get('GDRIVE_CLIENT_ID', '')
GDRIVE_CLIENT_SECRET = os.environ.get('GDRIVE_CLIENT_SECRET', '')
GDRIVE_REFRESH_TOKEN = os.environ.get('GDRIVE_REFRESH_TOKEN', '')

def _gdrive_download():
    """Download latest data files from Google Drive if not available locally."""
    import requests as _req
    if not GDRIVE_REFRESH_TOKEN:
        # Try reading from local token file
        token_path = Path(r'C:\Users\JKPARK\OneDrive\Documents\Claude\.gdrive-mcp\token.json')
        if token_path.exists():
            import json as _j
            rt = _j.load(open(token_path))['refresh_token']
        else:
            return  # No credentials, skip download
    else:
        rt = GDRIVE_REFRESH_TOKEN

    try:
        resp = _req.post('https://oauth2.googleapis.com/token', data={
            'client_id': GDRIVE_CLIENT_ID, 'client_secret': GDRIVE_CLIENT_SECRET,
            'refresh_token': rt, 'grant_type': 'refresh_token'}, timeout=10)
        at = resp.json().get('access_token')
        if not at:
            print("  GDrive token refresh failed, using local files", flush=True)
            return
        headers = {'Authorization': f'Bearer {at}'}

        # List files in folder
        r = _req.get('https://www.googleapis.com/drive/v3/files', headers=headers,
            params={'q': f"'{GDRIVE_FOLDER_ID}' in parents and trashed=false",
                    'fields': 'files(id,name,modifiedTime)', 'orderBy': 'modifiedTime desc'}, timeout=10)
        files = r.json().get('files', [])

        OUTPUT_DIR.mkdir(exist_ok=True)
        for f in files:
            local = OUTPUT_DIR / f['name']
            if local.exists():
                continue  # Already have it
            print(f"  Downloading {f['name']}...", flush=True)
            r = _req.get(f"https://www.googleapis.com/drive/v3/files/{f['id']}?alt=media",
                headers=headers, timeout=300)
            local.write_bytes(r.content)
            print(f"    {len(r.content):,} bytes", flush=True)
    except Exception as e:
        print(f"  GDrive download error: {e}", flush=True)

def _build_445_map():
    """Build 445 calendar: week_start(YYYY-MM-DD) → YYYYMM"""
    from datetime import timedelta
    pattern = [4,4,5,4,4,5,4,4,5,4,4,5]
    mapping = {}
    for year, first_sun in [(2025, datetime(2025,1,5)), (2026, datetime(2026,1,4)), (2027, datetime(2027,1,3))]:
        wk = 0
        for mi, cnt in enumerate(pattern):
            ym = f'{year}{mi+1:02d}'
            for _ in range(cnt):
                mapping[(first_sun + timedelta(weeks=wk)).strftime('%Y-%m-%d')] = ym
                wk += 1
    return mapping

def find_latest(pattern):
    f = sorted(OUTPUT_DIR.glob(pattern), key=os.path.getmtime, reverse=True)
    return f[0] if f else None

def parse_kd(s):
    if pd.isna(s) or str(s).strip() in ('', 'nan'):
        return pd.NaT
    m = re.match(r'(\d{4})\D+(\d{1,2})\D+(\d{1,2})', str(s))
    return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3))) if m else pd.NaT

def load_data():
    # Try downloading from Google Drive if no local data
    if not find_latest('_cache_*.parquet') and not find_latest('booking_snapshot_result_*.xlsx'):
        print("No local data, downloading from Google Drive...", flush=True)
        _gdrive_download()

    bf = find_latest('_cache_*.parquet') or find_latest('booking_snapshot_result_*.xlsx')
    sf = find_latest('BSA_raw_monthly3W_*.csv')
    if not bf:
        raise FileNotFoundError("No data available. Run daily_3w_dashboard.py first or check Google Drive.")

    dd = bf.stem.split('_')[-1]

    if bf.suffix == '.parquet':
        print(f"Loading {bf.name}...", flush=True)
        bkg = pd.read_parquet(bf)
        # Ensure derived columns exist
        if 'fst' not in bkg.columns:
            for c in ['FST_TEU', 'LST_TEU', 'CM1']:
                bkg[c] = bkg[c].astype(str).str.replace(',', '')
            bkg['fst'] = pd.to_numeric(bkg['FST_TEU'], errors='coerce').fillna(0)
            bkg['lst'] = pd.to_numeric(bkg['LST_TEU'], errors='coerce').fillna(0)
            bkg['cm1v'] = pd.to_numeric(bkg['CM1'], errors='coerce').fillna(0)
        if 'profit_type' not in bkg.columns and '\uace0/\uc800' in bkg.columns:
            bkg = bkg.rename(columns={'\uace0/\uc800': 'profit_type'})
        if 'dest' not in bkg.columns:
            bkg['dest'] = bkg['DLY_CTR_CD'].map(DEST_GROUP_MAP).fillna(bkg['DLY_CTR_CD'])
            bkg['origin'] = bkg['POR_CTR_CD']
            bkg['ori_port'] = bkg['POR_PLC_CD']
            bkg['dst_port'] = bkg['DLY_PLC_CD']
            bkg['team'] = [classify_team(o, d) for o, d in zip(bkg['POR_CTR_CD'], bkg['DLY_CTR_CD'])]
        if 'week_dt' not in bkg.columns:
            bkg['week_dt'] = bkg['week_start_date'].apply(parse_kd)
    else:
        cache = OUTPUT_DIR / f'_cache_{dd}.parquet'
        if cache.exists() and cache.stat().st_mtime >= bf.stat().st_mtime:
            print(f"Loading cache...", flush=True)
            bkg = pd.read_parquet(cache)
        else:
            t0 = time.time()
            print(f"Loading {bf.name}...", flush=True)
            bkg = pd.read_excel(bf, sheet_name='raw', dtype=str)
            bkg = bkg.rename(columns={'\uace0/\uc800': 'profit_type'})
            for c in ['FST_TEU', 'LST_TEU', 'CM1']:
                bkg[c] = bkg[c].astype(str).str.replace(',', '')
            bkg['fst'] = pd.to_numeric(bkg['FST_TEU'], errors='coerce').fillna(0)
            bkg['lst'] = pd.to_numeric(bkg['LST_TEU'], errors='coerce').fillna(0)
            bkg['cm1v'] = pd.to_numeric(bkg['CM1'], errors='coerce').fillna(0)
            bkg['dest'] = bkg['DLY_CTR_CD'].map(DEST_GROUP_MAP).fillna(bkg['DLY_CTR_CD'])
            bkg['origin'] = bkg['POR_CTR_CD']
            bkg['ori_port'] = bkg['POR_PLC_CD']
            bkg['dst_port'] = bkg['DLY_PLC_CD']
            bkg['team'] = [classify_team(o, d) for o, d in zip(bkg['POR_CTR_CD'], bkg['DLY_CTR_CD'])]
            bkg['week_dt'] = bkg['week_start_date'].apply(parse_kd)
            bkg.to_parquet(cache, index=False)
            print(f"  cached in {time.time()-t0:.0f}s", flush=True)

    # YYYYMM = 445 calendar (BSA와 동일)
    bkg['week_dt'] = bkg['week_start_date'].apply(parse_kd)
    _445 = _build_445_map()
    bkg['YYYYMM'] = bkg['week_dt'].apply(lambda d: _445.get(d.strftime('%Y-%m-%d'), '') if pd.notna(d) else '')

    bsa = None
    if sf:
        bsa = pd.read_csv(sf, dtype=str)
        # Exclude '전체' subtotal rows (handles UTF-8 and mojibake)
        bsa = bsa[bsa['DLY_Country'].str.len() <= 3]
        bsa = bsa[bsa['POR_Country'].str.len() <= 3]
        bsa = bsa[bsa['POR_Country'].str.len() <= 3]
        bsa['teu_bsa'] = pd.to_numeric(bsa['TEU_BSA (Actual)'].str.replace(',', ''), errors='coerce').fillna(0)
        bsa['dest'] = bsa['DLY_Country'].map(DEST_GROUP_MAP).fillna(bsa['DLY_Country'])
        bsa['origin'] = bsa['POR_Country']
        # 'team' column exists if downloaded per-team
        if 'team' not in bsa.columns:
            bsa['team'] = 'ALL'

    # Weeks per month (BKG_Sche 기준)
    wpm = bkg[bkg['week_dt'].notna()].groupby('YYYYMM')['week_start_date'].nunique().to_dict()

    print(f"Loaded: {len(bkg):,} rows", flush=True)
    return bkg, bsa, dd, wpm

BKG, BSA_DF, DATA_DATE, WPM = load_data()
ALL_MONTHS = sorted(BKG['YYYYMM'].dropna().unique())
ALL_TEAMS = ['OBT', 'EST', 'IST', 'JBT']

# Week number from week_start_date (2026-04-05 = 14주차)
BKG['week_num'] = BKG['week_dt'].apply(
    lambda d: (d.timetuple().tm_yday - 1) // 7 + 1 if pd.notna(d) else None)
# Week filter uses week_start_date string as value for exact matching
_wk_info = BKG[BKG['week_dt'].notna()].drop_duplicates('week_start_date')[
    ['YYYYMM', 'week_num', 'week_dt', 'week_start_date']].sort_values('week_dt')
WEEK_OPTS = {}
for _, r in _wk_info.iterrows():
    m = r['YYYYMM']
    wn = int(r['week_num'])
    wdt = r['week_dt'].strftime('%m/%d')
    # value = week_start_date original string for exact filter matching
    WEEK_OPTS.setdefault(m, []).append({'label': f"{wn}주차 ({wdt})", 'value': r['week_start_date']})


# ═══════════════════════════════════════════════════════════
# Global filter logic
# ═══════════════════════════════════════════════════════════
def gf(df, team, ori, ori_p, dst, dst_p, month=None, week=None):
    """Apply all global + month/week filters."""
    d = df
    if team != 'ALL':
        d = d[d['team'] == team]
    if ori != 'ALL':
        d = d[d['origin'] == ori]
    if ori_p != 'ALL':
        d = d[d['ori_port'] == ori_p]
    if dst != 'ALL':
        d = d[d['dest'] == dst]
    if dst_p != 'ALL':
        d = d[d['dst_port'] == dst_p]
    if month:
        d = d[d['YYYYMM'] == month]
    if week and week != 'ALL':
        d = d[d['week_start_date'] == week]
    return d

def _week_to_ww(week_str):
    """Convert week_start_date string to WW number (445 calendar)"""
    if not week_str or week_str == 'ALL':
        return None
    dt = parse_kd(week_str)
    if pd.isna(dt):
        return None
    start = datetime(2026, 1, 4)
    diff = (dt - start).days // 7
    return str(diff + 1) if diff >= 0 else None

def gf_bsa(team, ori, dst, ori_p='ALL', dst_p='ALL', week=None):
    if BSA_DF is None:
        return pd.DataFrame(columns=['origin', 'dest', 'YYYYMM', 'teu_bsa'])
    b = BSA_DF
    if team != 'ALL' and 'team' in b.columns:
        b = b[b['team'] == team]
    if ori != 'ALL':
        b = b[b['origin'] == ori]
    if ori_p != 'ALL' and 'POR_PORT' in b.columns:
        b = b[b['POR_PORT'] == ori_p]
    if dst != 'ALL':
        b = b[b['dest'] == dst]
    if dst_p != 'ALL' and 'DLY_PORT' in b.columns:
        b = b[b['DLY_PORT'] == dst_p]
    if week and week != 'ALL' and 'WW' in b.columns:
        ww = _week_to_ww(week)
        if ww:
            b = b[b['WW'] == ww]
    return b


# ═══════════════════════════════════════════════════════════
# Aggregation
# ═══════════════════════════════════════════════════════════
def agg_m(df, keys):
    if len(df) == 0:
        cols = list(keys) + ['bkg', 'normal', 'cm1', 'cm1_lst', 'cancel', 'hi_bkg', 'hi_norm',
                              'ship%', 'cm1teu', 'hi%']
        return pd.DataFrame(columns=cols)
    a = df.groupby(keys).agg(bkg=('fst', 'sum')).reset_index()
    n = df[df['LST_Status'] == 'Normal']
    na = n.groupby(keys).agg(normal=('fst', 'sum')).reset_index()
    # CM1/TEU: Normal 중 CM1 정산 완료 건만 (cm1v=0 = 미정산)
    hcm = n[n['cm1v'] != 0]
    ca = hcm.groupby(keys).agg(cm1=('cm1v', 'sum'), cm1_lst=('lst', 'sum')).reset_index()
    na = na.merge(ca, on=keys, how='left')
    cc = df[df['LST_Status'] == 'Cancel'].groupby(keys).agg(cancel=('fst', 'sum')).reset_index()
    hi = df[df['profit_type'] == '고수익화주'].groupby(keys).agg(hi_bkg=('fst', 'sum')).reset_index()
    hin = df[(df['profit_type'] == '고수익화주') & (df['LST_Status'] == 'Normal')]
    hna = hin.groupby(keys).agg(hi_norm=('fst', 'sum')).reset_index()
    m = a
    for r in [na, cc, hi, hna]:
        m = m.merge(r, on=keys, how='left')
    for c in ['normal', 'cm1', 'cm1_lst', 'cancel', 'hi_bkg', 'hi_norm']:
        if c in m.columns:
            m[c] = m[c].fillna(0)
    m['ship%'] = np.where(m['bkg'] > 0, m['normal'] / m['bkg'] * 100, 0)
    m['cm1teu'] = np.where(m['cm1_lst'] > 0, m['cm1'] / m['cm1_lst'], 0)
    m['hi%'] = np.where(m['bkg'] > 0, m['hi_bkg'] / m['bkg'] * 100, 0)
    return m


# ═══════════════════════════════════════════════════════════
# Layout helpers
# ═══════════════════════════════════════════════════════════
S = {'border': 'none', 'boxShadow': '0 1px 3px rgba(0,0,0,0.08)'}

def kpi_c(t, v, d=None, fmt=',.0f', sx='', col=None):
    de = []
    if d is not None:
        dc = C['ok'] if d >= 0 else C['ng']
        de = [html.Span(f" ({'+' if d>=0 else ''}{d:{fmt}}{sx})", style={'fontSize': '13px', 'color': dc})]
    return dbc.Card(dbc.CardBody([
        html.P(t, className='mb-1', style={'fontSize': '12px', 'color': C['mt']}),
        html.H4([f"{v:{fmt}}{sx}"] + de, className='mb-0', style={'color': col or C['txt'], 'fontWeight': '700'}),
    ]), style=S)

def st(t, s=None):
    e = [html.H6(t, className='mb-0', style={'fontWeight': '600'})]
    if s: e.append(html.Small(s, style={'color': C['mt']}))
    return html.Div(e, className='mb-2')


def build_summary_table(df_filtered, bsa_func, months, view_col, view_label, items):
    """Build Image-8 style multi-level header table."""
    last3 = months[-3:]
    # Column definitions: [level1, level2, level3]
    cols = [{'name': ['', '', view_label], 'id': 'lbl'}]
    for m in last3:
        ml = ML.get(m, m)
        wk = WPM.get(m, '?')
        h1 = f"{ml} 실적 ({wk}주)"
        h2a = '3주전 BKG (물량 및 실선적률)'
        h2b = '3주전 BKG (고수익화주)'
        for h2, sub_ids in [(h2a, [('BKG', f'{m}_b'), ('BSA', f'{m}_bs'), ('비중', f'{m}_r'),
                                     ('실선적', f'{m}_n'), ('실선적률', f'{m}_sr')]),
                             (h2b, [('BKG', f'{m}_hb'), ('비중', f'{m}_hr'),
                                     ('실선적', f'{m}_hn'), ('실선적률', f'{m}_hsr'),
                                     ('CM1', f'{m}_c'), ('CM1/TEU', f'{m}_ct')])]:
            for name, cid in sub_ids:
                cols.append({'name': [h1, h2, name], 'id': cid})

    def _calc_row(d, bsa_val, wk):
        """Calculate one row of metrics. wk=weeks to divide by (1=monthly, >1=weekly avg)."""
        b = d['fst'].sum() / wk; bs = bsa_val / wk
        n_d = d[d['LST_Status'] == 'Normal']; n = n_d['fst'].sum() / wk
        hcm = n_d[n_d['cm1v'] != 0]; c1 = hcm['cm1v'].sum(); cl = hcm['lst'].sum()
        hi = d[d['profit_type'] == '고수익화주']; hb = hi['fst'].sum() / wk
        hn = hi[hi['LST_Status'] == 'Normal']['fst'].sum() / wk
        return b, bs, n, c1, cl, hb, hn

    def _fmt_row(prefix, b, bs, n, c1, cl, hb, hn):
        r = {}
        r[f'{prefix}_b'] = f"{b:,.0f}"; r[f'{prefix}_bs'] = f"{bs:,.0f}"
        r[f'{prefix}_r'] = f"{b/bs*100:.0f}%" if bs else '-'
        r[f'{prefix}_n'] = f"{n:,.0f}"; r[f'{prefix}_sr'] = f"{n/b*100:.0f}%" if b else '-'
        r[f'{prefix}_hb'] = f"{hb:,.0f}"; r[f'{prefix}_hr'] = f"{hb/b*100:.0f}%" if b else '-'
        r[f'{prefix}_hn'] = f"{hn:,.0f}"; r[f'{prefix}_hsr'] = f"{hn/hb*100:.0f}%" if hb else '-'
        r[f'{prefix}_c'] = f"{c1:,.0f}"; r[f'{prefix}_ct'] = f"{c1/cl:,.0f}" if cl else '-'
        return r

    # Per-item rows (주간 평균)
    rows = []
    for it in items:
        row = {'lbl': it}
        for m in last3:
            wk = WPM.get(m, 4)
            w3 = df_filtered[(df_filtered['YYYYMM'] == m) & (df_filtered['Lead_time (BKG_Sche)'] == 'WOS-3')]
            d = w3[w3[view_col] == it]
            vals = _calc_row(d, bsa_func(m, it), wk)
            row.update(_fmt_row(m, *vals))
        rows.append(row)

    # 합계(주) — weekly average
    tr_w = {'lbl': '합계(주)'}
    for m in last3:
        wk = WPM.get(m, 4)
        w3 = df_filtered[(df_filtered['YYYYMM'] == m) & (df_filtered['Lead_time (BKG_Sche)'] == 'WOS-3')]
        vals = _calc_row(w3, bsa_func(m, None), wk)
        tr_w.update(_fmt_row(m, *vals))
    rows.append(tr_w)

    # 합계(월) — monthly total
    tr_m = {'lbl': '합계(월)'}
    for m in last3:
        w3 = df_filtered[(df_filtered['YYYYMM'] == m) & (df_filtered['Lead_time (BKG_Sche)'] == 'WOS-3')]
        vals = _calc_row(w3, bsa_func(m, None), 1)  # wk=1 = no division
        tr_m.update(_fmt_row(m, *vals))
    rows.append(tr_m)

    return dash_table.DataTable(
        data=rows, columns=cols, merge_duplicate_headers=True,
        style_header={'backgroundColor': '#e8f0fe', 'color': C['txt'],
                      'fontWeight': '600', 'fontSize': '11px', 'textAlign': 'center',
                      'border': f'1px solid {C["bdr"]}'},
        style_cell={'textAlign': 'center', 'fontSize': '12px', 'padding': '4px 6px',
                    'fontFamily': 'Segoe UI, sans-serif', 'whiteSpace': 'nowrap',
                    'border': f'1px solid {C["bdr"]}', 'minWidth': '60px'},
        style_data_conditional=[
            {'if': {'row_index': len(rows) - 2}, 'fontWeight': '700', 'backgroundColor': '#f0f4ff'},
            {'if': {'row_index': len(rows) - 1}, 'fontWeight': '700', 'backgroundColor': '#e8edf5'},
            {'if': {'column_id': 'lbl'}, 'textAlign': 'left', 'fontWeight': '600',
             'backgroundColor': '#fafafa'},
        ],
        style_table={'overflowX': 'auto'},
    )


def mk_tbl(rows, fc='lbl'):
    if not rows:
        return html.P("데이터 없음", style={'color': C['mt']})
    return dash_table.DataTable(
        data=rows, columns=[{'name': c, 'id': c} for c in rows[0]],
        style_header={'backgroundColor': C['pri'], 'color': '#fff', 'fontWeight': '600', 'fontSize': '12px', 'textAlign': 'center'},
        style_cell={'textAlign': 'center', 'fontSize': '12px', 'padding': '5px 8px', 'whiteSpace': 'nowrap'},
        style_data_conditional=[
            {'if': {'row_index': len(rows) - 1}, 'fontWeight': '700', 'backgroundColor': '#f8f9fa'},
            {'if': {'column_id': fc}, 'textAlign': 'left', 'fontWeight': '600'},
        ])


def _default_month():
    """현재 주 + 3주 후의 월"""
    from datetime import timedelta
    today = datetime.now()
    sun = today - timedelta(days=today.weekday() + 1) if today.weekday() != 6 else today
    target = sun + timedelta(days=21)
    m = target.strftime('%Y%m')
    return m if m in ALL_MONTHS else ALL_MONTHS[-1]

def _default_week():
    """현재 주 + 3주 후의 주차 (week_start_date value)"""
    from datetime import timedelta
    today = datetime.now()
    sun = today - timedelta(days=today.weekday() + 1) if today.weekday() != 6 else today
    target_sun = sun + timedelta(days=21)
    m = _default_month()
    weeks = WEEK_OPTS.get(m, [])
    for w in weeks:
        ws_dt = parse_kd(w['value'])
        if pd.notna(ws_dt) and ws_dt.date() == target_sun.date():
            return w['value']
    # Fallback: first week of the default month
    return weeks[0]['value'] if weeks else 'ALL'

DEF_MONTH = _default_month()
DEF_WEEK = _default_week()

def mdd(id_, idx=None):
    opts = [{'label': ML.get(m, m), 'value': m} for m in ALL_MONTHS]
    v = DEF_MONTH
    return dcc.Dropdown(id=id_, options=opts, value=v, clearable=False, style={'width': '90px'})

def wdd(id_):
    """Week dropdown — initialized with default month's weeks and default week."""
    wk_opts = [{'label': '전체', 'value': 'ALL'}] + WEEK_OPTS.get(DEF_MONTH, [])
    return dcc.Dropdown(id=id_, options=wk_opts,
                        value=DEF_WEEK, clearable=False, style={'width': '130px'})


# ═══════════════════════════════════════════════════════════
# Tab layouts
# ═══════════════════════════════════════════════════════════
def tab1():
    return dbc.Container([
        dbc.Row([
            dbc.Col(html.H5("소석률 현황", style={'fontWeight': '700'}), width='auto'),
            dbc.Col(mdd('t1-m'), width='auto'),
            dbc.Col(wdd('t1-w'), width='auto'),
        ], className='mb-3 align-items-center g-2'),
        dbc.Row(id='t1-kpis', className='mb-3 g-2'),
        dbc.Row([
            dbc.Col(dbc.Card(dbc.CardBody([st("월별 BKG vs BSA"),
                dcc.Graph(id='t1-bar', config={'displayModeBar': False}, style={'height': '450px'})]),
                style={**S, 'height': '520px'}), md=7),
            dbc.Col(dbc.Card(dbc.CardBody([html.Div(id='t1-oh'),
                dcc.Graph(id='t1-occ', config={'displayModeBar': False}, style={'height': '450px'})]),
                style={**S, 'height': '520px'}), md=5),
        ], className='mb-3 g-2'),
        dbc.Row([dbc.Col(dbc.Card(dbc.CardBody([html.Div(id='t1-th'), html.Div(id='t1-tbl')]), style=S))], className='g-2'),
    ], fluid=True, className='py-3')

def tab2():
    return dbc.Container([
        dbc.Row([
            dbc.Col(html.H5("부킹 트렌드", style={'fontWeight': '700'}), width='auto'),
            dbc.Col(mdd('t2-m'), width='auto'),
            dbc.Col(wdd('t2-w'), width='auto'),
            dbc.Col(dcc.Dropdown(id='t2-pf', options=[{'label': '전체', 'value': 'ALL'},
                {'label': '고수익화주', 'value': 'hi'}, {'label': '저수익화주', 'value': 'lo'},
                {'label': 'A+B', 'value': 'ab'}, {'label': 'C+D', 'value': 'cd'}],
                value='ALL', clearable=False, style={'width': '120px'}), width='auto'),
        ], className='mb-3 align-items-center g-2'),
        dbc.Tabs([dbc.Tab(label='도착지별', tab_id='dest'), dbc.Tab(label='선적지별', tab_id='origin'), dbc.Tab(label='루트별', tab_id='route'),
                  dbc.Tab(label='화주별', tab_id='cust'), dbc.Tab(label='영업사원별', tab_id='sales'),
                  dbc.Tab(label='AB vs CD', tab_id='abcd')], id='t2-sub', active_tab='dest', className='mb-3'),
        dbc.Row([dbc.Col(dbc.Card(dbc.CardBody([st("3주전 BKG 현황", "BKG/BSA/소석률/실선적/실선적률"), html.Div(id='t2-wos')]), style=S))], className='mb-3 g-2'),
        dbc.Row([dbc.Col(dbc.Card(dbc.CardBody([st("주차별 3주전 BKG"),
            dcc.Graph(id='t2-wk', config={'displayModeBar': False}, style={'height': '290px'})]), style=S))], className='g-2'),
    ], fluid=True, className='py-3')

def tab3():
    return dbc.Container([
        dbc.Row([dbc.Col(html.H5("전환 퍼널", style={'fontWeight': '700'}), width='auto'),
                 dbc.Col(mdd('t3-m'), width='auto'),
                 dbc.Col(wdd('t3-w'), width='auto')], className='mb-3 align-items-center g-2'),
        dbc.Row([
            dbc.Col(dbc.Card(dbc.CardBody([st("WOS-3 → 실선적"),
                dcc.Graph(id='t3-fn', config={'displayModeBar': False}, style={'height': '330px'})]), style=S), md=5),
            dbc.Col(dbc.Card(dbc.CardBody([st("실선적률 추이"),
                dcc.Graph(id='t3-tr', config={'displayModeBar': False}, style={'height': '330px'})]), style=S), md=7),
        ], className='mb-3 g-2'),
        dbc.Row([
            dbc.Col(dbc.Card(dbc.CardBody([st("캔슬 분석"), html.Div(id='t3-ct')]), style=S), md=6),
            dbc.Col(dbc.Card(dbc.CardBody([st("WOS 누적"),
                dcc.Graph(id='t3-wf', config={'displayModeBar': False}, style={'height': '280px'})]), style=S), md=6),
        ], className='g-2'),
    ], fluid=True, className='py-3')

def tab4():
    return dbc.Container([
        dbc.Row([dbc.Col(html.H5("수익성 분석", style={'fontWeight': '700'}), width='auto'),
                 dbc.Col(mdd('t4-m'), width='auto'),
                 dbc.Col(wdd('t4-w'), width='auto')], className='mb-3 align-items-center g-2'),
        dbc.Row([
            dbc.Col(dbc.Card(dbc.CardBody([st("CM1/TEU"),
                dcc.Graph(id='t4-cm', config={'displayModeBar': False}, style={'height': '330px'})]), style=S), md=6),
            dbc.Col(dbc.Card(dbc.CardBody([st("고수익(A+B) 비중"),
                dcc.Graph(id='t4-hi', config={'displayModeBar': False}, style={'height': '330px'})]), style=S), md=6),
        ], className='mb-3 g-2'),
        dbc.Row([dbc.Col(dbc.Card(dbc.CardBody([st("소석률 vs CM1/TEU"),
            dcc.Graph(id='t4-sc', config={'displayModeBar': False}, style={'height': '360px'})]), style=S))], className='g-2'),
    ], fluid=True, className='py-3')

def tab5():
    return dbc.Container([
        dbc.Row([dbc.Col(html.H5("부킹 패턴 (WOS 단계별)", style={'fontWeight': '700'}), width='auto'),
                 dbc.Col(mdd('t5-m'), width='auto'),
                 dbc.Col(wdd('t5-w'), width='auto')], className='mb-3 align-items-center g-2'),
        dbc.Row([dbc.Col(dbc.Card(dbc.CardBody([st("지역별 부킹 접수 시점"),
            html.Div(id='t5-dest-tbl')]), style=S))], className='mb-3 g-2'),
        dbc.Row([dbc.Col(dbc.Card(dbc.CardBody([st("부킹 시점 비중"),
            dcc.Graph(id='t5-wos-bar', config={'displayModeBar': False}, style={'height': '400px'})]), style=S))], className='mb-3 g-2'),
        dbc.Row([dbc.Col(dbc.Card(dbc.CardBody([st("화주별 부킹 접수 시점"),
            html.Div(id='t5-cust-tbl')]), style=S))], className='g-2'),
    ], fluid=True, className='py-3')


# ═══════════════════════════════════════════════════════════
# App layout + Google OAuth
# ═══════════════════════════════════════════════════════════
import secrets, json as _json
from flask import Flask, session, redirect, request, make_response

GOOGLE_CLIENT_ID = os.environ.get('GOOGLE_CLIENT_ID', '')
ALLOWED_DOMAINS = os.environ.get('ALLOWED_DOMAINS', 'ekmtc.com').split(',')

server = Flask(__name__)
server.secret_key = secrets.token_hex(32)

LOGIN_PAGE = f'''<!DOCTYPE html><html><head><meta charset="utf-8">
<title>-3W Dashboard Login</title>
<script src="https://accounts.google.com/gsi/client" async defer></script>
<style>
body {{ font-family: 'Segoe UI', sans-serif; display: flex; justify-content: center;
       align-items: center; height: 100vh; margin: 0; background: #f0f2f5; }}
.login-box {{ background: #fff; padding: 40px; border-radius: 12px;
             box-shadow: 0 2px 12px rgba(0,0,0,.1); text-align: center; }}
h2 {{ color: #1a73e8; margin-bottom: 8px; }}
p {{ color: #5f6368; margin-bottom: 24px; }}
.error {{ color: #ea4335; margin-top: 12px; display: none; }}
</style></head><body>
<div class="login-box">
  <h2>-3W Booking Dashboard</h2>
  <p>회사 Google 계정으로 로그인하세요</p>
  <div id="g_id_onload" data-client_id="{GOOGLE_CLIENT_ID}"
       data-callback="onLogin" data-auto_prompt="false"></div>
  <div class="g_id_signin" data-type="standard" data-size="large"
       data-theme="outline" data-text="sign_in_with" data-shape="rectangular"></div>
  <p class="error" id="err"></p>
</div>
<script>
function onLogin(resp) {{
  fetch('/auth/google', {{
    method: 'POST', headers: {{'Content-Type': 'application/json'}},
    body: JSON.stringify({{credential: resp.credential}})
  }}).then(r => r.json()).then(d => {{
    if (d.ok) location.href = '/';
    else {{ document.getElementById('err').style.display='block';
            document.getElementById('err').textContent = d.error; }}
  }});
}}
</script></body></html>'''

@server.route('/login')
def login_page():
    return LOGIN_PAGE

@server.route('/auth/google', methods=['POST'])
def auth_google():
    from google.oauth2 import id_token
    from google.auth.transport import requests as g_requests
    try:
        data = request.get_json()
        token = data.get('credential', '')
        info = id_token.verify_oauth2_token(token, g_requests.Request(), GOOGLE_CLIENT_ID)
        email = info.get('email', '')
        domain = email.split('@')[-1] if '@' in email else ''
        if ALLOWED_DOMAINS and domain not in ALLOWED_DOMAINS:
            return _json.dumps({'ok': False, 'error': f'{domain} 도메인은 접근할 수 없습니다.'})
        session['user'] = {'email': email, 'name': info.get('name', ''), 'picture': info.get('picture', '')}
        return _json.dumps({'ok': True})
    except Exception as e:
        return _json.dumps({'ok': False, 'error': str(e)})

@server.route('/logout')
def logout():
    session.clear()
    return redirect('/login')

@server.before_request
def check_auth():
    # localhost에서는 인증 건너뛰기
    if request.host.startswith('127.0.0.1') or request.host.startswith('localhost'):
        return
    if request.path in ('/login', '/auth/google') or request.path.startswith('/assets'):
        return
    if 'user' not in session:
        return redirect('/login')

app = dash.Dash(__name__, server=server, external_stylesheets=[dbc.themes.BOOTSTRAP],
                suppress_callback_exceptions=True, url_base_pathname='/')
app.title = '-3W Booking Dashboard'

DDS = {'width': '110px', 'display': 'inline-block', 'verticalAlign': 'middle'}

app.layout = html.Div([
    # Header
    html.Div([dbc.Container([dbc.Row([
        dbc.Col(html.H5("-3W Booking Dashboard", className='mb-0',
                         style={'fontWeight': '700', 'color': '#fff'}), width='auto'),
        dbc.Col(html.Small(f"Data: {DATA_DATE[:4]}-{DATA_DATE[4:6]}-{DATA_DATE[6:]}",
                           style={'color': 'rgba(255,255,255,.6)'}), className='text-end', width=True),
    ], className='align-items-center')], fluid=True)],
        style={'background': 'linear-gradient(135deg,#1a73e8,#1557b0)', 'padding': '10px 0'}),

    # Global filter bar — 이름 | 필터 | 이름 | 필터 형태
    html.Div([dbc.Container([dbc.Row([
        dbc.Col(html.Small("팀", style={'color': C['mt'], 'whiteSpace': 'nowrap'}), width='auto', className='pe-1'),
        dbc.Col(dcc.Dropdown(id='g-team', options=[{'label': '전체', 'value': 'ALL'}] +
                              [{'label': t, 'value': t} for t in ALL_TEAMS],
                              value='ALL', clearable=False, style={'width': '90px'}), width='auto', className='pe-3'),
        dbc.Col(html.Small("선적지", style={'color': C['mt'], 'whiteSpace': 'nowrap'}), width='auto', className='pe-1'),
        dbc.Col(dcc.Dropdown(id='g-ori', value='ALL', clearable=False, style={'width': '80px'}), width='auto', className='pe-3'),
        dbc.Col(html.Small("선적포트", style={'color': C['mt'], 'whiteSpace': 'nowrap'}), width='auto', className='pe-1'),
        dbc.Col(dcc.Dropdown(id='g-ori-p', value='ALL', clearable=False, style={'width': '90px'}), width='auto', className='pe-3'),
        dbc.Col(html.Small("도착지", style={'color': C['mt'], 'whiteSpace': 'nowrap'}), width='auto', className='pe-1'),
        dbc.Col(dcc.Dropdown(id='g-dst', value='ALL', clearable=False, style={'width': '80px'}), width='auto', className='pe-3'),
        dbc.Col(html.Small("도착포트", style={'color': C['mt'], 'whiteSpace': 'nowrap'}), width='auto', className='pe-1'),
        dbc.Col(dcc.Dropdown(id='g-dst-p', value='ALL', clearable=False, style={'width': '90px'}), width='auto', className='pe-4'),
        dbc.Col(html.Small("보기", style={'color': C['mt'], 'whiteSpace': 'nowrap'}), width='auto', className='pe-1'),
        dbc.Col(dcc.Dropdown(id='g-view', options=[{'label': '도착지별', 'value': 'dest'},
                              {'label': '선적지별', 'value': 'origin'}],
                              value='dest', clearable=False, style={'width': '100px'}), width='auto'),
    ], className='align-items-center py-2')], fluid=True)],
        style={'backgroundColor': '#fff', 'borderBottom': f'1px solid {C["bdr"]}'}),

    dbc.Tabs([
        dbc.Tab(tab1(), label='① 소석률 현황', tab_id='t1'),
        dbc.Tab(tab2(), label='② 부킹 트렌드', tab_id='t2'),
        dbc.Tab(tab3(), label='③ 전환 퍼널', tab_id='t3'),
        dbc.Tab(tab4(), label='④ 수익성', tab_id='t4'),
        dbc.Tab(tab5(), label='⑤ 부킹 패턴', tab_id='t5'),
    ], id='tabs', active_tab='t1'),
], style={'backgroundColor': C['bg'], 'minHeight': '100vh'})


# ═══════════════════════════════════════════════════════════
# Cascading filter callbacks
# ═══════════════════════════════════════════════════════════
@app.callback(
    [Output('g-ori', 'options'), Output('g-ori', 'value')],
    Input('g-team', 'value'))
def update_ori_opts(team):
    d = BKG if team == 'ALL' else BKG[BKG['team'] == team]
    opts = sorted(d['origin'].unique())
    return [{'label': '전체', 'value': 'ALL'}] + [{'label': o, 'value': o} for o in opts], 'ALL'

@app.callback(
    [Output('g-ori-p', 'options'), Output('g-ori-p', 'value')],
    [Input('g-team', 'value'), Input('g-ori', 'value')])
def update_ori_port(team, ori):
    d = BKG if team == 'ALL' else BKG[BKG['team'] == team]
    if ori != 'ALL':
        d = d[d['origin'] == ori]
    opts = sorted(d['ori_port'].dropna().unique())
    return [{'label': '전체', 'value': 'ALL'}] + [{'label': p, 'value': p} for p in opts], 'ALL'

@app.callback(
    [Output('g-dst', 'options'), Output('g-dst', 'value')],
    [Input('g-team', 'value'), Input('g-ori', 'value')])
def update_dst_opts(team, ori):
    d = BKG if team == 'ALL' else BKG[BKG['team'] == team]
    if ori != 'ALL':
        d = d[d['origin'] == ori]
    opts = sorted(d['dest'].unique())
    return [{'label': '전체', 'value': 'ALL'}] + [{'label': o, 'value': o} for o in opts], 'ALL'

@app.callback(
    [Output('g-dst-p', 'options'), Output('g-dst-p', 'value')],
    [Input('g-team', 'value'), Input('g-ori', 'value'), Input('g-dst', 'value')])
def update_dst_port(team, ori, dst):
    d = BKG if team == 'ALL' else BKG[BKG['team'] == team]
    if ori != 'ALL':
        d = d[d['origin'] == ori]
    if dst != 'ALL':
        d = d[d['dest'] == dst]
    opts = sorted(d['dst_port'].dropna().unique())
    return [{'label': '전체', 'value': 'ALL'}] + [{'label': p, 'value': p} for p in opts], 'ALL'


# Week dropdown cascading — update options when month changes
for _tid in ['t1', 't2', 't3', 't4']:
    @app.callback(
        [Output(f'{_tid}-w', 'options'), Output(f'{_tid}-w', 'value')],
        Input(f'{_tid}-m', 'value'),
        prevent_initial_call=True)
    def _update_week_opts(month, _t=_tid):
        wk_opts = [{'label': '전체', 'value': 'ALL'}] + WEEK_OPTS.get(month, [])
        return wk_opts, 'ALL'


# ═══════════════════════════════════════════════════════════
# Common filter args
# ═══════════════════════════════════════════════════════════
GF_INPUTS = [Input('g-team', 'value'), Input('g-ori', 'value'),
             Input('g-ori-p', 'value'), Input('g-dst', 'value'), Input('g-dst-p', 'value'),
             Input('g-view', 'value')]

def get_view(fd, team='ALL', view_mode='dest'):
    """Return view column based on user selection."""
    if view_mode == 'origin':
        return 'origin', '선적지'
    return 'dest', '도착지'


# ═══════════════════════════════════════════════════════════
# Tab 1
# ═══════════════════════════════════════════════════════════
@app.callback(
    [Output('t1-kpis', 'children'), Output('t1-bar', 'figure'),
     Output('t1-oh', 'children'), Output('t1-occ', 'figure'),
     Output('t1-th', 'children'), Output('t1-tbl', 'children')],
    GF_INPUTS + [Input('t1-m', 'value'), Input('t1-w', 'value')])
def cb1(team, ori, ori_p, dst, dst_p, view_mode, month, week):
    fd = gf(BKG, team, ori, ori_p, dst, dst_p, month=None, week=None)  # full for trends
    fd_mw = gf(BKG, team, ori, ori_p, dst, dst_p, month, week)  # month+week filtered
    vc, vl = get_view(fd, team, view_mode)
    ms = ALL_MONTHS
    pi = ms.index(month) - 1 if month in ms and ms.index(month) > 0 else None
    pm = ms[pi] if pi is not None and pi >= 0 else None

    shipped_status = ['Normal']
    w3 = fd[fd['Lead_time (BKG_Sche)'] == 'WOS-3']
    w3_mw = fd_mw[fd_mw['Lead_time (BKG_Sche)'] == 'WOS-3']
    bsa_m = gf_bsa(team, ori, dst, ori_p, dst_p, week)
    bsa_mm = bsa_m[bsa_m['YYYYMM'] == month]
    tbs = bsa_mm['teu_bsa'].sum()

    # KPI metrics — 4 cards: 전체BKG → 실선적 → 3주전BKG → 3주전 실선적
    total_bkg = fd_mw['fst'].sum()
    total_shipped = fd_mw[fd_mw['LST_Status'].isin(shipped_status)]['fst'].sum()
    w3_bkg = w3_mw['fst'].sum()
    w3_shipped = w3_mw[w3_mw['LST_Status'].isin(shipped_status)]['fst'].sum()

    # Rates — Normal only
    occ = total_shipped / tbs * 100 if tbs else 0          # 소석률 = 실선적/BSA
    sr = w3_shipped / w3_bkg * 100 if w3_bkg else 0        # 실선적률 = 3주전실선적/3주전BKG
    hcm = fd_mw[(fd_mw['LST_Status'] == 'Normal') & (fd_mw['cm1v'] != 0)]
    cm = hcm['cm1v'].sum() / hcm['lst'].sum() if hcm['lst'].sum() else 0
    do, ds, dc = None, None, None
    if pm:
        pfd = gf(BKG, team, ori, ori_p, dst, dst_p, pm, None)
        p_ship = pfd[pfd['LST_Status'] == 'Normal']['fst'].sum()
        p_bsa = bsa_m[bsa_m['YYYYMM'] == pm]['teu_bsa'].sum()
        pw3 = pfd[pfd['Lead_time (BKG_Sche)'] == 'WOS-3']
        pw3s = pw3[pw3['LST_Status'] == 'Normal']['fst'].sum()
        pw3b = pw3['fst'].sum()
        pcm = pfd[(pfd['LST_Status'] == 'Normal') & (pfd['cm1v'] != 0)]
        do = occ - (p_ship / p_bsa * 100 if p_bsa else 0)
        ds = sr - (pw3s / pw3b * 100 if pw3b else 0)
        dc = cm - (pcm['cm1v'].sum() / pcm['lst'].sum() if pcm['lst'].sum() else 0)

    kpis = dbc.Row([
        dbc.Col(kpi_c("전체 BKG", total_bkg, col=C['pri'])),
        dbc.Col(kpi_c("실선적", total_shipped, col=C['ok'])),
        dbc.Col(kpi_c("3주전 BKG", w3_bkg, col='#6c5ce7')),
        dbc.Col(kpi_c("3주전 실선적", w3_shipped, col='#00b894')),
        dbc.Col(kpi_c("BSA", tbs, col=C['mt'])),
        dbc.Col(kpi_c("소석률", occ, do, '.1f', '%', C['ok'] if occ <= 120 else C['ng'])),
        dbc.Col(kpi_c("실선적률", sr, ds, '.1f', '%', C['ok'] if sr >= 55 else C['warn'])),
        dbc.Col(kpi_c("CM1/TEU", cm, dc, ',.0f')),
    ], className='g-2')

    # Monthly bar — 실선적 vs BSA + 소석률(실선적/BSA) + 3주전실선적/BSA
    bar_ms = [m for m in ms if m <= month]
    bsa_m2 = bsa_m.groupby('YYYYMM')['teu_bsa'].sum().reindex(bar_ms, fill_value=0)
    # 전체 실선적 by month
    shipped_all = fd[fd['LST_Status'].isin(shipped_status)]
    ship_m = shipped_all.groupby('YYYYMM')['fst'].sum().reindex(bar_ms, fill_value=0)
    # 3주전 실선적 by month
    w3_shipped_all = w3[w3['LST_Status'].isin(shipped_status)]
    w3ship_m = w3_shipped_all.groupby('YYYYMM')['fst'].sum().reindex(bar_ms, fill_value=0)
    # 소석률 = 전체실선적 / BSA
    occ_m = [ship_m.iloc[i] / bsa_m2.iloc[i] * 100 if bsa_m2.iloc[i] > 0 else None
             for i in range(len(bar_ms))]
    # 3주전 실선적/BSA 비중
    w3occ_m = [w3ship_m.iloc[i] / bsa_m2.iloc[i] * 100 if bsa_m2.iloc[i] > 0 else None
               for i in range(len(bar_ms))]
    xl = [ML.get(m, m) for m in bar_ms]

    fb = go.Figure()
    fb.add_bar(x=xl, y=ship_m.values, name='실선적', marker_color=C['pri'], opacity=.85,
               text=[f"{v:,.0f}" for v in ship_m.values], textposition='outside', textfont=dict(size=10))
    fb.add_bar(x=xl, y=bsa_m2.values, name='BSA', marker_color=C['bdr'], opacity=.5,
               text=[f"{v:,.0f}" for v in bsa_m2.values], textposition='outside', textfont=dict(size=10))
    fb.add_scatter(x=xl, y=occ_m, name='소석률(실선적/BSA)', yaxis='y2', mode='lines+markers+text',
                   text=[f'{v:.0f}%' if v is not None else '' for v in occ_m],
                   textposition='top center', connectgaps=False,
                   line=dict(color=C['ng'], width=2.5), marker=dict(size=8))
    fb.add_scatter(x=xl, y=w3occ_m, name='3주전실선적/BSA', yaxis='y2', mode='lines+markers+text',
                   text=[f'{v:.0f}%' if v is not None else '' for v in w3occ_m],
                   textposition='bottom center', connectgaps=False,
                   line=dict(color='#6c5ce7', width=2, dash='dash'), marker=dict(size=6, symbol='diamond'))

    all_bar_vals = list(ship_m.values) + list(bsa_m2.values)
    y1_max = max(all_bar_vals) * 1.3 if any(v > 0 for v in all_bar_vals) else 1000
    occ_vals = [v for v in (occ_m + w3occ_m) if v is not None]
    y2_max = max(110, int(max(occ_vals, default=100) * 1.15 / 10 + 1) * 10)

    fb.update_layout(barmode='group', margin=dict(l=50, r=50, t=30, b=30),
                     yaxis=dict(title='TEU', gridcolor='#f0f0f0', range=[0, y1_max]),
                     yaxis2=dict(title='소석률 %', overlaying='y', side='right', range=[0, y2_max]),
                     legend=dict(orientation='h', y=1.15), plot_bgcolor='#fff', paper_bgcolor='#fff',
                     uniformtext_minsize=8, uniformtext_mode='hide')

    # Occupancy horizontal bar
    # 소석률 바: 실선적/BSA 기준 + (3주전실선적/BSA) 표시
    shipped_mw = fd_mw[fd_mw['LST_Status'].isin(shipped_status)]
    items = sorted(shipped_mw[vc].unique(), key=lambda x: -shipped_mw[shipped_mw[vc] == x]['fst'].sum())[:15]
    if not items:  # fallback
        items = sorted(fd_mw[vc].unique(), key=lambda x: -fd_mw[fd_mw[vc] == x]['fst'].sum())[:15]
    fo = go.Figure()
    orows = []
    bsa_grp = 'origin' if vc == 'origin' else 'dest'
    bsa_vc = bsa_mm.groupby(bsa_grp)['teu_bsa'].sum() if len(bsa_mm) else pd.Series(dtype=float)
    for it in items:
        ship = shipped_mw[shipped_mw[vc] == it]['fst'].sum()
        w3s = w3_mw[(w3_mw[vc] == it) & (w3_mw['LST_Status'].isin(shipped_status))]['fst'].sum()
        bs = bsa_vc.get(it, 0)
        ov = ship / bs * 100 if bs else 0
        orows.append((it, ov, ship, bs, w3s))
    orows.sort(key=lambda x: x[1])
    def _fmt(v):
        """천단위 구분자 (Plotly 호환 — 콤마 대신 thin space)"""
        s = f"{v:,.0f}"
        return s.replace(',', '\u2009')  # thin space as thousand separator

    if orows:
        clrs = [C['ok'] if v >= 65 else C['warn'] if v >= 50 else C['ng'] for _, v, _, _, _ in orows]
        y_labels = [f"({_fmt(ship)} / {_fmt(bs)})  {nm}" if bs > 0 else f"({_fmt(ship)} / -)  {nm}"
                    for nm, _, ship, bs, _ in orows]
        fo.add_bar(y=y_labels, x=[v for _, v, _, _, _ in orows], orientation='h',
                   marker_color=clrs, text=[f"{v:.0f}%" for _, v, _, _, _ in orows],
                   textposition='outside', textfont=dict(size=11),
                   cliponaxis=False, name='소석률(실선적/BSA)')
    else:
        pass
    fo.add_vline(x=50, line_dash='dot', line_color=C['mt'], opacity=.4)
    fo.update_layout(margin=dict(l=220, r=60, t=20, b=30),
                     xaxis=dict(title='소석률 %', autorange=True),
                     yaxis=dict(tickfont=dict(family='Consolas, monospace', size=11)),
                     plot_bgcolor='#fff', paper_bgcolor='#fff', showlegend=False)

    # Summary table (Image 8 format)
    def bsa_lookup(m, it):
        b = gf_bsa(team, ori, dst, ori_p, dst_p, week)
        b = b[b['YYYYMM'] == m]
        if it:
            bsa_key = 'origin' if vc == 'origin' else 'dest'
            b = b[b[bsa_key] == it]
        return b['teu_bsa'].sum()

    tbl = build_summary_table(fd, bsa_lookup, [m for m in ms if m <= month], vc, vl, items)

    return (kpis, fb, st(f"{vl}별 소석률 (실선적/BSA)", ML.get(month, month)), fo,
            st(f"{vl}별 월간 실적", "Image-8 포맷"), tbl)


# ═══════════════════════════════════════════════════════════
# Tab 2
# ═══════════════════════════════════════════════════════════
def _abcd_insight(tbl_df, gc, gl):
    """AB vs CD 분석 가이드 생성"""
    if tbl_df.empty:
        return html.Div()
    tips = []
    total_ab = tbl_df['ab'].sum(); total_cd = tbl_df['cd'].sum()
    total = total_ab + total_cd
    if total == 0:
        return html.Div()
    ab_pct = total_ab / total * 100

    # 1) 전체 AB 비중 진단
    if ab_pct >= 60:
        tips.append(html.Li([html.Strong("A+B 의존도 높음: "), f"전체 BKG의 {ab_pct:.0f}%가 A+B 등급. 대형화주 집중 리스크 — 특정 화주 이탈 시 영향 큼."]))
    elif ab_pct <= 30:
        tips.append(html.Li([html.Strong("C+D 비중 높음: "), f"전체 BKG의 {100-ab_pct:.0f}%가 C+D 등급. 중소화주 기반이 넓어 안정적이나, C+D 실선적률 관리 필요."]))

    # 2) 실선적률 차이 진단
    ab_ship = tbl_df['ab_ship'].sum(); cd_ship = tbl_df['cd_ship'].sum()
    ab_lft = (ab_ship / total_ab * 100) if total_ab else 0
    cd_lft = (cd_ship / total_cd * 100) if total_cd else 0
    gap = ab_lft - cd_lft
    if gap > 10:
        tips.append(html.Li([html.Strong(f"실선적률 격차 주의: "), f"A+B {ab_lft:.0f}% vs C+D {cd_lft:.0f}% (차이 {gap:.0f}%p). C+D 캔슬률이 높음 — 부킹 확정 관리 강화 필요."]))
    elif gap < -5:
        tips.append(html.Li([html.Strong(f"A+B 실선적률 하락: "), f"A+B {ab_lft:.0f}% vs C+D {cd_lft:.0f}%. 대형화주 부킹 변동이 크므로 선복 조정 주의."]))

    # 3) 지역별 AB 편중 진단
    tbl_df = tbl_df[tbl_df['total'] > 0].copy()
    tbl_df['ab_ratio'] = tbl_df['ab'] / tbl_df['total'] * 100
    if len(tbl_df) >= 2:
        top_ab = tbl_df.nlargest(1, 'ab_ratio').iloc[0]
        low_ab = tbl_df.nsmallest(1, 'ab_ratio').iloc[0]
        if top_ab['ab_ratio'] - low_ab['ab_ratio'] > 30:
            tips.append(html.Li([html.Strong("지역별 편차 큼: "),
                f"{top_ab[gc]}은 A+B {top_ab['ab_ratio']:.0f}%로 대형화주 집중, {low_ab[gc]}은 A+B {low_ab['ab_ratio']:.0f}%로 중소화주 중심. 지역별 차별 전략 필요."]))

    if not tips:
        tips.append(html.Li("등급별 비중 및 실선적률이 균형 잡힌 상태입니다."))

    return html.Div([
        html.H6("Analysis Guide", style={'fontWeight': '700', 'color': C['pri'], 'marginTop': '16px', 'marginBottom': '8px'}),
        html.Ul(tips, style={'fontSize': '13px', 'lineHeight': '1.8', 'color': '#333'})
    ], style={'background': '#e8f0fe', 'borderLeft': f"4px solid {C['pri']}", 'padding': '12px 16px', 'borderRadius': '0 4px 4px 0', 'marginTop': '12px'})


@app.callback(
    [Output('t2-wos', 'children'), Output('t2-wk', 'figure')],
    GF_INPUTS + [Input('t2-m', 'value'), Input('t2-w', 'value'), Input('t2-pf', 'value'), Input('t2-sub', 'active_tab')])
def cb2(team, ori, ori_p, dst, dst_p, view_mode, month, week, profit, subtab):
    df = gf(BKG, team, ori, ori_p, dst, dst_p, month, week)
    if profit == 'hi': df = df[df['profit_type'] == '고수익화주']
    elif profit == 'lo': df = df[df['profit_type'] == '저수익화주']
    elif profit == 'ab': df = df[df['grade'] == 'A+B']
    elif profit == 'cd': df = df[df['grade'] == 'C+D']

    # --- AB vs CD subtab ---
    if subtab == 'abcd':
        gc = 'origin' if view_mode == 'origin' else 'dest'
        gl = '선적지' if gc == 'origin' else '도착지'
        w3 = df[df['Lead_time (BKG_Sche)'] == 'WOS-3']
        ab = w3[w3['grade'] == 'A+B']; cd = w3[w3['grade'] == 'C+D']
        ab_g = ab.groupby(gc)['fst'].sum().reset_index().rename(columns={'fst': 'ab'})
        cd_g = cd.groupby(gc)['fst'].sum().reset_index().rename(columns={'fst': 'cd'})
        ab_n = ab[ab['LST_Status'] == 'Normal'].groupby(gc)['fst'].sum().reset_index().rename(columns={'fst': 'ab_ship'})
        cd_n = cd[cd['LST_Status'] == 'Normal'].groupby(gc)['fst'].sum().reset_index().rename(columns={'fst': 'cd_ship'})
        tbl = ab_g.merge(cd_g, on=gc, how='outer').merge(ab_n, on=gc, how='left').merge(cd_n, on=gc, how='left').fillna(0)
        tbl['total'] = tbl['ab'] + tbl['cd']
        tbl = tbl[tbl['total'] > 0].nlargest(15, 'total')

        rows = []
        for _, r in tbl.iterrows():
            tot = r['total']
            rows.append({
                gl: r[gc],
                'A+B BKG': f"{r['ab']:,.0f}", 'C+D BKG': f"{r['cd']:,.0f}",
                'A+B %': f"{r['ab']/tot*100:.0f}%" if tot else '-',
                'C+D %': f"{r['cd']/tot*100:.0f}%" if tot else '-',
                'A+B 실선적률': f"{r['ab_ship']/r['ab']*100:.0f}%" if r['ab'] else '-',
                'C+D 실선적률': f"{r['cd_ship']/r['cd']*100:.0f}%" if r['cd'] else '-',
                '전체 BKG': f"{tot:,.0f}",
            })
        # 합계
        if rows:
            def _s(v):
                try: return float(v.replace(',', ''))
                except: return 0
            tr = {gl: '합계'}
            for k in rows[0]:
                if k == gl: continue
                tr[k] = '' if '%' in str(rows[0].get(k, '')) else f"{sum(_s(r.get(k, '0')) for r in rows):,.0f}"
            tab_sum = _s(tr.get('A+B BKG', '0')); tcd_sum = _s(tr.get('C+D BKG', '0'))
            tab_ship = _s(tr.get('A+B 실선적률', '0')); tcd_ship = _s(tr.get('C+D 실선적률', '0'))
            ttot = tab_sum + tcd_sum
            tr['A+B %'] = f"{tab_sum/ttot*100:.0f}%" if ttot else '-'
            tr['C+D %'] = f"{tcd_sum/ttot*100:.0f}%" if ttot else '-'
            tab_s = tbl['ab_ship'].sum(); tcd_s = tbl['cd_ship'].sum()
            tr['A+B 실선적률'] = f"{tab_s/tab_sum*100:.0f}%" if tab_sum else '-'
            tr['C+D 실선적률'] = f"{tcd_s/tcd_sum*100:.0f}%" if tcd_sum else '-'
            rows.append(tr)

        # Chart: stacked bar A+B vs C+D by region
        fig = go.Figure()
        chart_df = tbl.nlargest(10, 'total')
        fig.add_bar(x=chart_df[gc], y=chart_df['ab'], name='A+B', marker_color='#1a73e8')
        fig.add_bar(x=chart_df[gc], y=chart_df['cd'], name='C+D', marker_color='#f9ab00')
        fig.update_layout(barmode='stack', margin=dict(l=40, r=20, t=20, b=30),
                          yaxis=dict(title='TEU', gridcolor='#f0f0f0'),
                          legend=dict(orientation='h', y=1.12), plot_bgcolor='#fff', paper_bgcolor='#fff')

        insight = _abcd_insight(tbl, gc, gl)
        return html.Div([mk_tbl(rows, gl), insight]), fig

    # --- Existing subtabs ---
    gc, gl = ('origin', '선적지') if subtab == 'origin' else ('dest', '도착지') if subtab == 'dest' else ('LST_route', '루트') if subtab == 'route' else ('Salesman_POR', '영업사원') if subtab == 'sales' else ('BKG_SHPR_CST_ENM', '화주')

    # WOS-3 only
    w3 = df[df['Lead_time (BKG_Sche)'] == 'WOS-3']
    w3_all = w3.groupby(gc)['fst'].sum().reset_index().rename(columns={'fst': 'bkg'})
    w3_n = w3[w3['LST_Status'] == 'Normal'].groupby(gc)['fst'].sum().reset_index().rename(columns={'fst': 'ship'})
    w3_hi = w3[w3['profit_type'] == '고수익화주'].groupby(gc)['fst'].sum().reset_index().rename(columns={'fst': 'hi'})
    tbl_df = w3_all.merge(w3_n, on=gc, how='left').merge(w3_hi, on=gc, how='left').fillna(0)
    # 화주별일 때 등급 + 영업사원 컬럼 추가
    if subtab == 'cust':
        if 'grade' in w3.columns:
            gr_map = w3.groupby('BKG_SHPR_CST_ENM')['grade'].agg(lambda x: x.mode().iloc[0] if len(x.mode()) > 0 else 'C+D').to_dict()
            tbl_df['등급'] = tbl_df[gc].map(gr_map).fillna('C+D')
        if 'Salesman_POR' in w3.columns:
            sm_map = w3.groupby('BKG_SHPR_CST_ENM')['Salesman_POR'].agg(lambda x: ', '.join(sorted(set(x.dropna().astype(str)) - {'','nan'}))).to_dict()
            tbl_df['영업사원'] = tbl_df[gc].map(sm_map).fillna('')
    # 영업사원별일 때 화주수 추가
    if subtab == 'sales' and 'BKG_SHPR_CST_NO' in w3.columns:
        cust_cnt = w3.groupby('Salesman_POR')['BKG_SHPR_CST_NO'].nunique().to_dict()
        tbl_df['화주수'] = tbl_df[gc].map(cust_cnt).fillna(0).astype(int)
    tbl_df = tbl_df.nlargest(15 if subtab not in ('sales', 'cust') else 50, 'bkg')

    rows = []
    for _, r in tbl_df.iterrows():
        row = {gl: r[gc]}
        if subtab == 'cust':
            if '등급' in tbl_df.columns:
                row['등급'] = r['등급']
            if '영업사원' in tbl_df.columns:
                row['영업사원'] = r['영업사원']
        if subtab == 'sales' and '화주수' in tbl_df.columns:
            row['화주수'] = str(int(r['화주수']))
        row.update({'BKG': f"{r['bkg']:,.0f}", '실선적': f"{r['ship']:,.0f}",
               '실선적률': f"{r['ship']/r['bkg']*100:.0f}%" if r['bkg'] else '-',
               '고수익BKG': f"{r['hi']:,.0f}",
               '고수익%': f"{r['hi']/r['bkg']*100:.0f}%" if r['bkg'] else '-'})
        if subtab in ('dest', 'route') or gc == 'origin':
            bv = gf_bsa(team, ori, dst, ori_p, dst_p, week)
            bsa_key = 'origin' if gc == 'origin' else 'dest'
            bv = bv[(bv['YYYYMM'] == month) & (bv[bsa_key] == r[gc])]['teu_bsa'].sum()
            row['BSA'] = f"{bv:,.0f}"
            row['소석률'] = f"{r['bkg']/bv*100:.0f}%" if bv else '-'
        rows.append(row)

    # 합계
    if rows:
        def _s(v):
            try: return float(v.replace(',', ''))
            except: return 0
        tr = {gl: '합계'}
        for k in rows[0]:
            if k == gl: continue
            tr[k] = '' if '%' in str(rows[0].get(k, '')) else f"{sum(_s(r.get(k, '0')) for r in rows):,.0f}"
        tb = _s(tr.get('BKG', '0')); ts = _s(tr.get('실선적', '0')); th = _s(tr.get('고수익BKG', '0'))
        tr['실선적률'] = f"{ts/tb*100:.0f}%" if tb else '-'
        tr['고수익%'] = f"{th/tb*100:.0f}%" if tb else '-'
        if 'BSA' in rows[0]:
            bt = gf_bsa(team, ori, dst, ori_p, dst_p, week)
            bt = bt[bt['YYYYMM'] == month]['teu_bsa'].sum()
            tr['소석률'] = f"{tb/bt*100:.0f}%" if bt else '-'
        rows.append(tr)

    # Weekly chart (WOS-3 only)
    wk = w3.groupby(['week_dt', 'dest'])['fst'].sum().reset_index()
    wk = wk[wk['week_dt'].notna()].sort_values('week_dt')
    fig = go.Figure()
    for i, d in enumerate(wk.groupby('dest')['fst'].sum().nlargest(8).index):
        dd = wk[wk['dest'] == d]
        fig.add_bar(x=dd['week_dt'].dt.strftime('%m/%d'), y=dd['fst'], name=d, marker_color=PAL[i % len(PAL)])
    fig.update_layout(barmode='stack', margin=dict(l=40, r=20, t=20, b=30),
                      yaxis=dict(title='TEU', gridcolor='#f0f0f0'),
                      legend=dict(orientation='h', y=1.12), plot_bgcolor='#fff', paper_bgcolor='#fff')
    return mk_tbl(rows, gl), fig


# ═══════════════════════════════════════════════════════════
# Tab 3
# ═══════════════════════════════════════════════════════════
@app.callback(
    [Output('t3-fn', 'figure'), Output('t3-tr', 'figure'),
     Output('t3-ct', 'children'), Output('t3-wf', 'figure')],
    GF_INPUTS + [Input('t3-m', 'value'), Input('t3-w', 'value')])
def cb3(team, ori, ori_p, dst, dst_p, view_mode, month, week):
    fd = gf(BKG, team, ori, ori_p, dst, dst_p)
    vc, vl = get_view(fd, team, view_mode)
    # All data filtered to WOS-3 (3W+4W) — consistent basis
    w3_all = fd[fd['Lead_time (BKG_Sche)'] == 'WOS-3']
    w3m = w3_all[w3_all['YYYYMM'] == month]
    if week and week != 'ALL':
        w3m = w3m[w3m['week_start_date'] == week]

    # Funnel: 3주전 BKG → 캔슬 제외 → 실선적
    w3b = w3m['fst'].sum(); w3c = w3m[w3m['LST_Status'] == 'Cancel']['fst'].sum()
    w3n = w3m[w3m['LST_Status'] == 'Normal']['fst'].sum()
    ff = go.Figure(go.Funnel(y=['3주전 BKG', '캔슬 제외', '실선적(Normal)'],
        x=[w3b, w3b - w3c, w3n], textinfo='value+percent initial',
        marker=dict(color=[C['pri'], C['warn'], C['ok']])))
    ff.update_layout(margin=dict(l=20, r=20, t=20, b=20), plot_bgcolor='#fff', paper_bgcolor='#fff')

    # 실선적률 추이 (WOS-3 기준, 월별)
    items = sorted(w3m[vc].unique(), key=lambda x: -w3m[w3m[vc] == x]['fst'].sum())[:8]
    ft = go.Figure()
    for i, it in enumerate(items):
        vals = []
        for m in ALL_MONTHS:
            wm = w3_all[w3_all['YYYYMM'] == m]
            dd = wm[wm[vc] == it]; b = dd['fst'].sum(); n = dd[dd['LST_Status'] == 'Normal']['fst'].sum()
            vals.append(n / b * 100 if b else None)
        ft.add_scatter(x=[ML.get(m, m) for m in ALL_MONTHS], y=vals, mode='lines+markers', name=it,
                       connectgaps=True, line=dict(color=PAL[i % len(PAL)], width=2))
    ft.update_layout(margin=dict(l=40, r=20, t=20, b=30), yaxis=dict(title='3주전 실선적률 %', gridcolor='#f0f0f0'),
                     legend=dict(orientation='h', y=1.12), plot_bgcolor='#fff', paper_bgcolor='#fff')

    # 캔슬 분석 (WOS-3 기준)
    bsa_data = gf_bsa(team, ori, dst, ori_p, dst_p, week)
    bsa_mm = bsa_data[bsa_data['YYYYMM'] == month]
    bsa_grp3 = 'origin' if vc == 'origin' else 'dest'
    bsa_vc = bsa_mm.groupby(bsa_grp3)['teu_bsa'].sum() if len(bsa_mm) else pd.Series(dtype=float)
    crows = []
    for it in items:
        dd = w3m[w3m[vc] == it]; b = dd['fst'].sum()
        if b == 0: continue
        n = dd[dd['LST_Status'] == 'Normal']['fst'].sum()
        c = dd[dd['LST_Status'] == 'Cancel']['fst'].sum()
        bs = bsa_vc.get(it, 0)
        crows.append({vl: it, '3주전BKG': f"{b:,.0f}", '실선적': f"{n:,.0f}", '캔슬': f"{c:,.0f}",
                      '캔슬률': f"{c/b*100:.0f}%", '실선적률': f"{n/b*100:.0f}%",
                      'BSA': f"{bs:,.0f}", '소석률': f"{n/bs*100:.0f}%" if bs else '-'})

    # 주차별 3주전 BKG vs 실선적 (워터폴 대체)
    wk = w3m.groupby('week_dt').agg(bkg=('fst', 'sum')).reset_index()
    wk_n = w3m[w3m['LST_Status'] == 'Normal'].groupby('week_dt')['fst'].sum().reset_index().rename(columns={'fst': 'ship'})
    wk = wk.merge(wk_n, on='week_dt', how='left').fillna(0).sort_values('week_dt')
    wk = wk[wk['week_dt'].notna()]
    fw = go.Figure()
    fw.add_bar(x=wk['week_dt'].dt.strftime('%m/%d'), y=wk['bkg'], name='3주전 BKG', marker_color=C['pri'], opacity=.7)
    fw.add_bar(x=wk['week_dt'].dt.strftime('%m/%d'), y=wk['ship'], name='실선적', marker_color=C['ok'], opacity=.85)
    fw.update_layout(barmode='group', margin=dict(l=40, r=20, t=20, b=30),
                     yaxis=dict(title='TEU', gridcolor='#f0f0f0'),
                     legend=dict(orientation='h', y=1.12), plot_bgcolor='#fff', paper_bgcolor='#fff')
    return ff, ft, mk_tbl(crows, vl), fw


# ═══════════════════════════════════════════════════════════
# Tab 4
# ═══════════════════════════════════════════════════════════
@app.callback(
    [Output('t4-cm', 'figure'), Output('t4-hi', 'figure'), Output('t4-sc', 'figure')],
    GF_INPUTS + [Input('t4-m', 'value'), Input('t4-w', 'value')])
def cb4(team, ori, ori_p, dst, dst_p, view_mode, month, week):
    fd = gf(BKG, team, ori, ori_p, dst, dst_p)
    vc, vl = get_view(fd, team, view_mode)
    w3 = fd[fd['Lead_time (BKG_Sche)'] == 'WOS-3']
    w3m = w3[w3['YYYYMM'] == month]
    if week and week != 'ALL':
        w3m = w3m[w3m['week_start_date'] == week]
    cur = agg_m(w3m, [vc]) if len(w3m) else pd.DataFrame()
    show = cur.nlargest(10, 'bkg')[vc].tolist() if len(cur) else []
    cs = cur[cur[vc].isin(show)].sort_values('cm1teu', ascending=True) if len(cur) else pd.DataFrame()

    fc = go.Figure()
    if len(cs):
        fc.add_bar(y=cs[vc], x=cs['cm1teu'], orientation='h', marker_color=C['pri'], opacity=.85,
                   text=[f"{v:,.0f}" for v in cs['cm1teu']], textposition='auto', textfont=dict(color='#fff'))
    fc.update_layout(margin=dict(l=50, r=30, t=20, b=30), xaxis=dict(title='CM1/TEU', gridcolor='#f0f0f0'),
                     plot_bgcolor='#fff', paper_bgcolor='#fff')

    fh = go.Figure()
    for m in ALL_MONTHS:
        wm = w3[w3['YYYYMM'] == m]
        if len(wm) == 0: continue
        ma = agg_m(wm, [vc])
        vals = ma.set_index(vc)['hi%'].reindex(show, fill_value=0)
        fh.add_bar(x=show, y=vals.values, name=ML.get(m, m),
                   text=[f"{v:.0f}%" for v in vals], textposition='auto', textfont=dict(size=9))
    fh.update_layout(barmode='group', margin=dict(l=40, r=20, t=20, b=30),
                     yaxis=dict(title='%', gridcolor='#f0f0f0', range=[0, 60]),
                     legend=dict(orientation='h', y=1.12), plot_bgcolor='#fff', paper_bgcolor='#fff')

    fs = go.Figure()
    if len(cs):
        bsa_v = gf_bsa(team, ori, dst, ori_p, dst_p, week)
        bsa_grp4 = 'origin' if vc == 'origin' else 'dest'
        bsa_v = bsa_v[bsa_v['YYYYMM'] == month].groupby(bsa_grp4)['teu_bsa'].sum()
        mx = cs['cm1'].max() or 1
        for i, (_, r) in enumerate(cs.iterrows()):
            bs = bsa_v.get(r[vc], 0); oc = r['bkg'] / bs * 100 if bs else 0
            fs.add_scatter(x=[oc], y=[r['cm1teu']], mode='markers+text', text=[r[vc]],
                           textposition='top center',
                           marker=dict(size=max(r['cm1'] / mx * 50, 8), color=PAL[i % len(PAL)], opacity=.7),
                           showlegend=False)
        fs.add_hline(y=cs['cm1teu'].median(), line_dash='dot', line_color=C['mt'], opacity=.3)
        fs.add_vline(x=50, line_dash='dot', line_color=C['mt'], opacity=.3)
    fs.update_layout(margin=dict(l=50, r=30, t=20, b=40), xaxis=dict(title='소석률 %', gridcolor='#f0f0f0'),
                     yaxis=dict(title='CM1/TEU', gridcolor='#f0f0f0'), plot_bgcolor='#fff', paper_bgcolor='#fff')
    return fc, fh, fs


# ═══════════════════════════════════════════════════════════
# Tab 5: 부킹 패턴 (WOS 단계별)
# ═══════════════════════════════════════════════════════════
@app.callback(
    [Output('t5-dest-tbl', 'children'), Output('t5-wos-bar', 'figure'), Output('t5-cust-tbl', 'children')],
    GF_INPUTS + [Input('t5-m', 'value'), Input('t5-w', 'value')])
def cb5(team, ori, ori_p, dst, dst_p, view_mode, month, week):
    fd = gf(BKG, team, ori, ori_p, dst, dst_p, month, week)
    vc, vl = get_view(fd, team, view_mode)
    wos_cols = [('WOS-3','w3'), ('WOS-2','w2'), ('WOS-1','w1'), ('Week of Sailing (WOS)','wos')]

    # 지역별 WOS 분포
    dest_rows = []
    for it in sorted(fd[vc].unique()):
        d = fd[fd[vc] == it]
        total = d['fst'].sum()
        if total == 0: continue
        row = {vl: it, '전체BKG': f"{total:,.0f}"}
        for lbl, lt in wos_cols:
            sub = d[d['Lead_time (BKG_Sche)'] == lbl]
            v = sub['fst'].sum()
            short = lbl.replace('Week of Sailing (WOS)', 'WOS')
            row[short] = f"{v:,.0f}"
            row[f'{short}%'] = f"{v/total*100:.0f}%" if total else '-'
        dest_rows.append(row)
    dest_rows.sort(key=lambda r: -float(r['전체BKG'].replace(',','')))

    # 화주별 WOS 분포
    cust_rows = []
    if 'Salesman_POR' in fd.columns:
        sm_map = fd.groupby('BKG_SHPR_CST_ENM')['Salesman_POR'].agg(
            lambda x: ', '.join(sorted(set(x.dropna().astype(str)) - {'','nan'}))).to_dict()
    else:
        sm_map = {}
    for it in sorted(fd['BKG_SHPR_CST_ENM'].unique()):
        d = fd[fd['BKG_SHPR_CST_ENM'] == it]
        total = d['fst'].sum()
        if total == 0: continue
        row = {'화주': it, '영업사원': sm_map.get(it, ''), '전체BKG': f"{total:,.0f}"}
        for lbl, lt in wos_cols:
            sub = d[d['Lead_time (BKG_Sche)'] == lbl]
            v = sub['fst'].sum()
            short = lbl.replace('Week of Sailing (WOS)', 'WOS')
            row[short] = f"{v:,.0f}"
            row[f'{short}%'] = f"{v/total*100:.0f}%" if total else '-'
        cust_rows.append(row)
    cust_rows.sort(key=lambda r: -float(r['전체BKG'].replace(',','')))
    cust_rows = cust_rows[:30]

    # Stacked bar chart
    top10 = dest_rows[:10]
    fig = go.Figure()
    colors = ['#1a73e8', '#f9ab00', '#ea4335', '#5f6368']
    for i, (lbl, _) in enumerate(wos_cols):
        short = lbl.replace('Week of Sailing (WOS)', 'WOS')
        fig.add_bar(x=[r[vl] for r in top10],
                    y=[float(r[short].replace(',','')) for r in top10],
                    name=short, marker_color=colors[i])
    fig.update_layout(barmode='stack', margin=dict(l=40, r=20, t=20, b=30),
                      yaxis=dict(title='TEU', gridcolor='#f0f0f0'),
                      legend=dict(orientation='h', y=1.1), plot_bgcolor='#fff', paper_bgcolor='#fff')

    return mk_tbl(dest_rows[:15], vl), fig, mk_tbl(cust_rows, '화주')


if __name__ == '__main__':
    import webbrowser
    from threading import Timer
    port = int(os.environ.get('PORT', 8071))
    Timer(2, lambda: webbrowser.open(f'http://localhost:{port}')).start()
    print(f"\n  Dashboard: http://localhost:{port}\n")
    server.run(debug=False, host='0.0.0.0', port=port)
