"""
-3W Booking Dashboard: Daily Automation
- Tableau에서 1.csv, 2.csv, BSA raw 다운로드
- booking snapshot 처리 (수식 계산, -3W 필터, 고/저 분류)
- output/ 폴더에 날짜별 결과 저장
"""
import sys, re, os, io, csv, json, time, warnings
import pandas as pd
import openpyxl
import requests, urllib3
from datetime import datetime, timedelta
from pathlib import Path

warnings.filterwarnings('ignore')
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
sys.stdout.reconfigure(encoding='utf-8')

# Load .env if exists
_env = Path(__file__).parent / '.env'
if _env.exists():
    for line in _env.read_text().splitlines():
        if '=' in line and not line.startswith('#'):
            k, v = line.split('=', 1)
            os.environ.setdefault(k.strip(), v.strip())

# ═══════════════════════════════════════════════════════════
# Configuration
# ═══════════════════════════════════════════════════════════
WORK_DIR = Path(os.environ.get('WORK_DIR', r'C:\Users\JKPARK\OneDrive\Documents\Claude\-3W bkg dashboard'))
TABLEAU_SERVER = os.environ.get('TABLEAU_SERVER', 'https://tableau.ekmtc.com')
TABLEAU_USER = os.environ.get('TABLEAU_USER', 'obt')
TABLEAU_PASS = os.environ.get('TABLEAU_PASS', '')

# Workbook: booking snapshot(전체) - contentUrl
BKG_WB_CONTENT_URL = 'bookingsnapshot'
BKG_WB_ID = '81c076dd-4666-488e-96eb-699612d9e109'
# BSA raw (월간회의3주전)
BSA_VIEW_URL = 'Q_17363223877520/BSArawBKGpattern'

# Filter settings
BKG_SCHEDULE_START = '2025-12-28 00:00:00'  # View 1 min date
TEMP_WB_NAME = 'temp_bkg_snapshot_v2'
TEMP_WB_PROJECT_ID = '3d94d4a3-1b23-4e39-8c9c-4a3b765c140d'  # OBT AI AGENT

TODAY_STR = datetime.now().strftime('%Y%m%d')


# ═══════════════════════════════════════════════════════════
# Phase 1: Tableau Download
# ═══════════════════════════════════════════════════════════
def tableau_rest_api():
    """REST API helper: sign in and return (session, api_ver, site_id)"""
    s = requests.Session()
    s.verify = False
    resp = s.get(f'{TABLEAU_SERVER}/api/2.4/serverinfo',
                 headers={'Accept': 'application/json'}, timeout=15)
    api_ver = resp.json()['serverInfo']['restApiVersion']
    resp = s.post(
        f'{TABLEAU_SERVER}/api/{api_ver}/auth/signin',
        json={'credentials': {'name': TABLEAU_USER, 'password': TABLEAU_PASS,
                               'site': {'contentUrl': ''}}},
        headers={'Content-Type': 'application/json', 'Accept': 'application/json'},
        timeout=30)
    data = resp.json()['credentials']
    s.headers['X-Tableau-Auth'] = data['token']
    site_id = data['site']['id']
    return s, api_ver, site_id


def ensure_temp_workbook(s, api_ver, site_id):
    """Download original TWB, modify filter, publish as temp workbook."""
    import xml.etree.ElementTree as ET

    # Check if temp workbook exists (search by name — contentUrl may have suffix)
    resp = s.get(
        f'{TABLEAU_SERVER}/api/{api_ver}/sites/{site_id}/workbooks',
        params={'filter': f'name:eq:{TEMP_WB_NAME}'},
        headers={'Accept': 'application/json'}, timeout=30)
    wbs = resp.json().get('workbooks', {}).get('workbook', [])

    if wbs:
        # Verify filter is correct
        wb_id = wbs[0]['id']
        actual_content_url = wbs[0].get('contentUrl', TEMP_WB_NAME)
        resp = s.get(f'{TABLEAU_SERVER}/api/{api_ver}/sites/{site_id}/workbooks/{wb_id}/content',
                     timeout=120)
        tree = ET.parse(io.BytesIO(resp.content))
        for f in tree.getroot().iter('filter'):
            if 'Calculation_0356804709482497' in f.get('column', ''):
                min_el = f.find('min')
                if min_el is not None and BKG_SCHEDULE_START in (min_el.text or ''):
                    print(f"  Temp workbook exists with correct filter")
                    return actual_content_url

        # Filter wrong, delete and re-create
        print(f"  Temp workbook filter outdated, re-publishing...")
        s.delete(f'{TABLEAU_SERVER}/api/{api_ver}/sites/{site_id}/workbooks/{wb_id}', timeout=60)
        time.sleep(3)

    # Download original TWB (may be .twbx zip format)
    print(f"  Downloading original TWB...")
    resp = s.get(f'{TABLEAU_SERVER}/api/{api_ver}/sites/{site_id}/workbooks/{BKG_WB_ID}/content',
                 timeout=120)
    content = resp.content
    # Handle .twbx (zip) format
    if content[:2] == b'PK':
        import zipfile
        with zipfile.ZipFile(io.BytesIO(content)) as z:
            twb_name = [n for n in z.namelist() if n.endswith('.twb')][0]
            content = z.read(twb_name)
    tree = ET.parse(io.BytesIO(content))

    # Modify Booking_schedule filter min
    for f in tree.getroot().iter('filter'):
        col = f.get('column', '')
        if 'Calculation_0356804709482497' in col and f.get('class') == 'quantitative':
            min_el = f.find('min')
            if min_el is not None:
                min_el.text = f'#{BKG_SCHEDULE_START}#'
                min_el.attrib.clear()

    twb_bytes = io.BytesIO()
    tree.write(twb_bytes, encoding='utf-8', xml_declaration=True)
    twb_content = twb_bytes.getvalue()

    # Publish
    print(f"  Publishing temp workbook...")
    boundary = '----TableauBoundary'
    payload = (
        f'--{boundary}\r\nContent-Disposition: name="request_payload"\r\n'
        f'Content-Type: text/xml\r\n\r\n'
        f'<tsRequest><workbook name="{TEMP_WB_NAME}" showTabs="true">'
        f'<project id="{TEMP_WB_PROJECT_ID}"/></workbook></tsRequest>\r\n'
        f'--{boundary}\r\nContent-Disposition: name="tableau_workbook"; '
        f'filename="{TEMP_WB_NAME}.twb"\r\nContent-Type: application/xml\r\n\r\n'
    ).encode('utf-8') + twb_content + f'\r\n--{boundary}--\r\n'.encode('utf-8')

    actual_content_url = TEMP_WB_NAME
    try:
        resp = s.post(
            f'{TABLEAU_SERVER}/api/{api_ver}/sites/{site_id}/workbooks',
            params={'overwrite': 'true'}, data=payload,
            headers={'Content-Type': f'multipart/mixed; boundary={boundary}'},
            timeout=600)
        if resp.status_code in (200, 201):
            print(f"  Published successfully")
            # Extract actual contentUrl from response (Tableau may append suffix)
            try:
                import xml.etree.ElementTree as ET2
                pub_tree = ET2.fromstring(resp.content)
                ns = {'t': 'http://tableau.com/api'}
                wb_el = pub_tree.find('.//t:workbook', ns) or pub_tree.find('.//workbook')
                if wb_el is not None:
                    actual_content_url = wb_el.get('contentUrl', TEMP_WB_NAME)
            except Exception:
                pass
    except requests.exceptions.ReadTimeout:
        print(f"  Publish timed out (likely succeeded)")
        time.sleep(5)

    # Fallback: query by name to get actual contentUrl
    if actual_content_url == TEMP_WB_NAME:
        resp = s.get(
            f'{TABLEAU_SERVER}/api/{api_ver}/sites/{site_id}/workbooks',
            params={'filter': f'name:eq:{TEMP_WB_NAME}'},
            headers={'Accept': 'application/json'}, timeout=30)
        found = resp.json().get('workbooks', {}).get('workbook', [])
        if found:
            actual_content_url = found[0].get('contentUrl', TEMP_WB_NAME)

    return actual_content_url


def download_csv_from_tableau(content_url, view_name, save_path, vf_params=None):
    """Download CSV from Tableau view using Playwright JS navigation."""
    from playwright.sync_api import sync_playwright

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(viewport={'width': 1920, 'height': 1080},
                                  ignore_https_errors=True, accept_downloads=True)
        page = ctx.new_page()

        # Login
        page.goto(f'{TABLEAU_SERVER}/#/signin', wait_until='networkidle', timeout=30000)
        time.sleep(3)
        page.fill('input[name="username"]', TABLEAU_USER)
        page.fill('input[name="password"]', TABLEAU_PASS)
        page.click('button[type="submit"]')
        try:
            page.wait_for_url('**/#/home**', timeout=15000)
        except Exception:
            pass
        time.sleep(3)

        # Load embed view to establish Tableau session
        page.goto(f'{TABLEAU_SERVER}/views/{content_url}/{view_name}?:embed=y&:showVizHome=n',
                  timeout=120000)
        time.sleep(15)

        # Download CSV via JS navigation (avoids redirect issues)
        csv_url = f'{TABLEAU_SERVER}/views/{content_url}/{view_name}.csv'
        if vf_params:
            csv_url += '?' + '&'.join(f'vf_{k}={v}' for k, v in vf_params.items())

        with page.expect_download(timeout=1800000) as dl_info:
            page.evaluate(f'window.location.href = "{csv_url}"')
        download = dl_info.value
        download.save_as(str(save_path))

        browser.close()
    return os.path.getsize(save_path)


def download_all():
    """Phase 1: Download all data from Tableau."""
    os.chdir(WORK_DIR)
    s, api_ver, site_id = tableau_rest_api()

    # 1. Ensure temp workbook with correct filter
    print("[1/3] Ensuring temp workbook...")
    wb_url = ensure_temp_workbook(s, api_ver, site_id)
    s.post(f'{TABLEAU_SERVER}/api/{api_ver}/auth/signout', timeout=10)

    # 2. Download View 1 (1.csv)
    print("[2/3] Downloading View 1 (1.csv)...")
    size = download_csv_from_tableau(wb_url, '1', WORK_DIR / '1_raw.csv')
    # Convert to UTF-16 tab-separated
    rows = []
    with open(WORK_DIR / '1_raw.csv', 'r', encoding='utf-8-sig') as f:
        for row in csv.reader(f):
            rows.append(row)
    with open(WORK_DIR / '1.csv', 'w', encoding='utf-16', newline='') as f:
        writer = csv.writer(f, delimiter='\t')
        for row in rows:
            writer.writerow(row)
    os.remove(WORK_DIR / '1_raw.csv')
    print(f"  1.csv: {os.path.getsize(WORK_DIR / '1.csv'):,} bytes ({len(rows)-1:,} rows)")

    # 3. Download View 2 (2.csv)
    print("[3/3] Downloading View 2 (2.csv)...")
    size = download_csv_from_tableau(wb_url, '2', WORK_DIR / '2_raw.csv')
    rows = []
    with open(WORK_DIR / '2_raw.csv', 'r', encoding='utf-8-sig') as f:
        for row in csv.reader(f):
            rows.append(row)
    with open(WORK_DIR / '2.csv', 'w', encoding='utf-16', newline='') as f:
        writer = csv.writer(f, delimiter='\t')
        for row in rows:
            writer.writerow(row)
    os.remove(WORK_DIR / '2_raw.csv')
    print(f"  2.csv: {os.path.getsize(WORK_DIR / '2.csv'):,} bytes ({len(rows)-1:,} rows)")


def download_bsa():
    """Download BSA raw (월간회의3주전) with YYYY filter."""
    print("[BSA] Downloading BSA raw...")
    year = datetime.now().year
    yyyy_filter = f'{year-1},{year},{year+1}'

    from playwright.sync_api import sync_playwright
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(viewport={'width': 1920, 'height': 1080},
                                  ignore_https_errors=True, accept_downloads=True)
        page = ctx.new_page()
        page.goto(f'{TABLEAU_SERVER}/#/signin', wait_until='networkidle', timeout=30000)
        time.sleep(3)
        page.fill('input[name="username"]', TABLEAU_USER)
        page.fill('input[name="password"]', TABLEAU_PASS)
        page.click('button[type="submit"]')
        try:
            page.wait_for_url('**/#/home**', timeout=15000)
        except Exception:
            pass
        time.sleep(3)

        page.goto(f'{TABLEAU_SERVER}/views/{BSA_VIEW_URL}?:embed=y&:showVizHome=n',
                  timeout=120000)
        time.sleep(15)

        # Download BSA per team via Playwright JS navigation
        import pandas as pd
        all_dfs = []
        for team in ['OBT', 'EST', 'IST', 'JBT']:
            csv_url = (f'{TABLEAU_SERVER}/views/{BSA_VIEW_URL}.csv'
                       f'?vf_YYYY={yyyy_filter}&Sales+Team={team}')
            print(f"  Downloading BSA: {team}...", end=' ')
            with page.expect_download(timeout=600000) as dl_info:
                page.evaluate(f'window.location.href = "{csv_url}"')
            download = dl_info.value
            tmp_path = download.path()
            df = pd.read_csv(tmp_path, dtype=str)
            df['team'] = team
            print(f"{len(df)} rows")
            all_dfs.append(df)
        combined = pd.concat(all_dfs, ignore_index=True)

        out_dir = WORK_DIR / 'output'
        out_dir.mkdir(exist_ok=True)
        out_path = out_dir / f'BSA_raw_monthly3W_{TODAY_STR}.csv'
        combined.to_csv(out_path, index=False)

        browser.close()

    print(f"  {out_path.name}: {os.path.getsize(out_path):,} bytes")


# ═══════════════════════════════════════════════════════════
# Phase 2: Booking Snapshot Processing
# ═══════════════════════════════════════════════════════════
def process_snapshot():
    """Process 1.csv + 2.csv + template -> booking_snapshot_result.xlsx"""
    os.chdir(WORK_DIR)

    # --- Load reference sheets ---
    print("[Process] Loading reference sheets...")
    wb = openpyxl.load_workbook('booking snapshot.xlsx', data_only=True)

    grade_lookup = {}
    for row in wb['grade'].iter_rows(min_row=2, values_only=True):
        if row[0] is not None:
            grade_lookup[str(row[0]).strip()] = str(row[2]).strip() if row[2] else 'C+D'

    week_month_lookup = {}
    for row in wb['\uc8fc\ucc28 \uc6d4'].iter_rows(min_row=2, values_only=True):
        if row[1] is not None:
            key = row[1]
            val = str(row[2]) if row[2] else ''
            key_str = key.strftime('%Y-%m-%d') if isinstance(key, datetime) else str(key).strip()
            week_month_lookup[key_str] = val

    wb.close()
    print(f"  grade: {len(grade_lookup)}, 주차월: {len(week_month_lookup)}")

    # --- Read CSV data ---
    print("[Process] Reading CSV files...")
    df1 = pd.read_csv('1.csv', encoding='utf-16', sep='\t', dtype=str)
    df2 = pd.read_csv('2.csv', encoding='utf-16', sep='\t', dtype=str)
    df1.columns = [re.sub(r'[^\x00-\x7F]+$', '', c).strip() for c in df1.columns]
    df2.columns = [re.sub(r'[^\x00-\x7F]+$', '', c).strip() for c in df2.columns]

    df2_dedup = df2.drop_duplicates(subset='BKG_NO', keep='first').set_index('BKG_NO')
    print(f"  1.csv: {len(df1):,}, 2.csv: {len(df2):,}, addon unique: {len(df2_dedup):,}")

    addon_col_map = {
        'B': 'LST_Route', 'C': 'LST_VSL', 'D': 'LST_VOY',
        'G': 'Date_vsl', 'I': 'week_start_date',
        'J': 'Booking_status', 'K': 'CM1_Booking', 'L': 'LST_TEU',
        'SM': 'Salesman_POR',
    }

    def addon_xlookup(bkg_no, col_letter):
        col_name = addon_col_map.get(col_letter, '')
        if not col_name or bkg_no not in df2_dedup.index:
            return None
        try:
            val = df2_dedup.loc[bkg_no, col_name]
            return str(val) if pd.notna(val) else None
        except:
            return None

    # --- Compute formulas ---
    print("[Process] Computing formulas...")
    output = df1.copy()

    def parse_korean_date(s):
        if pd.isna(s) or str(s).strip() in ('', 'nan'):
            return pd.NaT
        m = re.match(r'(\d{4})\D+(\d{1,2})\D+(\d{1,2})', str(s))
        return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3))) if m else pd.NaT

    date_N = output['Booking_date'].apply(parse_korean_date)
    date_O = output['Booking_schedule'].apply(parse_korean_date)
    total = len(output)
    bkg_nos = output['BKG_NO'].values
    shpr_codes = output['BKG_SHPR_CST_NO'].values
    dly_ctrs = output['DLY_CTR_CD'].values

    # Bulk lookups
    addon = {}
    for letter in ('G', 'J', 'K', 'L', 'B', 'C', 'D', 'SM'):
        addon[letter] = pd.Series([addon_xlookup(b, letter) for b in bkg_nos], dtype=object)

    output['Actual_Departure_schedule'] = addon['G'].where(addon['G'].notna(), output['Booking_schedule']).values
    output['LST_Status'] = addon['J'].fillna('').values
    output['CM1'] = addon['K'].fillna('').values
    output['LST_TEU'] = addon['L'].fillna('').values
    output['LST_route'] = addon['B'].fillna('').values
    output['LST_VSL'] = addon['C'].fillna('').values
    output['LST_VOY'] = addon['D'].fillna('').values
    output['Salesman_POR'] = addon['SM'].fillna('').values

    # week_start_date: Date_vsl(=Actual_Departure_schedule) 기준 일요일로 재계산
    print("  Computing week_start_date (from Date_vsl)...")
    date_R = output['Actual_Departure_schedule'].apply(parse_korean_date)
    output['week_start_date'] = [
        (d - timedelta(days=(d.isoweekday() % 7))).strftime('%Y\ub144 %m\uc6d4 %d\uc77c')
        if pd.notna(d) else '' for d in date_R]

    # Lead_time(Booking)
    diff_ON = (date_O - date_N).dt.days
    output['Lead_time(Booking)'] = ['1W' if d <= 7 else '2W' if d <= 14 else '3W' if d <= 21 else '4W'
                                     if pd.notna(d) else '' for d in diff_ON]

    # Lead_time(Actual)
    date_AC = date_R.apply(lambda d: d - timedelta(days=(d.isoweekday() % 7)) if pd.notna(d) else pd.NaT)
    diff_AC_N = (date_AC - date_N).dt.days
    output['Lead_time(Actual)'] = [
        'Week of Sailing (WOS)' if d < 1 else 'WOS-1' if d <= 7 else 'WOS-2' if d <= 14 else 'WOS-3'
        if pd.notna(d) else '' for d in diff_AC_N]

    # grade
    output['grade'] = [grade_lookup.get(str(s).strip(), 'C+D') if pd.notna(s) else 'C+D' for s in shpr_codes]

    # CM1/TEU
    # CM1, LST_TEU 콤마 제거 (Tableau CSV에서 "1,674" 형식)
    output['CM1'] = output['CM1'].apply(lambda x: str(x).replace(',', '') if pd.notna(x) else '')
    output['LST_TEU'] = output['LST_TEU'].apply(lambda x: str(x).replace(',', '') if pd.notna(x) else '')

    def safe_div(cm1, teu):
        try:
            c = float(cm1) if cm1 and cm1 not in ('', 'nan') else None
            t = float(teu) if teu and teu not in ('', 'nan', '0') else None
            return round(c / t, 2) if c is not None and t and t != 0 else ''
        except:
            return ''
    output['CM1/TEU'] = [safe_div(c, t) for c, t in zip(output['CM1'], output['LST_TEU'])]

    # D_group
    ae_countries = {'AE', 'SA', 'KW', 'QA', 'OM', 'BH', 'IQ', 'JO', 'EG'}
    output['D_group'] = ['MY/SG' if j in ('MY', 'SG') else 'AE' if j in ae_countries else j
                          for j in (str(x).strip() for x in dly_ctrs)]

    # YYYYMM
    def lookup_yyyymm(ws_date):
        s = str(ws_date).strip()
        if s in ('', 'nan'):
            return ''
        dt = parse_korean_date(s)
        if pd.notna(dt):
            key = dt.strftime('%Y-%m-%d')
            if key in week_month_lookup:
                return week_month_lookup[key]
        return week_month_lookup.get(s, '')
    output['YYYYMM'] = [lookup_yyyymm(w) for w in output['week_start_date']]

    # 고/저: POR_PORT + DLY_PORT 루트별 화주 CM1/TEU vs 루트 평균
    print("  Computing 고/저 (루트별 CM1/TEU 평균 대비)...")
    cm1_num = pd.to_numeric(output['CM1'].str.replace(',', ''), errors='coerce').fillna(0)
    teu_num = pd.to_numeric(output['LST_TEU'].str.replace(',', ''), errors='coerce').fillna(0)
    status_str = output['LST_Status'].astype(str).str.strip()
    # Normal + CM1 있는 건만 대상으로 루트 평균 및 화주별 CM1/TEU 계산
    mask = (status_str == 'Normal') & (cm1_num != 0) & (teu_num > 0)
    calc_df = pd.DataFrame({
        'shpr': output['BKG_SHPR_CST_NO'], 'por': output['POR_PLC_CD'],
        'dly': output['DLY_PLC_CD'], 'cm1': cm1_num, 'teu': teu_num, 'mask': mask
    })
    valid = calc_df[calc_df['mask']]
    # 루트 평균
    route_agg = valid.groupby(['por', 'dly']).agg(r_cm1=('cm1', 'sum'), r_teu=('teu', 'sum')).reset_index()
    route_agg['r_avg'] = route_agg['r_cm1'] / route_agg['r_teu']
    # 화주-루트별 CM1/TEU
    shpr_agg = valid.groupby(['shpr', 'por', 'dly']).agg(s_cm1=('cm1', 'sum'), s_teu=('teu', 'sum')).reset_index()
    shpr_agg['s_avg'] = shpr_agg['s_cm1'] / shpr_agg['s_teu']
    shpr_agg = shpr_agg.merge(route_agg[['por', 'dly', 'r_avg']], on=['por', 'dly'])
    shpr_agg['pt'] = shpr_agg.apply(lambda r: '고수익화주' if r['s_avg'] >= r['r_avg'] else '저수익화주', axis=1)
    # 룩업 딕셔너리
    pt_lookup = {(r['shpr'], r['por'], r['dly']): r['pt'] for _, r in shpr_agg.iterrows()}
    output['\uace0/\uc800'] = [
        pt_lookup.get((str(s).strip(), str(p).strip(), str(d).strip()), '')
        for s, p, d in zip(output['BKG_SHPR_CST_NO'], output['POR_PLC_CD'], output['DLY_PLC_CD'])]
    hi_cnt = sum(1 for v in output['\uace0/\uc800'] if v == '고수익화주')
    lo_cnt = sum(1 for v in output['\uace0/\uc800'] if v == '저수익화주')

    # 전월 기준 선적지별 고수익화주 태그
    print("  Computing 고수익태그 (전월 기준)...")
    # 월별 선적지별 평균 CM1/TEU 및 화주별 CM1/TEU
    calc_df2 = pd.DataFrame({
        'shpr': output['BKG_SHPR_CST_NO'], 'por': output['POR_PLC_CD'],
        'yyyymm': output['YYYYMM'], 'cm1': cm1_num, 'teu': teu_num,
        'mask': mask  # Normal & cm1!=0 & teu>0
    })
    valid2 = calc_df2[calc_df2['mask']]
    # 월별 선적지 평균
    por_month_avg = valid2.groupby(['por', 'yyyymm']).agg(
        p_cm1=('cm1', 'sum'), p_teu=('teu', 'sum')).reset_index()
    por_month_avg['p_avg'] = por_month_avg['p_cm1'] / por_month_avg['p_teu']
    # 월별 선적지별 화주 CM1/TEU
    shpr_por_month = valid2.groupby(['shpr', 'por', 'yyyymm']).agg(
        s_cm1=('cm1', 'sum'), s_teu=('teu', 'sum')).reset_index()
    shpr_por_month['s_avg'] = shpr_por_month['s_cm1'] / shpr_por_month['s_teu']
    shpr_por_month = shpr_por_month.merge(por_month_avg[['por', 'yyyymm', 'p_avg']], on=['por', 'yyyymm'])
    shpr_por_month['tag'] = shpr_por_month.apply(
        lambda r: '고수익' if r['s_avg'] >= r['p_avg'] else '저수익', axis=1)

    # 월 목록 정렬
    all_months = sorted(output['YYYYMM'].dropna().unique())
    all_months = [m for m in all_months if m]

    # 전월 태그 룩업: 해당 월의 전월 데이터, 없으면 가장 최근월
    def get_prev_tag(shpr_code, por_code, cur_month):
        if not cur_month or not shpr_code:
            return ''
        # 전월 계산
        y, m = int(cur_month[:4]), int(cur_month[4:])
        prev_months = []
        for i in range(1, 7):  # 최대 6개월 전까지
            pm = m - i
            py = y
            while pm <= 0:
                pm += 12
                py -= 1
            prev_months.append(f'{py}{pm:02d}')

        # 전월부터 순서대로 찾기
        for pm in prev_months:
            matches = shpr_por_month[
                (shpr_por_month['shpr'] == shpr_code) &
                (shpr_por_month['por'] == por_code) &
                (shpr_por_month['yyyymm'] == pm)]
            if len(matches) > 0:
                return matches.iloc[0]['tag']
        return ''

    # 성능을 위해 딕셔너리로 변환
    tag_dict = {}
    for _, r in shpr_por_month.iterrows():
        key = (r['shpr'], r['por'], r['yyyymm'])
        tag_dict[key] = r['tag']

    def get_prev_tag_fast(shpr_code, por_code, cur_month):
        if not cur_month or not shpr_code:
            return ''
        y, m = int(cur_month[:4]), int(cur_month[4:])
        for i in range(1, 7):
            pm = m - i
            py = y
            while pm <= 0:
                pm += 12
                py -= 1
            t = tag_dict.get((str(shpr_code).strip(), str(por_code).strip(), f'{py}{pm:02d}'), None)
            if t is not None:
                return t
        return ''

    output['고수익태그'] = [
        get_prev_tag_fast(s, p, m)
        for s, p, m in zip(output['BKG_SHPR_CST_NO'], output['POR_PLC_CD'], output['YYYYMM'])]
    hi_tag = sum(1 for v in output['고수익태그'] if v == '고수익')
    lo_tag = sum(1 for v in output['고수익태그'] if v == '저수익')
    print(f"  고수익태그: 고수익={hi_tag:,}, 저수익={lo_tag:,}, 미분류={len(output)-hi_tag-lo_tag:,}")
    print(f"  고수익: {hi_cnt:,}, 저수익: {lo_cnt:,}, 미분류: {len(output)-hi_cnt-lo_cnt:,}")

    # week_start (BKG_Sche): =INT(O2)-WEEKDAY(O2,1)+1  (Booking_schedule 기준 주 시작 일요일)
    print("  Computing week_start (BKG_Sche)...")
    def calc_week_start_bkg_sche(bkg_sche_str):
        dt = parse_korean_date(bkg_sche_str)
        if pd.isna(dt):
            return ''
        # Excel WEEKDAY(,1): Sun=1..Sat=7 → Python isoweekday: Mon=1..Sun=7
        # week_start(Sunday) = date - (isoweekday % 7) days
        sunday = dt - timedelta(days=(dt.isoweekday() % 7))
        return sunday.strftime('%Y-%m-%d')
    output['week_start (BKG_Sche)'] = [calc_week_start_bkg_sche(s) for s in output['Booking_schedule']]

    # Lead_time (BKG_Sche): =IF(AG2-N2<1,"WOS", <=7:"WOS-1", <=14:"WOS-2", else:"WOS-3")
    print("  Computing Lead_time (BKG_Sche)...")
    def calc_leadtime_bkg_sche(ws_str, bkg_date_str):
        if not ws_str or ws_str == '':
            return ''
        dt_ws = parse_korean_date(ws_str) if '\ub144' in str(ws_str) else pd.NaT
        if pd.isna(dt_ws):
            try:
                dt_ws = datetime.strptime(str(ws_str).strip(), '%Y-%m-%d')
            except:
                return ''
        dt_bkg = parse_korean_date(bkg_date_str)
        if pd.isna(dt_ws) or pd.isna(dt_bkg):
            return ''
        diff = (dt_ws - dt_bkg).days
        if diff < 1:
            return 'Week of Sailing (WOS)'
        elif diff <= 7:
            return 'WOS-1'
        elif diff <= 14:
            return 'WOS-2'
        else:
            return 'WOS-3'
    output['Lead_time (BKG_Sche)'] = [
        calc_leadtime_bkg_sche(w, b)
        for w, b in zip(output['week_start (BKG_Sche)'], output['Booking_date'])]

    # YYYYMM (BKG_Sche): week_start (BKG_Sche) 기준 월 매핑
    print("  Computing YYYYMM (BKG_Sche)...")
    def lookup_yyyymm_bkg_sche(ws_str):
        s = str(ws_str).strip()
        if s in ('', 'nan'):
            return ''
        # ws_str is YYYY-MM-DD format
        if s in week_month_lookup:
            return week_month_lookup[s]
        # Try parsing as date
        try:
            from datetime import datetime as _dt
            dt = _dt.strptime(s, '%Y-%m-%d')
            return week_month_lookup.get(dt.strftime('%Y-%m-%d'), '')
        except:
            return ''
    output['YYYYMM_BKG_Sche'] = [lookup_yyyymm_bkg_sche(w) for w in output['week_start (BKG_Sche)']]

    # Reorder columns
    col_order = [
        'BKG_NO', 'BKG_SHPR_CST_NO', 'BKG_SHPR_CST_ENM',
        'POR_CTR_CD', 'POR_PLC_CD', 'POL_CTR_CD', 'POL_PORT_CD',
        'POD_CTR_CD', 'POD_PORT_CD', 'DLY_CTR_CD', 'DLY_PLC_CD',
        'VSL_CD', 'VOY_NO', 'Booking_date', 'Booking_schedule',
        'Cancel_date', 'FST_TEU',
        'Actual_Departure_schedule', 'LST_Status', 'CM1', 'LST_TEU',
        'Lead_time(Booking)', 'Lead_time(Actual)', 'LST_route',
        'LST_VSL', 'LST_VOY', 'grade', 'CM1/TEU',
        'week_start_date', 'D_group', 'YYYYMM', '\uace0/\uc800',
        'week_start (BKG_Sche)', 'Lead_time (BKG_Sche)', 'YYYYMM_BKG_Sche',
        'Salesman_POR', '고수익태그'
    ]
    output = output[col_order]

    # --- Filters ---
    print("[Process] Filtering...")
    output = output[output['LST_Status'].astype(str).str.strip() != ''].reset_index(drop=True)
    print(f"  After LST_Status filter: {len(output):,}")

    # -3W snapshot filter: =IF(AND(P-N<=3,S="Cancel"),"제외",IF(AND(R-N>=21,S="Cancel",P-N<=7),"제외","대상"))
    date_N_f = output['Booking_date'].apply(parse_korean_date)
    date_P_f = output['Cancel_date'].apply(parse_korean_date)
    date_R_f = output['Actual_Departure_schedule'].apply(parse_korean_date)
    status_f = output['LST_Status'].astype(str).str.strip()
    is_cancel = status_f == 'Cancel'
    diff_PN = (date_P_f - date_N_f).dt.days  # Cancel_date - Booking_date
    diff_RN = (date_R_f - date_N_f).dt.days  # Actual_Departure - Booking_date

    # 조건1: 즉시 캔슬 (부킹 후 3일 이내 캔슬)
    cond1 = is_cancel & date_P_f.notna() & date_N_f.notna() & (diff_PN <= 3)
    # 조건2: 조기 부킹 후 빠른 캔슬 (출항 21일+ 전에 부킹했으나 7일 이내 캔슬)
    cond2 = is_cancel & date_R_f.notna() & date_N_f.notna() & date_P_f.notna() & (diff_RN >= 21) & (diff_PN <= 7)
    exclude = cond1 | cond2

    print(f"  조건1 즉시캔슬 (P-N<=3, Cancel): {cond1.sum():,}")
    print(f"  조건2 조기부킹캔슬 (R-N>=21, Cancel, P-N<=7): {cond2.sum():,}")
    print(f"  중복제외: {(cond1 & cond2).sum():,}")
    output = output[~exclude].reset_index(drop=True)
    print(f"  Final (대상 only): {len(output):,}")

    # --- Save ---
    out_dir = WORK_DIR / 'output'
    out_dir.mkdir(exist_ok=True)
    out_path = out_dir / f'booking_snapshot_result_{TODAY_STR}.xlsx'
    print(f"[Process] Saving {out_path.name}...")
    with pd.ExcelWriter(str(out_path), engine='openpyxl') as writer:
        output.to_excel(writer, sheet_name='raw', index=False)
    print(f"  {out_path.name}: {os.path.getsize(out_path):,} bytes, {len(output):,} rows")


# ═══════════════════════════════════════════════════════════
# Phase 3: Google Drive Upload
# ═══════════════════════════════════════════════════════════
GDRIVE_FOLDER_ID = '1JIxg6Y-_gRfI1HueXZ1Q9j4-Z5bxvNgv'
GDRIVE_CREDS_DIR = Path(r'C:\Users\JKPARK\OneDrive\Documents\Claude\.gdrive-mcp')

def upload_to_gdrive():
    """Upload parquet cache + BSA CSV to Google Drive for web dashboard."""
    print("[Upload] Building summary JSON + uploading to Google Drive...")
    import json as _json

    # --- Build aggregated JSON for static dashboard ---
    out_dir = WORK_DIR / 'output'
    bf = sorted(out_dir.glob('booking_snapshot_result_*.xlsx'), key=os.path.getmtime, reverse=True)
    sf = sorted(out_dir.glob('BSA_raw_monthly3W_*.csv'), key=os.path.getmtime, reverse=True)
    cache = sorted(out_dir.glob('_cache_*.parquet'), key=os.path.getmtime, reverse=True)

    if cache:
        bkg = pd.read_parquet(cache[0])
    elif bf:
        bkg = pd.read_excel(bf[0], sheet_name='raw', dtype=str)
        bkg = bkg.rename(columns={'\uace0/\uc800': 'profit_type'})
        for c in ['FST_TEU','LST_TEU','CM1']:
            bkg[c] = bkg[c].astype(str).str.replace(',','')
        bkg['fst'] = pd.to_numeric(bkg['FST_TEU'], errors='coerce').fillna(0)
        bkg['lst'] = pd.to_numeric(bkg['LST_TEU'], errors='coerce').fillna(0)
        bkg['cm1v'] = pd.to_numeric(bkg['CM1'], errors='coerce').fillna(0)
    else:
        print("  No data to aggregate, skipping JSON build")
        return

    # Ensure derived columns
    if 'profit_type' not in bkg.columns and '\uace0/\uc800' in bkg.columns:
        bkg = bkg.rename(columns={'\uace0/\uc800': 'profit_type'})
    if 'dest' not in bkg.columns:
        bkg['dest'] = bkg['DLY_CTR_CD']
        bkg['origin'] = bkg['POR_CTR_CD']
        bkg['ori_port'] = bkg['POR_PLC_CD']
        bkg['dst_port'] = bkg['DLY_PLC_CD']
        def _ct(o,d):
            if o not in ('KR','JP') and d != 'KR': return 'OBT'
            elif o == 'KR' and d != 'JP': return 'EST'
            elif o != 'JP' and d == 'KR': return 'IST'
            else: return 'JBT'
        bkg['team'] = [_ct(o,d) for o,d in zip(bkg['POR_CTR_CD'], bkg['DLY_CTR_CD'])]
    if 'fst' not in bkg.columns:
        bkg['fst'] = pd.to_numeric(bkg.get('FST_TEU','0').astype(str).str.replace(',',''), errors='coerce').fillna(0)
        bkg['lst'] = pd.to_numeric(bkg.get('LST_TEU','0').astype(str).str.replace(',',''), errors='coerce').fillna(0)
        bkg['cm1v'] = pd.to_numeric(bkg.get('CM1','0').astype(str).str.replace(',',''), errors='coerce').fillna(0)

    # YYYYMM = 주차 월 기준 (BSA와 동일, 원본 YYYYMM 사용)

    lt = bkg['Lead_time (BKG_Sche)']
    normal = bkg['LST_Status'] == 'Normal'
    cancel = bkg['LST_Status'] == 'Cancel'
    hi = bkg.get('profit_type','') == '고수익화주'

    bkg['is_normal'] = normal.astype(int)
    bkg['is_cancel'] = cancel.astype(int)
    bkg['is_hi'] = hi.astype(int)
    bkg['norm_fst'] = bkg['fst'] * bkg['is_normal']
    bkg['cm1_norm'] = bkg['cm1v'] * bkg['is_normal'] * (bkg['cm1v'] != 0).astype(int)
    bkg['lst_norm'] = bkg['lst'] * bkg['is_normal'] * (bkg['cm1v'] != 0).astype(int)

    # WOS stage columns
    for wos, label in [('WOS-3','w3'),('WOS-2','w2'),('WOS-1','w1'),('Week of Sailing (WOS)','wos')]:
        mask = (lt == wos).astype(int)
        bkg[f'{label}_fst'] = bkg['fst'] * mask
        bkg[f'{label}_norm_fst'] = bkg['fst'] * mask * bkg['is_normal']
    bkg['w3_canc_fst'] = bkg['fst'] * (lt == 'WOS-3').astype(int) * bkg['is_cancel']
    bkg['w3_hi_fst'] = bkg['fst'] * (lt == 'WOS-3').astype(int) * bkg['is_hi']
    bkg['w3_hi_norm_fst'] = bkg['fst'] * (lt == 'WOS-3').astype(int) * bkg['is_hi'] * bkg['is_normal']

    # Monthly aggregation with ports
    gk = ['team','origin','ori_port','dest','dst_port','YYYYMM']
    agg_cols = {'fst':'sum','norm_fst':'sum',
                'w3_fst':'sum','w3_norm_fst':'sum','w3_canc_fst':'sum','w3_hi_fst':'sum','w3_hi_norm_fst':'sum',
                'w2_fst':'sum','w2_norm_fst':'sum','w1_fst':'sum','w1_norm_fst':'sum','wos_fst':'sum','wos_norm_fst':'sum',
                'cm1_norm':'sum','lst_norm':'sum'}
    monthly = bkg.groupby(gk).agg(agg_cols).reset_index()

    # Weekly aggregation (simplified - no port detail)
    wk_keys = ['team','origin','dest','YYYYMM','week_start_date']
    weekly = bkg.groupby(wk_keys).agg(agg_cols).reset_index()

    # Shipper aggregation (화주별) — BKG > 0인 전체 화주
    shpr_keys = ['team','origin','ori_port','dest','dst_port','YYYYMM','BKG_SHPR_CST_NO','BKG_SHPR_CST_ENM','Salesman_POR','고수익태그']
    shipper = bkg.groupby(shpr_keys).agg(agg_cols).reset_index()
    shipper_all = shipper[shipper['fst'] > 0]
    print(f"    shipper: {len(shipper):,} → active: {len(shipper_all):,} rows")

    # WPM (주차 월 기준)
    import re as _re
    def _pkd(s):
        if pd.isna(s): return None
        m = _re.match(r'(\d{4})\D+(\d{1,2})\D+(\d{1,2})', str(s))
        return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3))) if m else None
    bkg['_wdt'] = bkg['week_start_date'].apply(_pkd)
    wpm = bkg[bkg['_wdt'].notna()].groupby('YYYYMM')['week_start_date'].nunique().to_dict()
    if wpm.get('202601', 0) < 4:
        wpm['202601'] = 4

    # BSA
    bsa_data = []
    if sf:
        bsa = pd.read_csv(sf[0], dtype=str)
        bsa = bsa[bsa['DLY_Country'].str.len() <= 3]
        bsa = bsa[bsa['POR_Country'].str.len() <= 3]
        bsa['teu_bsa'] = pd.to_numeric(bsa['TEU_BSA (Actual)'].str.replace(',',''), errors='coerce').fillna(0)
        bsa_agg = bsa.groupby(['team','POR_Country','POR_PORT','DLY_Country','DLY_PORT','YYYYMM'])['teu_bsa'].sum().reset_index()
        bsa_data = bsa_agg.to_dict('records')

    summary = {
        'data_date': TODAY_STR,
        'wpm': wpm,
        'months': sorted(bkg['YYYYMM'].dropna().unique().tolist()),
        'monthly': monthly.round(1).to_dict('records'),
        'weekly': weekly.round(1).to_dict('records'),
        'shipper': shipper_all.round(1).to_dict('records'),
        'bsa': bsa_data,
    }

    json_path = out_dir / f'dashboard_summary_{TODAY_STR}.json'
    with open(json_path, 'w', encoding='utf-8') as f:
        _json.dump(summary, f, ensure_ascii=False, separators=(',',':'))
    print(f"  Summary JSON: {json_path.name} ({os.path.getsize(json_path):,} bytes)")
    print(f"    monthly: {len(monthly):,} rows, weekly: {len(weekly):,} rows, bsa: {len(bsa_data):,} rows")

    with open(GDRIVE_CREDS_DIR / 'credentials.json') as f:
        creds = _json.load(f)['installed']
    with open(GDRIVE_CREDS_DIR / 'token.json') as f:
        token = _json.load(f)

    resp = requests.post('https://oauth2.googleapis.com/token', data={
        'client_id': creds['client_id'], 'client_secret': creds['client_secret'],
        'refresh_token': token['refresh_token'], 'grant_type': 'refresh_token'})
    at = resp.json()['access_token']
    headers = {'Authorization': f'Bearer {at}'}

    out_dir = WORK_DIR / 'output'

    # Build parquet cache from latest xlsx
    bf = sorted(out_dir.glob('booking_snapshot_result_*.xlsx'), key=os.path.getmtime, reverse=True)
    if bf:
        dd = bf[0].stem.split('_')[-1]
        cache = out_dir / f'_cache_{dd}.parquet'
        if not cache.exists():
            print(f"  Building parquet cache...")
            bkg = pd.read_excel(bf[0], sheet_name='raw', dtype=str)
            bkg.to_parquet(cache, index=False)
        _upload_file(headers, cache, f'_cache_{dd}.parquet')

    # Upload BSA CSV
    sf = sorted(out_dir.glob('BSA_raw_monthly3W_*.csv'), key=os.path.getmtime, reverse=True)
    if sf:
        _upload_file(headers, sf[0], sf[0].name)

    # Upload summary JSON (for static dashboard)
    jf = sorted(out_dir.glob('dashboard_summary_*.json'), key=os.path.getmtime, reverse=True)
    if jf:
        _upload_file(headers, jf[0], 'dashboard_summary.json')
        # Copy to dist/ for GitHub Pages hosting
        dist_dir = WORK_DIR / 'dist'
        if dist_dir.exists():
            import shutil
            shutil.copy2(jf[0], dist_dir / 'data.json')
            print(f"  Copied to dist/data.json")

    print("[Upload] Done.")


def _upload_file(headers, local_path, filename):
    """Upload or update a file in the Drive folder."""
    import json as _json

    # Check if file already exists
    q = f"name='{filename}' and '{GDRIVE_FOLDER_ID}' in parents and trashed=false"
    r = requests.get('https://www.googleapis.com/drive/v3/files',
        headers=headers, params={'q': q, 'fields': 'files(id)'})
    existing = r.json().get('files', [])

    data = open(local_path, 'rb').read()
    size = len(data)

    if existing:
        # Update existing
        fid = existing[0]['id']
        r = requests.patch(f'https://www.googleapis.com/upload/drive/v3/files/{fid}',
            headers={**headers, 'Content-Type': 'application/octet-stream'},
            params={'uploadType': 'media'}, data=data)
        print(f"  Updated: {filename} ({size:,} bytes)")
    else:
        # Create new
        metadata = _json.dumps({'name': filename, 'parents': [GDRIVE_FOLDER_ID]})
        import email.mime.multipart
        boundary = '===boundary==='
        body = (f'--{boundary}\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n'
                f'{metadata}\r\n--{boundary}\r\nContent-Type: application/octet-stream\r\n\r\n').encode()
        body += data + f'\r\n--{boundary}--'.encode()
        r = requests.post('https://www.googleapis.com/upload/drive/v3/files',
            headers={**headers, 'Content-Type': f'multipart/related; boundary={boundary}'},
            params={'uploadType': 'multipart'}, data=body)
        print(f"  Created: {filename} ({size:,} bytes)")


# ═══════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════
def main():
    start = time.time()
    print(f"{'='*60}")
    print(f"-3W Booking Dashboard - {datetime.now():%Y-%m-%d %H:%M:%S}")
    print(f"{'='*60}")

    print("\n--- Phase 1: Tableau Download ---")
    download_all()
    download_bsa()

    print("\n--- Phase 2: Booking Snapshot Processing ---")
    process_snapshot()

    print("\n--- Phase 3: Google Drive Upload ---")
    upload_to_gdrive()

    elapsed = time.time() - start
    print(f"\n{'='*60}")
    print(f"Complete in {elapsed/60:.1f} min")
    print(f"{'='*60}")


if __name__ == '__main__':
    main()
