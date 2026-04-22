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
sys.stdout.reconfigure(encoding='utf-8', line_buffering=True)
sys.stderr.reconfigure(encoding='utf-8', line_buffering=True)

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

_now = datetime.now()
TODAY_STR = _now.strftime('%Y%m%d')
DATASET_ID = os.environ.get('DASHBOARD_DATASET_ID', TODAY_STR)
DATASET_YEAR = int(os.environ.get(
    'DASHBOARD_YEAR',
    DATASET_ID if re.fullmatch(r'\d{4}', DATASET_ID) else str(_now.year)
))
DATASET_IS_YEARLY = os.environ.get(
    'DASHBOARD_YEARLY',
    '1' if re.fullmatch(r'\d{4}', DATASET_ID) else '0'
) == '1'
DATASET_INPUT_SUFFIX = os.environ.get(
    'DASHBOARD_INPUT_SUFFIX',
    '' if DATASET_ID == TODAY_STR else f'_{DATASET_ID}'
)
PUBLISH_LATEST = os.environ.get(
    'PUBLISH_LATEST',
    '1' if DATASET_ID == TODAY_STR else '0'
) == '1'

# 445 fiscal calendar. 2025 is handled as a 53-week year.
FISCAL_445 = {
    2025: (datetime(2024, 12, 29), [4,4,5,4,4,5,4,4,5,4,4,6]),
    2026: (datetime(2026, 1, 4),  [4,4,5,4,4,5,4,4,5,4,4,5]),
    2027: (datetime(2027, 1, 3),  [4,4,5,4,4,5,4,4,5,4,4,5]),
}

def fiscal_year_bounds(year):
    first_sun, pattern = FISCAL_445[year]
    last_sat = first_sun + timedelta(weeks=sum(pattern), days=-1)
    return first_sun, last_sat

def build_445_map():
    week_month_lookup = {}
    for year, (first_sun, pattern) in FISCAL_445.items():
        wk = 0
        for mi, cnt in enumerate(pattern):
            ym = f'{year}{mi+1:02d}'
            for _ in range(cnt):
                week_month_lookup[(first_sun + timedelta(weeks=wk)).strftime('%Y-%m-%d')] = ym
                wk += 1
    return week_month_lookup

def dataset_csv_path(stem):
    return WORK_DIR / f'{stem}{DATASET_INPUT_SUFFIX}.csv'

def classify_team(origin, dly_raw):
    """OBT/EST/IST/JBT based on origin & destination country codes."""
    o, d = str(origin).strip(), str(dly_raw).strip()
    if o not in ('KR', 'JP') and d != 'KR': return 'OBT'
    elif o == 'KR' and d != 'JP': return 'EST'
    elif o != 'JP' and d == 'KR': return 'IST'
    else: return 'JBT'

BSA_TEAMS = ('OBT', 'EST', 'IST', 'JBT')

def normalize_bsa_team(df):
    """Use Tableau's BSA Sales Team field as the canonical team."""
    team_col = next((c for c in ('Sales Team', 'Sales_Team', 'team', 'Team') if c in df.columns), None)
    df = df.copy()
    if team_col:
        df['team'] = df[team_col].astype(str).str.strip().str.upper()
    else:
        df['team'] = [classify_team(str(o).strip(), str(d).strip())
                      for o, d in zip(df['POR_Country'], df['DLY_Country'])]
    return df[df['team'].isin(BSA_TEAMS)].copy()

# Workbook: booking snapshot(전체) - contentUrl
BKG_WB_CONTENT_URL = 'bookingsnapshot'
BKG_WB_ID = '81c076dd-4666-488e-96eb-699612d9e109'
# BSA raw (월간회의3주전)
BSA_VIEW_URL = 'Q_17363223877520/BSArawBKGpattern'

# Filter settings
_today = _now
# Historical/yearly datasets use the requested fiscal-year bounds.
_fy_start, _fy_end = fiscal_year_bounds(DATASET_YEAR)
BKG_SCHEDULE_START = os.environ.get(
    'BKG_SCHEDULE_START',
    f'{_fy_start:%Y-%m-%d} 00:00:00' if DATASET_IS_YEARLY else '2025-12-28 00:00:00'
)
# END = 금주 일요일 + 4주 (토요일까지)
_this_sun = _today - timedelta(days=(_today.weekday()+1)%7)
_end_sat = _this_sun + timedelta(days=4*7+6)  # +4주 토요일
BKG_SCHEDULE_END = os.environ.get(
    'BKG_SCHEDULE_END',
    f'{_fy_end:%Y-%m-%d} 00:00:00' if DATASET_IS_YEARLY else _end_sat.strftime('%Y-%m-%d 00:00:00')
)
TEMP_WB_NAME = os.environ.get(
    'TEMP_WB_NAME',
    'temp_bkg_snapshot_v2' if PUBLISH_LATEST else f'temp_bkg_snapshot_v2_{DATASET_ID}'
)
TEMP_WB_PROJECT_ID = '3d94d4a3-1b23-4e39-8c9c-4a3b765c140d'  # OBT AI AGENT


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


def ensure_temp_workbook(s, api_ver, site_id, start=None, end=None, workbook_name=None):
    """Download original TWB, modify filter, publish as temp workbook."""
    import xml.etree.ElementTree as ET
    start = start or BKG_SCHEDULE_START
    end = end or BKG_SCHEDULE_END
    workbook_name = workbook_name or TEMP_WB_NAME
    need_view2_date_filter = DATASET_IS_YEARLY or os.environ.get('FILTER_VIEW2_DATE') == '1'

    # Check if temp workbook exists (search by name; contentUrl may have suffix)
    resp = s.get(
        f'{TABLEAU_SERVER}/api/{api_ver}/sites/{site_id}/workbooks',
        params={'filter': f'name:eq:{workbook_name}'},
        headers={'Accept': 'application/json'}, timeout=30)
    wbs = resp.json().get('workbooks', {}).get('workbook', [])

    if wbs:
        # Verify filter is correct (both min and max)
        wb_id = wbs[0]['id']
        actual_content_url = wbs[0].get('contentUrl', workbook_name)
        resp = s.get(f'{TABLEAU_SERVER}/api/{api_ver}/sites/{site_id}/workbooks/{wb_id}/content',
                     timeout=120)
        content = resp.content
        if content[:2] == b'PK':
            import zipfile
            with zipfile.ZipFile(io.BytesIO(content)) as z:
                twb_name = [n for n in z.namelist() if n.endswith('.twb')][0]
                content = z.read(twb_name)
        tree = ET.parse(io.BytesIO(content))
        schedule_ok = False
        view2_date_ok = not need_view2_date_filter
        for f in tree.getroot().iter('filter'):
            col = f.get('column', '')
            if 'Calculation_0356804709482497' in col:
                min_el = f.find('min')
                max_el = f.find('max')
                schedule_ok = min_el is not None and start in (min_el.text or '') and max_el is not None and end in (max_el.text or '')
            if need_view2_date_filter and 'Calculation_501025459300655110' in col:
                min_el = f.find('min')
                max_el = f.find('max')
                view2_date_ok = min_el is not None and start in (min_el.text or '') and max_el is not None and end in (max_el.text or '')

        if schedule_ok and view2_date_ok:
            print(f"  Temp workbook exists with correct filter ({start} ~ {end})")
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

    # Modify Booking_schedule filter min/max
    for f in tree.getroot().iter('filter'):
        col = f.get('column', '')
        if 'Calculation_0356804709482497' in col and f.get('class') == 'quantitative':
            min_el = f.find('min')
            if min_el is not None:
                min_el.text = f'#{start}#'
                min_el.attrib.clear()
            max_el = f.find('max')
            if max_el is not None:
                max_el.text = f'#{end}#'
                max_el.attrib.clear()
            else:
                # max 엘리먼트가 없으면 생성
                max_el = ET.SubElement(f, 'max')
                max_el.text = f'#{end}#'
            print(f"  Filter: {start} ~ {end}")

    if need_view2_date_filter:
        view2_filter = None
        for ws in tree.getroot().findall('.//worksheet'):
            if ws.get('name') != '2':
                continue
            view = ws.find('./table/view')
            if view is None:
                continue
            for f in view.findall('filter'):
                if 'Calculation_501025459300655110' in f.get('column', ''):
                    view2_filter = f
                    break
            if view2_filter is None:
                view2_filter = ET.SubElement(view, 'filter', {
                    'class': 'quantitative',
                    'column': '[sqlproxy.1vgswr41razzwa148ywuc0fpriw3].[none:Calculation_501025459300655110:qk]',
                    'included-values': 'in-range',
                })
            break
        if view2_filter is not None:
            min_el = view2_filter.find('min')
            if min_el is None:
                min_el = ET.SubElement(view2_filter, 'min')
            min_el.text = f'#{start}#'
            min_el.attrib.clear()
            max_el = view2_filter.find('max')
            if max_el is None:
                max_el = ET.SubElement(view2_filter, 'max')
            max_el.text = f'#{end}#'
            max_el.attrib.clear()
            print(f"  View 2 Date_vsl filter: {start} ~ {end}")
        else:
            print("  WARNING: worksheet 2 view not found; Date_vsl filter not added")

    twb_bytes = io.BytesIO()
    tree.write(twb_bytes, encoding='utf-8', xml_declaration=True)
    twb_content = twb_bytes.getvalue()

    # Publish
    print(f"  Publishing temp workbook...")
    boundary = '----TableauBoundary'
    payload = (
        f'--{boundary}\r\nContent-Disposition: name="request_payload"\r\n'
        f'Content-Type: text/xml\r\n\r\n'
        f'<tsRequest><workbook name="{workbook_name}" showTabs="true">'
        f'<project id="{TEMP_WB_PROJECT_ID}"/></workbook></tsRequest>\r\n'
        f'--{boundary}\r\nContent-Disposition: name="tableau_workbook"; '
        f'filename="{workbook_name}.twb"\r\nContent-Type: application/xml\r\n\r\n'
    ).encode('utf-8') + twb_content + f'\r\n--{boundary}--\r\n'.encode('utf-8')

    actual_content_url = workbook_name
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
                    actual_content_url = wb_el.get('contentUrl', workbook_name)
            except Exception:
                pass
    except requests.exceptions.ReadTimeout:
        print(f"  Publish timed out (likely succeeded)")
        time.sleep(5)

    # Fallback: query by name to get actual contentUrl
    if actual_content_url == workbook_name:
        deadline = time.time() + 1800
        while time.time() < deadline:
            resp = s.get(
                f'{TABLEAU_SERVER}/api/{api_ver}/sites/{site_id}/workbooks',
                params={'filter': f'name:eq:{workbook_name}'},
                headers={'Accept': 'application/json'}, timeout=30)
            found = resp.json().get('workbooks', {}).get('workbook', [])
            if found:
                actual_content_url = found[0].get('contentUrl', workbook_name)
                print(f"  Published workbook available: {actual_content_url}")
                break
            print("  Waiting for published workbook to become available...")
            time.sleep(30)

    return actual_content_url


def download_csv_from_tableau(content_url, view_name, save_path, vf_params=None):
    """Download CSV from Tableau view using Playwright JS navigation."""
    from playwright.sync_api import sync_playwright
    save_path = Path(save_path)
    tmp_path = save_path.with_name(f'{save_path.name}.download')
    tmp_path.unlink(missing_ok=True)

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
        download.save_as(str(tmp_path))
        os.replace(tmp_path, save_path)

        browser.close()
    return os.path.getsize(save_path)


def count_csv_rows(path):
    """Count downloaded UTF-8 CSV rows without materializing the file."""
    with open(path, 'r', encoding='utf-8-sig', newline='') as f:
        return max(sum(1 for _ in csv.reader(f)) - 1, 0)


def read_tableau_csv(path):
    """Read current UTF-8 Tableau CSVs, with fallback for older UTF-16 TSV files."""
    path = Path(path)
    with path.open('rb') as f:
        sample = f.read(4)
    if sample.startswith(b'\xff\xfe') or b'\x00' in sample:
        return pd.read_csv(path, encoding='utf-16', sep='\t', dtype=str)
    return pd.read_csv(path, encoding='utf-8-sig', dtype=str)


def drop_tableau_total_rows(df, label):
    """Drop Tableau grand-total rows that look like regular CSV records."""
    total_markers = {'전체', 'Total', 'Grand Total'}
    mask = pd.Series(False, index=df.index)
    for col in ['BKG_NO', 'Booking_status', 'LST_Status']:
        if col in df.columns:
            mask = mask | df[col].astype(str).str.strip().isin(total_markers)
    if mask.any():
        print(f"  {label}: dropped Tableau total rows: {mask.sum():,}")
        return df.loc[~mask].copy()
    return df


def fiscal_quarter_chunks(year):
    """Return 445 fiscal-quarter date ranges for a yearly one-off download."""
    first_sun, pattern = FISCAL_445[year]
    week_offset = 0
    for idx in range(4):
        weeks = sum(pattern[idx*3:(idx+1)*3])
        start = first_sun + timedelta(weeks=week_offset)
        end = start + timedelta(weeks=weeks, days=-1)
        week_offset += weeks
        yield idx + 1, f'{start:%Y-%m-%d} 00:00:00', f'{end:%Y-%m-%d} 00:00:00'


def download_all_chunked():
    """Download yearly booking views in fiscal-quarter chunks, then merge to 1_YYYY/2_YYYY."""
    print("[1/3] Downloading booking views by fiscal quarter...")
    requested_views = set(os.environ.get('DASHBOARD_DOWNLOAD_VIEWS', '1,2').split(','))
    view_specs = [(name, label) for name, label in [('1', 'View 1'), ('2', 'View 2')] if name in requested_views]
    parts = {name: [] for name, _ in view_specs}
    s, api_ver, site_id = tableau_rest_api()
    try:
        for chunk_no, start, end in fiscal_quarter_chunks(DATASET_YEAR):
            print(f"  Chunk Q{chunk_no}: {start} ~ {end}")
            wb_name = f'{TEMP_WB_NAME}_q{chunk_no}'
            wb_url = ensure_temp_workbook(s, api_ver, site_id, start=start, end=end, workbook_name=wb_name)

            for view_name, label in view_specs:
                part_path = WORK_DIR / f'{view_name}_{DATASET_ID}_q{chunk_no}.csv'
                print(f"    Downloading {label} ({part_path.name})...")
                size = download_csv_from_tableau(wb_url, view_name, part_path)
                rows = count_csv_rows(part_path)
                print(f"      {part_path.name}: {size:,} bytes ({rows:,} rows)")
                parts[view_name].append(part_path)
    finally:
        try:
            s.post(f'{TABLEAU_SERVER}/api/{api_ver}/auth/signout', timeout=10)
        except Exception:
            pass

    print("[2/3] Merging chunked booking CSVs...")
    for view_name in parts:
        frames = [read_tableau_csv(path) for path in parts[view_name]]
        combined = pd.concat(frames, ignore_index=True)
        before = len(combined)
        combined = combined.drop_duplicates()
        out_path = dataset_csv_path(view_name)
        combined.to_csv(out_path, index=False, encoding='utf-8-sig')
        print(f"  {out_path.name}: {os.path.getsize(out_path):,} bytes ({len(combined):,} rows, dropped {before-len(combined):,} duplicate rows)")
        for path in parts[view_name]:
            path.unlink(missing_ok=True)
    print("[3/3] Chunked booking download complete")


def download_all():
    """Phase 1: Download all data from Tableau."""
    if DATASET_IS_YEARLY and os.environ.get('DASHBOARD_CHUNKED_DOWNLOAD', '1') == '1':
        download_all_chunked()
        return

    os.chdir(WORK_DIR)
    s, api_ver, site_id = tableau_rest_api()

    # 1. Ensure temp workbook with correct filter
    print("[1/3] Ensuring temp workbook...")
    wb_url = ensure_temp_workbook(s, api_ver, site_id)
    s.post(f'{TABLEAU_SERVER}/api/{api_ver}/auth/signout', timeout=10)

    # 2. Download View 1 (1.csv)
    path1 = dataset_csv_path('1')
    print(f"[2/3] Downloading View 1 ({path1.name})...")
    size = download_csv_from_tableau(wb_url, '1', path1)
    rows = count_csv_rows(path1)
    print(f"  {path1.name}: {size:,} bytes ({rows:,} rows)")

    # 3. Download View 2 (2.csv)
    # View 2 is controlled by its own YYYYMM/status filters. Download it from
    # the original workbook so Tableau-side status/filter edits are not hidden
    # by a previously published temp workbook.
    path2 = dataset_csv_path('2')
    print(f"[3/3] Downloading View 2 ({path2.name})...")
    size = download_csv_from_tableau(BKG_WB_CONTENT_URL, '2', path2)
    rows = count_csv_rows(path2)
    print(f"  {path2.name}: {size:,} bytes ({rows:,} rows)")


def download_bsa():
    """Download BSA raw (월간회의3주전) per Sales Team."""
    print("[BSA] Downloading BSA raw...")
    year = DATASET_YEAR
    years = [year] if DATASET_IS_YEARLY else [year-1, year, year+1]
    yyyy_filter = ','.join(str(y) for y in years)
    yyyymm_all = ','.join(f'{y}{m:02d}' for y in years for m in range(1, 13))

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
        import urllib.parse
        import pandas as pd
        all_dfs = []
        for team in BSA_TEAMS:
            params = urllib.parse.urlencode({
                'vf_YYYY': yyyy_filter,
                'vf_YYYYMM': yyyymm_all,
                'Sales Team': team,
            }, safe=',')
            csv_url = f'{TABLEAU_SERVER}/views/{BSA_VIEW_URL}.csv?{params}'
            print(f"  Downloading BSA: {team}...", end=' ', flush=True)
            with page.expect_download(timeout=600000) as dl_info:
                page.evaluate(f'window.location.href = "{csv_url}"')
            download = dl_info.value
            tmp_path = download.path()
            df = normalize_bsa_team(read_tableau_csv(tmp_path))
            df = df[df['team'] == team]
            print(f"{len(df)} rows")
            all_dfs.append(df)
        combined = pd.concat(all_dfs, ignore_index=True)
        if DATASET_IS_YEARLY and 'YYYYMM' in combined.columns:
            before = len(combined)
            combined = combined[combined['YYYYMM'].astype(str).str.startswith(str(DATASET_YEAR))].copy()
            dropped = before - len(combined)
            if dropped:
                print(f"  Dropped BSA rows outside {DATASET_YEAR}: {dropped:,}")
            if combined.empty:
                print(f"  WARNING: no BSA rows found for {DATASET_YEAR} in Tableau CSV export")

        out_dir = WORK_DIR / 'output'
        out_dir.mkdir(exist_ok=True)
        out_path = out_dir / f'BSA_raw_monthly3W_{DATASET_ID}.csv'
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
    grade_lookup = {}
    week_month_lookup = {}

    # Grade: Tableau에서 분기별 다운로드 (Q1=01, Q2=04, Q3=07, Q4=10)
    grade_csv = WORK_DIR / 'output' / ('grade_latest.csv' if PUBLISH_LATEST else f'grade_{DATASET_ID}.csv')
    _quarter_month = {1: '01', 2: '04', 3: '07', 4: '10'}
    _q = (_today.month - 1) // 3 + 1
    _grade_yyyymm = f'{DATASET_YEAR}{_quarter_month[_q]}'
    _need_download = True
    if grade_csv.exists():
        # 기존 파일의 YYYYMM 확인 (첫 줄 주석 또는 파일 내용)
        _header = grade_csv.read_text(encoding='utf-8').split('\n')[0]
        if _grade_yyyymm in _header:
            _need_download = False

    if _need_download:
        print(f"  Downloading grade from Tableau (YYYYMM={_grade_yyyymm})...")
        try:
            _grade_save = WORK_DIR / 'output' / f'grade_download_{DATASET_ID}.csv'
            download_csv_from_tableau('Q_17363223877520', 'grade', _grade_save,
                                      vf_params={'YYYYMM': _grade_yyyymm})
            # 첫 줄에 YYYYMM 메타 추가하여 저장
            _gdata = _grade_save.read_text(encoding='utf-8-sig')
            grade_csv.write_text(f'# YYYYMM={_grade_yyyymm}\n' + _gdata, encoding='utf-8')
            _grade_save.unlink(missing_ok=True)
            print(f"  grade downloaded: {os.path.getsize(grade_csv):,} bytes")
        except Exception as e:
            print(f"  grade download failed: {e}")

    if grade_csv.exists():
        _gdf = pd.read_csv(grade_csv, comment='#', dtype=str)
        _shpr_col = next((c for c in _gdf.columns if 'Shipper' in c or 'CST' in c), _gdf.columns[0])
        _grade_col = next((c for c in _gdf.columns if 'grade' in c.lower()), _gdf.columns[-1])
        for _, r in _gdf.iterrows():
            code = str(r[_shpr_col]).strip() if pd.notna(r[_shpr_col]) else ''
            g = str(r[_grade_col]).strip() if pd.notna(r[_grade_col]) else ''
            if code and code != 'nan':
                if g == 'AB': grade_lookup[code] = 'A+B'
                elif g == 'CD': grade_lookup[code] = 'C+D'
                elif g in ('A+B', 'C+D'): grade_lookup[code] = g
        _ab = sum(1 for v in grade_lookup.values() if v == 'A+B')
        _cd = sum(1 for v in grade_lookup.values() if v == 'C+D')
        print(f"  grade: {len(grade_lookup)} shippers (A+B={_ab}, C+D={_cd})")
    else:
        # Fallback: grade from existing cache
        cache_files = sorted((WORK_DIR / 'output').glob('_cache_*.parquet'), key=os.path.getmtime, reverse=True)
        if cache_files:
            _cf = pd.read_parquet(cache_files[0], columns=['BKG_SHPR_CST_NO', 'grade'])
            for _, r in _cf.drop_duplicates('BKG_SHPR_CST_NO').iterrows():
                if pd.notna(r['BKG_SHPR_CST_NO']):
                    grade_lookup[str(r['BKG_SHPR_CST_NO']).strip()] = str(r['grade']).strip() if pd.notna(r['grade']) else ''
            print(f"  grade loaded from cache: {len(grade_lookup)}")

    # 445 calendar map
    week_month_lookup = build_445_map()
    print(f"  주차월: {len(week_month_lookup)}")

    # --- Read CSV data ---
    print("[Process] Reading CSV files...")
    path1 = dataset_csv_path('1')
    path2 = dataset_csv_path('2')
    df1 = read_tableau_csv(path1)
    df2 = read_tableau_csv(path2)
    df1.columns = [re.sub(r'[^\x00-\x7F]+$', '', c).strip() for c in df1.columns]
    df2.columns = [re.sub(r'[^\x00-\x7F]+$', '', c).strip() for c in df2.columns]
    df1 = drop_tableau_total_rows(df1, path1.name)
    df2 = drop_tableau_total_rows(df2, path2.name)

    # Base: 2.csv (모든 부킹), Supplement: 1.csv (상세 정보)
    df2_dedup = df2.drop_duplicates(subset='BKG_NO', keep='first')
    df1_unique = df1.drop_duplicates(subset='BKG_NO', keep='first')
    df1_dedup = df1_unique.set_index('BKG_NO')
    print(f"  1.csv: {len(df1):,}, 2.csv: {len(df2):,}")
    print(f"  Base (2.csv unique): {len(df2_dedup):,}, Supplement (1.csv unique): {len(df1_dedup):,}")
    if 'Booking_status' in df2_dedup.columns:
        status_mix = df2_dedup['Booking_status'].astype(str).str.strip().value_counts().head(10)
        status_msg = ', '.join(f'{k}={v:,}' for k, v in status_mix.items())
        print(f"  2.csv status mix: {status_msg}")

    def df2_col(*names):
        return next((c for c in names if c in df2_dedup.columns), None)

    def parse_korean_date(s):
        if pd.isna(s) or str(s).strip() in ('', 'nan'):
            return pd.NaT
        m = re.match(r'(\d{4})\D+(\d{1,2})\D+(\d{1,2})', str(s))
        return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3))) if m else pd.NaT

    def df1_lookup(bkg_no, col):
        if bkg_no not in df1_dedup.index: return None
        try:
            val = df1_dedup.loc[bkg_no, col]
            return str(val) if pd.notna(val) else None
        except: return None

    def load_previous_actual_schedule():
        out_dir = WORK_DIR / 'output'

        def snapshot_date(path):
            m = re.search(r'(\d{8})', path.stem)
            return m.group(1) if m else ''

        candidates = []
        for pattern in ['_cache_*.parquet', 'booking_snapshot_result_*.csv',
                        'booking_snapshot_result_*.xlsx']:
            for path in out_dir.glob(pattern):
                dd = snapshot_date(path)
                if dd and dd < TODAY_STR:
                    candidates.append((dd, path.stat().st_mtime, path))

        for _, _, path in sorted(candidates, reverse=True):
            try:
                cols = ['BKG_NO', 'Actual_Departure_schedule']
                if path.suffix.lower() == '.parquet':
                    prev = pd.read_parquet(path, columns=cols)
                elif path.suffix.lower() == '.csv':
                    prev = pd.read_csv(path, dtype=str, encoding='utf-8-sig',
                                       keep_default_na=False, usecols=cols)
                else:
                    prev = pd.read_excel(path, dtype=str, keep_default_na=False,
                                         usecols=cols)
                prev['BKG_NO'] = prev['BKG_NO'].astype(str).str.strip()
                prev['Actual_Departure_schedule'] = (
                    prev['Actual_Departure_schedule'].fillna('').astype(str).str.strip()
                )
                prev = prev[prev['BKG_NO'].ne('') & prev['Actual_Departure_schedule'].ne('')]
                prev = prev.drop_duplicates('BKG_NO', keep='last')
                lookup = dict(zip(prev['BKG_NO'], prev['Actual_Departure_schedule']))
                print(f"  Previous actual schedule lookup: {path.name} ({len(lookup):,} rows)")
                return lookup
            except Exception as e:
                print(f"  Previous actual schedule lookup skipped ({path.name}): {e}")
        print("  Previous actual schedule lookup: none")
        return {}

    # --- Build output from 2.csv base ---
    print("[Process] Building output (2.csv base)...")
    output = pd.DataFrame()
    bkg_nos = df2_dedup['BKG_NO'].values

    # Columns from 2.csv directly
    output['BKG_NO'] = bkg_nos
    # POR from 2.csv
    por_ctr_col = next((c for c in df2_dedup.columns if 'POR_Country' in c), None)
    por_port_col = next((c for c in df2_dedup.columns if 'POR_PORT' in c), None)
    output['POR_CTR_CD'] = df2_dedup[por_ctr_col].values if por_ctr_col else ''
    output['POR_PLC_CD'] = df2_dedup[por_port_col].values if por_port_col else ''
    # DLY from 2.csv
    dly_ctr_col = next((c for c in df2_dedup.columns if 'DLY_Country' in c), None)
    dly_port_col = next((c for c in df2_dedup.columns if 'DLY_PORT' in c), None)
    output['DLY_CTR_CD'] = df2_dedup[dly_ctr_col].values if dly_ctr_col else ''
    output['DLY_PLC_CD'] = df2_dedup[dly_port_col].values if dly_port_col else ''
    # Status, TEU, CM1, route, vessel from 2.csv
    output['LST_Status'] = df2_dedup['Booking_status'].values
    output['CM1'] = df2_dedup['CM1_Booking'].values
    lst_teu_col = df2_col('LST_TEU', 'TEU_Booking')
    if not lst_teu_col:
        raise KeyError('2.csv requires LST_TEU or TEU_Booking column')
    output['LST_TEU'] = df2_dedup[lst_teu_col].values
    output['LST_route'] = df2_dedup['LST_Route'].values if 'LST_Route' in df2_dedup.columns else ''
    output['LST_VSL'] = df2_dedup['LST_VSL'].values if 'LST_VSL' in df2_dedup.columns else ''
    output['LST_VOY'] = df2_dedup['LST_VOY'].values if 'LST_VOY' in df2_dedup.columns else ''
    output['Salesman_POR'] = df2_dedup['Salesman_POR'].values if 'Salesman_POR' in df2_dedup.columns else ''
    # Date_vsl from 2.csv
    date_vsl_col = next((c for c in df2_dedup.columns if 'Date_vsl' in c), None)
    output['Actual_Departure_schedule'] = df2_dedup[date_vsl_col].values if date_vsl_col else ''

    # Columns from 1.csv via lookup
    print("  Looking up 1.csv columns...")
    for col in ['BKG_SHPR_CST_NO', 'BKG_SHPR_CST_ENM', 'POL_CTR_CD', 'POL_PORT_CD',
                'POD_CTR_CD', 'POD_PORT_CD', 'VSL_CD', 'VOY_NO',
                'Booking_date', 'Booking_schedule', 'Cancel_date', 'FST_TEU']:
        output[col] = pd.Series([df1_lookup(b, col) for b in bkg_nos], dtype=object).fillna('')

    # Fallback: POR/DLY from 1.csv if 2.csv is empty
    for f2col, f1col in [('POR_CTR_CD','POR_CTR_CD'),('POR_PLC_CD','POR_PLC_CD'),
                         ('DLY_CTR_CD','DLY_CTR_CD'),('DLY_PLC_CD','DLY_PLC_CD')]:
        mask = output[f2col].astype(str).str.strip().isin(['','nan'])
        if mask.any():
            fb = pd.Series([df1_lookup(b, f1col) for b in bkg_nos], dtype=object).fillna('')
            output.loc[mask, f2col] = fb[mask]

    # Fallback: Booking_schedule/Booking_date from Date_vsl if 1.csv lookup failed
    for col in ['Booking_schedule', 'Booking_date']:
        empty = output[col].astype(str).str.strip().isin(['', 'nan', 'None', 'NaN'])
        if empty.any():
            output.loc[empty, col] = output.loc[empty, 'Actual_Departure_schedule']
            print(f"  {col} fallback to Date_vsl: {empty.sum():,}건")

    # FST_TEU fallback: if empty, use LST_TEU
    fst_empty = output['FST_TEU'].astype(str).str.strip().isin(['', 'nan', 'None'])
    output.loc[fst_empty, 'FST_TEU'] = output.loc[fst_empty, 'LST_TEU']

    # View 2 can arrive with only Normal/Confirm rows depending on the Tableau
    # workbook state. 17-Apr logic included Cancel rows in BKG/WOS BKG and kept
    # LST_TEU/CM1 at zero, so recover df1-only cancelled bookings from View 1.
    recovered_cancel_candidate_count = 0
    recovered_cancel_count = 0
    recovered_prev_actual_count = 0
    recovered_missing_actual_count = 0
    if 'Cancel_date' in df1_unique.columns:
        df2_bkg_set = set(df2_dedup['BKG_NO'].astype(str).str.strip())
        df1_bkg_key = df1_unique['BKG_NO'].astype(str).str.strip()
        missing_from_2 = df1_unique.loc[~df1_bkg_key.isin(df2_bkg_set)].copy()
        cancel_date_key = missing_from_2['Cancel_date'].fillna('').astype(str).str.strip()
        cancel_missing = missing_from_2[
            (cancel_date_key != '') &
            (~cancel_date_key.str.lower().isin(['nan', 'none', 'nat']))
        ].copy()

        recovered_cancel_candidate_count = len(cancel_missing)
        actual_from_prev = pd.Series(dtype=object)
        if recovered_cancel_candidate_count:
            prev_actual = load_previous_actual_schedule()
            actual_from_prev = cancel_missing['BKG_NO'].astype(str).str.strip().map(prev_actual).fillna('')
            has_actual = actual_from_prev.astype(str).str.strip().ne('')
            recovered_missing_actual_count = int((~has_actual).sum())
            cancel_missing = cancel_missing.loc[has_actual].copy()
            actual_from_prev = actual_from_prev.loc[has_actual]

        recovered_cancel_count = len(cancel_missing)
        if recovered_cancel_count:
            def take(col, default=''):
                if col in cancel_missing.columns:
                    return cancel_missing[col].astype(object).values
                return [default] * recovered_cancel_count

            recovered = pd.DataFrame(index=cancel_missing.index)
            recovered['BKG_NO'] = take('BKG_NO')
            recovered['POR_CTR_CD'] = take('POR_CTR_CD')
            recovered['POR_PLC_CD'] = take('POR_PLC_CD')
            recovered['DLY_CTR_CD'] = take('DLY_CTR_CD')
            recovered['DLY_PLC_CD'] = take('DLY_PLC_CD')
            recovered['LST_Status'] = 'Cancel'
            recovered['CM1'] = ''
            recovered['LST_TEU'] = '0'
            recovered['LST_route'] = ''
            recovered['LST_VSL'] = take('VSL_CD')
            recovered['LST_VOY'] = take('VOY_NO')
            recovered['Salesman_POR'] = ''

            for col in ['BKG_SHPR_CST_NO', 'BKG_SHPR_CST_ENM', 'POL_CTR_CD', 'POL_PORT_CD',
                        'POD_CTR_CD', 'POD_PORT_CD', 'VSL_CD', 'VOY_NO',
                        'Booking_date', 'Booking_schedule', 'Cancel_date', 'FST_TEU']:
                recovered[col] = take(col)

            recovered['Actual_Departure_schedule'] = actual_from_prev.astype(object).values
            recovered_prev_actual_count = recovered_cancel_count

            output = pd.concat([output, recovered[output.columns]], ignore_index=True)
    print(f"  Recovered Cancel candidates from 1.csv only: {recovered_cancel_candidate_count:,}")
    print(f"  Recovered Cancel rows with actual schedule: {recovered_cancel_count:,}")
    print(f"  Recovered Cancel actual schedules from previous snapshot: {recovered_prev_actual_count:,}")
    print(f"  Skipped recovered Cancel rows without actual schedule: {recovered_missing_actual_count:,}")

    total = len(output)
    bkg_nos = output['BKG_NO'].values
    shpr_codes = output['BKG_SHPR_CST_NO'].values
    dly_ctrs = output['DLY_CTR_CD'].values

    only_in_2 = sum(1 for b in bkg_nos if b not in df1_dedup.index)
    print(f"  Total: {total:,} (1.csv matched: {total-only_in_2:,}, 2.csv only: {only_in_2:,})")

    # --- Compute formulas ---
    print("[Process] Computing formulas...")

    date_N = output['Booking_date'].apply(parse_korean_date)
    date_O = output['Booking_schedule'].apply(parse_korean_date)

    # week_start_date: Actual_Departure_schedule 기준 일요일
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
    output['grade'] = [grade_lookup.get(str(s).strip(), '') if pd.notna(s) else '' for s in shpr_codes]

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

    # D_group is retained for downstream compatibility, but now stores the
    # destination country code without regional grouping.
    output['D_group'] = [str(x).strip() for x in dly_ctrs]

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
    shpr_agg['pt'] = shpr_agg.apply(lambda r: '고수익' if r['s_avg'] >= r['r_avg'] else '저수익', axis=1)
    # 룩업 딕셔너리
    pt_lookup = {(r['shpr'], r['por'], r['dly']): r['pt'] for _, r in shpr_agg.iterrows()}
    output['\uace0/\uc800'] = [
        pt_lookup.get((str(s).strip(), str(p).strip(), str(d).strip()), '')
        for s, p, d in zip(output['BKG_SHPR_CST_NO'], output['POR_PLC_CD'], output['DLY_PLC_CD'])]
    hi_cnt = sum(1 for v in output['\uace0/\uc800'] if v == '고수익')
    lo_cnt = sum(1 for v in output['\uace0/\uc800'] if v == '저수익')

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
        lambda r: '고수익화주' if r['s_avg'] >= r['p_avg'] else '저수익화주', axis=1)

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

    # 2순위: 전월 + 전체 선적지 (화주 전체 실적 기준)
    all_month_avg = valid2.groupby(['yyyymm']).agg(a_cm1=('cm1','sum'), a_teu=('teu','sum')).reset_index()
    all_month_avg['a_avg'] = all_month_avg['a_cm1'] / all_month_avg['a_teu']
    shpr_all_month = valid2.groupby(['shpr', 'yyyymm']).agg(s_cm1=('cm1','sum'), s_teu=('teu','sum')).reset_index()
    shpr_all_month['s_avg'] = shpr_all_month['s_cm1'] / shpr_all_month['s_teu']
    shpr_all_month = shpr_all_month.merge(all_month_avg[['yyyymm','a_avg']], on='yyyymm')
    shpr_all_month['tag'] = shpr_all_month.apply(lambda r: '고수익화주' if r['s_avg'] >= r['a_avg'] else '저수익화주', axis=1)

    # 3순위: 당월 데이터 (선적지별)
    # → tag_dict_cur: (shpr, por, cur_month) → tag
    tag_dict_cur = {}
    for _, r in shpr_por_month.iterrows():
        tag_dict_cur[(r['shpr'], r['por'], r['yyyymm'])] = r['tag']

    # 딕셔너리로 변환
    # 1순위: (shpr, por, yyyymm) → tag (전월 선적지별)
    tag_dict_por = {}
    for _, r in shpr_por_month.iterrows():
        tag_dict_por[(r['shpr'], r['por'], r['yyyymm'])] = r['tag']
    # 2순위: (shpr, yyyymm) → tag (전월 전체 선적지)
    tag_dict_all = {}
    for _, r in shpr_all_month.iterrows():
        tag_dict_all[(r['shpr'], r['yyyymm'])] = r['tag']

    def get_tag(shpr_code, por_code, cur_month, grade_val):
        s = str(shpr_code).strip() if shpr_code else ''
        p = str(por_code).strip() if por_code else ''
        if not cur_month or not s:
            return ''
        y, m = int(cur_month[:4]), int(cur_month[4:])

        # 전월부터 6개월 전까지
        for i in range(1, 7):
            pm = m - i; py = y
            while pm <= 0: pm += 12; py -= 1
            ym = f'{py}{pm:02d}'
            # 1순위: 전월 + 동일 선적지
            t = tag_dict_por.get((s, p, ym))
            if t: return t
            # 2순위: 전월 + 전체 선적지
            t = tag_dict_all.get((s, ym))
            if t: return t

        # 3순위: 당월 + 동일 선적지
        t = tag_dict_cur.get((s, p, cur_month))
        if t: return t

        # 4순위: grade C+D → 고수익, A+B → 저수익
        g = str(grade_val).strip() if grade_val else ''
        if g == 'C+D': return '고수익화주'
        if g == 'A+B': return '저수익화주'
        return ''

    # 선적지+화주별 단일 태그 결정 (각 화주+선적지별 최신월 기준)
    unique_pairs = output[['BKG_SHPR_CST_NO','POR_PLC_CD','grade','YYYYMM']].copy()
    unique_pairs['BKG_SHPR_CST_NO'] = unique_pairs['BKG_SHPR_CST_NO'].astype(str).str.strip()
    unique_pairs['POR_PLC_CD'] = unique_pairs['POR_PLC_CD'].astype(str).str.strip()
    unique_pairs['grade'] = unique_pairs['grade'].astype(str).str.strip()
    unique_pairs['YYYYMM'] = unique_pairs['YYYYMM'].astype(str).str.strip()

    latest_months = unique_pairs[unique_pairs['YYYYMM'] != '']
    latest_months = latest_months.groupby(['BKG_SHPR_CST_NO','POR_PLC_CD'], dropna=False)['YYYYMM']
    latest_months = latest_months.max().reset_index().rename(columns={'YYYYMM': 'latest_YYYYMM'})

    unique_pairs = unique_pairs.merge(latest_months, on=['BKG_SHPR_CST_NO','POR_PLC_CD'], how='left')
    unique_pairs = unique_pairs.drop_duplicates(subset=['BKG_SHPR_CST_NO','POR_PLC_CD'])

    pair_tag = {}
    for _, row in unique_pairs.iterrows():
        s = row['BKG_SHPR_CST_NO']
        p = row['POR_PLC_CD']
        g = row['grade']
        lm = row['latest_YYYYMM'] if pd.notna(row['latest_YYYYMM']) else ''
        pair_tag[(s, p)] = get_tag(s, p, lm, g)

    output['고수익태그'] = [
        pair_tag.get((str(s).strip(), str(p).strip()), '')
        for s, p in zip(output['BKG_SHPR_CST_NO'], output['POR_PLC_CD'])]
    hi_tag = sum(1 for v in output['고수익태그'] if v == '고수익화주')
    lo_tag = sum(1 for v in output['고수익태그'] if v == '저수익화주')
    empty_tag = len(output) - hi_tag - lo_tag
    print(f"  고수익태그: 고수익={hi_tag:,}, 저수익={lo_tag:,}, 미분류={empty_tag:,}")

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
    min_dashboard_month = f'{DATASET_YEAR}01'
    month_key = output['YYYYMM'].astype(str).str.strip()
    if DATASET_IS_YEARLY:
        out_of_scope = month_key.ne('') & ~month_key.str.startswith(str(DATASET_YEAR))
        scope_label = str(DATASET_YEAR)
    else:
        out_of_scope = month_key.ne('') & month_key.lt(min_dashboard_month)
        scope_label = f'{min_dashboard_month}+'
    if out_of_scope.any():
        print(f"  Dropped out-of-scope months ({scope_label}): {out_of_scope.sum():,}")
        output = output[~out_of_scope].reset_index(drop=True)
        print(f"  Final dashboard scope: {len(output):,}")
    print(f"  Final (대상 only): {len(output):,}")

    # --- Save ---
    out_dir = WORK_DIR / 'output'
    out_dir.mkdir(exist_ok=True)
    out_path = out_dir / f'booking_snapshot_result_{DATASET_ID}.csv'
    cache_path = out_dir / f'_cache_{DATASET_ID}.parquet'
    output = output.fillna('').astype(str)
    print(f"[Process] Saving {out_path.name}...")
    output.to_csv(out_path, index=False, encoding='utf-8-sig')
    print(f"  {out_path.name}: {os.path.getsize(out_path):,} bytes, {len(output):,} rows")
    print(f"[Process] Saving {cache_path.name}...")
    output.to_parquet(cache_path, index=False)
    print(f"  {cache_path.name}: {os.path.getsize(cache_path):,} bytes")


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
    bf = sorted(out_dir.glob(f'booking_snapshot_result_{DATASET_ID}.csv'), key=os.path.getmtime, reverse=True)
    sf = sorted(out_dir.glob(f'BSA_raw_monthly3W_{DATASET_ID}.csv'), key=os.path.getmtime, reverse=True)
    cache = sorted(out_dir.glob(f'_cache_{DATASET_ID}.parquet'), key=os.path.getmtime, reverse=True)
    if not bf:
        bf = sorted(out_dir.glob('booking_snapshot_result_*.csv'), key=os.path.getmtime, reverse=True)
    if not sf:
        sf = sorted(out_dir.glob('BSA_raw_monthly3W_*.csv'), key=os.path.getmtime, reverse=True)
    if not cache:
        cache = sorted(out_dir.glob('_cache_*.parquet'), key=os.path.getmtime, reverse=True)

    if cache:
        bkg = pd.read_parquet(cache[0])
    elif bf:
        bkg = pd.read_csv(bf[0], dtype=str, encoding='utf-8-sig')
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
    _pt_col = [c for c in bkg.columns if '/' in c and len(c) <= 4]
    if _pt_col and 'profit_type' not in bkg.columns:
        bkg = bkg.rename(columns={_pt_col[0]: 'profit_type'})
    elif 'profit_type' not in bkg.columns:
        bkg['profit_type'] = ''
    if 'dest' not in bkg.columns:
        bkg['dest'] = bkg['DLY_CTR_CD'].astype(str).str.strip()
        bkg['origin'] = bkg['POR_CTR_CD']
        bkg['ori_port'] = bkg['POR_PLC_CD']
        bkg['dst_port'] = bkg['DLY_PLC_CD']
        bkg['team'] = [classify_team(str(o).strip(),str(d).strip()) for o,d in zip(bkg['POR_CTR_CD'], bkg['DLY_CTR_CD'])]
    if 'fst' not in bkg.columns:
        bkg['fst'] = pd.to_numeric(bkg.get('FST_TEU','0').astype(str).str.replace(',',''), errors='coerce').fillna(0)
        bkg['lst'] = pd.to_numeric(bkg.get('LST_TEU','0').astype(str).str.replace(',',''), errors='coerce').fillna(0)
        bkg['cm1v'] = pd.to_numeric(bkg.get('CM1','0').astype(str).str.replace(',',''), errors='coerce').fillna(0)

    # YYYYMM = 445 calendar (BSA와 동일)
    def _build_445():
        return build_445_map()
    _445 = _build_445()
    import re as _re
    def _pkd(s):
        if pd.isna(s): return None
        m = _re.match(r'(\d{4})\D+(\d{1,2})\D+(\d{1,2})', str(s))
        return f'{int(m.group(1))}-{int(m.group(2)):02d}-{int(m.group(3)):02d}' if m else None

    # 전체 데이터 기준 집계 (소석률 = 전체 Normal, WOS별 = Lead_time 마스크)
    bkg['_ws_key'] = bkg['week_start_date'].apply(_pkd)
    bkg['YYYYMM'] = bkg['_ws_key'].map(_445).fillna('')

    lt = bkg['Lead_time (BKG_Sche)']
    normal = bkg['LST_Status'] == 'Normal'
    cancel = bkg['LST_Status'] == 'Cancel'
    if '고수익태그' in bkg.columns:
        hi = bkg['고수익태그'].astype(str).str.strip().eq('고수익화주')
    else:
        hi = bkg['profit_type'].astype(str).str.contains('고수익', na=False)
    route_hi = bkg['profit_type'].astype(str).str.contains('고수익', na=False)

    bkg['is_normal'] = normal.astype(int)
    bkg['is_cancel'] = cancel.astype(int)
    bkg['is_hi'] = hi.astype(int)
    bkg['is_route_hi'] = route_hi.astype(int)
    # 실선적(norm_lst): 전체 Normal (소석률 계산용)
    bkg['norm_lst'] = bkg['lst'] * bkg['is_normal']
    bkg['hi_fst'] = bkg['fst'] * bkg['is_hi']
    bkg['hi_norm_lst'] = bkg['lst'] * bkg['is_hi'] * bkg['is_normal']
    bkg['cm1_norm'] = bkg['cm1v'] * bkg['is_normal'] * (bkg['cm1v'] != 0).astype(int)
    bkg['lst_norm'] = bkg['lst'] * bkg['is_normal'] * (bkg['cm1v'] != 0).astype(int)
    # 고수익화주 Normal CM1 / LST_TEU (고수익화주 CM1/TEU 계산용)
    bkg['hi_cm1_norm'] = bkg['cm1v'] * bkg['is_normal'] * bkg['is_hi'] * (bkg['cm1v'] != 0).astype(int)
    bkg['hi_lst_norm'] = bkg['lst'] * bkg['is_normal'] * bkg['is_hi'] * (bkg['cm1v'] != 0).astype(int)

    # WOS stage columns (Lead_time 마스크 기반, WOS-3 BKG 등)
    for wos, label in [('WOS-3','w3'),('WOS-2','w2'),('WOS-1','w1'),('Week of Sailing (WOS)','wos')]:
        mask = (lt == wos).astype(int)
        bkg[f'{label}_fst'] = bkg['fst'] * mask
        bkg[f'{label}_norm_lst'] = bkg['lst'] * mask * bkg['is_normal']
    bkg['w3_canc_fst'] = bkg['fst'] * (lt == 'WOS-3').astype(int) * bkg['is_cancel']
    bkg['w3_hi_fst'] = bkg['fst'] * (lt == 'WOS-3').astype(int) * bkg['is_hi']
    bkg['w3_hi_norm_lst'] = bkg['lst'] * (lt == 'WOS-3').astype(int) * bkg['is_hi'] * bkg['is_normal']
    bkg['w3_route_hi_fst'] = bkg['fst'] * (lt == 'WOS-3').astype(int) * bkg['is_route_hi']
    # WOS-3 CM1 columns (3주전 BKG 맥락에서 CM1/TEU 계산용)
    w3_mask = (lt == 'WOS-3').astype(int)
    cm1_nz = (bkg['cm1v'] != 0).astype(int)
    bkg['w3_cm1_norm'] = bkg['cm1v'] * w3_mask * bkg['is_normal'] * cm1_nz
    bkg['w3_hi_cm1_norm'] = bkg['cm1v'] * w3_mask * bkg['is_normal'] * bkg['is_hi'] * cm1_nz
    # AB/CD grade columns
    is_ab = (bkg.get('grade', '') == 'A+B').astype(int)
    bkg['w3_ab_fst'] = bkg['fst'] * w3_mask * is_ab
    bkg['w3_ab_norm_lst'] = bkg['lst'] * w3_mask * is_ab * bkg['is_normal']
    bkg['w3_cd_fst'] = bkg['fst'] * w3_mask * (1 - is_ab)
    bkg['w3_cd_norm_lst'] = bkg['lst'] * w3_mask * (1 - is_ab) * bkg['is_normal']

    print(f"    Total: {len(bkg):,}, WOS-3: {(lt=='WOS-3').sum():,}, Normal: {normal.sum():,}")

    # Monthly aggregation with ports
    gk = ['team','origin','ori_port','dest','dst_port','YYYYMM']
    agg_cols = {'fst':'sum','norm_lst':'sum','hi_fst':'sum','hi_norm_lst':'sum',
                'w3_fst':'sum','w3_norm_lst':'sum','w3_canc_fst':'sum','w3_hi_fst':'sum','w3_hi_norm_lst':'sum',
                'w3_route_hi_fst':'sum',
                'w3_ab_fst':'sum','w3_ab_norm_lst':'sum','w3_cd_fst':'sum','w3_cd_norm_lst':'sum',
                'w2_fst':'sum','w2_norm_lst':'sum','w1_fst':'sum','w1_norm_lst':'sum','wos_fst':'sum','wos_norm_lst':'sum',
                'cm1_norm':'sum','lst_norm':'sum',
                'w3_cm1_norm':'sum','w3_hi_cm1_norm':'sum'}
    monthly = bkg.groupby(gk).agg(agg_cols).reset_index()

    # Weekly aggregation (with port detail for port filter support)
    wk_keys = ['team','origin','ori_port','dest','dst_port','YYYYMM','week_start_date']
    weekly = bkg.groupby(wk_keys).agg(agg_cols).reset_index()

    # Shipper aggregation (화주별, 주차별) — BKG > 0인 전체 화주
    shpr_keys = ['team','origin','ori_port','dest','dst_port','YYYYMM','week_start_date','BKG_SHPR_CST_NO','BKG_SHPR_CST_ENM','Salesman_POR','고수익태그','grade']
    _shpr_excl = ('w3_ab_fst','w3_ab_norm_lst','w3_cd_fst','w3_cd_norm_lst',
                  'w2_fst','w2_norm_lst','w1_fst','w1_norm_lst','wos_fst','wos_norm_lst',
                  'cm1_norm','lst_norm','hi_cm1_norm','hi_lst_norm',
                  'hi_fst','hi_norm_lst','w3_hi_cm1_norm')
    shpr_agg_cols = {k:v for k,v in agg_cols.items() if k not in _shpr_excl}
    shipper = bkg.groupby(shpr_keys).agg(shpr_agg_cols).reset_index()
    shipper_all = shipper[shipper['fst'] > 0]
    print(f"    shipper: {len(shipper):,} → active: {len(shipper_all):,} rows")

    # WPM (445 기준)
    wpm = bkg[bkg['YYYYMM']!=''].groupby('YYYYMM')['week_start_date'].nunique().to_dict()

    # BSA
    bsa_data = []
    if sf:
        bsa = pd.read_csv(sf[0], dtype=str)
        bsa = normalize_bsa_team(bsa)
        if DATASET_IS_YEARLY:
            bsa = bsa[bsa['YYYYMM'].astype(str).str.startswith(str(DATASET_YEAR))]
        bsa = bsa[bsa['DLY_Country'].str.len() <= 3]
        bsa = bsa[bsa['POR_Country'].str.len() <= 3]
        bsa['teu_bsa'] = pd.to_numeric(bsa['TEU_BSA (Actual)'].str.replace(',',''), errors='coerce').fillna(0)
        # Keep destination at country-code level to match BKG.
        bsa['dest'] = bsa['DLY_Country'].astype(str).str.strip()
        bsa['origin'] = bsa['POR_Country']
        bsa_agg = bsa.groupby(['team','origin','POR_PORT','dest','DLY_PORT','YYYYMM','WW'])['teu_bsa'].sum().reset_index()
        bsa_agg = bsa_agg[bsa_agg['teu_bsa'] > 0]  # teu_bsa=0 records contribute nothing; drop to avoid field-missing issue in JSON
        bsa_data = bsa_agg.to_dict('records')

    metric_keys = set(agg_cols) | {'teu_bsa'}

    def compact_records(records):
        compacted = []
        for rec in records:
            out = {}
            for key, val in rec.items():
                if pd.isna(val):
                    continue
                if key in metric_keys:
                    try:
                        num = round(float(val), 1)
                    except (TypeError, ValueError):
                        continue
                    if num == 0:
                        continue
                    out[key] = int(num) if float(num).is_integer() else num
                elif val != '':
                    out[key] = val
            compacted.append(out)
        return compacted

    summary = {
        'data_date': DATASET_ID,
        'wpm': wpm,
        'months': sorted(bkg['YYYYMM'].dropna().unique().tolist()),
        'monthly': compact_records(monthly.round(1).to_dict('records')),
        'weekly': compact_records(weekly.round(1).to_dict('records')),
        'shipper': compact_records(shipper_all.round(1).to_dict('records')),
        'bsa': compact_records(bsa_data),
    }

    json_path = out_dir / f'dashboard_summary_{DATASET_ID}.json'
    with open(json_path, 'w', encoding='utf-8') as f:
        _json.dump(summary, f, ensure_ascii=False, separators=(',',':'))
    print(f"  Summary JSON: {json_path.name} ({os.path.getsize(json_path):,} bytes)")
    print(f"    monthly: {len(monthly):,} rows, weekly: {len(weekly):,} rows, bsa: {len(bsa_data):,} rows")

    # Copy to dist/data.json only for the current/latest dataset.
    if PUBLISH_LATEST:
        import shutil
        dist_data = WORK_DIR / 'dist' / 'data.json'
        if dist_data.parent.exists():
            shutil.copy2(json_path, dist_data)
            print(f"  Copied to {dist_data}")
    else:
        print("  Historical dataset: dist/data.json unchanged")

    if os.environ.get('SKIP_GDRIVE_UPLOAD') == '1':
        print("[Upload] SKIP_GDRIVE_UPLOAD=1; local summary built, remote upload skipped.")
        return

    # Clean up old output files (keep only latest 2) only for routine latest runs.
    if PUBLISH_LATEST:
        for pattern in ['booking_snapshot_result_*.csv', 'booking_snapshot_result_*.xlsx', 'BSA_raw_monthly3W_*.csv',
                        '_cache_*.parquet', 'dashboard_summary_*.json']:
            files = sorted(out_dir.glob(pattern), key=os.path.getmtime, reverse=True)
            for old in files[2:]:
                old.unlink()
                print(f"  Cleaned old: {old.name}")
    else:
        print("  Historical dataset: output cleanup skipped")

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

    # Upload parquet cache built during processing.
    cf = sorted(out_dir.glob(f'_cache_{DATASET_ID}.parquet'), key=os.path.getmtime, reverse=True)
    if not cf:
        cf = sorted(out_dir.glob('_cache_*.parquet'), key=os.path.getmtime, reverse=True)
    if cf:
        _upload_file(headers, cf[0], cf[0].name)

    # Upload BSA CSV
    sf = sorted(out_dir.glob(f'BSA_raw_monthly3W_{DATASET_ID}.csv'), key=os.path.getmtime, reverse=True)
    if not sf:
        sf = sorted(out_dir.glob('BSA_raw_monthly3W_*.csv'), key=os.path.getmtime, reverse=True)
    if sf:
        _upload_file(headers, sf[0], sf[0].name)

    # Upload summary JSON (for static dashboard)
    jf = sorted(out_dir.glob(f'dashboard_summary_{DATASET_ID}.json'), key=os.path.getmtime, reverse=True)
    if not jf:
        jf = sorted(out_dir.glob('dashboard_summary_*.json'), key=os.path.getmtime, reverse=True)
    if jf:
        if PUBLISH_LATEST:
            # 1. 고정 파일 (GitHub Pages용)
            _upload_file(headers, jf[0], 'dashboard_summary.json')
            # 2. 날짜별 보관 (히스토리 비교용)
            _upload_file(headers, jf[0], f'dashboard_summary_{DATASET_ID}.json')
        else:
            # One-off/yearly backfills are loaded from the historical selector.
            _upload_file(headers, jf[0], f'dashboard_summary_{DATASET_ID}.json')

    print("[Upload] Done.")


def _upload_file(headers, local_path, filename):
    """Upload or update a file in the Drive folder."""
    import json as _json

    # Check if file already exists
    q = f"name='{filename}' and '{GDRIVE_FOLDER_ID}' in parents and trashed=false"
    r = requests.get('https://www.googleapis.com/drive/v3/files',
        headers=headers, params={'q': q, 'fields': 'files(id)'})
    existing = r.json().get('files', [])

    with open(local_path, 'rb') as fh:
        data = fh.read()
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
    if os.environ.get('SKIP_DOWNLOAD') == '1':
        print("[Skip] Using existing 1.csv, 2.csv, and latest BSA raw file.")
    else:
        download_all()
        if os.environ.get('SKIP_BSA_DOWNLOAD') == '1':
            print("[Skip] Using existing BSA raw file.")
        else:
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
