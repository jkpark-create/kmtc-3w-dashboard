// Playwright headless verification + video capture for the Sales Target screen.
// Records a webm + a series of PNG screenshots that show the three views,
// filter interactions, drill-down flow, and BKG_NO list expansion.
// Run with:  node scripts/test/verify_sales_target.cjs
// Requires:  a static server already running on http://127.0.0.1:8749/ rooted at dist/
//            (the caller orchestrates the server lifecycle)

const path = require('path');
const fs = require('fs');
const { chromium } = require('playwright');

const ROOT = path.resolve(__dirname, '..', '..');
const OUT = path.join(ROOT, 'verification-output');
const VIDEO_DIR = path.join(OUT, 'video');
const SHOTS_DIR = path.join(OUT, 'screenshots');
const BASE = process.env.SALES_TARGET_BASE || 'http://127.0.0.1:8749';

fs.mkdirSync(VIDEO_DIR, { recursive: true });
fs.mkdirSync(SHOTS_DIR, { recursive: true });

const log = [];
function step(msg) {
  const line = `[${new Date().toISOString()}] ${msg}`;
  log.push(line);
  console.log(line);
}

async function shot(page, name) {
  const p = path.join(SHOTS_DIR, `${name}.png`);
  await page.screenshot({ path: p, fullPage: false });
  step(`screenshot: ${name}.png`);
}

async function main() {
  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext({
    viewport: { width: 1480, height: 920 },
    recordVideo: { dir: VIDEO_DIR, size: { width: 1480, height: 920 } },
  });
  const page = await context.newPage();

  const consoleErrors = [];
  const requestFailures = [];
  page.on('console', m => { if (m.type() === 'error') consoleErrors.push(m.text()); });
  page.on('requestfailed', r => requestFailures.push(`${r.method()} ${r.url()} → ${r.failure()?.errorText}`));

  // ── 1) Land on the sales-target screen ──────────────────────────────────
  step(`navigate ${BASE}/sales-target/`);
  await page.goto(`${BASE}/sales-target/`, { waitUntil: 'networkidle' });
  await page.waitForSelector('.cards-row .card.summary-card', { timeout: 10000 });
  await page.waitForFunction(() => {
    const el = document.getElementById('cSales');
    return el && el.textContent && el.textContent !== '-' && el.textContent !== '0';
  }, { timeout: 10000 });
  await page.waitForTimeout(800);
  await shot(page, '01_landing_summary');

  // ── 2) Sanity-read the KPI cards ─────────────────────────────────────────
  const dataInfo = await page.locator('#dataInfo').innerText();
  const counts = await page.evaluate(() => ({
    origins: document.getElementById('cOrigin').textContent.trim(),
    sales: document.getElementById('cSales').textContent.trim(),
    custs: document.getElementById('cCust').textContent.trim(),
    bookingTarget: document.getElementById('cBkT').textContent.trim(),
    bookingPerform: document.getElementById('cBkP').textContent.trim(),
    bookingGap: document.getElementById('cBkG').textContent.trim(),
  }));
  step(`KPI cards: ${JSON.stringify(counts)}`);
  step(`data info: ${dataInfo}`);

  // ── 3) Apply an origin filter (CN_SHA) ──────────────────────────────────
  step('apply filter: origin = CN_SHA');
  await page.selectOption('#fOrigin', 'CN_SHA');
  await page.waitForTimeout(500);
  await shot(page, '02_filter_origin_CN_SHA');

  const rowCount = await page.locator('table.dt tbody tr.row-clickable').count();
  step(`CN_SHA → ${rowCount} salesperson rows visible`);

  // ── 4) Click a salesperson row to drill ─────────────────────────────────
  step('click first row → drill to chunk');
  const firstRow = page.locator('table.dt tbody tr.row-clickable').first();
  const rowOrigin = await firstRow.getAttribute('data-origin');
  const rowSales = await firstRow.getAttribute('data-sales');
  step(`drilling: origin=${rowOrigin}, sales=${rowSales}`);
  await firstRow.click();
  await page.waitForSelector('.crumbs', { timeout: 5000 });
  await page.waitForSelector('table.dt tbody tr.row-clickable', { timeout: 8000 });
  await page.waitForTimeout(700);
  await shot(page, '03_drill_shipper_table');

  const shipperCount = await page.locator('table.dt tbody tr.row-clickable').count();
  step(`shipper rows: ${shipperCount}`);

  // ── 5) Expand a shipper row → BKG_NO list ───────────────────────────────
  step('click first shipper row → BKG_NO detail');
  await page.locator('table.dt tbody tr.row-clickable').first().click();
  await page.waitForSelector('tr.detail-row', { timeout: 5000 });
  await page.waitForTimeout(700);
  await shot(page, '04_drill_bkg_no_list');

  const bkgRows = await page.locator('tr.detail-row table.dt tbody tr').count();
  step(`BKG_NO rows expanded: ${bkgRows}`);

  // ── 6) Change profit filter to "HI" only ────────────────────────────────
  step('apply filter: 고수익만');
  await page.selectOption('#fProfit', 'HI');
  await page.waitForTimeout(800);
  await shot(page, '05_filter_high_profit');

  // ── 7) Change WOS filter to ALL ─────────────────────────────────────────
  step('apply filter: WOS=ALL');
  await page.selectOption('#fWos', 'ALL');
  await page.waitForTimeout(800);
  await shot(page, '06_filter_wos_all');

  // ── 8) Switch to Pivot view ─────────────────────────────────────────────
  step('switch view → pivot');
  await page.selectOption('#fProfit', 'ALL');
  await page.selectOption('#fWos', 'W3');
  await page.locator('.view-tabs .vtab[data-view="pivot"]').click();
  await page.waitForSelector('.pivot-config', { timeout: 5000 });
  await page.waitForTimeout(2000);
  await shot(page, '07_pivot_default');

  // ── 9) Change pivot row to pod_country, col to grade ────────────────────
  step('pivot: row=POD 국가, col=등급, metric=W3 FST');
  await page.selectOption('#pivotRow', 'pod_country');
  await page.selectOption('#pivotCol', 'grade');
  await page.selectOption('#pivotMetric', 'w3_fst');
  await page.waitForTimeout(1500);
  await shot(page, '08_pivot_pod_grade');

  // ── 10) Reset filters then back to summary ──────────────────────────────
  step('reset and return to Target Summary view');
  await page.locator('.view-tabs .vtab[data-view="summary"]').click();
  await page.locator('#btnReset').click();
  await page.waitForTimeout(700);
  await shot(page, '09_back_to_summary_reset');

  // ── 11) Try Q2 progress mode ────────────────────────────────────────────
  step('switch quarter → Q2 (Progress)');
  await page.selectOption('#fQuarter', 'q2');
  await page.waitForTimeout(700);
  await shot(page, '10_q2_progress');

  // ── 12) Deep link with URL params (simulating click from main dashboard) ─
  step('deep link via URL params (?origin=CN_SHA&sales=WUXIAOCHEN&quarter=q1&view=drill)');
  await page.goto(`${BASE}/sales-target/?origin=CN_SHA&sales=WUXIAOCHEN&quarter=q1&view=drill`, { waitUntil: 'networkidle' });
  await page.waitForSelector('.crumbs', { timeout: 8000 });
  await page.waitForSelector('table.dt tbody tr.row-clickable', { timeout: 8000 });
  await page.waitForTimeout(900);
  await shot(page, '11_deep_link_drill');

  // ── 13) Expand the "전체 매칭 BKG 보기" panel ───────────────────────
  step('expand all-matching BKG panel');
  await page.locator('.all-bkg-panel [data-action="toggle-all-bkg"]').first().click();
  await page.waitForTimeout(800);
  await page.evaluate(() => {
    const el = document.querySelector('.all-bkg-panel');
    if (el) el.scrollIntoView({ block: 'center' });
  });
  await page.waitForTimeout(400);
  await shot(page, '12_all_matching_bkg_panel');
  const allBkgRows = await page.locator('.all-bkg-panel .all-bkg-body table.dt tbody tr').count();
  step(`flat BKG rows: ${allBkgRows}`);

  // ── 14) Pivot cell click → BKG list ─────────────────────────────────
  step('switch to pivot, click a cell to show BKG list');
  await page.locator('.view-tabs .vtab[data-view="pivot"]').click();
  await page.waitForSelector('.pivot-config', { timeout: 5000 });
  await page.waitForTimeout(1500);
  await page.selectOption('#pivotRow', 'pod_country');
  await page.selectOption('#pivotCol', 'grade');
  await page.selectOption('#pivotMetric', 'w3_fst');
  await page.waitForTimeout(1500);
  const firstCellHandle = await page.$('.pivot-cell');
  if (firstCellHandle) {
    await firstCellHandle.click();
    await page.waitForTimeout(800);
  }
  await shot(page, '13_pivot_cell_drill');
  const pivotPanelRows = await page.locator('#pivotBkgPanel table.dt tbody tr').count();
  step(`pivot-cell BKG rows: ${pivotPanelRows}`);

  step(`console errors: ${consoleErrors.length}`);
  if (consoleErrors.length) consoleErrors.slice(0, 5).forEach(e => step(`  err: ${e}`));
  step(`request failures: ${requestFailures.length}`);
  if (requestFailures.length) requestFailures.slice(0, 5).forEach(e => step(`  fail: ${e}`));

  await page.close();
  await context.close();
  await browser.close();

  // Move the recorded video to a stable path (OneDrive may lock files; retry with copy+unlink)
  const recorded = fs.readdirSync(VIDEO_DIR).filter(f => f.endsWith('.webm'));
  if (recorded.length) {
    const src = path.join(VIDEO_DIR, recorded[0]);
    const dst = path.join(OUT, 'sales_target_verification.webm');
    try {
      if (fs.existsSync(dst)) fs.unlinkSync(dst);
      fs.copyFileSync(src, dst);
      try { fs.unlinkSync(src); } catch {}
      step(`video → ${dst}`);
    } catch (err) {
      step(`video copy failed: ${err.message}; source kept at ${src}`);
    }
  }
  fs.writeFileSync(path.join(OUT, 'verification_log.txt'), log.join('\n'), 'utf8');

  const report = {
    base_url: BASE,
    landing_kpis: counts,
    data_info: dataInfo,
    cn_sha_sales_rows: rowCount,
    drilled_origin: rowOrigin,
    drilled_sales: rowSales,
    shipper_rows: shipperCount,
    bkg_rows_in_first_shipper: bkgRows,
    console_errors: consoleErrors,
    request_failures: requestFailures,
  };
  fs.writeFileSync(path.join(OUT, 'verification_report.json'), JSON.stringify(report, null, 2), 'utf8');
  step('report → verification_report.json');
  return report;
}

main().then(r => {
  console.log('\n=== VERIFICATION REPORT ===');
  console.log(JSON.stringify(r, null, 2));
  process.exit(0);
}).catch(err => {
  console.error('verification failed:', err);
  process.exit(1);
});
