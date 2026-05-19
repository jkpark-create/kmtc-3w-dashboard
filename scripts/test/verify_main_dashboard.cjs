// Verify the main dashboard's bySales overlay (Tab 2 → bySales sub-tab).
// The main dashboard gates the UI behind Google OAuth, so this script:
//   1. Pre-populates sessionStorage with fake credentials before the page boots
//   2. Stubs the tokeninfo verification fetch so checkSession() resolves true
//   3. Waits for Tab 2 → bySales, then screenshots the patched table
// Requires the static server to be running on http://127.0.0.1:8749/.

const path = require('path');
const fs = require('fs');
const { chromium } = require('playwright');

const ROOT = path.resolve(__dirname, '..', '..');
const OUT = path.join(ROOT, 'verification-output');
const VIDEO_DIR = path.join(OUT, 'video_main');
const SHOTS_DIR = path.join(OUT, 'screenshots');
fs.mkdirSync(VIDEO_DIR, { recursive: true });
fs.mkdirSync(SHOTS_DIR, { recursive: true });

const BASE = process.env.SALES_TARGET_BASE || 'http://127.0.0.1:8749';

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
  // Inject before page boot: fake auth + stubbed Google tokeninfo verifier
  await context.addInitScript(() => {
    sessionStorage.setItem('gtoken', 'STUB-TOKEN-FOR-VERIFICATION');
    sessionStorage.setItem('guser', JSON.stringify({
      email: 'verifier@ekmtc.com',
      name: 'Verifier',
      picture: '',
    }));
    const origFetch = window.fetch;
    window.fetch = (url, opts) => {
      const u = typeof url === 'string' ? url : url.url;
      if (u && u.includes('oauth2/v1/tokeninfo')) {
        return Promise.resolve(new Response('{}', { status: 200 }));
      }
      if (u && u.includes('googleapis.com/drive')) {
        return Promise.resolve(new Response(JSON.stringify({ files: [] }), { status: 200 }));
      }
      return origFetch(url, opts);
    };
  });

  const page = await context.newPage();
  const consoleErrors = [];
  page.on('console', m => { if (m.type() === 'error') consoleErrors.push(m.text()); });
  page.on('requestfailed', r => step(`request failed: ${r.method()} ${r.url()} → ${r.failure()?.errorText}`));

  step(`navigate ${BASE}/index.html`);
  await page.goto(`${BASE}/index.html`, { waitUntil: 'networkidle' });
  await page.waitForFunction(() => document.getElementById('app') && document.getElementById('app').style.display === 'block', { timeout: 15000 });
  step('main app shown after auth bypass');
  await page.waitForSelector('.tabs .tab', { timeout: 10000 });

  // The main dashboard data.json is huge — wait for content render
  await page.waitForFunction(() => document.getElementById('loading') && document.getElementById('loading').style.display === 'none', { timeout: 60000 });
  await page.waitForTimeout(800);
  await shot(page, 'main_00_tab1_loaded');

  // Switch to Tab 2 (부킹 트렌드)
  step('switch to Tab 2 (부킹 트렌드)');
  await page.locator('.tabs .tab').nth(1).click();
  await page.waitForTimeout(700);
  await shot(page, 'main_01_tab2_default');

  // Click the bySales sub-tab — text "영업사원별"
  step('switch to sub-tab: 영업사원별');
  await page.evaluate(() => {
    const buttons = Array.from(document.querySelectorAll('div'));
    const target = buttons.find(b => b.textContent.trim() === '영업사원별' && b.onclick);
    if (target) target.click();
  });
  await page.waitForTimeout(1200);
  // Wait for the patched columns to populate (sales-target/index.json fetched in background)
  await page.waitForFunction(() => document.querySelector('th.tgt-head'), { timeout: 15000 });
  await page.waitForTimeout(500);
  await shot(page, 'main_02_by_sales_with_targets');

  // Scroll the salesman table into view if needed
  await page.evaluate(() => {
    const t = document.querySelector('th.tgt-head');
    if (t) t.scrollIntoView({ behavior: 'instant', block: 'center' });
  });
  await page.waitForTimeout(400);
  await shot(page, 'main_03_target_columns_close');

  // Capture how many rows have target data populated
  const stats = await page.evaluate(() => ({
    salesmanTableExists: !!document.querySelector('table.summary'),
    targetHeaders: document.querySelectorAll('th.tgt-head').length,
    tgtMiniCells: document.querySelectorAll('td.tgt-mini').length,
    drillLinks: document.querySelectorAll('a.tgt-link').length,
    targetMode: (document.querySelector('.chart-card h3') || {}).innerText || '',
  }));
  step(`bySales stats: ${JSON.stringify(stats)}`);

  // Hover one target cell to make sure tooltip styles apply
  await page.evaluate(() => {
    const cells = document.querySelectorAll('td.tgt-mini');
    if (cells.length > 4) cells[3].scrollIntoView({ block: 'center' });
  });
  await page.waitForTimeout(300);
  await shot(page, 'main_04_target_cells_focus');

  await page.close();
  await context.close();
  await browser.close();

  const recorded = fs.readdirSync(VIDEO_DIR).filter(f => f.endsWith('.webm'));
  if (recorded.length) {
    const src = path.join(VIDEO_DIR, recorded[0]);
    const dst = path.join(OUT, 'main_dashboard_verification.webm');
    try {
      if (fs.existsSync(dst)) fs.unlinkSync(dst);
      fs.copyFileSync(src, dst);
      try { fs.unlinkSync(src); } catch {}
      step(`video → ${dst}`);
    } catch (err) {
      step(`video copy failed: ${err.message}; source kept at ${src}`);
    }
  }
  fs.writeFileSync(path.join(OUT, 'verification_main_log.txt'), log.join('\n'), 'utf8');
  const report = {
    base_url: BASE,
    bySales_stats: stats,
    console_errors: consoleErrors.slice(0, 10),
  };
  fs.writeFileSync(path.join(OUT, 'verification_main_report.json'), JSON.stringify(report, null, 2), 'utf8');
  console.log('\n=== MAIN DASHBOARD REPORT ===');
  console.log(JSON.stringify(report, null, 2));
}

main().catch(err => {
  console.error('verification failed:', err);
  process.exit(1);
});
