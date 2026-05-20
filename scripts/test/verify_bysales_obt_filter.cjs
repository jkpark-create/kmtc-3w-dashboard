// Validate the bySales table:
// 1) When team=OBT, KR/JP-based salespeople (CWSHIM, YOONSHIN) are excluded.
// 2) 합계 row sits ABOVE the data rows (first body row after the header).
const path = require('path');
const fs = require('fs');
const { chromium } = require('playwright');
const ROOT = path.resolve(__dirname, '..', '..');
const SHOTS = path.join(ROOT, 'verification-output', 'screenshots');
fs.mkdirSync(SHOTS, { recursive: true });
const BASE = 'http://127.0.0.1:8749';

(async () => {
  const browser = await chromium.launch({ headless: true });
  const ctx = await browser.newContext({ viewport: { width: 1920, height: 980 } });
  await ctx.addInitScript(() => {
    sessionStorage.setItem('gtoken', 'STUB');
    sessionStorage.setItem('guser', JSON.stringify({ email: 'verifier@ekmtc.com', name: 'Verifier', picture: '' }));
    const origFetch = window.fetch;
    window.fetch = (u, o) => {
      const url = typeof u === 'string' ? u : u.url;
      if (url.includes('oauth2/v1/tokeninfo')) return Promise.resolve(new Response('{}', { status: 200 }));
      if (url.includes('googleapis.com/drive')) return Promise.resolve(new Response('{"files":[]}', { status: 200 }));
      return origFetch(u, o);
    };
  });
  const page = await ctx.newPage();
  const errors = [];
  page.on('console', m => { if (m.type() === 'error') errors.push(m.text()); });

  await page.goto(`${BASE}/index.html`, { waitUntil: 'networkidle' });
  await page.waitForFunction(() => document.getElementById('app')?.style.display === 'block', { timeout: 15000 });
  await page.waitForFunction(() => document.getElementById('loading')?.style.display === 'none', { timeout: 60000 });

  // Make sure team filter is OBT
  await page.selectOption('#fTeam', 'OBT').catch(() => {});
  await page.waitForTimeout(400);

  // Switch to Tab 2 → 영업사원별
  await page.locator('.tabs .tab').nth(1).click();
  await page.waitForTimeout(400);
  await page.evaluate(() => {
    const t = Array.from(document.querySelectorAll('div')).find(b => b.textContent.trim() === '영업사원별' && b.onclick);
    if (t) t.click();
  });
  await page.waitForSelector('.chart-card table.summary tr', { timeout: 10000 });
  await page.waitForTimeout(600);

  const result = await page.evaluate(() => {
    const tbl = document.querySelector('.chart-card table.summary');
    const rows = [...tbl.querySelectorAll('tr')];
    // First row is the header (th), second should be 합계
    const headerCells = [...rows[0].querySelectorAll('th,td')].map(t => t.innerText.trim().replace(/\s+/g, ' '));
    const secondRow = rows[1] ? [...rows[1].children].map(c => c.innerText.trim().replace(/\s+/g, ' ')) : null;
    const allNames = rows.slice(1).map(r => r.children[0]?.innerText?.trim()).filter(Boolean);
    return {
      headerCells,
      secondRow,
      hasCWSHIM: allNames.includes('CWSHIM'),
      hasYOONSHIN: allNames.includes('YOONSHIN'),
      hasWUXIAOCHEN: allNames.includes('WUXIAOCHEN'),
      hasJAMESWANG: allNames.includes('JAMESWANG'),
      rowsCount: rows.length,
      totalIsTopBody: rows[1]?.classList.contains('total'),
      obt_salesmen_in_data: !!(window.DATA && Array.isArray(window.DATA.obt_salesmen)),
      obt_count: window.DATA?.obt_salesmen?.length || 0,
    };
  });
  console.log(JSON.stringify(result, null, 2));
  await page.screenshot({ path: path.join(SHOTS, '32_bysales_obt_filtered.png'), fullPage: false });

  console.log('errors:', errors.length);
  errors.slice(0, 5).forEach(e => console.log('  err:', e));
  await ctx.close();
  await browser.close();
})();
