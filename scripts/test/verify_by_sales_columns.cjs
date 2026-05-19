// Focused check: confirm 구간별고수익 부킹 / % / 실선적 / 실선적률 columns populate.
// Captures a cropped close-up of the table + dumps the first 5 data rows as text/JSON.

const path = require('path');
const fs = require('fs');
const { chromium } = require('playwright');

const ROOT = path.resolve(__dirname, '..', '..');
const OUT = path.join(ROOT, 'verification-output');
const SHOTS = path.join(OUT, 'screenshots');
fs.mkdirSync(SHOTS, { recursive: true });
const BASE = 'http://127.0.0.1:8749';

(async () => {
  const browser = await chromium.launch({ headless: true });
  const ctx = await browser.newContext({ viewport: { width: 1800, height: 1000 } });
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
  await page.goto(`${BASE}/index.html`, { waitUntil: 'networkidle' });
  await page.waitForFunction(() => document.getElementById('app')?.style.display === 'block', { timeout: 15000 });
  await page.waitForFunction(() => document.getElementById('loading')?.style.display === 'none', { timeout: 60000 });

  // Switch to Tab 2 then bySales
  await page.locator('.tabs .tab').nth(1).click();
  await page.waitForTimeout(500);
  await page.evaluate(() => {
    const tab = Array.from(document.querySelectorAll('div')).find(b => b.textContent.trim() === '영업사원별' && b.onclick);
    if (tab) tab.click();
  });
  await page.waitForSelector('th.tgt-head', { timeout: 15000 });
  await page.waitForTimeout(800);

  // Read header texts + first 5 body row values
  const dump = await page.evaluate(() => {
    const table = document.querySelector('.chart-card table.summary');
    if (!table) return null;
    const headers = [...table.querySelectorAll('thead tr th, tr:first-child th')].map(th => th.innerText.trim());
    const rows = [...table.querySelectorAll('tr')].slice(1, 6).map(tr => [...tr.children].map(td => td.innerText.trim()));
    return { headers, rows };
  });
  console.log('headers:', JSON.stringify(dump.headers, null, 2));
  console.log('first 5 data rows:');
  dump.rows.forEach((r, i) => console.log(`  ${i}:`, JSON.stringify(r)));
  fs.writeFileSync(path.join(OUT, 'by_sales_columns_dump.json'), JSON.stringify(dump, null, 2), 'utf8');

  // Crop screenshot to the table area
  const tableEl = await page.$('.chart-card table.summary');
  if (tableEl) {
    await tableEl.scrollIntoViewIfNeeded();
    await page.waitForTimeout(300);
    await tableEl.screenshot({ path: path.join(SHOTS, 'main_05_table_crop.png') });
    console.log('cropped table saved → main_05_table_crop.png');
  }

  // Take a wider, top-aligned shot to capture header + first rows
  await page.evaluate(() => window.scrollTo(0, 0));
  await page.waitForTimeout(200);
  await page.screenshot({ path: path.join(SHOTS, 'main_06_full_wide.png'), fullPage: false });
  await page.waitForTimeout(200);

  await ctx.close();
  await browser.close();
})();
