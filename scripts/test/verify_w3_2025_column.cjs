const path = require('path');
const fs = require('fs');
const { chromium } = require('playwright');
const ROOT = path.resolve(__dirname, '..', '..');
const SHOTS = path.join(ROOT, 'verification-output', 'screenshots');
fs.mkdirSync(SHOTS, { recursive: true });
const BASE = 'http://127.0.0.1:8749';
(async () => {
  const browser = await chromium.launch({ headless: true });
  const ctx = await browser.newContext({ viewport: { width: 1700, height: 920 } });
    await ctx.addInitScript(() => {
    sessionStorage.setItem('gtoken', 'STUB');
    sessionStorage.setItem('guser', JSON.stringify({ email: 'verifier@ekmtc.com', name: 'Verifier', picture: '' }));
  });
  const page = await ctx.newPage();
  const errs = [];
  page.on('console', m => { if (m.type() === 'error') errs.push(m.text()); });
  await page.goto(`${BASE}/sales-target/?country=CN&origins=CN_SHA`, { waitUntil: 'networkidle' });
  await page.waitForSelector('table.dt', { timeout: 8000 });
  await page.waitForTimeout(600);
  const headers = await page.evaluate(() => [...document.querySelectorAll('table.dt thead tr:first-child th')].map(t => t.textContent.trim()));
  console.log('headers row1:', JSON.stringify(headers));
  const teamTotal = await page.evaluate(() => {
    const tr = [...document.querySelectorAll('table.dt tbody tr')].find(t => t.children[1]?.textContent?.trim() === 'Team Total');
    return tr ? [...tr.children].map(c => c.textContent.trim()) : null;
  });
  console.log('CN_SHA Team Total row:', JSON.stringify(teamTotal));
  await page.screenshot({ path: path.join(SHOTS, '29_w3_2025_column.png'), fullPage: false });
  console.log('errors:', errs.length);
  await ctx.close(); await browser.close();
})();
