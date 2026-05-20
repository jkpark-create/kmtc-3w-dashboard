const path = require('path');
const fs = require('fs');
const { chromium } = require('playwright');
const ROOT = path.resolve(__dirname, '..', '..');
const SHOTS = path.join(ROOT, 'verification-output', 'screenshots');
fs.mkdirSync(SHOTS, { recursive: true });
const BASE = 'http://127.0.0.1:8749';
(async () => {
  const browser = await chromium.launch({ headless: true });
  const ctx = await browser.newContext({ viewport: { width: 2010, height: 920 } });
    await ctx.addInitScript(() => {
    sessionStorage.setItem('gtoken', 'STUB');
    sessionStorage.setItem('guser', JSON.stringify({ email: 'verifier@ekmtc.com', name: 'Verifier', picture: '' }));
  });
  const page = await ctx.newPage();
  page.on('console', m => { if (m.type() === 'error') console.log('ERR:', m.text()); });
  await page.goto(`${BASE}/sales-target/`, { waitUntil: 'networkidle' });
  await page.waitForSelector('table.dt', { timeout: 8000 });
  await page.waitForTimeout(700);
  const widths = await page.evaluate(() => {
    const ths = [...document.querySelectorAll('table.dt thead tr:first-child th')];
    return ths.map(t => ({ label: t.textContent.trim().replace(/\s+/g, ' '), width: Math.round(t.getBoundingClientRect().width) }));
  });
  console.log('column widths:');
  widths.forEach(w => console.log('  ', w.label.padEnd(30), w.width + 'px'));
  await page.screenshot({ path: path.join(SHOTS, '30_w3_2025_narrowed.png'), fullPage: false });
  await ctx.close(); await browser.close();
})();
