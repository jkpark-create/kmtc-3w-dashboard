const path = require('path');
const fs = require('fs');
const { chromium } = require('playwright');
const ROOT = path.resolve(__dirname, '..', '..');
const SHOTS = path.join(ROOT, 'verification-output', 'screenshots');
fs.mkdirSync(SHOTS, { recursive: true });
const BASE = 'http://127.0.0.1:8749';
(async () => {
  const browser = await chromium.launch({ headless: true });
  const ctx = await browser.newContext({ viewport: { width: 1900, height: 920 } });
    await ctx.addInitScript(() => {
    sessionStorage.setItem('gtoken', 'STUB');
    sessionStorage.setItem('guser', JSON.stringify({ email: 'verifier@ekmtc.com', name: 'Verifier', picture: '' }));
  });
  const page = await ctx.newPage();
  const errs = [];
  page.on('console', m => { if (m.type() === 'error') errs.push(m.text()); });

  await page.goto(`${BASE}/sales-target/?country=CN&origins=CN_SHA`, { waitUntil: 'networkidle' });
  await page.waitForSelector('table.dt', { timeout: 8000 });
  await page.waitForTimeout(500);

  // 1) Filter order
  const labels = await page.evaluate(() => [...document.querySelectorAll('.filters .group label')].map(l => l.textContent.trim()));
  console.log('Filter order:', JSON.stringify(labels));

  // 2) Drill view → expand first shipper → get booking_date cell (KO)
  await page.locator('.view-tabs .vtab[data-view="drill"]').click();
  await page.waitForTimeout(400);
  // Open msSales and pick first sales
  await page.locator('#msSales .ms-toggle').click();
  await page.waitForTimeout(200);
  const firstSales = await page.locator('#msSales .ms-opt input').first();
  if (await firstSales.count()) {
    await firstSales.click();
    await page.waitForTimeout(300);
  }
  await page.locator('body').click({ position: { x: 5, y: 5 } });
  await page.waitForSelector('.crumbs', { timeout: 8000 });
  await page.waitForSelector('table.dt tbody tr.row-clickable', { timeout: 8000 });
  await page.locator('table.dt tbody tr.row-clickable').first().click();
  await page.waitForSelector('tr.detail-row', { timeout: 5000 });
  await page.waitForTimeout(400);
  const bkgDateKo = await page.evaluate(() => {
    const tr = document.querySelector('tr.detail-row table.dt tbody tr');
    if (!tr) return null;
    return tr.children[3]?.textContent?.trim();
  });
  console.log('Tab2 BKG Booking date (KO):', bkgDateKo);

  // 3) Toggle EN
  await page.click('#langBtn');
  await page.waitForTimeout(500);
  // The drill view re-renders so re-expand shipper
  await page.waitForSelector('table.dt tbody tr.row-clickable', { timeout: 8000 });
  await page.locator('table.dt tbody tr.row-clickable').first().click();
  await page.waitForSelector('tr.detail-row', { timeout: 5000 });
  await page.waitForTimeout(400);
  const bkgDateEn = await page.evaluate(() => {
    const tr = document.querySelector('tr.detail-row table.dt tbody tr');
    if (!tr) return null;
    return tr.children[3]?.textContent?.trim();
  });
  console.log('Tab2 BKG Booking date (EN):', bkgDateEn);

  // 4) Pivot cell drill — booking date in flat panel
  await page.locator('.view-tabs .vtab[data-view="pivot"]').click();
  await page.waitForSelector('.pivot-config', { timeout: 5000 });
  await page.waitForTimeout(1000);
  const firstCell = await page.$('.pivot-cell');
  if (firstCell) { await firstCell.click(); await page.waitForTimeout(800); }
  const pivotDateEn = await page.evaluate(() => {
    const t = document.querySelector('#pivotBkgPanel table.dt tbody tr');
    return t ? t.children[8]?.textContent?.trim() : null;
  });
  console.log('Pivot cell detail Booking (EN):', pivotDateEn);
  await page.screenshot({ path: path.join(SHOTS, '31_date_en_pivot.png'), fullPage: false });

  console.log('errors:', errs.length);
  errs.slice(0, 5).forEach(e => console.log('  err:', e));
  await ctx.close(); await browser.close();
})();
