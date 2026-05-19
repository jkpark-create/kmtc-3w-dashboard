// Reproduce the user's scenario: select 3 MY ports (PEN, PGU, PKG+PKW) and
// many salespeople, then capture the panel header to verify it no longer
// shows the long comma-separated scope list.

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
  const ctx = await browser.newContext({ viewport: { width: 1480, height: 920 } });
  const page = await ctx.newPage();
  const errors = [];
  page.on('console', m => { if (m.type() === 'error') errors.push(m.text()); });

  await page.goto(`${BASE}/sales-target/?quarter=q2&country=MY&origins=PEN,PGU,PKG%2BPKW&view=drill`, { waitUntil: 'networkidle' });
  await page.waitForSelector('#cOrigin', { timeout: 15000 });

  // Open sales multi-select and pick all
  await page.locator('#msSales .ms-toggle').click();
  await page.waitForTimeout(200);
  await page.locator('#msSales [data-action="all"]').click();
  await page.waitForTimeout(200);
  await page.locator('body').click({ position: { x: 5, y: 5 } });
  await page.waitForTimeout(800);

  await page.waitForSelector('.panel-title', { timeout: 8000 });
  const panel = await page.evaluate(() => {
    const title = document.querySelector('.panel-title')?.textContent || '';
    const actions = document.querySelector('.panel-actions')?.textContent || '';
    return { title, actions };
  });
  console.log('panel-title:', JSON.stringify(panel.title));
  console.log('panel-actions:', JSON.stringify(panel.actions));
  console.log('title length:', panel.title.length);

  await page.screenshot({ path: path.join(SHOTS, '18_my_multi_scope_title.png'), fullPage: false });

  // Try crumbs which is the canonical scope display now
  const crumbs = await page.evaluate(() => document.querySelector('.crumbs')?.textContent || '');
  console.log('crumbs:', JSON.stringify(crumbs.slice(0, 200)));

  console.log('console errors:', errors.length);
  await ctx.close();
  await browser.close();
})();
