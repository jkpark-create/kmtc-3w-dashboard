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
  const ctx = await browser.newContext({ viewport: { width: 1600, height: 920 } });
  const page = await ctx.newPage();
  const errors = [];
  page.on('console', m => { if (m.type() === 'error') errors.push(m.text()); });

  await page.goto(`${BASE}/sales-target/`, { waitUntil: 'networkidle' });
  await page.waitForSelector('#msDestCountry .ms-toggle', { timeout: 8000 });
  await page.waitForTimeout(600);

  // Check filter order
  const labels = await page.evaluate(() => [...document.querySelectorAll('.filters .group label')].map(l => l.textContent.trim()));
  console.log('Filter order:', JSON.stringify(labels));

  // Open dest country MS, pick CN
  await page.locator('#msDestCountry .ms-toggle').click();
  await page.waitForTimeout(200);
  const destCnAvailable = await page.locator('#msDestCountry .ms-opt input[value="CN"]').count();
  console.log('dest CN option count:', destCnAvailable);
  if (destCnAvailable) {
    await page.locator('#msDestCountry .ms-opt input[value="CN"]').click();
    await page.waitForTimeout(300);
  }
  await page.locator('body').click({ position: { x: 5, y: 5 } });
  await page.waitForTimeout(400);
  await page.screenshot({ path: path.join(SHOTS, '19_dest_filter_CN.png'), fullPage: false });

  // Check dest port options narrowed to CN
  await page.locator('#msDestPort .ms-toggle').click();
  await page.waitForTimeout(200);
  const destPortCount = await page.locator('#msDestPort .ms-opt').count();
  console.log('dest port options after CN filter:', destPortCount);
  await page.locator('body').click({ position: { x: 5, y: 5 } });
  await page.waitForTimeout(200);

  // Move to drill view to confirm dest filter applies to BKG aggregation
  await page.locator('.view-tabs .vtab[data-view="summary"]').click();
  await page.waitForTimeout(300);

  // Now visit the guide page and check side-toc layout
  await page.goto(`${BASE}/sales-target/guide.html`, { waitUntil: 'networkidle' });
  await page.waitForTimeout(500);
  const guide = await page.evaluate(() => ({
    sideToc: !!document.querySelector('.side-toc'),
    langBtn: !!document.getElementById('langBtn'),
    langBtnText: document.getElementById('langBtn')?.textContent,
    sideTocRect: (() => {
      const el = document.querySelector('.side-toc');
      if (!el) return null;
      const r = el.getBoundingClientRect();
      return { top: Math.round(r.top), right: Math.round(window.innerWidth - r.right), width: Math.round(r.width) };
    })(),
    headingsKo: document.querySelectorAll('h2.ko').length,
    headingsEn: document.querySelectorAll('h2.en').length,
  }));
  console.log('Guide:', JSON.stringify(guide, null, 2));
  await page.screenshot({ path: path.join(SHOTS, '20_guide_side_toc.png'), fullPage: false });

  // Toggle EN
  await page.click('#langBtn');
  await page.waitForTimeout(300);
  const enState = await page.evaluate(() => ({
    langBtnText: document.getElementById('langBtn').textContent,
    visibleKo: [...document.querySelectorAll('h2.ko')].filter(el => el.offsetParent !== null).length,
    visibleEn: [...document.querySelectorAll('h2.en')].filter(el => el.offsetParent !== null).length,
  }));
  console.log('EN toggle:', JSON.stringify(enState, null, 2));
  await page.screenshot({ path: path.join(SHOTS, '21_guide_en.png'), fullPage: false });

  console.log('console errors:', errors.length);
  errors.slice(0, 5).forEach(e => console.log('  err:', e));
  await ctx.close();
  await browser.close();
})();
