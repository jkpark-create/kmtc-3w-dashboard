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
    await ctx.addInitScript(() => {
    sessionStorage.setItem('gtoken', 'STUB');
    sessionStorage.setItem('guser', JSON.stringify({ email: 'verifier@ekmtc.com', name: 'Verifier', picture: '' }));
  });
  const page = await ctx.newPage();
  const errors = [];
  page.on('console', m => { if (m.type() === 'error') errors.push(m.text()); });

  // 1) Sales target screen: toggle EN
  await page.goto(`${BASE}/sales-target/`, { waitUntil: 'networkidle' });
  await page.waitForSelector('#cOrigin');
  await page.waitForTimeout(500);
  await page.screenshot({ path: path.join(SHOTS, '14_lang_ko.png') });
  await page.click('#langBtn');
  await page.waitForTimeout(400);
  const en = await page.evaluate(() => ({
    langBtn: document.getElementById('langBtn').textContent,
    quarter: document.querySelector('label[data-i18n="fQuarter"]')?.textContent,
    country: document.querySelector('label[data-i18n="fCountry"]')?.textContent,
    sales: document.querySelector('label[data-i18n="fSales"]')?.textContent,
    title: document.querySelector('.panel-title')?.textContent,
  }));
  await page.screenshot({ path: path.join(SHOTS, '15_lang_en.png') });
  console.log('EN state:', JSON.stringify(en, null, 2));
  await page.click('#langBtn');
  await page.waitForTimeout(300);

  // 2) Sales target guide loads
  await page.goto(`${BASE}/sales-target/guide.html`, { waitUntil: 'networkidle' });
  await page.waitForTimeout(400);
  const guideInfo = await page.evaluate(() => ({
    title: document.title,
    headings: [...document.querySelectorAll('h2')].length,
    shotImgs: [...document.querySelectorAll('img')].length,
    brokenImgs: [...document.querySelectorAll('img')].filter(i => !i.complete || i.naturalWidth === 0).length,
  }));
  await page.screenshot({ path: path.join(SHOTS, '16_sales_target_guide.png'), fullPage: false });
  console.log('Sales-target guide:', JSON.stringify(guideInfo, null, 2));

  // 3) Main dashboard guide has sales-target section
  await page.goto(`${BASE}/guide.html`, { waitUntil: 'networkidle' });
  await page.waitForTimeout(400);
  const mainGuideInfo = await page.evaluate(() => {
    const el = document.getElementById('sales-target-link');
    return { hasSection: !!el };
  });
  await page.screenshot({ path: path.join(SHOTS, '17_main_guide_top.png'), fullPage: false });
  await page.evaluate(() => document.getElementById('sales-target-link')?.scrollIntoView());
  await page.waitForTimeout(300);
  await page.screenshot({ path: path.join(SHOTS, '17b_main_guide_sales_section.png'), fullPage: false });
  console.log('Main guide:', JSON.stringify(mainGuideInfo, null, 2));

  console.log('Console errors:', errors.length);
  errors.forEach(e => console.log('  err:', e));

  await ctx.close();
  await browser.close();
})();
