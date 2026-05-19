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
  const ctx = await browser.newContext({ viewport: { width: 1700, height: 920 } });
  const page = await ctx.newPage();
  const errors = [];
  page.on('console', m => { if (m.type() === 'error') errors.push(m.text()); });

  await page.goto(`${BASE}/sales-target/?country=CN&origins=CN_SHA`, { waitUntil: 'networkidle' });
  await page.waitForSelector('#msCountry', { timeout: 8000 });
  await page.waitForTimeout(600);

  // Verify filter is on a single row
  const filterMeta = await page.evaluate(() => {
    const groups = [...document.querySelectorAll('.filters .group')];
    const ys = groups.map(g => Math.round(g.getBoundingClientRect().top));
    const unique = [...new Set(ys)];
    return { rowsCount: unique.length, groupTops: ys, filterRowCount: groups.length };
  });
  console.log('Filter rows:', JSON.stringify(filterMeta));

  // Switch to pivot view, default row=origin
  await page.locator('.view-tabs .vtab[data-view="pivot"]').click();
  await page.waitForSelector('.pivot-config', { timeout: 5000 });
  await page.waitForTimeout(1200);

  // Set row=origin, row2=salesman, col=yyyymm, metric=fst
  await page.selectOption('#pivotRow', 'origin');
  await page.waitForTimeout(200);
  await page.selectOption('#pivotRow2', 'salesman');
  await page.waitForTimeout(300);
  await page.selectOption('#pivotCol', 'yyyymm');
  await page.waitForTimeout(300);
  await page.waitForSelector('table.dt', { timeout: 5000 });
  await page.waitForTimeout(500);
  await page.screenshot({ path: path.join(SHOTS, '22_pivot_subrow.png'), fullPage: false });

  // Check sub-row markers
  const subRows = await page.locator('td:has-text("↳")').count();
  console.log('Sub-row rows shown:', subRows);

  // Toggle heatmap
  await page.click('#pivotHeatmap');
  await page.waitForTimeout(500);
  await page.screenshot({ path: path.join(SHOTS, '23_pivot_heatmap.png'), fullPage: false });
  const heatCells = await page.evaluate(() => [...document.querySelectorAll('td.pivot-cell')].filter(c => c.style.background && c.style.background.includes('rgba(26')).length);
  console.log('Heatmap-styled cells:', heatCells);

  // Switch to LFT% (ratio metric)
  await page.selectOption('#pivotMetric', 'lst_rate_w3');
  await page.waitForTimeout(500);
  await page.screenshot({ path: path.join(SHOTS, '24_pivot_lst_rate.png'), fullPage: false });
  const ratioCells = await page.evaluate(() => [...document.querySelectorAll('table.dt tbody tr td')].slice(1, 10).map(c => c.textContent));
  console.log('Ratio cells sample:', JSON.stringify(ratioCells));

  // Switch to fst + col%
  await page.selectOption('#pivotMetric', 'fst');
  await page.waitForTimeout(200);
  await page.selectOption('#pivotNorm', 'col');
  await page.waitForTimeout(500);
  await page.screenshot({ path: path.join(SHOTS, '25_pivot_col_pct.png'), fullPage: false });

  // Top N = 10
  await page.selectOption('#pivotTopN', '10');
  await page.waitForTimeout(500);
  await page.screenshot({ path: path.join(SHOTS, '26_pivot_topN.png'), fullPage: false });
  const topRows = await page.locator('table.dt tbody tr').count();
  console.log('Top-N rows visible (incl total + sub-rows):', topRows);

  // Click column header to sort by 202601 desc → asc
  await page.selectOption('#pivotNorm', 'value');
  await page.waitForTimeout(300);
  const firstCol = await page.locator('th[data-action="pivot-sort"]').first();
  if (await firstCol.count()) {
    await firstCol.click();
    await page.waitForTimeout(400);
    await firstCol.click();
    await page.waitForTimeout(400);
    await page.screenshot({ path: path.join(SHOTS, '27_pivot_sort_asc.png'), fullPage: false });
  }
  const sortText = await page.evaluate(() => document.querySelector('.panel #pivot-body > div')?.textContent || '');
  console.log('Sort note:', sortText.slice(0, 200));

  // Guide centered
  await page.goto(`${BASE}/sales-target/guide.html`, { waitUntil: 'networkidle' });
  await page.waitForTimeout(500);
  const guideLayout = await page.evaluate(() => {
    const c = document.querySelector('.container');
    if (!c) return null;
    const r = c.getBoundingClientRect();
    return { left: Math.round(r.left), right: Math.round(window.innerWidth - r.right), width: Math.round(r.width), vw: window.innerWidth, sideTocVisible: document.querySelector('.side-toc') ? getComputedStyle(document.querySelector('.side-toc')).display : 'none' };
  });
  console.log('Guide layout:', JSON.stringify(guideLayout));
  await page.screenshot({ path: path.join(SHOTS, '28_guide_centered.png'), fullPage: false });

  console.log('console errors:', errors.length);
  errors.slice(0, 5).forEach(e => console.log('  err:', e));
  await ctx.close();
  await browser.close();
})();
