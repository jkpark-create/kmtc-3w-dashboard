const { chromium } = require('playwright');
(async () => {
  const browser = await chromium.launch({ headless: true });
  const ctx = await browser.newContext();
  await ctx.addInitScript(() => {
    sessionStorage.setItem('gtoken', 'STUB');
    sessionStorage.setItem('guser', JSON.stringify({ email: 'verifier@ekmtc.com', name: 'Verifier', picture: '' }));
  });
  const page = await ctx.newPage();
  page.on('console', m => console.log(`[${m.type()}]`, m.text()));
  page.on('pageerror', e => console.log('[pageerror]', e.message, e.stack?.split('\n').slice(0,3).join(' | ')));
  await page.goto('http://127.0.0.1:8749/sales-target/', { waitUntil: 'networkidle' });
  await page.waitForTimeout(2000);
  const info = await page.evaluate(() => ({
    hasMsCountry: !!document.getElementById('msCountry'),
    msCountryHtml: document.getElementById('msCountry')?.innerHTML?.slice(0,200),
    cOrigin: document.getElementById('cOrigin')?.textContent,
    cSales: document.getElementById('cSales')?.textContent,
    panelHtml: document.getElementById('viewPanel')?.innerHTML?.slice(0,300),
  }));
  console.log(JSON.stringify(info, null, 2));
  await browser.close();
})();
