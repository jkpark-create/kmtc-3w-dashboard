// Verify that /sales-target/ now requires login:
// 1) Unauthenticated visit → redirect to '..' (main dashboard root)
// 2) Authenticated visit (sessionStorage stubbed) → renders normally
// 3) Non-ekmtc.com user → also redirected
// 4) Guide page same gate
const path = require('path');
const fs = require('fs');
const { chromium } = require('playwright');
const ROOT = path.resolve(__dirname, '..', '..');
const SHOTS = path.join(ROOT, 'verification-output', 'screenshots');
fs.mkdirSync(SHOTS, { recursive: true });
const BASE = 'http://127.0.0.1:8749';

async function run(scenario, init) {
  const browser = await chromium.launch({ headless: true });
  const ctx = await browser.newContext({ viewport: { width: 1280, height: 800 } });
  if (init) await ctx.addInitScript(init);
  const page = await ctx.newPage();
  await page.goto(`${BASE}/sales-target/`, { waitUntil: 'load' });
  await page.waitForTimeout(700);
  const url = page.url();
  const title = await page.title();
  const visible = await page.evaluate(() => document.documentElement.classList.contains('auth-pending') ? 'hidden' : 'visible');
  console.log(`[${scenario}] url=${url} title="${title}" visibility=${visible}`);
  await ctx.close();
  await browser.close();
}

(async () => {
  // 1) No session — should bounce to /
  await run('unauth', null);
  // 2) Valid ekmtc user — should stay
  await run('ekmtc', () => {
    sessionStorage.setItem('gtoken', 'STUB');
    sessionStorage.setItem('guser', JSON.stringify({ email: 'verifier@ekmtc.com', name: 'V' }));
  });
  // 3) Non-ekmtc user — should bounce
  await run('outside-domain', () => {
    sessionStorage.setItem('gtoken', 'STUB');
    sessionStorage.setItem('guser', JSON.stringify({ email: 'a@gmail.com', name: 'X' }));
  });

  // 4) Guide page: unauth visit → also bounced
  const browser = await chromium.launch({ headless: true });
  const ctx = await browser.newContext();
  const page = await ctx.newPage();
  await page.goto(`${BASE}/sales-target/guide.html`, { waitUntil: 'load' });
  await page.waitForTimeout(700);
  console.log(`[guide-unauth] landed at ${page.url()}`);
  await ctx.close();
  await browser.close();
})();
