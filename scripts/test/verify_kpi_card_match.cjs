// Confirm: KPI rate cards match the Team Total row of the Target Summary
// when a single origin is filtered. Customer count is live-deduped and can differ.
const { chromium } = require('playwright');
const BASE = 'http://127.0.0.1:8749';

(async () => {
  const browser = await chromium.launch({ headless: true });
  const ctx = await browser.newContext({ viewport: { width: 1900, height: 920 } });
  await ctx.addInitScript(() => {
    sessionStorage.setItem('gtoken', 'STUB');
    sessionStorage.setItem('guser', JSON.stringify({ email: 'verifier@ekmtc.com', name: 'V' }));
  });
  const page = await ctx.newPage();
  const errors = [];
  page.on('console', m => { if (m.type() === 'error') errors.push(m.text()); });

  await page.goto(`${BASE}/sales-target/?country=VN&origins=VN_HPH`, { waitUntil: 'networkidle' });
  await page.waitForSelector('table.dt', { timeout: 8000 });
  await page.waitForTimeout(800);

  const result = await page.evaluate(() => {
    const grab = id => document.getElementById(id)?.textContent?.trim();
    const cards = {
      bkT: grab('cBkT'), bkP: grab('cBkP'), bkG: grab('cBkG'),
      lfT: grab('cLfT'), lfP: grab('cLfP'), lfG: grab('cLfG'),
      hpT: grab('cHpT'), hpP: grab('cHpP'), hpG: grab('cHpG'),
      cust: grab('cCust'),
    };
    const teamRow = [...document.querySelectorAll('table.dt tbody tr')]
      .find(tr => tr.children[1]?.textContent?.trim() === 'Team Total');
    const cells = teamRow ? [...teamRow.children].map(c => c.textContent.trim()) : null;
    return { cards, teamRow: cells };
  });
  console.log('cards :', JSON.stringify(result.cards));
  console.log('team  :', JSON.stringify(result.teamRow));
  // teamRow column layout: tab, name, '25 share, '25 W3/BSA,
  // bkT, bkP, bkAchv, bkG, lfT, lfP, lfAchv, lfG,
  // hpT, hpP, hpAchv, hpG, ac.total, ac.w3, ac.%
  if (result.teamRow) {
    const t = result.teamRow;
    const checks = [
      ['bkT', result.cards.bkT, t[4]],
      ['bkP', result.cards.bkP, t[5]],
      ['bkG', result.cards.bkG, t[7]],
      ['lfT', result.cards.lfT, t[8]],
      ['lfP', result.cards.lfP, t[9]],
      ['lfG', result.cards.lfG, t[11]],
      ['hpT', result.cards.hpT, t[12]],
      ['hpP', result.cards.hpP, t[13]],
      ['hpG', result.cards.hpG, t[15]],
    ];
    let ok = true;
    checks.forEach(([k, a, b]) => {
      const match = a === b;
      console.log(`${match ? '✓' : '✗'} ${k.padEnd(5)} card=${(a || '').padEnd(8)} team=${b}`);
      if (!match) ok = false;
    });
    console.log(`cust card=${result.cards.cust} team-total=${t[16]} (informational)`);
    console.log(ok ? '\nALL MATCH ✅' : '\nMISMATCH ⚠');
  }
  console.log('console errors:', errors.length);
  await ctx.close();
  await browser.close();
})();
