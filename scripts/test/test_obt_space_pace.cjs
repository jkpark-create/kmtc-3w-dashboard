const fs = require('fs');
const vm = require('vm');
const assert = require('assert/strict');
const path = require('path');
const context = vm.createContext({ window: {}, document: {addEventListener() {}}, console });
vm.runInContext(fs.readFileSync(path.join(__dirname, '../../dist/obt-exception-monitor/app.js'), 'utf8'), context);
vm.runInContext(`
state.raw = {data_date: '20260929'};
state.filters.sales = 'ALL';
state.spaceMeta = {};
state.history = {snapshots: ['20260908','20260915','20260922'].map(data_date => ({
 data_date, routes: [['CN|SHA|TH|BKK', weeksForSnapshotOffsets(data_date,[3])[0], 60,0,0,0,100]]
}))};
const row = {week:'20261042',weekStart:weeksForSnapshotOffsets('20260929',[3])[0],route:'R',vesselCode:'V',voyageNo:'1',bound:'E',departureDate:'20261020',priorCalls:[{port:'SHA',bsa_teu:100,booking_teu:20}],reusableTeu:30};
`, context);
const result = code => vm.runInContext(code, context);
assert.ok(Math.abs(result('assessSpacePace(row).lagTeu') - 40) < 1e-9);
assert.equal(result('summarizeSpaceOpportunities([row],{}).count'),1);
assert.equal(result('summarizeSpaceOpportunities([{...row,reusableTeu:29.99}],{}).count'),0);
assert.equal(result('summarizeSpaceOpportunities([{...row,priorCalls:[{port:"SHA",bsa_teu:100,booking_teu:70}]}],{}).count'),0);
assert.equal(result('summarizeSpaceOpportunities([row,{...row,reusableTeu:45}],{}).count'),2);
assert.equal(result('summarizeSpaceOpportunities([row,{...row,reusableTeu:45}],{}).reusableTeu'),45);
assert.equal(result('summarizeSpaceOpportunities([{...row,priorCalls:[]}],{}).unavailableCount'),1);
result('els.spaceOpportunity = {innerHTML:"",classList:{add(){},remove(){}}}; renderSpaceOpportunities(summarizeSpaceOpportunities(Array.from({length:8},(_,i)=>({...row,currentPort:"P"+i})),{}))');
assert.equal(result('els.spaceOpportunity.innerHTML.match(/class="space-opportunity-card"/g).length'),8);
assert.equal(result('summarizeSpaceOpportunities([{...row,priorCalls:[{port:"SHA",bsa_teu:100,booking_teu:60}]}],{}).count'),0);
result('state.history.snapshots.pop()');
assert.equal(result('assessSpacePace(row)'),null);
result('state.history = {snapshots:[]}');
assert.equal(result('assessSpacePace(row)'),null);
console.log('Space opportunity tests passed: pace, missing samples, threshold, all calls, deduplicated totals.');
