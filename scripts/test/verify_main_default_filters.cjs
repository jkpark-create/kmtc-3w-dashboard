const assert = require('assert');
const fs = require('fs');
const path = require('path');

const root = path.resolve(__dirname, '..', '..');
const index = fs.readFileSync(path.join(root, 'dist', 'index.html'), 'utf8');
const guide = fs.readFileSync(path.join(root, 'dist', 'guide.html'), 'utf8');

assert.match(
  index,
  /const _defMonth = `\$\{_now\.getFullYear\(\)\}\$\{String\(_now\.getMonth\(\)\+1\)\.padStart\(2,'0'\)\}`;/,
  'the initial month must use the current calendar month',
);
assert.doesNotMatch(index, /_sun\.setDate|_target\.setDate|setDate\(_sun\.getDate\(\) \+ 21\)/);
assert.match(index, /const cur = gvList\('fWeek'\);[\s\S]*el\._selected = cleanVals\(cur\)/);

assert.ok(
  guide.includes('오늘 날짜가 속한 월 및 전체 주차'),
  'the Korean guide must describe the new month/week defaults',
);
assert.ok(
  guide.includes('the current calendar month and all weeks'),
  'the English guide must describe the new month/week defaults',
);
assert.ok(!guide.includes('current week + 3 weeks ahead'));

console.log('Main dashboard default filter checks passed.');
