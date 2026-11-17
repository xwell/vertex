const assert = require('node:assert/strict');
const test = require('node:test');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const cron = require('node-cron');
const { loadModule, createLogger } = require('./helpers/load-module');
const source = fs.readFileSync(path.join(__dirname, '../tools/maintenance/daily-cleanup.js'), 'utf8');

function cleanup () {
  const logger = createLogger(); const removed = []; const now = Date.now();
  const fakeFs = {
    existsSync: () => true,
    readdirSync: dir => dir.endsWith('/logs') ? ['old.gz', 'new.gz', 'keep.log'] : ['old.torrent', 'new.torrent', 'keep.txt'],
    statSync: file => ({ mtime: new Date(file.includes('/old.') ? now - 4 * 86400000 : now) }),
    unlinkSync: file => removed.push(file)
  };
  const context = { logger, require: id => id === 'fs' ? fakeFs : require(id), Date };
  const fn = vm.runInNewContext(source, context, { timeout: 1000 });
  return { fn, context, removed, logger };
}

test('daily cleanup expression RETURNS a function and performs no cleanup during eval', () => {
  const s = cleanup(); assert.equal(typeof s.fn, 'function');
  assert.deepEqual(s.removed, []); assert.equal(s.logger.entries.length, 0);
});

test('daily cleanup retains original three-day age, extension and directory boundaries', async () => {
  const s = cleanup(); await s.fn();
  assert.deepEqual(s.removed, ['/vertex/logs/old.gz', '/vertex/torrents/old.torrent']);
  assert.equal(s.logger.entries.filter(x => x.level === 'error').length, 0);
});

test('scheduled Script invokes returned async function exactly once per callback, never on install', async t => {
  const s = cleanup(); let callback; let expression; let stops = 0;
  // Evaluate the exact production expression in an isolated context with synthetic fs only.
  const Script = loadModule('app/common/Script.js', {
    'node-cron': { schedule: (cron, fn) => { expression = cron; callback = fn; return { stop: () => { stops++; } }; } },
    '../libs/logger': s.logger,
    fs: s.context.require('fs')
  });
  const previous = global.runningScript; global.runningScript = {};
  t.after(() => { global.runningScript = previous; });
  const script = new Script({ id: 'synthetic', alias: 'cleanup', cron: '0 3 * * *', script: source });
  global.runningScript.synthetic = script;
  assert.equal(expression, '0 3 * * *'); assert.deepEqual(s.removed, []);
  await callback();
  assert.deepEqual(s.removed, ['/vertex/logs/old.gz', '/vertex/torrents/old.torrent']);
  assert.equal(s.logger.entries.filter(x => x.level === 'error').length, 0);
  script.destroy(); assert.equal(stops, 1); assert.equal(global.runningScript.synthetic, undefined);
});

test('daily cron is valid and matches once, rather than all sixty minutes of hour three', () => {
  assert.equal(cron.validate('0 3 * * *'), true);
  const TimeMatcher = require('node-cron/src/time-matcher');
  const daily = new TimeMatcher('0 3 * * *', 'Asia/Shanghai');
  const old = new TimeMatcher('* 3 * * *', 'Asia/Shanghai');
  let dailyMatches = 0; let oldMatches = 0;
  for (let minute = 0; minute < 60; minute++) {
    const date = new Date(Date.UTC(2026, 10, 17, 19, minute, 0));
    dailyMatches += Number(daily.match(date)); oldMatches += Number(old.match(date));
  }
  assert.equal(dailyMatches, 1); assert.equal(oldMatches, 60);
});
