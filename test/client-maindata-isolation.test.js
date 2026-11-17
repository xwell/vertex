const assert = require('node:assert/strict');
const test = require('node:test');
const { loadModule, createLogger } = require('./helpers/load-module');
const tick = () => new Promise(resolve => setImmediate(resolve));
const data = label => ({ label, torrents: [], freeSpaceOnDisk: 1000, uploadSpeed: 1, downloadSpeed: 2 });

function setup (t) {
  const logger = createLogger(); const timers = [];
  const guard = loadModule('app/libs/maindata-request-guard.js', { './logger': logger }, {
    setTimeout: (callback, ms) => { const timer = { callback, ms }; timers.push(timer); return timer; },
    clearTimeout: () => {}
  });
  const client = id => ({ id, alias: id, _client: { type: 'qBittorrent', maindataRequestGuard: true },
    cookie: 'synthetic-cookie', lastCookie: Date.now() / 1000, trackerStatus: {}, status: true, errorCount: 0,
    maindata: data('baseline-' + id), client: { getMaindata: async () => data('new-' + id) } });
  const a = client('a'); const b = client('b'); const previous = global.runningClient;
  global.runningClient = { a, b };
  t.after(() => { global.runningClient = previous; });
  return { a, b, guard, timers };
}

test('two opted-in clients have independent generations, cancellation, snapshots and counters', async t => {
  const s = setup(t); let finish; let signal;
  s.a.client.getMaindata = (url, cookie, options) => { signal = options.signal; return new Promise(resolve => { finish = resolve; }); };
  const old = s.guard.execute(s.a); await tick();
  await s.guard.execute(s.b);
  assert.equal(s.guard.status(s.a).active, true); assert.equal(signal.aborted, false);
  s.guard.cancel(s.a, 'rollback');
  assert.deepEqual(await old, { maindataGuardIgnored: true });
  finish(data('obsolete-a')); await tick();
  assert.equal(s.a.maindata.label, 'baseline-a'); assert.equal(s.b.maindata.label, 'new-b');
  assert.equal(s.guard.status(s.a).ignored, 1); assert.equal(s.guard.status(s.b).succeeded, 1);
  assert.equal(s.guard.status(s.b).failed, 0);
});

test('one downloader failure neither cancels nor changes the other downloader', async t => {
  const s = setup(t); let finish;
  s.a.client.getMaindata = () => new Promise(resolve => { finish = resolve; });
  s.b.client.getMaindata = async () => { const error = new Error('synthetic'); error.code = 'ECONNRESET'; throw error; };
  const pending = s.guard.execute(s.a); await tick();
  await assert.rejects(s.guard.execute(s.b), { code: 'ECONNRESET' });
  finish(data('fresh-a')); await pending;
  assert.equal(s.a.status, true); assert.equal(s.a.maindata.label, 'fresh-a'); assert.equal(s.a.errorCount, 0);
  assert.equal(s.b.status, false); assert.equal(s.b.maindata, null); assert.equal(s.b.errorCount, 1);
});
