const assert = require('node:assert/strict');
const test = require('node:test');
const { loadModule, createLogger } = require('./helpers/load-module');
const { createControl } = require('../app/libs/tracker-sync');
const tick = () => new Promise(resolve => setImmediate(resolve));
const deferred = () => { let resolve, reject; const promise = new Promise((a, b) => { resolve = a; reject = b; }); return { promise, resolve, reject }; };
const data = label => ({ label, torrents: [{ hash: 'synthetic', state: 'stalledUP', completed: 123 }], uploadSpeed: 100, downloadSpeed: 200, freeSpaceOnDisk: 1000 });

function setup (t, adapter = {}) {
  const timers = []; const cleared = new Set(); const logger = createLogger();
  const clock = { now: 1800000000000 }; const previousNow = Date.now;
  Date.now = () => clock.now;
  const previous = global.runningClient;
  const guard = loadModule('app/libs/maindata-request-guard.js', { './logger': logger }, {
    setTimeout: (callback, ms) => { const timer = { callback, ms }; timers.push(timer); return timer; },
    clearTimeout: timer => cleared.add(timer)
  });
  const client = { id: 'mock-only', alias: 'Synthetic', _client: { type: 'qBittorrent', maindataRequestGuard: true },
    cookie: 'test-cookie', lastCookie: clock.now / 1000, username: 'private-user', password: 'private-password', clientUrl: 'http://invalid.example/private',
    status: true, maindata: data('baseline'), errorCount: 0, trackerStatus: { synthetic: 'ok' },
    avgUploadSpeed: 10, avgDownloadSpeed: 20, lastMaindataSuccessTime: clock.now / 1000 - 1,
    client: { getMaindata: async () => data('fresh'), login: async () => 'new-cookie', ...adapter } };
  global.runningClient = { [client.id]: client };
  t.after(() => { global.runningClient = previous; Date.now = previousNow; });
  return { guard, client, timers, cleared, logger, clock };
}

function queueFor (s) {
  class TaskQueue { constructor (name) { this.name = name; } }
  const Queue = loadModule('app/libs/queues/ClientTaskQueue.js', {
    '../redis': { TaskQueue }, '../logger': s.logger, '../maindata-request-guard': s.guard
  }, {
    setTimeout: (callback, ms) => { const timer = { callback, ms }; s.timers.push(timer); return timer; },
    clearTimeout: timer => s.cleared.add(timer)
  });
  return new Queue();
}

test('guard is explicitly enabled only for an opted-in qBittorrent client', t => {
  const { guard, client } = setup(t);
  assert.equal(guard.enabled(client), true);
  for (const flag of [false, undefined, 'true', 1]) {
    client._client.maindataRequestGuard = flag;
    assert.equal(guard.enabled(client), false);
  }
  client._client = { type: 'Transmission', maindataRequestGuard: true };
  assert.equal(guard.enabled(client), false);
});

test('successful snapshot commit preserves fields, EMA, tracker status and exact request start', async t => {
  const { guard, client, clock, timers, cleared } = setup(t);
  const start = clock.now;
  client.client.getMaindata = async () => { clock.now += 500; return data('fresh'); };
  await guard.execute(client);
  assert.equal(client.maindata.label, 'fresh');
  assert.equal(client.maindata.freeSpaceOnDisk, 1000);
  assert.equal(client.maindata.seedingCount, 1);
  assert.equal(client.maindata.usedSpace, 123);
  assert.equal(client.maindata.torrents[0].trackerStatus, 'ok');
  assert.equal(client.avgUploadSpeed, 19);
  assert.equal(client.avgDownloadSpeed, 38);
  assert.equal(client.lastMaindataRequestStartedAt, start);
  assert.equal(client.lastMaindataSuccessTime, Math.floor(clock.now / 1000));
  assert.equal(client.status, true); assert.equal(client.errorCount, 0);
  assert.equal(guard.status(client).succeeded, 1);
  assert.equal(guard.status(client).active, false);
  assert.ok(timers.every(x => cleared.has(x)));
});

test('guard forwards remaining absolute deadline and cancellable signal to adapter', async t => {
  let options;
  const s = setup(t, { getMaindata: async (url, cookie, o) => { options = o; return data('fresh'); } });
  await s.guard.execute(s.client, { timeoutMs: 5000 });
  assert.equal(options.timeoutMs, 5000);
  assert.equal(options.signal.aborted, true);
  assert.equal(options.signal.listenerCount('abort'), 0);
});

test('HTTP deadline cannot be extended above original 30-second task bound', async t => {
  let options;
  const s = setup(t, { getMaindata: async (url, cookie, o) => { options = o; return data('fresh'); } });
  await s.guard.execute(s.client, { timeoutMs: 120000 });
  assert.equal(options.timeoutMs, 30000);
  assert.equal(s.timers[0].ms, 30000);
});

for (const bad of [0, -1, NaN, Infinity]) test('invalid deadline is rejected before HTTP: ' + bad, async t => {
  let calls = 0;
  const s = setup(t, { getMaindata: async () => { calls++; } });
  await assert.rejects(s.guard.execute(s.client, { timeoutMs: bad }), /invalid/);
  assert.equal(calls, 0);
});

for (const late of ['success', 'failure']) test('expired request cannot overwrite a subsequent healthy snapshot: late ' + late, async t => {
  const pending = deferred(); let calls = 0;
  const s = setup(t, { getMaindata: () => ++calls === 1 ? pending.promise : Promise.resolve(data('new')) });
  const old = s.guard.execute(s.client); const rejection = assert.rejects(old, { code: 'ETIMEDOUT' });
  await tick(); s.clock.now += 30000; s.timers[0].callback(); await rejection;
  assert.equal(s.client.status, false); assert.equal(s.client.errorCount, 1);
  await s.guard.execute(s.client);
  const successAt = s.client.lastMaindataSuccessTime;
  if (late === 'success') pending.resolve(data('old')); else pending.reject(new Error('late private failure'));
  await tick();
  assert.equal(s.client.status, true); assert.equal(s.client.maindata.label, 'new');
  assert.equal(s.client.lastMaindataSuccessTime, successAt); assert.equal(s.client.errorCount, 0);
  assert.equal(s.guard.status(s.client).timedOut, 1); assert.equal(s.guard.status(s.client).failed, 1);
});

for (const late of ['success', 'failure']) test('a newer generation supersedes a still-pending older ' + late, async t => {
  const pending = deferred(); let calls = 0;
  const s = setup(t, { getMaindata: () => ++calls === 1 ? pending.promise : Promise.resolve(data('new')) });
  const old = s.guard.execute(s.client); await tick();
  await s.guard.execute(s.client); assert.deepEqual(await old, { maindataGuardIgnored: true });
  if (late === 'success') pending.resolve(data('old')); else pending.reject(new Error('late'));
  await tick();
  assert.equal(s.client.maindata.label, 'new'); assert.equal(s.client.status, true);
  assert.equal(s.guard.status(s.client).failed, 0); assert.equal(s.guard.status(s.client).ignored, 1);
});

test('client replacement makes old success inert, including timestamps and averages', async t => {
  const pending = deferred(); const s = setup(t, { getMaindata: () => pending.promise });
  const old = s.guard.execute(s.client); await tick();
  const original = s.client; const replacement = { ...original, maindata: data('replacement') };
  global.runningClient[original.id] = replacement;
  pending.resolve(data('obsolete'));
  assert.deepEqual(await old, { maindataGuardIgnored: true });
  assert.equal(original.maindata.label, 'baseline'); assert.equal(replacement.maindata.label, 'replacement');
  assert.equal(original.lastMaindataSuccessTime, s.clock.now / 1000 - 1);
});

test('client replacement makes old failure inert', async t => {
  const pending = deferred(); const s = setup(t, { getMaindata: () => pending.promise });
  const old = s.guard.execute(s.client); await tick();
  global.runningClient[s.client.id] = { ...s.client };
  pending.reject(new Error('obsolete'));
  assert.deepEqual(await old, { maindataGuardIgnored: true });
  assert.equal(s.client.status, true); assert.equal(s.client.errorCount, 0);
});

for (const reason of ['client-destroyed', 'guard-disabled', 'rollback']) test('explicit cancellation is fenced and not recorded as a genuine failure: ' + reason, async t => {
  const pending = deferred(); let options;
  const s = setup(t, { getMaindata: (u, c, o) => { options = o; return pending.promise; } });
  const old = s.guard.execute(s.client); await tick(); s.guard.cancel(s.client, reason);
  assert.deepEqual(await old, { maindataGuardIgnored: true });
  assert.equal(options.signal.aborted, true); assert.equal(s.client.status, true);
  pending.resolve(data('late')); await tick(); assert.equal(s.client.maindata.label, 'baseline');
});

test('disabling opt-in while pending prevents commit', async t => {
  const pending = deferred(); const s = setup(t, { getMaindata: () => pending.promise });
  const old = s.guard.execute(s.client); await tick(); s.client._client.maindataRequestGuard = false;
  pending.resolve(data('late'));
  assert.deepEqual(await old, { maindataGuardIgnored: true }); assert.equal(s.client.maindata.label, 'baseline');
});

test('a delayed timer cannot make an after-deadline response fresh', async t => {
  const s = setup(t);
  s.client.client.getMaindata = async () => { s.clock.now += 30001; return data('late'); };
  await assert.rejects(s.guard.execute(s.client), { code: 'ETIMEDOUT' });
  assert.equal(s.client.status, false); assert.equal(s.client.maindata, null);
  assert.equal(s.client.lastMaindataSuccessTime, 1799999999);
});

test('already-cancelled outer signal starts no HTTP and removes its listener', async t => {
  let calls = 0; const signal = createControl(); signal.abort('rollback');
  const s = setup(t, { getMaindata: async () => { calls++; } });
  assert.deepEqual(await s.guard.execute(s.client, { signal }), { maindataGuardIgnored: true });
  assert.equal(calls, 0); assert.equal(signal.listenerCount('abort'), 0);
});

test('normal success leaves no timer or parent abort listener', async t => {
  const signal = createControl(); const s = setup(t);
  await s.guard.execute(s.client, { signal });
  assert.equal(signal.listenerCount('abort'), 0);
  assert.ok(s.timers.every(x => s.cleared.has(x)));
  signal.abort('late'); assert.equal(s.client.status, true);
});

test('current real failure remains fail-closed and increments error count only once', async t => {
  const failure = Object.assign(new Error('private-url/private-password'), { code: 'ECONNRESET' });
  const s = setup(t, { getMaindata: async () => { throw failure; } });
  await assert.rejects(s.guard.execute(s.client), { code: 'ECONNRESET' });
  assert.equal(s.client.maindata, null); assert.equal(s.client.status, false); assert.equal(s.client.errorCount, 1);
  const text = JSON.stringify(s.logger.entries);
  assert.ok(!text.includes('private-password') && !text.includes('private-url') && !text.includes('test-cookie'));
  assert.equal(s.guard.status(s.client).lastErrorCode, 'ECONNRESET');
});

for (const invalid of [null, {}, 'Unauthorized', { torrents: null }]) test('malformed/auth-rejected snapshot cannot partially commit: ' + JSON.stringify(invalid), async t => {
  const s = setup(t, { getMaindata: async () => invalid });
  await assert.rejects(s.guard.execute(s.client));
  assert.equal(s.client.maindata, null); assert.equal(s.client.lastMaindataSuccessTime, 1799999999);
});

test('Unauthorized performs one bounded login and retry under the SAME total deadline', async t => {
  let calls = 0; const budgets = [];
  const s = setup(t);
  s.client.client.getMaindata = async (u, c, o) => { budgets.push(o.timeoutMs); s.clock.now += 1000; return ++calls === 1 ? 'Unauthorized' : data('authorized'); };
  s.client.client.login = async (u, url, p, o) => { budgets.push(o.timeoutMs); s.clock.now += 1000; return 'new-cookie'; };
  await s.guard.execute(s.client);
  assert.deepEqual(budgets, [30000, 29000, 28000]);
  assert.equal(s.client.maindata.label, 'authorized'); assert.equal(s.client.cookie, 'new-cookie');
  assert.equal(s.guard.status(s.client).started, 1);
});

test('missing cookie and forced login are both bounded by guard', async t => {
  let logins = 0; const s = setup(t, { login: async () => { logins++; return 'new-cookie'; } });
  s.client.cookie = null; await s.guard.execute(s.client);
  await s.guard.execute(s.client, { forceLogin: true }); assert.equal(logins, 2);
});

test('late login response cannot replace newer cookie or snapshot', async t => {
  const pending = deferred(); const s = setup(t, { login: () => pending.promise });
  const old = s.guard.execute(s.client, { forceLogin: true }); await tick();
  await s.guard.execute(s.client); assert.deepEqual(await old, { maindataGuardIgnored: true });
  pending.resolve('obsolete-cookie'); await tick();
  assert.equal(s.client.cookie, 'test-cookie'); assert.equal(s.client.maindata.label, 'fresh');
});

for (const late of ['success', 'failure']) test('actual queue timeout + recovery fences old callback: ' + late, async t => {
  const pending = deferred(); let calls = 0;
  const s = setup(t, { getMaindata: () => ++calls === 1 ? pending.promise : Promise.resolve(data('new')) });
  const queue = queueFor(s); const task = { data: { clientId: s.client.id, action: 'getMaindata' }, priority: 'high' };
  const first = queue.executeTask(task); await tick(); s.clock.now += 30000;
  s.timers[0].callback(); await first; await tick();
  assert.equal(s.client.status, false); assert.equal(s.client.errorCount, 1);
  assert.equal(queue.failedClients.get(s.client.id).count, 1);
  assert.equal(queue.activeClientTasks.has(s.client.id), false);
  await queue.executeTask(task);
  if (late === 'success') pending.resolve(data('old')); else pending.reject(new Error('late'));
  await tick(); assert.equal(s.client.maindata.label, 'new'); assert.equal(s.client.status, true);
  assert.equal(queue.failedClients.has(s.client.id), false);
});

test('other non-opted-in client keeps legacy queue behavior and call signature', async t => {
  const s = setup(t); s.client._client.maindataRequestGuard = false;
  let args;
  s.client.client.getMaindata = async (...a) => { args = a; return data('legacy'); };
  await queueFor(s).executeTask({ data: { clientId: s.client.id, action: 'getMaindata' } });
  assert.equal(args.length, 2); assert.equal(s.client.maindata.label, 'legacy');
  assert.equal(s.guard.status(s.client).started, 0);
});

test('an ignored HALF_OPEN probe releases its in-flight flag without marking success/failure', async t => {
  const pending = deferred(); const s = setup(t, { getMaindata: () => pending.promise });
  const queue = queueFor(s);
  const probe = { state: 'HALF_OPEN', blocked: false, halfOpenInFlight: false, count: 1 };
  queue.failedClients.set(s.client.id, probe);
  const task = queue.executeTask({ data: { clientId: s.client.id, action: 'getMaindata' } });
  await tick(); assert.equal(probe.halfOpenInFlight, true);
  s.guard.cancel(s.client, 'rollback'); await task;
  assert.equal(probe.halfOpenInFlight, false); assert.equal(probe.state, 'HALF_OPEN'); assert.equal(probe.count, 1);
});

function inlineClient (s) {
  const Client = loadModule('app/common/Client.js', {
    '../libs/client/qb': {}, '../libs/client/de': {}, '../libs/client/tr': {}, '../libs/util': {},
    '../libs/redis': {}, '../libs/logger': s.logger, 'node-cron': {}, './Push': {},
    '../libs/queues/ClientTaskQueue': {}, '../libs/maindata-request-guard': s.guard, moment: () => ({ unix: () => s.clock.now / 1000 })
  });
  Object.setPrototypeOf(s.client, Client.prototype);
  Object.assign(s.client, { monitor: { push: false }, ntf: { connectClient: async () => {} }, mnt: {}, messageId: 1 });
  return Client;
}

test('inline Client.getMaindata uses the same guard and cannot resurrect a late old result', async t => {
  const pending = deferred(); let calls = 0;
  const s = setup(t, { getMaindata: () => ++calls === 1 ? pending.promise : Promise.resolve(data('new')) });
  inlineClient(s);
  const old = s.client.getMaindata(); await tick();
  await s.client.getMaindata(); await old;
  pending.resolve(data('old')); await tick(); assert.equal(s.client.maindata.label, 'new');
});

test('inline Client.login is bounded and genuine failure is not double-counted', async t => {
  const s = setup(t, { login: async () => { throw new Error('private login error'); } });
  inlineClient(s); await s.client.login();
  assert.equal(s.client.status, false); assert.equal(s.client.errorCount, 1);
  assert.equal(s.guard.status(s.client).failed, 1);
});

test('inline initialization retains connection notice after a bounded login and snapshot', async t => {
  const s = setup(t); inlineClient(s); let notices = 0;
  s.client.cookie = null; s.client.messageId = 0; s.client.ntf.connectClient = async () => { notices++; };
  await s.client.getMaindata();
  assert.equal(notices, 1); assert.equal(s.client.maindata.label, 'fresh'); assert.equal(s.client.cookie, 'new-cookie');
});

test('constructor-time inline request waits for same-tick instance registration before login', async t => {
  const s = setup(t); inlineClient(s); s.client.cookie = null; s.client.messageId = 0;
  let notices = 0; s.client.ntf.connectClient = async () => { notices++; };
  delete global.runningClient[s.client.id];
  const initialization = s.client.getMaindata();
  global.runningClient[s.client.id] = s.client;
  await initialization;
  assert.equal(s.client.status, true); assert.equal(s.client.maindata.label, 'fresh');
  assert.equal(s.client.cookie, 'new-cookie'); assert.equal(notices, 1);
  assert.equal(s.guard.status(s.client).succeeded, 1);
});

test('destroying an opted-in Client aborts its pending request without touching other clients', async t => {
  const pending = deferred(); const s = setup(t, { getMaindata: () => pending.promise });
  inlineClient(s);
  const original = s.client;
  original.maindataJob = { stop () {} }; original.recordJob = { stop () {} }; original.deleteRules = [];
  global.runningClient.other = { maindata: data('untouched'), status: true };
  const task = original.getMaindata(); await tick(); original.destroy(); await task;
  pending.resolve(data('late')); await tick();
  assert.equal(s.guard.status(original).active, false);
  assert.equal(original.maindata.label, 'baseline');
  assert.equal(global.runningClient[original.id], undefined);
  assert.equal(global.runningClient.other.maindata.label, 'untouched');
});

test('backward wall-clock input cannot stamp an obsolete response fresh', async t => {
  const s = setup(t);
  s.client.client.getMaindata = async () => { s.clock.now -= 1000; return data('backwards'); };
  await assert.rejects(s.guard.execute(s.client));
  assert.equal(s.client.maindata, null); assert.equal(s.client.lastMaindataSuccessTime, 1799999999);
});

test('only one summary per minute on successes; failures publish immediately without private fields', async t => {
  const s = setup(t); await s.guard.execute(s.client); await s.guard.execute(s.client);
  assert.equal(s.logger.entries.filter(x => String(x.args[0]).startsWith('MAINDATA_REQUEST_GUARD ')).length, 1);
  s.client.client.getMaindata = async () => { throw new Error('private-password'); };
  await assert.rejects(s.guard.execute(s.client));
  const logs = s.logger.entries.filter(x => String(x.args[0]).startsWith('MAINDATA_REQUEST_GUARD '));
  assert.equal(logs.length, 2); assert.ok(!JSON.stringify(logs).includes('private-password'));
});

test('qB adapter preserves legacy request path and forwards bounded guard deadline for GET/login', async () => {
  const calls = [];
  const util = { requestPromise: async (...args) => {
    calls.push(args);
    return args[0].url.endsWith('/login') ? { statusCode: 200, body: 'Ok.', headers: { 'set-cookie': ['sid=test; HttpOnly'] } }
      : { statusCode: 200, body: JSON.stringify({ server_state: {}, torrents: {} }) };
  } };
  const qb = loadModule('app/libs/client/qb.js', { '../util': util, '../logger': createLogger() });
  await qb.getMaindata('http://example.invalid', 'cookie');
  await qb.login('u', 'http://example.invalid', 'p');
  const deadline = { timeoutMs: 1234, signal: createControl() };
  await qb.getMaindata('http://example.invalid', 'cookie', deadline);
  await qb.login('u', 'http://example.invalid', 'p', deadline);
  assert.equal(calls[0].length, 1); assert.equal(calls[1].length, 1);
  assert.equal(calls[2][1], false); assert.equal(calls[2][2], deadline);
  assert.equal(calls[3][1], false); assert.equal(calls[3][2], deadline);
});
