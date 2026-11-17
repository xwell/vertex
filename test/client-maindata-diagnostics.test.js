const assert = require('node:assert/strict');
const test = require('node:test');
const { EventEmitter } = require('node:events');
const { loadModule, createLogger } = require('./helpers/load-module');
const { createControl } = require('../app/libs/tracker-sync');
const tick = () => new Promise(resolve => setImmediate(resolve));
const data = () => ({ torrents: [], uploadSpeed: 0, downloadSpeed: 0 });
function setup (t) {
  const oldNow = Date.now, previous = global.runningClient, clock = { now: 1800000000000 };
  Date.now = () => clock.now; const logger = createLogger(), timers = [];
  const diagnostics = loadModule('app/libs/maindata-request-diagnostics.js', { './logger': logger });
  const guard = loadModule('app/libs/maindata-request-guard.js', { './logger': logger, './maindata-request-diagnostics': diagnostics }, {
    setTimeout: (fn, ms) => { const timer = { fn, ms }; timers.push(timer); return timer; }, clearTimeout () {}
  });
  const client = { id: 'synthetic-only', _client: { type: 'qBittorrent', maindataRequestGuard: true, maindataRequestDiagnostics: true },
    clientUrl: 'http://private-host.invalid/secret-path', username: 'private-user', password: 'private-password', cookie: 'private-cookie',
    lastCookie: clock.now / 1000, trackerStatus: {}, status: true, errorCount: 0,
    client: { getMaindata: async () => data(), login: async () => 'new-private-cookie' } };
  global.runningClient = { [client.id]: client };
  t.after(() => { Date.now = oldNow; global.runningClient = previous; });
  return { client, diagnostics, guard, clock, logger, timers, logs: () => logger.entries.filter(x => String(x.args[0]).startsWith('MAINDATA_REQUEST_DIAGNOSTIC ')).map(x => JSON.parse(x.args[0].split('MAINDATA_REQUEST_DIAGNOSTIC ')[1])) };
}
function observed (s, kind, options, fail = false) {
  const d = s.diagnostics.requestOptions(s.client.clientUrl, options, kind);
  assert.equal(d.timeoutMs, options.timeoutMs); assert.equal(d.signal, options.signal);
  d.observe({ type: 'socket', reused: false }); s.clock.now += 70; d.observe({ type: 'connect' });
  s.clock.now += 30; d.observe({ type: 'headers', statusCode: fail ? 403 : 200 });
  d.observe({ type: 'bytes', bytes: 1234 }); s.clock.now += 50;
  d.observe({ type: 'end', outcome: fail ? 'failure' : 'success' }); return d;
}

test('diagnostics require independent explicit opt-in and do not add hooks for ordinary clients', async t => {
  const s = setup(t); delete s.client._client.maindataRequestDiagnostics; let opts;
  s.client.client.getMaindata = async (u, c, o) => { opts = o; return data(); };
  await s.guard.execute(s.client); assert.equal(opts.observe, undefined); assert.equal(s.logs().length, 0);
  assert.equal(s.diagnostics.queueData(s.client, { action: 'getMaindata' }).maindataDiagnostic, undefined);
  assert.equal(s.guard.status(s.client).succeeded, 1);
});

test('successful diagnostic records socket/connect/headers/bytes with unchanged deadline and no secrets', async t => {
  const s = setup(t); s.client.client.getMaindata = async (u, c, o) => { observed(s, 'main-get', o); return data(); };
  await s.guard.execute(s.client, { timeoutMs: 5000 }); const log = s.logs()[0], stage = log.stages[0];
  assert.equal(log.outcome, 'success'); assert.equal(log.budgetMs, 5000); assert.equal(stage.budgetMs, 5000);
  assert.equal(stage.connectMs, 70); assert.equal(stage.headersMs, 100); assert.equal(stage.bytes, 1234); assert.equal(stage.endedMs, 150);
  assert.equal(stage.phase, 'reading-body'); assert.equal(stage.ended, true);
  assert.ok(!JSON.stringify(log).includes('private-')); assert.equal(s.guard.status(s.client).succeeded, 1);
});

test('login and Unauthorized retry stages remain inside the same original absolute budget', async t => {
  const s = setup(t); s.client.cookie = null; let calls = 0;
  s.client.client.login = async (u, url, p, o) => { observed(s, 'login', o); return 'new-private-cookie'; };
  s.client.client.getMaindata = async (u, c, o) => { observed(s, 'main-get', o); return ++calls === 1 ? 'Unauthorized' : data(); };
  await s.guard.execute(s.client); const stages = s.logs()[0].stages;
  assert.deepEqual(stages.map(x => x.kind), ['login', 'main-get', 'login', 'main-get']);
  assert.deepEqual(stages.map(x => x.budgetMs), [30000, 29850, 29700, 29550]);
  assert.deepEqual(stages.map(x => x.attempt), [1, 1, 2, 2]); assert.equal(s.guard.status(s.client).started, 1);
});

for (const phase of ['connecting', 'waiting-headers', 'reading-body']) test('real timeout reports unfinished phase: ' + phase, async t => {
  const s = setup(t); let o;
  s.client.client.getMaindata = async (u, c, options) => {
    o = s.diagnostics.requestOptions(u, options, 'main-get'); o.observe({ type: 'socket', reused: false });
    if (phase !== 'connecting') o.observe({ type: 'connect' });
    if (phase === 'reading-body') { o.observe({ type: 'headers', statusCode: 200 }); o.observe({ type: 'bytes', bytes: 10 }); }
    return await new Promise(() => {});
  };
  const p = s.guard.execute(s.client); const failure = assert.rejects(p, { code: 'ETIMEDOUT' }); await tick();
  s.clock.now += 30000; s.timers[0].fn(); await failure; const log = s.logs()[0];
  assert.equal(log.outcome, 'failure'); assert.equal(log.code, 'ETIMEDOUT'); assert.equal(log.stages[0].phase, phase);
  assert.equal(s.client.status, false); assert.equal(s.guard.status(s.client).failed, 1);
  o.observe({ type: 'bytes', bytes: 999 }); assert.equal(s.diagnostics.status(s.client).lastFailure.stages[0].bytes, phase === 'reading-body' ? 10 : 0);
});

test('diagnostic observer data cannot inject URL/error text or additional arbitrary queue fields', async t => {
  const s = setup(t);
  s.client.client.getMaindata = async (u, c, o) => {
    const d = s.diagnostics.requestOptions(u, o, 'main-get');
    d.observe({ type: 'headers', statusCode: 200, url: 'private-url', cookie: 'private-cookie' });
    throw Object.assign(new Error('private-password'), { code: 'ECONNRESET' });
  };
  await assert.rejects(s.guard.execute(s.client, { diagnosticQueue: { totalWaitMs: 123, secret: 'private-queue', lastBusyAction: 'private-action', busyRetries: 999999 } }));
  const text = JSON.stringify(s.logs()); assert.ok(!text.includes('private-'));
  assert.equal(s.logs()[0].queue.busyRetries, 65535); assert.equal(s.logs()[0].queue.lastBusyAction, null);
});

test('success diagnostics log at most once per minute and genuine failures immediately', async t => {
  const s = setup(t); await s.guard.execute(s.client); await s.guard.execute(s.client); assert.equal(s.logs().length, 1);
  s.client.client.getMaindata = async () => { throw new Error('secret'); };
  await assert.rejects(s.guard.execute(s.client)); assert.equal(s.logs().length, 2);
  s.client.client.getMaindata = async () => data(); s.clock.now += 60000; await s.guard.execute(s.client);
  assert.equal(s.logs().length, 3); assert.equal(s.diagnostics.status(s.client).started, 4);
});

test('diagnostic logger failure never changes successful snapshot commit or failure semantics', async t => {
  const s = setup(t); const legacyInfo = s.logger.info;
  s.logger.info = (...args) => { if (String(args[0]).startsWith('MAINDATA_REQUEST_DIAGNOSTIC ')) throw new Error('logger failed'); legacyInfo(...args); };
  await s.guard.execute(s.client); assert.equal(s.client.status, true); assert.equal(s.guard.status(s.client).succeeded, 1);
});

test('superseded requests cannot mutate another generation diagnostic stage or publish a failure', async t => {
  const s = setup(t); let calls = 0, observer, release;
  s.client.client.getMaindata = async (u, c, o) => {
    const d = s.diagnostics.requestOptions(u, o, 'main-get');
    if (++calls === 1) { observer = d.observe; return await new Promise(resolve => { release = resolve; }); }
    observed(s, 'main-get', o); return data();
  };
  const first = s.guard.execute(s.client); await tick(); await s.guard.execute(s.client);
  assert.deepEqual(await first, { maindataGuardIgnored: true });
  observer({ type: 'bytes', bytes: 999999 }); release(data()); await tick();
  assert.equal(s.guard.status(s.client).failed, 0); assert.equal(s.diagnostics.status(s.client).ignored, 1);
});

test('busy retries retain original enqueue time without modifying priority, dedupe or scheduling', t => {
  const s = setup(t), raw = { action: 'getMaindata', clientId: s.client.id, params: { synthetic: true } };
  const initial = s.diagnostics.queueData(s.client, raw); assert.equal(raw.maindataDiagnostic, undefined);
  const task = { data: initial, timestamp: s.clock.now, priority: 'high' }; s.clock.now += 2000;
  s.diagnostics.busy(s.client, task, { action: 'autoDelete', startTime: s.clock.now - 1000 });
  s.clock.now += 5000; const retry = s.diagnostics.queueData(s.client, task.data);
  assert.equal(retry.maindataDiagnostic.enqueuedAt, initial.maindataDiagnostic.enqueuedAt);
  const info = s.diagnostics.queueInfo(s.client, { data: retry, timestamp: s.clock.now - 100 });
  assert.equal(info.totalWaitMs, 7000); assert.equal(info.lastQueueWaitMs, 100); assert.equal(info.busyRetries, 1);
  assert.equal(info.lockAgeMaxMs, 1000); assert.equal(info.lastBusyAction, 'autoDelete'); assert.equal(task.priority, 'high');
});

test('queue statistics are bounded and include existing autoDelete consumption delay separately from HTTP duration', t => {
  const s = setup(t); const d = s.diagnostics.queueData(s.client, { action: 'autoDelete' });
  s.clock.now += 5000; s.diagnostics.queueInfo(s.client, { data: d, timestamp: s.clock.now - 50 });
  s.diagnostics.queueFinished(s.client, 'autoDelete', s.clock.now - 1000);
  const status = s.diagnostics.status(s.client); assert.equal(status.queueByAction.autoDelete.maxWaitMs, 5000);
  assert.equal(status.queueByAction.autoDelete.maxRunMs, 1000); assert.equal(status.queueWaitMaxMs, 5000);
});

test('async request correlation does not attach unrelated concurrent requests even for the same URL', async t => {
  const s = setup(t); let release;
  s.client.client.getMaindata = async (u, c, o) => {
    assert.equal(typeof s.diagnostics.requestOptions(u, o, 'main-get').observe, 'function');
    return await new Promise(resolve => { release = resolve; });
  };
  const pending = s.guard.execute(s.client); await tick();
  const options = { timeoutMs: 1000, signal: createControl() };
  assert.equal(s.diagnostics.requestOptions(s.client.clientUrl, options, 'main-get'), options);
  release(data()); await pending;
});

test('diagnostic stage lists stay capped at four and never alter adapter retry count', async t => {
  const s = setup(t);
  s.client.client.getMaindata = async (u, c, o) => {
    for (let i = 0; i < 20; i++) s.diagnostics.requestOptions(u, o, 'main-get');
    return data();
  };
  await s.guard.execute(s.client); assert.equal(s.logs()[0].stages.length, 4);
  assert.equal(s.guard.status(s.client).started, 1);
});

test('actual queue busy retry keeps task metadata and passes measured wait to the guarded request', async t => {
  const s = setup(t);
  class TaskQueue { constructor () {} async enqueue (data, priority) { this.last = { data, priority }; } }
  const Queue = loadModule('app/libs/queues/ClientTaskQueue.js', {
    '../redis': { TaskQueue }, '../logger': s.logger, '../maindata-request-guard': s.guard, '../maindata-request-diagnostics': s.diagnostics
  }, { setTimeout: (fn, ms) => { const timer = { fn, ms }; s.timers.push(timer); return timer; }, clearTimeout () {} });
  const queue = new Queue(); const task = { data: { action: 'getMaindata', clientId: s.client.id }, timestamp: s.clock.now - 1000, priority: 'high' };
  queue.activeClientTasks.set(s.client.id, { action: 'autoDelete', startTime: s.clock.now - 500 });
  await queue.executeTask(task); assert.equal(s.timers[0].ms, 5000); assert.equal(task.data.maindataDiagnostic.busyRetries, 1);
  queue.activeClientTasks.delete(s.client.id); s.clock.now += 5000;
  await queue.executeTask(task); const log = s.logs()[0];
  assert.equal(log.queue.busyRetries, 1); assert.equal(log.queue.lastBusyAction, 'autoDelete'); assert.equal(log.queue.totalWaitMs, 6000);
  assert.equal(queue.activeClientTasks.has(s.client.id), false); assert.equal(s.guard.status(s.client).succeeded, 1);
});

function transport (t) {
  let callback; const pending = new EventEmitter(); pending.aborted = false; pending.abort = () => { pending.aborted = true; };
  const timers = []; const fn = loadModule('app/libs/request-with-deadline.js', { request: (options, cb) => { callback = cb; return pending; } }, {
    setTimeout: (fn, ms) => { const v = { fn, ms }; timers.push(v); return v; }, clearTimeout () {}
  });
  return { fn, pending, timers, finish: (...args) => callback(...args) };
}

test('transport observes bounded fields and removes every diagnostic listener on completion', async t => {
  const x = transport(t), events = [], control = createControl();
  const p = x.fn({}, { timeoutMs: 1000, signal: control, observe: x => events.push(x) });
  const socket = new EventEmitter(); socket.connecting = true; const response = new EventEmitter(); response.statusCode = 200;
  x.pending.emit('socket', socket); socket.emit('connect'); x.pending.emit('response', response); response.emit('data', Buffer.from('abc'));
  x.finish(null, { statusCode: 200 }); await p;
  assert.deepEqual(events.map(x => x.type), ['socket', 'connect', 'headers', 'bytes', 'end']);
  assert.equal(events[3].bytes, 3); assert.equal(socket.listenerCount('connect'), 0); assert.equal(response.listenerCount('data'), 0);
  assert.equal(x.pending.listenerCount('socket'), 0); assert.equal(x.pending.listenerCount('response'), 0); assert.equal(control.listenerCount('abort'), 0);
});

test('transport retains exact off-path without diagnostic socket/response listeners', async t => {
  const x = transport(t); const p = x.fn({}, { timeoutMs: 1000 });
  assert.equal(x.pending.listenerCount('socket'), 0); assert.equal(x.pending.listenerCount('response'), 0);
  x.finish(null, { statusCode: 200 }); await p;
});

test('transport observer exceptions cannot change success, cancellation or real socket abort', async t => {
  const x = transport(t), control = createControl();
  const p = x.fn({}, { timeoutMs: 1000, signal: control, observe () { throw new Error('observer'); } });
  x.pending.emit('socket', { connecting: false }); control.abort('deadline');
  await assert.rejects(p, { code: 'EABORTED' }); assert.equal(x.pending.aborted, true);
});

test('loopback real HTTP body is observed without changing the returned body or socket cleanup', async t => {
  const http = require('http'), request = require('../app/libs/request-with-deadline'), events = [];
  const server = http.createServer((req, res) => { res.write('abc'); setTimeout(() => res.end('def'), 20); });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise(resolve => server.close(resolve)));
  const result = await request({ url: 'http://127.0.0.1:' + server.address().port }, { timeoutMs: 2000, observe: e => events.push(e) });
  assert.equal(result.body, 'abcdef'); assert.equal(events.filter(x => x.type === 'bytes').reduce((s, x) => s + x.bytes, 0), 6);
  assert.equal(events[0].type, 'socket'); assert.ok(events.some(x => x.type === 'headers')); assert.equal(events[events.length - 1].outcome, 'success');
});

test('loopback body stall still aborts the real socket inside the unchanged request deadline', async t => {
  const http = require('http'), request = require('../app/libs/request-with-deadline'), events = []; let socketClosed = false;
  const server = http.createServer((req, res) => { req.socket.once('close', () => { socketClosed = true; }); res.writeHead(200); res.write('abc'); });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise(resolve => server.close(resolve)));
  await assert.rejects(request({ url: 'http://127.0.0.1:' + server.address().port }, { timeoutMs: 150, observe: e => events.push(e) }), { code: 'ETIMEDOUT' });
  await new Promise(resolve => setTimeout(resolve, 20));
  assert.equal(socketClosed, true); assert.ok(events.some(x => x.type === 'headers')); assert.equal(events[events.length - 1].outcome, 'failure');
});

test('transport deadline removes diagnostic listeners and aborts the actual HTTP request', async t => {
  const x = transport(t); const p = x.fn({}, { timeoutMs: 1000, observe () {} });
  const socket = new EventEmitter(); socket.connecting = true; x.pending.emit('socket', socket);
  x.timers[0].fn(); await assert.rejects(p, { code: 'ETIMEDOUT' });
  assert.equal(x.pending.aborted, true); assert.equal(socket.listenerCount('connect'), 0); assert.equal(x.pending.listenerCount('response'), 0);
});
