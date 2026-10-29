const assert = require('node:assert/strict');
const test = require('node:test');
const http = require('node:http');
const { once } = require('node:events');
const { loadModule, createLogger } = require('./helpers/load-module');
const tracker = require('../app/libs/tracker-sync');

function setupClient (t, overrides = {}) {
  const logger = createLogger();
  const calls = [];
  const clock = { now: 1800000000000 };
  const realNow = Date.now;
  Date.now = () => clock.now;
  t.after(() => { Date.now = realNow; });
  const redis = { get: async () => null, set: async () => {} };
  const util = { getRecord: async () => null, listDeleteRule: () => [] };
  const Client = loadModule('app/common/Client.js', {
    '../libs/client/qb': {}, '../libs/client/de': {}, '../libs/client/tr': {}, '../libs/util': util,
    '../libs/redis': redis, '../libs/logger': logger, 'node-cron': {}, './Push': {},
    '../libs/queues/ClientTaskQueue': {}, moment: Object.assign(() => ({ unix: () => Math.floor(clock.now / 1000) }), { unix: () => {} })
  });
  const client = Object.create(Client.prototype);
  Object.assign(client, { id: 'test', alias: 'Test client', status: true, lastMaindataSuccessTime: clock.now / 1000,
    minFreeSpace: 30 * 2 ** 30, trackerSyncCursor: 0, trackerSyncBatchSize: 50, trackerSyncBatch: null,
    trackerStatus: {}, lastTrackerSyncTime: 0, clientUrl: 'http://example.invalid', cookie: 'secret',
    client: { type: 'qBittorrent', getTrackerList: async (...args) => { calls.push(args); return { statusCode: 200, body: '[{"url":"udp://tracker.invalid","msg":"ok"}]' }; } },
    maindata: { freeSpaceOnDisk: 100 * 2 ** 30, downloadSpeed: 0, torrents: Array.from({ length: 20 }, (_, i) => ({ hash: String(i), name: 'synthetic' })) }, ...overrides });
  return { client, logger, calls, redis, util, clock, Client };
}

function setupQueue (t, client = {}) {
  const logger = createLogger();
  const timers = [];
  const cleared = new Set();
  const enqueued = [];
  class TaskQueue {
    constructor (name) { this.name = name; }
    async enqueue (...args) { enqueued.push({ queue: this.name, args }); return 'queued'; }
  }
  const Queue = loadModule('app/libs/queues/ClientTaskQueue.js', {
    '../redis': { TaskQueue }, '../logger': logger
  }, {
    setTimeout: (callback, ms) => { const timer = { callback, ms }; timers.push(timer); return timer; },
    clearTimeout: timer => cleared.add(timer)
  });
  const previous = global.runningClient;
  t.after(() => { global.runningClient = previous; });
  Object.assign(client, { id: 'test', alias: 'Test', status: true, minFreeSpace: 30 * 2 ** 30,
    _client: { type: 'qBittorrent' }, client: {}, lastMaindataSuccessTime: Date.now() / 1000,
    maindata: { freeSpaceOnDisk: 100 * 2 ** 30, downloadSpeed: 0 } });
  global.runningClient = { test: client };
  return { queue: new Queue(), client, timers, cleared, enqueued, logger,
    task: action => ({ data: { clientId: 'test', action }, priority: 'high' }) };
}

async function tick () { await new Promise(resolve => setImmediate(resolve)); }

test('Tracker slices visit at most five tasks and keep the exact remaining batch/cursor', async t => {
  const { client, calls } = setupClient(t);
  const first = await client.trackerSync();
  assert.equal(first.visited, 5); assert.equal(first.remaining, 15);
  assert.equal(client.trackerSyncCursor, 5);
  assert.ok(calls.every(a => a[3].timeoutMs === 3000));
  for (let i = 0; i < 3; i++) await client.trackerSync();
  assert.equal(client.trackerSyncBatch, null);
  assert.deepEqual(calls.map(a => a[2]), Array.from({ length: 20 }, (_, i) => String(i)));
  assert.equal(client.trackerSyncCursor, 0);
});

test('wall budget stops the batch before another request and truncates remaining request deadline', async t => {
  const { client, calls, clock } = setupClient(t);
  client.client.getTrackerList = async (...args) => { calls.push(args); clock.now += 2600; return { statusCode: 200, body: '[]' }; };
  const result = await client.trackerSync();
  assert.equal(calls.length, 2);
  assert.equal(calls[1][3].timeoutMs, 2400);
  assert.equal(result.reason, 'slice-budget');
  assert.equal(client.trackerSyncCursor, 1); // Second response arrived outside the slice; retry, no late status write.
  assert.equal(client.trackerStatus['1'], undefined);
});

test('stale/unknown/low-space snapshots skip maintenance without moving its cursor', async t => {
  const { client, calls, clock } = setupClient(t);
  client.lastMaindataSuccessTime -= 31;
  assert.equal((await client.trackerSync()).reason, 'snapshot-stale');
  client.lastMaindataSuccessTime = clock.now / 1000;
  client.maindata.freeSpaceOnDisk = undefined;
  assert.equal((await client.trackerSync()).reason, 'space-unknown');
  client.maindata.freeSpaceOnDisk = 40 * 2 ** 30;
  client.maindata.downloadSpeed = 100 * 2 ** 20;
  assert.equal((await client.trackerSync()).reason, 'space-reserve');
  assert.equal(calls.length, 0); assert.equal(client.trackerSyncCursor, 0);
});

test('failed request consumes only that member, logs sanitized code and yields', async t => {
  const { client, calls, logger } = setupClient(t);
  client.client.getTrackerList = async (...args) => { calls.push(args); throw Object.assign(new Error('http://secret:password@example.invalid/passkey'), { code: 'ETIMEDOUT' }); };
  const result = await client.trackerSync();
  assert.equal(result.reason, 'request-error'); assert.equal(result.remaining, 19);
  assert.equal(calls.length, 1); assert.equal(client.lastTrackerSyncTime, 0);
  assert.ok(!JSON.stringify(logger.entries).includes('password'));
});

test('cancellation after HTTP starts prevents status writes and duplicate cursor advancement', async t => {
  const { client, clock } = setupClient(t);
  let finish;
  client.client.getTrackerList = () => new Promise(resolve => { finish = resolve; });
  const control = tracker.createControl();
  const run = client.trackerSync({ signal: control });
  await tick();
  control.abort('foreground-pending');
  clock.now += 100;
  finish({ statusCode: 200, body: '[{"url":"udp://x","msg":"late"}]' });
  const result = await run;
  assert.equal(result.reason, 'foreground-pending');
  assert.equal(client.trackerStatus['0'], undefined); assert.equal(client.trackerSyncCursor, 0);
});

test('cancellation during Redis/SQLite wait cannot start a later HTTP request', async t => {
  const { client, util, calls } = setupClient(t);
  let finish;
  util.getRecord = () => new Promise(resolve => { finish = resolve; });
  const control = tracker.createControl();
  const run = client.trackerSync({ signal: control });
  await tick(); control.abort('task-timeout'); finish(null);
  await run;
  assert.equal(calls.length, 0); assert.equal(client.trackerSyncCursor, 0);
});

test('late cancelled old batch cannot clear a replacement batch or its success time', async t => {
  const { client, util } = setupClient(t);
  let finish;
  util.getRecord = () => new Promise(resolve => { finish = resolve; });
  const control = tracker.createControl();
  const run = client.trackerSync({ signal: control });
  await tick();
  const old = client.trackerSyncBatch;
  control.abort('task-timeout');
  old.index = old.items.length;
  const replacement = { started: Date.now(), index: 0, items: ['new'] };
  client.trackerSyncBatch = replacement;
  client.lastTrackerSyncTime = 123;
  finish(null);
  await run;
  assert.equal(client.trackerSyncBatch, replacement);
  assert.equal(client.lastTrackerSyncTime, 123);
});

test('expired batch yields without claiming a successful sync', async t => {
  const { client, clock } = setupClient(t);
  await client.trackerSync();
  const last = client.lastTrackerSyncTime;
  clock.now += tracker.limits.batchMaxMs + 1;
  client.lastMaindataSuccessTime = clock.now / 1000;
  assert.equal((await client.trackerSync()).reason, 'batch-expired');
  assert.equal(client.trackerSyncBatch, null); assert.equal(client.lastTrackerSyncTime, last);
  assert.equal(client.trackerSyncCursor, 5);
});

test('removed members, deleted DB entries, cache hits and hidden trackers retain safe behavior', async t => {
  const { client, redis, util, calls } = setupClient(t);
  client.trackerSyncBatch = { started: Date.now(), index: 0, items: ['removed', '0', '1', '2', '3'] };
  redis.get = async key => key.endsWith(':0') ? '1' : null;
  util.getRecord = async (_, [hash]) => hash === '1' ? { delete_time: 1 } : null;
  client.client.getTrackerList = async (...args) => { calls.push(args); return { statusCode: 200, body: '[{"url":"**","msg":"hidden"},{"url":"udp://x","msg":"ok"}]' }; };
  const result = await client.trackerSync();
  assert.equal(result.remaining, 0); assert.deepEqual(calls.map(a => a[2]), ['2', '3']);
  assert.equal(client.trackerStatus['2'], 'ok');
});

test('autoDelete queues maintenance instead of waiting for a Tracker HTTP batch', async t => {
  const { client } = setupClient(t);
  client._client = { type: 'qBittorrent' };
  client.deleteRules = [{ type: 'javascript', code: 'trackerStatus', id: 'site', deleteNum: 1 }];
  client.rejectDeleteRules = [];
  let scheduled = 0;
  client.scheduleTrackerSync = async () => { scheduled++; };
  client.trackerSync = async () => { throw new Error('must not await tracker'); };
  client._fitDeleteRule = () => false;
  await client.autoDelete();
  assert.equal(scheduled, 1);
});

test('Tracker never promoted ahead of foreground; foreground arrival cancels its request', async t => {
  const { queue, client, enqueued } = setupQueue(t);
  const control = tracker.createControl();
  queue.activeClientTasks.set('test', { action: 'trackerSync', control });
  await queue.enqueue({ clientId: 'test', action: 'trackerSync' }, 'high');
  assert.equal(enqueued[0].args[1], 'normal');
  await queue.enqueue({ clientId: 'test', action: 'autoDelete' }, 'high');
  assert.equal(control.aborted, true); assert.equal(control.reason, 'foreground-pending');
  assert.equal(client.status, true);
});

test('fragment continuation waits for a newer successful snapshot and keeps one timer', async t => {
  const { queue, client, enqueued, timers } = setupQueue(t);
  let calls = 0;
  client.lastMaindataSuccessTime = Math.floor(Date.now() / 1000) - 2;
  client.trackerSync = async () => { calls++; return { remaining: 10 }; };
  await queue.executeTrackerSync(client, tracker.createControl());
  assert.equal(calls, 1); assert.equal(timers.length, 1);
  await queue.executeTrackerSync(client, tracker.createControl());
  assert.equal(calls, 1); assert.equal(timers.length, 1);
  assert.equal(enqueued[0].args[0].action, 'getMaindata');
  queue.foregroundPending.clear();
  client.lastMaindataSuccessTime++;
  await queue.executeTrackerSync(client, tracker.createControl());
  assert.equal(calls, 2);
});

test('foreground pending and low space prevent a Tracker slice; stale snapshot requests refresh', async t => {
  const { queue, client, enqueued } = setupQueue(t);
  let calls = 0; client.trackerSync = async () => { calls++; };
  await queue.enqueue({ clientId: 'test', action: 'autoDelete' });
  await queue.executeTrackerSync(client, tracker.createControl());
  assert.equal(calls, 0);
  queue.foregroundPending.clear(); client.maindata.freeSpaceOnDisk = 1;
  await queue.executeTrackerSync(client, tracker.createControl()); assert.equal(calls, 0);
  client.maindata.freeSpaceOnDisk = 100 * 2 ** 30; client.lastMaindataSuccessTime -= 40;
  await queue.executeTrackerSync(client, tracker.createControl());
  assert.ok(enqueued.some(e => e.args[0].action === 'getMaindata'));
});

test('Tracker task timeout cancels underlying work, releases exact lock and does not mark qB offline', async t => {
  const { queue, client, timers, cleared, task } = setupQueue(t);
  let signal;
  client.trackerSync = args => { signal = args.signal; return new Promise(() => {}); };
  const run = queue.executeTask(task('trackerSync'));
  assert.equal(timers[0].ms, 10000);
  timers[0].callback();
  await run;
  assert.equal(signal.aborted, true); assert.equal(signal.reason, 'task-timeout');
  assert.equal(queue.activeClientTasks.has('test'), false);
  assert.equal(client.status, true); assert.ok(cleared.has(timers[0]));
  assert.equal(queue.failedClients.size, 0);
});

test('every completed task clears timeout; timed-out old work cannot remove a newer lock', async t => {
  const { queue, client, timers, cleared, task } = setupQueue(t);
  client.trackerSync = async () => ({ remaining: 0 });
  await queue.executeTask(task('trackerSync'));
  assert.ok(cleared.has(timers[0]));
  let finish;
  client.trackerSync = () => new Promise(resolve => { finish = resolve; });
  const run = queue.executeTask(task('trackerSync'));
  const newer = { action: 'trackerSync', control: tracker.createControl() };
  queue.activeClientTasks.set('test', newer);
  finish({ remaining: 0 }); await run;
  assert.equal(queue.activeClientTasks.get('test'), newer);
});

test('client destruction cancels maintenance and invalidates continuation timers', async t => {
  const { queue, client, timers, cleared } = setupQueue(t);
  queue.clearClientQueue = async () => 0;
  const control = tracker.createControl();
  queue.activeClientTasks.set('test', { action: 'trackerSync', control });
  queue._continueTrackerSync(client);
  await queue.cleanupClientOnDestroy('test');
  assert.equal(control.reason, 'client-destroyed'); assert.ok(cleared.has(timers[0]));
  assert.equal(queue.trackerContinuations.size, 0);
});

test('busy same-client operations remain serialized; unrelated clients are not blocked', async t => {
  const { queue, client, task, timers } = setupQueue(t);
  let deletes = 0; client.autoDelete = async () => { deletes++; };
  queue.activeClientTasks.set('test', { action: 'trackerSync', control: tracker.createControl() });
  await queue.executeTask(task('autoDelete'));
  assert.equal(deletes, 0); assert.equal(timers[0].ms, 5000);
  global.runningClient.other = { ...client, id: 'other' };
  await queue.executeTask({ data: { clientId: 'other', action: 'autoDelete' } });
  assert.equal(deletes, 1);
});

test('busy Tracker retries off the five-second fitTime phase with only one continuation', async t => {
  const { queue, task, timers } = setupQueue(t);
  queue.activeClientTasks.set('test', { action: 'flashFitTime' });
  await queue.executeTask(task('trackerSync'));
  await queue.executeTask(task('trackerSync'));
  assert.equal(timers.length, 1);
  assert.equal(timers[0].ms, 2000);
  assert.equal(queue.trackerContinuations.size, 1);
});

test('real Client + typeless qB adapter + queue composition executes Tracker slices', async t => {
  const { client, util } = setupClient(t);
  const md = client.maindata;
  const { queue, task } = setupQueue(t, client);
  client.maindata = md;
  const requests = [];
  const qb = loadModule('app/libs/client/qb.js', {
    '../logger': createLogger(),
    '../util': { ...util, requestPromise: async (...args) => { requests.push(args); return { statusCode: 200, body: '[{"url":"udp://test","msg":"ok"}]' }; } }
  });
  assert.equal(qb.type, undefined);
  client.client = qb;
  await queue.executeTask(task('trackerSync'));
  assert.equal(requests.length, 5);
  assert.equal(client.trackerStatus['0'], 'ok');
  assert.ok(requests.every(r => r[1] === false && r[2].timeoutMs === 3000));
});

test('Tracker uses configured type and does not run for other adapters', async t => {
  const { queue, client } = setupQueue(t);
  client._client.type = 'Transmission';
  client.client.type = 'qBittorrent'; // Deliberately misleading adapter metadata must not override configuration.
  let calls = 0; client.trackerSync = async () => { calls++; };
  await queue.executeTrackerSync(client, tracker.createControl());
  assert.equal(calls, 0);
});

test('qB adapter passes short deadline/cancel signal and disables browser fallback only for Tracker', async () => {
  const calls = [];
  const qb = loadModule('app/libs/client/qb.js', { '../logger': createLogger(), '../util': { requestPromise: async (...args) => { calls.push(args); return { body: '{}' }; } } });
  const control = tracker.createControl();
  await qb.getTrackerList('http://example.invalid', 'cookie', 'hash', { timeoutMs: 1200, signal: control });
  assert.equal(calls[0][0].timeout, 1200); assert.equal(calls[0][1], false);
  assert.equal(calls[0][2].signal, control);
  await qb.getTrackerList('http://example.invalid', 'cookie', 'hash');
  assert.equal(calls[1][0].timeout, undefined); assert.equal(calls[1][2], null);
});

test('deadline success preserves request response/body and clears timer/listener', async t => {
  const control = tracker.createControl();
  let callback;
  let aborts = 0;
  const cleared = [];
  const timers = [];
  const request = loadModule('app/libs/request-with-deadline.js', {
    request: (_, cb) => { callback = cb; return { abort: () => { aborts++; } }; }
  }, {
    setTimeout: (fn, ms) => { const timer = { fn, ms }; timers.push(timer); return timer; },
    clearTimeout: timer => cleared.push(timer)
  });
  const run = request({ url: 'http://example.invalid' }, { timeoutMs: 123, signal: control });
  const response = { statusCode: 200, body: '[1]' };
  callback(null, response, response.body);
  assert.equal(await run, response);
  assert.equal(cleared[0], timers[0]); assert.equal(control.listenerCount('abort'), 0);
  control.abort('late'); timers[0].fn(); assert.equal(aborts, 0);
});

test('pre-cancelled requests do not open a socket; invalid deadlines fail closed', async () => {
  let requests = 0;
  const request = loadModule('app/libs/request-with-deadline.js', { request: () => { requests++; } });
  const control = tracker.createControl(); control.abort('cancelled');
  await assert.rejects(request({}, { timeoutMs: 100, signal: control }), { code: 'EABORTED' });
  for (const timeoutMs of [0, -1, NaN, Infinity, undefined]) await assert.rejects(request({}, { timeoutMs }), /invalid request deadline/);
  assert.equal(requests, 0);
});

test('deadline destroys a real hung HTTP request and settles once', async t => {
  const sockets = new Set();
  const server = http.createServer(() => {});
  server.on('connection', socket => { sockets.add(socket); socket.once('close', () => sockets.delete(socket)); });
  server.listen(0, '127.0.0.1'); await once(server, 'listening');
  t.after(() => { for (const socket of sockets) socket.destroy(); server.close(); });
  const request = require('../app/libs/request-with-deadline');
  const started = Date.now();
  await assert.rejects(request({ url: `http://127.0.0.1:${server.address().port}/`, timeout: 10000 }, { timeoutMs: 70 }), { code: 'ETIMEDOUT' });
  await new Promise(resolve => setTimeout(resolve, 30));
  assert.equal(sockets.size, 0); assert.ok(Date.now() - started < 1500);
});

test('deadline cancellation aborts an actual request and removes signal listener', async t => {
  let seen;
  const connected = new Promise(resolve => { seen = resolve; });
  const sockets = new Set();
  const server = http.createServer(() => { seen(); });
  server.on('connection', socket => { sockets.add(socket); socket.once('close', () => sockets.delete(socket)); });
  server.listen(0, '127.0.0.1'); await once(server, 'listening');
  t.after(() => { for (const socket of sockets) socket.destroy(); server.close(); });
  const request = require('../app/libs/request-with-deadline');
  const control = tracker.createControl();
  const pending = request({ url: `http://127.0.0.1:${server.address().port}/` }, { timeoutMs: 5000, signal: control });
  const rejection = assert.rejects(pending, { code: 'EABORTED' });
  await connected; control.abort('foreground-pending'); await rejection;
  await new Promise(resolve => setTimeout(resolve, 30));
  assert.equal(sockets.size, 0); assert.equal(control.listenerCount('abort'), 0);
});
