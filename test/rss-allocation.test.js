const assert = require('node:assert/strict');
const test = require('node:test');
const realMoment = require('moment');
const bandwidth = require('../app/libs/bandwidth');
const { RssAllocator } = require('../app/libs/rss-allocator');
const { loadModule, createLogger } = require('./helpers/load-module');
const GiB = 1024 ** 3;

function harness (options = {}) {
  let now = 1800000000000;
  const clients = {};
  const allocator = new RssAllocator({ now: () => now, clients: () => clients, limits: options });
  function client (id, capacity = 125000000) {
    return (clients[id] = {
      id, alias: id, status: true, uploadBandwidth: capacity, downloadBandwidth: 125000000,
      lastMaindataSuccessTime: now / 1000, lastMaindataRequestStartedAt: now,
      minFreeSpace: 30 * GiB, avgUploadSpeed: 0, avgDownloadSpeed: 0,
      maindata: { uploadSpeed: 0, downloadSpeed: 0, leechingCount: 0, freeSpaceOnDisk: 1000 * GiB, torrents: [], queuedIO: 0 }
    });
  }
  const request = (index, mode = 'uploadSpeed', more = {}) => ({ clientIds: Object.keys(clients), mode, hash: 'hash-' + index, size: GiB, ...more });
  async function acquire (index, mode = 'uploadSpeed', more = {}) { return allocator.acquire(request(index, mode, more)); }
  function observe (token) {
    allocator.accepted(token);
    const c = clients[token.client.id];
    c.maindata = { ...c.maindata, leechingCount: c.maindata.leechingCount + 1, torrents: [...c.maindata.torrents, { hash: Array.from(token.hashes)[0] }] };
    c.lastMaindataSuccessTime = now / 1000;
    c.lastMaindataRequestStartedAt = now;
  }
  return { clients, client, allocator, request, acquire, observe, time: () => now, advance: ms => { now += ms; } };
}

function dispatchHarness (t, limits = {}) {
  const h = harness({ waitMs: 1000, ...limits });
  const logger = createLogger();
  const writes = [];
  const util = {
    getRecords: async () => [], getRecord: async () => null,
    runRecords: async (sql, rows) => writes.push({ sql, rows }), runRecord: async () => {},
    formatSize: String, uuid: { v4: () => 'test-uuid' }
  };
  const clock = value => realMoment(value === undefined ? h.time() : value);
  clock.unix = realMoment.unix;
  const Rss = loadModule('app/common/Rss.js', {
    '../libs/rss': { normalizeTorrentUrl: url => url, getTorrentNameByBencode: async () => ({ name: 'same-data', hash: 'reseed-new' }) },
    '../libs/util': util, '../libs/logger': logger,
    '../libs/redis': { get: async () => null, setWithExpire: async () => {} },
    moment: clock, 'node-cron': {}, '../libs/queues/RssTaskQueue': class {},
    '../libs/rate-limiter': {}, './Push': class {}, '../libs/rss-allocator': h.allocator
  });
  const Client = loadModule('app/common/Client.js', {
    '../libs/client/qb': {}, '../libs/client/de': {}, '../libs/client/tr': {},
    '../libs/util': util, '../libs/logger': logger, '../libs/redis': {}, moment: clock,
    'node-cron': {}, './Push': class {}, '../libs/queues/ClientTaskQueue': class {},
    '../libs/rss-allocator': h.allocator
  });
  const previous = global.runningClient;
  global.runningClient = h.clients;
  t.after(() => { global.runningClient = previous; });
  const task = (id = 'rss') => Object.assign(Object.create(Rss.prototype), {
    id, alias: id, _rss: { id }, clientArr: Object.keys(h.clients), clientSortBy: 'uploadSpeed',
    acceptRules: [], rejectRules: [], downloadUrlReplaceRules: [], addCountPerHour: 0, addCount: 0,
    lastRssTime: clock().unix() - 60, maxSleepTime: 600,
    ntf: { rejectTorrent: async () => {}, addTorrent: async () => {}, addTorrentError: async () => {} },
    cacheTorrentToClient: async () => {}
  });
  const apiClient = id => {
    const c = h.client(id);
    c._client = { type: 'qBittorrent' };
    c.apiCalls = [];
    c.client = {
      addTorrent: async (...args) => { c.apiCalls.push(args); return { statusCode: 200 }; },
      addTorrentByTorrentFile: async (...args) => { c.apiCalls.push(args); return { statusCode: 200 }; }
    };
    c.addTorrent = Client.prototype.addTorrent;
    c.addTorrentByTorrentFile = Client.prototype.addTorrentByTorrentFile;
    return c;
  };
  const torrent = i => ({ hash: 'hash-' + i, name: 'Torrent ' + i, size: GiB, pubTime: clock().unix() - 100, url: 'https://offline.test/' + i, link: 'https://offline.test/details/' + i });
  return { ...h, task, apiClient, torrent, writes, logger, util };
}

test('source preflight permits small torrents but final guard still rejects oversized chunks', async t => {
  const h = dispatchHarness(t);const c = h.apiClient('a');const task = h.task();
  c.maindata.freeSpaceOnDisk = c.minFreeSpace + 1024 * 1024;
  const small = h.torrent('small');small.size = 512 * 1024;
  await task.rss([small]);
  assert.equal(c.apiCalls.length, 1);
  assert.equal((await task._pushTorrent(h.torrent('big'))).addRows.length, 0);
  assert.equal(c.apiCalls.length, 1);
});

test('bit-rate conversion, legacy byte units and absent defaults are explicit', () => {
  assert.equal(bandwidth.parse(500, 'Mbps'), 62500000);
  assert.equal(bandwidth.parse(1, 'Gbps'), 125000000);
  assert.equal(bandwidth.parse(0.5, 'Gbps'), 62500000);
  assert.equal(bandwidth.parse(0.7, 'Gbps'), 87500000);
  assert.equal(bandwidth.parse(2.2, 'Gbps'), 275000000);
  assert.equal(bandwidth.parse(10, 'Gbps'), 1250000000);
  assert.equal(bandwidth.parse(0.5, 'GiB'), 536870912); // never silently reinterpret an old config
  assert.equal(bandwidth.parse(undefined), bandwidth.parse(1000, 'Mbps'));
  assert.equal(bandwidth.parse('', 'GiB'), 125000000);
  for (const [value, unit] of [[0, 'Mbps'], [-1, 'Mbps'], [Infinity, 'Mbps'], ['abc', 'Mbps'], [true, 'Mbps'], [[], 'Mbps'], [[500], 'Mbps'], [{}, 'Mbps'], [1, 'MBps'], [Number.MAX_VALUE, 'Gbps']]) assert.throws(() => bandwidth.parse(value, unit));
});

test('invalid bandwidth is rejected before ClientMod destroys a live client or writes config', () => {
  let writes = 0;
  let destroys = 0;
  const Model = loadModule('app/model/ClientMod.js', {
    fs: { writeFileSync: () => { writes++; } }, '../common/Client': class {}, '../common/Rss': class {},
    '../libs/logger': createLogger(), '../libs/util': { uuid: { v4: () => 'new-id' } }
  });
  const previous = global.runningClient;
  global.runningClient = { old: { destroy: () => { destroys++; } } };
  try {
    assert.throws(() => new Model().modify({ id: 'old', uploadBandwidth: -1, uploadBandwidthUnit: 'Mbps' }));
    assert.throws(() => new Model().add({ uploadBandwidth: 'no', uploadBandwidthUnit: 'Mbps' }));
    assert.equal(writes, 0);
    assert.equal(destroys, 0);
  } finally { global.runningClient = previous; }
});

for (const mode of ['uploadSpeed', 'downloadSpeed']) {
  test(mode + ' distributes 500M/1G/10G free capacity as 1:2:20 across singleton RSS rounds', async () => {
    const h = harness();
    for (const [id, cap] of [['small', 62500000], ['medium', 125000000], ['large', 1250000000]]) {
      const c = h.client(id);
      if (mode === 'uploadSpeed') c.uploadBandwidth = cap;
      else c.downloadBandwidth = cap;
    }
    const counts = { small: 0, medium: 0, large: 0 };
    for (let i = 0; i < 230; i++) {
      const a = await h.acquire(i, mode, { clientIds: i % 2 ? ['small', 'medium', 'large'] : ['large', 'medium', 'small'], rss: { id: 'source-' + i } });
      assert.equal(a.ok, true);
      counts[a.client.id]++;
      assert.equal(h.allocator.begin(a.token).ok, true);
      h.observe(a.token);
    }
    assert.deepEqual(counts, { small: 10, medium: 20, large: 200 });
  });
}

for (const [mode, rounds, expected] of [
  ['uploadSpeed', 440, { rbx: 50, bhs: 70, hdd: 220, vps: 100 }],
  ['downloadSpeed', 260, { rbx: 50, bhs: 50, hdd: 110, vps: 50 }]
]) {
  test(mode + ' uses operator-confirmed numeric Gbps settings, not the HDD link-name speed', async () => {
    const h = harness();
    for (const [id, up, down] of [['rbx', '0.5', '1'], ['bhs', '0.7', '1'], ['hdd', '2.2', '2.2'], ['vps', '1', '1']]) {
      const c = h.client(id, bandwidth.parse(up, 'Gbps'));
      c.downloadBandwidth = bandwidth.parse(down, 'Gbps');
    }
    const counts = { rbx: 0, bhs: 0, hdd: 0, vps: 0 };
    for (let i = 0; i < rounds; i++) {
      const a = await h.acquire(i, mode);
      assert.equal(a.ok, true);
      counts[a.client.id]++;
      assert.equal(h.allocator.begin(a.token).ok, true);
      h.observe(a.token);
    }
    assert.deepEqual(counts, expected);
  });
}

test('upload weights use remaining B/s rather than capacity or capped absolute scores', () => {
  const h = harness();const a = h.client('a', 125000000);const b = h.client('b', 62500000);
  a.maindata.uploadSpeed = 100000000;
  b.maindata.uploadSpeed = 12500000;
  assert.equal(h.allocator.inspect(a).weight, 25000000);
  assert.equal(h.allocator.inspect(b).weight, 50000000);
});

test('download load and known IO queue reduce upload preference; unknown IO is not fabricated', () => {
  const h = harness();const a = h.client('a');const b = h.client('b');
  a.maindata.downloadSpeed = a.downloadBandwidth / 2;
  a.maindata.queuedIO = 100;
  delete b.maindata.queuedIO;
  assert.equal(h.allocator.inspect(a).weight, h.allocator.inspect(b).weight / 4);
});

test('full upload or full download cannot win a new ordinary admission through a type bonus', async () => {
  const h = harness();const a = h.client('busy');const b = h.client('idle');
  a.maindata.uploadSpeed = a.uploadBandwidth;
  assert.equal((await h.acquire(1)).client, b);
  h.allocator.cancel((await h.acquire(2)).token);
  a.maindata.uploadSpeed = 0;a.maindata.downloadSpeed = a.downloadBandwidth;
  assert.equal(h.allocator.inspect(a).reason, 'no-headroom');
});

test('renaming HDD/SSD/VPS has no effect on measured weights', () => {
  const h = harness();const c = h.client('same');const first = h.allocator.inspect(c).weight;
  for (const alias of ['HDD', 'SSD', 'VPS']) { c.alias = alias;assert.equal(h.allocator.inspect(c).weight, first); }
});

test('space weight excludes used-space fiction and subtracts floor, writes and reservations', async () => {
  const h = harness();const a = h.client('a');const b = h.client('b');
  a.maindata.freeSpaceOnDisk = 31 * GiB;a.maindata.usedSpace = 10000 * GiB;
  b.maindata.freeSpaceOnDisk = 100 * GiB;
  assert.equal((await h.acquire(1, 'freeSpaceOnDisk')).client, b);
  const before = h.allocator.inspect(a, { mode: 'freeSpaceOnDisk', size: GiB }).usableSpace;
  const lease = await h.acquire(2, 'freeSpaceOnDisk', { forcedClientId: 'a' });
  assert.equal(lease.ok, true);
  assert.equal(h.allocator.inspect(a, { mode: 'freeSpaceOnDisk', size: GiB }).usableSpace, before - 256 * 1024 * 1024);
  a.maindata.downloadSpeed = 100 * 1024 * 1024;
  assert.equal(h.allocator.inspect(a).reason, 'space-reserve');
});

test('large buffer tasks reserve a bounded chunk, not the entire logical torrent size', async () => {
  const h = harness();const a = h.client('a');a.maindata.freeSpaceOnDisk = 31 * GiB;
  const r = await h.acquire(1, 'uploadSpeed', { size: 1000 * GiB });
  assert.equal(r.ok, true);assert.equal(r.token.bytes, 256 * 1024 * 1024);
});

test('stale, future, unknown and invalid snapshots fail closed', () => {
  const h = harness();const c = h.client('a');
  c.lastMaindataSuccessTime = (h.time() - 60001) / 1000;assert.equal(h.allocator.inspect(c).reason, 'snapshot-stale');
  c.lastMaindataSuccessTime = (h.time() + 1) / 1000;assert.equal(h.allocator.inspect(c).reason, 'snapshot-stale');
  c.lastMaindataSuccessTime = h.time() / 1000;
  for (const key of ['uploadSpeed', 'downloadSpeed', 'freeSpaceOnDisk', 'leechingCount']) {
    const old = c.maindata[key];delete c.maindata[key];assert.equal(h.allocator.inspect(c).reason, 'snapshot-invalid');c.maindata[key] = old;
  }
  c.uploadBandwidth = NaN;assert.equal(h.allocator.inspect(c).reason, 'bandwidth-invalid');
  c.uploadBandwidth = 125000000;delete c.maindata.torrents;
  assert.equal(h.allocator.inspect(c).reason, 'snapshot-invalid');
});

test('fresh completion cannot hide an old request, while sub-second timestamps remain valid', () => {
  const h = harness();const c = h.client('a');
  c.lastMaindataRequestStartedAt = h.time() - 60001;
  assert.equal(h.allocator.inspect(c).reason, 'snapshot-stale');
  h.advance(600);
  c.lastMaindataRequestStartedAt = h.time() - 100;
  c.lastMaindataSuccessTime = Math.floor(h.time() / 1000);
  assert.equal(h.allocator.inspect(c).eligible, true);
  for (const invalid of [h.time() + 1, -1, NaN, 'bad']) {
    c.lastMaindataRequestStartedAt = invalid;
    assert.equal(h.allocator.inspect(c).reason, 'snapshot-stale');
  }
});

test('current speed spikes and EMA both protect admission and source limits are rechecked', () => {
  const h = harness();const c = h.client('a');c.maxUploadSpeed = 50000000;c.avgUploadSpeed = 51000000;
  assert.equal(h.allocator.inspect(c).reason, 'client-upload-limit');
  c.avgUploadSpeed = 0;c.maindata.uploadSpeed = 51000000;assert.equal(h.allocator.inspect(c).reason, 'client-upload-limit');
  delete c.maxUploadSpeed;assert.equal(h.allocator.inspect(c, { rss: { maxClientUploadSpeed: 50000000 } }).reason, 'rss-upload-limit');
});

test('one download slot is atomically reserved before a second source can use it', async () => {
  const h = harness();const c = h.client('a');c.maxLeechNum = 1;
  const a = await h.acquire(1);assert.equal(a.ok, true);
  assert.equal((await h.acquire(2)).reasons.a, 'client-download-count');
  h.allocator.cancel(a.token);assert.equal((await h.acquire(3)).ok, true);
});

test('successful add stays reserved until independent observation, without optimistic double count', async () => {
  const h = harness();const c = h.client('a');c.maxLeechNum = 1;
  const a = await h.acquire(1);h.allocator.begin(a.token);h.allocator.accepted(a.token);
  assert.equal((await h.acquire(2)).reasons.a, 'client-download-count');
  h.observe(a.token);assert.equal((await h.acquire(3)).reasons.a, 'client-download-count');
  assert.equal(h.allocator._pending(c).count, 0);
});

test('submitted HTTP promise never expires and does not free an uncertain side-effect slot', async () => {
  const h = harness();const c = h.client('a');c.maxLeechNum = 1;
  const a = await h.acquire(1);h.allocator.begin(a.token);h.advance(1000000);c.lastMaindataSuccessTime = h.time() / 1000;c.lastMaindataRequestStartedAt = h.time();
  assert.equal((await h.acquire(2)).ok, false);assert.equal(a.token.status, 'submitted');
  h.allocator.failed(a.token);assert.equal(a.token.status, 'uncertain');
  assert.equal((await h.acquire(3)).ok, false);
});

test('uncertain absence requires the grace period AND a later-started fresh snapshot', async () => {
  const h = harness();const c = h.client('a');c.maxLeechNum = 1;
  const a = await h.acquire(1);h.allocator.begin(a.token);h.allocator.failed(a.token);
  h.advance(180001);c.lastMaindataSuccessTime = h.time() / 1000;
  assert.equal((await h.acquire(2)).ok, false);
  c.lastMaindataRequestStartedAt = h.time();assert.equal((await h.acquire(3)).ok, true);
});

test('observing an uncertain hash reconciles it before grace, without resending', async () => {
  const h = harness();const c = h.client('a');const a = await h.acquire(1);h.allocator.begin(a.token);h.allocator.failed(a.token);
  c.maindata.torrents = [{ hash: 'hash-1' }];await h.acquire(2);assert.equal(a.token.status, 'released');
});

test('expired unsent leases and cancelled decisions roll back all fair credits', async () => {
  const h = harness();h.client('a');h.client('b');
  const a = await h.acquire(1);h.allocator.cancel(a.token);
  assert.ok(Array.from(h.allocator.fair.values()).every(e => Math.abs(e.credit) < 1e-10));
  const b = await h.acquire(2);h.advance(30001);
  assert.equal(h.allocator.begin(b.token).ok, false);assert.equal(b.token.status, 'released');
});

test('client recreation cannot dispatch a lease owned by its old runtime', async () => {
  const h = harness();h.client('a');const a = await h.acquire(1);const old = a.client;
  h.allocator.destroyClient(old);h.client('a');assert.equal(h.allocator.begin(a.token).ok, false);
});

test('client recreation does not erase submitted/uncertain reservations', async () => {
  const h = harness();const c = h.client('a');c.maxLeechNum = 1;const a = await h.acquire(1);h.allocator.begin(a.token);
  h.allocator.destroyClient(c);const next = h.client('a');next.maxLeechNum = 1;
  assert.equal((await h.acquire(2)).ok, false);
});

test('RSS destruction and duplicate inflight hashes prevent unsent dispatch', async () => {
  const h = harness();h.client('a');const a = await h.acquire(1);assert.equal((await h.acquire(1)).reasons.a, 'duplicate-inflight');
  a.token.request.isActive = () => false;assert.equal(h.allocator.begin(a.token).ok, false);h.allocator.cancel(a.token);
  assert.equal((await h.acquire(2, 'uploadSpeed', { isActive: () => false })).reason, 'rss-destroyed');
});

test('shared-server byte reservations and API concurrency cannot be spent twice', async () => {
  const h = harness({ waitMs: 0 });const a = h.client('a');const b = h.client('b');
  a.sameServerClients = ['a', 'b'];b.sameServerClients = ['a', 'b'];
  const first = await h.acquire(1, 'uploadSpeed', { forcedClientId: 'a' });
  const second = await h.acquire(2, 'uploadSpeed', { forcedClientId: 'b' });
  assert.equal(first.ok && second.ok, true);
  assert.equal(h.allocator.begin(first.token).ok, true);
  assert.equal(h.allocator.begin(second.token).ok, true);
  const third = await h.acquire(3);
  assert.equal(third.ok, true); // logical routing is independent of POST queue speed
  assert.equal(h.allocator.begin(third.token).reason, 'add-busy');
  assert.equal((await h.allocator.ready(third.token)).reason, 'add-busy-timeout');
  h.allocator.cancel(third.token);
});

test('asymmetric transitive same-server declarations share speed, disk and concurrency', async () => {
  const h = harness();const a = h.client('a');const b = h.client('b');const c = h.client('c');
  a.sameServerClients = ['b'];c.sameServerClients = ['b'];
  a.maindata.uploadSpeed = 10000000;c.maindata.uploadSpeed = 20000000;
  const before = h.allocator.inspect(b);
  assert.equal(before.upHeadroom, 95000000);
  const first = await h.acquire(1, 'uploadSpeed', { forcedClientId: 'a' });
  assert.equal(h.allocator.begin(first.token).ok, true);
  const after = h.allocator.inspect(b);
  assert.equal(after.physicalConcurrent, 1);
  assert.equal(after.usableSpace, before.usableSpace - first.token.bytes);
  assert.equal(h.allocator.ruleContext().runningClient.c.maindata.freeSpaceOnDisk, c.maindata.freeSpaceOnDisk - first.token.bytes);
});

test('reseed consumes API capacity but not a new-download slot or disk chunk', async () => {
  const h = harness();const c = h.client('a');c.maindata.freeSpaceOnDisk = 0;c.maxLeechNum = 1;c.maindata.leechingCount = 1;
  const a = await h.acquire(1, 'uploadSpeed', { forcedClientId: 'a', reseed: true });assert.equal(a.ok, true);assert.equal(a.token.bytes, 0);
  assert.equal(h.allocator._pending(c).count, 0);
  assert.equal((await h.allocator.ready(a.token)).ok, true);
  assert.equal(h.allocator._pending(c).concurrent, 1);
});

test('real RSS + Client: maxLeechNum=1 admits only one of fifteen concurrent items', async t => {
  const h = dispatchHarness(t);const c = h.apiClient('a');c.maxLeechNum = 1;const task = h.task();
  await task.rss(Array.from({ length: 15 }, (_, i) => h.torrent(i)));
  assert.equal(c.apiCalls.length, 1);
  assert.equal(c.maindata.leechingCount, 0); // no optimistic increment + reservation double count
  assert.equal(h.writes.flatMap(w => w.rows).filter(row => row[8] === 1).length, 1);
});

test('two real RSS sources share a client POST limit, rather than 15 each', async t => {
  const h = dispatchHarness(t);const c = h.apiClient('a');let active = 0;let peak = 0;
  c.client.addTorrent = async () => { c.apiCalls.push('start');active++;peak = Math.max(peak, active);await new Promise(resolve => setTimeout(resolve, 2));active--;return { statusCode: 200 }; };
  const tasks = [h.task('one'), h.task('two')];
  await Promise.all(tasks.map((task, k) => task.rss(Array.from({ length: 15 }, (_, i) => h.torrent(k * 15 + i)))));
  assert.equal(c.apiCalls.length, 30);assert.equal(peak, 2);
});

test('forced target outside clientArr works even when all ordinary targets are offline', async t => {
  const h = dispatchHarness(t);const a = h.apiClient('a');a.status = false;const b = h.apiClient('b');const task = h.task();task.clientArr = ['a'];
  task.acceptRules = [{ id: 'force', client: 'b', type: 'javascript', code: '() => true', priority: 3 }];
  await task.rss([h.torrent(1)]);assert.equal(a.apiCalls.length, 0);assert.equal(b.apiCalls.length, 1);
});

test('forced target cannot bypass its own space/count/source speed restrictions', async t => {
  const h = dispatchHarness(t);const a = h.apiClient('a');const b = h.apiClient('b');const task = h.task();task.clientArr = ['a'];
  task.acceptRules = [{ id: 'force', client: 'b', type: 'javascript', code: '() => true', priority: 3 }];
  b.maindata.freeSpaceOnDisk = 0;await task.rss([h.torrent(1)]);assert.equal(b.apiCalls.length, 0);
  b.maindata.freeSpaceOnDisk = 1000 * GiB;b.maxLeechNum = 1;b.maindata.leechingCount = 1;
  await task.rss([h.torrent(2)]);assert.equal(b.apiCalls.length, 0);
  b.maxLeechNum = 0;task.maxClientUploadSpeed = 1;b.maindata.uploadSpeed = 2;
  await task.rss([h.torrent(3)]);assert.equal(b.apiCalls.length, 0);assert.equal(a.apiCalls.length, 0);
});

test('highest-priority rule override, category and savePath survive the refactor', async t => {
  const h = dispatchHarness(t);h.apiClient('a');const b = h.apiClient('b');const task = h.task();
  task.acceptRules = [
    { id: 'lower', client: 'a', type: 'javascript', code: '() => true', priority: '0' },
    { id: 'higher', client: 'b', type: 'javascript', code: '() => true', priority: '99', category: 'old', savePath: '/existing/path' }
  ];
  await task.rss([h.torrent(1)]);assert.equal(b.apiCalls.length, 1);
  assert.equal(b.apiCalls[0][6], '/existing/path');assert.equal(b.apiCalls[0][7], 'old');
});

test('rule becoming false during acquire/dedup prevents POST without rerouting', async t => {
  const h = dispatchHarness(t);const a = h.apiClient('a');const task = h.task();let calls = 0;
  task.acceptRules = [{ id: 'changing', type: 'conditions' }];task._fitRule = () => ++calls === 1;
  await task.rss([h.torrent(1)]);assert.equal(a.apiCalls.length, 0);
});

test('client recreation during async duplicate check cannot receive an old reservation POST', async t => {
  const h = dispatchHarness(t);const a = h.apiClient('a');const task = h.task();task.skipSameTorrent = true;
  task.checkTorrentExistsInClient = async () => { h.client('a');return null; };
  await task.rss([h.torrent(1)]);assert.equal(a.apiCalls.length, 0);
});

test('RSS destruction while preparing a file prevents any later add', async t => {
  const h = dispatchHarness(t);const a = h.apiClient('a');const task = h.task();task.pushTorrentFile = true;
  task._downloadTorrent = async () => { task.destroyed = true;return { filepath: '/offline.torrent', hash: 'real' }; };
  await task.rss([h.torrent(1)]);assert.equal(a.apiCalls.length, 0);
});

test('ambiguous POST error is held for reconciliation and never retried on a second downloader', async t => {
  const h = dispatchHarness(t);const a = h.apiClient('a');const b = h.apiClient('b');const task = h.task();task.clientArr = ['a'];
  a.client.addTorrent = async () => { a.apiCalls.push('start');throw Object.assign(new Error('timeout'), { code: 'ETIMEDOUT' }); };
  await task.rss([h.torrent(1)]);assert.equal(a.apiCalls.length, 1);assert.equal(b.apiCalls.length, 0);
  assert.equal(Array.from(h.allocator.states.get('a').tokens.values())[0].status, 'uncertain');
  assert.ok(h.writes.some(w => w.rows.some(row => row[6] === 3)));
});

test('positive add response followed by DB error remains an accepted add, not false failure', async t => {
  const h = dispatchHarness(t);const a = h.apiClient('a');const task = h.task();h.util.runRecord = async () => { throw new Error('DB unavailable'); };
  await task.rss([h.torrent(1)]);assert.equal(a.apiCalls.length, 1);
  assert.ok(h.writes.some(w => w.rows.some(row => row[8] === 1)));
  assert.equal(Array.from(h.allocator.states.get('a').tokens.values())[0].status, 'confirmed');
});

test('file path uses the same admission hook, registers real hash and preserves both history rows', async t => {
  const h = dispatchHarness(t);const a = h.apiClient('a');const task = h.task();task.pushTorrentFile = true;
  task._downloadTorrent = async () => ({ filepath: '/offline.torrent', hash: 'real-hash' });
  await task.rss([h.torrent(1)]);assert.equal(a.apiCalls.length, 1);assert.equal(a.maindata.leechingCount, 0);
  const token = Array.from(h.allocator.states.get('a').tokens.values())[0];assert.ok(token.hashes.has('real-hash'));
  assert.equal(h.writes.flatMap(w => w.rows).filter(row => row[8] === 1).length, 2);
});

test('reseed-only reuses existing data and final API guard still applies', async t => {
  const h = dispatchHarness(t);const a = h.apiClient('a');const task = h.task();task.autoReseed = true;task.onlyReseed = true;task.reseedClients = ['a'];
  a.maindata.torrents = [{ name: 'same-data', size: GiB, completed: GiB, hash: 'old', savePath: '/existing' }];
  a.maindata.freeSpaceOnDisk = 0;const result = await task._pushTorrent(h.torrent(1));
  assert.equal(a.apiCalls.length, 1);assert.equal(result.addRows[0][9], '辅种');assert.equal(a.apiCalls[0][3], true);
});

test('one concurrent batch retains 1:2:20 routing despite a two-POST-per-client semaphore', async t => {
  const h = dispatchHarness(t);
  const capacities = { small: 62500000, medium: 125000000, large: 1250000000 };
  const peaks = {};
  for (const [id, capacity] of Object.entries(capacities)) {
    const c = h.apiClient(id);c.uploadBandwidth = capacity;let active = 0;peaks[id] = 0;
    c.client.addTorrent = async () => { c.apiCalls.push('add');active++;peaks[id] = Math.max(peaks[id], active);await new Promise(resolve => setTimeout(resolve, 2));active--;return { statusCode: 200 }; };
  }
  await h.task().rss(Array.from({ length: 23 }, (_, i) => h.torrent(i)));
  assert.deepEqual(Object.fromEntries(Object.keys(capacities).map(id => [id, h.clients[id].apiCalls.length])), { small: 1, medium: 2, large: 20 });
  assert.ok(Object.values(peaks).every(peak => peak <= 2));
});

test('current space modes do not use array order or hypothetical future deletion', async () => {
  const h = harness();const a = h.client('a');const b = h.client('b');
  a.minFreeSpace = 0;b.minFreeSpace = 0;a.maindata.freeSpaceOnDisk = 100 * GiB;b.maindata.freeSpaceOnDisk = 200 * GiB;
  a.maindata.usedSpace = 100000 * GiB;
  const counts = { a: 0, b: 0 };
  for (let i = 0; i < 120; i++) {
    const lease = await h.acquire(i, 'freeSpaceOnDisk', { clientIds: i % 2 ? ['b', 'a'] : ['a', 'b'] });
    counts[lease.client.id]++;h.allocator.begin(lease.token);h.observe(lease.token);
  }
  assert.ok(Math.abs(counts.b - 80) <= 1);assert.ok(Math.abs(counts.a - 40) <= 1);
});

test('source-level download count limit uses the same atomic reservations', async t => {
  const h = dispatchHarness(t);const c = h.apiClient('a');const task = h.task();task.maxClientDownloadCount = 2;
  await task.rss(Array.from({ length: 15 }, (_, i) => h.torrent(i)));
  assert.equal(c.apiCalls.length, 2);
});

test('resource predicates read shared pending admissions without mutating real maindata', async t => {
  const h = dispatchHarness(t);const c = h.apiClient('a');const task = h.task();
  task.acceptRules = [{ id: 'predicate-cap', client: 'a', type: 'javascript', code: '() => global.runningClient.a.maindata.leechingCount < 2' }];
  await task.rss(Array.from({ length: 15 }, (_, i) => h.torrent(i)));
  assert.equal(c.apiCalls.length, 2);assert.equal(c.maindata.leechingCount, 0);
  assert.equal(h.allocator.ruleContext().runningClient.a.maindata.leechingCount, 2);
});

test('busy semaphore timeout releases an unsent lease rather than sending an extra POST', async () => {
  const h = harness({ waitMs: 0 });h.client('a');
  const first = await h.acquire(1);const second = await h.acquire(2);const third = await h.acquire(3);
  h.allocator.begin(first.token);h.allocator.begin(second.token);
  assert.equal((await h.allocator.ready(third.token)).reason, 'add-busy-timeout');
  h.allocator.cancel(third.token);assert.equal(third.token.status, 'released');assert.equal(h.allocator.waiters.size, 0);
});

test('pending resource caps include reseed leases and stay bounded before POST', async () => {
  const h = harness({ maxPendingAdds: 2 });h.client('a');
  assert.equal((await h.acquire(1, 'uploadSpeed', { forcedClientId: 'a', reseed: true })).ok, true);
  assert.equal((await h.acquire(2, 'uploadSpeed', { forcedClientId: 'a', reseed: true })).ok, true);
  assert.equal((await h.acquire(3, 'uploadSpeed', { forcedClientId: 'a', reseed: true })).reasons.a, 'pending-limit');
});

test('skipSameTorrent deduplicates across target clients before the history batch is written', async t => {
  const h = dispatchHarness(t);h.apiClient('a');h.apiClient('b');const first = h.task('first');const second = h.task('second');
  first.skipSameTorrent = true;second.skipSameTorrent = true;
  await Promise.all([first.rss([h.torrent(1)]), second.rss([h.torrent(1)])]);
  assert.equal(h.clients.a.apiCalls.length + h.clients.b.apiCalls.length, 1);
});

test('same real hash from two fake hashes is rechecked before POST', async t => {
  const h = dispatchHarness(t);const a = h.apiClient('a');const b = h.apiClient('b');
  const tasks = [h.task('one'), h.task('two')];
  for (const task of tasks) { task.skipSameTorrent = true;task.pushTorrentFile = true;task._downloadTorrent = async () => ({ filepath: '/offline', hash: 'shared-real' }); }
  await Promise.all(tasks.map((task, i) => task.rss([h.torrent(i)])));
  assert.equal(a.apiCalls.length + b.apiCalls.length, 1);
});

test('admission refresh clears expired successful-absence reservations even before source preparation', async () => {
  const h = harness({ maxPendingAdds: 1 });const c = h.client('a');const first = await h.acquire(1);
  h.allocator.begin(first.token);h.allocator.accepted(first.token);
  assert.equal(h.allocator.inspect(c).reason, 'pending-limit');
  h.advance(60001);c.lastMaindataSuccessTime = h.time() / 1000;c.lastMaindataRequestStartedAt = h.time();
  h.allocator.refresh();assert.equal(h.allocator.inspect(c).eligible, true);
});

test('allocation audit contains no link, cookie, password or torrent name', async t => {
  const h = dispatchHarness(t);h.apiClient('a');const task = h.task();const item = h.torrent(1);
  item.url = 'https://private.test/download?passkey=TOP_SECRET';item.name = 'PRIVATE_NAME';
  await task.rss([item]);
  const audit = h.logger.entries.filter(entry => entry.args[1] === 'RSS分配:' || entry.args[1] === 'RSS添加结果:');
  assert.ok(audit.length >= 2);assert.ok(!JSON.stringify(audit).includes('TOP_SECRET'));assert.ok(!JSON.stringify(audit).includes('PRIVATE_NAME'));
});
