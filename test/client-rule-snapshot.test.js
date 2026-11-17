const assert = require('node:assert/strict');
const test = require('node:test');
const { loadModule, createLogger } = require('./helpers/load-module');

function setup (t) {
  const logger = createLogger();
  const clock = { now: 1800000000000 };
  const originalNow = Date.now;
  Date.now = () => clock.now;
  t.after(() => { Date.now = originalNow; });
  const util = { sleep: async () => {}, runRecord: async () => {} };
  const moment = () => ({ unix: () => clock.now / 1000, format: () => 'synthetic-time',
    startOf: () => ({ unix: () => clock.now / 1000 - 100 }) });
  const Client = loadModule('app/common/Client.js', {
    '../libs/client/qb': {}, '../libs/client/de': {}, '../libs/client/tr': {},
    '../libs/util': util, '../libs/redis': {}, '../libs/logger': logger,
    'node-cron': {}, './Push': {}, '../libs/queues/ClientTaskQueue': {}, moment
  });
  const torrent = { hash: 'synthetic', name: 'synthetic', size: 100, totalSize: 100, uploaded: 0,
    downloaded: 0, completedTime: 1, addedTime: 1, state: 'error', category: 'FREE' };
  const normal = { id: 'normal', type: 'normal', fitTime: 10,
    conditions: [{ key: 'state', compareType: 'equals', value: 'error' }] };
  const client = Object.assign(Object.create(Client.prototype), { id: 'synthetic', status: true,
    _client: { type: 'qBittorrent' }, deleteRuleExecutionId: 0, rejectDeleteRules: [],
    deleteRules: [normal], fitTime: { normal: { synthetic: clock.now / 1000 - 30 } },
    probabilisticStats: { normal: { synthetic: { firstCheckTime: 1, checks: [{ timestamp: 1, result: true }] } } },
    maindata: { torrents: [torrent], freeSpaceOnDisk: 1000, leechingCount: 0, seedingCount: 1 },
    pausedTorrentHashes: [], avgUploadSpeed: 0, avgDownloadSpeed: 0,
    reannounceTorrent: async () => {}, deleteTorrent: async () => true });
  return { client, torrent, normal, logger, clock, util };
}

for (const missing of [null, undefined, {}, { torrents: null }, { torrents: [], freeSpaceOnDisk: null },
  { torrents: [], freeSpaceOnDisk: NaN }, { torrents: [], freeSpaceOnDisk: -1 }]) {
  test('missing/invalid snapshot fails closed without evaluating rules: ' + String(missing), t => {
    const s = setup(t); s.client.maindata = missing;
    assert.equal(s.client._fitConditions(s.torrent, s.normal.conditions), false);
    assert.equal(s.client._fitDeleteRule(s.normal, s.torrent), false);
    assert.equal(s.client._fitDeleteRule({ id: 'js', type: 'javascript', code: '(() => { throw new Error("must not evaluate"); })' }, s.torrent), false);
    assert.deepEqual(s.client.fitTime.normal, {});
    assert.deepEqual(s.client.probabilisticStats.normal, {});
    assert.equal(s.logger.entries.filter(e => e.level === 'error').length, 0);
  });
}

test('unhealthy client with a leftover snapshot cannot satisfy a delete rule', t => {
  const s = setup(t); s.client.status = false;
  assert.equal(s.client._fitDeleteRule(s.normal, s.torrent), false);
  assert.deepEqual(s.client.fitTime.normal, {});
});

test('available snapshots preserve ordinary and JavaScript rule results, including real zero space', t => {
  const s = setup(t); s.client.maindata.freeSpaceOnDisk = 0;
  assert.equal(s.client._fitDeleteRule(s.normal, s.torrent), true);
  assert.equal(s.client._fitDeleteRule({ id: 'js', type: 'javascript', code: '((data, torrent) => data.freeSpaceOnDisk === 0 && torrent.hash === "synthetic")' }, s.torrent), true);
  s.torrent.state = 'uploading';
  assert.equal(s.client._fitDeleteRule(s.normal, s.torrent), false);
});

test('snapshot skip diagnostics are bounded per client, not emitted per torrent, and contain no torrent data', t => {
  const s = setup(t); s.client.maindata = null;
  for (let i = 0; i < 1000; i++) s.client._fitDeleteRule(s.normal, { ...s.torrent, name: 'private-name', hash: 'private-hash' });
  assert.equal(s.client.ruleSnapshotSkipped, 1000);
  assert.equal(s.logger.entries.length, 1);
  assert.ok(!JSON.stringify(s.logger.entries).includes('private-'));
  s.clock.now += 60000; s.client._fitDeleteRule(s.normal, s.torrent);
  assert.equal(s.logger.entries.length, 2);
});

for (const probabilistic of [false, true]) test('fitTime polling clears unobserved confirmation before recovery: ' + probabilistic, t => {
  const s = setup(t); s.normal.probabilisticFitTime = probabilistic;
  s.client.maindata = null;
  s.client.flashFitTime(s.normal);
  assert.deepEqual(s.client.fitTime.normal, {});
  assert.deepEqual(s.client.probabilisticStats.normal, {});
  s.clock.now += 120000;
  s.client.maindata = { torrents: [s.torrent], freeSpaceOnDisk: 1000 };
  assert.equal(s.client._fitDeleteRule(s.normal, s.torrent), false);
  s.client.flashFitTime(s.normal);
  assert.equal(s.client._fitDeleteRule(s.normal, s.torrent), false);
  if (!probabilistic) assert.equal(s.client.fitTime.normal.synthetic, s.clock.now / 1000);
  else assert.equal(s.client.probabilisticStats.normal.synthetic.checks.length, 1);
});

test('autoDelete with unavailable data clears confirmation without any side effects', async t => {
  const s = setup(t); s.client.maindata = null;
  s.client.reannounceTorrent = async () => { throw new Error('no reannounce'); };
  s.client.deleteTorrent = async () => { throw new Error('no delete'); };
  await s.client.autoDelete();
  assert.deepEqual(s.client.fitTime.normal, {});
  assert.equal(s.client.deleteRuleExecutionId, 0);
});

test('snapshot lost during reannounce wait prevents records and deletion', async t => {
  const s = setup(t); let deleted = 0; let recorded = 0;
  s.util.sleep = async () => { s.client.maindata = null; };
  s.util.runRecord = async () => { recorded++; };
  s.client.deleteTorrent = async () => { deleted++; return true; };
  await s.client.autoDelete();
  assert.equal(deleted, 0); assert.equal(recorded, 0);
  assert.deepEqual(s.client.fitTime.normal, {});
});

test('available snapshot still permits the same selected deletion and fixed wait', async t => {
  const s = setup(t); let deleted = 0; const waits = [];
  s.util.sleep = async ms => { waits.push(ms); };
  s.client.deleteTorrent = async () => { deleted++; return true; };
  await s.client.autoDelete();
  assert.equal(deleted, 1); assert.deepEqual(waits, [2000]);
});
