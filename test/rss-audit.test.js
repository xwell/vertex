const assert = require('node:assert/strict');
const test = require('node:test');
const { RssAllocator } = require('../app/libs/rss-allocator');
const audit = require('../app/libs/rss-audit');
const { createLogger } = require('./helpers/load-module');
const dispatcher = require('../app/libs/rss-dispatcher');

function harness () {
  let now = 1800000000000;
  const clients = {};
  const logger = createLogger();
  const allocator = new RssAllocator({ now: () => now, clients: () => clients, limits: { waitMs: 0 } });
  const task = { id: 'rss-id', clientArr: ['a', 'b'], clientSortBy: 'uploadSpeed', acceptRules: [] };
  const item = { hash: 'a'.repeat(40), size: 1024, pubTime: now / 1000 - 10, _rssDiscoveredAt: now - 100 };
  function client (id, cap = 125000000) {
    return (clients[id] = { id, status: true, uploadBandwidth: cap, downloadBandwidth: 125000000,
      lastMaindataSuccessTime: now / 1000, lastMaindataRequestStartedAt: now,
      minFreeSpace: 1024, maindata: { torrents: [], freeSpaceOnDisk: 1e12, leechingCount: 0, uploadSpeed: 0, downloadSpeed: 0 } });
  }
  const events = () => logger.entries.filter(entry => entry.args[0] === 'RSS_ALLOCATION_EVENT').map(entry => JSON.parse(entry.args[1]));
  async function acquire (more = {}) {
    const telemetry = audit.create(logger, task, item, { clientIds: task.clientArr, ...more }, () => now);
    const result = await allocator.acquire({ clientIds: task.clientArr, forcedClientId: more.forcedClientId,
      hash: item.hash, size: item.size, rss: task, mode: task.clientSortBy, audit: telemetry });
    return { ...result, telemetry };
  }
  return { clients, logger, allocator, task, item, client, events, acquire, now: () => now, advance: ms => { now += ms; } };
}

test('decision captures every candidate, exact weight/share/credit and source/torrent identity', async () => {
  const h = harness();h.client('a');h.client('b', 62500000);
  const result = await h.acquire();const [event] = h.events();
  assert.equal(result.ok, true);assert.equal(event.rssId, 'rss-id');assert.equal(event.hash, h.item.hash);
  assert.equal(event.sizeBytes, 1024);assert.equal(event.discoveredAtMs, h.item._rssDiscoveredAt);
  assert.equal(event.candidates.length, 2);assert.equal(event.disposition, 'reserved');
  const a = event.candidates.find(row => row.clientId === 'a');
  assert.equal(a.weight, 125000000);assert.equal(a.normalizedShare, 2 / 3);
  assert.equal(a.creditBefore, 0);assert.ok(Math.abs(a.creditAfter + 1 / 3) < 1e-12);
  assert.equal(a.pendingAdds, 0);assert.equal(a.queuedIO, null);assert.equal(a.ioFactor, 1);
  assert.equal(a.snapshotFresh, true);assert.equal(a.sampleAgeMs, 0);
});

test('forced route snapshots outside target and labels ordinary clients not considered', async () => {
  const h = harness();h.client('a');h.client('b');h.client('outside');
  const result = await h.acquire({ forcedClientId: 'outside', rule: { id: 'force', priority: 99, code: '() => true' } });
  const [event] = h.events();assert.equal(result.client.id, 'outside');assert.equal(event.route, 'forced');
  assert.equal(event.ruleId, 'force');assert.match(event.ruleFingerprint, /^[a-f0-9]{64}$/);
  for (const row of event.candidates.filter(row => row.clientId !== 'outside')) {
    assert.equal(row.considered, false);assert.equal(row.eligible, null);assert.equal(row.reason, 'forced-not-considered');
    assert.equal(row.uploadCapacityBps, 125000000);
  }
});

test('offline, stale, capacity and space rejections retain available inputs without fake eligibility', async () => {
  const h = harness();const a = h.client('a');const b = h.client('b');a.status = false;b.lastMaindataSuccessTime -= 100;
  const result = await h.acquire();assert.equal(result.ok, false);
  const rows = h.events()[0].candidates;
  assert.equal(rows[0].reason, 'client-offline');assert.equal(rows[0].online, false);
  assert.equal(rows[1].reason, 'snapshot-stale');assert.equal(rows[1].sampleAgeMs, 100000);
  b.lastMaindataSuccessTime = h.now() / 1000;b.maindata.freeSpaceOnDisk = 0;
  await h.acquire();const row = h.events()[1].candidates.find(row => row.clientId === 'b');
  assert.equal(row.reason, 'space-reserve');assert.equal(row.freeBytes, 0);assert.equal(row.reservedBytes, 1024);
});

test('submission records the final changed input rather than relabeling the original selection snapshot', async () => {
  const h = harness();const a = h.client('a');h.client('b');
  const result = await h.acquire();a.maindata.uploadSpeed = 10000000;
  assert.equal(h.allocator.begin(result.token).ok, true);result.telemetry.submitted(result.token);result.telemetry.result('accepted');result.telemetry.result('uncertain');
  const events = h.events();assert.equal(events.length, 3);
  assert.equal(events[0].candidates[0].usedUploadBps, 0);
  assert.equal(events[1].finalInput.usedUploadBps, 10000000);assert.equal(events[2].outcome, 'accepted');
  assert.equal(events[2].submitted, true);assert.equal(events[2].fairnessRefunded, false);
  assert.ok(events.every(event => event.decisionId === events[0].decisionId && event.lease === result.token.id));
});

test('all shared server inputs are preserved without paths, credentials or torrent lists', async () => {
  const h = harness();const a = h.client('a');const b = h.client('b');a.sameServerClients = ['b'];
  a.password = 'TOP_SECRET';a.maindata.torrents = [{ name: 'PRIVATE_NAME', hash: 'old', savePath: '/private' }];
  b.maindata.uploadSpeed = 5000000;await h.acquire();
  const row = h.events()[0].candidates[0];assert.equal(row.sharedPeers.length, 2);assert.equal(row.usedUploadBps, 5000000);
  const encoded = JSON.stringify(h.events());assert.ok(!/TOP_SECRET|PRIVATE_NAME|savePath|password|\/private/.test(encoded));
});

test('audit failure cannot change routing, credit or POST success', async () => {
  const h = harness();h.client('a');h.client('b');
  const broken = new Proxy({}, { get () { throw new Error('audit unavailable'); } });
  const first = await h.allocator.acquire({ clientIds: ['a', 'b'], hash: 'one', audit: broken });
  assert.equal(first.ok, true);assert.equal(h.allocator.begin(first.token).ok, true);
  h.allocator.accepted(first.token);assert.equal(first.token.status, 'confirmed');
  h.logger.info = () => { throw new Error('disk unavailable'); };
  assert.equal((await h.acquire()).ok, true);
});

test('large configured candidate sets are explicitly truncated, not claimed as full snapshots', async () => {
  const h = harness();h.task.clientArr = Array.from({ length: 300 }, (_, i) => 'client-' + i);
  const result = await h.acquire();assert.equal(result.ok, false);
  const event = h.events()[0];assert.equal(event.snapshotTruncated, true);assert.equal(event.originalClientCount, 300);
  assert.ok(event.candidates.length <= 256);assert.ok(Buffer.byteLength(JSON.stringify(event)) < 512 * 1024);
});

test('real dispatcher emits decision/submitted/accepted and post failure stays uncertain', async () => {
  const h = harness();const a = h.client('a');h.task.clientArr = ['a'];
  Object.assign(h.task, { alias: 'RSS', ntf: { addTorrent: async () => {}, rejectTorrent: async () => {} },
    _normalizeTorrentUrl: url => url, _logTorrentTiming () {}, cacheTorrentToClient: async () => {}, addCount: 0 });
  const moment = () => ({ unix: () => h.now() / 1000 });
  const d = dispatcher({ rss: {}, util: {}, logger: h.logger, redis: {}, moment, allocator: h.allocator });
  a.addTorrent = async (...args) => args[9].onAccepted();
  const previous = global.runningClient;global.runningClient = h.clients;
  try {
    await d.push.call(h.task, { ...h.item, url: 'https://private/?passkey=SECRET', name: 'PRIVATE_NAME' });
    assert.deepEqual(h.events().map(event => event.stage), ['decision', 'submitted', 'result']);
    assert.equal(h.events()[2].outcome, 'accepted');
    a.addTorrent = async () => { throw Object.assign(new Error('private URL'), { code: 'ETIMEDOUT' }); };
    await d.push.call(h.task, { ...h.item, hash: 'b'.repeat(40), url: 'https://private/' });
    assert.equal(h.events()[h.events().length - 1].outcome, 'uncertain');assert.equal(h.events()[h.events().length - 1].error, 'ETIMEDOUT');
    assert.ok(!JSON.stringify(h.events()).includes('SECRET'));assert.ok(!JSON.stringify(h.events()).includes('PRIVATE_NAME'));
  } finally { global.runningClient = previous; }
});

test('source preflight records each rejected torrent with the exact captured matrix and no POST', () => {
  const h = harness();const a = h.client('a');a.status = false;h.task.clientArr = ['a'];
  const previous = global.runningClient;global.runningClient = h.clients;
  const d = dispatcher({ rss: {}, util: {}, logger: h.logger, redis: {}, moment: () => {}, allocator: h.allocator });
  try {
    assert.equal(d.canPrepare.call(h.task), false);a.status = true;h.advance(1000);
    d.noClient.call(h.task, [h.item, { ...h.item, hash: 'b'.repeat(40) }]);
    const decisions = h.events().filter(event => event.stage === 'decision');assert.equal(decisions.length, 2);
    assert.ok(decisions.every(event => event.disposition === 'preflight-rejected' && event.candidates[0].online === false));
    assert.ok(decisions.every(event => event.inputAtMs === h.now() - 1000 && event.preflightProbeBytes === 1));
  } finally { global.runningClient = previous; }
});
