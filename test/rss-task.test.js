const assert = require('node:assert/strict');
const test = require('node:test');
const realMoment = require('moment');
const { loadModule, createLogger } = require('./helpers/load-module');

function setup (getTorrents = async () => [], moment = realMoment) {
  const logger = createLogger();
  const writes = [];
  const Rss = loadModule('app/common/Rss.js', {
    '../libs/rss': { getTorrents, normalizeTorrentUrl: url => url },
    '../libs/util': {
      getRecords: async () => [],
      runRecords: async (sql, rows) => writes.push({ sql, rows }),
      formatSize: String
    },
    '../libs/logger': logger,
    '../libs/redis': {},
    'node-cron': {},
    moment,
    '../libs/queues/RssTaskQueue': class {},
    '../libs/rate-limiter': {},
    './Push': class {}
  });
  const task = Object.assign(Object.create(Rss.prototype), {
    id: 'test-rss',
    alias: 'Test RSS',
    _rss: { id: 'test-rss' },
    urls: ['https://one.test/rss?passkey=private-secret'],
    clientArr: [],
    clientSortBy: 'downloadSpeed',
    acceptRules: [],
    rejectRules: [],
    downloadUrlReplaceRules: [],
    addCountPerHour: 0,
    addCount: 0,
    maxSleepTime: 600,
    lastRssTime: moment().unix() - 60,
    ntf: {
      rssError: async () => {},
      rejectTorrent: async () => {},
      addTorrent: async () => {},
      addTorrentError: async () => {}
    },
    cacheTorrentToClient: async () => {},
    _persistLastRssTime: async timestamp => {
      task.persisted = timestamp;
      task.lastRssTime = timestamp;
    }
  });
  return { task, logger, writes };
}

function client (id) {
  return {
    id,
    alias: id,
    status: true,
    avgUploadSpeed: 0,
    avgDownloadSpeed: 0,
    maindata: { uploadSpeed: 0, downloadSpeed: 0, leechingCount: 0, freeSpaceOnDisk: 1e15, torrents: [] }
  };
}

function torrent (index) {
  return {
    hash: `hash-${index}`,
    name: `Torrent ${index}`,
    size: 1024 + index,
    link: `https://tracker.test/details.php?id=${index}`,
    url: `https://tracker.test/download.php?id=${index}`,
    pubTime: Math.floor(Date.now() / 1000) - 400
  };
}

test('all-source failure rejects, sends an error notification and does not advance last success', async () => {
  const { task, logger } = setup(async () => { throw new Error('upstream failure'); });
  let notifications = 0;
  task.ntf.rssError = async () => { notifications++; };
  await assert.rejects(task.rss(), /所有源均失败/);
  assert.equal(task.persisted, undefined);
  assert.equal(notifications, 1);
  assert.ok(!JSON.stringify(logger.entries).includes('private-secret'));
});

test('one failed source does not prevent a valid empty source from succeeding', async () => {
  const { task } = setup(async url => {
    if (url.includes('one.test')) throw new Error('unavailable');
    return [];
  });
  task.urls.push('https://two.test/rss');
  await task.rss();
  assert.ok(task.persisted > 0);
});

test('an invalid source URL cannot break failure reporting for another healthy source', async () => {
  const { task } = setup(async url => {
    if (!url.startsWith('https://')) throw new Error('无效RSS地址');
    return [];
  });
  task.urls = ['not-a-url?passkey=private-secret', 'https://two.test/rss'];
  await task.rss();
  assert.ok(task.persisted > 0);
});

test('dryrun exposes source errors instead of displaying an empty success', async () => {
  const { task } = setup(async () => { throw new Error('invalid XML'); });
  await assert.rejects(task.dryrun(), /invalid XML/);
});

test('each client runs at most 15 torrent jobs in one RSS task, while clients remain parallel', async t => {
  const previous = global.runningClient;
  t.after(() => { global.runningClient = previous; });
  global.runningClient = { one: client('one'), two: client('two') };
  const { task, writes, logger } = setup();
  task.clientArr = ['one', 'two'];
  const active = { one: 0, two: 0 };
  const peaks = { one: 0, two: 0 };
  const handled = new Set();
  let overlapped = false;
  task._pushTorrent = async (item, selected) => {
    active[selected.id]++;
    peaks[selected.id] = Math.max(peaks[selected.id], active[selected.id]);
    overlapped = overlapped || (active.one > 0 && active.two > 0);
    assert.ok(active[selected.id] <= 15);
    await new Promise(resolve => setTimeout(resolve, 2));
    active[selected.id]--;
    assert.ok(!handled.has(item.hash));
    handled.add(item.hash);
    if (item.hash === 'hash-7') throw new Error('one item failed');
    return { noteRows: [], noteCategoryRows: [], addRows: [[item.hash]] };
  };
  await task.rss(Array.from({ length: 80 }, (_, i) => torrent(i)));
  assert.deepEqual(peaks, { one: 15, two: 15 });
  assert.equal(handled.size, 80);
  assert.equal(overlapped, true);
  assert.equal(writes.flatMap(write => write.rows).length, 79);
  assert.equal(task.persisted, undefined); // 手动传入种子不刷新订阅成功时间。
  const discovery = logger.entries.filter(entry => entry.args[1] === 'RSS种子阶段:')
    .map(entry => JSON.parse(entry.args[2]));
  assert.equal(discovery.length, 80);
  assert.equal(discovery[0].stage, '发现');
  assert.ok(discovery[0].sincePublicationMs >= 400000);
  assert.ok(discovery[0].discoveredAt);
});

test('a healthy source is still processed when another source fails, with hashes deduplicated', async t => {
  const previous = global.runningClient;
  t.after(() => { global.runningClient = previous; });
  global.runningClient = { one: client('one') };
  const { task } = setup(async url => {
    if (url.includes('one.test')) throw new Error('unavailable');
    return [torrent(1), torrent(1)];
  });
  task.urls.push('https://two.test/rss');
  task.clientArr = ['one'];
  const handled = [];
  task._pushTorrent = async item => { handled.push(item.hash); };
  await task.rss();
  assert.deepEqual(handled, ['hash-1']);
});

test('add_time is captured at API completion, before cache updates and notifications', async () => {
  let now = Math.floor(Date.now() / 1000);
  const initial = now;
  const moment = value => realMoment(value === undefined ? now * 1000 : value);
  moment.unix = realMoment.unix;
  const { task, logger } = setup(undefined, moment);
  const selected = client('one');
  selected.addTorrent = async () => { now += 5; };
  task.cacheTorrentToClient = async () => { now += 20; };
  task.ntf.addTorrent = async () => { now += 300; };
  const result = await task._pushTorrent(torrent(1), selected);
  assert.equal(result.addRows.length, 1);
  assert.equal(result.addRows[0][7], initial + 5);
  assert.equal(result.addRows[0][6], initial + 5);
  const completion = logger.entries.find(entry => entry.args[1] === 'RSS种子阶段:');
  assert.equal(JSON.parse(completion.args[2]).stage, '添加完成');
  assert.equal(JSON.parse(completion.args[2]).client, 'one');
});

test('valid URL replacement is applied, while an invalid regex cannot create a false success', async () => {
  for (const valid of [true, false]) {
    const { task, logger } = setup();
    task.useCustomRegex = true;
    task.regexStr = valid ? '/tracker\\.test/g' : 'invalid-pattern';
    task.replaceStr = 'mirror.test';
    const selected = client('one');
    const calls = [];
    selected.addTorrent = async url => calls.push(url);
    const result = await task._pushTorrent(torrent(1), selected);
    assert.equal(result.addRows.length, valid ? 1 : 0);
    assert.equal(result.noteRows.length, valid ? 0 : 1);
    if (valid) {
      assert.equal(calls[0], 'https://mirror.test/download.php?id=1');
    } else {
      assert.equal(calls.length, 0);
      assert.equal(result.noteRows[0][6], 3);
      assert.ok(!logger.entries.some(entry => entry.args[1] === 'RSS种子阶段:'));
    }
  }
});

test('torrent-file download failure retains link fallback; successful files retain true-hash records', async () => {
  for (const fail of [true, false]) {
    const { task, logger } = setup();
    task.pushTorrentFile = true;
    const calls = [];
    task._downloadTorrent = async () => {
      if (fail) throw new Error('download unavailable');
      return { filepath: '/test/file.torrent', hash: 'real-hash' };
    };
    const selected = client('one');
    selected.addTorrent = async () => calls.push('link');
    selected.addTorrentByTorrentFile = async () => calls.push('file');
    const result = await task._pushTorrent(torrent(1), selected);
    assert.deepEqual(calls, [fail ? 'link' : 'file']);
    assert.equal(result.addRows.length, fail ? 1 : 2);
    const stages = logger.entries.filter(entry => entry.args[1] === 'RSS种子阶段:').map(entry => JSON.parse(entry.args[2]).stage);
    assert.ok(stages.includes('添加完成'));
    assert.equal(stages.includes('种子文件下载完成'), !fail);
  }
});
