const assert = require('node:assert/strict');
const test = require('node:test');
const realMoment = require('moment');
const { loadModule, createLogger } = require('./helpers/load-module');

const successKey = 'vertex:rss:last_success:test-rss';
const sleepReason = '拒绝原因: 最长休眠时间';

function setup (t, previousAge = 60) {
  let now = 1800000000;
  const clock = value => realMoment(value === undefined ? now * 1000 : value);
  clock.unix = realMoment.unix;
  const logger = createLogger();
  const store = new Map();
  if (previousAge !== null) store.set(successKey, String(now - previousAge));
  const history = new Map();
  const redisWrites = [];
  const enqueued = [];
  const pushes = [];
  const notifications = [];
  const state = {
    items: [],
    failFetch: false,
    failHistory: false,
    failRead: false,
    failPersist: false,
    failPush: false,
    allowed: Infinity,
    quotaCalls: 0,
    fetchSeconds: 0,
    processingSeconds: 0
  };
  class Queue {
    async enqueue (data) { enqueued.push(data); return 'mock-job'; }
    cleanupRssOnDestroy () {}
  }
  class Push {
    async rssError () { notifications.push('rss-error'); }
    async rejectTorrent () {}
  }
  const Rss = loadModule('app/common/Rss.js', {
    '../libs/rss': {
      normalizeTorrentUrl: url => url,
      getTorrents: async url => {
        if (state.failFetch || url.includes('failed.test')) throw new Error('upstream unavailable');
        now += state.fetchSeconds;
        return state.items.map(item => ({ ...item }));
      }
    },
    '../libs/util': {
      listPush: () => [],
      listRssRule: () => [],
      calSize: () => 0,
      formatSize: String,
      getRecords: async (sql, values) => values.slice(1).filter(hash => history.has(hash)).map(hash => ({ hash })),
      runRecords: async (sql, rows) => {
        if (state.failHistory) throw new Error('history write failed');
        rows.forEach(row => history.set(row[0], { type: row[6], reason: row[7] }));
      }
    },
    '../libs/logger': logger,
    '../libs/redis': {
      get: async key => {
        if (state.failRead) throw new Error('Redis read failed');
        return store.get(key);
      },
      set: async (key, value) => {
        if (state.failPersist) throw new Error('Redis write failed');
        redisWrites.push({ key, value });
        store.set(key, value);
      }
    },
    'node-cron': { schedule: () => ({ stop () {} }) },
    moment: clock,
    '../libs/queues/RssTaskQueue': Queue,
    '../libs/rate-limiter': {
      reset: async () => {},
      consume: async (key, limit, duration, count) => {
        state.quotaCalls++;
        const allowed = Math.min(count, limit, state.allowed);
        return { allowed, remaining: Math.max(0, limit - allowed) };
      }
    },
    './Push': Push
  });
  const oldRss = global.runningRss;
  const oldQueue = global.rssTaskQueue;
  const oldClients = global.runningClient;
  t.after(() => {
    global.runningRss = oldRss;
    global.rssTaskQueue = oldQueue;
    global.runningClient = oldClients;
  });
  global.runningRss = {};
  global.rssTaskQueue = new Queue();
  global.runningClient = {
    one: { id: 'one', alias: 'mock-client', status: true, maindata: { leechingCount: 0, freeSpaceOnDisk: 1e15, downloadSpeed: 0 } }
  };
  const createTask = (options = {}) => {
    const task = new Rss({
      id: 'test-rss',
      alias: 'Test RSS',
      maxSleepTime: 600,
      cron: '* * * * *',
      rssUrls: ['https://tracker.test/rss'],
      clientArr: ['one'],
      clientSortBy: 'downloadSpeed',
      ...options
    });
    task._pushTorrent = async item => {
      now += state.processingSeconds;
      if (state.failPush) throw new Error('client unavailable');
      pushes.push(item.hash);
      return null;
    };
    global.runningRss[task.id] = task;
    return task;
  };
  const items = count => Array.from({ length: count }, (_, i) => ({
    hash: `hash-${i}`,
    name: `Torrent ${i}`,
    size: 1024,
    link: `https://tracker.test/details.php?id=${i}`,
    url: `https://tracker.test/download.php?id=${i}`,
    pubTime: now - 86400 * 90
  }));
  return {
    createTask,
    store,
    history,
    redisWrites,
    enqueued,
    pushes,
    notifications,
    logger,
    state,
    items,
    time: () => now,
    advance: seconds => { now += seconds; }
  };
}

test('long-failed RSS recovery rejects all 50 old items even after scheduling; next cycle resumes', async t => {
  const h = setup(t, 86400 * 90);
  h.state.items = h.items(50);
  const task = h.createTask({ addCountPerHour: 500 });
  await task.lastRssTimeReady;
  const previous = task.lastRssTime;
  await task.scheduleRssFetch();
  assert.equal(task.lastRssTime, previous);
  assert.equal(h.redisWrites.length, 0);
  assert.equal(h.enqueued.length, 1);
  await task.rss();
  assert.equal(h.pushes.length, 0);
  assert.equal(h.state.quotaCalls, 0);
  assert.equal(h.history.size, 50);
  assert.ok([...h.history.values()].every(row => row.type === 2 && row.reason === sleepReason));
  assert.equal(task.lastRssTime, h.time());
  h.advance(60);
  h.state.items = h.items(51);
  await task.rss();
  assert.deepEqual(h.pushes, ['hash-50']);
});

test('fresh tasks and legacy timestamps take a protected baseline cycle, without trusting old successful-looking times', async t => {
  const h = setup(t, null);
  h.store.set('vertex:rss:last_time:test-rss', String(h.time()));
  h.store.set('vertex:rss:last_attempt:test-rss', String(h.time()));
  h.state.items = h.items(50);
  const task = h.createTask();
  await task.scheduleRssFetch();
  await task.rss();
  assert.equal(h.pushes.length, 0);
  assert.equal(h.history.size, 50);
  assert.equal(h.store.get(successKey), String(h.time()));
  assert.ok(h.redisWrites.every(write => write.key === successKey));
  h.advance(60);
  h.state.items = h.items(51);
  await task.rss();
  assert.deepEqual(h.pushes, ['hash-50']);
});

test('initial fetch errors do not consume first-cycle protection before the source actually recovers', async t => {
  const h = setup(t, null);
  h.state.failFetch = true;
  const task = h.createTask();
  await assert.rejects(task.rss(), /所有源均失败/);
  assert.equal(h.store.has(successKey), false);
  h.advance(60);
  h.state.failFetch = false;
  h.state.items = h.items(50);
  await task.rss();
  assert.equal(h.history.size, 50);
  assert.equal(h.pushes.length, 0);
  assert.equal(h.store.get(successKey), String(h.time()));
});

test('sleep rejection takes precedence over unavailable clients and never spends the hourly quota', async t => {
  const h = setup(t, 601);
  h.state.items = h.items(1);
  global.runningClient.one.status = false;
  await h.createTask().rss();
  assert.equal(h.history.get('hash-0').reason, sleepReason);
  assert.equal(h.state.quotaCalls, 0);
  assert.equal(h.pushes.length, 0);
});

test('success within or exactly at the limit is accepted regardless of torrent publication age', async t => {
  const h = setup(t, 600);
  h.state.items = h.items(1);
  const task = h.createTask({ maxSleepTime: '600' });
  await task.rss();
  assert.deepEqual(h.pushes, ['hash-0']);
  assert.equal(task.maxSleepTime, 600);
  assert.equal(task.lastRssTime, h.time());
  assert.equal(h.history.size, 0);
});

test('one second past the threshold triggers sleep protection', async t => {
  const h = setup(t, 601);
  h.state.items = h.items(1);
  await h.createTask().rss();
  assert.equal(h.pushes.length, 0);
  assert.equal(h.history.get('hash-0').reason, sleepReason);
});

test('repeated fetch failure and repeated scheduling never move the trusted baseline', async t => {
  const h = setup(t);
  h.state.failFetch = true;
  const task = h.createTask();
  const previous = h.store.get(successKey);
  for (let i = 0; i < 12; i++) {
    await task.scheduleRssFetch();
    await assert.rejects(task.rss(), /所有源均失败/);
    h.advance(60);
  }
  assert.equal(h.store.get(successKey), previous);
  assert.equal(h.redisWrites.length, 0);
  assert.equal(h.notifications.length, 12);
  h.state.failFetch = false;
  h.state.items = h.items(1);
  await task.rss();
  assert.equal(h.pushes.length, 0);
  assert.equal(h.history.get('hash-0').reason, sleepReason);
});

test('a successful empty feed establishes a baseline without a downloader', async t => {
  const h = setup(t, null);
  global.runningClient = {};
  const task = h.createTask();
  await task.rss();
  assert.equal(task.lastRssTime, h.time());
  assert.equal(h.history.size, 0);
  global.runningClient.one = { id: 'one', alias: 'mock', status: true, maindata: {} };
  h.advance(60);
  h.state.items = h.items(1);
  await task.rss();
  assert.deepEqual(h.pushes, ['hash-0']);
});

test('unavailable clients for longer than 600 seconds do not become RSS sleep', async t => {
  const h = setup(t);
  global.runningClient.one.status = false;
  const task = h.createTask();
  for (let i = 0; i < 12; i++) {
    h.state.items = [h.items(i + 1)[i]];
    await task.rss();
    assert.equal(task.lastRssTime, h.time());
    h.advance(60);
  }
  assert.ok([...h.history.values()].every(row => row.reason === '拒绝原因: 无可用下载器'));
  global.runningClient.one.status = true;
  h.state.items = [h.items(13)[12]];
  await task.rss();
  assert.deepEqual(h.pushes, ['hash-12']);
});

test('exhausted quota for longer than 600 seconds does not cause false sleep when quota recovers', async t => {
  const h = setup(t);
  h.state.allowed = 0;
  const task = h.createTask();
  for (let i = 0; i < 12; i++) {
    h.state.items = [h.items(i + 1)[i]];
    await task.rss();
    assert.equal(task.lastRssTime, h.time());
    h.advance(60);
  }
  assert.ok([...h.history.values()].every(row => row.reason.includes('单小时推送上限')));
  h.state.allowed = 1;
  h.state.items = [h.items(13)[12]];
  await task.rss();
  assert.deepEqual(h.pushes, ['hash-12']);
});

test('all items rejected by rules for longer than 600 seconds still count as healthy fetches', async t => {
  const h = setup(t);
  const task = h.createTask();
  task.rejectRules = [{ alias: 'reject all' }];
  task._fitRule = () => true;
  for (let i = 0; i < 12; i++) {
    h.state.items = [h.items(i + 1)[i]];
    await task.rss();
    assert.equal(task.lastRssTime, h.time());
    h.advance(60);
  }
  task.rejectRules = [];
  h.state.items = [h.items(13)[12]];
  await task.rss();
  assert.deepEqual(h.pushes, ['hash-12']);
});

test('downstream push failures do not turn successful source fetches into sleep', async t => {
  const h = setup(t);
  h.state.failPush = true;
  const task = h.createTask();
  for (let i = 0; i < 12; i++) {
    h.state.items = h.items(1);
    await task.rss();
    assert.equal(task.lastRssTime, h.time());
    h.advance(60);
  }
  h.state.failPush = false;
  await task.rss();
  assert.deepEqual(h.pushes, ['hash-0']);
});

test('URL edits and instance recreation preserve a trustworthy baseline; later inactivity is still protected', async t => {
  const h = setup(t);
  h.state.items = h.items(1);
  const first = h.createTask();
  await first.rss();
  first.destroy();
  h.advance(60);
  const second = h.createTask({ rssUrls: ['https://replacement.test/rss'] });
  h.state.items = [h.items(2)[1]];
  await second.rss();
  assert.deepEqual(h.pushes, ['hash-0', 'hash-1']);
  second.destroy();
  h.advance(601);
  const third = h.createTask();
  h.state.items = [h.items(3)[2]];
  await third.rss();
  assert.equal(h.history.get('hash-2').reason, sleepReason);
  assert.equal(h.pushes.length, 2);
});

test('protection records must be durable before establishing the recovered baseline', async t => {
  const h = setup(t, 86400);
  const previous = h.store.get(successKey);
  h.state.items = h.items(50);
  h.state.failHistory = true;
  const task = h.createTask();
  await assert.rejects(task.rss(), /history write failed/);
  assert.equal(h.store.get(successKey), previous);
  assert.equal(task.lastRssTime, Number(previous));
  assert.equal(h.pushes.length, 0);
  h.advance(60);
  h.state.failHistory = false;
  await task.rss();
  assert.equal(h.history.size, 50);
  assert.equal(h.pushes.length, 0);
  assert.equal(h.store.get(successKey), String(h.time()));
});

test('sleep protection only records unprocessed non-frozen torrents', async t => {
  const h = setup(t, 86400);
  h.state.items = h.items(3);
  h.history.set('hash-0', { type: 1, reason: 'already processed' });
  h.state.items[1].name = '[FROZEN] Torrent';
  await h.createTask().rss();
  assert.equal(h.history.get('hash-0').reason, 'already processed');
  assert.equal(h.history.has('hash-1'), false);
  assert.equal(h.history.get('hash-2').reason, sleepReason);
});

test('manual torrent push and dryrun cannot initialize or refresh the subscription success baseline', async t => {
  const h = setup(t, null);
  h.state.items = h.items(1);
  const task = h.createTask({ dryrun: true });
  await task.rss(h.state.items);
  assert.deepEqual(h.pushes, ['hash-0']);
  assert.equal(task.lastRssTime, 0);
  assert.equal(h.store.has(successKey), false);
  await task.dryrun();
  assert.equal(h.store.has(successKey), false);
  await task.rss();
  assert.equal(h.history.get('hash-0').reason, sleepReason);
  assert.equal(h.pushes.length, 1);
});

test('multi-source partial success counts as a task-level successful fetch', async t => {
  const h = setup(t);
  h.state.items = h.items(1);
  const task = h.createTask({ rssUrls: ['https://failed.test/rss', 'https://tracker.test/rss'] });
  await task.rss();
  assert.deepEqual(h.pushes, ['hash-0']);
  assert.equal(h.store.get(successKey), String(h.time()));
});

test('sleep compares successful fetch times, not the beginning of a slow request', async t => {
  const h = setup(t, 540);
  h.state.items = h.items(1);
  h.state.fetchSeconds = 61;
  await h.createTask().rss();
  assert.equal(h.history.get('hash-0').reason, sleepReason);
  assert.equal(h.pushes.length, 0);
});

test('downstream processing must not overwrite success time with a late completion timestamp', async t => {
  const h = setup(t);
  h.state.items = h.items(1);
  h.state.processingSeconds = 700;
  const fetchedAt = h.time();
  const task = h.createTask();
  await task.rss();
  assert.equal(h.store.get(successKey), String(fetchedAt));
  assert.equal(task.lastRssTime, fetchedAt);
  h.state.processingSeconds = 0;
  h.state.items = [h.items(2)[1]];
  await task.rss();
  assert.equal(h.history.get('hash-1').reason, sleepReason);
});

test('malformed and future persisted success times cannot bypass the protected first cycle', async t => {
  const h = setup(t, null);
  h.state.items = h.items(1);
  for (const value of ['not-a-time', 'Infinity', '0', '-1', String(h.time() + 10000)]) {
    h.store.set(successKey, value);
    h.history.clear();
    await h.createTask().rss();
    assert.equal(h.pushes.length, 0);
    assert.equal(h.history.get('hash-0').reason, sleepReason);
    assert.equal(h.store.get(successKey), String(h.time()));
  }
});

test('Redis read failure uses first-cycle protection instead of trusting a missing in-memory timestamp', async t => {
  const h = setup(t);
  h.state.failRead = true;
  h.state.items = h.items(1);
  await h.createTask().rss();
  assert.equal(h.pushes.length, 0);
  assert.equal(h.history.get('hash-0').reason, sleepReason);
});

test('persistence failure keeps an in-memory success, while instance recreation remains conservative', async t => {
  const h = setup(t, null);
  h.state.items = h.items(1);
  h.state.failPersist = true;
  const task = h.createTask();
  await task.rss();
  assert.equal(task.lastRssTime, h.time());
  assert.equal(h.store.has(successKey), false);
  assert.equal(h.history.get('hash-0').reason, sleepReason);
  h.advance(60);
  h.state.items = h.items(2);
  await task.rss();
  assert.deepEqual(h.pushes, ['hash-1']);
  task.destroy();
  h.state.failPersist = false;
  h.advance(60);
  h.state.items = h.items(3);
  await h.createTask().rss();
  assert.equal(h.history.get('hash-2').reason, sleepReason);
  assert.deepEqual(h.pushes, ['hash-1']);
  assert.equal(h.store.get(successKey), String(h.time()));
});

test('a clock rollback cannot cause an old future baseline to bypass protection', async t => {
  const h = setup(t);
  const task = h.createTask();
  await task.rss();
  h.advance(-1000);
  h.state.items = h.items(1);
  await task.rss();
  assert.equal(h.history.get('hash-0').reason, sleepReason);
  assert.equal(h.store.get(successKey), String(h.time()));
  assert.equal(h.pushes.length, 0);
});

test('a recent success remains recent even if downstream history storage fails', async t => {
  const h = setup(t);
  h.state.items = h.items(1);
  global.runningClient.one.status = false;
  h.state.failHistory = true;
  const task = h.createTask();
  await assert.rejects(task.rss(), /history write failed/);
  assert.equal(task.lastRssTime, h.time());
  assert.equal(h.store.get(successKey), String(h.time()));
});

test('invalid configured sleep limits fall back to 600 seconds rather than disabling protection', async t => {
  const h = setup(t, 601);
  h.state.items = h.items(1);
  for (const value of [undefined, 'bad-limit', -1, Infinity]) {
    h.store.set(successKey, String(h.time() - 601));
    h.history.clear();
    const task = h.createTask({ maxSleepTime: value });
    assert.equal(task.maxSleepTime, 600);
    await task.rss();
    assert.equal(h.history.get('hash-0').reason, sleepReason);
  }
});
