const assert = require('node:assert/strict');
const test = require('node:test');
const { loadModule, createLogger } = require('./helpers/load-module');

function setup (t, rss = async () => {}) {
  const logger = createLogger();
  const timers = [];
  const cleared = new Set();
  const enqueued = [];
  class TaskQueue {
    constructor (name, options) { Object.assign(this, options); }
    logTaskFailure () {}
    async enqueue (...args) { enqueued.push(args); return 'queued'; }
  }
  const Queue = loadModule('app/libs/queues/RssTaskQueue.js', {
    '../redis': { TaskQueue },
    '../logger': logger
  }, {
    setTimeout: (callback, ms) => {
      const timer = { callback, ms };
      timers.push(timer);
      return timer;
    },
    clearTimeout: timer => cleared.add(timer)
  });
  const previous = global.runningRss;
  t.after(() => { global.runningRss = previous; });
  const instance = { alias: 'Test RSS', isRunning: false, addCount: 3, rss };
  global.runningRss = { test: instance };
  const queue = new Queue();
  const task = action => ({ id: 'test-job', timestamp: Date.now() - 2000, data: { rssId: 'test', action } });
  return { queue, instance, task, timers, cleared, enqueued, logger };
}

test('real fetch success clears source failures and releases the timeout timer', async t => {
  const { queue, instance, task, timers, cleared, logger } = setup(t);
  queue.recordRssFailure('test', new Error('previous failure'));
  await queue.executeTask(task('fetchRss'));
  assert.equal(queue.failedRssSources.has('test'), false);
  assert.equal(instance.isRunning, false);
  assert.equal(timers[0].ms, 300000);
  assert.ok(cleared.has(timers[0]));
  assert.ok(logger.entries.some(entry => entry.level === 'info' && /排队耗时=/.test(entry.args[0])));
});

test('fetch errors count as failures; clearCount does not reset consecutive source failures', async t => {
  const { queue, instance, task, timers, cleared } = setup(t, async () => { throw new Error('invalid RSS'); });
  await queue.executeTask(task('fetchRss'));
  assert.equal(queue.failedRssSources.get('test').count, 1);
  assert.equal(instance.isRunning, false);
  await queue.executeTask(task('clearCount'));
  assert.equal(instance.addCount, 0);
  assert.equal(queue.failedRssSources.get('test').count, 1);
  assert.ok(timers.every(timer => cleared.has(timer)));
});

test('five consecutive fetch failures block scheduling and unblock after the cooldown', async t => {
  const { queue, task, enqueued } = setup(t, async () => { throw new Error('invalid RSS'); });
  for (let i = 0; i < 5; i++) await queue.executeTask(task('fetchRss'));
  assert.equal(queue.isRssBlocked('test'), true);
  assert.equal(await queue.enqueue({ rssId: 'test', action: 'fetchRss' }), undefined);
  assert.equal(enqueued.length, 0);
  queue.failedRssSources.get('test').lastFailTime = Date.now() - queue.blockDuration - 1;
  assert.equal(await queue.enqueue({ rssId: 'test', action: 'fetchRss' }), 'queued');
  assert.equal(enqueued.length, 1);
});

test('a timeout counts once; skipping a still-running fetch must not erase that failure', async t => {
  let finish;
  const pending = new Promise(resolve => { finish = resolve; });
  const { queue, instance, task, timers, cleared } = setup(t, () => pending);
  const execution = queue.executeTask(task('fetchRss'));
  assert.equal(instance.isRunning, true);
  timers[0].callback();
  await execution;
  assert.equal(queue.failedRssSources.get('test').count, 1);
  assert.ok(cleared.has(timers[0]));
  await queue.executeTask(task('fetchRss'));
  assert.equal(queue.failedRssSources.get('test').count, 1);
  assert.equal(instance.isRunning, true);
  finish();
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(instance.isRunning, false);
});

test('maintenance failures do not count as RSS source failures', async t => {
  const { queue, task } = setup(t);
  queue.executeClearCount = async () => { throw new Error('maintenance error'); };
  await queue.executeTask(task('clearCount'));
  assert.equal(queue.failedRssSources.has('test'), false);
});
