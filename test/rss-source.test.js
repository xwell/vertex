const assert = require('node:assert/strict');
const test = require('node:test');
const { promisify } = require('util');
const { loadModule, createLogger } = require('./helpers/load-module');

const emptyFeed = '<rss version="2.0"><channel><title>Test</title></channel></rss>';
const feed = `<rss version="2.0"><channel><title>Test</title><item>
  <title>Test torrent</title><link>https://tracker.test/details.php?id=123</link>
  <guid>test-hash</guid><pubDate>Sat, 24 Oct 2026 12:00:00 +0800</pubDate>
  <enclosure length="1024" url="https://tracker.test/download.php?id=123&amp;passkey=secret"/>
</item></channel></rss>`;

function setup (response = { statusCode: 200, body: feed }) {
  const cache = new Map();
  const writes = [];
  const requests = [];
  const logger = createLogger();
  const redis = {
    get: async key => cache.get(key),
    del: async key => cache.delete(key),
    setWithExpire: async (key, value, ttl) => {
      cache.set(key, value);
      writes.push({ key, value, ttl });
    }
  };
  const rss = loadModule('app/libs/rss.js', {
    './util': {
      promisify,
      requestPromise: async options => {
        requests.push(options);
        if (response instanceof Error) throw response;
        return response;
      }
    },
    './redis': redis,
    './logger': logger
  });
  return { rss, cache, writes, requests, logger };
}

test('valid RSS is parsed and cached; a cache hit makes no network request', async () => {
  const { rss, writes, requests } = setup();
  const url = 'https://tracker.test/rss?passkey=secret';
  const torrents = await rss.getTorrents(url);
  assert.equal(torrents.length, 1);
  assert.equal(torrents[0].hash, 'test-hash');
  assert.equal(torrents[0].url, 'https://tracker.test/download.php?id=123&passkey=secret');
  assert.equal(writes[0].ttl, 40);
  assert.deepEqual(await rss.getTorrents(url), torrents);
  assert.equal(requests.length, 1);
});

test('HTTP failures reject instead of becoming empty success and are never cached', async () => {
  for (const statusCode of [403, 429, 500, 503]) {
    const { rss, writes } = setup({ statusCode, body: feed });
    await assert.rejects(rss.getTorrents('https://tracker.test/rss'), new RegExp(`HTTP ${statusCode}`));
    assert.equal(writes.length, 0);
  }
});

test('invalid, non-RSS and empty responses are not cached', async () => {
  for (const body of ['', null, '<rss><channel>', '<html><body>Access denied</body></html>', '{"error":"rate limited"}']) {
    const { rss, writes } = setup({ statusCode: 200, body });
    await assert.rejects(rss.getTorrents('https://tracker.test/rss'));
    assert.equal(writes.length, 0);
  }
});

test('a valid empty feed succeeds for the different site adapters', async () => {
  const hosts = [
    'tracker.test', 'rss.m-team.cc', 'pt.sjtu.edu.cn', 'filelist.io', 'beyond-hd.me',
    'bitporn.eu', 'jptv.club', 'torrentdb.net', 'www.empornium.is', 'jpopsuki.eu',
    'www.skyey2.com', 'hdbits.org', 'hd-torrents.org', 'hdcity.city', 'iptorrents.com',
    'mikanani.me', 'learnflakes.net', 'avistaz.to', 'rss.torrentleech.org', 'api.fsm.name',
    'www.happyfappy.org', 'kimoji.club', 'fappaizuri.me', 'lusthive.org', 'cinematik.net'
  ];
  for (const host of hosts) {
    const { rss } = setup({ statusCode: 200, body: emptyFeed });
    assert.deepEqual(await rss.getTorrents(`https://${host}/rss?key=test`), [], host);
  }
});

test('invalid cache left by an older version is evicted and refreshed in the same fetch', async () => {
  const { rss, cache, requests, writes } = setup();
  const url = 'https://tracker.test/rss';
  cache.set(`vertex:rss:${url}`, '<html>Old error page</html>');
  assert.equal((await rss.getTorrents(url)).length, 1);
  assert.equal(requests.length, 1);
  assert.equal(writes.length, 1);
  assert.equal(cache.get(`vertex:rss:${url}`), feed);
});

test('XML viewer markup does not force a 310-second cache; site-specific intervals remain', async () => {
  for (const [host, ttl] of [['tracker.test', 40], ['lemon.test', 150], ['hhanclub.net', 150], ['sharkpt.net', 310]]) {
    const { rss, writes } = setup({ statusCode: 200, body: `<html><style id="xml-viewer-style"></style>${feed}</html>` });
    assert.equal((await rss.getTorrents(`https://${host}/rss`)).length, 1);
    assert.equal(writes[0].ttl, ttl, host);
    assert.ok(!writes[0].value.includes('xml-viewer-style'));
  }
  const { rss, writes } = setup({ statusCode: 200, body: '<html>xml-viewer-style</html>' });
  await assert.rejects(rss.getTorrents('https://tracker.test/rss'), /未找到 RSS/);
  assert.equal(writes.length, 0);
});

test('Puppeteer responses without an HTTP status remain compatible', async () => {
  const { rss, writes } = setup({ body: `<html>xml-viewer-style${feed}</html>` });
  assert.equal((await rss.getTorrents('https://tracker.test/rss')).length, 1);
  assert.equal(writes[0].ttl, 40);
});

test('site-specific cache-busting and SoulVoice authentication remain compatible', async t => {
  for (const [url, hasSuffix] of [
    ['https://piggo.me/rss?key=test', false],
    ['https://rss.m-team.cc/rss?key=test', false],
    ['https://kamept.com/rss?key=test', true]
  ]) {
    const { rss, requests } = setup({ statusCode: 200, body: emptyFeed });
    await rss.getTorrents(url);
    assert.equal(requests[0] !== url, hasSuffix);
    if (hasSuffix) assert.ok(requests[0].startsWith(url + '&'));
  }
  const previous = global.runningSite;
  t.after(() => { global.runningSite = previous; });
  global.runningSite = { SoulVoice: { cookie: 'test-cookie' } };
  const { rss, requests } = setup({ statusCode: 200, body: emptyFeed });
  await rss.getTorrents('https://pt.soulvoice.club/rss');
  assert.equal(requests[0].headers.cookie, 'test-cookie');
});

test('invalid source URLs reject with sanitized errors before making requests', async () => {
  const { rss, requests, logger } = setup();
  await assert.rejects(rss.getTorrents('not-a-url?passkey=private-secret'), /无效RSS地址/);
  assert.equal(requests.length, 0);
  assert.ok(!JSON.stringify(logger.entries).includes('private-secret'));
});

test('network error codes propagate, without leaking the RSS URL into errors or logs', async () => {
  const url = 'https://tracker.test/rss?passkey=private-secret';
  const original = Object.assign(new Error(`request failed: ${url}`), { code: 'ETIMEDOUT' });
  const { rss, logger, writes } = setup(original);
  await assert.rejects(rss.getTorrents(url), error => {
    assert.equal(error.code, 'ETIMEDOUT');
    assert.match(error.message, /tracker.test/);
    assert.ok(!error.message.includes('private-secret'));
    return true;
  });
  assert.ok(!JSON.stringify(logger.entries).includes('private-secret'));
  assert.equal(writes.length, 0);
});
