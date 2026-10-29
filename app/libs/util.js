const util = require('util');
const fs = require('fs');
const path = require('path');
const uuid = require('uuid');
const tar = require('tar');
const md5 = require('md5-node');
const CryptoJS = require('crypto-js');
const cron = require('node-cron');
const request = require('request');
const requestWithDeadline = require('./request-with-deadline');
const Database = require('better-sqlite3');
const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
const url = require('url');
const { JSDOM } = require('jsdom');
const moment = require('moment');
const redlock = require('./redlock');

const logger = require('./logger');
const scrape = require('./scrape');

const db = new Database(path.join(__dirname, '../db/sql.db'));

// 启用 WAL 模式以提高并发性能和数据安全性
try {
  db.pragma('journal_mode = WAL');
  logger.info('SQLite WAL 模式已启用');
} catch (e) {
  logger.error('启用 WAL 模式失败:', e);
}

// 数据库迁移函数
const runMigrations = function () {
  try {
    // 创建每日统计表
    db.exec(`
      CREATE TABLE IF NOT EXISTS daily_stats (
        id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
        stats_date TEXT NOT NULL UNIQUE,
        total_uploaded INTEGER NOT NULL DEFAULT 0,
        total_downloaded INTEGER NOT NULL DEFAULT 0,
        add_count INTEGER NOT NULL DEFAULT 0,
        reject_count INTEGER NOT NULL DEFAULT 0,
        delete_count INTEGER NOT NULL DEFAULT 0,
        per_tracker_stats TEXT,
        created_at INTEGER NOT NULL
      );
    `);
    
    // 创建基础索引
    db.exec(`
      CREATE INDEX IF NOT EXISTS index_daily_stats_date
      ON daily_stats (stats_date);
    `);
    
    // 🚀 性能优化：添加关键索引
    logger.info('开始创建性能优化索引...');
    
    // torrent_flow 表索引 - 用于时间范围查询（update: 原表已经有 time, hash 索引）
    // db.exec(`
    //   CREATE INDEX IF NOT EXISTS index_torrent_flow_time
    //   ON torrent_flow (time);
    // `);
    
    // // torrent_flow 表索引 - 用于连接查询
    // db.exec(`
    //   CREATE INDEX IF NOT EXISTS index_torrent_flow_hash
    //   ON torrent_flow (hash);
    // `);
    
    // torrents 表索引 - 用于连接查询 (update: 原表已经有 hash 索引)
    // db.exec(`
    //   CREATE INDEX IF NOT EXISTS index_torrents_hash
    //   ON torrents (hash);
    // `);
    
    // torrents 表索引 - 用于时间范围查询
    db.exec(`
      CREATE INDEX IF NOT EXISTS index_torrents_record_time
      ON torrents (record_time);
    `);
    
    // 组合索引 - 优化 torrent_flow 的时间+hash 查询
    db.exec(`
      CREATE INDEX IF NOT EXISTS index_torrent_flow_time_hash
      ON torrent_flow (time, hash);
    `);
    
    // torrents 表组合索引 - 优化按类型和时间的查询
    db.exec(`
      CREATE INDEX IF NOT EXISTS index_torrents_record_type_time
      ON torrents (record_type, record_time);
    `);

    // torrents 表新增 client_id 字段
    const torrentColumns = db.prepare('PRAGMA table_info(torrents)').all().map(item => item.name);
    if (torrentColumns.indexOf('client_id') === -1) {
      db.exec('ALTER TABLE torrents ADD COLUMN client_id TEXT');
      logger.info('torrents 表新增 client_id 字段');
    }
    
    // 追加索引 - 覆盖查询以优化 sum(upload)/sum(download) 等聚合
    db.exec(`
      CREATE INDEX IF NOT EXISTS idx_torrents_record_time_covering
      ON torrents (record_time, upload, download);
    `);
    
    // 追加索引 - 提升按 delete_time 的条件过滤
    db.exec(`
      CREATE INDEX IF NOT EXISTS idx_torrents_delete_time
      ON torrents (delete_time);
    `);
    
    // 追加索引 - tracker + 时间范围查询，并覆盖 upload/download 聚合
    db.exec(`
      CREATE INDEX IF NOT EXISTS idx_torrents_tracker_time_covering
      ON torrents (tracker, record_time, upload, download);
    `);
    
    // 🎯 种子历史查询优化索引
    logger.info('开始创建种子历史查询优化索引...');
    
    // 1. 优化record_type + id排序的查询
    db.exec(`
      CREATE INDEX IF NOT EXISTS idx_torrents_record_type_id_desc 
      ON torrents (record_type, id DESC);
    `);
    
    // 2. 优化rss_id + id排序的查询
    db.exec(`
      CREATE INDEX IF NOT EXISTS idx_torrents_rss_id_id_desc 
      ON torrents (rss_id, id DESC);
    `);
    
    // 3. 优化record_type + rss_id + id的组合查询
    db.exec(`
      CREATE INDEX IF NOT EXISTS idx_torrents_type_rss_id_desc 
      ON torrents (record_type, rss_id, id DESC);
    `);
    
    // tracker_flow 表索引 - 用于时间范围查询 (update: 原表已经有 time 索引)
    // db.exec(`
    //   CREATE INDEX IF NOT EXISTS index_tracker_flow_time
    //   ON tracker_flow (time);
    // `);
    
    // sites 表索引 - 用于时间范围查询
    db.exec(`
      CREATE INDEX IF NOT EXISTS index_sites_update_time
      ON sites (update_time);
    `);
    
    logger.info('数据库迁移和索引优化完成');
  } catch (e) {
    logger.error('数据库迁移失败:', e);
  }
};

// 执行迁移
runMigrations();

puppeteer.use(StealthPlugin());

let browser;

const ttl = 15000;

for (const k of Object.keys(util)) {
  exports[k] = util[k];
}

exports.redlock = redlock;

exports.getRecords = async function (sql, options = []) {
  let _sql = sql;
  if (options) {
    options.forEach((item) => {
      _sql = _sql.replace(/\?/, item);
    });
  }
  logger.debug('Get Records:', _sql);
  return db.prepare(sql).all(...options);
};

exports.runRecord = async function (sql, options = []) {
  let _sql = sql;
  if (options) {
    options.forEach((item) => {
      _sql = _sql.replace(/\?/, item);
    });
  }
  logger.debug('Run Record:', _sql);
  return db.prepare(sql).run(...options);
};

exports.runRecords = async function (sql, rows = []) {
  if (!rows || rows.length === 0) {
    return;
  }
  const stmt = db.prepare(sql);
  const trx = db.transaction((items) => {
    for (const row of items) {
      stmt.run(...row);
    }
  });
  return trx(rows);
};

exports.getRecord = async function (sql, options = []) {
  let _sql = sql;
  if (options) {
    options.forEach((item) => {
      _sql = _sql.replace(/\?/, item);
    });
  }
  logger.debug('Get Record:', _sql);
  return db.prepare(sql).get(...options);
};

const _importJson = function (path) {
  const jsonString = fs.readFileSync(path, { encoding: 'utf-8' });
  return JSON.parse(jsonString);
};

exports.scrapeFree = scrape.free;
exports.scrapeHr = scrape.hr;

exports._requestPromise = util.promisify(request);
exports.requestPromise = async function (_options, usePuppeteer = true, deadline = null) {
  let options;
  if (typeof _options === 'string') {
    options = {
      url: _options
    };
  } else {
    options = { ..._options };
  }
  if (!options.headers) {
    options.headers = {};
  };
  options.headers['User-Agent'] = global.userAgent || 'Vertex';
  if (!options.timeout) {
    options.timeout = 120000;
  }
  if (global.proxy) {
    const host = new URL(options.url).host;
    if (global.domains.split('\n').indexOf(host) !== -1) {
      options.proxy = global.proxy;
    }
  }
  if (global.trustAllCerts) {
    options.rejectUnauthorized = !global.trustAllCerts;
  }
  const res = deadline ? await requestWithDeadline(options, deadline) : await exports._requestPromise(options);
  if (usePuppeteer && res.body && typeof res.body === 'string' && (res.body.indexOf('trk_jschal_js') !== -1 || res.body.indexOf('jschl-answer') !== -1 || (res.body.indexOf('cloudflare-static') !== -1 && res.body.indexOf('email-decode.min.js') === -1))) {
    logger.info(new url.URL(options.url).hostname, '疑似遇到 5s 盾, 启用 Puppeteer 抓取页面....');
    return await exports.requestUsePuppeteer(options);
  }
  return res;
};

exports.requestUsePuppeteer = async function (options) {
  const lock = await exports.redlock.lock('vertex:puppeteer', ttl);
  logger.debug('locked');
  try {
    if (!browser || browser.process().exitCode === 0) {
      browser = await puppeteer.launch({
        executablePath: '/usr/bin/chromium',
        args: [
          '--no-sandbox'
        ],
        headless: false
      });
    }
    lock.unlock();
    logger.debug('unlocked');
  } catch (e) {
    logger.error(e);
    lock.unlock();
    logger.debug('unlocked');
  }
  const page = await browser.newPage();
  try {
    await page.setViewport({ width: 800, height: 600 });
    await page.setUserAgent(options.headers['User-Agent']);
    if (options.headers.cookie) {
      const cookieList = options.headers.cookie.split(';').map(i => i.trim())
        .filter(i => i !== '')
        .map(i => {
          return {
            name: i.split('=')[0],
            value: i.split('=')[1],
            domain: new url.URL(options.url).hostname
          };
        });
      if (cookieList.length !== 0) await page.setCookie(...cookieList);
    }
    await page.goto(options.url, {});
    await page.waitForTimeout(30000);
    const body = await page.content();
    await page.close();
    if ((await browser.pages()).length === 1) {
      logger.info('关闭 Puppeteer....');
      await browser.close();
    }
    return {
      body
    };
  } catch (e) {
    logger.error('Puppeteer 报错:\n', e);
    await page.close();
    if ((await browser.pages()).length === 1) {
      logger.info('关闭 Puppeteer....');
      await browser.close();
    }
    return {
      body: ''
    };
  }
};

exports.exec = util.promisify(require('child_process').exec);
exports.uuid = uuid;
exports.md5 = md5;
exports.tar = tar;

exports.formatQSString = function (qs) {
  return Object.keys(qs).filter(item => qs[item]).map(item => `${item}=${encodeURIComponent(qs[item])}`).join('&');
};

exports.scrapeNameByFile = async function (_filename, type, _year = false, useFullname = false) {
  const filename = _filename.replace(/[[\]]/g, '');
  if (!global.tmdbApiKey) {
    throw new Error('未填写 The Movie Database Api Key');
  }
  let url = `https://api.themoviedb.org/3/search/${type || 'multi'}?language=zh-CN&api_key=${global.tmdbApiKey}&query=`;
  let searchKey = filename.split(/19\d\d|20\d\d|S\d\d/)[0]
    .replace(/[a-zA-z]+TV(\dK)?\.?/, '')
    .replace(/(Jade)\.?/, '')
    .replace(/[\u4e00-\u9fa5\uff01\uff1a]+\d+[\u4e00-\u9fa5\uff01\uff1a]+/g, '')
    .replace(/[\u4e00-\u9fa5\uff01\uff1a。:?？，,·・]/g, '')
    .replace(/\./g, ' ').trim();
  if (filename.match(/^[\u4e00-\u9fa5·]+[A-RT-Za-rt-z]*[\u4e00-\u9fa5·]+/)) {
    searchKey = filename.match(/^[\u4e00-\u9fa5·]+[A-RT-Za-rt-z]*[\u4e00-\u9fa5·]+/)[0].replace(/[^\u4e00-\u9fa5A-Za-z]/g, ' ').replace(/第.*季/g, '');
  }
  let year = filename.match(/[^d](19\d{2}|20\d{2})[^d]/);
  const season = filename.match(/S0[^1]/);
  if (useFullname) {
    searchKey = _filename.replace(/\.\d{4}$/, '');
    year = _filename.match(/\.(\d{4})$/, '');
  }
  url += encodeURI(searchKey);
  if (year && !season) {
    url += `&first_air_date_year=${year[1]}&primary_release_year=${year[1]}`;
  }
  const res = await exports.requestPromise(url);
  const body = JSON.parse(res.body);
  if (body.status_code === 7) {
    logger.error(filename, searchKey, body);
    throw new Error('The Movie Database Api Key 失效, 请重试');
  }
  if (!body.results) {
    logger.error(filename, searchKey, body);
    throw new Error('请求 TMDB Api 返回有误, 请重试');
  }
  body.results = body.results.sort((a, b) => b.popularity - a.popularity);
  const temp = body.results.filter(item => item.name === searchKey || item.original_name === searchKey);
  if (temp[0]) {
    body.results = temp;
  }
  logger.debug('根据文件名抓取影视剧名', filename, searchKey, body.results[0]?.name || body.results[0]?.title || '');
  logger.debug('tmdb api url', url);
  if (_year) {
    return {
      name: body.results[0]?.name || body.results[0]?.title || '',
      year: (body.results[0]?.release_date || body.results[0]?.first_air_date || '').split('-')[0],
      type: type || body.results[0]?.media_type
    };
  }
  return body.results[0]?.name || body.results[0]?.title || '';
};

exports.scrapeEpisodeByFilename = function (_filename, ignoreKeys = '') {
  let filename = _filename
    .replace(/【/g, '[')
    .replace(/】/g, ']');
  ignoreKeys.split(',').forEach((item) => { filename = filename.replace(new RegExp(item, 'gi'), ''); });
  let episode = +(filename.match(/[Ee][Pp]?(\d+)[. ]+/) || [])[1];
  if (!episode) {
    episode = +(filename.match(/\[(\d+)[Vv]?\d?\]/) || [])[1];
  }
  if (!episode) {
    episode = +(filename.match(/第(\d+)[话話集]/) || [])[1];
  }
  if (!episode) {
    const episodes = filename
      .replace(/((mp)|(MP)|(Mp))4/g, '')
      .replace(/\d+[KkFfPpi年月日期季]+/g, '')
      .replace(/\d+\.\d+/g, '')
      .replace(/[xXHh]26[45]/g, '')
      .replace(/[Ss]\d+/g, '')
      .replace(/((1st)|(2nd)|(3rd))/g, '')
      .replace(/\[[.\dA-Za-z_-]*\]/g, '')
      .replace(/\d+[MmBbGg]/g, '')
      .replace(/\d*[Xx]\d+/g, '')
      .match(/\d+/g)
      ?.map(item => +item.match(/\d+/))
      .filter(item => [0, 480, 720, 1080, 576, 2160, 1920].indexOf(item) === -1) || [];
    if (episodes.length === 1) {
      episode = episodes[0];
    }
  };
  return episode;
};

exports.listSite = function () {
  const files = fs.readdirSync(path.join(__dirname, '../data/site'));
  const list = [];
  for (const file of files) {
    if (path.extname(file) === '.json') {
      list.push(_importJson(path.join(__dirname, '../data/site', file)));
    }
  }
  return list;
};

exports.listPush = function () {
  const files = fs.readdirSync(path.join(__dirname, '../data/push'));
  const list = [];
  for (const file of files) {
    if (path.extname(file) === '.json') {
      list.push(_importJson(path.join(__dirname, '../data/push', file)));
    }
  }
  return list;
};

exports.listClient = function () {
  const files = fs.readdirSync(path.join(__dirname, '../data/client'));
  const clientList = [];
  for (const file of files) {
    if (path.extname(file) === '.json') {
      clientList.push(_importJson(path.join(__dirname, '../data/client', file)));
    }
  }
  return clientList;
};

exports.listServer = function () {
  const files = fs.readdirSync(path.join(__dirname, '../data/server'));
  const serverList = [];
  for (const file of files) {
    if (path.extname(file) === '.json') {
      serverList.push(_importJson(path.join(__dirname, '../data/server', file)));
    }
  }
  return serverList;
};

exports.listRss = function () {
  const files = fs.readdirSync(path.join(__dirname, '../data/rss'));
  const rssList = [];
  for (const file of files) {
    if (path.extname(file) === '.json') {
      const rss = _importJson(path.join(__dirname, '../data/rss', file));
      if (rss.rssUrl && !rss.rssUrls) {
        rss.rssUrls = [rss.rssUrl];
      }
      rssList.push(rss);
    }
  }
  return rssList;
};

exports.listDeleteRule = function () {
  const files = fs.readdirSync(path.join(__dirname, '../data/rule/delete'));
  const deleteRuleList = [];
  for (const file of files) {
    if (path.extname(file) === '.json') {
      deleteRuleList.push(_importJson(path.join(__dirname, '../data/rule/delete', file)));
    }
  }
  return deleteRuleList;
};

exports.listLinkRule = function () {
  const files = fs.readdirSync(path.join(__dirname, '../data/rule/link'));
  const linkRuleList = [];
  for (const file of files) {
    if (path.extname(file) === '.json') {
      linkRuleList.push(_importJson(path.join(__dirname, '../data/rule/link', file)));
    }
  }
  return linkRuleList;
};

exports.listRssRule = function () {
  const files = fs.readdirSync(path.join(__dirname, '../data/rule/rss'));
  const rssRuleList = [];
  for (const file of files) {
    if (path.extname(file) === '.json') {
      rssRuleList.push(_importJson(path.join(__dirname, '../data/rule/rss', file)));
    }
  }
  return rssRuleList;
};

exports.listRaceRule = function () {
  const files = fs.readdirSync(path.join(__dirname, '../data/rule/race'));
  const raceRuleList = [];
  for (const file of files) {
    if (path.extname(file) === '.json') {
      raceRuleList.push(_importJson(path.join(__dirname, '../data/rule/race', file)));
    }
  }
  return raceRuleList;
};

exports.listRaceRuleSet = function () {
  const files = fs.readdirSync(path.join(__dirname, '../data/rule/raceSet'));
  const raceRuleSetList = [];
  for (const file of files) {
    if (path.extname(file) === '.json') {
      raceRuleSetList.push(_importJson(path.join(__dirname, '../data/rule/raceSet', file)));
    }
  }
  return raceRuleSetList;
};

exports.listDouban = function () {
  const files = fs.readdirSync(path.join(__dirname, '../data/douban'));
  const DoubanList = [];
  for (const file of files) {
    if (path.extname(file) === '.json') {
      const douban = _importJson(path.join(__dirname, '../data/douban', file));
      if (douban.enable === undefined) {
        douban.enable = true;
      }
      DoubanList.push(douban);
    }
  }
  return DoubanList;
};

exports.listDoubanSet = function () {
  const files = fs.readdirSync(path.join(__dirname, '../data/douban/set'));
  const doubanSetList = [];
  for (const file of files) {
    if (path.extname(file) === '.json') {
      doubanSetList.push(_importJson(path.join(__dirname, '../data/douban/set', file)));
    }
  }
  return doubanSetList;
};

exports.listCrontabJavaScript = function () {
  const files = fs.readdirSync(path.join(__dirname, '../data/script'));
  const scriptList = [];
  for (const file of files) {
    if (path.extname(file) === '.json') {
      scriptList.push(_importJson(path.join(__dirname, '../data/script', file)));
    }
  }
  return scriptList;
};

exports.listWatch = function () {
  const files = fs.readdirSync(path.join(__dirname, '../data/watch'));
  const watchList = [];
  for (const file of files) {
    if (path.extname(file) === '.json') {
      watchList.push(_importJson(path.join(__dirname, '../data/watch', file)));
    }
  }
  return watchList;
};

exports.listWatchSet = function () {
  const files = fs.readdirSync(path.join(__dirname, '../data/watch/set'));
  const watchSetList = [];
  for (const file of files) {
    if (path.extname(file) === '.json') {
      watchSetList.push(_importJson(path.join(__dirname, '../data/watch/set', file)));
    }
  }
  return watchSetList;
};

exports.listIRC = function () {
  const files = fs.readdirSync(path.join(__dirname, '../data/irc'));
  const ircList = [];
  for (const file of files) {
    if (path.extname(file) === '.json') {
      ircList.push(_importJson(path.join(__dirname, '../data/irc', file)));
    }
  }
  return ircList;
};

exports.getLinkMapping = function () {
  return _importJson(path.join(__dirname, '../data/link-mapping.json'));
};

exports.saveLinkMapping = function () {
  const mappingPath = path.join(__dirname, '../data/link-mapping.json');
  return fs.writeFileSync(mappingPath, JSON.stringify(global.linkMapping, null, 2));
};

exports.formatSize = function (size) {
  if (size < 1024) {
    return `${size.toFixed(2)} Byte`;
  }
  if (size < 1024 * 1024) {
    return `${(size / 1024).toFixed(2)} KiB`;
  }
  if (size < 1024 * 1024 * 1024) {
    return `${(size / 1024 / 1024).toFixed(2)} MiB`;
  }
  if (size < 1024 * 1024 * 1024 * 1024) {
    return `${(size / 1024 / 1024 / 1024).toFixed(3)} GiB`;
  }
  if (size < 1024 * 1024 * 1024 * 1024 * 1024) {
    return `${(size / 1024 / 1024 / 1024 / 1024).toFixed(3)} TiB`;
  }
  if (size < 1024 * 1024 * 1024 * 1024 * 1024 * 1024) {
    return `${(size / 1024 / 1024 / 1024 / 1024 / 1024).toFixed(3)} PiB`;
  }
};

exports.calSize = function (size, unit) {
  const unitMap = {
    KiB: 1024,
    MiB: 1024 * 1024,
    GiB: 1024 * 1024 * 1024,
    TiB: 1024 * 1024 * 1024 * 1024,
    PiB: 1024 * 1024 * 1024 * 1024 * 1024
  };
  return Math.floor(+size * (unitMap[unit] || 1));
};

exports.formatTime = function (ms) {
  const d = parseInt(ms / (24 * 3600 * 1000));
  const h = parseInt(ms / (3600 * 1000) % 24);
  const m = parseInt(ms / (60 * 1000) % 60);
  const s = parseInt(ms / (1000) % 60);
  const ss = parseInt(ms % 1000);
  let str = '';
  if (d) {
    str += `${d < 10 ? '0' + d : d}:`;
  }
  if (h) {
    str += `${h < 10 ? '0' + h : h}:`;
  }
  if (m) {
    str += `${m < 10 ? '0' + m : m}:`;
  }
  if (s) {
    str += `${s < 10 ? '0' + s : s}:`;
  }
  str += `${ss < 10 ? '00' + ss : ss < 100 ? '0' + ss : ss}`;
  return str;
};

exports.sleep = function (time) {
  return new Promise((resolve, reject) => {
    setTimeout(() => {
      return resolve('ping');
    }, time);
  });
};

exports.randomColor = function () {
  return '#' + (new Array(6)).fill(1).map(() => '0123456789abcdef'[parseInt(Math.random() * 15)]).join('');
};

exports.mikanSearch = async function (name) {
  const url = `https://mikanani.me/Home/Search?searchstr=${encodeURIComponent(name)}`;
  const html = (await exports.requestPromise({
    url: url
  })).body;
  const document = new JSDOM(html).window.document;
  const torrentDoms = document.querySelectorAll('.js-search-results-row');
  const torrents = [];
  for (const _torrent of torrentDoms) {
    const torrent = {
      size: 0,
      name: '',
      hash: '',
      id: 0,
      url: '',
      link: ''
    };
    torrent.name = _torrent.querySelector('.magnet-link-wrap').innerHTML.trim();
    torrent.size = _torrent.children[1].innerHTML.trim();
    if (torrent.size) {
      torrent.size = exports.calSize(...torrent.size.replace(/ ?([MGKT])B/, ' $1iB').replace(/,/g, '').split(' '));
    } else {
      torrent.size = 0;
    }
    const link = _torrent.querySelector('.magnet-link-wrap').href.trim();
    torrent.link = 'https://mikanani.me' + link;
    torrent.hash = link.match(/Episode\/(.*)/)[1];
    torrent.id = torrent.hash;
    torrent.url = 'https://mikanani.me' + _torrent.querySelector('a[href*=Download]').href.trim();
    torrent.pubTime = moment(_torrent.children[2].innerHTML.trim().replace(/\//g, '-')).unix();
    torrents.push(torrent);
  }
  return torrents;
};

exports.syncCookieCloud = async (cc) => {
  const { uuid, passwd, host, sites, douban } = cc;
  const { body } = await exports.requestPromise(`${host}/get/${uuid}`);
  const { encrypted } = JSON.parse(body);
  const key = CryptoJS.MD5(uuid + '-' + passwd).toString().substring(0, 16);
  const decrypted = CryptoJS.AES.decrypt(encrypted, key).toString(CryptoJS.enc.Utf8);
  const parsed = JSON.parse(decrypted);
  const cookies = Object.values(parsed.cookie_data).flat().map(item => ({
    domain: item.domain,
    cookie: `${item.name}=${item.value}`
  }));

  const _sites = exports.listSite();
  const _doubans = exports.listDouban();
  for (const s of sites) {
    // 判断站点是否启用
    const __site = _sites.filter(item => item.name === s)[0];
    if (!__site || !global.runningSite[s]) {
      continue;
    }
    // 拿到 host
    const sitehost = new URL(global.runningSite[s].index).host;
    // 过滤出 cookie
    const cookie = cookies.filter(item => item.domain.replace(/^\./, '') === sitehost).map(item => item.cookie).join(';');
    if (__site.cookie === cookie) {
      logger.info('站点', __site.name, 'Cookie 未改变');
      continue;
    }
    __site.cookie = cookie;
    // 写入文件
    fs.writeFileSync(path.join(__dirname, '../data/site', __site.name + '.json'), JSON.stringify(__site, null, 2));
    global.runningSite[s].cookie = __site.cookie;
    logger.info('站点', __site.name, '同步 Cookie');
  }

  // douban
  if (douban) {
    for (const d of Object.values(global.runningDouban)) {
      const _douban = _doubans.filter(item => item.id === d.id)[0];
      const cookie = cookies.filter(item => item.domain.endsWith('douban.com')).map(item => item.cookie).join(';');
      if (_douban.cookie === cookie) {
        logger.info('豆瓣 Cookie 未改变');
        continue;
      }
      _douban.cookie = cookie;
      global.runningDouban[_douban.id].cookie = cookie;
      fs.writeFileSync(path.join(__dirname, '../data/douban/', d.id + '.json'), JSON.stringify(_douban, null, 2));
      logger.info('豆瓣同步 Cookie');
    }
  }
};

exports.initCookieCloud = function () {
  const setting = JSON.parse(fs.readFileSync(path.join(__dirname, '../data/setting.json')));
  if (global.cookiecloud) {
    global.cookiecloud.stop();
    delete global.cookiecloud;
  }
  if (!setting.cookiecloud?.enable) {
    return;
  }
  const cc = setting.cookiecloud;
  global.cookiecloud = cron.schedule(cc.cron, async () => {
    try {
      logger.info('开始执行 CookieCloud 同步...');
      await exports.syncCookieCloud(cc);
      logger.info('CookieCloud 同步完成');
    } catch (e) {
      logger.error('CookieCloud 同步失败:', e);
    };
  }, {
    scheduled: true,
    timezone: 'Asia/Shanghai'
  });
  // init
};

// 每日统计聚合函数
exports.aggregateDailyStats = async function (targetDate) {
  const moment = require('moment');
  
  // 获取中国时区的moment对象
  const getMomentCN = (input) => {
    if (input) {
      return moment(input).utcOffset(8 * 60); // UTC+8
    }
    return moment().utcOffset(8 * 60); // UTC+8
  };
  
  try {
    const date = targetDate || getMomentCN().subtract(1, 'day').format('YYYY-MM-DD');
    const startTime = getMomentCN(date).startOf('day').unix();
    const endTime = getMomentCN(date).endOf('day').unix();
    
    logger.info(`开始聚合 ${date} 的统计数据 (UTC+8 时区)`);
    logger.info(`时间范围: ${startTime} - ${endTime} (${getMomentCN(startTime * 1000).format('YYYY-MM-DD HH:mm:ss')} - ${getMomentCN(endTime * 1000).format('YYYY-MM-DD HH:mm:ss')})`);
    
    // 检查是否已经存在该日期的统计数据
    const existingStats = await exports.getRecord('SELECT * FROM daily_stats WHERE stats_date = ?', [date]);
    if (existingStats) {
      logger.info(`${date} 的统计数据已存在，跳过聚合`);
      return;
    }
    
    // 检查是否有数据需要聚合
    const torrentCount = await exports.getRecord('SELECT count(*) as count FROM torrents WHERE record_time >= ? AND record_time <= ?', [startTime, endTime]);
    logger.info(`发现 ${torrentCount.count} 条种子记录需要聚合`);
    
    // 聚合种子统计数据
    const uploadDownloadStats = await exports.getRecord(
      'SELECT sum(upload) as uploaded, sum(download) as downloaded FROM torrents WHERE record_time >= ? AND record_time <= ?',
      [startTime, endTime]
    );
    
    const addCount = (await exports.getRecord(
      'SELECT count(*) as count FROM torrents WHERE record_type = 1 AND record_time >= ? AND record_time <= ?',
      [startTime, endTime]
    )).count;
    
    const rejectCount = (await exports.getRecord(
      'SELECT count(*) as count FROM torrents WHERE record_type = 2 AND record_time >= ? AND record_time <= ?',
      [startTime, endTime]
    )).count;
    
    const deleteCount = (await exports.getRecord(
      'SELECT count(*) as count FROM torrents WHERE delete_time IS NOT NULL AND delete_time >= ? AND delete_time <= ?',
      [startTime, endTime]
    )).count;
    
    // 聚合每个tracker的统计数据
    const perTrackerStats = await exports.getRecords(
      'SELECT sum(upload) as uploaded, sum(download) as downloaded, tracker FROM torrents WHERE tracker IS NOT NULL AND record_time >= ? AND record_time <= ? GROUP BY tracker',
      [startTime, endTime]
    );
    
    // 插入聚合数据
    await exports.runRecord(
      'INSERT INTO daily_stats (stats_date, total_uploaded, total_downloaded, add_count, reject_count, delete_count, per_tracker_stats, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
      [
        date,
        uploadDownloadStats.uploaded || 0,
        uploadDownloadStats.downloaded || 0,
        addCount,
        rejectCount,
        deleteCount,
        JSON.stringify(perTrackerStats),
        getMomentCN().unix()
      ]
    );
    
    logger.info(`${date} 的统计数据聚合完成 (UTC+8): 上传=${uploadDownloadStats.uploaded || 0}, 下载=${uploadDownloadStats.downloaded || 0}, 添加=${addCount}, 拒绝=${rejectCount}, 删除=${deleteCount}`);
  } catch (e) {
    logger.error('每日统计聚合失败:', e);
    throw e;
  }
};

// 检查并补齐最近几天的统计数据，避免定时任务错过导致缺口
exports.ensureDailyStats = async function (options = {}) {
  const moment = require('moment');
  const days = Number.isInteger(options.days) ? options.days : 7;
  const skipEmpty = options.skipEmpty !== false;

  const getMomentCN = (input) => {
    if (input) {
      return moment(input).utcOffset(8 * 60); // UTC+8
    }
    return moment().utcOffset(8 * 60); // UTC+8
  };

  try {
    if (days <= 0) {
      logger.info('ensureDailyStats: 跳过执行 (days <= 0)');
      return;
    }

    const today = getMomentCN();
    const endDate = today.clone().subtract(1, 'day').format('YYYY-MM-DD');
    const startDate = today.clone().subtract(days, 'day').format('YYYY-MM-DD');

    const startTime = getMomentCN(startDate).startOf('day').unix();
    const endTime = getMomentCN(endDate).endOf('day').unix();

    if (startTime > endTime) {
      logger.info('ensureDailyStats: 无需补齐 (时间范围为空)');
      return;
    }

    logger.info(`ensureDailyStats: 检查范围 ${startDate} ~ ${endDate} (UTC+8)`);

    const existingStats = await exports.getRecords(
      'SELECT stats_date FROM daily_stats WHERE stats_date >= ? AND stats_date <= ?',
      [startDate, endDate]
    );
    const existingSet = new Set(existingStats.map(item => item.stats_date));

    const dateList = [];
    const cursor = getMomentCN(startDate);
    const endMoment = getMomentCN(endDate);
    while (cursor.isSameOrBefore(endMoment, 'day')) {
      dateList.push(cursor.format('YYYY-MM-DD'));
      cursor.add(1, 'day');
    }

    const missingDates = dateList.filter(date => !existingSet.has(date));
    if (missingDates.length === 0) {
      logger.info('ensureDailyStats: 最近统计数据完整，无需补齐');
      return;
    }

    // 一次性统计范围内每天的记录数量，避免在循环中频繁查询
    const counts = await exports.getRecords(
      "SELECT date(record_time + 28800, 'unixepoch') as stats_date, count(*) as count FROM torrents WHERE record_time >= ? AND record_time <= ? GROUP BY stats_date",
      [startTime, endTime]
    );
    const countMap = new Map(counts.map(item => [item.stats_date, item.count]));

    missingDates.sort();
    logger.info(`ensureDailyStats: 发现 ${missingDates.length} 天缺失，开始补齐`);

    for (const date of missingDates) {
      const count = countMap.get(date) || 0;
      if (skipEmpty && count === 0) {
        logger.info(`ensureDailyStats: ${date} 无种子记录，跳过聚合`);
        continue;
      }
      await exports.aggregateDailyStats(date);
    }
  } catch (e) {
    logger.error('ensureDailyStats 失败:', e);
    throw e;
  }
};

// 数据库优雅关闭方法
exports.closeDatabase = function () {
  try {
    if (db) {
      // 在关闭前执行 WAL 检查点，确保所有更改都写入主数据库文件
      db.pragma('wal_checkpoint(TRUNCATE)');
      logger.info('SQLite WAL 检查点完成');
      
      // 关闭数据库连接
      db.close();
      logger.info('数据库连接已安全关闭');
    }
  } catch (e) {
    logger.error('关闭数据库时发生错误:', e);
  }
};
