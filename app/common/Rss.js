const rss = require('../libs/rss');
const util = require('../libs/util');
const logger = require('../libs/logger');
const redis = require('../libs/redis');
const cron = require('node-cron');
const bencode = require('bencode');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const moment = require('moment');
const RssTaskQueue = require('../libs/queues/RssTaskQueue');
const rateLimiter = require('../libs/rate-limiter');
const Push = require('./Push');
const allocator = require('../libs/rss-allocator');
const dispatcher = require('../libs/rss-dispatcher')({ rss, util, logger, redis, moment, allocator });

class Rss {
  constructor(rss) {
    this._rss = rss;
    this.id = rss.id;
    this.alias = rss.alias;
    const maxSleepTime = Number(rss.maxSleepTime);
    this.maxSleepTime = Number.isFinite(maxSleepTime) && maxSleepTime >= 0 ? maxSleepTime : 600;
    this.lastRssTime = 0;
    // 旧 last_time 可能把抓取失败记为成功，不能用作新保护逻辑的基线。
    this.lastRssTimeKey = `vertex:rss:last_success:${this.id}`;
    this.lastRssTimeReady = this._loadLastRssTime();
    this.rateLimitKey = `vertex:rss:rate:${this.id}`;
    this.rateLimitResetReady = this._resetRateLimitCounter();
    this.urls = rss.rssUrls;
    this.clientArr = rss.clientArr || [rss.client];
    this.clientSortBy = rss.clientSortBy;
    this.autoReseed = rss.autoReseed;
    this.onlyReseed = rss.onlyReseed;
    this.reseedClients = rss.reseedClients;
    this.pushMessage = rss.pushMessage;
    this.skipSameTorrent = rss.skipSameTorrent;
    this.scrapeFree = rss.scrapeFree;
    this.scrapeHr = rss.scrapeHr;
    this.sleepTime = rss.sleepTime;
    this.cookie = rss.cookie;
    this.savePath = rss.savePath;
    this.category = rss.category;
    this.paused = rss.paused;
    this.autoTMM = rss.autoTMM;
    this.useCustomRegex = rss.useCustomRegex;
    this.regexStr = rss.regexStr;
    this.replaceStr = rss.replaceStr;
    this.downloadUrlReplaceRules = Array.isArray(rss.downloadUrlReplaceRules) ? rss.downloadUrlReplaceRules : [];
    this.addCountPerHour = +rss.addCountPerHour || 20;
    this.addCount = 0;
    this.pushTorrentFile = rss.pushTorrentFile;
    this.notify = util.listPush().filter(item => item.id === rss.notify)[0] || {};
    this.notify.push = rss.pushNotify;
    this.notify.dryrun = rss.dryrun;
    this.ntf = new Push(this.notify);
    this._acceptRules = rss.acceptRules || [];
    this._rejectRules = rss.rejectRules || [];

    // 初始化任务队列（全局单例）
    if (!global.rssTaskQueue) {
      global.rssTaskQueue = new RssTaskQueue();
    }
    this.taskQueue = global.rssTaskQueue;
    this.acceptRules = util.listRssRule().filter(item => (this._acceptRules.indexOf(item.id) !== -1)).sort((a, b) => +b.priority - +a.priority);
    this.rejectRules = util.listRssRule().filter(item => (this._rejectRules.indexOf(item.id) !== -1)).sort((a, b) => +b.priority - +a.priority);
    this.downloadLimit = util.calSize(rss.downloadLimit, rss.downloadLimitUnit);
    this.uploadLimit = util.calSize(rss.uploadLimit, rss.uploadLimitUnit);
    this.maxClientUploadSpeed = util.calSize(rss.maxClientUploadSpeed, rss.maxClientUploadSpeedUnit);
    this.maxClientDownloadSpeed = util.calSize(rss.maxClientDownloadSpeed, rss.maxClientDownloadSpeedUnit);
    this.maxClientDownloadCount = +rss.maxClientDownloadCount;
    this.isRunning = false;
    this.destroyed = false;
    if (!rss.dryrun) {
      // 修改为入队操作
      this.rssJob = cron.schedule(rss.cron, () => this.scheduleRssFetch());
      this.clearCount = cron.schedule('0 * * * *', () => this.scheduleClearCount());

      logger.info('Rss 任务', this.alias, '初始化完毕');
    }
  }

  _getRuleHostname(host) {
    if (!host) return '';
    try {
      return new URL(host.indexOf('://') === -1 ? `http://${host}` : host).hostname.toLowerCase();
    } catch (e) {
      return host.toLowerCase();
    }
  }

  _normalizeTorrentUrl(url) {
    let torrentUrl = rss.normalizeTorrentUrl(url);
    if (!torrentUrl || this.downloadUrlReplaceRules.length === 0) return torrentUrl;
    try {
      const parsedUrl = new URL(torrentUrl);
      const matchedRule = this.downloadUrlReplaceRules.find(rule => rule && rule.from && rule.to && parsedUrl.hostname.toLowerCase() === this._getRuleHostname(rule.from));
      if (!matchedRule) return torrentUrl;
      parsedUrl.hostname = this._getRuleHostname(matchedRule.to);
      return parsedUrl.toString();
    } catch (e) {
      return torrentUrl;
    }
  }

  _normalizeTorrentUrls(torrents) {
    torrents.forEach(torrent => {
      if (torrent && torrent.url) {
        torrent.url = this._normalizeTorrentUrl(torrent.url);
      }
    });
    return torrents;
  }

  async _loadLastRssTime() {
    if (!this.lastRssTimeKey) return;
    try {
      const stored = await redis.get(this.lastRssTimeKey);
      const parsed = Number(stored);
      if (Number.isFinite(parsed) && parsed > 0 && parsed <= moment().unix()) {
        this.lastRssTime = parsed;
        logger.info(this.alias, `加载上次RSS成功抓取时间: ${moment.unix(parsed).format('YYYY-MM-DD HH:mm:ss')}`);
      } else if (stored) {
        logger.warn(this.alias, 'RSS成功抓取时间无效，将启用首轮保护');
      }
    } catch (error) {
      logger.error(this.alias, '加载上次RSS成功抓取时间失败，将启用首轮保护:', error);
    }
  }

  async _persistLastRssTime(timestamp) {
    if (!this.lastRssTimeKey) return;
    if (!Number.isFinite(timestamp)) return;
    this.lastRssTime = timestamp;
    try {
      await redis.set(this.lastRssTimeKey, String(timestamp));
    } catch (error) {
      logger.error(this.alias, '持久化RSS成功抓取时间失败:', error);
    }
  }

  async _resetRateLimitCounter() {
    if (!this.rateLimitKey) return;
    try {
      await rateLimiter.reset(this.rateLimitKey);
      logger.info(this.alias, '已重置RSS推送限流计数');
    } catch (error) {
      logger.error(this.alias, '重置RSS推送限流计数失败:', error);
    }
  }

  async _batchRejectTorrents(torrents, reason) {
    if (!torrents || torrents.length === 0) return;
    const nowTime = moment().unix();
    const rows = torrents.map((torrent) => [
      torrent.hash,
      torrent.name,
      torrent.size,
      this.id,
      torrent.link,
      nowTime,
      2,
      reason
    ]);
    await util.runRecords('INSERT INTO torrents (hash, name, size, rss_id, link, record_time, record_type, record_note) values (?, ?, ?, ?, ?, ?, ?, ?)', rows);
    for (const torrent of torrents) {
      await this.ntf.rejectTorrent(this._rss, undefined, torrent, reason);
    }
  }

  _all(str, keys) {
    if (!keys || keys.length === 0) return true;
    for (const key of keys) {
      if (str.indexOf(key) === -1) return false;
    }
    return true;
  };

  _sum(arr) {
    let sum = 0;
    for (const item of arr) {
      sum += item;
    }
    return sum;
  }

  _getSum(a, b) {
    return a + b;
  };

  async _downloadTorrent(url, _hash) {
    if (_hash && fs.existsSync(path.join(__dirname, '../../torrents', _hash + '.torrent'))) {
      return { hash: _hash, filepath: path.join(__dirname, '../../torrents', _hash + '.torrent') };
    }
    const res = await util.requestPromise({
      url: this._normalizeTorrentUrl(url),
      method: 'GET',
      encoding: null,
      headers: {
        cookie: this.cookie
      }
    });
    const buffer = Buffer.from(res.body, 'utf-8');
    if (res.statusCode < 200 || res.statusCode >= 300 || buffer[0] !== 100) {
      const contentType = res.headers['content-type'] || '';
      const bodyPrefix = buffer.slice(0, 80).toString('utf8').replace(/[\r\n\t]+/g, ' ');
      throw new Error(`下载种子文件失败: 状态码 ${res.statusCode}, Content-Type ${contentType}, 响应前缀 ${bodyPrefix}`);
    }
    const torrent = bencode.decode(buffer);
    const size = torrent.info.length || torrent.info.files.map(i => i.length).reduce(this._getSum, 0);
    const fsHash = crypto.createHash('sha1');
    fsHash.update(bencode.encode(torrent.info));
    const md5 = fsHash.digest('md5');
    let hash = '';
    for (const v of md5) {
      hash += v < 16 ? '0' + v.toString(16) : v.toString(16);
    };
    const filepath = path.join(__dirname, '../../torrents', hash + '.torrent');
    fs.writeFileSync(filepath, buffer);
    return {
      filepath,
      hash,
      size,
      name: torrent.info.name.toString()
    };
  };

  _fitConditions(_torrent, conditions) {
    let fit = true;
    const torrent = { ..._torrent };
    torrent.description = torrent.description || '';
    for (const condition of conditions) {
      let value;
      switch (condition.compareType) {
        case 'equals':
          fit = fit && (torrent[condition.key] === condition.value || torrent[condition.key] === +condition.value);
          break;
        case 'bigger':
          value = 1;
          condition.value.split('*').forEach(item => {
            value *= +item;
          });
          fit = fit && torrent[condition.key] > value;
          break;
        case 'smaller':
          value = 1;
          condition.value.split('*').forEach(item => {
            value *= +item;
          });
          fit = fit && torrent[condition.key] < value;
          break;
        case 'contain':
          fit = fit && condition.value.split(',').filter(item => torrent[condition.key].indexOf(item) !== -1).length !== 0;
          break;
        case 'includeIn':
          fit = fit && condition.value.split(',').indexOf(torrent[condition.key]) !== -1;
          break;
        case 'notContain':
          fit = fit && condition.value.split(',').filter(item => torrent[condition.key].indexOf(item) !== -1).length === 0;
          break;
        case 'notIncludeIn':
          fit = fit && condition.value.split(',').indexOf(torrent[condition.key]) === -1;
          break;
        case 'regExp':
          fit = fit && (torrent[condition.key] + '').match(new RegExp(condition.value, 'ig'));
          break;
        case 'notRegExp':
          fit = fit && !(torrent[condition.key] + '').match(new RegExp(condition.value, 'ig'));
          break;
      }
    }
    return fit;
  }

  _fitRule(_rule, _torrent, allocationContext) {
    const rule = { ..._rule };
    const torrent = { ..._torrent };
    if (rule.type === 'javascript') {
      try {
        // Resource predicates see pending RSS admissions, not an unreserved old
        // count. This synchronous resource view does not mutate global state.
        // Read by rule.code through direct eval below.
        // eslint-disable-next-line no-unused-vars
        const global = allocationContext || globalThis;
        // eslint-disable-next-line no-eval
        return (eval(rule.code))(torrent);
      } catch (e) {
        logger.error(this.alias, 'Rss 规则', rule.alias, '存在语法错误\n', e);
        return false;
      }
    } else {
      try {
        return rule.conditions.length !== 0 && this._fitConditions(torrent, rule.conditions);
      } catch (e) {
        logger.error(this.alias, 'Rss 规则', rule.alias, '遇到错误\n', e);
        return false;
      }
    }
  }

  destroy() {
    this.destroyed = true;
    logger.info('销毁 Rss 实例:', this.alias);
    this.rssJob.stop();
    delete this.rssJob;
    this.clearCount.stop();
    delete this.clearCount;

    // 设置实例为非运行状态
    this.isRunning = false;

    // 清理任务队列中的阻塞状态
    if (this.taskQueue) {
      try {
        this.taskQueue.cleanupRssOnDestroy(this.id);
        logger.info(`已清理RSS源 ${this.alias} 的阻塞状态`);
      } catch (error) {
        logger.error(`清理RSS源 ${this.alias} 任务队列失败:`, error);
      }
    }

    delete global.runningRss[this.id];
  }

  /**
   * 将种子信息缓存到Redis中，用于跳过相同种子检查
   * @param {string} clientId - 下载器ID
   * @param {object} torrent - 种子信息对象，需包含hash和size
   * @param {number} expireTime - 缓存过期时间（秒），默认10分钟
   */
  async cacheTorrentToClient(clientId, torrent, expireTime = 600) {
    try {
      // 基础参数检查
      if (!clientId || !torrent || !torrent.hash || !torrent.size) {
        logger.warn(this.alias, `缓存种子参数不完整: clientId=${clientId}, torrent=${JSON.stringify(torrent || {})}`);
        return;
      }

      // 缓存种子hash
      const hashKey = `vertex:client_torrent:${clientId}:hash:${torrent.hash}`;
      await redis.setWithExpire(hashKey, '1', expireTime);

      // 缓存种子大小
      const sizeKey = `vertex:client_torrent:${clientId}:size:${torrent.size}`;
      await redis.setWithExpire(sizeKey, torrent.hash, expireTime);

      logger.debug(this.alias, `缓存种子到客户端 ${clientId}, Hash: ${torrent.hash}, Size: ${util.formatSize(torrent.size)}, 名称: ${torrent.name?.substring(0, 30)}..., 过期时间: ${expireTime}秒`);
    } catch (err) {
      logger.error(this.alias, `缓存种子到Redis失败: ${err.message}`);
    }
  }

  /**
   * 检查客户端是否已存在相同的种子（通过hash或size）
   * @param {string} clientId - 下载器ID
   * @param {object} torrent - 种子信息对象，需包含hash和size
   * @return {object|null} - 如果存在返回{exists: true, reason: '原因'}, 否则返回null
   */
  async checkTorrentExistsInClient(clientId, torrent) {
    try {
      // 基础参数检查
      if (!clientId || !torrent || !torrent.hash || !torrent.size) {
        logger.warn(this.alias, `检查客户端种子存在性参数不完整: clientId=${clientId}, torrent=${JSON.stringify(torrent || {})}`);
        return null;
      }

      // 检查hash是否存在
      const hashKey = `vertex:client_torrent:${clientId}:hash:${torrent.hash}`;
      const hashExists = await redis.get(hashKey);

      if (hashExists) {
        return {
          exists: true,
          reason: '拒绝原因: 种子已添加过'
        };
      }

      // 检查size是否存在
      const sizeKey = `vertex:client_torrent:${clientId}:size:${torrent.size}`;
      const sizeExists = await redis.get(sizeKey);

      if (sizeExists) {
        return {
          exists: true,
          reason: '拒绝原因: 下载器中已存在同大小种子',
          existingHash: sizeExists
        };
      }

      return null;
    } catch (err) {
      logger.error(this.alias, `检查客户端种子存在性失败: ${err.message}`);
      return null; // 出错时返回null，允许继续处理
    }
  }

  reloadRssRule() {
    logger.info('重新载入 Rss 规则', this.alias);
    this.acceptRules = util.listRssRule().filter(item => (this._acceptRules.indexOf(item.id) !== -1)).sort((a, b) => +b.priority - +a.priority);
    this.rejectRules = util.listRssRule().filter(item => (this._rejectRules.indexOf(item.id) !== -1)).sort((a, b) => +b.priority - +a.priority);
  }

  reloadPush() {
    logger.info('Rss', this.alias, '重新载入推送方式');
    this.notify = util.listPush().filter(item => item.id === this._rss.notify)[0] || {};
    this.notify.push = this._rss.pushNotify;
    this.ntf = new Push(this.notify);
  }

  _logTorrentTiming (torrent, stage, startedAt, client) {
    const now = Date.now();
    const pubTime = Number(torrent.pubTime);
    const hasPubTime = Number.isFinite(pubTime) && pubTime > 0;
    const discoveredAt = torrent._rssDiscoveredAt;
    logger.info(this.alias, 'RSS种子阶段:', JSON.stringify({
      stage,
      hash: torrent.hash,
      client: client?.alias,
      stageMs: Number.isFinite(startedAt) ? now - startedAt : 0,
      discoveredAt: Number.isFinite(discoveredAt) ? moment(discoveredAt).format('YYYY-MM-DD HH:mm:ss.SSS') : null,
      sinceDiscoveryMs: Number.isFinite(discoveredAt) ? now - discoveredAt : null,
      publishedAt: hasPubTime ? moment.unix(pubTime).format('YYYY-MM-DD HH:mm:ss') : null,
      sincePublicationMs: hasPubTime ? now - pubTime * 1000 : null
    }));
  }

  async _pushTorrent(torrent, client) {
    return await dispatcher.push.call(this, torrent, client);
  }

  async rss(_torrents) {
    if (this.lastRssTimeReady) {
      await this.lastRssTimeReady;
      this.lastRssTimeReady = null;
    }
    if (this.rateLimitResetReady) {
      await this.rateLimitResetReady;
      this.rateLimitResetReady = null;
    }
    const startTime = moment();
    logger.debug(this.alias, 'RSS任务开始执行');

    let torrents = [];
    if (_torrents) {
      torrents = this._normalizeTorrentUrls(_torrents);
    } else {
      // 从多个URL获取种子并合并，单个源失败不中断整体
      const results = await Promise.allSettled(this.urls.map(url => rss.getTorrents(url)));
      const sourceErrors = [];
      let successCount = 0;
      for (let i = 0; i < results.length; i++) {
        const result = results[i];
        if (result.status === 'fulfilled') {
          successCount += 1;
          torrents.push(...result.value);
        } else {
          sourceErrors.push(result.reason);
        }
      }
      for (const error of sourceErrors) {
        logger.error(this.alias, 'RSS源获取失败:', error);
      }
      if (successCount === 0) {
        const error = new Error('RSS获取失败: 所有源均失败');
        logger.error(this.alias, error.message);
        await this.ntf.rssError(this._rss);
        throw error;
      }

      // 根据hash去重，防止不同URL源提供相同种子
      const uniqueTorrents = [];
      const hashSet = new Set();
      for (const torrent of torrents) {
        if (!hashSet.has(torrent.hash)) {
          hashSet.add(torrent.hash);
          uniqueTorrents.push(torrent);
        }
      }
      torrents = this._normalizeTorrentUrls(uniqueTorrents);
    }

    const fetchedAt = Date.now();
    const fetchMs = fetchedAt - startTime.valueOf();
    logger.debug(this.alias, `获取种子完成，耗时: ${fetchMs}ms，数量: ${torrents.length}`);

    // 使用上一轮真正成功抓取的时间，而不是当前轮调度/开始尝试的时间。
    // 手动推送不属于订阅抓取，既不触发休眠保护，也不刷新成功基线。
    const successTime = moment().unix();
    const previousSuccessTime = this.lastRssTime;
    const hasSuccessTime = Number.isFinite(previousSuccessTime) && previousSuccessTime > 0 && previousSuccessTime <= successTime;
    const rejectAfterSleep = !_torrents && (!hasSuccessTime || successTime - previousSuccessTime > this.maxSleepTime);
    if (!_torrents && !rejectAfterSleep) {
      // 成功获取/解析即记为成功，不能被下载器状态、额度、规则或后续推送结果影响。
      await this._persistLastRssTime(successTime);
    }

    // 过滤掉已处理和被冻结的种子和超过每小时推送上限的种子
    let newTorrents = [];

    const existingHashes = new Set();
    const hashList = Array.from(new Set(torrents.map(item => item.hash).filter(Boolean)));
    if (hashList.length > 0) {
      const batchSize = 300;
      for (let i = 0; i < hashList.length; i += batchSize) {
        const batch = hashList.slice(i, i + batchSize);
        const placeholders = batch.map(() => '?').join(',');
        const rows = await util.getRecords(
          `SELECT hash FROM torrents WHERE rss_id = ? AND hash IN (${placeholders})`,
          [this.id, ...batch]
        );
        for (const row of rows) {
          existingHashes.add(row.hash);
        }
      }
    }

    // 长期失败恢复、首次运行及旧版本迁移时，先将本轮未处理种子记为拒绝。
    // 拒绝记录落库前不能前移基线，否则写入失败后的下一轮可能补抓整个历史列表。
    if (rejectAfterSleep) {
      const pendingTorrents = torrents.filter(torrent => !existingHashes.has(torrent.hash) && torrent.name.indexOf('[FROZEN]') === -1);
      const detail = hasSuccessTime
        ? `距上次成功抓取 ${successTime - previousSuccessTime}s, 阈值 ${this.maxSleepTime}s`
        : '无可信成功抓取记录，启用首轮保护';
      logger.warn(this.alias, `触发最长休眠时间拒绝: ${detail}, 待处理 ${pendingTorrents.length}`);
      await this._batchRejectTorrents(pendingTorrents, '拒绝原因: 最长休眠时间');
      await this._persistLastRssTime(successTime);
      return;
    }

    // 过滤种子
    const rejectedRuleRows = [];
    for (const torrent of torrents) {
      // 检查是否在数据库中已存在
      if (torrent.hash && existingHashes.has(torrent.hash)) continue;
      // 检查是否被冻结
      if (torrent.name.indexOf('[FROZEN]') !== -1) continue;

      // 本轮首次见到的未处理种子；与站点 pubDate 分开记录，便于定位源更新延迟。
      torrent._rssDiscoveredAt = fetchedAt;
      this._logTorrentTiming(torrent, '发现');

      // 检查拒绝规则
      let reject = false;
      for (const rejectRule of this.rejectRules) {
        if (this._fitRule(rejectRule, torrent)) {
          rejectedRuleRows.push([
            torrent.hash,
            torrent.name,
            torrent.size,
            this.id,
            torrent.link,
            moment().unix(),
            2,
            `拒绝规则: ${rejectRule.alias}`
          ]);
          await this.ntf.rejectTorrent(this._rss, undefined, torrent, `拒绝规则: ${rejectRule.alias}`);
          reject = true;
          break;
        }
      }
      if (reject) continue;

      newTorrents.push(torrent);
    }
    if (rejectedRuleRows.length > 0) {
      await util.runRecords('INSERT INTO torrents (hash, name, size, rss_id, link, record_time, record_type, record_note) values (?, ?, ?, ?, ?, ?, ?, ?)', rejectedRuleRows);
    }

    logger.debug(this.alias, `种子过滤完成，耗时: ${moment().diff(startTime, 'seconds')}秒，有效数量: ${newTorrents.length}`);

    // 如果没有有效种子，直接返回
    if (newTorrents.length === 0) {
      return;
    }

    logger.info(this.alias, `RSS阶段统计: 获取=${fetchMs}ms, 过滤=${Date.now() - fetchedAt}ms, 待处理=${newTorrents.length}`);

    // Include explicit rule/reseed targets: an empty ordinary candidate set
    // must not reject a healthy forced route before its predicate is evaluated.
    if (!dispatcher.canPrepare.call(this)) {
      await this._batchRejectTorrents(newTorrents, '拒绝原因: 无可用下载器');
      return;
    }

    // 检查每小时推送上限（Redis滑动窗口）
    if (this.addCountPerHour > 0) {
      const limit = Number(this.addCountPerHour);
      const key = this.rateLimitKey;
      // 按大小排序（从大到小），优先接受大种子
      newTorrents.sort((a, b) => +b.size - +a.size);
      const consumeResult = await rateLimiter.consume(key, limit, 3600, newTorrents.length);
      const allowed = Math.max(0, consumeResult.allowed);
      const remainingBefore = consumeResult.remaining + allowed;
      const usedBefore = limit - remainingBefore;
      if (allowed < newTorrents.length) {
        const acceptableTorrents = newTorrents.slice(0, allowed);
        const rejectedTorrents = newTorrents.slice(allowed);
        const limitReason = `拒绝原因: 达到单小时推送上限: 本轮前 ${usedBefore} / ${limit}，本轮接受 ${allowed} 个`;
        await this._batchRejectTorrents(rejectedTorrents, limitReason);
        logger.info(this.alias, `每小时推送上限为 ${limit}，本轮前已推送 ${usedBefore}，本轮接受 ${acceptableTorrents.length} 个种子，拒绝 ${rejectedTorrents.length} 个种子`);
        newTorrents = acceptableTorrents;
      } else {
        logger.info(this.alias, `每小时推送上限为 ${limit}，本轮前已推送 ${usedBefore}，本轮接受 ${allowed} 个种子`);
      }
      if (newTorrents.length === 0) {
        return;
      }
    }

    await dispatcher.batch.call(this, newTorrents);
  }

  async dryrun() {
    const torrents = (await Promise.all(this.urls.map(url => rss.getTorrents(url)))).flat();
    for (const torrent of torrents) {
      let reject = false;
      for (const rejectRule of this.rejectRules) {
        if (this._fitRule(rejectRule, torrent)) {
          torrent.status = '匹配到拒绝规则: ' + rejectRule.alias;
          reject = true;
          break;
        }
      }
      if (reject) {
        continue;
      }
      const fitRules = this.acceptRules.filter(item => this._fitRule(item, torrent));
      if (this.acceptRules.length === 0) {
        torrent.status = '无选择规则, 默认选中该种子';
        continue;
      } else if (fitRules.length === 0) {
        torrent.status = '未匹配到规则';
        continue;
      } else {
        torrent.status = '匹配到选择规则: ' + fitRules[0].alias;
        continue;
      }
    }
    return torrents;
  }

  async mikanSearch(name) {
    const torrents = await util.mikanSearch(name);
    for (const torrent of torrents) {
      let reject = false;
      for (const rejectRule of this.rejectRules) {
        if (this._fitRule(rejectRule, torrent)) {
          torrent.status = '匹配到拒绝规则: ' + rejectRule.alias;
          reject = true;
          break;
        }
      }
      if (reject) {
        continue;
      }
      const fitRules = this.acceptRules.filter(item => this._fitRule(item, torrent));
      if (this.acceptRules.length === 0) {
        torrent.status = '无选择规则, 默认选中该种子';
        continue;
      } else if (fitRules.length === 0) {
        torrent.status = '未匹配到规则';
        continue;
      } else {
        torrent.status = '匹配到选择规则: ' + fitRules[0].alias;
        continue;
      }
    }
    return torrents;
  }

  /**
   * 测试Redis缓存机制的正确性
   * @param {string} clientId - 客户端ID
   * @param {string} hash - 种子哈希值
   * @param {number} size - 种子大小
   * @return {Object} 测试结果
   */
  async testCacheMechanism(clientId, hash, size) {
    const results = {
      cacheStatus: { status: 'untested' },
      redisMethods: {
        keys: typeof redis.keys === 'function',
        get: typeof redis.get === 'function',
        set: typeof redis.set === 'function',
        del: typeof redis.del === 'function',
        expire: typeof redis.expire === 'function',
        setWithExpire: typeof redis.setWithExpire === 'function'
      }
    };

    // 测试Redis缓存
    try {
      // 创建测试种子对象
      const testTorrent = { hash, size, name: '测试种子' };

      // 测试缓存写入
      await this.cacheTorrentToClient(clientId, testTorrent, 60); // 1分钟过期
      results.cacheStatus.write = 'success';

      // 测试缓存读取 - 通过hash
      const hashExists = await this.checkTorrentExistsInClient(clientId, testTorrent);
      results.cacheStatus.readByHash = hashExists && hashExists.exists ? 'success' : 'failed';

      // 测试缓存读取 - 通过size
      const sizeExists = await redis.get(`vertex:client_torrent:${clientId}:size:${size}`);
      results.cacheStatus.readBySize = sizeExists ? 'success' : 'failed';

      // 测试缓存过期时间
      const ttlHash = await redis.ttl(`vertex:client_torrent:${clientId}:hash:${hash}`);
      const ttlSize = await redis.ttl(`vertex:client_torrent:${clientId}:size:${size}`);
      results.cacheStatus.ttlHash = ttlHash;
      results.cacheStatus.ttlSize = ttlSize;

      // 测试缓存删除
      await redis.del(`vertex:client_torrent:${clientId}:hash:${hash}`);
      await redis.del(`vertex:client_torrent:${clientId}:size:${size}`);
      const afterDeleteHash = await redis.get(`vertex:client_torrent:${clientId}:hash:${hash}`);
      results.cacheStatus.delete = !afterDeleteHash ? 'success' : 'failed';

      results.cacheStatus.status = 'success';
    } catch (e) {
      results.cacheStatus.error = e.message;
      results.cacheStatus.status = 'error';
    }

    return results;
  }

  // 调度RSS获取任务
  async scheduleRssFetch() {
    try {
      const taskId = await this.taskQueue.enqueue({
        rssId: this.id,
        action: 'fetchRss'
      });
      if (taskId) logger.debug(this.alias, `RSS任务已调度: id=${taskId}`);
    } catch (error) {
      logger.error(`调度RSS任务失败: ${this.alias}`, error);
    }
  }

  // 调度清零计数任务
  async scheduleClearCount() {
    try {
      await this.taskQueue.enqueue({
        rssId: this.id,
        action: 'clearCount'
      });
    } catch (error) {
      logger.error(`调度清零任务失败: ${this.alias}`, error);
    }
  }
}
module.exports = Rss;
