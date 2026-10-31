// Dependencies are injected by Rss: this module does not open Redis/SQLite or
// start timers itself, and the real dispatch path can be tested in isolation.
module.exports = ({ rss, util, logger, redis, moment, allocator }) => {
  const active = task => !task.destroyed;

  async function dispatch (task, torrent, options, send) {
    const request = {
      clientIds: options.clientIds || task.clientArr,
      forcedClientId: options.forcedClientId,
      reseed: !!options.reseed,
      mode: task.clientSortBy || 'uploadSpeed',
      rss: task,
      hash: torrent.hash,
      size: torrent.size,
      isActive: () => active(task)
    };
    const allocation = await allocator.acquire(request);
    if (!allocation.ok) return allocation;
    const { token, client } = allocation;
    let accepted = false;
    let addedAt;
    const admission = {
      reserved: true,
      onAccepted: () => {
        if (accepted) return;
        accepted = true;
        addedAt = moment().unix();
        allocator.accepted(token);
      }
    };
    try {
      if (task.skipSameTorrent && !options.reseed) {
        const exist = await task.checkTorrentExistsInClient(client.id, torrent);
        if (exist?.exists) return { ok: false, reason: exist.reason, client };
        const same = await util.getRecord('SELECT * FROM torrents WHERE hash = ? AND add_time > ? AND record_type = 1', [torrent.hash, moment().unix() - 600]);
        if (same?.id) return { ok: false, reason: '拒绝原因: 种子已添加过', client };
        if (client._client?.type === 'qBittorrent') {
          const peer = client.maindata.torrents.find(item => +item.size === +torrent.size);
          if (peer) {
            await task.cacheTorrentToClient(client.id, peer);
            return { ok: false, reason: '拒绝原因: 下载器中已存在同大小种子', client };
          }
        }
      }
      const readiness = await allocator.ready(token);
      if (!readiness.ok) return { ...readiness, client };
      if (options.rule && !task._fitRule(options.rule, torrent, allocator.ruleContext(token))) return { ok: false, reason: 'accept-rule-changed', client };
      const check = allocator.begin(token, options.truehash);
      if (!check.ok) return { ...check, client };
      logger.info(task.alias, 'RSS分配:', JSON.stringify({ ...allocator.describe(token), ruleId: options.rule?.id || null }));
      try {
        await send(client, admission);
        // Adapters/mocks predating the admission hook return only after their
        // add succeeds. Real Client invokes the hook before post-add SQLite I/O.
        admission.onAccepted();
      } catch (error) {
        if (!accepted) {
          allocator.failed(token);
          logger.error(task.alias, 'RSS添加结果:', JSON.stringify({ lease: token.id, clientId: client.id, outcome: 'uncertain', error: error.code || error.name || 'Error' }));
          return { ok: false, reason: '添加请求失败，结果待确认', client, uncertain: true };
        }
        // A bookkeeping failure after a positive API response must not turn a
        // real successful add into a false failure followed by a second POST.
        logger.error(task.alias, 'RSS添加后记录失败:', JSON.stringify({ lease: token.id, clientId: client.id, error: error.code || error.name || 'Error' }));
      }
      logger.info(task.alias, 'RSS添加结果:', JSON.stringify({ lease: token.id, clientId: client.id, outcome: 'accepted' }));
      return { ok: true, client, addedAt };
    } finally {
      // Only unsent reservations are cancelled. Confirmed/uncertain requests
      // stay in the shared ledger until independently observed/reconciled.
      allocator.cancel(token);
    }
  }

  async function push (torrent, explicitClient) {
    const task = this;
    const startedAt = Date.now();
    const records = { noteRows: [], noteCategoryRows: [], addRows: [] };
    const reject = async (reason, client, type = 2) => {
      const note = reason.startsWith('拒绝原因:') ? reason : '拒绝原因: ' + reason;
      records.noteRows.push([torrent.hash, torrent.name, torrent.size, task.id, torrent.link, moment().unix(), type, note, client?.id || null]);
      try { await task.ntf.rejectTorrent(task._rss, client, torrent, note); } catch (error) { logger.error(task.alias, 'RSS拒绝通知失败:', error.name); }
      return records;
    };
    const recordAdd = (hash, category, client, at, note = '添加种子') => {
      records.addRows.push([hash, torrent.name, torrent.size, task.id, torrent.link, category, at, at, 1, note, client.id]);
    };
    if (!active(task)) return reject('RSS任务已销毁');

    // Reseeding reuses existing data: no new-download slot/space reservation,
    // but the final target still needs a fresh snapshot and shared API capacity.
    if (task.autoReseed && !torrent.hash.includes('fakehash')) {
      for (const id of task.reseedClients || []) {
        const client = global.runningClient?.[id];
        if (!client?.status || !client.maindata) continue;
        for (const peer of client.maindata.torrents) {
          if (+peer.size !== +torrent.size || +peer.completed !== +peer.size) continue;
          const info = await rss.getTorrentNameByBencode(task._normalizeTorrentUrl(torrent.url));
          if (peer.name !== info.name || peer.hash === info.hash) continue;
          const result = await dispatch(task, torrent, { forcedClientId: id, reseed: true, truehash: info.hash }, (target, admission) => {
            return target.addTorrent(task._normalizeTorrentUrl(torrent.url), torrent.hash, true, task.uploadLimit, task.downloadLimit, peer.savePath, task.category, undefined, undefined, admission);
          });
          if (result.ok) {
            task.addCount++;
            recordAdd(torrent.hash, task.category, result.client, result.addedAt, '辅种');
            task._logTorrentTiming(torrent, '辅种完成', startedAt, result.client);
            try { await task.ntf.addTorrent(task._rss, result.client, torrent); } catch (error) { logger.error(task.alias, 'RSS辅种通知失败:', error.name); }
            return records;
          }
          if (result.uncertain) return reject(result.reason, result.client, 3);
        }
      }
    }
    if (task.onlyReseed) return records;

    // Prepare network-dependent inputs before acquiring a short unsent lease.
    // Free/HR scraping and link fallback behavior are intentionally retained.
    if (task.scrapeFree) {
      try {
        if (!await util.scrapeFree(torrent.link, task.cookie)) {
          const scraped = await redis.get(`vertex:scrape:free:${torrent.hash}`);
          if (task.sleepTime && moment().unix() - +task.sleepTime < torrent.pubTime && !scraped) {
            await redis.setWithExpire(`vertex:scrape:free:${torrent.hash}`, '7777', 4 * 3600);
            try { await task.ntf.rejectTorrent(task._rss, explicitClient, torrent, '拒绝原因: 非免费种'); } catch (error) { logger.error(task.alias, 'RSS拒绝通知失败:', error.name); }
            return records;
          }
          return reject('非免费种', explicitClient);
        }
      } catch (error) {
        if (task.ntf.scrapeError) await task.ntf.scrapeError(task._rss, torrent);
        return reject('抓取免费种子失败', explicitClient);
      }
    }
    if (task.scrapeHr) {
      try {
        if (await util.scrapeHr(torrent.link, task.cookie)) return reject('HR', explicitClient);
      } catch (error) {
        if (task.ntf.scrapeError) await task.ntf.scrapeError(task._rss, torrent);
        return reject('抓取 HR 种子失败', explicitClient);
      }
    }
    let filepath;
    let truehash;
    let link = task._normalizeTorrentUrl(torrent.url);
    if (task.pushTorrentFile) {
      try {
        const at = Date.now();
        ({ filepath, hash: truehash } = await task._downloadTorrent(torrent.url, torrent.hash));
        task._logTorrentTiming(torrent, '种子文件下载完成', at);
      } catch (error) {
        logger.warn(task.alias, '下载种子文件失败，使用链接推送:', error.code || error.name);
      }
    }
    if (!filepath && task.useCustomRegex) {
      const match = task.regexStr?.match(/^\/(.*)\/([gimuy]*)$/);
      if (!match) return reject('种子下载链接替换正则表达式无效', explicitClient, 3);
      try { link = link.replace(new RegExp(match[1], match[2]), task.replaceStr); } catch (error) { return reject('种子下载链接替换正则表达式无效', explicitClient, 3); }
    }

    // Preserve highest-priority and random same-priority rule semantics. Rules
    // are evaluated after preparation and the chosen predicate is rechecked at
    // dispatch, so a forced route cannot bypass final-target resource guards.
    const ruleContext = allocator.ruleContext();
    const fits = task.acceptRules.filter(rule => task._fitRule(rule, torrent, ruleContext));
    if (task.acceptRules.length && !fits.length) return reject('不符合所有规则', explicitClient);
    let rule = {};
    if (fits.length) {
      const priority = item => Number.isFinite(Number(item.priority)) ? Number(item.priority) : 0;
      const highest = Math.max(...fits.map(priority));
      const best = fits.filter(item => priority(item) === highest);
      rule = best[Math.floor(Math.random() * best.length)];
    }
    let savePath = rule.savePath || task.savePath;
    if (savePath) savePath = savePath.replace('{RANDOM}', util.uuid.v4().replace(/-/g, ''));
    const category = rule.category || task.category;
    const result = await dispatch(task, torrent, {
      clientIds: explicitClient ? [explicitClient.id] : task.clientArr,
      forcedClientId: rule.client,
      truehash,
      rule: rule.id ? rule : null
    }, (client, admission) => {
      task.addCount++;
      if (filepath) return client.addTorrentByTorrentFile(filepath, truehash, false, task.uploadLimit, task.downloadLimit, savePath, category, task.autoTMM, task.paused, admission);
      return client.addTorrent(link, torrent.hash, false, task.uploadLimit, task.downloadLimit, savePath, category, task.autoTMM, task.paused, admission);
    });
    if (!result.ok) {
      const reasons = result.reasons ? Object.values(result.reasons).join(',') : result.reason;
      return reject(result.reason === 'no-eligible-client' ? '无可用下载器: ' + reasons : result.reason, result.client, result.uncertain ? 3 : 2);
    }
    const client = result.client;
    task._logTorrentTiming(torrent, '添加完成', startedAt, client);
    recordAdd(torrent.hash, category, client, result.addedAt);
    if (truehash && truehash !== torrent.hash) recordAdd(truehash, category, client, result.addedAt);
    try {
      await task.cacheTorrentToClient(client.id, torrent);
      if (truehash && truehash !== torrent.hash) await task.cacheTorrentToClient(client.id, { ...torrent, hash: truehash });
    } catch (error) { logger.error(task.alias, 'RSS添加后缓存失败:', error.name); }
    try { await task.ntf.addTorrent(task._rss, client, torrent); } catch (error) { logger.error(task.alias, 'RSS添加通知失败:', error.name); }
    return records;
  }

  async function batch (torrents) {
    // Bound preparation within one RSS; the allocator additionally enforces a
    // shared per-client add concurrency across all ten RSS queue workers.
    const maxConcurrent = 15;
    const sql = {
      noteRows: 'INSERT INTO torrents (hash, name, size, rss_id, link, record_time, record_type, record_note, client_id) values (?, ?, ?, ?, ?, ?, ?, ?, ?)',
      noteCategoryRows: 'INSERT INTO torrents (hash, name, size, rss_id, link, category, record_time, record_type, record_note, client_id) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
      addRows: 'INSERT INTO torrents (hash, name, size, rss_id, link, category, record_time, add_time, record_type, record_note, client_id) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
    };
    for (let i = 0; i < torrents.length && active(this); i += maxConcurrent) {
      const results = await Promise.all(torrents.slice(i, i + maxConcurrent).map(async torrent => {
        try { return await this._pushTorrent(torrent); } catch (error) {
          logger.error(this.alias, 'RSS种子处理失败:', error.code || error.name);
          return null;
        }
      }));
      for (const key of Object.keys(sql)) {
        const rows = results.flatMap(result => result?.[key] || []);
        if (rows.length) await util.runRecords(sql[key], rows);
      }
    }
    logger.info(this.alias, `RSS任务并行处理完成，共处理 ${torrents.length} 个种子`);
  }

  function canPrepare () {
    allocator.refresh();
    const ids = new Set([...(this.clientArr || []), ...this.acceptRules.map(rule => rule.client).filter(Boolean), ...(this.autoReseed ? this.reseedClients || [] : [])]);
    for (const id of ids) {
      const reseed = !!this.autoReseed && (this.reseedClients || []).includes(id);
      // This is only a source-level preflight before reading actual torrents.
      // Do not require a full 256 MiB chunk when a smaller torrent could fit.
      const metric = allocator.inspect(global.runningClient?.[id], { mode: this.clientSortBy || 'uploadSpeed', rss: this, reseed, size: 1 });
      if (metric.eligible) return true;
    }
    return false;
  }

  return { push, batch, canPrepare };
};
