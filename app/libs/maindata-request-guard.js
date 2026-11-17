const { createControl } = require('./tracker-sync');
const diagnostics = require('./maindata-request-diagnostics');
const logger = () => require('./logger');

// Opt-in per downloader. No process-wide timer, credentials, URLs or torrent data in diagnostics.
const states = new WeakMap();
const maximumMs = 30000;
const version = 'maindata-guard-v1';

function enabled (client) {
  return client?._client?.type === 'qBittorrent' && client._client.maindataRequestGuard === true;
}

function stateFor (client) {
  if (!states.has(client)) {
    states.set(client, {
      sequence: 0,
      active: null,
      lastLogAt: 0,
      started: 0,
      succeeded: 0,
      failed: 0,
      cancelled: 0,
      ignored: 0,
      timedOut: 0,
      maxDurationMs: 0,
      lastStartedAt: null,
      lastSuccessAt: null,
      lastFailureAt: null,
      lastErrorCode: null
    });
  }
  return states.get(client);
}

function status (client) {
  const s = states.get(client);
  if (!s) return { version, enabled: enabled(client), started: 0, active: false };
  const { active, lastLogAt, ...counts } = s;
  return {
    version,
    enabled: enabled(client),
    ...counts,
    active: !!active,
    activeSequence: active?.sequence || null,
    activeStartedAt: active?.startedAt || null
  };
}

function publish (client, force = false) {
  const s = stateFor(client);
  if (!force && Date.now() - s.lastLogAt < 60000) return;
  s.lastLogAt = Date.now();
  logger().info('MAINDATA_REQUEST_GUARD ' + JSON.stringify({ clientId: client.id, time: Date.now() / 1000, ...status(client) }));
}

function cancel (client, reason = 'client-destroyed') {
  const token = states.get(client)?.active;
  if (token) token.control.abort(reason);
}

async function execute (client, { timeoutMs = maximumMs, signal, forceLogin = false } = {}) {
  if (!enabled(client)) throw new Error('maindata request guard is not enabled');
  if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) throw new Error('invalid maindata request deadline');
  const s = stateFor(client);
  if (s.active) s.active.control.abort('superseded');
  const startedAt = Date.now();
  const token = { sequence: ++s.sequence, startedAt, deadlineAt: startedAt + Math.min(timeoutMs, maximumMs), control: createControl() };
  s.active = token; s.started++; s.lastStartedAt = startedAt;
  const isOwner = () => s.active === token && global.runningClient?.[client.id] === client && enabled(client);
  const live = () => isOwner() && !token.control.aborted && Date.now() >= startedAt && Date.now() < token.deadlineAt;
  const ignored = () => {
    const error = new Error('obsolete maindata request');
    error.maindataGuardIgnored = true;
    return error;
  };
  const check = () => { if (!live()) throw ignored(); };
  let onAbort;
  const cancelled = new Promise((resolve, reject) => {
    onAbort = () => {
      const error = new Error('maindata request ended');
      error.code = ['deadline', 'task-timeout'].includes(token.control.reason) ? 'ETIMEDOUT' : 'EABORTED';
      reject(error);
    };
    token.control.once('abort', onAbort);
  });
  // Observe an already-cancelled signal without starting HTTP. Race listeners are removed in finally.
  const externalAbort = () => token.control.abort(signal.reason || 'task-ended');
  if (signal) signal.once('abort', externalAbort);
  if (signal?.aborted) externalAbort();
  const timer = setTimeout(() => token.control.abort('deadline'), Math.max(1, token.deadlineAt - Date.now()));
  const request = async operation => {
    check();
    const options = { timeoutMs: Math.max(1, token.deadlineAt - Date.now()), signal: token.control };
    const result = await Promise.race([Promise.resolve().then(() => { check(); return operation(options); }), cancelled]);
    check();
    return result;
  };
  const login = async () => {
    const cookie = await request(options => client.client.login(client.username, client.clientUrl, client.password, options));
    if (typeof cookie !== 'string' || !cookie) throw new Error('invalid qBittorrent login response');
    check();
    client.cookie = cookie;
    client.lastCookie = Math.floor(Date.now() / 1000);
    logger().info('下载器', client.alias, '登录成功');
  };
  try {
    check();
    if (forceLogin || !client.cookie || client.lastCookie < Math.floor(startedAt / 1000) - 3000) await login();
    let data = await request(options => client.client.getMaindata(client.clientUrl, client.cookie, options));
    if (data === 'Unauthorized') {
      await login();
      data = await request(options => client.client.getMaindata(client.clientUrl, client.cookie, options));
    }
    if (!data || typeof data !== 'object' || !Array.isArray(data.torrents)) throw new Error('invalid qBittorrent maindata response');
    // Prepare off to the side: malformed input cannot partially replace a healthy snapshot.
    const prepared = {
      ...data,
      clientId: client.id,
      clientAlias: client.alias,
      torrents: data.torrents.map(t => ({ ...t, trackerStatus: client.trackerStatus[t.hash] || '' })),
      leechingCount: 0,
      seedingCount: 0,
      usedSpace: 0
    };
    for (const t of prepared.torrents) {
      prepared.usedSpace += t.completed;
      if (['downloading', 'stalledDL', 'Downloading'].includes(t.state)) prepared.leechingCount++;
      else if (['uploading', 'stalledUP', 'Seeding'].includes(t.state)) prepared.seedingCount++;
    }
    check();
    // No await between the last generation/identity/deadline check and the complete state commit.
    client.maindata = prepared;
    client.avgDownloadSpeed = data.downloadSpeed * 0.1 + (client.avgDownloadSpeed || 0) * 0.9;
    client.avgUploadSpeed = data.uploadSpeed * 0.1 + (client.avgUploadSpeed || 0) * 0.9;
    client.lastMaindataRequestStartedAt = startedAt;
    client.lastMaindataSuccessTime = Math.floor(Date.now() / 1000);
    client.status = true; client.errorCount = 0;
    s.succeeded++; s.lastSuccessAt = Date.now();
    return prepared;
  } catch (error) {
    const reason = token.control.reason;
    const obsolete = !isOwner() || ['superseded', 'client-destroyed', 'guard-disabled', 'rollback'].includes(reason);
    if (obsolete) {
      s.ignored++;
      return { maindataGuardIgnored: true };
    }
    // Current genuine failures remain fail-closed. Do not let an obsolete callback write this state.
    if (token.control.aborted) s.cancelled++;
    if (Date.now() >= token.deadlineAt || ['deadline', 'task-timeout'].includes(reason)) {
      s.timedOut++; error.code = 'ETIMEDOUT';
    }
    client.status = false; client.maindata = null; client.errorCount = (client.errorCount || 0) + 1;
    s.failed++; s.lastFailureAt = Date.now();
    s.lastErrorCode = ['ETIMEDOUT', 'EABORTED', 'ECONNRESET', 'ECONNREFUSED', 'ENOTFOUND', 'EAI_AGAIN'].includes(error.code) ? error.code : 'REQUEST_FAILED';
    logger().warn('主数据请求保护失败: ' + JSON.stringify({
      clientId: client.id,
      sequence: token.sequence,
      code: s.lastErrorCode,
      durationMs: Date.now() - startedAt
    }));
    const failure = new Error('guarded maindata request failed');
    failure.code = s.lastErrorCode;
    throw failure;
  } finally {
    clearTimeout(timer);
    token.control.removeListener('abort', onAbort);
    if (signal) signal.removeListener('abort', externalAbort);
    // A misbehaving adapter cannot retain an unobserved rejection after this request finishes.
    cancelled.catch(() => {});
    token.control.abort('request-finished');
    if (s.active === token) s.active = null;
    s.maxDurationMs = Math.max(s.maxDurationMs, Date.now() - startedAt);
    publish(client, s.lastFailureAt >= startedAt);
  }
}

module.exports = { version, maximumMs, enabled, execute: diagnostics.wrapExecute(execute, status), cancel, status };
