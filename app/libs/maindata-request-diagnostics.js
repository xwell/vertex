// Opt-in, bounded diagnostics only. No timers, HTTP, queue scheduling or business decisions here.
const logger = () => require('./logger');
const states = new WeakMap();
const { AsyncLocalStorage } = require('async_hooks');
const requests = new AsyncLocalStorage(); // Correlate async stages without URL-keyed/global in-flight guesses.
const boundMs = [100, 500, 1000, 3000, 10000, 30000];
const actions = new Set(['getMaindata', 'autoDelete', 'flashFitTime', 'trackerSync', 'autoReannounce', 'record']);
const finite = x => Number.isFinite(x) && x >= 0;
const ms = x => finite(x) ? Math.min(86400000, Math.round(x)) : null;
const safe = fn => { try { return fn(); } catch (_) { return undefined; } };
const enabled = c => c?._client?.type === 'qBittorrent' && c._client.maindataRequestGuard === true && c._client.maindataRequestDiagnostics === true;
function state (client) {
  if (!states.has(client)) {
    states.set(client, {
      since: Date.now() / 1000,
      lastLogAt: 0,
      started: 0,
      succeeded: 0,
      failed: 0,
      ignored: 0,
      queueBusyDeferrals: 0,
      busyByAction: {},
      queueByAction: {},
      durationHistogram: Array(boundMs.length + 1).fill(0),
      queueWaitMaxMs: 0,
      durationMaxMs: 0,
      lastFailure: null
    });
  }
  return states.get(client);
}
function status (client) {
  if (!enabled(client) && !states.has(client)) return { version: 'maindata-diagnostics-v1', enabled: false };
  return JSON.parse(JSON.stringify({ version: 'maindata-diagnostics-v1', enabled: enabled(client), boundsMs: boundMs, ...state(client) }));
}
function queueData (client, data) {
  if (!enabled(client) || !['getMaindata', 'autoDelete'].includes(data.action)) return data;
  const d = data.maindataDiagnostic;
  const origin = finite(d?.enqueuedAt) && d.enqueuedAt <= Date.now() ? d.enqueuedAt : Date.now();
  return {
    ...data,
    maindataDiagnostic: {
      enqueuedAt: origin,
      busyRetries: Math.min(65535, finite(d?.busyRetries) ? d.busyRetries : 0),
      lockAgeMaxMs: ms(d?.lockAgeMaxMs) || 0,
      lastBusyAction: actions.has(d?.lastBusyAction) ? d.lastBusyAction : null
    }
  };
}
function busy (client, task, lock) {
  if (!enabled(client) || !['getMaindata', 'autoDelete'].includes(task.data.action)) return;
  const hadOrigin = finite(task.data.maindataDiagnostic?.enqueuedAt);
  task.data = queueData(client, task.data);
  if (!hadOrigin && finite(task.timestamp) && task.timestamp <= Date.now()) task.data.maindataDiagnostic.enqueuedAt = task.timestamp;
  const d = task.data.maindataDiagnostic; const s = state(client); const action = actions.has(lock.action) ? lock.action : 'other';
  d.busyRetries = Math.min(65535, d.busyRetries + 1); d.lastBusyAction = action;
  d.lockAgeMaxMs = Math.max(d.lockAgeMaxMs, ms(Date.now() - lock.startTime) || 0);
  s.queueBusyDeferrals++; s.busyByAction[action] = Math.min(Number.MAX_SAFE_INTEGER, (s.busyByAction[action] || 0) + 1);
}
function queueInfo (client, task) {
  if (!enabled(client) || !['getMaindata', 'autoDelete'].includes(task.data.action)) return undefined;
  const d = task.data.maindataDiagnostic;
  const origin = finite(d?.enqueuedAt) ? d.enqueuedAt : task.timestamp;
  const result = {
    totalWaitMs: ms(Date.now() - origin),
    lastQueueWaitMs: ms(Date.now() - task.timestamp),
    busyRetries: Math.min(65535, finite(d?.busyRetries) ? d.busyRetries : 0),
    lockAgeMaxMs: ms(d?.lockAgeMaxMs) || 0,
    lastBusyAction: actions.has(d?.lastBusyAction) ? d.lastBusyAction : null,
    originRecorded: !!d
  };
  const s = state(client);
  s.queueWaitMaxMs = Math.max(s.queueWaitMaxMs, result.totalWaitMs || 0);
  const stats = s.queueByAction[task.data.action] || { starts: 0, maxWaitMs: 0, lastWaitMs: null, maxRunMs: 0 };
  stats.starts++; stats.lastWaitMs = result.totalWaitMs; stats.maxWaitMs = Math.max(stats.maxWaitMs, result.totalWaitMs || 0);
  s.queueByAction[task.data.action] = stats;
  return result;
}
function queueFinished (client, action, startedAt) {
  if (!enabled(client) || !['getMaindata', 'autoDelete'].includes(action)) return;
  const stats = state(client).queueByAction[action];
  if (stats) stats.maxRunMs = Math.max(stats.maxRunMs, ms(Date.now() - startedAt) || 0);
}
function cleanQueue (d) {
  if (!d || typeof d !== 'object') return null;
  return {
    totalWaitMs: ms(d.totalWaitMs),
    lastQueueWaitMs: ms(d.lastQueueWaitMs),
    busyRetries: Math.min(65535, finite(d.busyRetries) ? Math.floor(d.busyRetries) : 0),
    lockAgeMaxMs: ms(d.lockAgeMaxMs) || 0,
    lastBusyAction: actions.has(d.lastBusyAction) ? d.lastBusyAction : null,
    originRecorded: d.originRecorded === true
  };
}
function requestOptions (url, deadline, kind) {
  if (!deadline || !['login', 'main-get'].includes(kind)) return deadline;
  const trace = requests.getStore();
  if (!trace || trace.closed || !enabled(trace.client) || url !== trace.client.clientUrl || trace.stages.length >= 4) return deadline;
  const count = trace.stages.filter(s => s.kind === kind).length;
  const stage = {
    kind,
    attempt: count + 1,
    startedAfterMs: ms(Date.now() - trace.startedAt),
    budgetMs: ms(deadline.timeoutMs),
    socketMs: null,
    connectMs: null,
    tlsMs: null,
    headersMs: null,
    endedMs: null,
    bytes: 0,
    statusCode: null,
    reusedSocket: null,
    ended: false,
    phase: 'waiting-socket',
    outcome: null
  };
  trace.stages.push(stage); const started = Date.now();
  const observe = event => safe(() => {
    if (trace.closed || stage.ended || !event || typeof event !== 'object') return;
    const elapsed = ms(Date.now() - started);
    if (event.type === 'socket') { stage.socketMs = elapsed; stage.reusedSocket = event.reused === true; stage.phase = event.reused ? 'waiting-headers' : 'connecting'; } else if (event.type === 'connect') { stage.connectMs = elapsed; stage.phase = 'waiting-headers'; } else if (event.type === 'tls') { stage.tlsMs = elapsed; stage.phase = 'waiting-headers'; } else if (event.type === 'headers') { stage.headersMs = elapsed; stage.phase = 'reading-body'; stage.statusCode = Number.isInteger(event.statusCode) ? event.statusCode : null; } else if (event.type === 'bytes') stage.bytes = Math.min(Number.MAX_SAFE_INTEGER, stage.bytes + (finite(event.bytes) ? event.bytes : 0));
    else if (event.type === 'end') { stage.endedMs = elapsed; stage.ended = true; stage.outcome = event.outcome === 'success' ? 'success' : 'failure'; }
  });
  return { ...deadline, observe };
}
function wrapExecute (execute, guardStatus) {
  return async function diagnosedExecute (client, options) {
    if (!enabled(client)) return await execute(client, options);
    const startedAt = Date.now(); const s = state(client);
    const trace = { client, startedAt, stages: [], closed: false };
    // Existing guard still owns supersession. Every async operation retains its own closed trace.
    s.started++;
    return await requests.run(trace, async () => {
      let outcome = 'failure';
      try {
        const pending = execute(client, options);
        trace.sequence = safe(() => guardStatus(client).sequence);
        const result = await pending;
        outcome = result?.maindataGuardIgnored ? 'ignored' : 'success';
        return result;
      } finally {
        trace.closed = true;
        safe(() => {
          const elapsedMs = ms(Date.now() - startedAt); const gs = guardStatus(client);
          s[outcome === 'success' ? 'succeeded' : outcome === 'ignored' ? 'ignored' : 'failed']++;
          s.durationHistogram[boundMs.findIndex(b => elapsedMs <= b) < 0 ? boundMs.length : boundMs.findIndex(b => elapsedMs <= b)]++;
          s.durationMaxMs = Math.max(s.durationMaxMs, elapsedMs || 0);
          const detail = {
            version: 'maindata-diagnostics-v1',
            clientId: client.id,
            time: Date.now() / 1000,
            sequence: trace.sequence ?? gs.sequence,
            outcome,
            durationMs: elapsedMs,
            budgetMs: Math.min(30000, options?.timeoutMs || 30000),
            code: outcome === 'failure' ? gs.lastErrorCode : null,
            queue: cleanQueue(options?.diagnosticQueue),
            stages: trace.stages.map(x => ({ ...x })),
            currentStage: trace.stages.length ? trace.stages[trace.stages.length - 1].kind : 'not-started'
          };
          if (outcome === 'failure') s.lastFailure = detail;
          if (outcome === 'failure' || Date.now() - s.lastLogAt >= 60000) {
            s.lastLogAt = Date.now();
            logger().info('MAINDATA_REQUEST_DIAGNOSTIC ' + JSON.stringify({ ...detail, totals: status(client) }));
          }
        });
      }
    });
  };
}
module.exports = { enabled, status, queueData, busy, queueInfo, queueFinished, requestOptions, wrapExecute };
