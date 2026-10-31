// Read-only, bounded allocation telemetry. Logging failure must never affect admission.
const crypto = require('crypto');
const runtimeId = Date.now().toString(36) + '-' + crypto.randomBytes(8).toString('hex');
let sequence = 0;
const MAX_CLIENTS = 256;
const MAX_BYTES = 512 * 1024;
const number = value => value !== null && value !== '' && Number.isFinite(Number(value)) ? Number(value) : null;
const id = value => typeof value === 'string' && /^[\w.:-]{1,160}$/.test(value) ? value : null;
const fingerprint = value => {
  try { return crypto.createHash('sha256').update(JSON.stringify(value)).digest('hex'); } catch (error) { return null; }
};
const reason = value => typeof value === 'string' && /^[a-z][a-z0-9-]{0,79}$/.test(value) ? value : 'unspecified';
const ids = values => Array.from(new Set((Array.isArray(values) ? values : []).map(id).filter(Boolean))).slice(0, MAX_CLIENTS);

function safe (audit, method, ...args) {
  try { return audit?.[method](...args); } catch (error) { /* Observability is fail-open. */ }
}

function snapshot (client, now) {
  try {
    const data = client?.maindata || {};
    const at = number(client?.lastMaindataSuccessTime);
    return {
      online: !!client?.status,
      sampleAtMs: at === null ? null : at * 1000,
      sampleAgeMs: at === null ? null : now - at * 1000,
      requestStartedAtMs: number(client?.lastMaindataRequestStartedAt),
      uploadBps: number(data.uploadSpeed),
      downloadBps: number(data.downloadSpeed),
      avgUploadBps: number(client?.avgUploadSpeed),
      avgDownloadBps: number(client?.avgDownloadSpeed),
      uploadCapacityBps: number(client?.uploadBandwidth),
      downloadCapacityBps: number(client?.downloadBandwidth),
      freeBytes: number(data.freeSpaceOnDisk),
      downloading: number(data.leechingCount),
      minFreeBytes: number(client?.minFreeSpace),
      maxDownloading: number(client?.maxLeechNum),
      maxUploadBps: number(client?.maxUploadSpeed),
      maxDownloadBps: number(client?.maxDownloadSpeed),
      queuedIO: number(data.queuedIO)
    };
  } catch (error) { return {}; }
}

function create (logger, task, torrent, options = {}, now = Date.now) {
  const startedAtMs = now();
  const decisionId = 'rssa-' + runtimeId + '-' + (++sequence);
  const rows = new Map();
  let lease = null;
  let chosen = null;
  let decided = false;
  let finished = false;
  let sentAtMs = null;
  let finalInput = null;
  const originalIds = ids(options.clientIds || task.clientArr);
  const forcedId = id(options.forcedClientId);
  const context = {
    version: 2,
    algorithm: 'headroom-swrr-v1',
    runtimeId,
    decisionId,
    rssId: id(task.id),
    hash: id(torrent.hash) || (torrent.hash ? 'opaque-' + fingerprint(String(torrent.hash)) : null),
    trueHash: id(options.truehash),
    sizeBytes: number(torrent.size),
    publishedAtMs: number(torrent.pubTime) === null ? null : number(torrent.pubTime) * 1000,
    discoveredAtMs: number(torrent._rssDiscoveredAt),
    mode: ['uploadSpeed', 'downloadSpeed', 'freeSpaceOnDisk', 'leechingCount'].includes(task.clientSortBy) ? task.clientSortBy : 'uploadSpeed',
    route: options.reseed ? 'reseed' : forcedId ? 'forced' : 'weighted',
    originalClientIds: originalIds,
    originalClientCount: Array.isArray(options.clientIds || task.clientArr) ? new Set(options.clientIds || task.clientArr).size : 0,
    forcedClientId: forcedId,
    ruleId: id(options.rule?.id),
    rulePriority: number(options.rule?.priority),
    ruleFingerprint: options.rule ? fingerprint(options.rule) : null,
    configFingerprint: fingerprint({ clients: originalIds, mode: task.clientSortBy, rules: task.acceptRules, up: task.maxClientUploadSpeed, down: task.maxClientDownloadSpeed, count: task.maxClientDownloadCount }),
    rssLimits: { maxUploadBps: number(task.maxClientUploadSpeed), maxDownloadBps: number(task.maxClientDownloadSpeed), maxDownloading: number(task.maxClientDownloadCount) }
  };
  function emit (stage, extra) {
    try {
      const event = { ...context, eventId: decisionId + ':' + stage, stage, atMs: now(), lease, clientId: chosen, ...extra };
      let encoded = JSON.stringify(event);
      while (Buffer.byteLength(encoded) > MAX_BYTES && event.candidates?.length > 1) {
        event.snapshotTruncated = true;
        const selected = event.candidates.find(row => row.clientId === chosen);
        event.candidates = [...(selected ? [selected] : []), ...event.candidates.filter(row => row !== selected).slice(0, Math.floor(event.candidates.length / 2))];
        encoded = JSON.stringify(event);
      }
      if (Buffer.byteLength(encoded) <= MAX_BYTES) logger.info('RSS_ALLOCATION_EVENT', encoded);
    } catch (error) { /* Do not throw, retry or change fairness when telemetry fails. */ }
  }
  return {
    candidate (clientId, metric) {
      if (rows.size >= MAX_CLIENTS) return;
      rows.set(clientId, {
        clientId: id(clientId),
        considered: true,
        ...metric.audit,
        eligible: !!metric.eligible,
        reason: metric.eligible ? null : reason(metric.reason),
        weight: number(metric.weight)
      });
    },
    fair (clientId, before, share, after) {
      const row = rows.get(clientId);
      if (row) Object.assign(row, { creditBefore: number(before), normalizedShare: number(share), creditAfter: number(after) });
    },
    debit (clientId, after) {
      const row = rows.get(clientId);
      if (row) row.creditAfter = number(after);
    },
    decision (result, request, clients, limits) {
      if (decided) return;
      decided = true;
      lease = id(result.token?.id);
      chosen = id(result.client?.id);
      const related = Array.from(new Set([...originalIds, forcedId, ...rows.keys()].filter(Boolean)));
      for (const clientId of related.slice(0, MAX_CLIENTS)) {
        if (!rows.has(clientId) && rows.size < MAX_CLIENTS) {
          rows.set(clientId, {
            clientId,
            considered: false,
            eligible: null,
            reason: forcedId && clientId !== forcedId ? 'forced-not-considered' : 'not-evaluated',
            ...snapshot(clients[clientId], now())
          });
        }
      }
      emit('decision', {
        disposition: result.ok ? 'reserved' : 'rejected',
        reason: result.ok ? null : reason(result.reason),
        candidates: Array.from(rows.values()),
        relatedClientCount: related.length,
        snapshotTruncated: related.length > MAX_CLIENTS || context.originalClientCount > originalIds.length,
        allocatorLimits: { ...limits },
        preparationMs: now() - startedAtMs
      });
    },
    preflight (candidates, limits, why = 'no-eligible-client', extra = {}) {
      if (decided) return;
      decided = true;
      emit('decision', {
        disposition: 'preflight-rejected',
        reason: reason(why),
        candidates,
        allocatorLimits: { ...limits },
        snapshotTruncated: false,
        ...extra
      });
    },
    checked (token) {
      finalInput = token.dispatchMetric?.audit || null;
    },
    submitted (token) {
      sentAtMs = now();
      finalInput = token.dispatchMetric?.audit || null;
      emit('submitted', {
        trueHash: id(Array.from(token.hashes).find(hash => hash !== torrent.hash)) || context.trueHash,
        finalInput,
        finalWeight: number(token.dispatchMetric?.weight),
        waitMs: sentAtMs - token.createdAt
      });
    },
    result (outcome, why, error) {
      if (finished) return;
      finished = true;
      emit('result', {
        outcome: reason(outcome),
        reason: why ? reason(why) : null,
        finalInput,
        error: typeof error === 'string' && /^[\w-]{1,64}$/.test(error) ? error : null,
        submitted: sentAtMs !== null,
        fairnessRefunded: sentAtMs === null && context.route === 'weighted',
        elapsedMs: now() - startedAtMs,
        postMs: sentAtMs === null ? null : now() - sentAtMs
      });
    },
    get finished () { return finished; }
  };
}

module.exports = { create, safe, snapshot, number, id, reason };
