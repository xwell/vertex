const bandwidth = require('./bandwidth');

const MiB = 1024 * 1024;
const limits = Object.freeze({
  snapshotMaxAgeMs: 60000,
  maxConcurrentAdds: 2,
  maxPendingAdds: 64,
  maxWaiters: 128,
  waitMs: 30000,
  preparedLeaseMs: 30000,
  confirmedGraceMs: 60000,
  uncertainGraceMs: 180000,
  spaceHorizonSeconds: 30,
  spaceChunkBytes: 256 * MiB,
  maxFairClients: 256
});

const finite = value => Number.isFinite(Number(value)) && value !== null && value !== '';
const nonnegative = value => finite(value) && Number(value) >= 0;
const positiveLimit = value => finite(value) && Number(value) > 0 ? Number(value) : 0;

// Single Vertex process, shared by all RSS queue workers. Acquiring a lease does
// not await: inspect + fair selection + reservation are one JS event-loop turn.
// Submitted requests never expire while their HTTP promise is still running.
class RssAllocator {
  constructor (options = {}) {
    this.now = options.now || Date.now;
    this.clients = options.clients || (() => global.runningClient || {});
    this.limits = { ...limits, ...options.limits };
    this.states = new Map();
    this.fair = new Map();
    this.recentHashes = new Map();
    this.waiters = new Set();
    this.sequence = 0;
  }

  _state (id) {
    if (!this.states.has(id)) this.states.set(id, { tokens: new Map() });
    return this.states.get(id);
  }

  _observed (client, token) {
    return !!client?.maindata?.torrents?.some(torrent => token.hashes.has(torrent.hash));
  }

  _fresh (client, now) {
    const at = Number(client?.lastMaindataSuccessTime) * 1000;
    if (!Number.isFinite(at) || at <= 0 || at > now || now - at > this.limits.snapshotMaxAgeMs) return false;
    const marker = client?.lastMaindataRequestStartedAt;
    // A late response must not turn an old request into a fresh resource sample.
    // Missing markers are tolerated for adapters that only expose success time.
    if (marker === undefined || marker === null || marker === 0) return true;
    const startedAt = Number(marker);
    return Number.isFinite(startedAt) && startedAt > 0 && startedAt <= now && startedAt < at + 1000 && now - startedAt <= this.limits.snapshotMaxAgeMs;
  }

  _groupIds (id) {
    const clients = this.clients();
    const ids = new Set([id]);
    // Treat "same server" as an undirected connected group. An asymmetric
    // declaration must not let a sibling spend the same physical budget twice.
    let changed = true;
    while (changed) {
      changed = false;
      for (const [key, client] of Object.entries(clients)) {
        const peers = client.sameServerClients || [];
        if (!ids.has(key) && !peers.some(peer => ids.has(peer))) continue;
        for (const peer of [key, ...peers]) {
          if (!ids.has(peer)) { ids.add(peer); changed = true; }
        }
      }
    }
    return Array.from(ids);
  }

  _sweep (now) {
    const clients = this.clients();
    for (const [id, state] of this.states) {
      const client = clients[id];
      for (const token of state.tokens.values()) {
        if ((token.status === 'reserved' || token.status === 'ready') && (now - token.createdAt >= this.limits.preparedLeaseMs || client !== token.client)) {
          this._release(token, true);
        } else if (token.status === 'confirmed' || token.status === 'uncertain') {
          // A fresh full maindata contains the task: counts and physical free
          // space now come from qB, not our optimistic reservation.
          if (this._fresh(client, now) && this._observed(client, token)) {
            this._remember(token);
            this._release(token, false);
          } else {
            const grace = token.status === 'confirmed' ? this.limits.confirmedGraceMs : this.limits.uncertainGraceMs;
            // Do not use a request started before the outcome/grace boundary as
            // evidence of absence. Client refresh records its request start.
            const sampleStart = Number(client?.lastMaindataRequestStartedAt);
            if (this._fresh(client, now) && now - token.finishedAt >= grace && Number.isFinite(sampleStart) && sampleStart > token.finishedAt + grace) {
              this._release(token, false);
            }
          }
        }
      }
      if (state.tokens.size === 0) this.states.delete(id);
    }
    for (const [key, value] of this.fair) {
      if (now - value.usedAt > 10 * 60 * 1000 || clients[value.id] !== value.client) this.fair.delete(key);
    }
    while (this.fair.size > this.limits.maxFairClients * 4) this.fair.delete(this.fair.keys().next().value);
    for (const [hash, at] of this.recentHashes) if (now - at > 600000) this.recentHashes.delete(hash);
  }

  _release (token, refund) {
    const state = this.states.get(token.client.id);
    if (!state?.tokens.has(token.id)) return;
    state.tokens.delete(token.id);
    if (refund) {
      for (const change of token.fairChanges || []) {
        if (this.fair.get(change.key) === change.entry) change.entry.credit -= change.delta;
      }
    }
    token.status = 'released';
    this._wake();
  }

  _wake () {
    for (const wake of Array.from(this.waiters)) wake();
  }

  _wait (milliseconds) {
    return new Promise(resolve => {
      const wake = () => {
        clearTimeout(timer);
        this.waiters.delete(wake);
        resolve();
      };
      this.waiters.add(wake);
      const timer = setTimeout(wake, milliseconds);
    });
  }

  _pending (client, own) {
    const result = { count: 0, bytes: 0, concurrent: 0, total: 0, hashes: new Set() };
    for (const token of this.states.get(client.id)?.tokens.values() || []) {
      if (token === own) continue;
      result.total++;
      // Once independently observed, do not double count it even if a different
      // in-flight request is still finishing its bookkeeping.
      if (!this._observed(client, token)) {
        result.count += token.reseed ? 0 : 1;
        result.bytes += token.bytes;
      }
      if (token.status === 'ready' || token.status === 'submitted') result.concurrent++;
      for (const hash of token.hashes) result.hashes.add(hash);
    }
    return result;
  }

  inspect (client, request = {}, own) {
    const now = this.now();
    const reject = reason => ({ eligible: false, reason, client });
    if (!client || this.clients()[client.id] !== client || !client.status || !client.maindata) return reject('client-offline');
    if (!this._fresh(client, now)) return reject('snapshot-stale');
    const data = client.maindata;
    if (!Array.isArray(data.torrents) || !nonnegative(data.uploadSpeed) || !nonnegative(data.downloadSpeed) || !nonnegative(data.leechingCount) || !nonnegative(data.freeSpaceOnDisk)) return reject('snapshot-invalid');
    const pending = this._pending(client, own);
    const rss = request.rss || {};
    const groupIds = this._groupIds(client.id);
    let up = 0;
    let down = 0;
    let physicalReservedBytes = 0;
    let physicalConcurrent = 0;
    for (const id of groupIds) {
      const peer = this.clients()[id];
      if (!peer) continue; // disabled/removed sibling has no active workload here
      if (!peer.status || !peer.maindata || !this._fresh(peer, now) || !nonnegative(peer.maindata.uploadSpeed) || !nonnegative(peer.maindata.downloadSpeed)) return reject('shared-server-stale');
      const groupPending = this._pending(peer, own);
      physicalReservedBytes += groupPending.bytes;
      physicalConcurrent += groupPending.concurrent;
      up += Math.max(Number(peer.maindata.uploadSpeed), nonnegative(peer.avgUploadSpeed) ? Number(peer.avgUploadSpeed) : 0);
      down += Math.max(Number(peer.maindata.downloadSpeed), nonnegative(peer.avgDownloadSpeed) ? Number(peer.avgDownloadSpeed) : 0);
    }
    for (const [value, current, reason] of [
      [client.maxUploadSpeed, up, 'client-upload-limit'],
      [client.maxDownloadSpeed, down, 'client-download-limit'],
      [rss.maxClientUploadSpeed, up, 'rss-upload-limit'],
      [rss.maxClientDownloadSpeed, down, 'rss-download-limit']
    ]) {
      if (positiveLimit(value) && current >= Number(value)) return reject(reason);
    }
    const count = Number(data.leechingCount) + pending.count;
    if (!request.reseed) {
      if (positiveLimit(client.maxLeechNum) && count >= Number(client.maxLeechNum)) return reject('client-download-count');
      if (positiveLimit(rss.maxClientDownloadCount) && count >= Number(rss.maxClientDownloadCount)) return reject('rss-download-count');
    }
    if (pending.total >= this.limits.maxPendingAdds) return reject('pending-limit');
    if (request.hash && pending.hashes.has(request.hash)) return reject('duplicate-inflight');
    const upCapacity = client.uploadBandwidth === undefined ? bandwidth.DEFAULT_BPS : Number(client.uploadBandwidth);
    const downCapacity = client.downloadBandwidth === undefined ? bandwidth.DEFAULT_BPS : Number(client.downloadBandwidth);
    if (!Number.isSafeInteger(upCapacity) || !Number.isSafeInteger(downCapacity) || upCapacity <= 0 || downCapacity <= 0) return reject('bandwidth-invalid');
    const upHeadroom = Math.max(0, upCapacity - up);
    const downHeadroom = Math.max(0, downCapacity - down);
    const size = nonnegative(request.size) && Number(request.size) > 0 ? Number(request.size) : this.limits.spaceChunkBytes;
    const bytes = request.reseed ? 0 : Math.min(size, this.limits.spaceChunkBytes);
    const usableSpace = Number(data.freeSpaceOnDisk) - positiveLimit(client.minFreeSpace) - physicalReservedBytes - down * this.limits.spaceHorizonSeconds;
    if (!request.reseed && usableSpace < bytes) return reject('space-reserve');
    const mode = request.mode || 'uploadSpeed';
    const upFraction = upHeadroom / upCapacity;
    const downFraction = downHeadroom / downCapacity;
    const queued = nonnegative(data.queuedIO) ? Number(data.queuedIO) : 0;
    const ioFactor = 1 / (1 + queued / 100);
    let weight;
    if (mode === 'uploadSpeed') weight = upHeadroom * downFraction * ioFactor;
    else if (mode === 'downloadSpeed') weight = downHeadroom * upFraction * ioFactor;
    else if (mode === 'freeSpaceOnDisk') weight = Math.max(0, usableSpace - bytes) * upFraction * downFraction * ioFactor;
    else if (mode === 'leechingCount') weight = upFraction * downFraction * ioFactor / (count + 1);
    else return reject('mode-invalid');
    if (!request.reseed && (!Number.isFinite(weight) || weight <= 0)) return reject('no-headroom');
    return { eligible: true, client, weight: request.reseed ? 1 : weight, bytes, count, pending, physicalConcurrent, upCapacity, downCapacity, upHeadroom, downHeadroom, usableSpace, mode };
  }

  _tryAcquire (request) {
    const now = this.now();
    this._sweep(now);
    if (request.isActive && !request.isActive()) return { ok: false, reason: 'rss-destroyed' };
    if (request.rss?.skipSameTorrent && request.hash) {
      if (this.recentHashes.has(request.hash)) return { ok: false, reason: 'duplicate-recent' };
      for (const state of this.states.values()) {
        if (Array.from(state.tokens.values()).some(token => token.hashes.has(request.hash))) return { ok: false, reason: 'duplicate-inflight' };
      }
    }
    const ids = Array.from(new Set(request.forcedClientId ? [request.forcedClientId] : request.clientIds || [])).sort();
    const candidates = [];
    const reasons = {};
    for (const id of ids) {
      const metric = this.inspect(this.clients()[id], request);
      if (metric.eligible && !this.states.has(id) && this.states.size >= this.limits.maxFairClients) reasons[id] = 'allocator-state-limit';
      else if (metric.eligible) candidates.push(metric);
      else reasons[id] = metric.reason;
    }
    if (candidates.length === 0) return { ok: false, reason: 'no-eligible-client', reasons, busy: Object.values(reasons).includes('add-busy') };
    let selected;
    let fairKey;
    const fairChanges = [];
    if (request.forcedClientId) selected = candidates[0];
    else {
      // Persistent smooth weighted round robin, normalized per decision. No ceil
      // quota, per-batch reset, alias bonus, capped score or non-transitive sort.
      const total = candidates.reduce((sum, metric) => sum + metric.weight, 0);
      let highest = -Infinity;
      for (const metric of candidates) {
        const key = metric.mode + ':' + metric.client.id;
        let entry = this.fair.get(key);
        if (!entry || entry.client !== metric.client) {
          entry = { id: metric.client.id, client: metric.client, credit: 0, usedAt: now };
          this.fair.set(key, entry);
        }
        const delta = metric.weight / total;
        entry.credit += delta;
        fairChanges.push({ key, entry, delta });
        entry.usedAt = now;
        if (entry.credit > highest) {
          highest = entry.credit;
          selected = metric;
          fairKey = key;
        }
      }
      this.fair.get(fairKey).credit -= 1;
      fairChanges.find(change => change.key === fairKey).delta -= 1;
    }
    const client = selected.client;
    const token = {
      id: 'rss-add-' + now + '-' + (++this.sequence),
      client,
      status: 'reserved',
      createdAt: now,
      bytes: selected.bytes,
      reseed: !!request.reseed,
      hashes: new Set(request.hash ? [request.hash] : []),
      request,
      fairKey,
      fairChanges,
      metric: selected
    };
    this._state(client.id).tokens.set(token.id, token);
    return { ok: true, client, token };
  }

  async acquire (request) {
    return this._tryAcquire(request);
  }

  refresh () {
    this._sweep(this.now());
  }

  async ready (token) {
    const deadline = this.now() + this.limits.waitMs;
    while (true) {
      this._sweep(this.now());
      if (token.status !== 'reserved' || (token.request.isActive && !token.request.isActive())) return { ok: false, reason: 'lease-expired' };
      const metric = this.inspect(token.client, { ...token.request, hash: null }, token);
      if (!metric.eligible) return { ok: false, reason: metric.reason };
      if (metric.physicalConcurrent < this.limits.maxConcurrentAdds) {
        token.status = 'ready';
        return { ok: true };
      }
      if (this.now() >= deadline || this.waiters.size >= this.limits.maxWaiters) return { ok: false, reason: 'add-busy-timeout' };
      await this._wait(Math.min(250, Math.max(1, deadline - this.now())));
    }
  }

  begin (token, hash) {
    if (hash) token.hashes.add(hash);
    this._sweep(this.now());
    if (!['reserved', 'ready'].includes(token.status) || (token.request.isActive && !token.request.isActive())) return { ok: false, reason: 'lease-expired' };
    const pending = this._pending(token.client, token);
    if (Array.from(token.hashes).some(value => pending.hashes.has(value))) return { ok: false, reason: 'duplicate-inflight' };
    if (token.request.rss?.skipSameTorrent) {
      for (const value of token.hashes) {
        if (this.recentHashes.has(value)) return { ok: false, reason: 'duplicate-recent' };
        for (const state of this.states.values()) {
          if (Array.from(state.tokens.values()).some(other => other !== token && other.hashes.has(value))) return { ok: false, reason: 'duplicate-inflight' };
        }
      }
    }
    const metric = this.inspect(token.client, { ...token.request, hash: null }, token);
    if (!metric.eligible) return { ok: false, reason: metric.reason };
    if (metric.physicalConcurrent >= this.limits.maxConcurrentAdds) return { ok: false, reason: 'add-busy' };
    token.status = 'submitted';
    token.sentAt = this.now();
    return { ok: true };
  }

  _remember (token) {
    for (const hash of token.hashes) this.recentHashes.set(hash, this.now());
    while (this.recentHashes.size > 4096) this.recentHashes.delete(this.recentHashes.keys().next().value);
  }

  accepted (token) {
    if (token.status !== 'submitted') return;
    this._remember(token);
    token.status = 'confirmed';
    token.finishedAt = this.now();
    this._wake();
  }

  failed (token) {
    if (token.status === 'submitted') {
      // POST may have succeeded remotely even if its response/connection failed.
      // Quarantine rather than retrying another downloader or reusing its slot.
      token.status = 'uncertain';
      token.finishedAt = this.now();
      this._wake();
    } else if (token.status === 'reserved' || token.status === 'ready') this._release(token, true);
  }

  cancel (token) {
    if (token?.status === 'reserved' || token?.status === 'ready') this._release(token, true);
  }

  destroyClient (client) {
    for (const token of this.states.get(client.id)?.tokens.values() || []) {
      if (token.client === client && (token.status === 'reserved' || token.status === 'ready')) this._release(token, true);
    }
    for (const [key, value] of this.fair) if (value.client === client) this.fair.delete(key);
    this._wake();
  }

  ruleContext (own) {
    this._sweep(this.now());
    const context = Object.create(global);
    context.runningClient = {};
    const clients = this.clients();
    for (const [id, client] of Object.entries(clients)) {
      const view = Object.create(client);
      if (client.maindata) {
        const pending = this._pending(client, own);
        const group = this._groupIds(id);
        const bytes = group.reduce((sum, key) => sum + (clients[key] ? this._pending(clients[key], own).bytes : 0), 0);
        view.maindata = {
          ...client.maindata,
          leechingCount: Number(client.maindata.leechingCount) + pending.count,
          freeSpaceOnDisk: Math.max(0, Number(client.maindata.freeSpaceOnDisk) - bytes)
        };
      }
      context.runningClient[id] = view;
    }
    return context;
  }

  describe (token) {
    const metric = token.metric;
    return {
      version: 1,
      lease: token.id,
      mode: metric.mode,
      clientId: token.client.id,
      forced: !!token.request.forcedClientId,
      reseed: token.reseed,
      uploadCapacityBps: metric.upCapacity,
      downloadCapacityBps: metric.downCapacity,
      uploadHeadroomBps: metric.upHeadroom,
      downloadHeadroomBps: metric.downHeadroom,
      usableSpaceBytes: metric.usableSpace,
      reservedBytes: token.bytes,
      effectiveDownloading: metric.count,
      weight: metric.weight
    };
  }
}

module.exports = new RssAllocator();
module.exports.RssAllocator = RssAllocator;
module.exports.limits = limits;
