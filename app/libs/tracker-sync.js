const { EventEmitter } = require('events');

// Read-only maintenance must yield well before a normal 10s refresh/20s delete cadence.
const limits = Object.freeze({
  requestTimeoutMs: 3000,
  sliceMs: 5000,
  sliceTorrents: 5,
  batchMaxMs: 180000,
  continueDelayMs: 2000,
  snapshotMaxAgeSeconds: 30,
  reserveSeconds: 120
});

function createControl () {
  const signal = new EventEmitter();
  signal.aborted = false;
  signal.abort = reason => {
    if (signal.aborted) return;
    signal.aborted = true;
    signal.reason = reason;
    signal.emit('abort');
  };
  return signal;
}

function blockReason (client, nowMs = Date.now()) {
  if (!client.status || !client.maindata) return 'offline';
  const age = nowMs / 1000 - client.lastMaindataSuccessTime;
  if (!Number.isFinite(age) || !client.lastMaindataSuccessTime || age < 0 || age > limits.snapshotMaxAgeSeconds) return 'snapshot-stale';
  const free = client.maindata.freeSpaceOnDisk;
  if (!Number.isFinite(free) || free < 0) return 'space-unknown';
  const down = Math.max(0, Number(client.maindata.downloadSpeed) || 0);
  const minimum = Math.max(0, Number(client.minFreeSpace) || 0);
  if (free < minimum + down * limits.reserveSeconds) return 'space-reserve';
  return null;
}

module.exports = { limits, createControl, blockReason };
