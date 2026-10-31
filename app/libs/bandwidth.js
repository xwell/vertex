// Only bandwidth capacity fields use bit-rate units. Do not change util.calSize:
// disk sizes, speed limits and legacy KiB/MiB/GiB fields remain byte based.
const DEFAULT_BPS = 125000000;
const multipliers = Object.freeze({
  bps: 1 / 8,
  Kbps: 1000 / 8,
  Mbps: 1000000 / 8,
  Gbps: 1000000000 / 8,
  'B/s': 1,
  KiB: 1024,
  MiB: 1024 * 1024,
  GiB: 1024 * 1024 * 1024
});

function parse (value, unit) {
  if (value !== undefined && value !== null && typeof value !== 'number' && typeof value !== 'string') throw new Error('带宽必须为数值');
  if (value === undefined || value === null || String(value).trim() === '') return DEFAULT_BPS;
  const number = Number(value);
  const multiplier = multipliers[unit];
  if (!Number.isFinite(number) || number <= 0 || multiplier === undefined) {
    throw new Error('带宽必须为正数，单位须为 Mbps/Gbps 或旧版 KiB/MiB/GiB 每秒');
  }
  const result = Math.floor(number * multiplier);
  if (!Number.isSafeInteger(result) || result <= 0) throw new Error('带宽超出有效范围');
  return result;
}

function validate (client) {
  parse(client.uploadBandwidth, client.uploadBandwidthUnit);
  parse(client.downloadBandwidth, client.downloadBandwidthUnit);
}

module.exports = { parse, validate, DEFAULT_BPS };
