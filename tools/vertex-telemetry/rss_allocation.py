"""Bounded RSS audit ingestion. No downloader requests; never persist raw log lines."""
import datetime as dt
import hashlib
import json
import math
import re
import time

SCHEMA = '''
CREATE TABLE IF NOT EXISTS rss_events(
 id TEXT PRIMARY KEY, decision_key TEXT NOT NULL, decision_id TEXT, stage TEXT NOT NULL,
 ts REAL NOT NULL, lease TEXT, rss_id TEXT, hash TEXT, client_id TEXT, route TEXT,
 mode TEXT, outcome TEXT, reason TEXT, quality TEXT NOT NULL, payload_size INTEGER NOT NULL, payload TEXT);
CREATE UNIQUE INDEX IF NOT EXISTS rss_event_stage ON rss_events(decision_key,stage);
CREATE INDEX IF NOT EXISTS rss_event_time ON rss_events(ts);
CREATE INDEX IF NOT EXISTS rss_event_hash ON rss_events(hash,ts);
CREATE INDEX IF NOT EXISTS rss_event_rss ON rss_events(rss_id,ts);
CREATE TABLE IF NOT EXISTS rss_minutes(
 minute INTEGER,rss_id TEXT,client_id TEXT,route TEXT,mode TEXT,stage TEXT,outcome TEXT,
 reason TEXT,quality TEXT,count INTEGER NOT NULL,
 PRIMARY KEY(minute,rss_id,client_id,route,mode,stage,outcome,reason,quality));
CREATE TABLE IF NOT EXISTS rss_names(kind TEXT,id TEXT,alias TEXT,PRIMARY KEY(kind,id));
'''
MAX_BYTES = 512 * 1024
ID = re.compile(r'^[\w.:-]{1,200}$', re.ASCII)
CODE = re.compile(r'^[a-zA-Z][a-zA-Z0-9-]{0,79}$')
MODES = {'uploadSpeed', 'downloadSpeed', 'freeSpaceOnDisk', 'leechingCount'}
ROUTES = {'weighted', 'forced', 'reseed', 'unknown'}
STAGES = {'decision', 'submitted', 'result'}
NUMBERS = set('sampleAtMs sampleAgeMs requestStartedAtMs uploadBps downloadBps avgUploadBps avgDownloadBps uploadCapacityBps downloadCapacityBps freeBytes downloading minFreeBytes maxDownloading maxUploadBps maxDownloadBps queuedIO pendingAdds pendingDownloading pendingBytes apiActive effectiveDownloading usedUploadBps usedDownloadBps sharedReservedBytes sharedApiActive uploadHeadroomBps downloadHeadroomBps usableSpaceBytes reservedBytes writeReserveBytes uploadFreeFraction downloadFreeFraction ioFactor weight creditBefore normalizedShare creditAfter'.split())
ROOT_NUMBERS = set('sizeBytes publishedAtMs discoveredAtMs rulePriority atMs relatedClientCount originalClientCount preparationMs inputAtMs preflightProbeBytes finalWeight waitMs elapsedMs postMs'.split())
ROOT_IDS = set('algorithm runtimeId decisionId eventId rssId hash trueHash forcedClientId ruleId ruleFingerprint configFingerprint lease clientId'.split())
LIMITS = set('snapshotMaxAgeMs maxConcurrentAdds maxPendingAdds maxWaiters waitMs preparedLeaseMs confirmedGraceMs uncertainGraceMs spaceHorizonSeconds spaceChunkBytes maxFairClients'.split())


def numeric(value):
    return value if type(value) in (int, float) and math.isfinite(value) else None


def identifier(value):
    return value if isinstance(value, str) and ID.fullmatch(value) else None


def code(value):
    return value if isinstance(value, str) and CODE.fullmatch(value) else None


def encode(value):
    return json.dumps(value, ensure_ascii=False, separators=(',', ':'), allow_nan=False)


def numeric_map(value, keys):
    return {key: numeric(value[key]) for key in keys if key in value} if isinstance(value, dict) else {}


def candidate(value, shared=False):
    if not isinstance(value, dict) or not identifier(value.get('clientId')):
        raise ValueError('invalid candidate identity')
    result = {'clientId': value['clientId'], **numeric_map(value, NUMBERS)}
    for key in ('online', 'currentRuntime', 'snapshotFresh', 'considered', 'eligible'):
        if key in value:
            result[key] = value[key] if type(value[key]) is bool else None
    if 'reason' in value:
        result['reason'] = code(value['reason'])
    if not shared:
        result['sharedGroupIds'] = [identifier(key) for key in value.get('sharedGroupIds', [])[:256] if identifier(key)]
        result['sharedPeers'] = [candidate(peer, True) for peer in value.get('sharedPeers', [])[:256]]
    return result


def sanitize(value):
    if not isinstance(value, dict) or value.get('version') != 2:
        raise ValueError('unsupported RSS audit version')
    stage = value.get('stage')
    identity = identifier(value.get('decisionId'))
    at = numeric(value.get('atMs'))
    if stage not in STAGES or not identity or not at or not 0 < at < 4102444800000:
        raise ValueError('invalid event envelope')
    if value.get('eventId') != identity + ':' + stage:
        raise ValueError('event identity mismatch')
    if value.get('mode') not in MODES or value.get('route') not in ROUTES:
        raise ValueError('invalid allocation mode/route')
    result = {'version': 2, 'stage': stage, 'mode': value['mode'], 'route': value['route'],
              **{key: identifier(value[key]) for key in ROOT_IDS if key in value},
              **numeric_map(value, ROOT_NUMBERS)}
    for key in ('snapshotTruncated', 'submitted', 'fairnessRefunded'):
        if key in value:
            result[key] = value[key] if type(value[key]) is bool else None
    for key in ('disposition', 'reason', 'outcome', 'error'):
        if key in value:
            result[key] = code(value[key])
    result['originalClientIds'] = [key for key in value.get('originalClientIds', [])[:256] if identifier(key)]
    if 'candidates' in value:
        if not isinstance(value['candidates'], list) or len(value['candidates']) > 256:
            raise ValueError('invalid candidate set')
        result['candidates'] = [candidate(row) for row in value['candidates']]
        if len({row['clientId'] for row in result['candidates']}) != len(result['candidates']):
            raise ValueError('duplicate candidate identity')
    if isinstance(value.get('finalInput'), dict):
        result['finalInput'] = candidate({**value['finalInput'], 'clientId': result.get('clientId')})
    result['allocatorLimits'] = numeric_map(value.get('allocatorLimits'), LIMITS)
    result['rssLimits'] = numeric_map(value.get('rssLimits'), {'maxUploadBps', 'maxDownloadBps', 'maxDownloading'})
    if stage == 'result' and result.get('outcome') not in {'accepted', 'uncertain', 'cancelled', 'rejected', 'filtered', 'interrupted'}:
        raise ValueError('unknown result outcome')
    return result


def timestamp(line):
    found = re.match(r'^\[(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?)', line)
    if not found:
        raise ValueError('missing legacy timestamp')
    value = dt.datetime.fromisoformat(found[1].replace('Z', '+00:00'))
    return (value if value.tzinfo else value.replace(tzinfo=dt.timezone(dt.timedelta(hours=8)))).timestamp()


class RSSStore:
    def __init__(self, db, config):
        self.db, self.config = db, config
        self.db.executescript(SCHEMA)

    def meta(self, key, default=0):
        row = self.db.execute('SELECT value FROM meta WHERE key=?', ('rss:' + key,)).fetchone()
        return json.loads(row[0]) if row else default

    def set_meta(self, key, value):
        self.db.execute('INSERT OR REPLACE INTO meta VALUES(?,?)', ('rss:' + key, encode(value)))

    def _count(self, row, delta):
        key = (int(row['ts'] // 60) * 60, *[row.get(name) or '' for name in ('rss_id', 'client_id', 'route', 'mode', 'stage', 'outcome', 'reason', 'quality')])
        self.db.execute('INSERT OR IGNORE INTO rss_minutes VALUES(?,?,?,?,?,?,?,?,?,0)', key)
        self.db.execute('UPDATE rss_minutes SET count=count+? WHERE minute=? AND rss_id=? AND client_id=? AND route=? AND mode=? AND stage=? AND outcome=? AND reason=? AND quality=?', (delta, *key))
        self.db.execute('DELETE FROM rss_minutes WHERE count<=0 AND minute=? AND rss_id=? AND client_id=? AND route=? AND mode=? AND stage=? AND outcome=? AND reason=? AND quality=?', key)

    def ingest(self, event, quality='v2'):
        ts = event['atMs'] / 1000
        if ts < max(time.time() - self.config.get('aggregate_days', 90) * 86400, self.meta('min_ingest_ts')):
            return False
        decision_key = 'lease:' + event['lease'] if event.get('lease') else 'decision:' + event['decisionId']
        identity = hashlib.sha256(encode([decision_key, event['stage']]).encode()).hexdigest()
        old = self.db.execute('SELECT * FROM rss_events WHERE id=?', (identity,)).fetchone()
        if old and (old['quality'] == 'v2' or quality != 'v2'):
            return False
        row = dict(id=identity, decision_key=decision_key, decision_id=event['decisionId'], stage=event['stage'], ts=ts,
                   lease=event.get('lease'), rss_id=event.get('rssId'), hash=event.get('hash'), client_id=event.get('clientId'),
                   route=event.get('route', 'unknown'), mode=event.get('mode'), outcome=event.get('outcome') or event.get('disposition'),
                   reason=event.get('reason'), quality=quality, payload_size=len(encode(event).encode()), payload=encode(event))
        raw_cutoff = self.meta('raw_cutoff', time.time() - self.config.get('raw_days', 7) * 86400)
        if ts < raw_cutoff:
            row.update(payload=None, payload_size=0)
        if old:
            self._count(dict(old), -1)
        self.db.execute('INSERT OR REPLACE INTO rss_events VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', tuple(row.values()))
        self._count(row, 1)
        self.set_meta('last_event_at', max(ts, self.meta('last_event_at')))
        self.set_meta('ingested_events', self.meta('ingested_events') + (0 if old else 1))
        if quality == 'v2' and event['stage'] == 'decision':
            self.set_meta('last_full_decision_at', max(ts, self.meta('last_full_decision_at')))
        return True

    def parse(self, line):
        marker = 'RSS_ALLOCATION_EVENT '
        legacy = 'RSS分配:' if 'RSS分配:' in line else 'RSS添加结果:' if 'RSS添加结果:' in line else None
        if marker not in line and not legacy:
            return False
        try:
            if len(line.encode()) > MAX_BYTES + 4096:
                raise ValueError('oversized event')
            text = line.split(marker if marker in line else legacy, 1)[1].strip()
            value = json.loads(text, parse_constant=lambda value: (_ for _ in ()).throw(ValueError('invalid number')))
            if marker in line:
                event = sanitize(value)
                self.ingest(event)
            else:
                if not isinstance(value, dict) or not identifier(value.get('lease')) or not identifier(value.get('clientId')):
                    raise ValueError('invalid legacy identity')
                stage = 'decision' if legacy == 'RSS分配:' else 'result'
                event = {'version': 1, 'decisionId': 'legacy-' + value['lease'], 'lease': value['lease'],
                         'atMs': timestamp(line) * 1000, 'clientId': value['clientId'], 'stage': stage,
                         'route': 'reseed' if value.get('reseed') is True else 'forced' if value.get('forced') is True else 'weighted' if stage == 'decision' else 'unknown',
                         'mode': value.get('mode') if value.get('mode') in MODES else None,
                         'disposition': 'submitted' if stage == 'decision' else None,
                         'outcome': value.get('outcome') if value.get('outcome') in {'accepted', 'uncertain'} else None,
                         'selectedInput': numeric_map(value, {'uploadCapacityBps', 'downloadCapacityBps', 'uploadHeadroomBps', 'downloadHeadroomBps', 'usableSpaceBytes', 'reservedBytes', 'effectiveDownloading', 'weight'})}
                if stage == 'result' and event['outcome'] is None:
                    raise ValueError('invalid legacy result')
                self.ingest(event, 'legacy-selected-only')
        except (ValueError, TypeError, KeyError, AttributeError, RecursionError):
            self.set_meta('malformed_events', self.meta('malformed_events') + 1)
        return True

    def names(self, data_root):
        for kind, directory in [('client', 'client'), ('rss', 'rss')]:
            for file in (data_root / directory).glob('*.json'):
                try:
                    value = json.loads(file.read_text())
                    key = identifier(value.get('id'))
                    alias = value.get('alias')
                    if not key or not isinstance(alias, str):
                        continue
                    alias = alias[:120]
                    if re.search(r'https?://|passkey|cookie|password|token=', alias, re.I):
                        alias = '[redacted]'
                    self.db.execute('INSERT OR REPLACE INTO rss_names VALUES(?,?,?)', (kind, key, alias))
                except (OSError, ValueError, TypeError):
                    continue

    def maintenance(self, raw_cutoff, aggregate_cutoff):
        self.db.execute('UPDATE rss_events SET payload=NULL,payload_size=0 WHERE ts<? AND payload_size>0', (raw_cutoff,))
        self.db.execute('DELETE FROM rss_events WHERE ts<?', (aggregate_cutoff,))
        self.db.execute('DELETE FROM rss_minutes WHERE minute<?', (aggregate_cutoff,))
        total = self.db.execute('SELECT coalesce(sum(payload_size),0) FROM rss_events').fetchone()[0]
        budget = self.config.get('rss_max_raw_bytes', 128 * 1024 ** 2)
        if total > budget:
            retained = 0
            for row in self.db.execute('SELECT ts,payload_size FROM rss_events WHERE payload_size>0 ORDER BY ts DESC'):
                retained += row['payload_size']
                if retained > budget:
                    raw_cutoff = max(raw_cutoff, row['ts'] + .000001)
                    break
            self.db.execute('UPDATE rss_events SET payload=NULL,payload_size=0 WHERE ts<? AND payload_size>0', (raw_cutoff,))
        count = self.db.execute('SELECT count(*) FROM rss_events').fetchone()[0]
        maximum = self.config.get('rss_max_header_events', 250000)
        if count > maximum:
            cutoff = self.db.execute('SELECT ts FROM rss_events ORDER BY ts LIMIT 1 OFFSET ?', (count - maximum - 1,)).fetchone()[0]
            self.db.execute('DELETE FROM rss_events WHERE ts<=?', (cutoff,))
            self.set_meta('min_ingest_ts', max(self.meta('min_ingest_ts'), cutoff + .000001))
        self.set_meta('raw_cutoff', raw_cutoff)

    def health(self):
        return {'schemaVersion': 2, **{key: self.meta(key, None if key.endswith('_at') else 0)
                for key in ('last_event_at', 'last_full_decision_at', 'ingested_events', 'malformed_events')}}
