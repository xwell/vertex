#!/usr/bin/env python3
"""Read-only qB/Vertex telemetry. Python standard library; no torrent mutation API."""
import argparse
import collections
import concurrent.futures
import datetime as dt
import fcntl
import gzip
import hashlib
import http.cookiejar
import json
import math
import os
from pathlib import Path
import re
import shutil
import signal
import sqlite3
import time
import urllib.error
import urllib.parse
import urllib.request
import zlib

from rss_allocation import RSSStore

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
DEFAULT_CONFIG = ROOT / '.local/telemetry/config.json'
TORRENT_FIELDS = ('name category tags state added_on completion_on size total_size completed progress '
                  'uploaded downloaded upspeed dlspeed amount_left num_seeds num_complete num_leechs '
                  'num_incomplete availability ratio time_active seeding_time dl_limit up_limit').split()
SERVER_FIELDS = ('alltime_ul alltime_dl up_info_speed dl_info_speed free_space_on_disk queued_io_jobs '
                 'read_cache_overload write_cache_overload average_time_queue total_queued_size '
                 'total_peer_connections total_buffers_size up_rate_limit dl_rate_limit connection_status').split()
STOP = False


def dumps(obj):
    return json.dumps(obj, ensure_ascii=False, separators=(',', ':'), allow_nan=False)


def digest(value):
    return hashlib.sha256(value.encode()).hexdigest()


def atomic_json(path, obj):
    path = Path(path)
    temporary = path.with_suffix(path.suffix + '.tmp')
    temporary.write_text(json.dumps(obj, ensure_ascii=False, indent=2, allow_nan=False) + '\n')
    temporary.chmod(0o600)
    temporary.replace(path)


def number(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value) and value >= 0


def clean_torrent(data):
    # Explicit allowlist: no passkeys, cookies, paths, magnets or comments in the database.
    result = {}
    for k, v in data.items():
        if k in TORRENT_FIELDS:
            result[k] = v if v is None or isinstance(v, (str, bool)) or number(v) else None
    if 'tracker' in data:
        try:
            result['tracker'] = urllib.parse.urlsplit(data['tracker']).hostname or ''
        except (ValueError, TypeError):
            result['tracker'] = ''
    return result


def clean_server(data):
    result = {}
    for k, v in data.items():
        if k not in SERVER_FIELDS:
            continue
        if k == 'connection_status':
            result[k] = v if isinstance(v, str) else None
        else:
            try:
                numeric = float(v) if isinstance(v, str) else v
                result[k] = numeric if number(numeric) else None
            except ValueError:
                result[k] = None
    return result


def is_free(t):
    return 'FREE' in re.split(r'[|,;\s]+', str(t.get('category', '')).upper())


def counters_delta(previous, current):
    if all(number(x.get(k)) for x in (previous, current) for k in ('uploaded', 'downloaded')):
        up = current['uploaded'] - previous['uploaded']
        down = current['downloaded'] - previous['downloaded']
        if up >= 0 and down >= 0:
            return up, down
    return None


class StorageHints:
    """Runtime-only exact Vertex name/size/path grouping. Persist counts, never paths/keys."""
    def __init__(self):
        self.identities = {}
        self.hints = {}

    def apply(self, raw_torrents, torrents, removed, full):
        if full:
            self.identities = {}
            self.hints = {}
        for h, patch in raw_torrents.items():
            identity = self.identities.setdefault(h, {})
            for field in ('name', 'size', 'save_path'):
                if field in patch:
                    identity[field] = patch[field]
        for h in removed:
            self.identities.pop(h, None)
        keys = {}
        for h, t in self.identities.items():
            if (isinstance(t.get('name'), str) and number(t.get('size')) and t['size'] > 0 and
                    isinstance(t.get('save_path'), str) and t['save_path']):
                keys[h] = (t['name'], t['size'], t['save_path'])
        counts = collections.Counter(keys.values())
        hints = {}
        for h in self.identities:
            hint = {'known': h in keys, 'sharedPeers': counts[keys[h]] - 1 if h in keys else None}
            hints[h] = hint
            # A removal/rename may change a peer not otherwise present in this delta.
            if full or h in torrents or self.hints.get(h) != hint:
                torrents.setdefault(h, {})['bufferStorage'] = hint
        self.hints = hints


class QB:
    def __init__(self, client_id, path, timeout=5):
        self.client_id, self.path, self.timeout = client_id, Path(path), timeout
        self.opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))
        self.rid = 0
        self.authenticated = False
        self.versions = {}
        self.failures = 0
        self.retry_at = 0
        self.storage_hints = StorageHints()

    def request(self, endpoint, body=None):
        if endpoint not in ('auth/login', 'app/version', 'app/webapiVersion') and not re.fullmatch(r'sync/maindata\?rid=\d+', endpoint):
            raise ValueError('endpoint not allowed')
        req = urllib.request.Request(self.base + '/api/v2/' + endpoint, data=body,
                                     headers={'Referer': self.base + '/'})
        with self.opener.open(req, timeout=self.timeout) as response:
            raw = response.read(32 * 1024 * 1024 + 1)
            if len(raw) > 32 * 1024 * 1024:
                raise ValueError('response too large')
            return response.status, raw

    def poll(self):
        started = time.monotonic()
        try:
            if not self.authenticated:
                c = json.loads(self.path.read_text())
                if c['id'] != self.client_id or c['type'] != 'qBittorrent':
                    raise ValueError('client identity mismatch')
                self.base = c['clientUrl'].rstrip('/')
                status, raw = self.request('auth/login', urllib.parse.urlencode(
                    {k: c[k] for k in ('username', 'password')}).encode())
                if status != 204 and raw.strip() != b'Ok.':
                    raise ValueError('login failed')
                self.authenticated = True
                self.rid = 0
                self.versions = {name: self.request('app/' + name)[1].decode().strip()
                                 for name in ('version', 'webapiVersion')}
            _, raw = self.request('sync/maindata?rid=' + str(self.rid))
            data = json.loads(raw)
            if not isinstance(data, dict) or type(data.get('rid')) is not int or data['rid'] < 0:
                raise ValueError('invalid synchronization response')
            if self.rid == 0 and not data.get('full_update'):
                raise ValueError('missing initial full update')
            if any(k in data and not isinstance(data[k], dict) for k in ('torrents', 'server_state')):
                raise ValueError('invalid synchronization maps')
            if data.get('full_update') and 'server_state' not in data:
                raise ValueError('incomplete full update')
            if not isinstance(data.get('torrents_removed', []), list):
                raise ValueError('invalid removed list')
            torrents = {h: clean_torrent(t) for h, t in data.get('torrents', {}).items()
                        if re.fullmatch(r'[a-fA-F0-9]{40,64}', h) and isinstance(t, dict)}
            if len(torrents) != len(data.get('torrents', {})):
                raise ValueError('invalid torrent data')
            removed = data.get('torrents_removed', [])
            if not all(isinstance(h, str) and re.fullmatch(r'[a-fA-F0-9]{40,64}', h) for h in removed):
                raise ValueError('invalid removed hash')
            self.storage_hints.apply(data.get('torrents', {}), torrents, removed, bool(data.get('full_update')))
            self.rid = data['rid']
            self.failures = 0
            return {'ok': True, 'ts': time.time(), 'ms': (time.monotonic() - started) * 1000,
                    'bytes': len(raw), 'full': bool(data.get('full_update')), 'torrents': torrents,
                    'removed': removed, 'server': clean_server(data.get('server_state', {})),
                    'versions': self.versions}
        except Exception as exc:
            # Never log exception strings: urllib errors may contain credentials or server URLs.
            self.failures += 1
            self.retry_at = time.time() + min(300, 10 * 2 ** min(self.failures - 1, 5))
            self.rid = 0
            if isinstance(exc, urllib.error.HTTPError) and exc.code in (401, 403):
                self.authenticated = False
            if isinstance(exc, ValueError):
                self.authenticated = False
            return {'ok': False, 'ts': time.time(), 'ms': (time.monotonic() - started) * 1000,
                    'error': type(exc).__name__ + (':' + str(exc.code) if isinstance(exc, urllib.error.HTTPError) else '')}


SCHEMA = '''
CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS clients(id TEXT PRIMARY KEY,alias TEXT NOT NULL,versions TEXT);
CREATE TABLE IF NOT EXISTS policies(id TEXT PRIMARY KEY,client_id TEXT,observed_at REAL,build TEXT,
 mode TEXT,profile TEXT,code TEXT,rules TEXT,scheduling TEXT);
CREATE INDEX IF NOT EXISTS policies_time ON policies(client_id,observed_at);
CREATE TABLE IF NOT EXISTS polls(id INTEGER PRIMARY KEY,client_id TEXT,ts REAL,ok INTEGER,ms REAL,
 bytes INTEGER,full_update INTEGER,torrent_count INTEGER,server TEXT,error TEXT,policy_id TEXT);
CREATE INDEX IF NOT EXISTS polls_time ON polls(client_id,ts);
CREATE TABLE IF NOT EXISTS frames(poll_id INTEGER PRIMARY KEY,full_snapshot INTEGER,payload BLOB);
CREATE TABLE IF NOT EXISTS runs(id INTEGER PRIMARY KEY,client_id TEXT,hash TEXT,first_seen REAL,
 last_seen REAL,ended_at REAL,end_reason TEXT,name TEXT,tracker TEXT,added_on REAL);
CREATE INDEX IF NOT EXISTS runs_hash ON runs(client_id,hash,last_seen);
CREATE TABLE IF NOT EXISTS current_torrents(client_id TEXT,hash TEXT,run_id INTEGER,data TEXT,
 last_poll REAL,last_sample REAL,PRIMARY KEY(client_id,hash));
CREATE TABLE IF NOT EXISTS samples(id INTEGER PRIMARY KEY,run_id INTEGER,poll_id INTEGER,ts REAL,
 up REAL,down REAL,upspeed REAL,dlspeed REAL,progress REAL,free INTEGER,data TEXT);
CREATE INDEX IF NOT EXISTS samples_run_time ON samples(run_id,ts);
CREATE INDEX IF NOT EXISTS samples_poll ON samples(poll_id);
CREATE INDEX IF NOT EXISTS samples_time ON samples(ts);
CREATE TABLE IF NOT EXISTS minutes(client_id TEXT,minute INTEGER,good_seconds REAL DEFAULT 0,
 observed_up REAL DEFAULT 0,observed_down REAL DEFAULT 0,paid_down REAL DEFAULT 0,
 unknown_cost_down REAL DEFAULT 0,up_speed_seconds REAL DEFAULT 0,down_speed_seconds REAL DEFAULT 0,
 free_min REAL,low_seconds REAL DEFAULT 0,critical_seconds REAL DEFAULT 0,
 io_high_seconds REAL DEFAULT 0,polls INTEGER DEFAULT 0,failures INTEGER DEFAULT 0,
 PRIMARY KEY(client_id,minute));
CREATE TABLE IF NOT EXISTS events(id TEXT PRIMARY KEY,ts REAL,client_id TEXT,kind TEXT,hash TEXT,
 run_id INTEGER,rule_id TEXT,build TEXT,payload TEXT);
CREATE INDEX IF NOT EXISTS events_time ON events(client_id,ts,kind);
CREATE TABLE IF NOT EXISTS log_offsets(path TEXT PRIMARY KEY,inode INTEGER,offset INTEGER,anchor TEXT);
'''


class Store:
    def __init__(self, path, config):
        self.path, self.config = Path(path), config
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.db = sqlite3.connect(self.path, timeout=15)
        self.db.row_factory = sqlite3.Row
        self.db.execute('PRAGMA auto_vacuum=INCREMENTAL')
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('PRAGMA journal_size_limit=16777216')
        self.db.execute('PRAGMA synchronous=NORMAL')
        self.db.execute('PRAGMA busy_timeout=15000')
        self.db.execute('PRAGMA max_page_count=' + str(config.get('max_db_bytes', 2 * 1024 ** 3) // 4096))
        self.db.executescript(SCHEMA)
        self.rss = RSSStore(self.db, config)
        if 'scheduling' not in {r[1] for r in self.db.execute('PRAGMA table_info(policies)')}:
            self.db.execute("ALTER TABLE policies ADD COLUMN scheduling TEXT DEFAULT '{}'")
        self.path.chmod(0o600)
        self.set_meta_default('started_at', time.time())
        if self.meta('action_identity_version', 1) < 2:
            for row in list(self.db.execute("SELECT * FROM events WHERE kind IN ('action-success','action-failure')")):
                payload = json.loads(row['payload'])
                raw = payload['name']
                if payload.get('rule') and payload['rule'].get('alias'):
                    raw += ' ' + payload['rule']['alias']
                identity = digest(dumps([row['ts'], row['client_id'], row['kind'], raw]))
                if identity != row['id']:
                    self.db.execute('INSERT OR IGNORE INTO events VALUES(?,?,?,?,?,?,?,?,?)',
                                    (identity, *tuple(row)[1:]))
                    self.db.execute('DELETE FROM events WHERE id=?', (row['id'],))
            self.set_meta('action_identity_version', 2)
        self.db.commit()
        self.current = {}
        self.last = {}
        for row in self.db.execute('SELECT * FROM current_torrents'):
            self.current.setdefault(row['client_id'], {})[row['hash']] = {
                'run_id': row['run_id'], 'data': json.loads(row['data']),
                'last_poll': row['last_poll'], 'last_sample': row['last_sample']}
        for row in self.db.execute('SELECT value FROM meta WHERE key LIKE "last:%"'):
            value = json.loads(row['value'])
            self.last[value['client_id']] = value

    def meta(self, key, default=None):
        row = self.db.execute('SELECT value FROM meta WHERE key=?', (key,)).fetchone()
        return json.loads(row[0]) if row else default

    def set_meta(self, key, value):
        self.db.execute('INSERT OR REPLACE INTO meta VALUES(?,?)', (key, dumps(value)))

    def set_meta_default(self, key, value):
        self.db.execute('INSERT OR IGNORE INTO meta VALUES(?,?)', (key, dumps(value)))

    def event(self, ts, cid, kind, payload, hash_=None, run_id=None, rule_id=None, build=None, key=None):
        encoded = dumps(payload)
        identity = key or digest(dumps([ts, cid, kind, hash_, encoded]))
        self.db.execute('INSERT OR IGNORE INTO events VALUES(?,?,?,?,?,?,?,?,?)',
                        (identity, ts, cid, kind, hash_, run_id, rule_id, build, encoded))

    def record(self, target, result):
        cid, ts = target['clientId'], result['ts']
        current = self.current.setdefault(cid, {})
        previous = self.last.get(cid)
        minute = int(ts // 60) * 60
        self.db.execute('INSERT OR IGNORE INTO minutes(client_id,minute) VALUES(?,?)', (cid, minute))
        self.db.execute('INSERT OR REPLACE INTO clients VALUES(?,?,?)',
                        (cid, target['alias'], dumps(result.get('versions', previous.get('versions', {}) if previous else {}))))
        if not result['ok']:
            self.db.execute('INSERT INTO polls(client_id,ts,ok,ms,error) VALUES(?,?,0,?,?)',
                            (cid, ts, result['ms'], result['error']))
            self.db.execute('UPDATE minutes SET failures=failures+1 WHERE client_id=? AND minute=?', (cid, minute))
            self.event(ts, cid, 'poll-failed', {'error': result['error']})
            self.set_meta('last_failure:' + cid, ts)
            return
        server = {} if result['full'] or previous is None else dict(previous['server'])
        server.update(result['server'])
        merged = {} if result['full'] else {h: dict(v['data']) for h, v in current.items()}
        for h, patch in result['torrents'].items():
            merged.setdefault(h, {}).update(patch)
        for h in result['removed']:
            merged.pop(h, None)
        for h in set(current) - set(merged):
            old = current.pop(h)
            self.db.execute('UPDATE runs SET ended_at=?,end_reason=? WHERE id=?', (ts, 'disappeared', old['run_id']))
            self.db.execute('DELETE FROM current_torrents WHERE client_id=? AND hash=?', (cid, h))
            self.event(ts, cid, 'disappeared', {'last_seen': old['last_poll'], 'attribution': 'unknown'}, h, old['run_id'])
        policy_id = self.meta('policy:' + cid)
        poll_id = self.db.execute('INSERT INTO polls(client_id,ts,ok,ms,bytes,full_update,torrent_count,server,policy_id) '
                                  'VALUES(?,?,1,?,?,?,?,?,?)',
                                  (cid, ts, result['ms'], result['bytes'], result['full'], len(merged), dumps(server), policy_id)).lastrowid
        checkpoint = result['full'] or ts - self.meta('frame_checkpoint:' + cid, 0) >= 300
        frame = {'torrents': merged if checkpoint else result['torrents'],
                 'removed': [] if checkpoint else result['removed'], 'server': server}
        self.db.execute('INSERT INTO frames VALUES(?,?,?)', (poll_id, checkpoint, zlib.compress(dumps(frame).encode(), 3)))
        if checkpoint:
            self.set_meta('frame_checkpoint:' + cid, ts)
        up = down = paid = unknown_cost = 0
        interval = ts - previous['ts'] if previous else 0
        continuous = (0 < interval <= self.config['interval_seconds'] * 3 and
                      self.meta('last_failure:' + cid, 0) <= (previous['ts'] if previous else 0))
        for h, t in merged.items():
            old = current.get(h)
            delta = counters_delta(old['data'], t) if old else None
            reset = old and (old['data'].get('added_on') != t.get('added_on') or
                             (delta is None and all(number(x.get(k)) for x in (old['data'], t)
                                                    for k in ('uploaded', 'downloaded'))))
            if reset:
                self.db.execute('UPDATE runs SET ended_at=?,end_reason=? WHERE id=?', (ts, 'counter-or-age-reset', old['run_id']))
                self.event(ts, cid, 'counter-or-age-reset', {}, h, old['run_id'])
                old = None
                delta = None
            if old is None:
                run_id = self.db.execute('INSERT INTO runs(client_id,hash,first_seen,last_seen,name,tracker,added_on) VALUES(?,?,?,?,?,?,?)',
                                         (cid, h, ts, ts, t.get('name'), t.get('tracker'), t.get('added_on'))).lastrowid
                old = {'run_id': run_id, 'data': {}, 'last_poll': ts, 'last_sample': 0}
                self.event(ts, cid, 'first-observed', {'added_on': t.get('added_on')}, h, run_id)
            if delta and continuous:
                up += delta[0]
                down += delta[1]
                if is_free(old['data']) != is_free(t):
                    unknown_cost += delta[1]
                elif not is_free(t):
                    paid += delta[1]
            last_sample = old['last_sample']
            changed = any(old['data'].get(k) != t.get(k) for k in ('state', 'category', 'tags', 'bufferStorage'))
            if (not number(t.get('progress')) or t['progress'] < 1 or changed or
                    ts - last_sample >= self.config['seed_sample_seconds']):
                self.db.execute('INSERT INTO samples(run_id,poll_id,ts,up,down,upspeed,dlspeed,progress,free,data) '
                                'VALUES(?,?,?,?,?,?,?,?,?,?)', (old['run_id'], poll_id, ts, t.get('uploaded'), t.get('downloaded'),
                                 t.get('upspeed'), t.get('dlspeed'), t.get('progress'), is_free(t), dumps(t)))
                last_sample = ts
            self.db.execute('UPDATE runs SET last_seen=?,name=?,tracker=? WHERE id=?',
                            (ts, t.get('name'), t.get('tracker'), old['run_id']))
            self.db.execute('INSERT OR REPLACE INTO current_torrents VALUES(?,?,?,?,?,?)',
                            (cid, h, old['run_id'], dumps(t), ts, last_sample))
            current[h] = {'run_id': old['run_id'], 'data': t, 'last_poll': ts, 'last_sample': last_sample}
        good = interval if continuous else 0
        if previous and not continuous:
            self.event(ts, cid, 'sampling-gap', {'seconds': interval})
        free = server.get('free_space_on_disk')
        disk = target['profile']['disk']
        low = good if number(free) and free < disk['lowGiB'] * 1024 ** 3 else 0
        critical = good if number(free) and free < disk['criticalGiB'] * 1024 ** 3 else 0
        io = good if any(number(server.get(k)) and server[k] > 10 for k in ('read_cache_overload', 'write_cache_overload')) else 0
        self.db.execute('''UPDATE minutes SET good_seconds=good_seconds+?,observed_up=observed_up+?,
 observed_down=observed_down+?,paid_down=paid_down+?,unknown_cost_down=unknown_cost_down+?,
 up_speed_seconds=up_speed_seconds+?,down_speed_seconds=down_speed_seconds+?,
 free_min=CASE WHEN ? IS NULL THEN free_min WHEN free_min IS NULL THEN ? ELSE min(free_min,?) END,
 low_seconds=low_seconds+?,critical_seconds=critical_seconds+?,io_high_seconds=io_high_seconds+?,polls=polls+1
 WHERE client_id=? AND minute=?''', (good, up, down, paid, unknown_cost,
            good * (server.get('up_info_speed') or 0), good * (server.get('dl_info_speed') or 0),
            free, free, free, low, critical, io, cid, minute))
        last = {'client_id': cid, 'ts': ts, 'server': server, 'versions': result['versions']}
        self.last[cid] = last
        self.set_meta('last:' + cid, last)

    def snapshot_policies(self, manifest, data_root):
        for target in manifest['targets']:
            cid = target['clientId']
            client = json.loads((data_root / 'client' / (cid + '.json')).read_text())
            rules = []
            active = None
            for rule_id in client.get('deleteRules', []):
                r = json.loads((data_root / 'rule/delete' / (rule_id + '.json')).read_text())
                meta = {k: r.get(k) for k in ('id', 'alias', 'type', 'priority', 'deleteNum', 'fitTime',
                                              'pause', 'limitSpeed', 'onlyDeleteTorrent')}
                meta['codeSha256'] = digest(r.get('code', ''))
                rules.append(meta)
                if rule_id == target['ruleId']:
                    active = r
            if active is None:
                self.event(time.time(), cid, 'policy-unbound', {'rule_id': target['ruleId']})
                continue
            # Actual deployed code is the replay source; do not silently trust a stale manifest.
            code = active['code']
            build = re.search(r"const BUILD = '([^']+)';", code)
            mode = re.search(r"const MODE = '([^']+)';", code)
            profile_match = re.search(r'const config =\s*', code)
            profile = json.JSONDecoder().raw_decode(code[profile_match.end():])[0] if profile_match else None
            if not build or not mode or profile is None:
                raise ValueError('unrecognized deployed policy')
            scheduling = {'updateCron': client.get('cron'), 'deleteCron': client.get('autoDeleteCron'),
                          'ruleId': target['ruleId']}
            identity = digest(dumps([cid, code, rules, scheduling]))
            if self.meta('policy:' + cid) != identity:
                self.db.execute('INSERT OR IGNORE INTO policies VALUES(?,?,?,?,?,?,?,?,?)',
                                (identity, cid, time.time(), build[1], mode[1], dumps(profile), code, dumps(rules), dumps(scheduling)))
                self.set_meta('policy:' + cid, identity)
                self.event(time.time(), cid, 'policy-observed', {'policy_id': identity,
                    'manifest_matches': digest(code) == target['codeSha256']}, rule_id=target['ruleId'], build=build[1])
            target['profile'] = profile

    def maintenance(self):
        now = time.time()
        cutoff = now - self.config['raw_days'] * 86400
        page_size = self.db.execute('PRAGMA page_size').fetchone()[0]
        used = (self.db.execute('PRAGMA page_count').fetchone()[0] - self.db.execute('PRAGMA freelist_count').fetchone()[0]) * page_size
        low_disk = shutil.disk_usage(self.path.parent).free < self.config['min_free_bytes']
        if used > self.config['max_db_bytes'] * .80 or low_disk:
            oldest = self.db.execute('SELECT min(ts),max(ts) FROM samples').fetchone()
            if oldest[0] is not None:
                cutoff = max(cutoff, oldest[0] + (oldest[1] - oldest[0]) * .25)
            self.event(now, None, 'retention-pressure', {'used_bytes': used, 'low_disk': low_disk, 'raw_cutoff': cutoff})
        # Retain a full frame before the cutoff so the oldest remaining deltas are replayable.
        checkpoints = [row[0] for row in self.db.execute('''SELECT max(p.ts) FROM frames f JOIN polls p ON p.id=f.poll_id
            WHERE f.full_snapshot=1 AND p.ts<=? GROUP BY p.client_id''', (cutoff,)) if row[0] is not None]
        if checkpoints:
            cutoff = min(cutoff, min(checkpoints))
        self.db.execute('DELETE FROM samples WHERE ts<?', (cutoff,))
        self.db.execute('DELETE FROM frames WHERE poll_id IN (SELECT id FROM polls WHERE ts<?)', (cutoff,))
        self.db.execute('DELETE FROM polls WHERE ts<?', (cutoff,))
        self.db.execute("DELETE FROM events WHERE kind='policy' AND ts<?", (cutoff,))
        self.rss.maintenance(cutoff, now - self.config['aggregate_days'] * 86400)
        self.db.execute('DELETE FROM minutes WHERE minute<?', (now - self.config['aggregate_days'] * 86400,))
        self.db.execute('DELETE FROM events WHERE ts<?', (now - self.config['aggregate_days'] * 86400,))
        self.db.execute('DELETE FROM runs WHERE ended_at<? AND id NOT IN (SELECT run_id FROM samples)',
                        (now - self.config['aggregate_days'] * 86400,))
        self.db.commit()
        self.db.execute('PRAGMA wal_checkpoint(PASSIVE)')
        self.db.execute('PRAGMA incremental_vacuum(2048)')
        self.set_meta('maintenance_at', now)
        self.db.commit()


class LogReader:
    def __init__(self, store, targets, directory):
        self.store, self.targets, self.directory = store, targets, Path(directory)

    def recover_tail(self, name, old):
        for rotated in sorted(self.directory.glob(name + '.*'), key=lambda p: p.stat().st_mtime, reverse=True)[:3]:
            if not rotated.is_file():
                continue
            compressed = rotated.name.endswith('.gz')
            if not compressed and rotated.stat().st_ino != old['inode']:
                continue
            try:
                with (gzip.open(rotated, 'rb') if compressed else rotated.open('rb')) as stream:
                    stream.seek(max(0, old['offset'] - 128))
                    anchor = digest(stream.read(old['offset'] - max(0, old['offset'] - 128)).hex())
                    if anchor != old['anchor']:
                        continue
                    for raw in stream:
                        self.parse(raw.decode('utf-8', errors='replace'))
                return True
            except (OSError, EOFError):
                continue
        return False

    def available_returns(self, cid, name, rule_id, ts, action_id=None, lookback=120):
        # A success or failure consumes its exact true event. Legacy actions without this
        # link still consume the same lifecycle's nearby return; repeated names stay safe.
        return list(self.store.db.execute('''SELECT e.id return_id,e.rule_id return_rule_id,r.id,r.hash FROM events e
            JOIN runs r ON r.id=e.run_id WHERE e.client_id=? AND e.kind='policy-returned'
            AND (? IS NULL OR e.rule_id=?) AND e.ts BETWEEN ? AND ? AND r.name=?
            AND NOT EXISTS (SELECT 1 FROM events a WHERE a.client_id=e.client_id
              AND a.kind IN ('action-success','action-failure') AND a.id!=?
              AND (json_extract(a.payload,'$.returnEventId')=e.id OR
                (a.rule_id=e.rule_id AND json_extract(a.payload,'$.returnEventId') IS NULL AND a.run_id=r.id
                  AND a.ts BETWEEN e.ts-5 AND e.ts+120)))''',
            (cid, rule_id, rule_id, ts - lookback, ts + 5, name, action_id or '')))

    def reconcile(self):
        # qB/Vertex often contain different hashes with the same display name. Resolve only
        # when exactly one lifecycle both matches the name and disappears near the action.
        # Several simultaneous identical-name removals remain explicitly unresolved.
        for event in list(self.store.db.execute("""SELECT * FROM events WHERE kind='action-success'
            AND run_id IS NULL AND ts>=?""", (time.time() - self.store.config['raw_days'] * 86400,))):
            payload = json.loads(event['payload'])
            if payload.get('action') != 'delete':
                continue
            returned = self.available_returns(event['client_id'], payload['name'], event['rule_id'], event['ts'], event['id'])
            if len(returned) == 1:
                match = returned[0]
                payload.update(attribution='policy-return-and-name-match', returnEventId=match['return_id'])
                self.store.db.execute('UPDATE events SET hash=?,run_id=?,payload=? WHERE id=?',
                    (match['hash'], match['id'], dumps(payload), event['id']))
                continue
            matches = list(self.store.db.execute('''SELECT id,hash FROM runs WHERE client_id=? AND name=?
                AND first_seen<=? AND last_seen>=? AND end_reason='disappeared' AND ended_at BETWEEN ? AND ?''',
                (event['client_id'], payload['name'], event['ts'] + 30, event['ts'] - 120,
                 event['ts'] - 30, event['ts'] + 120)))
            if len(matches) == 1:
                payload['attribution'] = 'unique-name-and-disappearance-match'
                payload['associationEvidence'] = 'one matching lifecycle disappeared within -30/+120 seconds'
                self.store.db.execute('UPDATE events SET hash=?,run_id=?,payload=? WHERE id=?',
                    (matches[0]['hash'], matches[0]['id'], dumps(payload), event['id']))

    def parse(self, line):
        if self.store.rss.parse(line):
            return
        if 'BUFFER_POLICY_EVENT ' in line:
            try:
                e = json.loads(line.split('BUFFER_POLICY_EVENT ', 1)[1])
            except ValueError:
                return
            if (e.get('clientId') not in {t['clientId'] for t in self.targets} or not number(e.get('time')) or
                    e.get('kind') != 'delete-returned' or e.get('mode') != 'delete' or not isinstance(e.get('eventId'), str)):
                return
            h = e.get('candidate', {}).get('hash')
            if not isinstance(h, str):
                return
            runs = list(self.store.db.execute('''SELECT id FROM runs WHERE client_id=? AND hash=?
                AND first_seen<=? AND last_seen>=?''', (e['clientId'], h, e['time'] + 30, e['time'] - 120)))
            self.store.event(e['time'], e['clientId'], 'policy-returned', e, h, runs[0]['id'] if len(runs) == 1 else None,
                             e.get('ruleId'), e.get('build'), key='policy-returned:' + e['eventId'])
            return
        if 'BUFFER_POLICY ' in line:
            try:
                e = json.loads(line.split('BUFFER_POLICY ', 1)[1])
            except ValueError:
                return
            if e.get('clientId') not in {t['clientId'] for t in self.targets} or not number(e.get('time')):
                return
            # Policy payload contains only our structured telemetry, not arbitrary surrounding log text.
            self.store.event(e['time'], e['clientId'], 'policy', e, build=e.get('build'))
            return
        # app-error 的真实格式可能缺少时间后的 ]；只接受严格 ISO 前缀，不把 ERROR 字样解析进时间。
        found = re.match(r'^\[(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?)'
                         r'(?:\]|\[ERROR[^\]]*\]).*?下载器 (.+?) 删除种子(成功|失败): (.+)$', line.strip())
        if not found:
            return
        try:
            parsed = dt.datetime.fromisoformat(found[1])
            ts = (parsed if parsed.tzinfo else parsed.replace(tzinfo=dt.timezone(dt.timedelta(hours=8)))).timestamp()
        except ValueError:
            return
        target = next((t for t in self.targets if t['alias'] == found[2]), None)
        if target is None:
            return
        cid = target['clientId']
        raw = found[4]
        row = self.store.db.execute('''SELECT policies.rules FROM polls p JOIN policies ON policies.id=p.policy_id
            WHERE p.client_id=? AND p.ok=1 AND p.ts<=? ORDER BY p.ts DESC LIMIT 1''', (cid, ts)).fetchone()
        if row is None:
            row = self.store.db.execute('SELECT rules FROM policies WHERE id=?', (self.store.meta('policy:' + cid),)).fetchone()
        rules = json.loads(row[0]) if row else []
        matched = sorted([r for r in rules if r.get('alias') and raw.endswith(' ' + r['alias'])],
                         key=lambda r: len(r['alias']), reverse=True)
        rule = matched[0] if matched else None
        name = raw[:-(len(rule['alias']) + 1)] if rule else raw
        kind = 'action-success' if found[3] == '成功' else 'action-failure'
        action = ('pause' if rule.get('pause') else 'limit-speed' if rule.get('limitSpeed') else 'delete') if rule else 'unknown'
        candidates = list(self.store.db.execute('SELECT id,hash FROM runs WHERE client_id=? AND name=? AND last_seen>=? AND first_seen<=?',
                                                (cid, name, ts - 300, ts + 30)))
        action_id = digest(dumps([ts, cid, kind, raw]))
        # A request may time out at 120s after reannounce/wait overhead (observed 122.721s).
        # Extend only failure matching, bounded to 180s; success/legacy matching stays 120s.
        # All possible unconsumed returns remain candidates: ambiguity is never resolved by nearest-name guessing.
        returned = self.available_returns(cid, name, rule.get('id') if rule else None, ts, action_id,
                                          lookback=180 if kind == 'action-failure' else 120)
        # 失败行没有规则别名；仅唯一尚未消费的精确 true 可补规则，重名/多批次保持 unresolved。
        if kind == 'action-failure' and rule is None and len(returned) == 1:
            rule = next((r for r in rules if r.get('id') == returned[0]['return_rule_id']), None)
            action = ('pause' if rule.get('pause') else 'limit-speed' if rule.get('limitSpeed') else 'delete') if rule else 'unknown'
        run = returned[0] if len(returned) == 1 else candidates[0] if len(candidates) == 1 else None
        attribution = 'policy-return-and-name-match' if len(returned) == 1 else 'unique-name-match' if run else 'unresolved'
        payload = {'name': name, 'action': action, 'rule': rule, 'attribution': attribution}
        if len(returned) == 1:
            payload['returnEventId'] = returned[0]['return_id']
        self.store.event(ts, cid, kind, payload,
            run['hash'] if run else None, run['id'] if run else None, rule.get('id') if rule else None, key=action_id)

    def read(self):
        for name in ('app-info.log', 'app-error.log', 'app-sc.log'):
            path = self.directory / name
            if not path.exists():
                continue
            stat = path.stat()
            old = self.store.db.execute('SELECT * FROM log_offsets WHERE path=?', (str(path),)).fetchone()
            offset = 0
            if old and old['inode'] == stat.st_ino and old['offset'] <= stat.st_size:
                offset = old['offset']
            elif old:
                # Log4js compresses rotated files. Match the checkpoint anchor before replaying a tail.
                recovered = self.recover_tail(name, old)
                self.store.event(time.time(), None, 'log-rotation', {'file': name, 'old_tail_recovered': recovered})
            else:
                offset = max(0, stat.st_size - 2 * 1024 * 1024)
            with path.open('rb') as stream:
                if offset and old and old['inode'] == stat.st_ino:
                    stream.seek(max(0, offset - 128))
                    if digest(stream.read(offset - max(0, offset - 128)).hex()) != old['anchor']:
                        recovered = self.recover_tail(name, old)
                        self.store.event(time.time(), None, 'log-truncated', {'file': name, 'old_tail_recovered': recovered})
                        offset = 0
                stream.seek(offset)
                if offset and not old:
                    stream.readline()
                # Bound work per cycle so busy application logs cannot starve qB polling.
                for _ in range(20000):
                    start = stream.tell()
                    raw = stream.readline(1024 * 1024)
                    if not raw:
                        break
                    if not raw.endswith(b'\n'):
                        stream.seek(start)
                        break
                    self.parse(raw.decode('utf-8', errors='replace'))
                position = stream.tell()
                stream.seek(max(0, position - 128))
                anchor = digest(stream.read(position - max(0, position - 128)).hex())
            self.store.db.execute('INSERT OR REPLACE INTO log_offsets VALUES(?,?,?,?)', (str(path), stat.st_ino, position, anchor))


def wait_until(deadline, stopped=None):
    """Sample the remaining duration once; a second clock read must not produce negative sleep."""
    stopped = stopped or (lambda: STOP)
    while not stopped():
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return
        time.sleep(min(1, remaining))


def run(config, once=False):
    os.umask(0o077)
    state = Path(config['state_dir'])
    state.mkdir(parents=True, exist_ok=True, mode=0o700)
    lock = (state / 'collector.lock').open('w')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    store = Store(state / 'telemetry.sqlite', config)
    manifest_path = Path(config['manifest'])
    manifest = json.loads(manifest_path.read_text())
    targets = manifest['targets']
    if {t['clientId'] for t in targets} != set(config['client_ids']):
        raise ValueError('target set changed')
    data_root = Path(config['vertex_data'])
    clients = {t['clientId']: QB(t['clientId'], data_root / 'client' / (t['clientId'] + '.json'), config['timeout_seconds']) for t in targets}
    logs = LogReader(store, targets, config['vertex_logs'])
    last_policies = last_maintenance = 0
    process_started_at = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(clients)) as pool:
        while not STOP:
            started = time.monotonic()
            now = time.time()
            if now - last_maintenance >= 300:
                # Reclaim first, including after a restart caused by an exhausted page budget.
                store.maintenance()
                last_maintenance = now
            if now - last_policies >= 60:
                manifest = json.loads(manifest_path.read_text())
                if {t['clientId'] for t in manifest['targets']} != set(config['client_ids']):
                    raise ValueError('target set changed')
                targets = manifest['targets']
                logs.targets = targets
                store.snapshot_policies(manifest, data_root)
                store.rss.names(data_root)
                last_policies = now
            tasks = {t['clientId']: pool.submit(clients[t['clientId']].poll) for t in targets if now >= clients[t['clientId']].retry_at}
            for target in targets:
                if target['clientId'] in tasks:
                    store.record(target, tasks[target['clientId']].result())
            logs.read()
            logs.reconcile()
            store.set_meta('heartbeat', time.time())
            store.db.commit()
            atomic_json(state / 'health.json', {'time': time.time(), 'started_at': store.meta('started_at'),
                'pid': os.getpid(), 'process_started_at': process_started_at,
                'last_success': {cid: x['ts'] for cid, x in store.last.items()},
                'failures': {cid: qb.failures for cid, qb in clients.items()},
                'tracked': {cid: len(v) for cid, v in store.current.items()},
                'database_bytes': store.path.stat().st_size,
                'rss_allocation': store.rss.health(),
                'cycle_seconds': time.monotonic() - started})
            if once:
                break
            remaining = config['interval_seconds'] - (time.monotonic() - started)
            deadline = time.monotonic() + max(0, remaining)
            wait_until(deadline)
    store.db.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, default=DEFAULT_CONFIG)
    parser.add_argument('--once', action='store_true')
    args = parser.parse_args()
    config = json.loads(args.config.read_text())
    if not 5 <= config['interval_seconds'] <= 60 or config['seed_sample_seconds'] < config['interval_seconds']:
        raise ValueError('invalid collection cadence')
    run(config, args.once)


if __name__ == '__main__':
    def stop(signum, frame):
        global STOP
        STOP = True
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    main()
