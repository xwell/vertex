import gzip
import json
from pathlib import Path
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest import mock

from collector import Store, LogReader, QB, clean_server, clean_torrent

HASH = 'a' * 40
TARGET = {'clientId': 'test', 'alias': 'test-qb', 'profile': {'disk': {'lowGiB': 50, 'criticalGiB': 35}}}
CONFIG = {'interval_seconds': 10, 'seed_sample_seconds': 60, 'raw_days': 7, 'aggregate_days': 90,
          'max_db_bytes': 100 * 1024 ** 2, 'min_free_bytes': 0}


def torrent(**kw):
    return {'name': 'movie', 'added_on': 100, 'uploaded': 0, 'downloaded': 0, 'progress': .1,
            'state': 'downloading', 'category': 'FREE|A', 'upspeed': 0, 'dlspeed': 1000, **kw}


def result(ts, torrents=None, full=False, removed=None, **kw):
    return {'ok': True, 'ts': ts, 'ms': 5, 'bytes': 1000, 'full': full,
            'torrents': torrents or {}, 'removed': removed or [], 'versions': {'version': 'test'},
            'server': {'free_space_on_disk': 100 * 1024 ** 3, 'up_info_speed': 10, 'dl_info_speed': 1000}, **kw}


class CollectorTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name)
        self.store = Store(self.path / 'test.sqlite', CONFIG)

    def tearDown(self):
        self.store.db.close()
        self.tmp.cleanup()

    def record(self, r):
        self.store.record(TARGET, r)
        self.store.db.commit()

    def test_incremental_fields_are_merged_and_missing_torrents_remain(self):
        self.record(result(1000, {HASH: torrent()}, True))
        self.record(result(1010, {HASH: {'uploaded': 100, 'downloaded': 1000}}))
        self.record(result(1020))
        current = self.store.current['test'][HASH]
        self.assertEqual(current['data']['name'], 'movie')
        self.assertEqual(current['data']['downloaded'], 1000)
        self.assertEqual(self.store.db.execute('SELECT sum(observed_up) FROM minutes').fetchone()[0], 100)
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM runs').fetchone()[0], 1)

    def test_failure_does_not_remove_torrents_or_invent_zero_speed(self):
        self.record(result(1000, {HASH: torrent()}, True))
        self.record({'ok': False, 'ts': 1010, 'ms': 5000, 'error': 'TimeoutError'})
        self.assertIn(HASH, self.store.current['test'])
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM events WHERE kind="disappeared"').fetchone()[0], 0)
        self.assertIsNone(self.store.db.execute('SELECT server FROM polls WHERE ok=0').fetchone()[0])

    def test_restart_uses_new_full_snapshot_without_double_counting(self):
        self.record(result(1000, {HASH: torrent(uploaded=100)}, True))
        run_id = self.store.current['test'][HASH]['run_id']
        self.store.db.close()
        self.store = Store(self.path / 'test.sqlite', CONFIG)
        self.record(result(1010, {HASH: torrent(uploaded=150, downloaded=30)}, True))
        self.assertEqual(self.store.current['test'][HASH]['run_id'], run_id)
        self.assertEqual(self.store.db.execute('SELECT sum(observed_up) FROM minutes').fetchone()[0], 50)

    def test_disappearance_and_readdition_have_distinct_lifecycles(self):
        self.record(result(1000, {HASH: torrent()}, True))
        original = self.store.current['test'][HASH]['run_id']
        self.record(result(1010, removed=[HASH]))
        self.record(result(1020, {HASH: torrent()}, True))
        self.assertNotEqual(self.store.current['test'][HASH]['run_id'], original)
        self.assertEqual(self.store.db.execute('SELECT end_reason FROM runs WHERE id=?', (original,)).fetchone()[0], 'disappeared')

    def test_counter_reset_does_not_create_giant_or_negative_transfer(self):
        self.record(result(1000, {HASH: torrent(uploaded=10000, downloaded=10000)}, True))
        self.record(result(1010, {HASH: {'uploaded': 5, 'downloaded': 5}}))
        self.assertEqual(self.store.db.execute('SELECT sum(observed_up) FROM minutes').fetchone()[0], 0)
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM runs').fetchone()[0], 2)

    def test_long_gap_preserves_counters_but_is_not_claimed_as_observed_transfer(self):
        self.record(result(1000, {HASH: torrent()}, True))
        self.record(result(1400, {HASH: torrent(uploaded=50000)}, True))
        self.assertEqual(self.store.db.execute('SELECT sum(good_seconds) FROM minutes').fetchone()[0], 0)
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM events WHERE kind="sampling-gap"').fetchone()[0], 1)

    def test_free_change_marks_uncertain_cost_instead_of_fabricating_paid_cost(self):
        self.record(result(1000, {HASH: torrent()}, True))
        self.record(result(1010, {HASH: {'downloaded': 100, 'category': 'PAID'}}))
        row = self.store.db.execute('SELECT sum(paid_down),sum(unknown_cost_down) FROM minutes').fetchone()
        self.assertEqual(tuple(row), (0, 100))

    def test_stable_completed_torrents_are_downsampled_but_rollups_keep_transfer(self):
        self.record(result(1000, {HASH: torrent(progress=1, state='uploading')}, True))
        for i in range(1, 7):
            self.record(result(1000 + i * 10, {HASH: {'uploaded': i * 100}}))
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM samples').fetchone()[0], 2)
        self.assertEqual(self.store.db.execute('SELECT sum(observed_up) FROM minutes').fetchone()[0], 600)

    def test_sanitization_drops_credentials_paths_and_passkeys(self):
        clean = clean_torrent({'name': 'movie', 'tracker': 'https://tracker.example/secret/announce?passkey=abc',
                              'save_path': '/private', 'magnet_uri': 'secret', 'comment': 'secret', 'upspeed': -1})
        self.assertEqual(clean, {'name': 'movie', 'tracker': 'tracker.example', 'upspeed': None})
        self.assertEqual(clean_server({'read_cache_overload': '10.5', 'last_external_address_v4': '1.2.3.4',
                                      'queued_io_jobs': -1}), {'read_cache_overload': 10.5, 'queued_io_jobs': None})

    def test_negative_peer_update_replaces_previous_value_with_unknown(self):
        self.record(result(1000, {HASH: torrent(num_seeds=8)}, True))
        self.record(result(1010, {HASH: clean_torrent({'num_seeds': -1})}))
        self.assertIsNone(self.store.current['test'][HASH]['data']['num_seeds'])

    def test_retention_keeps_checkpoint_needed_by_remaining_deltas(self):
        for i, ts in enumerate([1000, 1010, 1300, 1310, 1600, 1610]):
            self.record(result(ts, {HASH: torrent(uploaded=i * 100)}, full=i == 0))
        with mock.patch('collector.time.time', return_value=CONFIG['raw_days'] * 86400 + 1350):
            self.store.maintenance()
        row = self.store.db.execute('SELECT p.ts,f.full_snapshot FROM polls p JOIN frames f ON f.poll_id=p.id ORDER BY p.ts LIMIT 1').fetchone()
        self.assertEqual(tuple(row), (1300, 1))
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM frames f LEFT JOIN polls p ON p.id=f.poll_id WHERE p.id IS NULL').fetchone()[0], 0)

    def test_retention_prunes_large_policy_logs_but_preserves_action_history(self):
        self.store.event(1000, 'test', 'policy', {'window': {}})
        self.store.event(1000, 'test', 'action-success', {'name': 'movie', 'action': 'delete'})
        self.store.event(2000, 'test', 'policy', {'window': {}})
        with mock.patch('collector.time.time', return_value=CONFIG['raw_days'] * 86400 + 1500):
            self.store.maintenance()
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM events WHERE kind='policy'").fetchone()[0], 1)
        self.assertEqual(self.store.db.execute("SELECT count(*) FROM events WHERE kind='action-success'").fetchone()[0], 1)

    def test_log_restart_deduplicates_and_recovers_compressed_rotation_tail(self):
        directory = self.path / 'logs'
        directory.mkdir()
        log = directory / 'app-info.log'
        def line(ts):
            return 'BUFFER_POLICY ' + json.dumps({'time': ts, 'clientId': 'test', 'build': 'v6'}) + '\n'
        log.write_text(line(1000))
        reader = LogReader(self.store, [TARGET], directory)
        reader.read()
        self.store.db.commit()
        with log.open('a') as stream:
            stream.write(line(1010))
        with gzip.open(directory / 'app-info.log.2026-10-23.gz', 'wb') as stream:
            stream.write(log.read_bytes())
        log.unlink()
        log.write_text(line(1020))
        reader.read()
        reader.read()
        rows = list(self.store.db.execute('SELECT ts FROM events WHERE kind="policy" ORDER BY ts'))
        self.assertEqual([r[0] for r in rows], [1000, 1010, 1020])

    def test_copy_truncate_uses_anchor_and_does_not_skip_new_content(self):
        directory = self.path / 'logs'
        directory.mkdir()
        log = directory / 'app-info.log'
        def line(ts):
            return 'BUFFER_POLICY ' + json.dumps({'time': ts, 'clientId': 'test', 'build': 'v6'}) + '\n'
        log.write_text(line(1000))
        reader = LogReader(self.store, [TARGET], directory)
        reader.read()
        log.write_text(line(1010) + line(1020))
        reader.read()
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM events WHERE kind="policy"').fetchone()[0], 3)

    def test_policy_snapshot_reads_rendered_json_and_preserves_version_changes(self):
        data = self.path / 'vertex'
        (data / 'client').mkdir(parents=True)
        (data / 'rule/delete').mkdir(parents=True)
        (data / 'client/test.json').write_text(json.dumps({'deleteRules': ['r']}))
        target = {**TARGET, 'ruleId': 'r', 'codeSha256': 'not-the-current-hash'}
        code = "const BUILD = 'v6'; const MODE = 'observe'; const config = " + json.dumps(TARGET['profile'], indent=4) + ';'
        rule = {'id': 'r', 'alias': 'pause rule', 'pause': True, 'code': code}
        (data / 'rule/delete/r.json').write_text(json.dumps(rule))
        self.store.snapshot_policies({'targets': [target]}, data)
        self.store.snapshot_policies({'targets': [target]}, data)
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM policies').fetchone()[0], 1)
        rule['code'] = code.replace("'v6'", "'v7'")
        (data / 'rule/delete/r.json').write_text(json.dumps(rule))
        self.store.snapshot_policies({'targets': [target]}, data)
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM policies').fetchone()[0], 2)
        reader = LogReader(self.store, [TARGET], self.path)
        reader.parse('[2026-10-23T19:00:00.000] [INFO] 下载器 test-qb 删除种子成功: movie pause rule')
        row = self.store.db.execute('SELECT payload FROM events WHERE kind="action-success"').fetchone()
        self.assertEqual(json.loads(row[0])['action'], 'pause')
        # Re-reading the same action after a policy update must not change its identity.
        rule['code'] = code.replace("'v6'", "'v8'")
        (data / 'rule/delete/r.json').write_text(json.dumps(rule))
        self.store.snapshot_policies({'targets': [target]}, data)
        reader.parse('[2026-10-23T19:00:00.000] [INFO] 下载器 test-qb 删除种子成功: movie pause rule')
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM events WHERE kind="action-success"').fetchone()[0], 1)

    def test_same_name_actions_resolve_only_unique_nearby_disappearance(self):
        other = 'b' * 40
        self.record(result(1000, {HASH: torrent(), other: torrent()}, True))
        self.store.event(1010, 'test', 'action-success',
                         {'name': 'movie', 'action': 'delete', 'attribution': 'unresolved'}, key='action')
        self.record(result(1020, removed=[HASH]))
        reader = LogReader(self.store, [TARGET], self.path)
        with mock.patch('collector.time.time', return_value=1025):
            reader.reconcile()
        row = self.store.db.execute('SELECT * FROM events WHERE id="action"').fetchone()
        self.assertEqual(row['hash'], HASH)
        self.assertEqual(json.loads(row['payload'])['attribution'], 'unique-name-and-disappearance-match')
        self.store.event(1011, 'test', 'action-success',
                         {'name': 'movie', 'action': 'delete', 'attribution': 'unresolved'}, key='ambiguous')
        self.record(result(1030, removed=[other]))
        with mock.patch('collector.time.time', return_value=1040):
            reader.reconcile()
        self.assertIsNone(self.store.db.execute('SELECT hash FROM events WHERE id="ambiguous"').fetchone()[0])

    def test_qb_http_auth_delta_and_expired_session_recovery_use_only_read_endpoints(self):
        calls = []
        state = {'expire': False, 'rid': 0}
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_POST(self):
                calls.append(('POST', self.path))
                self.rfile.read(int(self.headers.get('Content-Length', 0)))
                self.send_response(204)
                self.send_header('Set-Cookie', 'SID=test; Path=/')
                self.end_headers()

            def do_GET(self):
                calls.append(('GET', self.path))
                if state['expire']:
                    state['expire'] = False
                    self.send_response(403)
                    self.end_headers()
                    return
                self.send_response(200)
                self.end_headers()
                if self.path.startswith('/api/v2/app/'):
                    self.wfile.write(b'5.2.2')
                else:
                    state['rid'] += 1
                    full = self.path.endswith('rid=0')
                    self.wfile.write(json.dumps({'rid': state['rid'], 'full_update': full,
                        'server_state': {}, 'torrents': {HASH: torrent()} if full else {HASH: {'uploaded': 100}}}).encode())
        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            config = self.path / 'client.json'
            config.write_text(json.dumps({'id': 'test', 'type': 'qBittorrent', 'clientUrl':
                'http://127.0.0.1:' + str(server.server_port), 'username': 'test', 'password': 'secret'}))
            qb = QB('test', config)
            self.assertTrue(qb.poll()['full'])
            self.assertFalse(qb.poll()['full'])
            state['expire'] = True
            failure = qb.poll()
            self.assertFalse(failure['ok'])
            self.assertEqual(failure['error'], 'HTTPError:403')
            self.assertNotIn('secret', json.dumps(failure))
            self.assertTrue(qb.poll()['full'])
            self.assertEqual([path for method, path in calls if method == 'POST'], ['/api/v2/auth/login'] * 2)
            with self.assertRaisesRegex(ValueError, 'endpoint not allowed'):
                qb.request('torrents/delete', b'hashes=all')
        finally:
            server.shutdown()
            server.server_close()
            thread.join()


if __name__ == '__main__':
    unittest.main()
