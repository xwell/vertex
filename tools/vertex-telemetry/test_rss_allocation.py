import gzip
import json
from pathlib import Path
import tempfile
import time
import unittest
from unittest import mock

from collector import Store, LogReader
from rss_backfill import backfill
from rss_report import summarize, render
from test_collector import CONFIG, TARGET


def event(stage='decision', suffix='1', **extra):
    value = {'version': 2, 'algorithm': 'headroom-swrr-v1', 'runtimeId': 'runtime',
             'decisionId': 'rssa-runtime-' + suffix, 'eventId': 'rssa-runtime-' + suffix + ':' + stage,
             'stage': stage, 'atMs': time.time() * 1000, 'lease': 'rss-add-' + suffix,
             'rssId': 'rss-test', 'hash': 'a' * 40, 'clientId': 'rbx', 'route': 'weighted', 'mode': 'uploadSpeed',
             'originalClientIds': ['rbx', 'hdd'], 'disposition': 'reserved',
             'candidates': [{'clientId': 'rbx', 'eligible': True, 'considered': True, 'online': True,
                             'snapshotFresh': True, 'normalizedShare': .25, 'weight': 25, 'creditBefore': 0, 'creditAfter': -.75},
                            {'clientId': 'hdd', 'eligible': True, 'considered': True, 'normalizedShare': .75, 'weight': 75}],
             **extra}
    if stage == 'result' and 'outcome' not in value:
        value['outcome'] = 'accepted'
    return value


def line(value):
    return '[2026-10-31T16:00:00.000][INFO] RSS_ALLOCATION_EVENT ' + json.dumps(value) + '\n'


class RSSAuditTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.store = Store(self.root / 'data.sqlite', CONFIG)
        self.reader = LogReader(self.store, [TARGET], self.root)

    def tearDown(self):
        self.store.db.close()
        self.tmp.cleanup()

    def ingest(self, value):
        self.reader.parse(line(value))
        self.store.db.commit()

    def test_all_clients_and_seed_identity_ingested_even_outside_qb_policy_targets(self):
        self.ingest(event())
        row = self.store.db.execute('SELECT * FROM rss_events').fetchone()
        self.assertEqual(row['client_id'], 'rbx')
        self.assertEqual(row['rss_id'], 'rss-test')
        self.assertEqual(row['hash'], 'a' * 40)
        payload = json.loads(row['payload'])
        self.assertEqual(len(payload['candidates']), 2)
        self.assertEqual(payload['candidates'][0]['creditAfter'], -.75)
        self.assertTrue(payload['candidates'][0]['snapshotFresh'])

    def test_out_of_order_results_and_repeated_lines_do_not_duplicate_rollups(self):
        for stage in ['result', 'decision', 'submitted', 'result', 'decision']:
            self.ingest(event(stage))
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM rss_events').fetchone()[0], 3)
        self.assertEqual(self.store.db.execute('SELECT sum(count) FROM rss_minutes').fetchone()[0], 3)
        summary = summarize(self.store.db, time.time() - 60, time.time() + 60)
        self.assertEqual(summary['outcomes']['accepted'], 1)
        self.assertEqual(summary['decisionsWithoutResult'], 0)
        self.assertEqual(summary['fairnessExposure'][0]['submittedSelections'], 1)
        self.assertEqual(summary['fairnessExposure'][0]['expectedSubmittedSelections'], .25)

    def test_log_rotation_and_restart_keep_rss_identity(self):
        log = self.root / 'app-info.log'
        log.write_text(line(event()))
        self.reader.read();self.store.db.commit()
        with log.open('a') as target:
            target.write(line(event('result')))
        with gzip.open(self.root / 'app-info.log.old.gz', 'wb') as target:
            target.write(log.read_bytes())
        log.unlink();log.write_text(line(event('submitted')))
        self.reader.read();self.store.db.commit()
        self.store.db.close();self.store = Store(self.root / 'data.sqlite', CONFIG)
        self.reader = LogReader(self.store, [TARGET], self.root)
        self.reader.read()
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM rss_events').fetchone()[0], 3)
        self.assertEqual(self.store.db.execute('SELECT sum(count) FROM rss_minutes').fetchone()[0], 3)

    def test_legacy_events_are_selected_only_and_v2_upgrades_without_double_counting(self):
        self.reader.parse('[2026-10-31T16:00:00.000] RSS分配: ' + json.dumps({'version': 1, 'lease': 'rss-add-1', 'clientId': 'rbx', 'mode': 'uploadSpeed', 'forced': False, 'uploadCapacityBps': 62500000}))
        old = self.store.db.execute('SELECT * FROM rss_events').fetchone()
        self.assertEqual(old['quality'], 'legacy-selected-only');self.assertIsNone(old['rss_id'])
        self.assertNotIn('candidates', json.loads(old['payload']))
        self.ingest(event())
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM rss_events').fetchone()[0], 1)
        self.assertEqual(self.store.db.execute('SELECT sum(count) FROM rss_minutes').fetchone()[0], 1)
        self.reader.parse('[2026-10-31T16:00:00.000] RSS分配: ' + json.dumps({'lease': 'rss-add-1', 'clientId': 'rbx'}))
        self.assertEqual(self.store.db.execute('SELECT quality FROM rss_events').fetchone()[0], 'v2')

    def test_malformed_unknown_and_secret_fields_never_persist_raw_text(self):
        bad = event();bad['password'] = 'SUPER_SECRET';bad['url'] = 'https://private/passkey=SUPER_SECRET'
        bad['candidates'][0].update(cookie='SUPER_SECRET', savePath='/SUPER_SECRET', torrents=[{'name': 'SUPER_SECRET'}])
        self.ingest(bad)
        self.assertNotIn('SUPER_SECRET', self.store.db.execute('SELECT payload FROM rss_events').fetchone()[0])
        for changes in [{'version': 99}, {'stage': 'weird'}, {'atMs': float('nan')}, {'mode': 'weird'}, {'decisionId': 'https://secret/'}, {'candidates': 'wrong'}, {'eventId': 'wrong'}]:
            self.reader.parse(line(event(**changes)))
        self.reader.parse('RSS_ALLOCATION_EVENT {bad SECRET}')
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM rss_events').fetchone()[0], 1)
        self.assertGreaterEqual(self.store.rss.health()['malformed_events'], 8)
        self.assertNotIn('SECRET', '\n'.join(str(tuple(row)) for row in self.store.db.execute('SELECT * FROM meta')))

    def test_rejected_and_filtered_are_not_counted_as_submitted_fairness_opportunities(self):
        self.ingest(event(suffix='rejected', clientId=None, disposition='rejected', candidates=[{'clientId': 'rbx', 'eligible': False, 'considered': True, 'reason': 'snapshot-stale'}]))
        self.ingest(event('result', 'rejected', clientId=None, outcome='rejected'))
        self.ingest(event(suffix='forced', route='forced', candidates=[{'clientId': 'rbx', 'eligible': True, 'considered': True}, {'clientId': 'hdd', 'eligible': None, 'considered': False, 'reason': 'forced-not-considered'}]))
        self.ingest(event('result', 'forced', route='forced', outcome='accepted'))
        summary = summarize(self.store.db, time.time() - 60, time.time() + 60)
        self.assertEqual(summary['fairnessExposure'], [])
        self.assertEqual(summary['candidateExclusions'][0]['reason'], 'snapshot-stale')
        self.assertEqual(summary['notConsidered'][0]['reason'], 'forced-not-considered')

    def test_raw_retention_preserves_summary_and_dedup_headers(self):
        self.ingest(event(atMs=(time.time() - 10) * 1000))
        self.store.rss.maintenance(time.time(), time.time() - 100)
        row = self.store.db.execute('SELECT * FROM rss_events').fetchone()
        self.assertIsNone(row['payload']);self.assertEqual(row['payload_size'], 0)
        self.assertEqual(self.store.db.execute('SELECT sum(count) FROM rss_minutes').fetchone()[0], 1)
        self.ingest(event())
        self.assertEqual(self.store.db.execute('SELECT sum(count) FROM rss_minutes').fetchone()[0], 1)

    def test_rss_storage_budget_prunes_raw_details_without_discarding_aggregate(self):
        for i in range(5):
            self.ingest(event(suffix=str(i)))
        self.store.rss.config = {**CONFIG, 'rss_max_raw_bytes': 3000, 'rss_max_header_events': 3}
        self.store.rss.maintenance(time.time() - 1000, time.time() - 10000)
        self.assertLessEqual(self.store.db.execute('SELECT coalesce(sum(payload_size),0) FROM rss_events').fetchone()[0], 3000)
        self.assertLessEqual(self.store.db.execute('SELECT count(*) FROM rss_events').fetchone()[0], 3)
        self.assertEqual(self.store.db.execute('SELECT sum(count) FROM rss_minutes').fetchone()[0], 5)

    def test_missing_results_remain_unknown_and_html_escapes_names(self):
        self.ingest(event())
        self.store.db.execute('INSERT INTO rss_names VALUES(?,?,?)', ('client', 'rbx', '<script>bad</script>'))
        summary = summarize(self.store.db, time.time() - 60, time.time() + 60)
        self.assertEqual(summary['decisionsWithoutResult'], 1)
        self.assertEqual(summary['recentDecisions'][0]['result'], 'missing-or-in-flight')
        text = render(summary)
        self.assertNotIn('<script>', text);self.assertIn('&lt;script&gt;', text)

    def test_backfill_is_rss_only_and_does_not_move_main_log_cursor(self):
        log = self.root / 'app-info.log'
        log.write_text('BUFFER_POLICY ' + json.dumps({'clientId': 'test', 'time': time.time(), 'build': 'v12'}) + '\n' + line(event()))
        with mock.patch('rss_backfill.timestamp', return_value=time.time()):
            result = backfill(self.store, self.root, time.time() - 10)
            again = backfill(self.store, self.root, time.time() - 10)
        self.assertTrue(result['complete']);self.assertEqual(result['newUniqueEvents'], 1)
        self.assertEqual(again['newUniqueEvents'], 0)
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM log_offsets').fetchone()[0], 0)
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM events').fetchone()[0], 0)

    def test_v2_result_snapshot_is_allowlisted_and_true_hash_available(self):
        self.ingest(event('submitted', trueHash='b' * 40, finalInput={'usedUploadBps': 123, 'pendingAdds': 2, 'clientUrl': 'SECRET'}))
        payload = json.loads(self.store.db.execute('SELECT payload FROM rss_events').fetchone()[0])
        self.assertEqual(payload['finalInput']['usedUploadBps'], 123)
        self.assertEqual(payload['trueHash'], 'b' * 40)
        self.assertNotIn('SECRET', json.dumps(payload))

if __name__ == '__main__':
    unittest.main()
