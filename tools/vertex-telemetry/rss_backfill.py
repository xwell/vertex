#!/usr/bin/env python3
"""One-shot RSS-only log backfill. Requires collector stopped; never rewinds its cursor."""
import argparse
import fcntl
import gzip
import json
from pathlib import Path
import time

from collector import DEFAULT_CONFIG, Store, atomic_json
from rss_allocation import timestamp


def backfill(store, directory, since, max_bytes=128 * 1024 ** 2, seconds=60):
    start = time.monotonic()
    read = lines = recognized = 0
    complete = True
    files = []
    before = store.rss.meta('ingested_events')
    paths = []
    for name in ('app-info.log', 'app-error.log'):
        paths.extend(file for file in directory.glob(name + '*') if file.is_file() and file.stat().st_mtime >= since)
    for file in sorted(set(paths), key=lambda path: path.stat().st_mtime)[:64]:
        files.append(file.name)
        opener = gzip.open if file.suffix == '.gz' else open
        with opener(file, 'rb') as source:
            while True:
                raw = source.readline(1024 * 1024)
                if not raw:
                    break
                read += len(raw)
                lines += 1
                if read > max_bytes or time.monotonic() - start > seconds:
                    complete = False
                    break
                if not raw.endswith(b'\n'):
                    continue
                line = raw.decode('utf-8', errors='replace')
                if not any(marker in line for marker in ('RSS_ALLOCATION_EVENT ', 'RSS分配:', 'RSS添加结果:')):
                    continue
                try:
                    if timestamp(line) < since:
                        continue
                except ValueError:
                    continue
                recognized += int(store.rss.parse(line))
                if recognized % 500 == 0:
                    store.db.commit()
        store.db.commit()
        if not complete:
            break
    return {'at': time.time(), 'since': since, 'complete': complete and len(paths) <= 64,
            'files': files, 'bytesRead': read, 'linesRead': lines, 'recognizedLines': recognized,
            'newUniqueEvents': store.rss.meta('ingested_events') - before,
            'seconds': time.monotonic() - start,
            'note': 'Legacy events remain selected-only; no RSS/hash/candidate state is invented. Existing deletion log offsets are not changed.'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, default=DEFAULT_CONFIG)
    parser.add_argument('--since', type=float, required=True, help='Unix seconds; only still-retained matching logs are scanned')
    parser.add_argument('--max-mib', type=int, default=128)
    parser.add_argument('--seconds', type=int, default=60)
    args = parser.parse_args()
    if args.since <= 0 or not 1 <= args.max_mib <= 512 or not 1 <= args.seconds <= 300:
        parser.error('invalid backfill budget')
    config = json.loads(args.config.read_text())
    state = Path(config['state_dir'])
    with (state / 'collector.lock').open('a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise SystemExit('Collector is running; stop it before RSS-only backfill. Do not run a second writer.')
        store = Store(state / 'telemetry.sqlite', config)
        try:
            store.rss.names(Path(config['vertex_data']))
            result = backfill(store, Path(config['vertex_logs']), args.since, args.max_mib * 1024 ** 2, args.seconds)
            store.db.commit()
            atomic_json(state / 'reports/rss-backfill.json', result)
            print(json.dumps(result, ensure_ascii=False))
            if not result['complete']:
                raise SystemExit('Backfill partial: bounded budget reached; no complete-history claim.')
        finally:
            store.db.close()

if __name__ == '__main__':
    main()
