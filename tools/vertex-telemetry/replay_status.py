#!/usr/bin/env python3
"""Separate replay attempt health from the last successful (possibly old) artifact."""
import argparse
import fcntl
import html
import json
import os
from pathlib import Path
import shutil
import time
import uuid

from collector import DEFAULT_CONFIG, atomic_json

STALE_SECONDS = 2 * 3600


def load(path):
    try:
        value = json.loads(Path(path).read_text())
        return value if isinstance(value, dict) else {}
    except (OSError, ValueError):
        return {}


def status_for_report(directory, now=None):
    now = time.time() if now is None else now
    status = load(Path(directory) / 'replay-status.json')
    if not status:
        old = load(Path(directory) / 'replay-latest.json')
        return {'status': 'unverified-legacy' if old else 'missing', 'checkedAt': now,
                'lastSuccessfulTo': old.get('to'), 'note': '旧产物不代表本轮回放成功；请查看独立回放状态。'}
    status = dict(status)
    status['checkedAt'] = now
    if status['status'] == 'running' and now > status['deadlineAt']:
        status['status'] = 'interrupted'
    if status['status'] == 'success' and now > status['validUntil']:
        status['status'] = 'stale'
    if status.get('lastSuccessfulTo') is not None:
        status['lastSuccessAgeSeconds'] = max(0, now - status['lastSuccessfulTo'])
    return status


def publish(directory, status):
    directory = Path(directory)
    atomic_json(directory / 'replay-status.json', status)
    # No old candidates behind a filename called "latest" after a failed/new attempt.
    if status['status'] != 'success':
        atomic_json(directory / 'replay-latest.json', {**status, 'clients': [],
            'note': '本轮未成功完成；历史成功结果仅见 replay-last-success.json，不可当作本轮结果。'})
    text = html.escape(json.dumps(status, ensure_ascii=False, indent=2))
    expiry = status.get('validUntil') if status['status'] == 'success' else status.get('deadlineAt', 0)
    page = ('<!doctype html><html lang="zh-CN"><meta charset="utf-8"><meta http-equiv="refresh" content="60">'
            '<title>离线回放状态</title><h1>离线回放状态（与采集、报告分开）</h1>'
            '<p id="freshness" data-expiry="' + str(float(expiry or 0)) + '">状态更新时间及有效期见下方 Unix 时间；页面每分钟刷新。</p>'
            '<p>仅 success 且未过期表示近期完整回放成功；running / failed 不能使用旧候选作本轮结论。</p>'
            '<p><a href="latest.html">监测报告</a> · <a href="replay-latest.json">本轮结果/状态</a> · '
            '<a href="replay-last-success.json">历史最后成功结果（检查时间）</a></p><pre>' + text + '</pre>'
            '<script>const e=document.getElementById("freshness");'
            'if(Number(e.dataset.expiry)&&Date.now()/1000>Number(e.dataset.expiry))'
            '{e.textContent="此状态已超过有效期/运行截止时间，请勿当作最新成功结果。";e.style.color="red";}</script></html>')
    tmp = directory / 'replay-status.html.tmp'
    tmp.write_text(page)
    tmp.chmod(0o600)
    tmp.replace(directory / 'replay-status.html')


def begin(directory, start, end, budget_seconds, now=None):
    now = time.time() if now is None else now
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    previous = load(directory / 'replay-status.json')
    last = load(directory / 'replay-last-success.json')
    if not last:
        legacy = load(directory / 'replay-latest.json')
        if legacy.get('clients') and legacy.get('status', 'success') == 'success':
            last = legacy
            atomic_json(directory / 'replay-last-success.json', legacy)
    status = {'status': 'running', 'attemptId': uuid.uuid4().hex,
              'invocationId': os.environ.get('INVOCATION_ID'), 'startedAt': now, 'updatedAt': now,
              'deadlineAt': now + budget_seconds, 'from': start, 'to': end,
              'completedJobs': 0, 'totalJobs': None,
              'lastSuccessfulTo': last.get('to', previous.get('lastSuccessfulTo')),
              'lastSuccessfulPath': previous.get('lastSuccessfulPath')}
    publish(directory, status)
    return status


def progress(directory, status, completed, total, client_id=None, variant=None):
    status.update(updatedAt=time.time(), completedJobs=completed, totalJobs=total,
                  currentJob={'clientId': client_id, 'variant': variant})
    publish(directory, status)


def fail(directory, status, error, now=None):
    status.update(status='failed', updatedAt=time.time() if now is None else now, error=str(error)[:1000])
    publish(directory, status)


def succeed(directory, status, result, name, now=None):
    now = time.time() if now is None else now
    directory = Path(directory)
    # Publish data first; the authoritative health status becomes success last.
    result.update(status='success', generatedAt=now, attemptId=status['attemptId'], validUntil=result['to'] + STALE_SECONDS)
    atomic_json(directory / name, result)
    atomic_json(directory / 'replay-last-success.json', result)
    atomic_json(directory / 'replay-latest.json', result)
    status.update(status='success', updatedAt=now, validUntil=result['validUntil'],
                  lastSuccessfulTo=result['to'], lastSuccessfulPath=name,
                  completedJobs=status['totalJobs'], currentJob=None)
    publish(directory, status)


def cleanup_workspaces(state):
    # Called only while holding the replay lock, or after systemd stopped this invocation.
    for path in Path(state).glob('replay-work-*'):
        if path.is_dir() and not path.is_symlink():
            shutil.rmtree(path)


def finalize(directory, service_result, invocation_id):
    """ExecStopPost also runs after OOM/timeout, when Python cannot publish failure."""
    directory = Path(directory)
    with (directory.parent / 'replay.lock').open('a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return
        status = load(directory / 'replay-status.json')
        if not invocation_id or status.get('invocationId') != invocation_id:
            return  # Do not overwrite an unrelated/manual attempt.
        if status.get('status') == 'running':
            fail(directory, status, 'systemd: ' + service_result + '; replay did not publish a complete result')
        cleanup_workspaces(directory.parent)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['finalize'])
    parser.add_argument('--config', type=Path, default=DEFAULT_CONFIG)
    args = parser.parse_args()
    state = Path(json.loads(args.config.read_text())['state_dir'])
    finalize(state / 'reports', os.environ.get('SERVICE_RESULT', 'unknown'), os.environ.get('INVOCATION_ID'))


if __name__ == '__main__':
    main()
