#!/usr/bin/env python3
"""Generate bounded, read-only telemetry reports; never modify deletion rules."""
import argparse
import collections
import datetime as dt
import html
import itertools
import json
import math
from pathlib import Path
import sqlite3
import statistics
import time

from collector import DEFAULT_CONFIG, atomic_json, number
from timing import aggregate_function_timing
from planning import aggregate_planning
from replay_status import status_for_report
from rss_report import summarize as summarize_rss, render as render_rss

GiB = 1024 ** 3
MiB = 1024 ** 2


def counter_total(polls, field):
    total = 0
    resets = 0
    previous = None
    for p in polls:
        current = p['server'].get(field)
        if not number(current):
            previous = None
            continue
        if previous is not None:
            if current >= previous:
                total += current - previous
            else:
                resets += 1
        previous = current
    return total, resets


def raw_low_return(row, profiles):
    t, server = row['data'], row['server']
    profile = profiles.get(row['policy_id'])
    if not profile or not number(t.get('progress')) or t['progress'] >= 1:
        return None
    if t.get('state') not in ('downloading', 'forcedDL', 'stalledDL'):
        return None
    bc = profile['blackCar']
    down, up = t.get('dlspeed'), t.get('upspeed')
    if not number(down) or not number(up) or down <= 0:
        return None
    share = down / server['dl_info_speed'] if number(server.get('dl_info_speed')) and server['dl_info_speed'] else 0
    capacity_share = down / (profile['downloadMbps'] * 1000000 / 8)
    heavy = down >= bc['minDownloadMiB'] * MiB and (capacity_share >= bc['minCapacityShare'] or
             (down >= bc['dominantDownloadMiB'] * MiB and share >= bc['minDownloadShare']))
    connected, swarm = t.get('num_seeds'), t.get('num_complete')
    competition = (number(connected) and connected >= bc['minConnectedSeeders']) or (number(swarm) and swarm >= bc['minSwarmSeeders'])
    if not heavy or not competition:
        return None
    tiers = bc.get('baseRatios', [{'minDownloadMiB': 0, 'ratio': bc.get('maxUploadDownloadRatio', .03)}])
    limit = next(x['ratio'] for x in tiers if down >= x['minDownloadMiB'] * MiB)
    return limit if up / down < limit else None


def recovery_episode(rows, anchor_index, limit, end):
    anchor = rows[anchor_index]
    t = anchor['data']
    result = {'clientId': anchor['client_id'], 'hash': anchor['hash'], 'runId': anchor['run_id'],
              'name': anchor['name'], 'tracker': t.get('tracker'), 'category': t.get('category'),
              'anchor': anchor['ts'], 'initialDownloadMiB': t.get('dlspeed', 0) / MiB,
              'ratioLimit': limit, 'outcomes': {}}
    for minutes in (5, 15, 60):
        target = anchor['ts'] + minutes * 60
        followup = next((r for r in rows[anchor_index + 1:] if r['ts'] >= target), None)
        outcome = {'status': 'pending' if target > end else 'censored'}
        if followup and followup['ts'] - target <= 90 and number(t.get('uploaded')) and number(followup['data'].get('uploaded')):
            delta = followup['data']['uploaded'] - t['uploaded']
            # A large unobserved gap cannot be classified as a measured recovery.
            between = [r for r in rows[anchor_index:] if r['ts'] <= followup['ts']]
            has_gap = any(b['ts'] - a['ts'] > 90 for a, b in zip(between, between[1:]))
            if delta >= 0 and not has_gap:
                average = delta / (followup['ts'] - anchor['ts'])
                outcome = {'status': 'observed', 'uploadGiB': delta / GiB, 'averageUploadMiB': average / MiB,
                           'recovered': average >= t['dlspeed'] * limit}
        result['outcomes'][str(minutes)] = outcome
    return result


def recoveries(db, start, end, profiles):
    query = '''SELECT s.*,r.client_id,r.hash,r.name,p.server,p.policy_id FROM samples s
      JOIN runs r ON r.id=s.run_id JOIN polls p ON p.id=s.poll_id
      WHERE s.ts BETWEEN ? AND ? ORDER BY s.run_id,s.ts'''
    episodes = []
    for _, group in itertools.groupby(db.execute(query, (start, end)), key=lambda r: r['run_id']):
        rows = []
        for row in group:
            r = dict(row)
            r['data'], r['server'] = json.loads(r['data']), json.loads(r['server'])
            rows.append(r)
        beginning = None
        anchored = False
        for i, row in enumerate(rows):
            limit = raw_low_return(row, profiles)
            gap = i > 0 and (row['ts'] - rows[i - 1]['ts'] > 30 or row['policy_id'] != rows[i - 1]['policy_id'])
            if limit is None or gap:
                beginning, anchored = None, False
            if limit is not None:
                if beginning is None:
                    beginning = row['ts']
                if not anchored and row['ts'] - beginning >= 60:
                    episodes.append(recovery_episode(rows, i, limit, end))
                    anchored = True
    result = {}
    for cid in {e['clientId'] for e in episodes}:
        client = [e for e in episodes if e['clientId'] == cid]
        horizons = {}
        for horizon in ('5', '15', '60'):
            statuses = collections.Counter(e['outcomes'][horizon]['status'] for e in client)
            measured = [e['outcomes'][horizon] for e in client if e['outcomes'][horizon]['status'] == 'observed']
            horizons[horizon] = {**dict(statuses), 'recovered': sum(e['recovered'] for e in measured),
                                 'medianUploadGiB': statistics.median(e['uploadGiB'] for e in measured) if measured else None}
        examples = sorted(client, key=lambda e: e['outcomes']['5'].get('uploadGiB', -1), reverse=True)[:15]
        result[cid] = {'episodes': len(client), 'horizonsMinutes': horizons, 'examples': examples}
    return result


def flow_groups(db, cid, start, end):
    """Per-tracker/category estimates from valid consecutive per-torrent counters."""
    groups = {}
    previous = None
    for row in db.execute('''SELECT s.run_id,s.ts,s.up,s.down,s.free,s.data,r.tracker FROM samples s
        JOIN runs r ON r.id=s.run_id WHERE r.client_id=? AND s.ts BETWEEN ? AND ? ORDER BY s.run_id,s.ts''',
                           (cid, start - 90, end)):
        t = json.loads(row['data'])
        if previous and previous['run_id'] == row['run_id'] and row['ts'] >= start and 0 < row['ts'] - previous['ts'] <= 90:
            if all(number(x[k]) for x in (previous, row) for k in ('up', 'down')):
                up, down = row['up'] - previous['up'], row['down'] - previous['down']
                if up >= 0 and down >= 0:
                    key = (t.get('tracker') or 'unknown', t.get('category') or '')
                    group = groups.setdefault(key, {'tracker': key[0], 'category': key[1], 'up': 0, 'down': 0,
                                                    'paid': 0, 'unknownCost': 0, 'runs': set()})
                    group['up'] += up
                    group['down'] += down
                    if row['free'] != previous['free']:
                        group['unknownCost'] += down
                    elif not row['free']:
                        group['paid'] += down
                    group['runs'].add(row['run_id'])
        previous = row
    return [{'tracker': g['tracker'], 'category': g['category'], 'observedRuns': len(g['runs']),
             'uploadGiB': g['up'] / GiB, 'downloadGiB': g['down'] / GiB,
             'estimatedPaidDownloadGiB': g['paid'] / GiB, 'unknownCostGiB': g['unknownCost'] / GiB,
             'estimatedNetGiB': (g['up'] - g['paid']) / GiB}
            for g in sorted(groups.values(), key=lambda g: g['up'] + g['down'], reverse=True)[:30]]


def hour_bins(db, cid, start):
    return [dict(r) for r in db.execute('''SELECT CAST(minute/3600 AS INTEGER)*3600 AS hour,
      sum(good_seconds)/60 AS observedMinutes,sum(observed_up)/1073741824 AS uploadGiB,
      sum(paid_down)/1073741824 AS paidDownloadGiB,min(free_min)/1073741824 AS minFreeGiB,
      sum(low_seconds)/60 AS lowMinutes,sum(critical_seconds)/60 AS criticalMinutes
      FROM minutes WHERE client_id=? AND minute>=? GROUP BY hour ORDER BY hour''', (cid, int(start // 60) * 60))]


def deletion_cases(db, actions, logs):
    cases = []
    for action in actions[-30:]:
        run = db.execute('SELECT * FROM runs WHERE id=?', (action['run_id'],)).fetchone()
        case = {'at': action['ts'], 'hash': action['hash'], 'name': action['payload'].get('name'),
                'rule': (action['payload'].get('rule') or {}).get('alias'),
                'association': action['payload']['attribution'], 'firstCandidateAt': None,
                'reason': None, 'candidateToActionSeconds': None,
                'disappearanceAt': run['ended_at'] if run and run['end_reason'] == 'disappeared' else None}
        captured = db.execute('''SELECT policies.scheduling FROM polls p JOIN policies ON policies.id=p.policy_id
          WHERE p.client_id=? AND p.ok=1 AND p.ts<=? ORDER BY p.ts DESC LIMIT 1''',
                              (action['client_id'], action['ts'])).fetchone()
        rule_id = json.loads(captured[0] or '{}').get('ruleId') if captured else None
        if rule_id == action['rule_id'] and action['hash']:
            returned = db.execute('''SELECT ts,payload FROM events WHERE client_id=? AND hash=? AND rule_id=?
                AND kind='policy-returned' AND ts BETWEEN ? AND ? ORDER BY ts DESC LIMIT 1''',
                (action['client_id'], action['hash'], action['rule_id'], action['ts'] - 120, action['ts'] + 5)).fetchone()
            if returned:
                evidence = json.loads(returned['payload'])
                case.update(returnedTrueAt=returned['ts'], returnToActionSeconds=action['ts'] - returned['ts'],
                            reason=evidence.get('candidate', {}).get('reason'),
                            executionSnapshot={k: evidence.get(k) for k in (
                                'build', 'runtimeId', 'sampleTime', 'generation', 'freeGiB', 'targetGiB', 'emergencyReclaim',
                                'precriticalReclaim', 'precriticalHold', 'reclaimPlanning', 'criticalLargeReclaim',
                                'recoveryLargeReclaim', 'executionGuard', 'candidate')})
            matches = []
            for log in logs:
                if not action['ts'] - 600 <= log.get('time', 0) <= action['ts'] + 120:
                    continue
                for c in log.get('window', {}).get('candidateExamples', []):
                    at = c.get('observedAt')
                    if c.get('hash') == action['hash'] and number(at) and action['ts'] - 600 <= at <= action['ts']:
                        matches.append((at, c.get('reason')))
            if matches:
                earliest = min(matches)
                case.update(firstCandidateAt=earliest[0], reason=case['reason'] or earliest[1],
                            candidateToActionSeconds=action['ts'] - earliest[0])
        cases.append(case)
    return cases


def make_report(db, hours=24, now=None):
    now = now or time.time()
    first = db.execute('SELECT min(ts) FROM polls WHERE ok=1').fetchone()[0]
    if first is None:
        return {'generatedAt': now, 'status': 'no-data', 'clients': [], 'rssAllocation': summarize_rss(db, now - hours * 3600, now)}
    start = max(first, now - hours * 3600)
    profiles = {r['id']: json.loads(r['profile']) for r in db.execute('SELECT id,profile FROM policies')}
    report = {'generatedAt': now, 'from': start, 'to': now, 'hours': (now - start) / 3600,
              'status': 'collecting', 'clients': [], 'rssAllocation': summarize_rss(db, start, now),
              'limitations': [
                  '累计流量来自客户端传输计数，不等于站点实际入账；FREE 分类仅用于估计。',
                  '单种增量缺少首次观测前和最后观测后的一小段；全局计数可辅助核对。',
                  '资源压力时长按轮询近似；策略窗口计数可能跨越报告起点。',
                  '恢复分析采用原始速度条件，不等同完整删种判定；被删除样本的后续收益不可观测。',
                  '恢复定义：后续 5/15/60 分钟平均上传速度达到片段起点下行速度乘该档回报阈值。片段数不等于独立种子数。',
                  '不能用 SSD/HDD 原始收益直接作 A/B 对照，也不能将历史回放视为真实反事实收益。']}
    recovery = recoveries(db, start, now, profiles)
    for client in db.execute('SELECT * FROM clients ORDER BY id'):
        cid = client['id']
        rows = list(db.execute('SELECT * FROM polls WHERE client_id=? AND ts>=? ORDER BY ts', (cid, start)))
        successful = [{**dict(r), 'server': json.loads(r['server'])} for r in rows if r['ok']]
        sums = db.execute('''SELECT sum(good_seconds) good,sum(observed_up) up,sum(observed_down) down,
          sum(paid_down) paid,sum(unknown_cost_down) unknown,sum(low_seconds) low,sum(critical_seconds) critical,
          sum(io_high_seconds) io,min(free_min) min_free FROM minutes WHERE client_id=? AND minute>=?''',
                          (cid, int(start // 60) * 60)).fetchone()
        span = now - successful[0]['ts'] if successful else now - start
        up, up_resets = counter_total(successful, 'alltime_ul')
        down, down_resets = counter_total(successful, 'alltime_dl')
        events = [{**dict(r), 'payload': json.loads(r['payload'])} for r in db.execute(
            'SELECT * FROM events WHERE client_id=? AND ts>=? ORDER BY ts', (cid, start))]
        counts = collections.Counter(e['kind'] for e in events)
        logs = [e['payload'] for e in events if e['kind'] == 'policy']
        windows = [e['window'] for e in logs if 'window' in e]
        blockers = collections.Counter()
        reasons = collections.Counter()
        emergency_tiers = collections.Counter()
        for window in windows:
            blockers.update(window.get('lowValueBlockers', {}))
            reasons.update(window.get('candidateReasons', {}))
            emergency_tiers.update(window.get('emergencyCandidateTiers', {}))
        actions = [e for e in events if e['kind'] == 'action-success' and e['payload'].get('action') == 'delete']
        matched = 0
        matched_runs = set()
        for event in actions:
            run = db.execute('SELECT ended_at,end_reason FROM runs WHERE id=?', (event['run_id'],)).fetchone()
            if run and run['end_reason'] == 'disappeared' and run['ended_at'] is not None and abs(run['ended_at'] - event['ts']) <= 120:
                matched += 1
                matched_runs.add(event['run_id'])
        summary = {'clientId': cid, 'alias': client['alias'], 'versions': json.loads(client['versions']),
                   'polls': len(rows), 'successfulPolls': len(successful), 'failedPolls': len(rows) - len(successful),
                   'lastSuccessAgeSeconds': now - successful[-1]['ts'] if successful else None,
                   'coverageFraction': min(1, (sums['good'] or 0) / span) if span > 0 else 0,
                   'requestP95Ms': sorted(r['ms'] for r in successful)[max(0, math.ceil(len(successful) * .95) - 1)] if successful else None,
                   'responseBytes': sum(r['bytes'] or 0 for r in rows),
                   'clientCounterUploadGiB': up / GiB, 'clientCounterDownloadGiB': down / GiB,
                   'clientCounterResets': up_resets + down_resets,
                   'observedTorrentUploadGiB': (sums['up'] or 0) / GiB,
                   'observedPaidDownloadGiB': (sums['paid'] or 0) / GiB,
                   'unknownCostDownloadGiB': (sums['unknown'] or 0) / GiB,
                   'estimatedNetGiB': ((sums['up'] or 0) - (sums['paid'] or 0)) / GiB,
                   'minimumFreeGiB': sums['min_free'] / GiB if sums['min_free'] is not None else None,
                   'fixedLowSpaceMinutes': (sums['low'] or 0) / 60,
                   'fixedCriticalSpaceMinutes': (sums['critical'] or 0) / 60,
                   'cacheOverloadMinutes': (sums['io'] or 0) / 60,
                   'eventCounts': dict(counts), 'policyBuilds': sorted({e.get('build', '?') for e in logs}),
                   'latestPolicyAgeSeconds': now - logs[-1]['time'] if logs else None,
                   'policyErrors': sum(e.get('reason') == 'error' for e in logs),
                   'policyCandidateOccurrences': dict(reasons), 'protectionOccurrences': dict(blockers),
                   'emergencyRankingEvaluations': sum(w.get('emergencyRankingEvaluations', 0) for w in windows),
                   'emergencyCandidateTiers': dict(emergency_tiers),
                   'recoveryRankingEvaluations': sum(w.get('recoveryRankingEvaluations', 0) for w in windows),
                   'precriticalRankingEvaluations': sum(w.get('precriticalRankingEvaluations', 0) for w in windows),
                   'guardPassedReturns': sum(w.get('guardPassedReturns', 0) for w in windows),
                   'guardDegradedCriticalReturns': sum(w.get('guardDegradedCriticalReturns', 0) for w in windows),
                   'guardBlocks': dict(sum((collections.Counter(w.get('guardBlocks', {})) for w in windows), collections.Counter())),
                   'functionTiming': aggregate_function_timing(logs),
                   'planningStatistics': aggregate_planning(logs),
                   'policyCoreTiming': {
                       'evaluations': sum(w.get('evaluations', 0) for w in windows),
                       'meanMs': (sum(w.get('evaluationMsTotal', 0) for w in windows) /
                                  sum(w.get('evaluations', 0) for w in windows)) if sum(w.get('evaluations', 0) for w in windows) else None,
                       'maxMs': max((w.get('evaluationMsMax', 0) for w in windows), default=None)}, 
                   'sharedDataKnownOccurrences': sum(w.get('sharedDataKnownOccurrences', 0) for w in windows),
                   'sharedDataTaskOccurrences': sum(w.get('sharedDataTaskOccurrences', 0) for w in windows),
                   'torrentOnlyCandidateOccurrences': sum(w.get('torrentOnlyCandidateOccurrences', 0) for w in windows),
                   'runtimeIds': sorted({e['runtimeId'] for e in logs if e.get('runtimeId')}),
                   'policySpacePressureEvaluations': sum(w.get('spacePressureEvaluations', 0) for w in windows),
                   'policyIOPressureEvaluations': sum(w.get('ioPressureEvaluations', 0) for w in windows),
                   'policyBandwidthPressureEvaluations': sum(w.get('bandwidthPressureEvaluations', 0) for w in windows),
                   'fieldCoverage': {k: sum(number(r['server'].get(k)) for r in successful) for k in (
                       'free_space_on_disk', 'queued_io_jobs', 'read_cache_overload', 'write_cache_overload')},
                   'loggedDeleteSuccess': len(actions), 'deleteSuccessWithMatchedDisappearance': len(matched_runs),
                   'deleteSuccessActionsWithMatchedDisappearance': matched,
                   'duplicateDeleteSuccessActions': matched - len(matched_runs),
                   'policyTrueReturnEvents': counts['policy-returned'],
                   'deletionCases': deletion_cases(db, actions, logs),
                   'actionFailures': counts['action-failure'],
                   'samplingGaps': counts['sampling-gap'],
                   'hourly': hour_bins(db, cid, start),
                   'trackerCategoryFlow': flow_groups(db, cid, start, now),
                   'recovery': recovery.get(cid, {'episodes': 0, 'horizonsMinutes': {}, 'examples': []})}
        report['clients'].append(summary)
    enough = report['hours'] >= 3 and bool(report['clients']) and all(
        c['coverageFraction'] >= .95 and c['policyErrors'] == 0 and c['lastSuccessAgeSeconds'] is not None and
        c['lastSuccessAgeSeconds'] <= 30 for c in report['clients'])
    report['status'] = 'ready-for-initial-review' if enough else 'collecting'
    report['review'] = {'automaticPolicyChanges': False,
                        'decision': 'review-measured-episodes' if enough else 'retain-current-until-more-data',
                        'reason': '检查压力周期、幸存样本恢复、实际执行及回放差异后再决定参数；运行时间本身不构成修改证据。'}
    return report


def render_html(report):
    def cell(value):
        return html.escape(str(value))
    replay_health = report.get('offlineReplay', {'status': 'not-checked'})
    replay_banner = ('<section><h2>离线回放（独立任务）</h2><p>报告生成时状态：' +
        cell(replay_health.get('status')) + '；该状态是生成时快照，不代表随后回放已完成。</p>' +
        '<p><a href="replay-status.html">查看本轮实时状态 / 有效期 / 最后成功时间</a>。'
        '失败、运行中或过期时，不得把历史候选当作最新结果；不影响在线采集和规则执行。</p><pre>' +
        cell(json.dumps(replay_health, ensure_ascii=False, indent=2)) + '</pre></section>')
    cards = []
    for c in report['clients']:
        metrics = [
            ('采样覆盖', f"{c['coverageFraction']:.1%}"), ('请求成功/失败', f"{c['successfulPolls']} / {c['failedPolls']}"),
            ('全局上传 / 下载 GiB', f"{c['clientCounterUploadGiB']:.2f} / {c['clientCounterDownloadGiB']:.2f}"),
            ('估计净流量 GiB', f"{c['estimatedNetGiB']:.2f}"), ('最低空闲 GiB', c['minimumFreeGiB']),
            ('低水位 / 紧急水位分钟', f"{c['fixedLowSpaceMinutes']:.2f} / {c['fixedCriticalSpaceMinutes']:.2f}"),
            ('删除成功日志 / 独立匹配消失', f"{c['loggedDeleteSuccess']} / {c['deleteSuccessWithMatchedDisappearance']}"),
            ('策略错误', c['policyErrors']), ('低回报片段', c['recovery']['episodes']),
            ('核心评估平均 / 最大 ms', f"{c['policyCoreTiming']['meanMs']} / {c['policyCoreTiming']['maxMs']}"),
            ('函数调用数 / 缓存命中', f"{c['functionTiming']['calls']} / {c['functionTiming']['cacheCalls']}"),
            ('函数平均 / p95 桶上界 / 最大 ms',
             f"{c['functionTiming']['meanMs']} / {c['functionTiming']['p95UpperBoundMs']} / {c['functionTiming']['maxMs']}")]
        table = ''.join('<tr><th>' + cell(k) + '</th><td>' + cell(v) + '</td></tr>' for k, v in metrics)
        hours = ''.join('<tr><td>' + cell(dt.datetime.fromtimestamp(h['hour'], dt.timezone(dt.timedelta(hours=8))).strftime('%m-%d %H:00')) +
                        '</td><td>' + cell(round(h['uploadGiB'] or 0, 2)) + '</td><td>' + cell(round(h['paidDownloadGiB'] or 0, 2)) +
                        '</td><td>' + cell(round(h['observedMinutes'] or 0, 1)) + '</td></tr>' for h in c['hourly'])
        cards.append('<section><h2>' + cell(c['alias']) + '</h2><table>' + table + '</table>' +
                     '<h3>逐小时流量</h3><table><tr><th>新加坡时间</th><th>上传 GiB</th><th>估计付费下载 GiB</th><th>观测分钟</th></tr>' +
                     hours + '</table><details><summary>判定、分组流量与恢复数据</summary><pre>' +
                     cell(json.dumps({k: c[k] for k in ('policyBuilds', 'runtimeIds', 'policyCandidateOccurrences', 'emergencyRankingEvaluations', 'recoveryRankingEvaluations',
                        'precriticalRankingEvaluations', 'planningStatistics', 'guardBlocks', 'guardPassedReturns', 'guardDegradedCriticalReturns', 'functionTiming', 'policyCoreTiming', 'emergencyCandidateTiers', 'sharedDataKnownOccurrences', 'sharedDataTaskOccurrences', 'torrentOnlyCandidateOccurrences', 'duplicateDeleteSuccessActions', 'protectionOccurrences', 'deletionCases', 'trackerCategoryFlow', 'recovery')},
                                     ensure_ascii=False, indent=2)) + '</pre></details></section>')
    stamp = dt.datetime.fromtimestamp(report['generatedAt'], dt.timezone(dt.timedelta(hours=8))).isoformat(timespec='seconds')
    return '<!doctype html><html lang="zh-CN"><meta charset="utf-8"><title>Vertex 长期监测</title><style>' + \
        'body{font:16px system-ui;background:#f5f7fa;color:#172b4d;max-width:1100px;margin:36px auto;padding:0 20px}' + \
        'section{background:white;padding:24px;margin:20px 0;border:1px solid #dce2e8;border-radius:12px}' + \
        'table{width:100%;border-collapse:collapse}td,th{text-align:left;padding:10px;border-bottom:1px solid #eee}' + \
        'pre{white-space:pre-wrap;overflow-wrap:anywhere}summary{cursor:pointer;margin-top:20px}</style>' + \
        '<h1>Vertex 长期监测</h1><p>' + cell(stamp) + ' · ' + cell(report.get('status')) + ' · ' + \
        cell(round(report.get('hours', 0), 2)) + ' 小时</p>' + render_rss(report.get('rssAllocation', {'status': 'collector-upgrade-needed'})) + replay_banner + ''.join(cards) + '<section><h2>解释边界</h2><ul>' + \
        ''.join('<li>' + cell(x) + '</li>' for x in report.get('limitations', [])) + '</ul></section></html>'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, default=DEFAULT_CONFIG)
    parser.add_argument('--hours', type=float, default=24)
    parser.add_argument('--review', action='store_true')
    args = parser.parse_args()
    if not 0 < args.hours <= 24 * 90:
        parser.error('hours must be positive and at most 2160')
    config = json.loads(args.config.read_text())
    state = Path(config['state_dir'])
    directory = state / 'reports'
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    db = sqlite3.connect('file:' + str(state / 'telemetry.sqlite') + '?mode=ro', uri=True, timeout=15)
    db.row_factory = sqlite3.Row
    db.execute('BEGIN')
    report = make_report(db, args.hours)
    db.close()
    report['offlineReplay'] = status_for_report(directory)
    stamp = dt.datetime.now(dt.timezone(dt.timedelta(hours=8))).strftime('%Y%m%d-%H%M%S')
    name = ('review-' if args.review else 'report-') + stamp
    atomic_json(directory / (name + '.json'), report)
    atomic_json(directory / 'latest.json', report)
    text = render_html(report)
    for path in (directory / (name + '.html'), directory / 'latest.html'):
        tmp = path.with_suffix('.tmp')
        tmp.write_text(text)
        tmp.chmod(0o600)
        tmp.replace(path)
    if args.review:
        atomic_json(directory / 'review-latest.json', report)
    # Retention includes exported reports, not only database tables.
    for path in directory.glob('*-*.*'):
        if path.stat().st_mtime < time.time() - config['aggregate_days'] * 86400:
            path.unlink()
    print(json.dumps({'report': str(directory / (name + '.html')), 'status': report['status'],
                      'hours': report.get('hours', 0)}, ensure_ascii=False))


if __name__ == '__main__':
    main()
