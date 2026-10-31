"""Read-only RSS allocation summaries and decision lookup; no counterfactual claims."""
import argparse
import collections
import html
import json
from pathlib import Path
import sqlite3
import time


def available(db):
    return bool(db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='rss_events'").fetchone())


def summarize(db, start, end):
    if not available(db):
        return {'status': 'collector-upgrade-needed', 'clients': [], 'rssTasks': [], 'recentDecisions': []}
    begin = int(start // 60) * 60
    names = {(row['kind'], row['id']): row['alias'] for row in db.execute('SELECT * FROM rss_names')}
    clients = {}
    tasks = {}
    stages = collections.Counter()
    results = collections.Counter()
    quality = collections.Counter()
    hourly = collections.Counter()
    rows = db.execute('''SELECT rss_id,client_id,route,mode,stage,outcome,quality,sum(count) n
        FROM rss_minutes WHERE minute>=? AND minute<=?
        GROUP BY rss_id,client_id,route,mode,stage,outcome,quality''', (begin, end))
    def bucket(mapping, key, kind):
        return mapping.setdefault(key, {'id': key, 'alias': names.get((kind, key), key or '历史未知/未选择'),
                                      'decisions': 0, 'submitted': 0, 'outcomes': collections.Counter(),
                                      'routes': collections.Counter(), 'legacyEvents': 0})
    for row in rows:
        count = row['n']
        stages[row['stage']] += count
        quality[row['quality']] += count
        if row['stage'] == 'result':
            results[row['outcome']] += count
        for item in [bucket(tasks, row['rss_id'], 'rss'), *([bucket(clients, row['client_id'], 'client')] if row['client_id'] else [])]:
            item['legacyEvents'] += count if row['quality'] != 'v2' else 0
            if row['stage'] == 'decision':
                item['decisions'] += count
                item['routes'][row['route']] += count
            elif row['stage'] == 'submitted':
                item['submitted'] += count
            else:
                item['outcomes'][row['outcome']] += count
    for row in db.execute("SELECT CAST(minute/3600 AS INTEGER)*3600 hour,client_id,route,outcome,sum(count) n FROM rss_minutes WHERE minute>=? AND minute<=? AND stage='result' GROUP BY hour,client_id,route,outcome", (begin, end)):
        hourly[(row['hour'], row['client_id'], row['route'], row['outcome'])] += row['n']

    cases = []
    exclusions = collections.Counter()
    not_considered = collections.Counter()
    fair = {}
    examined = complete = 0
    query = '''SELECT d.*,r.outcome result_outcome,r.ts result_at,s.ts submitted_at
        FROM rss_events d LEFT JOIN rss_events r ON r.decision_key=d.decision_key AND r.stage='result'
        LEFT JOIN rss_events s ON s.decision_key=d.decision_key AND s.stage='submitted'
        WHERE d.stage='decision' AND d.ts BETWEEN ? AND ? AND d.payload IS NOT NULL
        ORDER BY d.ts DESC LIMIT 20001'''
    for row in db.execute(query, (start, end)):
        examined += 1
        if examined > 20000:
            break
        event = json.loads(row['payload'])
        candidates = event.get('candidates', [])
        for candidate in candidates:
            if candidate.get('considered') is True and candidate.get('eligible') is False:
                exclusions[(candidate['clientId'], candidate.get('reason') or 'unknown')] += 1
            elif candidate.get('considered') is False:
                not_considered[(candidate['clientId'], candidate.get('reason') or 'unknown')] += 1
        eligible = [candidate for candidate in candidates if candidate.get('considered') and candidate.get('eligible')]
        full = row['quality'] == 'v2' and not event.get('snapshotTruncated') and bool(eligible)
        if full:
            complete += 1
        if (full and event.get('route') == 'weighted' and row['submitted_at'] is not None
                and row['result_outcome'] not in ('cancelled', 'rejected', 'filtered')
                and all(type(candidate.get('normalizedShare')) in (float, int) for candidate in eligible)):
            for candidate in eligible:
                data = fair.setdefault(candidate['clientId'], {'clientId': candidate['clientId'], 'eligibleOccurrences': 0,
                                                             'expectedSubmittedSelections': 0, 'submittedSelections': 0})
                data['eligibleOccurrences'] += 1
                data['expectedSubmittedSelections'] += candidate['normalizedShare']
                data['submittedSelections'] += candidate['clientId'] == event.get('clientId')
        if len(cases) < 20:
            if row['payload_size'] > 65536:
                event = {key: value for key, value in event.items() if key != 'candidates'}
                event['detailOmittedForReportSize'] = True
            cases.append({'decision': event, 'quality': row['quality'], 'result': row['result_outcome'] or 'missing-or-in-flight',
                          'resultAt': row['result_at'], 'submittedAt': row['submitted_at']})
    dangling = db.execute("SELECT count(*) FROM rss_events d LEFT JOIN rss_events r ON r.decision_key=d.decision_key AND r.stage='result' WHERE d.stage='decision' AND d.ts BETWEEN ? AND ? AND r.id IS NULL", (start, end)).fetchone()[0]
    first = db.execute("SELECT min(ts) FROM rss_events WHERE quality='v2' AND stage='decision'").fetchone()[0]
    return {
        'status': 'recording' if stages else 'waiting-for-events', 'summaryFrom': begin, 'summaryTo': end,
        'summaryGranularitySeconds': 60, 'firstV2DecisionAt': first,
        'stages': dict(stages), 'outcomes': dict(results), 'quality': dict(quality),
        'clients': sorted(clients.values(), key=lambda row: row['id']), 'rssTasks': sorted(tasks.values(), key=lambda row: row['id']),
        'hourly': [{'hour': key[0], 'clientId': key[1], 'route': key[2], 'outcome': key[3], 'count': count} for key, count in sorted(hourly.items())],
        'candidateExclusions': [{'clientId': key[0], 'reason': key[1], 'occurrences': count} for key, count in exclusions.most_common()],
        'notConsidered': [{'clientId': key[0], 'reason': key[1], 'occurrences': count} for key, count in not_considered.most_common()],
        'fairnessExposure': list(fair.values()), 'rawDecisionsExamined': min(examined, 20000),
        'rawDetailScanLimited': examined > 20000, 'completeEligibleMatrices': complete,
        'decisionsWithoutResult': dangling, 'recentDecisions': cases,
        'limitations': [
            'accepted 是正向添加 API 回应，不等于已独立观察到 qB 创建任务；结果缺失不自动记为失败。',
            '旧日志仅有选中目标；RSS ID、种子与未选目标输入缺失时保持未知，不能补造完整候选快照。',
            '自由分配、强制定向、辅种分开统计；预检查失败和过滤不是公平轮转机会。',
            '轮转可能选择非最大权重机器；曝光表只比较完整且已提交快照的累计份额，受起止信用、候选集及运行实例影响，不自动判定不公平。',
            '总量按分钟聚合，起点向下取整；候选原因与曝光只覆盖仍保留的原始明细，最多扫描最近 20000 次决策。',
            '不新增 RBX/VPS qB 流量采样，不声称收益归因或未选目标的反事实收益。']
    }


def render(section):
    escape = lambda value: html.escape(str(value))
    if section.get('status') == 'collector-upgrade-needed':
        return '<section><h2>RSS 分配审计</h2><p>采集器尚未建立 RSS 审计表。</p></section>'
    def table(items):
        text = '<table><tr><th>目标 / RSS</th><th>决策</th><th>提交</th><th>结果</th><th>路由</th><th>旧格式事件</th></tr>'
        for item in items:
            text += '<tr>' + ''.join('<td>' + escape(value) + '</td>' for value in [item['alias'], item['decisions'], item['submitted'], json.dumps(item['outcomes'], ensure_ascii=False), json.dumps(item['routes'], ensure_ascii=False), item['legacyEvents']]) + '</tr>'
        return text + '</table>'
    return ('<section><h2>RSS 分配审计</h2><p>状态：' + escape(section['status']) + '；结果：' +
            escape(json.dumps(section['outcomes'], ensure_ascii=False)) + '；缺结果/在途：' + escape(section['decisionsWithoutResult']) + '</p>' +
            '<h3>按下载器</h3>' + table(section['clients']) + '<h3>按 RSS 任务</h3>' + table(section['rssTasks']) +
            '<details><summary>候选排除原因、轮转曝光和逐小时统计</summary><pre>' +
            escape(json.dumps({key: section[key] for key in ['quality', 'firstV2DecisionAt', 'candidateExclusions', 'notConsidered', 'fairnessExposure', 'hourly', 'rawDetailScanLimited']}, ensure_ascii=False, indent=2)) + '</pre></details>' +
            '<details><summary>最近 20 次决策：种子、候选状态、权重与结果</summary><pre>' +
            escape(json.dumps(section['recentDecisions'], ensure_ascii=False, indent=2)) + '</pre></details><ul>' +
            ''.join('<li>' + escape(value) + '</li>' for value in section['limitations']) + '</ul></section>')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--db', type=Path, default=Path(__file__).resolve().parent.parent / '.local/telemetry/telemetry.sqlite')
    parser.add_argument('--decision')
    parser.add_argument('--hash')
    parser.add_argument('--rss-id')
    parser.add_argument('--limit', type=int, default=20)
    args = parser.parse_args()
    if not 1 <= args.limit <= 100:
        parser.error('limit must be 1..100')
    clauses = []
    values = []
    for flag, clause in [(args.decision, '(decision_id=? OR lease=?)'), (args.hash, 'hash=?'), (args.rss_id, 'rss_id=?')]:
        if flag:
            clauses.append(clause)
            values.extend([flag] * clause.count('?'))
    with sqlite3.connect('file:' + str(args.db) + '?mode=ro', uri=True, timeout=15) as db:
        db.row_factory = sqlite3.Row
        query = 'SELECT * FROM rss_events' + (' WHERE ' + ' AND '.join(clauses) if clauses else '') + ' ORDER BY ts DESC LIMIT ?'
        rows = [dict(row) for row in db.execute(query, (*values, args.limit))]
        for row in rows:
            row['payload'] = json.loads(row['payload']) if row['payload'] else None
        print(json.dumps(rows, ensure_ascii=False, indent=2))

if __name__ == '__main__':
    main()
