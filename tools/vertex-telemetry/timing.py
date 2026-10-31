"""Bounded histogram aggregation for completed policy function-body invocations.

No per-invocation samples, I/O, network, percentile interpolation or action counts.
"""
import math

BOUNDS_MS = [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 20, 50, 100]
PATHS = ('cacheCalls', 'evaluationCalls', 'reusedCalls', 'rejectedCalls')


def finite(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value) and value >= 0


def aggregate_function_timing(logs):
    counts = [0] * (len(BOUNDS_MS) + 1)
    result = {'records': 0, 'invalidRecords': 0, 'calls': 0, 'totalMs': 0, 'maxMs': None,
              **{k: 0 for k in PATHS}, 'trueReturnInvocations': 0, 'clockSources': [],
              'from': None, 'to': None, 'meanMs': None, 'p50UpperBoundMs': None, 'p95UpperBoundMs': None,
              'boundsMs': BOUNDS_MS[:], 'histogram': counts,
              'note': '仅覆盖已输出的已完成函数调用；当前摘要调用计入下一窗口。分位数为固定桶上界，不是精确分位数。'
                      '包含函数内复核和日志，不含 Vertex eval 解析、后续重新汇报/等待/qB 请求；true 返回调用数不是独立删除数。'}
    clocks = set()
    for log in logs:
        timing = log.get('functionTiming')
        if timing is None:
            continue
        valid = isinstance(timing, dict) and timing.get('boundsMs') == BOUNDS_MS
        if valid:
            histogram = timing.get('histogram')
            valid = (isinstance(histogram, list) and len(histogram) == len(counts) and
                     all(isinstance(x, int) and not isinstance(x, bool) and x >= 0 for x in histogram) and
                     all(finite(timing.get(k)) for k in ('calls', 'totalMs', 'maxMs', 'from', 'to', *PATHS, 'trueReturns')) and
                     all(isinstance(timing[k], int) and not isinstance(timing[k], bool) for k in ('calls', *PATHS, 'trueReturns')) and
                     sum(histogram) == timing['calls'] == sum(timing[k] for k in PATHS) and
                     timing['trueReturns'] <= timing['calls'] and timing['maxMs'] <= timing['totalMs'] + 1e-6 and
                     timing['to'] >= timing['from'] and timing.get('clock') in ('hrtime', 'performance', 'date'))
        if not valid:
            result['invalidRecords'] += 1
            continue
        result['records'] += 1
        result['calls'] += timing['calls']
        result['totalMs'] += timing['totalMs']
        result['maxMs'] = max(result['maxMs'] or 0, timing['maxMs'])
        for key in PATHS:
            result[key] += timing[key]
        result['trueReturnInvocations'] += timing['trueReturns']
        result['from'] = min(result['from'], timing['from']) if result['from'] is not None else timing['from']
        result['to'] = max(result['to'], timing['to']) if result['to'] is not None else timing['to']
        clocks.add(timing['clock'])
        for i, value in enumerate(histogram):
            counts[i] += value
    result['clockSources'] = sorted(clocks)
    if result['calls']:
        result['meanMs'] = result['totalMs'] / result['calls']
        for key, q in (('p50UpperBoundMs', .5), ('p95UpperBoundMs', .95)):
            target = math.ceil(result['calls'] * q)
            cumulative = 0
            for i, count in enumerate(counts):
                cumulative += count
                if cumulative >= target:
                    result[key] = min(result['maxMs'], BOUNDS_MS[i]) if i < len(BOUNDS_MS) else result['maxMs']
                    break
    return result
