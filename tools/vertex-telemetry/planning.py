"""Bounded v10 summary aggregation; never count planned group bytes as released."""
import collections

COUNTERS = ('precriticalHeldEvaluations', 'planningChangedEvaluations', 'groupPlanEvaluations',
            'groupPlanReturns', 'groupPlanCompletions', 'criticalLargeEvaluations', 'criticalLargeChangedEvaluations',
            'stalledQualifiedOccurrences', 'stalledEligibleOccurrences', 'smallDeficitPlanningEvaluations',
            'recoveryLargeEvaluations', 'recoveryLargeChangedEvaluations', 'lowReturnRecentQualifiedOccurrences',
            'lowReturnLegacyProtectionBypassed', 'lowReturnMaterialUploadProtected')
MAPS = ('precriticalHoldExits', 'groupPlanCancellations')


def aggregate_planning(logs):
    windows = [log['window'] for log in logs if 'window' in log]
    latest = next((log for log in reversed(logs) if 'reclaimPlanning' in log), {})
    return {'records': sum('reclaimPlanning' in log for log in logs),
            **{key: sum(w.get(key, 0) for w in windows) for key in COUNTERS},
            **{key: dict(sum((collections.Counter(w.get(key, {})) for w in windows), collections.Counter()))
               for key in MAPS},
            'guardExamples': [e for w in windows for e in w.get('guardExamples', [])][-20:],
            'groupCancellationExamples': [e for w in windows for e in w.get('groupCancellationExamples', [])][-20:],
            'latestCriticalLargeReclaim': latest.get('criticalLargeReclaim'),
            'latestRecoveryLargeReclaim': latest.get('recoveryLargeReclaim'),
            'latestLowReturnEvidence': latest.get('lowReturnEvidence'),
            'latestStalledCleanup': latest.get('stalledCleanup'),
            'latestPlanning': latest.get('reclaimPlanning'), 'latestHold': latest.get('precriticalHold'),
            'note': '候选/评估可重复；组发放不是成功，组完成只表示后续快照成员消失；eventualReclaim不是此次直接释放量。'}
