# Opt-in qBittorrent main-data request guard

`maindataRequestGuard: true` in a qBittorrent downloader's persisted client JSON enables the guarded pipeline. The flag is deliberately opt-in; absent/false flags, Transmission and Deluge retain the existing behavior. The downloader settings UI does not currently expose this experimental switch.

## Behavior

- One request generation spans an optional bounded login, main-data GET, and at most one Unauthorized re-login/GET retry. All stages share the original maximum **30-second absolute budget**; shorter caller budgets are honored.
- The qB adapter passes the deadline and cancellation signal through `util.requestPromise` to the existing `request-with-deadline` transport. Timeout/cancellation aborts the actual request, rather than only winning a `Promise.race`.
- A newer generation supersedes/cancels the previous one. Before committing cookies, snapshots, averages, health, errors or freshness timestamps, the guard verifies generation, current downloader instance, opt-in flag and deadline.
- Queue task timeout, client destruction, explicit rollback and supersession cancel their request. Obsolete callbacks cannot clear a newer snapshot or stamp an older response fresh.
- Current genuine failures remain fail-closed: status is false, main data is unknown/null, and error count increases once. Queue circuit tracking remains active. This does **not** suppress real failures or guarantee uninterrupted inventory evidence.
- Snapshot preparation precedes the final synchronous commit. Constructor-time inline refresh defers one microtask so `global.runningClient[id]` has been registered before identity validation.
- No torrent operation, change to RSS allocation, deletion threshold, cold/seed duration or evidence backfill is performed.

## Diagnostics

`MAINDATA_REQUEST_GUARD` structured log summaries appear on successful requests at most once per minute, and immediately on current failures. They contain client ID, generation/counters, bounded duration and sanitized error code, not credentials, URLs or torrent identifiers.

Counters are request generations, not HTTP attempts: an authentication retry can make several HTTP calls inside one generation. `started/succeeded/failed/ignored/timedOut` are not deletion counts. `status(client)` reports diagnostics without contacting qBittorrent; no diagnostic timer or new cron job is installed.

## Optional request-phase and queue diagnostics (BHS enabled2026-11-12)

`maindataRequestDiagnostics: true` independently opts a **guarded qBittorrent** client into `maindata-request-diagnostics.js`. Absent/false stays off. The diagnostics helper creates no timers, requests, Cron jobs or business decisions. `AsyncLocalStorage` associates each async request generation with its own stages without URL-keyed guesses or cross-client trace attachment.

`MAINDATA_REQUEST_DIAGNOSTIC` INFO records are emitted at most once/minute for sampled successful/ignored results and immediately for genuine failures. Fixed counters/histogram summarize all observed completed generations; a request has at most4 stage records (optional login, GET, re-login, GET retry). Fields include stage kind/attempt, remaining budget, socket/connect/TLS/header/end times, received response bytes/status and last observed phase. Completed successful HTTP may still be followed by adapter validation/authentication handling; `phase='reading-body'` with `ended=true/outcome=success` does not mean a body is stuck.

Existing getMaindata/autoDelete tasks carry only bounded diagnostic metadata across the **existing** busy-retry path. Total enqueue-to-start wait is distinguished from the last enqueue wait, busy retries, lock action/age and run-duration maxima. No queue priority, dedupe key/TTL, retry delay, lock or scheduling cadence changes. Per-minute snapshots can lag the currently finishing queue task by one completion. UI task/torrent state is not changed.

URLs, headers, Cookie, usernames/passwords, torrent IDs/names and raw error messages are not diagnostic fields. Observer/logger exceptions cannot change transport outcomes. Transport listeners are removed on completion/cancellation. Existing30s absolute budget, real socket abort, generation/instance fencing, snapshot commit and true-failure semantics are unchanged; no automatic transport retry was added.

The authorized2026-11-12 BHS hot overlay wraps the **original guard execute/status closures**, preserving their private WeakMap/counters and all original client/queue/Cron/policy objects. It adds five source files' updates/new helper, one BHS diagnostic flag, and six reversible function-reference overlays. Existing non-diagnostic adapter/util callers retain their exact prior closures; `util.js` is not reinitialized (no SQLite migrations or Puppeteer startup). A fresh process uses normal module imports of the same updated source. No service restart occurred.

Private overlay/rollback: `/root/devs/vertex_scripts/.local/bhs-maindata-phase-diagnostics-20261112/`; report: `VERTEX_BHS_MAINDATA_PHASE_DIAGNOSTICS_20261112.md` in the installed scripts directory. This overlay intentionally supersedes some2026-11-07 method references. The old hotloader's all-reference fingerprint no longer represents the complete current stack; use the new overlay's status/rollback first, not the old install/rollback script. The underlying guard itself remains v1; diagnostics has its own version/origin and does not reset earlier counters.

## Missing snapshot rule evaluation

Rule evaluation now fails closed when the client is explicitly unhealthy, main data/torrents are unavailable, or disk free space is not a finite non-negative number. Unknown space is never replaced with zero. Ordinary conditions capture one synchronous snapshot; JavaScript rules are not evaluated on unavailable input. The normal per-rule result and real zero-space behavior are unchanged when input is valid.

An unavailable observation clears the affected rule's continuous/probabilistic fit-time evidence. `flashFitTime` also clears evidence rather than silently carrying a pre-outage confirmation through recovery. `autoDelete` rechecks availability between targets and after the existing reannounce/wait, before recording/sending a deletion. These checks do not change the configured delete count, wait, cron, upload/HR rules, or the Buffer policy's own freshness limits. They do not claim to cancel an already-sent deletion or make every asynchronous step atomic.

`RULE_SNAPSHOT_SKIPPED` warnings contain client ID, reason and a cumulative skip count only, at most once per minute per client, rather than logging one exception per torrent. This is an unavailable observation, not a successful delete or an HTTP failure counter. Existing guarded BHS state is independent from another downloader's opt-in.

For RBX, use the existing `maindataRequestGuard: true` only after the guarded code is installed. Diagnostics can independently opt in with `maindataRequestDiagnostics: true`. This repair does **not** add a five-second connect timeout, transport retries, keep-alive, or incremental RID sync. Source/image publication alone does not enable a persisted downloader flag or replace the running container.

## Tests

```sh
node --test test/client-*.test.js test/rss-*.test.js
```

`test/client-maindata-guard.test.js` covers opt-in isolation, real queue timeout/recovery with delayed adapter completions, generation/instance fencing, abort propagation, authentication budget, malformed data, listener cleanup, construction/destruction, inline paths, circuit probes, default-client compatibility and safe diagnostics. Adapter completions are mocked: no test contacts a production downloader.

`test/client-maindata-diagnostics.test.js` adds21 cases for opt-in/privacy, async correlation, all HTTP phases, fixed stage bounds, queue waits, logging failures, stale callbacks, same-budget authentication and observed real loopback success/body-stall abort. The2026-11-12 full app suite196 and hot-overlay mocks9 passed; the same205 tests passed on productionNode14.20.0 under network-none, readonly,0.2CPU/512MiB (test registration shim only).

Existing request-with-deadline tests additionally verify that actual loopback HTTP sockets are aborted. Production Node14 compatibility is checked separately with the guard tests under a registration shim (only Node's newer `node:test` registration is replaced).

## Deployment notes

A normal fresh application start loads the updated Client/queue/adapter and helper from disk. Updating source files alone does not replace methods already loaded in a running Node process.

The 2026-11-07 BHS deployment uses a fingerprint-checked, authenticated one-off hot install, updating seven method references without creating a replacement client, queue, cron or policy state. During that hot install, adapter callers without a deadline retain the exact original legacy closures. Only BHS is opted in. A source/runtime/config baseline and reversible rollback are kept privately outside the repository.

Current-container files survive `docker restart`; they are not a new published/pinned image. Recreating the container from the unchanged old image requires reinstalling these source changes (or building a reviewed new image). The persisted flag by itself does not patch an old image. Do not silently recreate/restart the process to perform this rollout: that would reset in-memory inventory evidence.
