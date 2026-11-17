# Daily cleanup Script example

`daily-cleanup.js` is a **function expression**, intended to be copied as the `script` field of an explicitly configured Vertex Script. Evaluating it returns an async function; it performs no cleanup until the Script scheduler calls that function.

Use the five-field cron `0 3 * * *` for once daily at 03:00 in the application's timezone. `* 3 * * *` runs every minute of hour three. Do not add an invocation `()` to the expression: Vertex already invokes the returned function. An async IIFE starts cleanup during eval, then returns a Promise which the scheduler incorrectly attempts to call.

The example preserves the existing cleanup scope: only `.gz` files in `/vertex/logs` and `.torrent` files in `/vertex/torrents` whose mtime is more than three days old. It does not delete qB tasks or payload data. Check these directories and the retention requirement before configuring it. This file is not automatically installed, executed or scheduled by the image.

`test/client-script-contract.test.js` evaluates the example with a synthetic filesystem, verifies that eval has no cleanup side effects, checks age/extension boundaries and the once-daily cron, and runs the actual Script scheduler callback against the mock only. No production cleanup is executed by tests.
