const request = require('request');

// Unlike Promise.race alone, settling the deadline/cancellation destroys the actual HTTP request.
module.exports = function requestWithDeadline (options, { timeoutMs, signal } = {}) {
  if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) return Promise.reject(new Error('invalid request deadline'));
  return new Promise((resolve, reject) => {
    let pending;
    let timer;
    let settled = false;
    const finish = (error, response) => {
      if (settled) return;
      settled = true;
      if (timer) clearTimeout(timer);
      if (signal) signal.removeListener('abort', onAbort);
      if (error) reject(error);
      else resolve(response);
    };
    const abort = code => {
      if (settled) return;
      const error = new Error(code === 'ETIMEDOUT' ? 'request deadline exceeded' : 'request cancelled');
      error.code = code;
      finish(error);
      if (pending) {
        try { pending.abort(); } catch (_) { /* Already settled; a transport cleanup error cannot leak a timer. */ }
      }
    };
    const onAbort = () => abort('EABORTED');
    if (signal?.aborted) {
      onAbort();
      return;
    }
    try {
      pending = request(options, (error, response) => finish(error, response));
      if (!settled) {
        if (signal) signal.once('abort', onAbort);
        timer = setTimeout(() => abort('ETIMEDOUT'), timeoutMs);
        if (signal?.aborted) onAbort();
      }
    } catch (error) {
      finish(error);
    }
  });
};
