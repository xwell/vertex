const request = require('request');

// Deadline/cancellation destroys the actual HTTP request. Optional observations cannot affect it.
module.exports = function requestWithDeadline (options, { timeoutMs, signal, observe } = {}) {
  if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) return Promise.reject(new Error('invalid request deadline'));
  return new Promise((resolve, reject) => {
    let pending;
    let timer;
    let settled = false;
    const listeners = [];
    const emit = event => { if (typeof observe === 'function') { try { observe(event); } catch (_) { /* Diagnostics are never authority. */ } } };
    const listen = (object, event, callback) => {
      object.on(event, callback); listeners.push([object, event, callback]);
    };
    const finish = (error, response) => {
      if (settled) return;
      settled = true;
      if (timer) clearTimeout(timer);
      if (signal) signal.removeListener('abort', onAbort);
      for (const [object, event, callback] of listeners) object.removeListener(event, callback);
      emit({ type: 'end', outcome: error ? 'failure' : 'success' });
      if (error) reject(error);
      else resolve(response);
    };
    const abort = code => {
      if (settled) return;
      const error = new Error(code === 'ETIMEDOUT' ? 'request deadline exceeded' : 'request cancelled');
      error.code = code;
      finish(error);
      if (pending) {
        try { pending.abort(); } catch (_) { /* Already settled; cleanup cannot leak a timer. */ }
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
        if (typeof observe === 'function') {
          listen(pending, 'socket', socket => {
            emit({ type: 'socket', reused: !socket.connecting });
            if (socket.connecting) listen(socket, 'connect', () => emit({ type: 'connect' }));
            if (socket.encrypted && !socket._secureEstablished) listen(socket, 'secureConnect', () => emit({ type: 'tls' }));
          });
          listen(pending, 'response', response => {
            emit({ type: 'headers', statusCode: response.statusCode });
            listen(response, 'data', chunk => emit({ type: 'bytes', bytes: Buffer.isBuffer(chunk) ? chunk.length : Buffer.byteLength(chunk) }));
          });
        }
        if (signal) signal.once('abort', onAbort);
        timer = setTimeout(() => abort('ETIMEDOUT'), timeoutMs);
        if (signal?.aborted) onAbort();
      }
    } catch (error) {
      finish(error);
    }
  });
};
