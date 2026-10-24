const fs = require('fs');
const path = require('path');
const vm = require('vm');
const { createRequire } = require('module');

// 注入外部边界，测试不能启动真实的 Redis、SQLite、Cron 或下载器。
exports.loadModule = function (relativePath, mocks, timers = {}) {
  const filename = path.resolve(__dirname, '../..', relativePath);
  const source = fs.readFileSync(filename, 'utf8');
  const realRequire = createRequire(filename);
  const requireMock = id => Object.prototype.hasOwnProperty.call(mocks, id) ? mocks[id] : realRequire(id);
  const module = { exports: {} };
  const evaluate = vm.runInThisContext(`(function (require, module, exports, __filename, __dirname, setTimeout, clearTimeout) {\n${source}\n})`, { filename });
  evaluate(requireMock, module, module.exports, filename, path.dirname(filename), timers.setTimeout || setTimeout, timers.clearTimeout || clearTimeout);
  return module.exports;
};

exports.createLogger = function () {
  const entries = [];
  const logger = { entries };
  for (const level of ['debug', 'info', 'warn', 'error']) {
    logger[level] = (...args) => entries.push({ level, args });
  }
  return logger;
};
