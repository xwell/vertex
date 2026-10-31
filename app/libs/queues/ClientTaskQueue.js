const { TaskQueue } = require('../redis');
const logger = require('../logger');
const redis = require('../redis');
const trackerSync = require('../tracker-sync');

class ClientTaskQueueWorker extends TaskQueue {
  constructor(queueName, executor, options) {
    super(queueName, options);
    this.executor = executor;
  }

  async executeTask(task) {
    return await this.executor.executeTask(task);
  }
}

class ClientTaskQueue {
  constructor() {
    this.actionTimeoutMs = {
      getMaindata: 30000,
      autoDelete: 300000,
      trackerSync: 10000,
      autoReannounce: 60000,
      record: 120000,
      flashFitTime: 60000
    };

    this.activeClientTasks = new Map();
    this.lowPriorityActions = new Set(['record', 'autoReannounce']);
    this.busyRetryDelayMs = 5000;
    this.foregroundActions = new Set(['getMaindata', 'autoDelete', 'flashFitTime']);
    this.foregroundPending = new Map();
    this.trackerContinuations = new Map();
    this.trackerRefreshRequired = new Map();

    this.failedClients = new Map();
    this.blockDuration = 2 * 60 * 1000;
    this.maxFailuresBeforeBlock = 5;
    this.circuitBreaker = {
      quickFailWindowMs: 2 * 60 * 1000,
      quickFailThreshold: 10,
      halfOpenSuccessThreshold: 1
    };

    this.fastActions = new Set(['getMaindata', 'flashFitTime']);
    this.slowActions = new Set(['autoDelete', 'trackerSync', 'record', 'autoReannounce']);
    this.circuitTrackedActions = new Set(['getMaindata']);
    this.circuitProbeActions = new Set(['getMaindata']);

    this.fastQueue = new ClientTaskQueueWorker('client_fast', this, { maxConcurrent: 6 });
    this.slowQueue = new ClientTaskQueueWorker('client_slow', this, { maxConcurrent: 4 });
  }

  _selectQueue(action) {
    return this.fastActions.has(action) ? this.fastQueue : this.slowQueue;
  }

  _getActionTimeout(action, taskData) {
    if (taskData && Number.isFinite(taskData.timeoutMs) && taskData.timeoutMs > 0) {
      return action === 'trackerSync' ? Math.min(taskData.timeoutMs, this.actionTimeoutMs.trackerSync) : taskData.timeoutMs;
    }
    return this.actionTimeoutMs[action] || 30000;
  }

  _getDedupeConfig(taskData) {
    const { clientId, action, params } = taskData || {};
    if (!clientId || !action) {
      return {};
    }
    let key = `vertex:queue:dedupe:client:${clientId}:${action}`;
    if (action === 'flashFitTime' && params?.rule?.id) {
      key = `${key}:${params.rule.id}`;
    }
    const timeoutMs = this._getActionTimeout(action, taskData);
    const ttlSeconds = Math.ceil(timeoutMs / 1000) + 60;
    return { dedupeKey: key, dedupeTtlSeconds: ttlSeconds };
  }

  _isLowPriority(action) {
    return this.lowPriorityActions.has(action);
  }

  _isCircuitTrackedAction(action) {
    return this.circuitTrackedActions.has(action);
  }

  _isCircuitProbeAction(action) {
    return this.circuitProbeActions.has(action);
  }

  _hasForegroundPending (clientId) {
    const pending = this.foregroundPending.get(clientId);
    if (!pending) return false;
    for (const [action, at] of pending) if (Date.now() - at > 60000) pending.delete(action);
    if (!pending.size) this.foregroundPending.delete(clientId);
    return pending.size > 0;
  }

  _continueTrackerSync (client) {
    if (this.trackerContinuations.has(client.id)) return;
    const timer = setTimeout(() => {
      this.trackerContinuations.delete(client.id);
      if (global.runningClient?.[client.id] !== client) return;
      this.enqueue({ clientId: client.id, action: 'trackerSync' }, 'normal').catch(error => {
        logger.error(`Tracker分片入队失败: ${client.alias}`, error);
      });
    }, trackerSync.limits.continueDelayMs);
    this.trackerContinuations.set(client.id, timer);
  }

  _getOrCreateClientInfo(clientId) {
    let info = this.failedClients.get(clientId);
    if (!info) {
      info = {
        state: 'CLOSED',
        count: 0,
        lastFailTime: 0,
        blocked: false,
        blockedAt: 0,
        openUntil: 0,
        quickFailures: [],
        halfOpenInFlight: false,
        halfOpenSuccess: 0
      };
      this.failedClients.set(clientId, info);
    }
    return info;
  }

  _refreshClientCircuit(clientId, info) {
    if (info.state === 'OPEN' && info.openUntil && Date.now() >= info.openUntil) {
      info.state = 'HALF_OPEN';
      info.blocked = false;
      info.halfOpenInFlight = false;
      info.halfOpenSuccess = 0;
      logger.warn(`客户端 ${clientId} 断路器进入半开，允许探测任务`);
      this.failedClients.set(clientId, info);
    }
  }

  _openClientCircuit(clientId, info, reason) {
    const now = Date.now();
    info.state = 'OPEN';
    info.blocked = true;
    info.blockedAt = now;
    info.openUntil = now + this.blockDuration;
    info.halfOpenInFlight = false;
    info.halfOpenSuccess = 0;
    this.failedClients.set(clientId, info);

    logger.error(`客户端 ${clientId} ${reason}，断路器打开 ${this.blockDuration / 1000} 秒`);

    this.clearClientQueue(clientId).then(clearedTasks => {
      const updatedInfo = this.failedClients.get(clientId);
      if (updatedInfo && updatedInfo.state === 'OPEN') {
        updatedInfo.clearedTasks = clearedTasks;
        this.failedClients.set(clientId, updatedInfo);
        logger.error(`已清理客户端 ${clientId} 积压任务 ${clearedTasks} 个`);
      }
    }).catch(clearError => {
      logger.error(`清理客户端 ${clientId} 积压任务失败:`, clearError);
    });
  }

  _closeClientCircuit(clientId) {
    this.failedClients.delete(clientId);
    logger.info(`客户端 ${clientId} 断路器关闭，恢复正常`);
  }

  // 检查客户端是否被阻塞
  isClientBlocked(clientId) {
    const clientInfo = this.failedClients.get(clientId);
    if (!clientInfo) return false;
    this._refreshClientCircuit(clientId, clientInfo);
    return !!clientInfo.blocked;
  }

  // 记录客户端失败
  recordClientFailure(clientId, error) {
    const now = Date.now();
    const clientInfo = this._getOrCreateClientInfo(clientId);

    this._refreshClientCircuit(clientId, clientInfo);

    if (clientInfo.state === 'OPEN') return;

    if (clientInfo.state === 'HALF_OPEN') {
      this._openClientCircuit(clientId, clientInfo, '半开探测失败');
      return;
    }

    clientInfo.count++;
    clientInfo.lastFailTime = now;
    clientInfo.quickFailures = clientInfo.quickFailures || [];
    clientInfo.quickFailures.push(now);

    const windowStart = now - this.circuitBreaker.quickFailWindowMs;
    clientInfo.quickFailures = clientInfo.quickFailures.filter(time => time > windowStart);

    logger.debug(`客户端 ${clientId} 失败计数: ${clientInfo.count}/${this.maxFailuresBeforeBlock}, 近2分钟快速失败: ${clientInfo.quickFailures.length} 次`);

    const shouldBlockForQuickFailures = clientInfo.quickFailures.length >= this.circuitBreaker.quickFailThreshold;

    if (clientInfo.count >= this.maxFailuresBeforeBlock || shouldBlockForQuickFailures) {
      const blockReason = shouldBlockForQuickFailures ? `2分钟内快速失败${clientInfo.quickFailures.length}次` : `连续失败${clientInfo.count}次`;
      this._openClientCircuit(clientId, clientInfo, blockReason);
    } else {
      this.failedClients.set(clientId, clientInfo);
    }
  }

  // 记录客户端成功
  recordClientSuccess(clientId) {
    const clientInfo = this.failedClients.get(clientId);
    if (!clientInfo) {
      return;
    }

    this._refreshClientCircuit(clientId, clientInfo);

    if (clientInfo.state === 'OPEN') return;

    if (clientInfo.state === 'HALF_OPEN') {
      clientInfo.halfOpenInFlight = false;
      clientInfo.halfOpenSuccess = (clientInfo.halfOpenSuccess || 0) + 1;

      if (clientInfo.halfOpenSuccess >= this.circuitBreaker.halfOpenSuccessThreshold) {
        this._closeClientCircuit(clientId);
      } else {
        this.failedClients.set(clientId, clientInfo);
      }
      return;
    }

    this.failedClients.delete(clientId);
    logger.debug(`客户端 ${clientId} 成功执行任务，清除失败记录`);
  }

  async enqueue(taskData, priority = 'normal') {
    const { clientId, action } = taskData;

    if (clientId) {
      const clientInfo = this.failedClients.get(clientId);
      if (clientInfo) {
        this._refreshClientCircuit(clientId, clientInfo);
      }

      if (clientInfo?.state === 'OPEN') {
        logger.error(`客户端 ${clientId} 断路器打开，跳过任务入队: ${taskData.action}, 失败次数: ${clientInfo?.count}, 阻塞时间: ${clientInfo?.blockedAt ? new Date(clientInfo.blockedAt).toISOString() : 'unknown'}`);
        return;
      }

      if (clientInfo?.state === 'HALF_OPEN') {
        if (!this._isCircuitProbeAction(action)) {
          logger.warn(`客户端 ${clientId} 断路器半开，仅允许探测动作，跳过任务入队: ${taskData.action}`);
          return;
        }
        if (clientInfo.halfOpenInFlight) {
          logger.warn(`客户端 ${clientId} 断路器半开探测中，跳过任务入队: ${taskData.action}`);
          return;
        }
      }
    }

    // Even overdue Tracker maintenance must not jump ahead of deletes/refreshes.
    const actualPriority = action === 'trackerSync' ? 'normal' : priority;
    if (this.foregroundActions.has(action)) {
      if (!this.foregroundPending.has(clientId)) this.foregroundPending.set(clientId, new Map());
      this.foregroundPending.get(clientId).set(action, Date.now());
      const active = this.activeClientTasks.get(clientId);
      if (active?.action === 'trackerSync') active.control?.abort('foreground-pending');
    }

    const dedupeConfig = this._getDedupeConfig(taskData);
    const queue = this._selectQueue(action);
    return queue.enqueue(taskData, actualPriority, dedupeConfig);
  }

  async getQueueStatus() {
    const fastStatus = await this.fastQueue.getQueueStatus();
    const slowStatus = await this.slowQueue.getQueueStatus();
    return {
      queueName: 'client',
      highPriority: fastStatus.highPriority + slowStatus.highPriority,
      normalPriority: fastStatus.normalPriority + slowStatus.normalPriority,
      total: fastStatus.total + slowStatus.total,
      activeWorkers: fastStatus.activeWorkers + slowStatus.activeWorkers,
      maxConcurrent: fastStatus.maxConcurrent + slowStatus.maxConcurrent,
      fast: fastStatus,
      slow: slowStatus
    };
  }

  getBlockedClientsStatus() {
    const blocked = [];
    for (const [clientId, info] of this.failedClients.entries()) {
      if (info.state === 'OPEN') {
        const remaining = Math.max(0, (info.openUntil || 0) - Date.now());
        blocked.push({
          clientId,
          failures: info.count,
          remainingTime: Math.ceil(remaining / 1000),
          clearedTasks: info.clearedTasks || 0,
          state: info.state
        });
      }
    }
    return blocked;
  }

  async clearClientQueue(clientId) {
    let clearedCount = 0;
    try {
      clearedCount += await this._clearQueueByName('client_fast', clientId);
      clearedCount += await this._clearQueueByName('client_slow', clientId);
    } catch (error) {
      logger.error(`清理客户端 ${clientId} 队列时出错:`, error);
    }
    return clearedCount;
  }

  async _clearQueueByName(queueName, clientId) {
    const highPriorityTasks = await this.removeTasksFromQueue(`vertex:queue:${queueName}:high`, clientId);
    const normalPriorityTasks = await this.removeTasksFromQueue(`vertex:queue:${queueName}:normal`, clientId);
    logger.error(`已清理客户端 ${clientId} 的积压任务(${queueName}): 高优先级 ${highPriorityTasks} 个, 普通优先级 ${normalPriorityTasks} 个`);
    return highPriorityTasks + normalPriorityTasks;
  }

  async removeTasksFromQueue(queueKey, clientId) {
    let removedCount = 0;
    try {
      const tasks = await redis.lrange(queueKey, 0, -1);
      if (!tasks || tasks.length === 0) return 0;

      for (const taskStr of tasks) {
        try {
          const task = JSON.parse(taskStr);
          if (task.data && task.data.clientId === clientId) {
            await redis.lrem(queueKey, 1, taskStr);
            if (task.dedupeKey) {
              await redis.del(task.dedupeKey);
            }
            removedCount++;
          }
        } catch (parseError) {
          continue;
        }
      }
    } catch (error) {
      logger.error(`从队列 ${queueKey} 移除任务时出错:`, error);
    }
    return removedCount;
  }

  async cleanupClientOnDestroy(clientId) {
    const timer = this.trackerContinuations.get(clientId);
    if (timer) clearTimeout(timer);
    this.trackerContinuations.delete(clientId);
    this.trackerRefreshRequired.delete(clientId);
    this.foregroundPending.delete(clientId);
    this.activeClientTasks.get(clientId)?.control?.abort('client-destroyed');
    const clientInfo = this.failedClients.get(clientId);
    if (clientInfo) {
      this.failedClients.delete(clientId);
      logger.info(`已清除客户端 ${clientId} 的阻塞状态`);
    }

    try {
      const clearedTasks = await this.clearClientQueue(clientId);
      logger.info(`已清理客户端 ${clientId} 的积压任务 ${clearedTasks} 个`);
      return clearedTasks;
    } catch (error) {
      logger.error(`清理客户端 ${clientId} 积压任务失败:`, error);
      return 0;
    }
  }

  async executeTask(task) {
    const { clientId, action, params } = task.data;
    let lockToken = null;
    let timeoutTimer = null;

    try {
      if (this.isClientBlocked(clientId)) {
        logger.warn(`客户端 ${clientId} 被阻塞，跳过任务执行: ${action}`);
        return;
      }

      const client = global.runningClient[clientId];
      if (!client) {
        logger.warn(`客户端 ${clientId} 不存在，跳过任务`);
        return;
      }

      const activeTask = this.activeClientTasks.get(clientId);
      if (activeTask) {
        // A fixed 5s retry can phase-lock forever with flashFitTime's 5s cron.
        // Maintenance uses its deduplicated 2s continuation instead, while foreground retry semantics stay unchanged.
        if (action === 'trackerSync') {
          this._continueTrackerSync(client);
          return;
        }
        if (this._isLowPriority(action)) {
          logger.debug(`客户端 ${client.alias} 正在执行 ${activeTask.action}，跳过低优先级任务: ${action}`);
          return;
        }
        logger.debug(`客户端 ${client.alias} 正在执行 ${activeTask.action}，延后任务: ${action}`);
        setTimeout(() => {
          this.enqueue(task.data, task.priority).catch(error => {
            logger.error(`延后任务重新入队失败: ${client.alias}, 动作: ${action}`, error);
          });
        }, this.busyRetryDelayMs);
        return;
      }

      const circuitInfo = this.failedClients.get(clientId);
      if (circuitInfo) {
        this._refreshClientCircuit(clientId, circuitInfo);
        if (circuitInfo.state === 'OPEN') {
          logger.warn(`客户端 ${clientId} 断路器打开，跳过任务执行: ${action}`);
          return;
        }
        if (circuitInfo.state === 'HALF_OPEN') {
          if (!this._isCircuitProbeAction(action)) {
            logger.warn(`客户端 ${clientId} 断路器半开，仅允许探测动作，跳过任务执行: ${action}`);
            return;
          }
          if (circuitInfo.halfOpenInFlight) {
            logger.warn(`客户端 ${clientId} 断路器半开探测中，跳过任务执行: ${action}`);
            return;
          }
          circuitInfo.halfOpenInFlight = true;
          this.failedClients.set(clientId, circuitInfo);
          logger.warn(`客户端 ${clientId} 断路器半开，执行探测任务: ${action}`);
        }
      }

      lockToken = { action, startTime: Date.now(), control: action === 'trackerSync' ? trackerSync.createControl() : null };
      this.activeClientTasks.set(clientId, lockToken);

      logger.debug(`开始执行客户端任务: ${client.alias}, 动作: ${action}`);
      const startTime = Date.now();

      const timeoutMs = this._getActionTimeout(action, task.data);
      const timeoutPromise = new Promise((resolve, reject) => {
        timeoutTimer = setTimeout(() => {
          lockToken.control?.abort('task-timeout');
          reject(new Error(`任务超时: ${action} (${Math.ceil(timeoutMs / 1000)}秒)`));
        }, timeoutMs);
      });

      const taskPromise = (async () => {
        switch (action) {
          case 'getMaindata':
            return await this.executeGetMaindata(client);
          case 'autoDelete':
            return await this.executeAutoDelete(client, params);
          case 'trackerSync':
            return await this.executeTrackerSync(client, lockToken.control);
          case 'autoReannounce':
            return await this.executeAutoReannounce(client);
          case 'record':
            return await this.executeRecord(client);
          case 'flashFitTime':
            return await this.executeFlashFitTime(client, params);
          default:
            throw new Error(`未知的客户端动作: ${action}`);
        }
      })();

      const result = await Promise.race([taskPromise, timeoutPromise]);

      const duration = Date.now() - startTime;
      logger.debug(`客户端任务完成: ${client.alias}, 动作: ${action}, 耗时: ${duration}ms`);

      if (this._isCircuitTrackedAction(action)) {
        this.recordClientSuccess(clientId);
      }

      return result;
    } catch (error) {
      if (this.isClientBlocked(clientId)) {
        logger.error(`客户端 ${clientId} 已被阻塞，任务已丢弃: ${task.data.action}`);
        return;
      }

      if (this._isCircuitTrackedAction(action)) {
        this.recordClientFailure(clientId, error);
      }
      logger.error(`客户端任务执行失败: ${clientId}, 动作: ${task.data.action}`, error);

      if (this.isClientBlocked(clientId)) {
        logger.error(`客户端 ${clientId} 因连续失败已被阻塞，任务已丢弃: ${task.data.action}`);
        return;
      }

      const client = global.runningClient[clientId];
      if (client && action !== 'trackerSync') {
        client.errorCount++;
        client.status = false;
      }

      logger.error(`客户端任务失败已处理: ${clientId}, 继续处理下一个任务`);
    } finally {
      if (timeoutTimer) clearTimeout(timeoutTimer);
      lockToken?.control?.abort('slice-ended');
      if (lockToken && this.activeClientTasks.get(clientId) === lockToken) this.activeClientTasks.delete(clientId);
      if (lockToken && this.foregroundActions.has(action)) {
        this.foregroundPending.get(clientId)?.delete(action);
        if (!this.foregroundPending.get(clientId)?.size) this.foregroundPending.delete(clientId);
      }
    }
  }

  async executeGetMaindata(client) {
    const requestStartedAt = Date.now();
    try {
      const res = await client.client.getMaindata(client.clientUrl, client.cookie);

      if (typeof res === 'string') {
        if (res === 'Unauthorized') {
          await client.login();
          return;
        } else {
          client.cookie.sessionId = res;
          return await client.client.getMaindata(client.clientUrl, client.cookie);
        }
      }

      if (res.torrents) {
        const statusLeeching = ['downloading', 'stalledDL', 'Downloading'];
        const statusSeeding = ['uploading', 'stalledUP', 'Seeding'];

        client.maindata = res;
        client.maindata.clientId = client.id;
        client.maindata.clientAlias = client.alias;
        client.maindata.leechingCount = 0;
        client.maindata.seedingCount = 0;
        client.maindata.usedSpace = 0;

        client.maindata.torrents.forEach((item) => {
          item.trackerStatus = client.trackerStatus[item.hash] || '';
          client.maindata.usedSpace += item.completed;
          if (statusLeeching.indexOf(item.state) !== -1) {
            client.maindata.leechingCount += 1;
          } else if (statusSeeding.indexOf(item.state) !== -1) {
            client.maindata.seedingCount += 1;
          }
        });

        client.avgDownloadSpeed = res.downloadSpeed * 0.1 + (client.avgDownloadSpeed || 0) * 0.9;
        client.avgUploadSpeed = res.uploadSpeed * 0.1 + (client.avgUploadSpeed || 0) * 0.9;

        client.lastMaindataRequestStartedAt = requestStartedAt;
        client.lastMaindataSuccessTime = Math.floor(Date.now() / 1000);
        client.status = true;
        client.errorCount = 0;
      } else {
        logger.warn(`客户端 ${client.alias} 返回的数据格式异常`);
      }

      return res;
    } catch (error) {
      client.errorCount++;
      client.status = false;
      client.maindata = null;
      logger.error(`客户端 ${client.alias} 获取数据失败:`, error);
      throw error;
    }
  }

  async executeAutoDelete(client, params) {
    if (!client.autoDelete) {
      return;
    }

    logger.debug(`执行自动删种: ${client.alias}`);
    return await client.autoDelete();
  }

  async executeTrackerSync(client, signal) {
    // client.client is the adapter module (qb.js), which has no type property.
    // Use the configured downloader type, as Client's inline path did before this refactor.
    if (client._client?.type !== 'qBittorrent') return;
    const blocked = trackerSync.blockReason(client);
    if (blocked) {
      logger.debug(`Tracker同步让出: ${client.alias}, 原因: ${blocked}`);
      if (blocked === 'snapshot-stale') await this.enqueue({ clientId: client.id, action: 'getMaindata' }, 'high');
      return;
    }
    if (this._hasForegroundPending(client.id)) {
      this._continueTrackerSync(client);
      return;
    }
    const refreshAfter = this.trackerRefreshRequired.get(client.id);
    if (refreshAfter !== undefined && client.lastMaindataSuccessTime <= refreshAfter) {
      await this.enqueue({ clientId: client.id, action: 'getMaindata' }, 'high');
      this._continueTrackerSync(client);
      return;
    }
    this.trackerRefreshRequired.delete(client.id);
    const result = await client.trackerSync({ signal, shouldYield: () => this._hasForegroundPending(client.id) });
    if (signal?.aborted && signal.reason !== 'foreground-pending') return result;
    if (global.runningClient?.[client.id] === client && result?.remaining > 0) {
      this.trackerRefreshRequired.set(client.id, client.lastMaindataSuccessTime);
      this._continueTrackerSync(client);
    }
    return result;
  }

  async executeAutoReannounce(client) {
    if (client.client.type !== 'qBittorrent' || !client.autoReannounce) {
      return;
    }

    logger.debug(`执行自动重新汇报: ${client.alias}`);
    return await client.autoReannounce();
  }

  async executeRecord(client) {
    logger.debug(`执行记录任务: ${client.alias}`);
    return await client.record();
  }

  async executeFlashFitTime(client, params) {
    if (!params || !params.rule) {
      logger.warn(`FlashFitTime任务缺少规则参数: ${client.alias}`);
      return;
    }

    logger.debug(`执行适配时间刷新: ${client.alias}, 规则: ${params.rule.alias}`);
    return await client.flashFitTime(params.rule);
  }

  handleTorrentStatusChanges(client, oldTorrents, newTorrents) {
    try {
      const oldTorrentMap = new Map();
      oldTorrents.forEach(torrent => {
        oldTorrentMap.set(torrent.hash, torrent);
      });

      newTorrents.forEach(newTorrent => {
        const oldTorrent = oldTorrentMap.get(newTorrent.hash);
        if (oldTorrent && oldTorrent.state !== newTorrent.state) {
          logger.debug(`种子状态变化: ${newTorrent.name}, ${oldTorrent.state} -> ${newTorrent.state}`);
        }
      });
    } catch (error) {
      logger.error(`处理种子状态变化时出错: ${client.alias}`, error);
    }
  }
}

module.exports = ClientTaskQueue;
