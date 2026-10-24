const { TaskQueue } = require('../redis');
const logger = require('../logger');

class RssTaskQueue extends TaskQueue {
  constructor() {
    super('rss_fetch', {
      maxConcurrent: 10 // 最多同时10个RSS请求，移除重试配置
    });

    this.actionTimeoutMs = {
      fetchRss: 300000,
      clearCount: 10000
    };
    
    // RSS源故障管理
    this.failedRssSources = new Map(); // rssId -> { count, lastFailTime, blocked }
    this.blockDuration = 10 * 60 * 1000; // RSS源阻塞10分钟（比客户端长，给更多恢复时间）
    this.maxFailuresBeforeBlock = 5; // 5次连续失败后阻塞（更谨慎，避免因偶发流控阻塞）
  }

  // 检查RSS源是否被阻塞
  isRssBlocked(rssId) {
    const rssInfo = this.failedRssSources.get(rssId);
    if (!rssInfo || !rssInfo.blocked) {
      return false;
    }
    
    // 检查阻塞是否过期
    if (Date.now() - rssInfo.lastFailTime > this.blockDuration) {
      // 重置RSS源状态
      this.failedRssSources.delete(rssId);
      logger.info(`RSS源 ${rssId} 阻塞已解除，重新开始处理任务`);
      return false;
    }
    
    return true;
  }

  // 记录RSS源失败（修复版：确保阻塞状态立即生效）
  recordRssFailure(rssId, error) {
    const now = Date.now();
    const rssInfo = this.failedRssSources.get(rssId) || { count: 0, lastFailTime: 0, blocked: false };
    
    // 如果已经被阻塞，不再处理
    if (rssInfo.blocked) {
      return;
    }
    
    // 任何类型的错误都计数（根据用户建议移除网络错误区分）
    rssInfo.count++;
    rssInfo.lastFailTime = now;
    
    logger.debug(`RSS源 ${rssId} 失败计数: ${rssInfo.count}/${this.maxFailuresBeforeBlock}`);
    
    // 达到阈值则立即阻塞
    if (rssInfo.count >= this.maxFailuresBeforeBlock) {
      rssInfo.blocked = true;
      rssInfo.blockedAt = now;
      
      // 立即保存阻塞状态
      this.failedRssSources.set(rssId, rssInfo);
      
      logger.error(`RSS源 ${rssId} 连续失败 ${rssInfo.count} 次，立即阻塞 ${this.blockDuration/1000} 秒`);
    } else {
      // 未达到阈值，正常保存计数
      this.failedRssSources.set(rssId, rssInfo);
    }
  }

  // 记录RSS源成功（修复版：保留阻塞状态）
  recordRssSuccess(rssId) {
    const rssInfo = this.failedRssSources.get(rssId);
    if (!rssInfo) {
      return; // 没有失败记录，无需处理
    }
    
    // 如果RSS源被阻塞，不清除记录，只重置计数
    if (rssInfo.blocked) {
      rssInfo.count = 0; // 重置失败计数，但保持阻塞状态
      this.failedRssSources.set(rssId, rssInfo);
      logger.debug(`RSS源 ${rssId} 成功执行任务，重置失败计数，但保持阻塞状态`);
    } else {
      // 未被阻塞，清除失败记录
      this.failedRssSources.delete(rssId);
      logger.debug(`RSS源 ${rssId} 成功执行任务，清除失败记录`);
    }
  }

  // 判断是否为网络错误
  isNetworkError(error) {
    if (!error) return false;
    const networkErrors = ['ESOCKETTIMEDOUT', 'ECONNREFUSED', 'ECONNRESET', 'ENOTFOUND', 'ETIMEDOUT', 'EHOSTUNREACH'];
    return networkErrors.includes(error.code) || 
           (error.message && networkErrors.some(code => error.message.includes(code))) ||
           (error.message && error.message.includes('getaddrinfo')) || // DNS解析失败
           (error.message && error.message.includes('timeout')); // 各类超时错误
  }

  // 重写enqueue方法，添加阻塞检查
  _getActionTimeout(action, taskData) {
    if (taskData && Number.isFinite(taskData.timeoutMs) && taskData.timeoutMs > 0) {
      return taskData.timeoutMs;
    }
    return this.actionTimeoutMs[action] || 30000;
  }

  _getDedupeConfig(taskData) {
    const { rssId, action } = taskData || {};
    if (!rssId || !action) {
      return {};
    }
    const key = `vertex:queue:dedupe:rss:${rssId}:${action}`;
    const timeoutMs = this._getActionTimeout(action, taskData);
    const ttlSeconds = Math.ceil(timeoutMs / 1000) + 60;
    return { dedupeKey: key, dedupeTtlSeconds: ttlSeconds };
  }

  async enqueue(taskData, priority = 'normal') {
    const { rssId } = taskData;
    
    // 检查RSS源是否被阻塞
    if (rssId && this.isRssBlocked(rssId)) {
      logger.warn(`RSS源 ${rssId} 被阻塞，跳过任务入队: ${taskData.action}`);
      return;
    }

    const dedupeConfig = this._getDedupeConfig(taskData);
    return super.enqueue(taskData, priority, dedupeConfig);
  }

  // 获取阻塞状态信息
  getBlockedRssStatus() {
    const blocked = [];
    for (const [rssId, info] of this.failedRssSources.entries()) {
      if (info.blocked) {
        const remaining = Math.max(0, this.blockDuration - (Date.now() - info.lastFailTime));
        blocked.push({
          rssId,
          failures: info.count,
          remainingTime: Math.ceil(remaining / 1000)
        });
      }
    }
    return blocked;
  }

  // 清理RSS源销毁时的状态
  cleanupRssOnDestroy(rssId) {
    // 清除失败记录和阻塞状态
    const rssInfo = this.failedRssSources.get(rssId);
    if (rssInfo) {
      this.failedRssSources.delete(rssId);
      logger.info(`已清除RSS源 ${rssId} 的阻塞状态`);
    }
  }

  async executeTask(task) {
    const { rssId, action, params } = task.data;
    let timeoutId;

    try {
      // 再次检查阻塞状态
      if (this.isRssBlocked(rssId)) {
        logger.debug(`RSS源 ${rssId} 被阻塞，跳过任务执行`);
        return;
      }
      
      const rssInstance = global.runningRss[rssId];
      if (!rssInstance) {
        logger.warn(`RSS实例 ${rssId} 不存在，跳过任务`);
        return;
      }

      const startTime = Date.now();
      const queueWaitMs = Number.isFinite(task.timestamp) ? Math.max(0, startTime - task.timestamp) : 0;
      const message = `开始执行RSS任务: ${rssInstance.alias}, 动作: ${action}, id=${task.id}, 排队耗时=${queueWaitMs}ms`;
      if (queueWaitMs >= 1000) {
        logger.info(message);
      } else {
        logger.debug(message);
      }

      const timeoutMs = this._getActionTimeout(action, task.data);
      const taskPromise = (async () => {
        switch (action) {
          case 'fetchRss':
            return await this.executeFetchRss(rssInstance, params);
          case 'clearCount':
            return await this.executeClearCount(rssInstance);
          default:
            throw new Error(`未知的RSS动作: ${action}`);
        }
      })();

      const timeoutPromise = new Promise((resolve, reject) => {
        timeoutId = setTimeout(() => {
          reject(new Error(`任务超时: ${action} (${Math.ceil(timeoutMs / 1000)}秒)`));
        }, timeoutMs);
      });

      const result = await Promise.race([taskPromise, timeoutPromise]);

      const duration = Date.now() - startTime;
      logger.debug(`RSS任务完成: ${rssInstance.alias}, id=${task.id}, 排队耗时=${queueWaitMs}ms, 执行耗时=${duration}ms`);

      // 清零计数及“已在运行”的跳过不代表抓取成功，不能清除源的失败记录。
      if (action === 'fetchRss' && result === true) {
        this.recordRssSuccess(rssId);
      }
      
      return result;
    } catch (error) {
      // 先检查是否已阻塞
      if (this.isRssBlocked(rssId)) {
        logger.error(`RSS源 ${rssId} 已被阻塞，任务已丢弃: ${action}`);
        return;
      }
      
      // 只有实际抓取失败才计入 RSS 源的连续失败次数。
      if (action === 'fetchRss') {
        this.recordRssFailure(rssId, error);
      }
      
      logger.error(`RSS任务执行失败: ${rssId}`, error);
      
      // 再次检查阻塞状态（可能在recordRssFailure中触发阻塞）
      if (this.isRssBlocked(rssId)) {
        logger.error(`RSS源 ${rssId} 因连续失败已被阻塞，任务已丢弃: ${action}`);
        return;
      }
      
      // 无重试，直接丢弃失败任务
      this.logTaskFailure(task, error);
      
      // 不再重新抛出错误，让任务队列继续处理下一个任务
      logger.error(`RSS任务失败已处理: ${rssId}, 继续处理下一个任务`);
    } finally {
      // 正常完成也要释放计时器，避免每轮都残留一个五分钟的定时器。
      clearTimeout(timeoutId);
    }
  }

  async executeFetchRss(rssInstance, params) {
    // 检查是否已在运行
    if (rssInstance.isRunning) {
      logger.debug(`RSS ${rssInstance.alias} 已在运行中，跳过本次执行`);
      return false;
    }

    rssInstance.isRunning = true;
    
    try {
      // 直接调用原始的rss方法，利用其内置的去重逻辑
      await rssInstance.rss(params?.torrents);
      return true;
    } finally {
      rssInstance.isRunning = false;
    }
  }

  async executeClearCount(rssInstance) {
    rssInstance.addCount = 0;
    logger.debug(`RSS ${rssInstance.alias} 计数已清零`);
  }
}

module.exports = RssTaskQueue;
