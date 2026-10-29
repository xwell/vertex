# Tracker 同步分片与前台任务优先（2026-10-29）

## 背景

BHS 在 10-29 14:53～14:58 出现约 218 秒陈旧快照，独立 qB 采集仍持续成功；期间 Tracker 同步有 ETIMEDOUT，随后记录 49/50 个成员完成。原 Tracker 同步串行等待最多 50 个请求、共用客户端互斥锁，单请求默认超时 120 秒；自动删种还会内联 await 整批同步。毫秒级删种函数不能解决这一分钟级执行停顿。

## 改动

- **删种不再内联等待 Tracker HTTP 批次**，改为调度后台维护。Tracker 状态仍由成功结果缓存及主数据刷新/删种回填；旧站点规则使用上次可用结果，新结果不承诺在同一轮立即出现。
- 修正后台队列的类型判断：读取 `client._client.type`，不是没有 type 属性的 `client.client`（qB 适配模块）。原定时 Tracker 同步会被这处旧判断跳过，过去主要依靠删种中的内联调用；新版本恢复真正的后台维护。
- 单个 Tracker HTTP 请求上限 **3 秒**；`request-with-deadline.js` 到期或取消时会 **abort 实际请求**，不是只让 Promise.race 返回。该限制只由 qB Tracker API 显式使用，普通 qB 操作、RSS 和站点请求超时不变。Tracker JSON API 禁用 Puppeteer 回退。
- 每片最多 **5 个成员 / 5 秒**，每批仍最多 50 个；时间到、请求错误或前台任务到达后让出，游标只推进实际处理的成员，不提前跨过整批。
- 后续分片隔 2 秒正常优先级入队，且必须先看到**更新一次的成功主数据快照**。等待新快照或发现快照陈旧时，会按需调度高优先级主数据刷新，因此维护期间可能增加刷新请求；主数据 Cron 本身不变。每客户端最多一个续片 timer；整批 180 秒过期让出，保留实际推进的游标。没有修改 5 分钟 Tracker Cron，也没有新增常驻轮询器。
- Tracker 遇到同客户端忙时也使用上述去重 2 秒续片，不套用前台任务的 5 秒忙重试，避免与每 5 秒 fitTime 调度长期同相碰撞。
- Tracker 不再因长期未同步提升高优先级。`getMaindata`、`autoDelete`、`flashFitTime` 入队时标记前台待办，正在执行的 Tracker 请求立即取消；前台待办期间不启动新的 Tracker 片。
- 快照未知/超过 30 秒/未来时间、客户端离线、空闲未知时不做 Tracker 维护。空间余量不足 **client.minFreeSpace + 当前下载速率×120 秒**时让出；这只是维护调度保留量，不改任何删种水位或候选资格。
- Tracker 任务总 watchdog 从 300 秒变成 **10 秒**，兜底约束 Redis/SQLite 等慢依赖。触发时取消片控制器；所有 await 后复核取消/时间/前台待办，不会在旧任务后台恢复后继续发请求或写新 Tracker 状态。
- 保留同客户端互斥，不把刷新、删除与 Tracker 并发放开。释放锁核对具体任务 token；任务结束清理超时 timer。Tracker 维护错误不将整个 qB 标为离线，也不计作主数据断路器失败。
- 销毁客户端时取消 Tracker HTTP/续片 timer，避免旧实例继续入队。

分片正常结束 INFO 日志包含 synced/visited、剩余数、耗时及让出原因；请求错误通过 `reason=request-error` 片末记录计数。WARN 详情只记录 hash 和错误码，不写请求对象、Cookie、Tracker URL 或响应正文。当前 logger 配置没有 WARN 文件 sink，不能把缺少详情当作请求零失败；运维监控据 INFO error-yield 计数。批请求“检查过”不等于成功，只有实际收到并解析成功才更新 `lastTrackerSyncTime`。

## 边界

- 此为共享 Vertex 客户端代码，影响所有 qB 客户端的 Tracker 维护调度；不是仅 BHS 的删种策略部署。
- 维护主动让出可能令部分 Tracker 状态更新更慢，尤其长期高下载/低空间期间。旧站点错误规则可能晚发现失效种，但不能为状态更新阻塞主数据和删种数分钟。
- 保留原有规则、RSS、Cron、限速/暂停逻辑与冷却。不改 BHS v12、HDD observe 或其他下载器绑定。
- 普通刷新/删除既有 watchdog 及底层请求取消语义不在此次全面改造范围内；只为 Tracker 链路新增可取消控制器。
- 加载代码需受控重启 Vertex；qB 不重启。Vertex 内存 EMA、确认窗口及六小时零进度证据会重新预热，不能把重启后短窗与重启前连续证据混算。本次上线验收中又发现忙重试相位及旧类型判断两个集成边界，追加修正/受控加载；各次时刻及最终状态如实记录在运维报告，不把“代码已复制”当作分片已运行。

## 测试

```bash
npm run test:client
npm run test:rss
```

新增测试隔离真实 Redis/SQLite/qB，覆盖分片预算/游标、低空间/缺测、慢请求、取消、旧任务晚到、前台优先、互斥、续片去重、新快照约束及销毁，另组合真实 Client 方法、无 type 属性的真实 qB 模块及队列，避免 mock 属性与生产实例不一致。忙重试测试覆盖 5 秒 fitTime 相位碰撞；本地 loopback HTTP 验证到期和取消确实断开 socket，不向真实下载器发送请求。构建工作流同时执行 RSS 和客户端测试。另在生产 Node v14.20.0 上用最小 test-runner 运行相同隔离测试，不依赖该版本不存在的 node:test。

## 部署与回滚

先核对运行代码哈希及全部 client/规则配置，保留原四文件的精确备份并将新增两个 helper 一并部署，生产 Node14 隔离验证后在 BHS 无压力、无在途动作的安全窗口复制代码并受控重启。共享 helper 为本次新增，回滚时先恢复旧 `Client.js`、`ClientTaskQueue.js`、`qb.js`、`util.js` 再重启；新增 helper 可保留但不会被旧代码引用。禁止回滚业务数据库或修改删种规则来“配合”代码回退。

实际部署时刻、版本哈希、重启、在线监测与回滚命令见外部运维记录 `vertex_scripts/BUFFER_POLICY_REPLAY_TRACKER_FIX_20261029.md`。
