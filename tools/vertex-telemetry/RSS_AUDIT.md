# RSS 分配审计升级

这是第一阶段：记录实际分配输入/结果，不修改算法、规则、qB 限速或删种行为，不扩大持续 qB 采样目标。

## 能看到什么

- 全部 RSS 与全部相关下载器的决策（不只 BHS/HDD）：RSS ID、种子 hash、大小/发现/发布时间、最终下载器及 lease/decisionId。
- 每台候选的在线/快照新鲜度、容量、速度/EMA、同机聚合、下载数与预留、空间/保底、I/O、权重及轮转信用。
- 标准化排除原因；强制定向时未参与的普通下载器明确标为 `forced-not-considered`。
- 决策与真正发 POST 前的最终校验快照分开；accepted、uncertain、取消、过滤和无目标拒绝分别统计。没有结果时保持 missing-or-in-flight，不自动记失败。
- 免费/HR/接受规则准备阶段过滤也记录；更早的最长休眠/每小时额度/抓取失败/冻结等不承诺完整审计覆盖，它们沿用原 Vertex 记录，不属于自由分配机会。

Vertex 输出 `RSS_ALLOCATION_EVENT` v2，独立监控只消费日志，不事后查询 qB 来伪造决策输入。每个事件最多 256 个候选/512 KiB，截断有明确标记。日志只含精简数值与 ID/指纹，无 URL/passkey/Cookie/密码/保存路径/种子名/完整规则代码/任务列表。

## 数据与查询

- `rss_events`：按 decision/lease+stage 唯一去重，保存精简索引与原始脱敏载荷。
- `rss_minutes`：按分钟、RSS、下载器、路由、模式、阶段/结果/原因/质量聚合。
- `rss_names`：从现有配置读取经过限长/敏感过滤的显示别名，不保存连接配置。
- `health.json.rss_allocation`：最后事件/完整决策时间、入库和异常格式计数。
- `reports/latest.html` / `latest.json`：新增 RSS 分配审计，列出分组总量、逐小时结果、候选排除、轮转曝光和最近 20 条完整明细。

```bash
# 以实际配置文件为准；查询全部是只读
python3 /root/devs/vertex_scripts/telemetry/rss_report.py --rss-id <RSS_ID>
python3 /root/devs/vertex_scripts/telemetry/rss_report.py --hash <TORRENT_HASH>
python3 /root/devs/vertex_scripts/telemetry/rss_report.py --decision <DECISION_ID_OR_LEASE>
```

在 payload 的 `candidates` 中查看各相关下载器状态；`submitted` 的 `finalInput` 是发送前复查，不能与最初的 decision 快照混淆。精简索引保留而原始明细已清理时，payload 为 null，不生成虚假的空矩阵。

## 旧日志补采

旧版 `RSS分配` / `RSS添加结果` 仅有选中目标，标记 `legacy-selected-only`，不推断 RSS/hash/未选状态。新旧同一 lease/stage 只保留一次，v2 可升级旧条目并修正聚合。

回填只能在停止 collector 并成功持有相同单实例锁时运行；不回退主 log_offsets，不影响已有删种 true/动作关联。支持仍保留的普通/压缩轮换日志，默认解压后 128 MiB/60 秒预算，未完成明确标为 partial。

```bash
systemctl stop vertex-telemetry.service
python3 /root/devs/vertex_scripts/telemetry/rss_backfill.py --since <UNIX_SECONDS>
# 无论回填是否完成，都应恢复采集；不得再运行第二个 collector --once
systemctl start vertex-telemetry.service
```

## 存储与解释边界

原始 payload 沿用 7 天/磁盘压力裁剪，并额外设默认 128 MiB 的 RSS 原始载荷预算；去重头最多 250000 条，分钟统计按 90 天保留，仍服从全局 2 GiB/磁盘保护。容量压力可能缩短明细保留，不能保证永远能查到所有原始输入。

报表总量按分钟聚合，起点向下取整；候选原因/曝光仅扫描仍保留的最近最多 20000 条决策，显示截断口径。自由/forced/辅种分开，过滤/无目标不是轮转机会。曝光表只取完整且已提交的自由决策；平滑轮转不是每次选最大权重，短窗偏差还受起止信用、候选变化与退还影响，不自动判定算法不合理。

accepted 是添加 API 正向回应，不等于独立 qB 创建确认。当前不新增 RBX/VPS 流量采样或上传收益关联；不能声称某个种子换到另一台必然更赚钱。

## 部署与回滚

新增 SQLite 表是兼容性扩展，不删改旧表。只重启独立采集器加载模块，保留数据库、偏移、生命周期和旧策略证据；Vertex 部分需发布新日志协议代码并短暂重启应用。定时 report/replay 任务原样保留，报告按次加载新模块。回滚仅恢复原采集/报告 Python 模块，旧代码忽略新增 RSS 表，不回退或删除已经采集的业务数据。

可追踪源码位于 Vertex Git 仓库 `tools/vertex-telemetry/`，此目录保存本次涉及的核心采集/报表模块及自包含 RSS/collector 测试。生产安装路径仍是 `/root/devs/vertex_scripts/telemetry/`；升级时复制这些核心文件，不删除原有 replay、策略分析测试或服务文件。所有真实配置、数据库、凭据、报告及状态均不得提交 Git。
