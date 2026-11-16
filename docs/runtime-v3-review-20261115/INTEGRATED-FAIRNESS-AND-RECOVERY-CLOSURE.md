# 集成版公平运行、恢复与元数据验收（2026-11-16）

这次关闭的是原预算下真实共享分支无法完成的问题，同时验证元数据只读不再触发业务。产品 root 仍要求能看懂、配置、运行真实计划并分析评测；引擎 root 仍要求同一 Dataset 的正确、有限资源、高性能计算。本文不把阶段通过当成全部意图完成。

## 集成版实测

| 固定源码 | 实际执行 | 结果 |
| --- | --- | --- |
| `f67daa91da944dd43f3f09607b3897079facfeaa` | 正式 Ray Core V012：原共享双分支、热点键、磁盘不足 | 1 passed，139.29 秒，0 skipped |
| `c835adaa32c003a26e8a7e486446a74fb3bc03d3` | Local 正式 V012/V017，加回收、元数据只读、封存、环境投影、空闲通知与时钟控制 | 122 passed，272.02 秒，0 skipped |

原 64 行、每行 4 KiB、双分支 128 行、1 worker、16 MiB、120 秒限制保持。Ray 该场景 89.088 秒完成，Local 37.377 秒完成。热点键仍使用原 16,384 行、每行 4 KiB，产出 128 组；原 8 MiB 小磁盘仍以 `artifact_staging_bytes` 明确拒绝，不能产生成功 final ACK。

修复仅让恢复阶段在第一次实际业务启动前开启原 checkpoint 时钟，并使 worker 空闲通知携带当前活动代次；过期空闲通知不再把刚恢复的来源停车。恢复、credit、ACK、工件与资源权威继续复用原协议。Ray 原分配 36+3+3、Local 22+3+3 均 CONFIRMED，任务子进程全部退出。

`schema(fetch_if_missing=False)` 与 `columns(fetch_if_missing=False)` 在原 executor 元数据入口返回，不再进入 action/业务执行。真实 UDF、HTTP、action、cut 均为零；之后明确读取数据仍执行原业务并提交。不是新增缓存或独立执行路径。

## 路由恢复证据与范围

owner 固定 `17a93bc69cfaac8215d263710d61dd102b6f5e69`：Local 16 passed/152.193 秒，Ray Core 正式 V017 1 passed/176.022 秒。六个真实 worker 终止与恢复场景、共享前缀不重算、未知路由拒绝、封存 code-pin 与临时模型环境投影均有实际证据。根节点 Local 集成复验已经覆盖这些修复；集成版 Ray V017 另行执行，未用 owner 版本冒充最终同版本通过。

目标/广播/hash/rebalance 的原生首次封存协议与公开 Dataset 的路由 Prompt 证据分别记录；不声称新增公开路由 DSL 或已覆盖两机全部路由故障矩阵。owner 环境回执只实际设置 RESOURCE_ROOT/HOME，FORGE_DOMAIN 未设置；该组没有 Forge 调用，已明确更正，不能把缩写命令当三域隔离证据。

## 源码绑定和保护

[独立复核回执](INTEGRATED-FAIRNESS-AND-RECOVERY-VERIFICATION.json) 保存原始结果路径、摘要和逐项比对。根节点两组 1873/1875 项输入分别与固定提交全等；原附件及 23/48 份公平运行归档逐字核验。owner 两组各 2188 项输入及空闲通知专项的四组输入均核验，无摘要不符。

主仓 HEAD `e40a6dd3dd756aba423dca4c7d995ada0a4bff65`、原 dirty 状态及 18 个文件摘要不变；七个冻结源码字节不变。此证明仅覆盖本次边界，不抹去 CURRENT-STATUS 已披露的早期测试边界事故。

## 尚未完成

旧原生入口的 execution/sink_schema 预算与类型语义、其余历史失败、最终同版本产品/API 与完整性能验收继续推进。GC 预检候选只证明功能与原预算负例保持，未证明整链提速；原纯 CPU 性能与历史基准仍有明显差距，不宣称高性能或两大 root 已全部实现。
