# 离线模型与真实 worker 使用量：实施记录

本轮继续在隔离分支实施，不修改主仓。本记录是局部通过证据，不是全部意图完成声明。

## 实际缺口和修复

统一执行给 Prompt/Agent 注入 RemotePromptJournal 后，原 `offline_dir` / `offline_store` 在 client 工厂被拒；这破坏了公开 API 的离线请求导出、外部回填、原 schema 校验与多轮 Agent 能力。另 `DataAPI.prompt_usage()` 仍遍历 driver 上未执行的 coordinator，独立 worker 的实际调用计量缺失。

新增窄 `NativeJournalCommand.operation=offline_lookup`。worker 只传原 `request_record`，driver 从原封存 PromptFactory 读取唯一 options/model，不接受 RPC 中指定路径或覆盖配置。driver 复用原离线客户端；外部 SQLite/文件保存的是用户声明的请求/响应输入，不承担运行进度、预算、效果、提交或恢复权威。原 RunJournal/CommitCoordinator 仍唯一。离线 lookup 既不申请付费额度也不退化为 HTTP cache miss，pending 保持原错误与请求引用。正常 HTTP 仍走原 native journal。

请求经现有有界 artifact/RPC 传递。离线响应文件按上限读取；SQLite 以一个 SQL statement 同时判大小与取值，超过上限不先把完整响应搬入 Python。返回值也受控制记录预算约束。被验证的声明缓存至多为当前封存图节点数，不在 driver 构造模型 actor。外部离线库自身的原磁盘策略保持，未把这些文件伪装成运行 artifact 的磁盘计费。

worker 原 coordinator 的使用量随现有 progress 观测传 driver，按 occurrence/partition/boot 校验单调并累计，重发不重复计数。API 读取观测投影，原预算事务完全不变；新 worker boot 只累计其新工作。固定八个累计计数，不建立每请求第二账本。它是已收到的运行观测，进程骤死尚未送达的计量不等于付费效果权威；后者须看原 journal，不能据零观测断言请求未发生。

## 保留的业务断言与测试迁移

- 三条原 Agent 离线测试未改断言：含图像的原上下文、三类坏 JSON、模型修复、原生 read_documents、多轮回填、重复读取不发 HTTP 全通过。
- 原 `test_prompt_offline` 的父进程 list 回调收集改成真实 Dataset.take_all，保留异步→同步→异步链及结果断言。
- 已在 API-MATRIX 明确退役的显式 checkpoint 改用原生 write_lance 提交边界验同一业务语义；保留同步参数绑定、原 ValueError、失败无结果发布，未恢复第二套 checkpoint 协议。
- 新增真实两种离线存储、累积在线 worker 使用量、响应字节上限、端口能力、同一观测重发与新 boot 的反例。

## 运行证据（分批，不能相加冒充单次全量）

- `/tmp/demiflow-offline-r1.xml`：三条原 Agent 业务链已走通，最终使用量字段缺失，3 failed；促成第二个实际修复。
- `/tmp/demiflow-offline-r2.xml`：9 passed，含原三条 Agent 离线业务、两种存储、在线使用量。
- `/tmp/demiflow-offline-r3.xml`：22 passed / 1 failed。唯一失败为历史测试调用已退役 checkpoint，非离线功能失败。
- `/tmp/demiflow-offline-r4.xml`：1 passed / 1 failed。验证迁移后的原生 write_lance 继续传原 ValueError；测试暂误用了 DataflowError，已恢复原 ValueError 断言。
- `/tmp/demiflow-offline-r5.xml`：最后原生写出/失败不发布用例 1 passed。

后续仍需合流真实 durable 调度、RPC 丢回包与全部恢复/产品回归；本记录不宣称跨主机或物理 GPU 已验。
