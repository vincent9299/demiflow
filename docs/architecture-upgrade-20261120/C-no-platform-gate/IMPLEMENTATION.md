# 去平台运行门禁：实现交接

本次真实用户授权取消平台源码身份一致性对运行和恢复的限制。没有实施 S2/H1 或通用兼容框架；旧来源记录保留，不重写 run digest、seal、queue scope/commit/ACK 或验收签名。

- 起点：main `122edf4bd0c827338846abd0114f0b36cf8c1b6d`。
- 唯一实现依赖：已审 P12 `1423d2bf22f78a26e7b5ae6263ae434a61b62926`，本树 merge `9c2bcb8b0d2837dde5be9c804ce097b2add909c7`。
- 设计固定：`7994f6832c57a8947a49dc0b948759d4a65e5ba4`；父审裁定 `reviews/C-no-platform-gate-design-v1/PARENT-DECISION.txt` SHA-256 `1b8cace96c2ca754845125093a93b16cf7920664c7a42a8e4e54b526fd87e1c4`。
- 产品固定：`a0d59d74a88baaf1edf91b1da52e6b8f40c919f6`。最终测试及文档提交由共享 HANDOFF.json 固定，后续两者不改变产品内容。

## 已实现

原生 RunContext、已有 seal 编译、worker program 装载、checkpoint 恢复及共享存储域验证不再比较当前平台来源。新 run 仍只进行一次来源扫描，恢复保留原 configuration。原生及工作台 resume 在任何 attempt/清理/索引发布前严格验证持久化 `format=1`（严格 int）与 `aligned-barrier-v2`；未知格式具体拒绝。

新 producer/consumer 使用不含平台的 v3 业务合同。旧 v2 只读处理：原 RunRef 的真实来源可以重建原合同并核对原 hash；跨新 Run 的 consumer 从同 generation 的原 task acquisition scope 获取完整声明，验证 scope hash、task/pool/queue/generation/fingerprint 后比较业务投影。旧 producer 只有混合 hash、或旧 consumer 没有完整 scope 且不能由原 RunRef 重建时，按缺少原始业务材料具体拒绝。不会重签、重新绑定或把缺材料归咎于平台不同。旧 scope SQL 在数据库端投影至最多 65537 bytes，超限/非 text 在解码前拒绝。

Forge 使用当前平台 snapshot 进行新检查/准备，但旧 workspace snapshot 只记录来源。原 workspace policy、native manifest workspace 字节摘要先验证，再排除唯一平台字段比较实际业务。原封存 runtime 文件继续验证精确摘要，环境检查保留 Python/第三方/extras 等真实内容，移除 platform_distribution 当前版本比较。平台漂移单列 provenance，不制造 stale/incomplete。

`require` 使用独立只读 runtime_acceptance 视图。原受保护任务 binding components、或原签名 registration/implementation 链提供独立配置证据；配置、源、验证命令/log、原 review 与 developer/verifier 签名、最新用户 turn、部署 policy/root、准备证书全部继续校验。原开发 status 仍报告工具变化。平台自有上下文/readonly/supplemental 来源可复用原认证值，任何 project.root 内业务路径始终实读。注册 workspace 的原签名 raw SHA 与所有非 platform 字段先复核，再重建原 fingerprint；原状态和签名不写回。只有混合 hash 且缺独立配置材料的历史任务具体拒绝。

P4 只新增 `RunContext.validate_recovery_identity` 这一个纯校验 member，复用既有 control 导出；不开放其他 RunContext 状态。删去已无真实工作台消费者的 inspection.platform_identity。运行时名单与生成的 JSON 同步。

## 冻结与合流

仅获准 queue_execution.py 四处消费点及其 FROZEN 基准改变：`2a9851ce7df32da80906a859ef61080ea7dc5535d543a68953ab6cf71666a7c2` → `7d962b68a75baaf78c9eaf6f7d9d68c85a736b38ee428bd7bdee4eb23c56c584`。其他六个冻结源码及基准不变；GUARDS 未改，组合闸由父协调/唯一集成者负责。

与 E2 dataflow.py 的 completed_live_nodes/revalidate_action hunk 不重叠；与 B 新 reader/图片准入修复不重叠。host/_surface.py 与生成 JSON 合流时须保留其他已审接口增量；tests/merge_gate.py 只合入上述 FROZEN 一项，保留集成 GUARDS。

## 验证与边界

定向证据在共享 `tasks/C/no-platform-gate-implementation-v1/`，每批固定源码 export、命令、原始 log/XML、最终逐 case 映射均保存。B/E 非原作者原生/queue 真实跨版本与 HMAC 签名证据在 `reviews/C-implementation-cross-v1/`。原生实际 main producer ACK/cut 到新 process worker 恢复成功；未知协议拒绝时 attempt/state/seal/原 ACK/artifacts 不变。SQLite v2/v3 × sink/explicit 真实 producer/consumer、原 RunRef/完整旧 scope 跨新 Run 均通过；业务变化/缺材料拒绝且没有新 queue 效果。受控 HTTP 仅用于私有 fixture。

初始失败并未删除或合成全绿：c23 的过宽任务被中断；新 workspace 夹具依次修正 CAS、直接内部调用缺隔离 IO、错误参数及重复执行；新增纯 helper 断言修正为原 development ForgeError；历史 resume 夹具补真实协议与原生身份；P4 补同步文档及删无用导出。原始 851a965 未拒绝未知 protocol 的真实反例也保留，由 c264070 修复并真实复验。最终定向结果以 HANDOFF.json 为准。

未声称四线验收、Fable 通过、部署或发布。PostgreSQL 分支只静态核验 SQL，未连接真实 PG；整任务签名 registration/implementation 与实际 accepted-task 的组合由分层证据覆盖，未运行完整模型/provider。平台物理迁移及任意未知持久化格式不作承诺。P12/P8/P2 保持原版本和历史只读的既有交付无需以本任务为前置重启。
