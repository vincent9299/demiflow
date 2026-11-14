# DemiFlow 七项集成问题修复与验收

> 复审更正：本文件原有“七项全部闭环”结论已被 qwen 的三个新增反例推翻；1346 项通过仅为当时所选测试的真实结果，不再作为整批关闭结论。后续补修、恢复能力边界和新验收见 [QWEN-REPAIR-REPORT.md](QWEN-REPAIR-REPORT.md)。旧证据保留供追溯。

对应原报告 `REPORT.md` 的 IR-01–IR-07。原报告、失败日志和原始 lane 回执保留，不改写历史结论。本次在 `engine/gpt-integration-review-20261114` 隔离分支实施；主仓不参与修改或测试。

最终验收结果见文末和 `repair-evidence/verification.json`。本批交付范围是七项已确认问题及其暴露的组合退步，不把卡片数量或测试数量换算为整个产品/平台的完成百分比。

## 修复后的全局调用关系

公开 Dataset 终端进入原有 RunAction → 一次准入决定 → 固定输入 → 编译或读取原封存 logical/physical/bindings → 解释 ExecutionLayout → 校验实际子程序、代码、源和 stage 对应 → 按已封存 backend/profile 执行 → 原 CommitCoordinator/RunJournal 发布。

工作台只读取这条执行链封存的事实。报告图、执行图和执行观测通过真实 occurrence 身份桥关联，不通过重新编译一个展示用 DAG 声称已经绑定执行。

Local/Ray worker 路径继续使用既有 worker 协议和资源管理器。native driver 的 v2 布局没有另造调度器、提交账本或资源分配器；持久化权威仍然只有原有运行日志。

## 七项修复

| 问题 | 实际改动与交付行为 | 主要验收依据 |
|---|---|---|
| IR-01 P0，公开 API 没有消费封存布局 | `LocalDatasetExecutor._bind_plan/_execute_plan` 接入 `bind_native_execution/execute_native_plan`；take/iter/count/materialize/schema/size/write 及非队列 run_stream 入口消费真实布局；write_lance 各分支共用原 sink/commit。`_apply_plan` 接受合法 ExecutionPlacement，并传递 CPU、行数及字节上限。 | 从公开终端观察真实执行入口；纯 copy 写出也存在 F1；stage 交换必须拒绝；小预算先于业务函数拒绝；历史写出恢复、宽行、提前关闭结果迭代器。 |
| IR-02 P0，原生配置遗漏/身份碰撞 | Embedding 的事实投影绑定真实 actor 的模型、端点、凭证引用、输入输出、调用/错误输出、预算与 policy；构造后配置漂移拒绝。共享 `operation_identity` 同时服务 action identity 和 code pin，包含新特化算子的 batch/flush 参数。Deduplicate 补齐 when/quotas/容量参数，绑定真实 actor；VectorSearch 深拷贝声明并核对实际 actor 的 spec/query/output/options/filter_column，构造后修改必须拒绝。 | batch_size、call_output、error_output、max_requests、flush_interval 改变均改变身份；actor 篡改在事实/身份/封存前被拒绝；真实恢复不能使用另一组输入的旧请求；13 项新增 Deduplicate/VectorSearch 身份反例先红后绿。 |
| IR-03 P0，Embedding 默认准入与多 worker 恢复断裂 | 默认响应上限调整为 8 MiB；显式更大但无法容纳的声明在发送前拒绝。模型效果恢复与批缓冲参与者分开。每个 worker 保留 pending/ordinal，保存确定性分配游标；timed batch 保存已经发送的批边界；packed 子请求有独立 occurrence；取消后排空已准入请求，发布 ACK 前不进入下一窗口。 | 真 HTTP、多 worker、默认配置；after_prepared/after_acked 的异常和真实进程退出；已完成请求不重复发送，unknown 不重新请求；打包和定时批的输入多重集保持。 |
| IR-04 P1，多输入 DAG 编译错误 | 按节点端口和实际 ProgramSlice 绑定源/算子/合成源/终端，所有叶源覆盖；合成节点使用可选 `source_stage_id`，不伪造 operator index。实际 DAG 与 program→stage 映射均复核。复合源身份使用子程序/已提交物化引用，避免序列化运行时 reader。 | 公开 union/join/reduce，嵌套物化输入；错误的 stage 对应被拒绝；真实 Lance 写出、图读取和源码身份桥。 |
| IR-05 P1，prepend 嵌套公开 action | 前缀使用内部有限读取和原 source capture 协议，固定 mutable head，沿用父运行的日志/提交/资源权威；不调用公开 iter_rows。内部捕获记录完成后，恢复读取同一份前缀并继续已发布位置。 | 原 R012、前缀与 Counter 状态组合、提交后恢复；保留嵌套公共 action 守卫。 |
| IR-06 P1，auto_json 分类和准入不一致 | 普通 JSON 实例默认 shared_colocated，与统一状态策略一致；同一实例并发 >1 仍拒绝。工厂创建的非 JSON 状态若没有状态钩子或明确原生恢复适配，启动前拒绝，不能默默丢掉。 | 普通 Counter 真实 materialize/resume；不带钩子的资源状态反例；S 状态族和 U4 恢复。 |
| IR-07 P1，Arrow 资源上限没有约束真实字节 | reader 使用封存物理窗口，业务 callback 组大小保持；按 retained buffers 计数，切片必要时脱离父缓冲；输入/输出/转换分别留预算。复用已有资源准入，任务文件在派发前经原 artifact authority 预留；IPC 每次写入前检查真实字节，消费/失败后先删除文件再释放额度。 | 大父缓冲的小切片、IPC 头部/编码超过预算、部分文件清理、reader=2/callback=5 的真实写出；L011/LNU 系列与 payload/native resource 回归。 |

## 集成验收额外发现并修复的接线问题

这些直接影响两大 root，随本批收口，未作为边角扩展：

1. **工作台动态刷新**：最初读取图时引擎还没有封存计划。运行观测状态改变或运行中定期刷新同一 run/attempt 的图；保留切换页面的票据和请求顺序检查，迟到响应不能覆盖另一次运行。
2. **多输入身份桥**：旧线性下标会把 join 子程序的节点关联错。native 路径逐 ProgramSlice 映射 source/operator/terminal；固定 source 后保留既有报告绑定；没有对应关系时仍诚实缺测。
3. **迭代器提前关闭**：delivery 已经排空且保留交付对象后，外层 action 记录 `delivery=closed`；它不是全源运行成功或可重放的证明。其他异常不自动完成。
4. **显式宽行预算**：封存约束只继续收紧已准入的显式上限，不能再次用默认估算替换它。
5. **Ray 真跨进程引用**：仅传 ObjectRef 的 hex 会丢失 owner 注册，消费者卡在第一行。采用 Ray 自己的有界引用序列化，数据仍在 plasma，驱动只处理引用；旧无 owner 的临时引用明确拒绝，读取有超时。worker 异常通过控制通道及时抛出。K4/K5 冻结协议不变。
6. **run_stream 恢复能力如实声明**：非队列 run_stream 封存并消费 local_stream 布局和阶段上限，标记 delivery_only；中断重放继续由原 RunContext 守卫拒绝。队列来源和增量生产者仍用既有 native_checkpoint 协议，没有降级队列的恢复语义。
7. **隔离验收入口**：除进程内导入来源断言，增加独立 Forge domain；不修改共享 editable 安装，也不放开固定主机历史分区守卫。

## 契约增量

- `ProgramSlice.source_occurrence_id` 表示该子程序真实源节点的 occurrence，包括叶源和合成源。v2 ProgramBinding 仅在合成源属于执行算子时增加可选 `source_stage_id`；读取校验与实际 DAG 核对同时落实。
- `StageBufferSnapshot` 增加 `next_slot`，旧单 worker 快照按 0 读取；每个 slot 的 pending 和 batch ordinal 保留。已经发送的模型批边界、请求 membership 绑定进同一 RunJournal，不能在重试中重新选择业务输入。
- 非队列 `run_stream` 的 ActionDecision.recovery 从不实的 native_checkpoint 改为 delivery_only，与既有 RunContext 重放准入一致。Queue/producer 不变。
- Embedding 原生事实使用 `demiflow.embeddings.v2`；不重写旧历史记录，不把新语义伪装成旧代码的同一身份。
- Ray DataRef 的 `location` 是有界、带版本前缀的临时引用编码，不构成持久恢复证据。整图恢复仍从已提交 source/cut 重放，不把 plasma 当持久账本。
- 本批新增的 `seal_native_identity_bridge` 只做绑定和封存，不读取或执行工作台/Forge 业务代码。引擎对产品的依赖方向未倒置。

## 验收方法与证据纪律

- 所有 Python 测试使用 `pinned_pytest.py`：固定解释器、工作树和 PYTHONPATH，进程内检查 demiflow 来源；保留完整命令、源码及测试 SHA、HEAD/dirty 与实际导入位置；最终五组还检查运行前后的 Python/JS/CSS/HTML 字节不变。
- 外部模型仅使用临时本地 HTTP fixture；Embedding 的恢复测试实际调用运行日志、原生恢复适配、checkpoint、sink、真实传输。进程损失使用子进程 `os._exit`，不把抛一个异常冒充进程崩溃。
- Ray 使用临时自有集群和真实 actor，退出时关闭；从独立消费者进程读取引用。没有 GPU 业务调用。
- 浏览器使用真实 Chromium、临时项目和独立自动端口，完成检查/运行/封存图/源码/观测；不开主仓服务。
- 失败日志保留。早期浏览器夹具没有导入 demo、选择入口后未检查、测试服务拒绝检查、测试表达式触发 CSP，以及在验收期间改变平台源码导致快照过期，都记录为无效/未通过轮次，不能用作成功证据。临时 Ray 栈探针移除，不进入交付实现。
- 旧 `tests/test_embeddings.py` 的私有 SQLite 缓存用例改为公开 `take_all` 交付路径，保留响应校验、错误不重发、预算、服务回收、singleflight、缓存命中、图片和打包断言。契约 M004/M006 明确允许独立缓存模式，但禁止原生 checkpoint 与私有 journal 并存；native materialize 的拒绝反例及真正 RunContext 崩溃恢复另行验证。跨 worker 数量变化会改变请求分组，精确缓存重放测试改为同一声明；不将不同计划声称为同一次恢复。
- 有意迁移的旧断言：原先“普通 JSON 实例必拒绝”“write copy 不封存”“旧线性 source 列表”“seal_write_topology 是公共 write 入口”“IPC 文件不算头部”等不再符合新契约。迁移到真实新入口、实际展开图和精确的拒绝边界，保留数据、恢复、资源及代码变化反例；没有通过删测试或批量 deselect 消除失败。

## 用户实际看到的变化

- **产品**：从同一工作台配置并启动流程后，运行图会更新为本次真正封存的计划；选择 map/join 等节点能追到正确的源码/参数节点及已有观测。没有测量或没有绑定的字段继续如实缺测。浏览器验收使用双 Lance 输入、map/filter/join/write_lance 的真实流程，输出为 2 行。
- **引擎 API**：take/iter/count/materialize/write 等终端使用同一封存布局；多输入 DAG 按实际子程序连接，资源约束进入执行器；批模型恢复保留各 worker 的输入归属和请求记录，不重复已知效果；Counter/prepend 和 Arrow 写出遵守相同恢复及资源边界。普通 run_stream 明确是交付语义，队列保留自己的原生恢复协议。

## 完成边界

这一批修复证明所覆盖的产品运行闭环和引擎路径已连接；不代表 Notebook、Case、所有业务 V 卡、任意外部算子/任意后端组合都达到 100%。本次装配之后其他会话的在途文件没有复制进来，也没有把其自报 tested 当作本次独立验收。

内存保证是资源准入、受管缓冲及编码的明确上限；任意 Python UDF 和第三方库内部瞬间分配不等同于可预测的 RSS 硬沙箱。封存的字面源继续拒绝非有限浮点元数据（如 NaN），无效向量会在源身份或领域校验处拒绝。外部请求处于 unknown 时明确拒绝自动重发，不能以“自动恢复成功”代替对已发生效果的确认。

## 最终结果

**本批 IR-01–IR-07 全部修复并通过独立集成验收。共 1346 passed，0 failed。**

| 最终验收组 | 通过 | 跳过 | 说明 |
|---|---:|---:|---|
| `accepted-units` | 916 | 6 | 全部 upgrade_v2 单元契约；普通解释器没有 Ray。 |
| `accepted-core` | 420 | 2 | 本批反例、原生模型、写出与历史恢复、多输入、资源、工作台与队列边界。 |
| `accepted-browser` | 2 | 0 | 真实浏览器运行、封存图、源码、观测及输出。 |
| `accepted-ray` | 3 | 0 | 线性/图 Local–Ray 等价、消费者进程死亡后的整图恢复。 |
| `accepted-ray-lifecycle` | 5 | 0 | 复用 L016 原断言补验成功/失败收尾、资源关闭、actor 死亡恢复与无孤儿。 |

跳过项不计作通过：6 项 Ray 跳过中的 5 项已在独立 Ray 环境补跑通过；另 1 项是 L018 的历史占位测试，真实图执行与恢复另有上述 3 项证据。剩余 2 项 PostgreSQL 集成用例没有 task-private DSN，未连接共享或生产数据库替代验收。本次未宣称 PostgreSQL 真实后端已验收。

五组最终运行的 **1038 份源码/前端文件摘要完全相同且运行前后不变，598 份测试文件摘要与交付内容一致**。每组均保留完整命令、解释器、真实模块导入路径、开始/结束时间及 HEAD+dirty；这批测试是在 `b4dd417` 上带修复工作区运行，不伪称已经测过提交后的 Git SHA。提交交付时另核对 Git 中的文件内容与这些摘要一致。

7 份冻结文件逐字节一致。主仓仍为 `e40a6dd3dd756aba423dca4c7d995ada0a4bff65`，原有 11 项文档状态保留；未 push、未合并主仓、未停止其他在途会话。

- 机器验收索引：[verification.json](repair-evidence/verification.json)
- 完整命令与输入摘要：同目录各 `accepted-*-provenance.json`
- 通过日志与失败历史：同目录 `*.console.log`；历史失败不作本轮成功依据。
- 浏览器实拍：[执行计划、源码绑定和已有观测](repair-evidence/bound-plan-observations.png)
- 冻结文件及主仓只读核对：`frozen-audit.json`、`main-read-only-audit.json`。
