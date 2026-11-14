# Qwen 独立复审后的补修

基线：`8830efa01f46d25fb7cd9ccd8dc5828638213e21`。
工作树：`/yzp/zhaozy/yangzepeng/0905/_demiflow/worktrees/gpt_integration_review_20261114`。
分支：`engine/gpt-integration-review-20261114`。

输入为 `/tmp/demiflow-qwen-repair-review-20261114/REVIEW.md`（362 行）。接受其中三个新增阻塞，撤回上一份报告“七项全部闭环”的总判定。旧日志和旧报告的具体测试结果保留；那些测试通过不等于没有回归。本报告记录补修与边界，不把局部测试计数换算成产品/引擎整体完成率。

产品 root 仍是让用户看懂、配置、运行真正的 pipeline，并分析、比较、评测数据；引擎 root 仍是正确、稳定、有界、通用且权威单一的计算平台。本轮直接保护历史身份、真实内存准入、可用的 actor 生命周期和诚实的恢复能力。主仓、在线示例与训练服务均不修改。

## 修复决定与连接

### QWEN-02：历史身份

`operation_identity` 保留原生事实生产者输出的原始 JSON；不再对其整体调用 `describe`。原来的二次投影会把大于 1024 项的 enum/placeholder 改成摘要，改变曾经有效的身份。新特化算子的 batch/flush policy 仍参与身份，原生配置漂移仍拒绝。

真实 Prompt 配置分别含 1、1024、1025、1500 个枚举值，核对其原生操作和 action 投影与 `b4dd417` 的历史公式一致。修复是恢复历史语义，不放开历史平台版本保护，不改任何已有封存文件。对 `recovery_identity.py` 的改动超出原设计卡的 `describe` 符号范围，属于用户已授权的七项集成修复及其复审补修；此处显式记录，不能再称原符号冻结完全未变。七份字节冻结文件另行核对。

同一反例也核对了 code pin 的操作投影。补充核对：`b4dd417` 的字面源本来就使用 `source_identity` 的 `literal_source/rows` 形状，所以先前四处测试 fixture 的改写本身不能证明字面源的历史身份漂移；大 enum 的二次投影才是本条已确认的反例。

### QWEN-01：普通执行与真实恢复能力分开

没有采用“空 `vars` 就一概无状态”的建议：`partial(actor)` 的外层字典也可以为空，内部却有可变业务状态。注册沿 `partial.func` 找实际 owner；函数按原函数规则处理，actor 仍经过统一状态策略。带自定义属性的 partial 不擅自忽略。

本地 driver 对明确可识别的锁、Event、Task 等运行期资源，且其他字段全部是严格 JSON 的实例，允许完成普通执行。**这不意味着资源或其附属业务状态获得了恢复适配。** 在第一次业务调用之前，driver 用原 RunJournal 的不可变绑定保存 action 的 `state-recovery-restriction`，标记 `attempt_only`。不增加 journal、提交者、调度器或另一套计划。

完整链路：实例注册 → driver 写限制事实 → 原执行/提交 → 工作台 `read_sealed_plan` / `_sealed_entry` 读限制事实 → 图详情明确展示“本次可运行，中断不能继续恢复” → 工作台恢复准入及引擎 RunAction 在重放前拒绝未完成 action。已完成结果继续复用，不为一次后续失败销毁先前提交。封存图本身不重写；编译期能力和运行期限制分别展示。

不在资源集合中的业务对象、半声明钩子仍拒绝；远端 worker/严格状态消费者仍需要真实适配器。锁/Task 支持不是任意连接池、任意 C 扩展对象或任何带 `close()` 对象的通用恢复承诺。需要中断恢复的 actor 应声明成对的 `snapshot_state/restore_state`。运行期资源不能被静默删除后冒充可恢复状态。

`StreamResources.close` 即使某一 owner 的清理抛出取消，也继续释放其余 owner，最后才传播错误；MapWorker 的注册失败清理保证即使 `astop` 出错也调用 `aclose`。原有取消、线程排空和资源关闭断言保留。

### QWEN-04 / QWEN-05：实际共享并发

工厂构造出真实实例后，注册使用实际共享并发重新检查 auto_json。共享 stage 的有状态实例在并发大于 1 时、第一次业务调用前拒绝；独立 worker 实例按每个 owner 的并发 1 注册，不把全局 worker 数误作同一对象的共享并发。

直接 actor 及折叠到上游 stage 的 predicate，也使用实际 stage 的并发。因而 FilterOp 自身没有 concurrency 字段，不再成为绕过条件。hooks 模式保持原契约，不擅自降低用户声明的并发来换测试通过。

### QWEN-03：cast 的同时驻留预算

`apply_batch` 在 cast 之前计算：保留的 Python 行 + 原 Arrow buffer + cast 需要的新 buffer。计入新数据与 validity buffer 的 64 字节对齐；类型相同的共享 buffer 不算第二份。结构、列表和常见原始类型按已知分配形状计算；无法证明的转换明确拒绝，不先分配再检查。

转换 Python 行时预留共存的 builder/result 余量；`values` 局部列表在转换完成后释放。callback 返回对象可能仍持有行，因此其计量不因删除局部变量而归零。IPC 仍通过原 `_CappedWriter` 限制真实累计磁盘字节，失败不发布临时结果。

等宽 int64→float64、扩宽 int8→int64，在只能放一份副本的预算下均应在分配前拒绝；充足预算下实际 IPC 读回必须与真实 cast 结果相等；同类型共享 buffer 的原准入保持。使用 Arrow 独立代理内存池验证拒绝时新 cast 分配为 0，避免线程采样漏峰值。

该限制约束平台控制的 payload 和转换共存，不承诺任意用户 callback、Python/Arrow 内部元数据或整个进程 RSS 都由一个字节参数精确限制。未知大小的用户分配仍受已有资源/进程保护。

## 验收纪律和未完成项

全部测试用本树 `pinned_pytest.py`，固定解释器与进程内模块路径；完整命令、输入 SHA、HEAD/dirty、日志均在 `/tmp/demiflow-gpt-integration-review-20261114/qwen-fix-*`。不在主仓测试，不覆盖 qwen 的证据。

先完整跑了此前漏掉的 `test_stream_map.py` 和 `test_arrow_batch_sink.py`：**42 failed / 43 passed**。其中 5 项正是 qwen 找到的新增 actor 回归。补修后这 5 项均转绿；该旧套件另有 32 项仍要求公开终端拒绝已接通的流式能力，以及 5 项调用已经退役的显式 checkpoint API。不能删除这些失败记录或把未执行项算通过。

Qwen 指出的 `bound_result_cache` / `async_materialize` 七项既有失败、X-Api-Key 脱敏、跨并发数的 Embedding 批请求缓存不变性仍为独立已知问题。未声称这一批消除全仓所有失败，也未连接 PostgreSQL 或生产模型服务来补验。原有 Ray/真实浏览器证据只证明其对应旧提交；本轮受影响路径的验证结果另记，不冒充全量重跑。

最终命令、计数、源码一致性和提交以本目录补修证据索引为准。

## 实际验收结果与证据版本

| 验收组 | 原始结果 | 解释 |
|---|---|---|
| 核心运行/状态/写出恢复/身份/Arrow/工作台 | 314 passed，1 failed | 唯一失败是新增清理测试检查了声明对象，而非执行器创建的实例。改为检查 `w.callable._fn` 的 stop/close 各一次；未改变生产清理逻辑来迎合断言。原失败日志保留。 |
| 反例与 S012 生命周期复验 | 23 passed | 上述取证修正后全部通过；既有注册失败、取消、启动一次、收尾幂等断言保持。 |
| 最终状态/反例/恢复专项 | 160 passed | 最后增加资源字段数量的分配前上限（16384），在最终源码上复跑完整 S 族、新反例和真实流恢复。 |
| 真实 Ray 执行与生命周期 | 8 passed | 线性/图等价、跨进程恢复、关闭/取消/actor 死亡等原断言。 |
| 真实工作台浏览器 | 2 passed | 实际运行、封存图、源码和观测关联；不是线上旧服务的截图。新增资源限制文案的专门浏览器场景未单独运行。 |
| 原有 stream_map/cache/materialize 全文件 | 54 passed，44 failed | 37 个旧契约预期冲突 + 7 个 qwen 已归因的既有失败，逐 nodeid 留存。5 个本次引入的 actor 回归已全部转绿。 |

不累加重复覆盖成一个夸大的通过数，不把旧套件 44 项失败算作绿色。**本轮三个新增阻塞的反例和相邻状态并发缺口已补修；全仓仍不是全绿，整个产品/引擎也不是 100%。**

最后一次生产修改仅是 `runtime_resource_state` 在复制字段前增加数量上限。较早的 Ray、浏览器、核心组与最终源码相比，仅此文件的这一行不同；已保存精确 [差分](qwen-repair-evidence/field-bound-delta.patch)，并在最终字节上做 160 项状态专项。没有把较早组标记成“全部输入摘要与最后提交一致”。各组的源码差异、测试清单差异、真实退出码、模块路径及运行前后不变检查均在 [证据索引](qwen-repair-evidence/verification.json) 中。

[最终输入 SHA 清单](qwen-repair-evidence/inputs.json) 可与 Git 提交逐字节比对；原始完整 provenance 仍留在 `/tmp/demiflow-gpt-integration-review-20261114`，仓内保存其 SHA 和精简记录。四种真实 Arrow cast 的预留与实际分配峰值对照见 [Arrow 探针](qwen-repair-evidence/qwen-fix-arrow-allocation-probe.json)。

主仓仍为 `e40a6dd3dd756aba423dca4c7d995ada0a4bff65`，原有 11 项文档状态不变；7 份字节冻结文件均与补修基线一致。未 push、未合并主仓、未更新在线服务、未停止其他会话。
