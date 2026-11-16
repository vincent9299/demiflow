# 模型兼容：原行为、真实 worker 和当前边界

本页记录模型兼容的已交付增量，不宣称全部模型/产品意图完成。原始两大 root 和 I07/I08/I09/I12/I13 保持：真实执行、有限额度、原提交恢复、业务能力和历史来源必须同时成立。

## 原失败的对照与本轮修复

原 `267b914` 的七个 Embedding nodeid 未改动跑通，7 passed / 7.33s；证据 `/tmp/demiflow-original-embedding-baseline-verification.json`。因此不把这些失败一概解释为“旧测试过时”。七个是 adaptive gate、两个 ManagedHTTPService、原生请求额度、取消收尾、预取重叠、图片只解码一次。托管服务仍在另路收口，本页没有将它们计为通过。

`8025771` 只在 Embedding 原生 reserve 边界保留其原 `RecoveryContractError`；共享 RunPromptJournal 的 Prompt 额度异常保持 `PromptBudgetExceededError`，原原子预留不改。原取消测试先验证被动计划再启动 actor，顺序与公开 run_stream 相同。固定该提交六项验证 6 passed / 41.33s，包含 Embedding 额度/取消/只解码一次，以及 Prompt 原额度和两项原额度恢复反例。

已有 M020/C07 契约要求 gate 从声明式 request_policy 在 worker 重建，不能把调用方的 live gate 悄悄替换。adaptive 正例改用该声明并观察实际 worker 的 gate，保留 HTTP 并发 `1 < peak <= 3`、capacity=3、fresh=9、缓存第二次 fresh=0 的断言；新增 live gate 零 HTTP、零私有 DB 的拒绝反例。

预取、prepare 并发和图片解码的测试钩子必须位于真实 worker。显式 thread provider 要在创建 Dataset 时绑定，不能只在执行既有 Dataset 时套 context。本轮一次错误地只在执行时绑定，得到真实红证据 `/tmp/demiflow-embedding-integration-16b.log`，未改生产绕过。修正创建位置后保留全部原重叠、HTTP、payload、计数与清理断言；prepare 并发还新增非零观测要求，防止空观测误通过。失败作用域让原领域错误自然传播，不捕获后伪造整个 run 成功。

## 已集成的能力

- 显式 Embedding 私有 SQLite 缓存：原 delivery-only take/iter/普通 run_stream；driver 持原私有 HTTP journal，RunJournal 仍唯一提交和有限请求准入权威。未声明两个 SQLite 原子提交；中间崩溃保守保留额度、原 run 标 attempt-only。历史大缓存先在 SQLite 投影中按 UTF-8 字节拒绝，再进入 Python。原 native/受管/队列/SaveLance 禁止私有账本的边界不放开。详见 [PRIVATE-EMBEDDING-DELIVERY.md](PRIVATE-EMBEDDING-DELIVERY.md)。
- 既有 `map_prompt_async(routes=..., route=...)`：被动工厂重建、原 occurrence 与 worker lease、原 journal 原子 run/node/route 额度、子 Prompt 使用量和有状态 when 的恢复限制。真实 Local 非 final ACK 恢复及同机 Ray 恢复均通过，8 次 HTTP 不因恢复增加。详见 [NATIVE-ROUTED-PROMPT-CLOSURE.md](NATIVE-ROUTED-PROMPT-CLOSURE.md)。

## 版本与独立验证

私有缓存 owner `a61b826`，48 passed / 225.25s，2142 项源码摘要与提交逐字节一致。原 SQLite 默认行为另 10 passed / 2.27s；不把其中被排除的 Prompt 集成用例计为通过。缓存验收前两次 runner 把 `/tmp/ray` 当成 namespace package，已保留红日志；最终入口移除 runner 自身 `/tmp`，生产没有为测试更改导入。

原生路由 owner `651a9bd`，Local 18 passed / 76.07s，Ray 1 passed / 27.65s；两组各2133项源码及9份附件独立核对一致。Local 唯一 deselected 是已由真实 Ray 单独运行的那一例。

总控集成 `d85dca1` 上，三项原 Embedding 观测/额度/预取正例与14项原生路由案例同时通过，17 passed / 81.71s，Ray 例在此 Local 集合中明确排除。另集成早期 `16b8574` 组合6项通过包含原私有缓存/原run准入断点/禁止混入native获取与写入；其中三项观测迁移失败保留为上述红证据，不合并算全绿。各组有重叠，不累加成独立通过总数。

总控源码/附件独立复核见 [MODEL-COMPATIBILITY-PRIMARY-VERIFICATION.json](MODEL-COMPATIBILITY-PRIMARY-VERIFICATION.json)。

## 尚未关闭

普通 run_stream 合作停止的已准入 HTTP 排空与真实模型指标正在收口；ManagedHTTPService 的真正子进程生命周期与 GPU 资源接线尚未集成。路由离线 store、Prompt 私有 delivery 缓存、受管路由及正式物理 target/broadcast 门仍需分别取证。以上原生路由成功不推定这些能力完成，模型兼容也不代替正式性能与最终同版本完整矩阵。
