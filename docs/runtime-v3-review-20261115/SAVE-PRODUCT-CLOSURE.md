# SaveLance：检查、执行、固定结果与恢复入口

基线：`8fe949f`，隔离分支 `engine/gpt-v3-residency-20261115`。这是平台/产品接线包；未修改主仓、生产服务、承重七件，不引入第二份提交或恢复账本。

## 用户实际得到的能力

- 工作台能准备和运行 `read_lance → save_lance(mode='overwrite') → write_lance`，也能运行原生 `run_stream()`。Save 目标必须已经授予 create 或固定 expected_version 的 overwrite 权限。
- Save 和最终结果分别显示各自固定版本。第二个微批失败时，第一批已 ACK 的结果仍可读取，最终输出不会被伪造为成功。
- 已发布但尚未写入原提交回执的微批可以进入原引擎恢复。准入只读取原 prepared、原 sink.inspect 和封存身份，不生成 ACK 或 UI 结果。原 CommitCoordinator 完成后才能登记结果。
- 历史 checkpoint 输出的固定版本读取保留；没有恢复已经退休的 open_checkpoint 执行入口。

## 权威与接口

`execution/dataflow_stage_facts.py` 是只读适配：从原 runtime_commit_scopes、runtime_commits/runtime_receipts、dataflow seal 和原 identity bridge 取得 parent action / node occurrence / report node / scope / commit / schema / 固定引用。无 ACK 的事实不能由 `native_stage_commit` 投影为结果。

`CheckedExecution.native_save_policy` 对照报告的 Save 节点与封存 target grant。真实 writer 绑定初始目标时验证固定 CAS；create 首批使用原 `LanceWriteSpec(mode='create')`，后续使用原版本 CAS append。恢复沿原 scope 的 spec，不重写历史策略。

`NativeStreamLanceService` 每个 node 只保留最后 ACK 的 scope/commit/ordinal，旧批次重放不回退该引用。真实 finish 或失败后的 close 才向原不可变 action_event 发一次 stage_commit；不会把每个微批塞进另一个事件账本。

`workbench/runs/native_stages.py` 验证登记和读取时的原生身份。事件丢失时，reconcile 对原 ACK scopes 做有界只读投影。`admit_prepared` 仅证明恢复准入，调用原 `_BatchSink.inspect`；外部版本变化、未知归属仍拒绝。它不调用 commit/recover，不构造 writer，不发布数据。

工作台移除旧 `workers=1` 特化，使用统一引擎默认最多 4 个活跃 worker，保留原 manifest 内存、RSS、磁盘与时间预算。另独立验证实际单 worker 的 durable 分期/崩溃恢复。编译器只将已验证的无 predicate `StreamLanceFactory` 按其真实状态 hooks 标记 adapter；旧 sealed kernel 不重写，未证明可恢复的用户 predicate 不提升保证。

`run_stream` 的检查只消除未使用的 null callback/退休 checkpoint 默认参数与真实执行参数之间的形状差异；非 null 声明不被忽略。

## 实际验证与证据口径

专用文件：`tests/test_runtime_v3_save_product.py`（11 个展开后的用例）。各用例均已实际执行，最后一组的状态见 JSON 与原始 JUnit。以下是不同固定候选的逐批证据，不把早期失败命令伪装成最终全绿跑次：

- `/tmp/demiflow-save-product-final.xml`：7 passed / 2 failed。两个失败是测试夹具错误（用只读 journal connection 修改负例状态、Path 与 str 比较），产品运行均已越过成功断言；二者随后修复并重验。
- `/tmp/demiflow-save-admission-r1.xml`：4 passed，包含修正的两个夹具和精确故障窗恢复/外部改写两臂。
- `/tmp/demiflow-save-admission-final.xml`：最后两臂另加真实 prepared scope 上的原 sink.inspect 不确定故障注入；最终状态与当前源码摘要记录在配套 JSON。
- `/tmp/demiflow-save-native-r1.xml`：较早候选的原 Save/stream suffix 26 passed，包含原 6 个提交故障窗、未知外部后继、取消、单行预算与活对象历史保护。该组在最终产品 admission helper 加入之前运行，不能冒称最终完整源码全量复跑。

产品故障使用真实管理的 supervisor、worker、unit 与 journal。测试 shim 只在原 `after_sink_commit_before_receipt` 故障点退出 worker；未改封存业务/runtime 文件，未伪造 execution_stopped/accounting/ACK。恢复前 prepared/receipts={}，恢复后原 commit_id ACKed；3 行只产生 3 个 Save 版本，没有重复 overwrite。

首跑还验证：原 bridge 节点关联、固定表内容、原 create spec、真实 4-worker policy、内存/磁盘不超过 manifest、cleanup complete、伪造 scope/parent/node/reference 拒绝、无 ACK 不给结果。单 worker 用例使用真实 durable exchange，在原 native receipt 故障后恢复，预算 output_rows=2、原版本/提交身份不漂移。

## 不隐藏的剩余问题

额外旧产品回归 `/tmp/demiflow-save-products-regression.xml` 为 **5 passed / 3 failed**。未修改或削弱原断言；已交 root 和 artifact_recovery 单独处理：

1. `test_managed_native_families[flat_batch]`：旧 prepare stream_residency 隐式窗口未给控制开销留余量，268445696 > 268435456。
2. `[actor]`：真实输出成功，但缺 created 生命周期观测。
3. `[actor_instance]`：真实输出成功，但 started 为 None。

运行中逐微批实时刷新未增加；finish/失败 close 登记最后 ACK，reconcile 可从原账本重建。未宣称此包单独完成 Embedding 产品接入（由另一会话负责）、物理 GPU 验证或全产品 100%。
