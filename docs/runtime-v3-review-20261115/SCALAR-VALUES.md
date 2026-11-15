# 数据值保真修复

基于 `a580adb`。本树执行并断言实际 demiflow 导入位置；主仓未写、未跑测试。

## 原始问题与意图

公共 `from_items` 的输入检查拒绝日期/时间/Decimal，真实 `read_lance` worker 的记录编码也拒绝这些值。tuple 中 set 原先可用，却在新记录传输层被拒。这违反 I01/I09/I13：统一引擎不能减少实际业务数据能力或在结果/恢复中改变类型。

先建立真实反例：3 条失败（原始日志 `/tmp/demiflow-regression-scalars-red.log`）。包含直接 codec、from_items、Lance 输入三条；其中两条直接走原 worker、RunContext 和物化提交，没有假执行器。

## 修复边界

- 新 data-only scalar 模块只接受确切的内置 date、datetime、time、timedelta、Decimal。无动态类型导入、pickle、任意对象重建；自定义 tzinfo 在调用其方法前拒绝。
- timezone 只接受 UTC、内置固定偏移与具名 ZoneInfo，保存 fold/键/名称/实际 UTC 偏移。解码时当前 tzdata 若给出不同偏移，明确拒绝而不改写历史时刻。
- set/frozenset 采用确定的 typed 编码，Record 内部深冻结、业务读取恢复原 set/frozenset 类型；消费者修改不影响封存快照。
- check_size 和输入 copy 准入认识这些显式数据值。未知对象仍拒绝，且分类为 INVALID_CONTRACT。
- describe 仅增加此前不支持的 scalar 分支。既有 JSON、bytes、set、enum、native prompt 身份分支不改。承重七件对 `49b666f` 逐字一致。
- 增加聚合叶子字节的编码前下界检查、解码深度/节点限制；最终 encoded row 上界保留。此项不冒充进程 RSS 的硬保证。
- 关联 key 的 JSON 编码契约不改；允许某类型作为 payload 不代表它可作 key。

## 验收

`tests/test_runtime_v3_scalar_values.py` + 原 `array_values` / `execution_recovery_identity` / `runtime_v3_x`：**55 passed，29.14 秒，无跳过**。

真实端到端覆盖 from_items 的完整 scalar/container 组合与 Lance 的 date/timestamp/Decimal：worker 执行 → materialize →结果读取 → 中断 → 原 RunRef 恢复。原提交条目字节内容不变、attempt=2、工作函数仅执行 1 次，恢复结果保留类型、精度、时区、fold、负零。

输入 F 组另行通过。附加误选的工作台 runtime_contract 文件有 2 条独立旧夹具失败（`__new__` 未初始化 `pending_retirement`）；它们不调用此次 scalar 检查，不纳入 55 项通过结论，也未隐藏。

逐 nodeid、输入摘要和日志位置见 `SCALAR-VALUES-EVIDENCE.json`。当前子任务未复跑 Ray，不宣称多机完整验收。
