# 统一 stream composition 公开入口闭环

## 修复

1. 清除 `reject_streaming_map_options` 对已交付 Local relational / Ray Core 的旧后端拒绝；未知引擎仍拒绝。合法显式 map 策略继续由同一 native compiler/worker 执行。
2. `validate_stream_plan` 用原 compiler 的有界输入展开、`compile_kernel_spec` 和唯一 registry 校验全图；Dataset.run_stream 传入原 source，因此 join/union 后空顶层 plan 不会掩盖子输入 stream 节点。无流声明仍拒；未知 kernel、非法 backend_options、异步普通 map、节点/边界超限继续在业务读取/构造之前拒绝。未创建第二份持久执行图。
3. `explain_dataset` 将 executor.plan 的五个闭合声明元数据字段与 RuntimePolicy 字段分开。action/category/caps/native options 各自校验；未知字段仍交原 policy validator 拒绝。报告仅是编译能力，不把 source/sink parallelism hints 宣称为已实际准入的 placement。

## 实际 Local / Ray Core 验证

同一个实际 stream map 共享两路自 join，再接嵌套 join 和 limit，分别通过 take_all（归一为 iter_rows）与 run_stream 终端。精确数据、每 action 每输入仅调用一次、原 journal 的 ACK 均通过；Ray 使用 env-ray 私有集群并 finally shutdown，RayDataBlocker 未发现访问。没有修改主仓、共享环境或生产 cluster。

Local 组合 9 passed / 48.63s；真实 Ray 1 passed / 33.48s；最终 closed preflight 专项 5 passed / 0.83s。组合存在重叠，不求和冒充独立用例数。最终仅 explain category 类型反例晚于实际执行验证，单独复验；文件摘要、XML 与范围诚实记录于配套 JSON。

## 历史迁移与仍真实红项

JSON/Lance checkpoint 的退役拒绝仍保留。新增真实原生结果/表 ACK 后中断 run，再用同一 RunRef 恢复，回调不增加、表版本不重写；已完成 run 不允许创建新 attempt 的守卫未修改。prepend 对 configured prefix 的有限同步约束经复核保留，不把它误改成开放能力。

30 条旧显式 map 终端拒绝已转为具体成功/零读取断言，嵌套关系转为精确结果/共享源只读一次。这些不能据小组合宣称全绿：原 2-worker fixture 的 attempt-only input 无法安全 park，另 native save_lance 节点导致全图 nonrecoverable 禁止 park，均已交 root/residency 处理，未提高 workers 或将 from_iter 换成 from_items。逐历史 nodeid 对应在 JSON；保留新负臂与原资源额度。

原 stream miss/catch/drop、队列峰值、延迟及 stage_policies 断言未删除，仍需真实观测接线。原 checkpoint writer故障与部分旧生命周期观察还未在本批迁移。不会以当前验证代替整个 test_stream_map 或平台全量验收。
