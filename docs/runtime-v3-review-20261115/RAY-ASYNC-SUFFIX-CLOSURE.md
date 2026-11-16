# Ray 异步后缀与原资源契约（2026-11-16）

原 I01/I04/I09 要同一 Dataset 在 Local/Ray Core 保持业务能力、独立资源准入和原提交恢复。真实显式 ACK 队列链含 source→Fetch→Map→ACK→terminal，原 4 worker 上限下 Local 合为独立 source 加一个后缀，Ray 却仅因 provider 条件被迫保留 5 个 stage，并拒绝启动。queue 的活租约不能靠停车后重放来规避。

`a48ce01` 只去掉 `async_suffix` 的 Local 限定。两种 provider 仍走同一 GraphRunner；源保持独立，异步后缀 CPU 与同时驻留内存逐阶段相加，必须在一个实际宿主内准入。显式 limits/isolation/accelerators 不被吞并；没有提高 worker、credit 或恢复保证。

真实队列链的原资源是 source 1000 CPU millis 加后缀 4000 CPU millis。4 worker 不等于 4 CPU。I09 harness 原来把私有 Ray 的 num_cpus 也设为 4，属于夹具假设；当前测试按实际 6 CPU/4 worker 运行，原性能基准不变。补修后的 4 CPU 反例仍以 RESOURCE_INFEASIBLE 拒绝，并保留零 HTTP/零 claim 证据；不能用首次 5 stage 的失败替代这一反例。

总控固定版本验收：

- `fe40ec5`：新契约正例真实复现 5 stage 对应期望 2 stage 的失败。
- `a48ce01`：真实 Ray 8 行异步链、原 ACK/结果/清理和旧 default suffix 回归共 20 passed / 20.69 秒；另 1 failed 是新增负例未填合法 ResourceProfile.limits.cpu_millis，在构造夹具时已被原校验拒绝，没有执行到被测逻辑。
- `879baac`：只补上述负例的必填 CPU limit；7 个编译/宿主/profile 约束全通过，真实 Ray 用例已在前一生产字节相同的提交通过，不重复算两次。
- `301b426`：工作台 GPU、tensor 负例及受管 image/document Embedding 组合 **55 passed / 299.81 秒**。覆盖真实 prepare/start/CPU→CUDA→CPU、历史计划桥/原 ACK/固定 Lance、取消释放，以及对象封存后篡改零 HTTP。该组合检查新资源事实与此前 Embedding 对象授权的实际集成。

原始证据均在 companion [总控摘要复核](PRODUCT-GPU-RAY-ASYNC-PRIMARY-VERIFICATION.json)，逐组与 git archive 字节绑定，保留红回执。GPU owner 的完整 V030 七个产品用例及 74 项合同历史负例仍绑定 `fae6a05`，不改称在本版本全量复跑。

Ray I09 完整五组由队列 owner 继续验收；本文件不提前宣称完成。轻量 CPU 性能、完整路由恢复门及最终集成矩阵仍另列，不据此宣称全部原意图实现。
