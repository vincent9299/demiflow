# Forge 统一纯变换接线，2026-11-16

原始问题：同一 `read_lance(version=1) → sort / repartition → write_lance` 在真实 Dataset 上成功，但 FlowChecker 以 FL-02 unknown 拒绝。检查目录仅 36/98 个既有公开入口有声明；此数字是声明盘点，不是能力完成率。

本次把 sort/repartition/random_shuffle/randomize_block_order/random_sample 的原公开签名、参数归一化、字段与真实 LogicalOp 事实接入 Forge。检查器的 pull/stream 标签继续读旧报告，但不再用其隔断统一引擎已经支持的异步→关系→排序组合；操作支持性由原统一 operator_registry 判定。未知调用、未绑定 backend_options、字段错误、未声明终结动作仍拒绝。未改变任何执行器或提交权威。

## 验证

- 原红对照：`/tmp/demiflow-forge-capability-probe.json`；两个检查 incomplete，两个真实写表成功；无变换对照检查 checked。
- `/tmp/demiflow-forge-pure-final.xml`：86 passed（全部新纯变换、真实异步→join→sort→原 journal ACK、历史身份投影，以及原 Forge 接线检查）。
- `/tmp/demiflow-forge-order-r2.xml`：60 passed；唯一红是新异步测试夹具将 nullable Arrow 字段错写成非空 annotation，已修并在最终 86 项重跑。该组包括原 sealed plan / identity bridge / Forge platform contract。
- `/tmp/demiflow-forge-legacy.xml`：84 passed，原两条旧限制断言失败。更新为未知调用仍拒、重分区缺终结动作仍拒，以及合法异步直接写出通过；没有删除危险调用/字段/权限反例。最终 86 项含更新后的整份 flow checks。

已有 sort 等类型的恢复身份仍是原 `describe(operation)`；新增无 native effect 的事实不会改变该选择。测试锁定 operation_identity 与 compiler 原 type/fields 投影。承重七件保持原字节。

## 仍需处理的产品范围

原生分组、prepend、多种模型/批次仍有声明或 CheckedExecution 接线缺口；阶段 SaveLance 的旧 checkpoint-only 产品守卫与退休入口冲突。这些不由本次五种纯变换证明。外部系统资源、任意动态调用、诊断与迭代器也不机械标为可托管。

独立产品浏览器在源版本 ccb648a 暴露 `ThreadResourceProvider` 原 cleanup evidence 路径 247 字节加退出后缀后超过 frozen DTO 256 字符的真失败。对应资源路修复后再复验，不记产品通过。

同版本真实同机 Ray/thread 12 项通过，见 `/tmp/demiflow-ray-v3.xml`：原生 HTTP/关系、队列抓取结算/历史、finite phases、durable parking、三种计时首批与线程实际退出/code-pin/恢复。不是多机、GPU或完整98 API Ray证明。
