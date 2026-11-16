# I09 / V022：原生队列完整业务与 producer 跨阶段恢复

本轮结果是 **Local 全部通过，Ray 正常与恢复通过，Ray 显式 ACK 的物理规划仍有阻塞**。不能把它描述为 I09 已全部闭环。生产修复提交 `471b7285717e3805e1bf2b1c095d0a1d561ee5b5`，只改 `dataflow.py`、`dataflow_producer.py`、`queue_producer.py`，45 行增加 / 6 行删除。隔离树为 `/yzp/zhaozy/yangzepeng/0905/_demiflow/worktrees/gpt_v3_artifact_recovery_20261115`，分支 `engine/gpt-v3-artifact-recovery-20261115`。依赖总控已交付的 durable credit `763b973` 和 immutable helper `92d9474`，本树对应 `328fdb2` / `eb24a78`。

[机器证据](QUEUE-BUSINESS-20261116.json)绑定 1040 份源码/测试载体的 SHA256、原始业务报告和 JUnit 字节、全部原 run journal 分配记录及七份冻结文件。测试时 HEAD 为 eb24a78，生产及 harness 是当时的明确 dirty 内容；逐份摘要已核实与交付文件相等。没有把 dirty 测试冒称当时已提交，也没有在主仓测试、安装依赖、推送或改共享服务。

## 修复的实际问题

原 4 worker / 两个输入、每个输入两条候选的真实 search→flat_map→enqueue 需要经过多个驻留阶段。旧实现每个物理 cut 都调用 native end_unit；source 的第一个 park cut 后就丢掉 `_unit`，随后真正 search/publication 被 FENCED。修复仍照常提交物理 cut、返回原 ACK；只有所有已记录的下游与 fanout delivery 都由实际 consumer frontier 确认排空，才终结 native source unit。判定在 `DurableResidency.before_commit` 验证原 frontier 后进行，不用“park 完成”替代消费。

恢复时 source cursor 可能已指向下一位置，原 native unit 却仍在途。现在复用原 checkpoint residency 和原 source `lease_ref`，通过原 store 验证摘要，并校验 generation、position、occurrence、next_cursor 的精确绑定。不能以 next_position 推定 native 已完成。崩溃发生在 native unit-end 已确认而 Dataset cut 未确认之间时，仍重用原 native publication receipts。

部分 publication 已确认而 worker cut 丢失时，恢复的 native offset 可能已经包含这次输出。旧逻辑再次递增会漏掉下一个 ordinal。现在先从同一原 receipt 恢复其 output_ordinal，再做原递增；同时验证 occurrence 和 source_position，不把另一 unit 的旧 receipt 当成当前进度。未知请求、pending、原 QueuePublishSink/CommitCoordinator/SQL generation 与幂等守卫均保留。

## 真正执行的业务

`tests/runtime_v3_queue_business_scenarios.py` 用公开 Dataset API 跑以下组合，默认始终 4 workers：

1. 两个固定关键词经真实 native search，每条返回两条候选；flat_map 产生四条任务，再用原 enqueue 发布和封池。
2. 真实 read_queue→RawFetch→大小/SHA 校验→保留原 delivery 的纯 map→QueueResultSink 原生结果与 ACK。三个 HTTP 对象成功，一个固定 404 作为预期失败。
3. 从原 native completion/receipt 的固定 queue snapshot 读历史，校验已结算事实和原 ObjectRef 字节；三个成功对象各展开两条记录。
4. 六条记录各执行真实 `agentmap_async` 两轮：模型请求原 `read_documents` 工具，平台实际执行，第二轮检查工具确实返回正文。共十二个请求，原 run 请求预算恰好消耗十二。
5. 真 write_lance(create) 后，固定版本 1 read_lance→map 后处理，校验六个完整 ID、答案、原内容摘要。

外部 search/model/HTTP 对端由私有本地服务器替代；引擎、source、RawFetch、队列原生提交、模型循环与工具、Lance、恢复镜像、资源管理均是真实现。没有为调度手工 materialize，没有把模型/Lance 塞进不受支持的自动 ACK 拓扑。

五组场景为 normal、recovery、explicit、unknown、boundaries。恢复点在已有真实非 final Dataset ACK 且只完成部分 native tasks 后，通过异常中断，再使用同一 RunRef 恢复；保留原 commit 前缀及 sealed plan，所有对象仍各 HTTP 一次。未完成的原 lease 等待正常过期，未强行撤销旧 epoch。explicit 使用原 ack_queue 合法尾部。unknown 在真实 HTTP 已返回、native result 未固定处注入失败，删除本任务缓存后换新 RunContext，同一 native source 仍拒绝重放，HTTP 总数保持一。

`tests/runtime_v3_queue_boundary_scenarios.py` 另验空开放池不是 EOI、模型/Lance/flat_map 非法自动 ACK 拓扑在 claim/HTTP 前拒绝、原 raw receipt 已确认后 lease 过期不能最终结算、v1 archive 逐字只读且仍标 legacy_not_certified。只有 fault 注入与原提交观察器使用委托钩子，没有替换成功的权威行为。

## 已执行与阻塞

| 证据 | 结果 | 原始路径 |
|---|---|---|
| producer 单位正常、source park 后、首条 native publication 后、native unit-end 后 | 4 passed / 159.15s；每单位原 output ordinal 都是 0,1,2；总计仅两次 search | `/tmp/demiflow-producer-unit-r1.xml` |
| 原两条 search 历史恢复 + helper 声明 | 7 passed / 60.19s，未改 worker 或旧用例 | `/tmp/demiflow-producer-search-history-r2.xml` |
| 最终固定源码 Local 全门 | 1 passed / 293.08s，五组全过，19 次真实 execute_action，无 ray.data | `/tmp/demiflow-queue-full-local-r3.xml` |
| 同源码 Ray 全门 | 1 failed / 266.54s；normal、recovery 全链通过；explicit 编译拒绝，后续两组未运行 | `/tmp/demiflow-queue-full-ray-r4.xml` |

原日志、原 SQLite/journal、Lance 和完整报告均保留在 `/tmp/demiflow-queue-full-local-r3-business`、`/tmp/demiflow-queue-full-ray-r4-business`，不被通用 fixture 的删除步骤带走。每个报告都校验运行前后源码一致。只读枚举全部 `run.sqlite` 的原 resource_allocations：Local 113 条、Ray 83 条，全部 terminal=1 / cleanup=CONFIRMED；Ray 每条都有原 ray-actor-dead 证据。两者私有进程树最终 children=[]。Ray 的 local capacity 目录为零不代表没有分配，报告以这 83 条原 journal 事实为准。此功能验收没有宣称对每个 Ray 业务 actor 都做独立 PID/birth 采样。

Ray 未闭环项：read_queue→fetch→pure map→ack_queue→run_stream 在 4 worker 下留下 5 个 memory-connected stages。queue 是 lease_queue，不能安全 park；当前 Ray 不采用 Local 已支持的 async suffix co-location，因此 RESOURCE_INFEASIBLE。总控拥有后续物理规划窄修，不能以删除 pure map、增 worker、允许 queue parking 代替修复。完整有界 graph、kernel traits/profiles、stage/exchange、policy/hosts/bounds 位于 `/tmp/demiflow-queue-ray-explicit-plan/plan.json`；同目录 `probe.py` 是原 API 最小复现，`outcome.json` 绑定源码，实际 HTTP 为零。

首次历史 search 红例是候选漏了总控已交付的 immutable helper default 修复，合入 92d9474 后原用例通过。早期 Ray 启动失败分别是测试启动器在 import ray 后才设 auth mode、env-ray 缺 native search/model 依赖；均保留原失败日志，不记为产品回归或成功。更早的边界失败来自 harness 的 pytest.raises 范围和封存前 fault hook，均纠正后由最终 Local 门覆盖。曾有一次诊断因自己在运行期间改测试源触发 CODE_MISMATCH；后续固定源码运行并保留前后摘要，不把该次计入回归。

## 可复跑入口与依赖

运行目录必须是上述隔离树。Local：

```bash
DEMIFLOW_RESOURCE_ROOT=/tmp/demiflow-i09-local-domain \
DEMIFLOW_V3_PROVIDER=local \
DEMIFLOW_V3_QUEUE_OUTPUT_ROOT=/tmp/demiflow-i09-local-business \
PYTHONPATH="$PWD:$PWD/tests" PYTHONDONTWRITEBYTECODE=1 \
/yzp/zhaozy/yangzepeng/0905/env/bin/python -B -m pytest -q \
tests/test_runtime_v3_acceptance.py::test_queue_full_business \
--basetemp=/tmp/demiflow-i09-local-tests --junitxml=/tmp/demiflow-i09-local.xml
```

输出目录必须为新建、直接位于 `/tmp` 下的 `demiflow-*` 名称。Ray 同命令改 provider=ray_core，并使用 `/tmp/demiflow-queue-ray-env-20261116/bin/python`。这份私有 venv 通过标准 `venv --without-pip` 创建，自己的 `.pth` 仅依次引用既有 env-ray 和 normal env 的 site-packages；没有复制包或安装依赖。原 native search 会清空传入 PYTHONPATH，因此单纯 driver 的低优先 fallback 不够，必须让原 sys.executable 子进程也拥有此配置。真实 driver/另一实际 Ray worker 记录的八项模块来源和版本完全相等；Ray 2.60.0、Arrow 25.0.1、Lance 12.0.0 仍来自 env-ray。原生业务 actor 继续执行原 code-pin。依赖记录 `/tmp/demiflow-queue-full-ray-r4-business/dependency-provenance.json`。

启动器只建自有单机 4 CPU / 0 GPU / 128MiB object store 的 Ray，token 仅在内存/环境中，不输出；结束仅关闭自己的实例。完整场景有 480 秒外层期限，超时先请求原引擎清理，再只处理本次拥有的进程。业务各 RunContext 的原有限预算不因此变成无限。

本轮没有验证 PostgreSQL、两机完整队列业务、这个 fixture 的工作台 UI 或业务 SIGKILL。非 final ACK 异常恢复和三个 native 断点是真实恢复证据，不能把它说成 OS kill 覆盖。性能正式五轮仍等待总控固定候选，未因本功能链路通过而宣称性能完成。
