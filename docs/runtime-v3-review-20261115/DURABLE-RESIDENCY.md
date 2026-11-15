# 有界持久增量分期执行

本轮在 `engine/gpt-v3-residency-20261115` 隔离分支实施；主仓不参与编辑、运行、测试。有限分期、设备授予先前分别见 `FINITE-RESIDENCY.md`、`DEVICE-GRANTS.md`。

## 用户行为与意图

普通 Dataset 编译新计划时优先 memory_incremental；若 memory 连通组无法满足驻留预算，且源可恢复、算子声明状态可冻结，则自动尝试 durable_incremental。仍由原物理 solver 验证每个连通组与宿主资源。`ExchangeSpec.reason` 记录 `resident_budget_requires_checkpointed_parking`，实际模式随原 physical plan 封存。调用者不必填写新开关；历史物理计划不会重新解释为另一模式；固定 memory 合同仍要求同时驻留，无法满足必须拒绝。

执行只让当前组驻留；它产出一个有限窗口便提交原 journal cut，provider 确认进程退出后才返还驻留额度。消费者组消费真实 artifact、提交消费 frontier，原生产者随后恢复同一 source cursor / route ordinal / channel credit 状态继续运行。业务行不集中到 driver 执行。Local 和 Ray Core 共用 runner / driver / coordinator。

## C08 增量字段：同一 native cut carrier

`dataflow-cut-v3.residency.schema = demiflow.durable_residency.v1`。此为新执行模式附加记录，既有非分期与 finite_residency 历史读取保留。未改 frozen DTO 或承重七件；没有第二 journal 或第二提交权威。

字段：`epoch`（前一个 ACKed cut.sequence）、`active_stages`、`visited_stages`、`completed_stages`、`phase_complete`、`finite_seals`、`satisfied_source_splits`、`edge_eoi`、`satisfied_inputs`（edge / consumer partition）、`deliveries`。

每个 `deliveries[delivery_id]` 包含原 worker 发来的 `payload`、`consumed`、`producer_cut`、`consumer_cut`。payload 保留 producer/target partition、edge、record_id、record.sequence、route_epoch、route_sequence 和 durable ArtifactRef。原实际 producer frontier 保存同一 payload 与尚未释放的 credit lease；真实 consumer frontier 保存已经处理的 delivery IDs；`emitted_eoi` 和 `closed_inputs` 分别证明实际发送的 EOI 与逻辑输入完成，恢复必须核验。记录 `consumed` 不足以成为证明；提交和恢复都验证实际 consumer contribution。原 producer_cut/consumer_cut、boot_id、contribution.cut_id 不改写为当前代。

停车绝不是消费 ACK。消费者尚未执行时，无 ACK、无完成观察、无贡献。ACK 消息丢失时，真实消费 frontier 可以证明消费事实；没有 frontier 就不能放行。重放排序用原 producer/epoch/route sequence，不使用 canonical JSON 的 delivery-id 键序。只保证对应声明的分区内顺序，多生产者不捏造额外全局顺序。

driver 在 retire 前完成原 CommitCoordinator 提交，之后确认 provider cleanup=CONFIRMED，再重新 place。只在 cleanup 确认时释放 resident charge。恢复保留每个 visited participant 的最新真实贡献，原来源从历史 ACKed cut 核验；源游标与终端累计 manifest 不因当前组不含源/终端而丢失。若业务完成发生在一个非 final cut 的 begin 之后，仍由存活 owner 再贡献真实 final cut，不能改写旧 cut.final。

GC 必须遍历 residency.deliveries[].payload.ref 以及 worker-frontier.durable_deliveries[].ref，并保留被原 producer/consumer cut 引用的真实 frontier。该字段是原 native 提交状态，不是临时文件存在性证明。

## 生命周期与瞬态身份

原 RunResourceState 历史 allocation 表不删除。活跃请求与 max_allocations 仅统计 cleanup 未确认者；已确认历史通过原分页接口仍可读。精确 allocation 查询不再依赖历史第一页 64 条。scheduler 的 retirements / operation-to-allocation / receipts 只在实际确认退出后清除；unknown cleanup 保持占用。资源表中的内部 terminal 位表示无需再次 ensure_present，不能据此伪造业务 EOI。`DriverJournalService.retire_worker(lease)` 独立检查原 resource_allocations 的 CONFIRMED、拒绝未完成 RPC，再清该 boot 的活跃权限与易失 reserve 回复；原 native calls / 预算 / effect / commit 均保留。NativeAcquisitionService 退订后仍拒绝旧 boot，不退回无人校验模式。

worker 控制 RPC、terminal 临时投递 ID 加 allocation_id + boot_id 防止同阶段多次启动复用旧回复。source/record/route delivery/effect/native 请求身份保持 attempt / boot 无关。native journal 客户端没有重置的顺序计数器；已有 boot 绑定的配置/授权 RPC 与稳定业务操作 ID 保留。

## 明确边界

1. 首次 park 之前尚无已提交 cut，既有恢复守卫仍会拒绝从封存文件单独恢复；没有为通过测试放宽准入。已有原 cut 时的下一次 park 前崩溃、park 后崩溃、消费 cut 后崩溃应恢复精确结果。
2. 一个同步算子 invocation 的扩张若超过实际 durable credit window，当前诚实拒绝，不能伪造 ACK。任意无限 generator 中途可暂停状态尚未实现；这不是完整通用的可抢占算子模型。
3. park 等待原生 in-flight 请求获得真实结果后再退出，避免人为杀死已收费请求制造 unknown；未知结果仍由原 native journal 拒绝重发。物理 GPU 推理未验证。
4. 这是 correctness 优先的最小调度：一组一个有限窗口便可能启停进程，吞吐优化（更大但仍有界批次、空闲宿主复用）尚未声称完成。
5. 宿主亲和性等造成分期需求时的组合仍需更多实机验证；有 durable 输出时 source quantum 受真实 credit 窗口约束，不能把 planner 可行性当实际设备授予。

## 验证

集中 Local 回归 `/tmp/demiflow-residency-final8.xml`：**71 passed, 3 skipped，320.90s**，覆盖 durable / finite residency、真实资源生命周期、既有 C08 checkpoint、公开 Dataset、设备授予和 native RPC。3 个跳过为该解释器无 Ray；同机真实 Ray Core `/tmp/demiflow-durable-ray8.xml`：**1 passed，21.53s**（本轮新增 durable 路线）。finite / device 的同机 Ray 证据见各自先前记录。

集中组后仅增加“cut 证明增长后再次检查 metadata max_state_bytes”与相应反例；最终源码针对该边界、真实 Dataset 执行、park 前已有 cut / park 后 / 消费 cut 后恢复复验 `/tmp/demiflow-durable-bound9.xml`：**5 passed，73.77s**。不把前一次集中测试误称为这两个末改文件的逐字节最终快照测试。

额外真实反例 `/tmp/demiflow-durable-edges7.xml`：**3 passed，30.25s**（仅清易失 native 回包而保留原 paid 请求与预算、旧 boot FENCED、逻辑 limit 恢复、durable write_lance 恢复）。70 次真实 Local 进程启停、max_allocations=1、原历史 64+6 条分页、unknown cleanup 继续收费也包含在集中组。

执行命令显式设定 `PYTHONPATH=$PWD:$PWD/tests`、`DEMIFLOW_RESOURCE_ROOT=/tmp/demiflow-…`、`python -B -m pytest --basetemp=/tmp/demiflow-… --junitxml=/tmp/demiflow-….xml`。Local 使用 `/yzp/zhaozy/yangzepeng/0905/env/bin/python`；Ray 使用相邻 `env-ray/bin/python`、私有 `ray.init(address="local")` 并在 finally `ray.shutdown()`。测试期间源码与提交均冻结。承重七件与原冻结基线逐字相同，`git diff --check` 通过。中途失败日志不作为通过证据。源分支、多重性、初次无cut拒绝、真实消费ACK丢失、恢复前后顺序、70次资源生命周期与未确认退出反例，以及同机 Ray Core 均单独覆盖。
