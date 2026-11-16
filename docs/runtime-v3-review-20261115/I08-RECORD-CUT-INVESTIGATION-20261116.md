# I08 局部性能调查（已完成窄范围调查；候选撤回）

范围：仅原 20,000 输入 → 10,000 输出轻 CPU 用例、相同最多 4 workers/256 MiB/120 秒，保持 40 原 ACK。不再试 socket 或扩大引擎重构。

## 1. 逐行构造的真实调用链

`source_readers._BaseReader.next_record` → `_encoded_size` → `_encode_payload` → `Record`：20,000 次源 Record。

`WorkerGraphRunner._output` 在同一 stage 直接 `_push`；`OperatorKernel.push` → `_task_record` 在 task_origin 节点产生输入批次 provenance：20,000 次 `clone_record` → `dataclasses.replace` → 原 Record 构造。后续 Filter/Map 只核验传播的 group/ordinal，不再产生该 clone。

`invoke_unary(MapOp)` → `_materialize_row`（隔离传入用户回调的可变行）→ 原回调 → `_output_record`（字节预算验证、冻结输出）→ `Record`：第一 Map 20,000、第二 Map 10,000，合计 30,000。Filter 返回原 Record；`preserve_block` 在不变 sidecar 上已复用原 Record。

所以业务线程 profile 的 70,000 次 Record 构造 = 20,000 + 20,000 + 30,000。不能把 Filter 或 preserve_block 误判成又造了一份记录。`operator.push` 的 90,000 profiler call 包含生成器恢复，不等于 90,000 次业务输入：真实 transform 输入 50,000，产出 yield 40,000。

源码边界保持：源输入/新业务输出必须做深层复制与冻结，不能因为 Record 标注 frozen 就信任未校验的外部值；task provenance 必须在原批次/actor生命周期边界生效。此前仅约 0.093 秒潜力的 clone 特殊构造已撤回，不重新引入。

## 2. 原用例已融合；其他合法阻挡边界

原封存 `PhysicalDataflow` 已是 5 logical nodes → 1 stage/1 partition；CPU 1000m、memory 1,802,240 字节。4 条逻辑边均同 producer/consumer partition，Runner 直接调用原核，不经过中间 peer 网络。因此扩大融合不能解决当前这条用例。

- `dataflow_fusion.fuse_single_worker_pipeline`：需要纯、无状态、单源单终端线性、单 partition、单 kernel stage；异步/流式/定时 flush 不和阻塞 source 融合；显式 GPU/隔离/requirements/limits 保留；汇总 resident + control 必须在原额度和单个真实 host 内。足够 worker 的短链保持独立可放置，这一策略影响别的短链，不影响本用例。
- `dataflow_thread_fusion.fuse_thread_suffix`：保留独立 source，有限合并 suffix；各原 kernel/task/state 仍独立；纯同步 CPU 可取 max CPU，实际并发/状态 suffix 保留 sum CPU；仍按所有窗口共存相加内存和单 host 准入。
- `dataflow_finite_fusion.fuse_finite_suffix`：仅有限 CPU producer 与同步后缀；不搬 source/异步/device/显式隔离，不越有限输入所有者。
- `dataflow_place.select_fusion_groups/_fusible`：原 profile、partition 数、pure/none/mergeable、单线性边与累积驻留窗口均需一致；不能跨多输入/源/终端边界直接拼。

## 3. 358 个 worker put 并非重复相同文件

原业务线程 `dataflow_store.put` caller：

| caller | 次数 | 意义 |
|---|---:|---|
| `OperatorKernel.snapshot` | 120 | 3 个核 × 40 cuts，含输出游标/task frontier |
| `TerminalBlocks.flush` | 40 | 每次 cut 新增终端数据尾部 |
| `SegmentForest._put` | 78 | 40 个当前根封存 + 38 个二叉归并节点 |
| `WorkerGraphRunner.snapshot` | 80 | 40 frontier + 40 sink manifest |
| `WorkerGraphRunner.step` | 40 | CutContribution 控制运输载体 |
| 总计 | 358 | 同时对应 40 原提交边界 |

只读原 SQLite 对照：120 kernel image、78 segment 等各自 count == distinct digest，不能靠无界缓存或跳过每 cut 快照“去重”。原工件路径包含 begin 准入、写入、prepare 意图、发布、settle，未知回复不退账的语义必须保留。

1192 次 artifact RPC 的权威实际 dispatch 累计约 3 秒；queue/reply 等待约 0.45 秒。journal 已复用连接；没有现成批量 artifact API。累计 profile 时间相互包含，不是可相加的改造收益。

## 4. 按可验证整链潜在影响排序

1. **同一个终端 Record wire 重复编码**（确有冗余，但候选因共驻预算反例撤回）。`WorkerGraphRunner._push` 的原 `_encode_record` 完整验证并生成 wire 包入 spool，随后 `_publish` 又从同一 Record 生成相同 wire。原 20k 对照恰为 20k wire 编码、其中 10k 重复，累计约 1.312 秒；其中一半只是潜在可消除时间上限，非收益承诺。私有 `dataflow_sinks._encode_record_frames` 返回 `(spool_bytes, wire_bytes)`，原 `_encode_record` 保持返回 spool；终端立即通过原 `_publish(validated_raw=...)` 消费本次 wire。完整首次 Record/tree/encoded_bytes/封装长度验证、字节/身份/sidecar、原 row/spill/output_rows/credit/ACK/持久化均保持。无跨记录缓存，不改 DTO。生产 `7b11585`；同生产最终测试 `2b98a35`：实际 worker 编码 2→1、类型/负零/同字节、wire 有界而 spool 超限的 0 发布/0 commit、原 nonfinal ACK 恢复全部通过。原 codec/route/peer/fusion 67 项同生产通过。同FS两暖四交替仅观测约0.201秒/1.52%差异，波动更大，未建立显著收益；随后真实block切换反例证明新增wire共驻超原窗口，5aceb0f已完整回退生产及特定候选测试，不能采纳7b11585。
2. **小 CutContribution 运输载体**（本轮不实现）。40 个控制元数据文档各 2021–2025 字节，worker 持久化只为发送 ref；原 coordinator 已把完整 contribution 放进最终 commit states，恢复不依赖该临时运输 ref。若复用 WorkerEvent 已有 bounded payload，可省 40 put/120 artifact RPC；profile 该 caller 累计约 0.402 秒。先证明控制窗口总驻留、大小/数量、encode/decode、boot/participant/fence/幂等/unknown 行为；超上限继续原 ref。需修改 Runner.step 和 DataflowExecution._handle_event，总控要求先不动后者，避免与图恢复交叉。不是新 commit 协议或持久化权威。
3. **Record 深层重构/批量执行**（当前没有可直接证明等价的局部候选）。必须区分可变业务行的隔离、每行大小验证、原task/actor生命周期、每逻辑节点指标、filter/expansion语义、cut位置。不建议删构造/绕校验；证据只足以定位成本，未证明安全收益。

取证 `/tmp/demiflow-terminal-encoding-investigation.json`，绑定原 profile、原 journal 只读查询、原 graph/physical 文件摘要。


## 5. 测量更正与候选撤回

第一轮同主机但异源码文件系统：baseline 在 `/tmp` 本机 xfs，candidate 在 `/yzp` dpc 共享盘。原观测 12.6938413145 → 13.448915152 秒（+5.95%）**不能归因于代码**。原记录保留：`/tmp/demiflow-terminal-encode-comparison-r1/report.json`。此前 iovec 报告已在独立文档提交 `095d307` 纠正同一混杂，不将其 +1.11% 判作代码变慢。

同FS纠正：用 Git 将固定候选 `2b98a35` 放到 `/tmp/demiflow-terminal-wire-candidate-20261116`，基线 `b91b3a9` 在 `/tmp/demiflow-record-copy-baseline-20261116`。两树同 xfs device 66318、相同解释器、相同 benchmark/pyproject、0 pyc、显式进口路径与3个私有环境变量，唯一生产差异为 sinks/runner 两文件。每次运行原预算、完整输入输出与清理不变，2次暖机后按 B/C/C/B 测量。

| 臂 | 测量1(s) | 测量2(s) | 中位(s) |
| --- | ---: | ---: | ---: |
| baseline | 13.055706279 | 13.325118772 | 13.1904125255 |
| candidate | 12.823296236 | 13.155267546 | 12.989281891 |

观测差约0.201秒（1.5248%），两臂内部波动约0.27–0.33秒，不能宣称显著加速，更不能据此通过原20%或完整五轮性能门。六次全为原10,000行完整相同摘要、40/40原ACK、原资源池active/charged清零、0子进程残留、测试前后源码不漂移。

完整命令、环境、source manifests和资源采样在每次trial.json；索引 `/tmp/demiflow-terminal-encode-samefs-comparison-r1/report.json`。只读环境核验 `/tmp/demiflow-terminal-samefs-alignment.json`。执行脚本 `/tmp/demiflow-terminal-encode-samefs-comparison.py`，固定解释器 `/yzp/zhaozy/yangzepeng/0905/env/bin/python -P -B`，各运行数据目录均在该报告目录下的独立 `/tmp/demiflow-*` 子树。

### 后续内存审查推翻了采纳前提

原 `_encode_record` 返回 spool 后，内部 wire bytes 已释放。新 helper 让 `delivery_raw` 跨过 `TerminalBlocks.append`，而切换 block 的 append 会先 flush 旧 tail。两个独立 `max_row_bytes` 校验不足以证明同时驻留符合原 `max_batch_bytes + max_row_bytes` 窗口。

固定真实反例 `64a4730`，2条约6KB行、2个逻辑block，`max_row_bytes=max_batch_bytes=8192`：

- 旧tail真实读取chunk 6,494字节 + 当前spool 6,494字节 = 12,988 ≤ 原payload 16,384。
- 候选额外活跃wire 6,332字节，使总数19,320 > 16,384，超2,936字节。
- 原 Dataset 正常输出且ACK，说明此前大小/恢复测试漏了这个共驻边界；这不是RSS峰值推算，而是实际flush消费点的活跃编码缓冲字节下界。

证据 `/tmp/demiflow-terminal-flush-red-evidence/verification.json`。原生产在 `5aceb0f` 回退后，同一守卫 **1 passed**（3.70秒），`held_wire_bytes=0`；证据 `/tmp/demiflow-terminal-flush-baseline-evidence/verification.json`。`demiflow/` 与 `b91b3a9` 字节全等。为噪声量级的性能差异增加新计费或条件快路不值得，本微优化收口。

### 证据纪律与待接入交付

- 初次计数测试 `fed4ec6` 只在父进程打探针，得到0，属于测试观测错误，不能证明重复编码；`ffff41a` 改为真实worker观测后，10个record逐个出现2次，才是有效红基线。
- `9970739` 组合70 passed/1新夹具失败：原67项codec/route/peer/fusion通过；失败夹具没有把探针与终端放到同一worker，并在RunContext内部catch后误以为run可成功结束。`2b98a35` 固定夹具后新4项全部通过。以上仅证明功能与所覆盖的边界，不能覆盖后来发现的内存共驻问题。
- 可单独挑取 **`64a4730`**：新增 `tests/test_runtime_v3_terminal_flush_budget.py`，在原实现绿，实际观察原store chunk，不替换内核/allocator/提交，不改变预算。
- 不挑取候选生产 **`7b11585`** 或候选专属编码次数测试；其撤回提交为 **`5aceb0f`**。
- 文档及本JSON独立交付；原生兼容先前交付与模型恢复证据保持，不把本次候选回退误写成原生兼容功能回退。

## 6. 另外发现的原有flush窗口缺口（转总控，未在本轮擅改）

对上述守卫仅将输入从2行扩到6行、仍2块/同8192行与批预算，使用已回退的原实现 `5aceb0f`：旧tail实际chunk19,482字节 + 当前spool6,494字节 = **25,976 > 16,384**，且 `held_wire_bytes=0`。真实Local输出/ACK完成后观测断言失败。源码 `TerminalBlocks.flush` 用 `min(65536, max_row_bytes + max_control_bytes)` 读取旧tail，没有按原 `max_batch_bytes` 窗口收紧；这与已撤回的额外wire问题独立。

探索取证 `/tmp/demiflow-terminal-oldflush-real-deps-evidence/verification.json`，探针源码与摘要 `/tmp/demiflow-terminal-oldflush-probe-source.json`。探针使用 env-ray 解释器只为有真实依赖，实际运行仍为Local，没有启动Ray业务。第一探针用普通env且文件在/tmp，不经过repo conftest，因缺ray.exceptions未进入业务；该失败单独保留 `/tmp/demiflow-terminal-oldflush-evidence/verification.json`，不拿它作为预算结论。此旧缺口已报告总控，未靠扩大grant处理，未扩展本轮微优化范围。
