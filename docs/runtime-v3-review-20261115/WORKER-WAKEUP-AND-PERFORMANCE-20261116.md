# Worker 唤醒修复与性能归因（2026-11-16）

结论：两处真实失唤醒已修，历史、资源和提交边界保持。共享 control birth 的新传输格式没有证明整链收益，已从本工作树撤回，不建议合入。I08 性能目标仍未完成。

工作树：`gpt_v3_residency_20261115`；分支：`engine/gpt-v3-residency-20261115`。主仓未写入、未测试，未 push、未操作非本任务服务。所有临时业务数据与探针在 `/tmp/demiflow-*`。

## 可集成改动

| 提交 | 内容 | 性质 |
|---|---|---|
| `6fe61ca` | `WorkerGraphRunner._run` 在 step 前记录原 condition 的通知序号；step 无工作后，仅在序号未变、未取消时沿用原 5ms wait。`receive`/`wake` 在同锁下递增序号。 | 生产修复＋同步反例 |
| `68d99b3` | `_publish` 在检查信用前记录同一通知序号；ACK 已释放信用时，不在错误分支丢失通知后再等原 10ms。原 reserve/peer release/credit release/新发布顺序不变。 | 生产修复＋实际 publish/ACK 反例 |
| `a4a6fbe` | 迁移 3 个旧融合测试，并新增独立 worker 资源授予正例；不改生产。 | 独立测试修订 |

不改变 checkpoint、语义 identity、Wire DTO、历史编码、信用额度或等待时长。没有增加另一份调度/资源/提交权威，没有通过缩短 sleep 或空转提速。原 7 件冻结文件与 `362d8528c27ae60a0d6ae0ccc986a0ff93847de8` 逐字一致。

## 红绿证据与真实闭环

执行解释器为 `/yzp/zhaozy/yangzepeng/0905/env/bin/python -B -P`，`PYTHONPATH` 显式指向本树及本树 `tests`，`PYTHONDONTWRITEBYTECODE=1`。

1. step/notify 原实现：用线程屏障把 command、wake、cancel 放在“本次 step 已观察不到工作”与 wait 之间，三条都在通知已发生后仍进入 wait。`/tmp/demiflow-worker-wakeup-red.log`：**3 failed / 2 passed**。无依赖机器速度的毫秒断言。
2. credit/ACK 原实现：真实 `_publish` 与真实 `receive(ack)`，在容量检查和 wait 间释放旧 buffer。`/tmp/demiflow-credit-wakeup-red-r2.log`：**1 failed**，实际错误为多执行一次 `wait(.01)`。首次探针因 Record 测试 fixture 的 encoded_bytes 不正确未到达该缝，原失败档保留，不算红基线证据。
3. 两处修复及原 idle 通知组：`/tmp/demiflow-credit-wakeup-green-r2.log`，**14 passed**。
4. 固定 `68d99b3`，`/tmp/demiflow-worker-wakeup-acceptance-r1/`：**21 passed / 3 failed**，运行 94.05s。通过项是上述 14 项、**真实 6 个提交崩溃窗口**、**真实 driver 重启拒绝旧 worker 事件**。`verification.json` 绑定 2148 条源码/测试摘要，前后全等，7 件冻结件全等，进程 exit 1 正确反映那 3 个旧 fixture 失败。
5. 那 3 个失败用 **全部生产字节等于 `53c8010`** 的对照再跑：`/tmp/demiflow-worker-wakeup-old-three/`，同样 **3 failed**，10.53s。不是 wake 引入。
6. 固定 `a4a6fbe` 迁移后，`/tmp/demiflow-worker-wakeup-fixtures-fixed-r2/`：**4 passed**，17.63s、exit 0、2148 条摘要前后全等、冻结件全等、工作树干净。

以上组成明确的 **25 项覆盖**；不是声称“原带 3 失败的集合全绿”，也没有为了新测试文件重跑已过的 6 个崩溃窗口。本轮未重跑真实 Ray/GPU；原 Local/Ray 共用同一个 runner，Ray 仍需 root 合流后的真实功能验收。

### 旧测试为何迁移

- `test_plain_iterator_map_early_close_keeps_source_independent[thread/process]` 的实际 take(1)、源仅读 6 行、源与后缀 2 个独立 allocation 均成功；旧 helper 最后仍断言 fused CPU 必须为各节点之和，和当前“纯同步节点共享一个 worker CPU、共存内存求和”的契约冲突。只对该纯同步正例按 max 验 CPU，保留其他非该形态的 sum 断言，并读取原 `resource_allocations.effective` 核实实际授予的 CPU/内存。新增 `fusion=False` 的三个独立 worker，核实实际授予总量 **3000mCPU**，没有把各独立 worker 也改为 max。
- 重复 delivery 反例此前以 MapOp 为注入目标，合法融合使这个跨 worker 接收点消失，导致**根本没有注入重复**。现通过原 policy `fusion=False` 保住真实传输缝；仍要求重复/乱序错误、恰好注入一次、业务只调用一次，未改拒绝断言。

## 性能实验的去留

原 workload 始终为 `fast_cpu`：20,000 输入、10,000 输出、4 worker 上限、256 MiB、原 120s deadline。输入输出摘要未变；没有改 checkpoint/窗口、用 thread 替代 process、融合任意 from_iter 或削弱恢复。

| 固定版本 | 证据 | 结果与范围 |
|---|---|---|
| `53c8010` | `/tmp/demiflow-perf-batch-distribution-r2/fast_cpu/` | 仅 driver profile：36.526s，52,776 事件。95.3% 事件处于 driver 已就绪多条批次。 |
| `dd40aea` | `/tmp/demiflow-perf-shared-birth-r1/fast_cpu/` | 新瞬态 birth batch 整链 27.337s，正确输出，但相比前阶段 26.983s **无实质收益**。 |
| `1e977cd` | `/tmp/demiflow-perf-shared-birth-profile-r2/fast_cpu/` | 修复候选拆散跨帧读取后再次完整 profile：38.012s。worker 真批 5,863 次、47,713/50,000 业务事件，批机会真实；driver poll 从旧 profile 11.52s 变为 10.70s，节省不足解释整体差距。profile 耗时不是正式同比。 |
| `3c572ef` | Git 撤回记录 | 撤回上述两个 wire 实验提交；后续生产恢复原 control 格式，实验提交/原始证据仍可追溯。 |
| `6fe61ca` | `/tmp/demiflow-perf-worker-wake-r1/fast_cpu/` | 仅第一处 wake 修复，原整链 25.880s、10,000 行正确、源摘要不变。单次诊断，不能证明稳定的 4% 改善，更不能称 I08 达标。 |

新格式没有因为局部微基准好看就合入。原 `encode_wire` canonical 字节始终未改。

## Source worker 的真实归因

固定 `a4a6fbe`：`/tmp/demiflow-perf-source-worker-profile-r4/fast_cpu/`，实际 source 进程 **56594**，原数据与预算，原 10,000 输出摘要正确。完整证据 `verification.json`；原 cProfile 为 `source-worker-56594.prof`，文本为同名 `.txt`。

诊断模块 `/tmp/demiflow-source-worker-profile-support/owned_profile_harness.py` 是原 harness 的私有副本，只添加 source profiler 生命周期及显式 OWN_TREE；差异保存在 `probe.patch`，摘要由原 snapshot 记录。原 harness 文件未改。父进程单独 monkeypatch 的前几次尝试被原 code-pin/模块来源守卫拒绝，0 业务行；未放松守卫，改为父/worker 导入同一份实际模块字节后成功。

profile 覆盖 source 生成器首次调用到最后 EOI，包含让出 generator 后在同 business thread 上执行的 runner。执行时长 **30.028s 含 profiler 开销**，不能作性能比较。表中为包含时间，不相加；profiler 初始化/落盘在覆盖外，disable 自身仅约 0.6µs。其余插桩开销未校准，不能伪称已经扣除。

| 原函数 | 次数 | profile 包含时间 |
|---|---:|---:|
| 全覆盖 | 14,083,542 function calls | 24.555s |
| 原 condition wait | 4,228 | 15.373s |
| 其中可直接归属 `_publish` 信用等待 | 1,893 | 7.333s |
| 其中可直接归属原 journal RPC 等待 | 1,001 | 2.025s |
| `source_readers.next_record` | 20,000 | 2.332s |
| `_route_record_document` | 20,000 | 2.725s |
| 原 identity | 60,343 | 1.287s |

剩余约 6.015s wait 的 caller 属于 profiler 启用前已存在的调用栈，没有可直接归属的完整 caller 记录；不能把它硬判成某一种 idle。Source 主要等待下游/driver，source 自身编码微优化的收益上限低。任意 from_iter 仍保持独立，不能拿融合源阻塞下游的退步换分数。

下一步是同 workload 的同步后缀 consumer profiler，定位真实 peer 读取、逐节点 Record、terminal spool 与 credit 等待关键路径，再决定生产修改；截至本记录还没有 consumer profile 结果。
