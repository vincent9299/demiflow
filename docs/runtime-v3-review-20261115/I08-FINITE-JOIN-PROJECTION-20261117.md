# I08：有限输入 join 的已验证文档复用

生产提交：`87cc0c301504715b451e68c11339c80c3c788ff5`，基线 `048c07a`。
工作树：`/yzp/zhaozy/yangzepeng/0905/_demiflow/worktrees/gpt_v3_perf_mainpath_20261116`。
分支：`engine/gpt-v3-wide-mainpath-20261117`。主仓未修改或运行测试；未 push、未合主仓。
机器证据：同目录 `I08-FINITE-JOIN-PROJECTION-20261117.json`。

## 结论与范围

当前基线的原两条 40k×8KiB 双侧 join **已经通过**，不能沿用旧版本超时结论。
本次在相同数据、256MiB pool、2GiB RSS 声明、120s deadline、2 threads、1/2 partitions
以及原完整值验证下，减少了有限输入交给 join 时的重复 payload 解码和编码。
原 Record、typed key 语义、原 join 算法、原 artifact/journal/cut/ACK 都保持。

这是通过功能和恢复验收的性能候选，**不是 I08 正式性能门关闭**。
正式要求仍是 PERFORMANCE-PLAN 的批准历史基线、预热、交替五轮及资源采样；本次没有完成这些条件。
也未把 Local 的宽行时长推断成 Ray 宽行时长。

## 归因证据

原用例从 Python `from_iter` 构造输入，不是 Lance 扫描。
既有 Arrow carrier 仅在部分单源直通 terminal 的 Lance 路径启用，单纯扩 Lance join 白名单不会改变此用例。
没有复用另起执行进程/资源权威的旧 DataFusionSession。

诊断 cProfile 只包真实 worker 的 `WorkerGraphRunner._run`，不是驱动或 bootstrap 主线程：

- 两个 source 各完成 40,001 条，worker 累计约 39 秒；此前“source 尚未完成”的旧诊断不再适用于当前基线。
- join worker 的 `validate_finite_members` 先扫描约 22.1 秒，随后 `borrowed_records` 再扫描约 22.8 秒。
- `_decode_record_document` 调用 181,574 次，累计约 32.3 秒；第二扫描后 `_Spool.append` 再编码原宽 payload。
- 上述是包含子调用的剖析数据，不能相加当总 CPU 时间。cProfile 用例自身因 120s 超时失败，不能拿它当候选性能通过。

原始记录在 `/tmp/demiflow-wide-profile-r1`，摘要为 `worker-profile-summary.json`。
临时诊断测试已归档到该目录，没有作为一个默认超时测试交付到仓库。

## 代码契约

1. `dataflow_finite_input.py` 的完整 scope 扫描仍检查成功 seal、全部 producer、generation、
   原 manifest/ref SHA 与字节数、各条 Record/provenance/schema 引用、routing、未选中的行和总计数。
   只有整次扫描结束后才返回带本进程证明的 `_ValidatedFiniteEntry`。
   证明绑定原 store 对象、完整不可变 WorkerAssignment（run/action/attempt/fence）、bounds、spec
   和完整 member descriptor；原 ref digest 间接绑定每条 schema/payload 文档。
2. `_SpoolPort` 仅在内存保留证明，原 snapshot 中仍是原 `borrowed-finite-input.v1` 普通字典。
   恢复没有证明，使用优化前必须重新完整验证原 scope。证明不能跨 assignment、store 或描述符变更复用。
   证明元数据若不能在原 state 窗口内共存，走原 Record 路径。
3. 普通 `records()` 仍迭代真实 Record。只有 join 的私有 `for_join` 消费已验证成员时可拿到
   `_EncodedJoinInput`：原 canonical document 字节和原 typed codec 解码出的 join/routing key。
   第二次读取仍核验真实字节 SHA、size、route、scope、成员和记录数。
4. 原 join 在两边输入全部读完并校验完之前不产出任何输出。投影载体将原 document 原样写入同一
   journal-owned scratch；命中后仍调用原 `_decode_record_document` 构造真实 Record。
   原 NULL、重复、左右不同 key 名、bytes/tuple、-0.0、热点多重度和 key 编解码没有替换。
5. 投影前以 `(32 + 4 + 8 × key字段数) × document字节数` 对原 join row window 作保守峰值预检，
   覆盖 JSON 容器/投影、外层帧和 scratch 副本；不扩大预算。
   大单行、外部 payload ref、重复 key 声明、metadata 紧限等不适配情况返回同一 kernel 的原 Record 路径。
6. 没有新增 payload 文件格式、状态账本、提交权威或 GC 规则。原 borrower 仍只引用原 producer 工件，
   不负责释放 producer 引用；只有原 scratch 的原 staging reservation 随成功/失败清理。

生产范围仅四个文件：

- `demiflow/execution/dataflow_finite_input.py`
- `demiflow/execution/kernels/operator.py`：SpoolPort 和私有迭代器；未改 actor 生命周期或 finish 分支。
- `demiflow/execution/kernels/join_spool.py`
- `demiflow/execution/kernels/relational.py`：选择私有 join 迭代入口。

新增测试 `tests/test_runtime_v3_join_projection.py`。没有修改或放宽历史测试。
七个冻结文件与 `048c07a` 逐字相同，机器报告附核验。

## 验收

所有正式调用均经原 `runtime_v3_regression_scenarios.execute_batch`，包含 pytest 进程内导入根断言、
三个精确私有环境域、私有 TMPDIR、setup/call/teardown 和运行前后输入摘要。

| 批次 | 结果 | 说明 |
|---|---:|---|
| 原基线 wide-current-r1 | 2 passed | 原 p1/p2，未经 cProfile |
| 初版 join-project-r1 | 21 passed | 类型、路由、损坏、恢复和原 3 个宽 payload 功能用例 |
| 初版 join-project-r2 | 2 passed | 原 p1/p2 |
| join-project-final | 46 passed | 全部 20 个原 finite borrow 历史、20 个新反例、原 5 个宽 payload、原 ACK 恢复 |
| **固定 87cc0c3：join-project-bound-final** | **23 passed** | 最终 assignment/fence 绑定与清理断言、原 p1/p2、真实 Local ACK 中断恢复 |
| **固定 87cc0c3：join-project-ray-final** | **2 passed** | 实际 Ray Core typed finite borrow、原 finite join ACK 中断恢复 |

46 项批次之后仅补了显式 WorkerAssignment 对象绑定和相应负例/计费断言；最终固定提交的 23+2
重新覆盖全部新增测试、原两条宽行和 Local/Ray 恢复。旧批次没有冒称绑定最终提交。
最终两份收据包含的五个交付文件摘要均与生产提交逐字匹配。

关键新反例包括：所有五种 routing、恢复必须重新完整验证、跨 store/assignment 拒绝、descriptor 改动拒绝、
最后一个 member 损坏时零 join 输出且原 scratch reservation 释放、原 producer 计费不退、
metadata/row window 不足时保持原路径、原 schema/provenance/document 字节一致。

固定提交 Local 收据：`/tmp/demiflow-join-project-bound-final/batch/receipt.json`
SHA256：`eac96435accf776d77d6b87a50878f7e0a0c57499d7b59397d7761a0a082f584`。

固定提交 Ray 收据：`/tmp/demiflow-join-project-ray-final/batch/receipt.json`
SHA256：`100d39a6b3bd5862f6282c7ba647b26dd94d30d3e8c5fff73915ef8e0ef66ebe`。
Ray 使用已审 optional-env 和自己的 `address='local'` 集群，结束调用原 provider 的 `ray.shutdown()`。

## 性能观察的限度

| 原用例 | 基线 048c07a，单次 call | 固定 87cc0c3，单次 call |
|---|---:|---:|
| p1 | 119.335s | 99.527s |
| p2 | 114.697s | 98.280s |

两边均在原 `/tmp` 文件系统、相同预算和完整原 oracle 下运行。
这是约 14–17% 的单次改善信号，call 也含部分建图/清理；不是已控制噪声的五轮性能结论。
宽 payload 每条原值、40,002 条结果、NULL、重复、物理 budget 和真实 bounded join index 检查都未改变。
本 lane 没有单独宣称新的实测进程树峰值 RSS/FD 数字。

接下来应先与当前 transport 变更合流，再按批准方案测实际 `finite_sealed` iter_rows companion 和其他性能场景。
仍未完成的正式五轮、Ray 原宽行性能及组合候选性能都在 JSON 的 `not_run` 中明确列出。
