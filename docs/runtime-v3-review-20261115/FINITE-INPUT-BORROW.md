# 有限输入直接借用原封存记录

本批满足 I03/I07/I08：真实 finite-complete 算子完整验证原封存数据后复用原引用，去除 producer→consumer 的整份重新编码、重新写入。提交与存储仍是原 RunJournal/ArtifactStore；没有新增恢复账本、提高预算或改变计划/路由身份。

## 实现与边界

- 新 `dataflow_finite_input.py` 检查原 seal 的精确 shape、scope/generation、完整且唯一的 producer membership、成功状态；完整扫描所有 producer 的全部帧，包括目标不属于本 reader 的记录。验证原 segment 索引、真实字节/行数、每个内容摘要、严格 typed record、五种路由及 `max_row_bytes`。外部 `payload_ref` 也必须完整读验摘要并遵守同一行预算。
- 所有成员完成验证前不向 kernel 交付任何引用。描述符只含原 seal/manifest ArtifactRef 与原 assignment 的 edge/producer/target，以及实际扫描所得计数；元数据仍受原 `max_state_bytes`/分区数约束。
- `_SpoolPort` 可以按原顺序穿插 owned prefix 与 borrowed members；恢复继续读取旧 refs/pending 格式。重新读取 borrowed members 时重新核 seal、摘要、计数与路由。释放只释放自己原有的 refs，不调用上游对象的 unpin，更不删除上游对象。
- runner 仅为原 `finite_complete` port 采用该接口；`StreamPrependOp`、source、terminal、增量输入保留原路径。已提前满足的 consumer 维持原 no-op/EOI 行为。
- 小预算反例发现原 `_ChunkStream.readline` 超限分支缺少 `DataflowError` import，本批只补该 import，保留原拒绝阈值。

## 验收

每个命令均在本隔离树执行，以 `PYTHONPATH=$PWD:$PWD/tests` 与独立 `/tmp/demiflow-regression-capacity-*` 资源域运行原 env Python；runner 在进程内断言 `demiflow.__file__` 处于本树。完整命令：

```sh
PYTHONPATH="$PWD:$PWD/tests" DEMIFLOW_RESOURCE_ROOT=/tmp/demiflow-regression-capacity-borrow-combo /yzp/zhaozy/yangzepeng/0905/env/bin/python -B /tmp/demiflow-regression-runner.py -q tests/test_runtime_v3_finite_borrow.py tests/test_runtime_v3_finite_finish.py tests/test_runtime_v3_finite_segments.py tests/test_runtime_v3_k.py tests/test_runtime_v3_finite_spool.py --basetemp=/tmp/demiflow-regression-borrow-combo --junitxml=/tmp/demiflow-regression-borrow-combo.xml
PYTHONPATH="$PWD:$PWD/tests" DEMIFLOW_RESOURCE_ROOT=/tmp/demiflow-regression-capacity-borrow-final /yzp/zhaozy/yangzepeng/0905/env/bin/python -B /tmp/demiflow-regression-runner.py -q tests/test_runtime_v3_finite_borrow.py tests/test_runtime_v3_finite_finish.py::test_finite_join_output_checkpoint_restores_exact_prefix_and_multiplicity tests/test_runtime_v3_durable_residency.py::test_parked_limit_propagates_real_input_satisfaction --basetemp=/tmp/demiflow-regression-borrow-final --junitxml=/tmp/demiflow-regression-borrow-final.xml
PYTHONPATH="$PWD:$PWD/tests" DEMIFLOW_RESOURCE_ROOT=/tmp/demiflow-regression-capacity-borrow-tiny /yzp/zhaozy/yangzepeng/0905/env/bin/python -B /tmp/demiflow-regression-runner.py -q 'tests/test_local_kernel.py::test_join_matches_legacy_canonical_semantics[thread-inner]' tests/test_runtime_v3_join_spool.py::test_real_wide_join_under_small_memory_grant --basetemp=/tmp/demiflow-regression-borrow-tiny --junitxml=/tmp/demiflow-regression-borrow-tiny.xml
```

- 组合 **50 passed / 30.18s**；最后补提前关闭 no-op 与 snapshot 元数据脱离后，相关 **17 passed / 25.66s**。
- 最终字节上的原 **128KiB join** 与 **16MiB / 24×120KB 宽行 join**：**2 passed / 11.29s**。原预算、数据规模、数据/顺序/清理断言未改。
- 新 15 项包含五种 route、两 producer、多 reader、日期/Decimal/bytes/frozenset/负零、旧 owned/pending 镜像、有界元数据、三种预算反例、目标外损坏、membership/行数/摘要损坏、空 producer、外部 payload_ref 及无重复写入/无误释放。
- 真 Dataset sort 镜像断言 borrowed 引用存在且 owned/pending 为空。真 join 100×10 中途 ACK 中断、同 RunRef 恢复准确1000行；原 cut 镜像实读 left100/right10 各 borrowed1、owned0，保留原 finish_cursor 与 ACK。

JSON 附件绑定测试 XML 和最终 Python 字节摘要，不把组合50与最终19重复项相加作独立测试数。不宣称当前补丁已新跑 Ray，亦不宣称原40k/120秒压力已通过；根协调者继续同预算压力验收。

## 31 行耗时诊断

同步根 `16c92ad` socketpair 唤醒后，原 `test_narrow_chain_batch_boundaries_and_limit_match_serial` 为 **1 passed / 92.61s**，上轮106项中为147.387s。两次临时 profile 都只包到 lazy action 的 setup，不能把约1秒的累计值当完整执行耗时。有效的实际封存证据是默认4worker得到7stage/6条 durable_incremental，显式2worker得到2stage（source独立，兼容下游6occurrence共址）；布局/park开销已交根协调者继续处理。

诊断文件：`/tmp/demiflow-regression-narrow-wakeup.xml`、`/tmp/demiflow-regression-narrow-physical-0.json`、`/tmp/demiflow-regression-narrow-physical-1.json`。原106验收仍见独立 `ORIGINAL106-ACCEPTANCE.md/json`，本批不重复跑全部106。

## 文件纪律

测试完成后才提交。主仓及其它工作树未改、未跑主仓测试；无 push/merge 主仓/共享环境安装。承重七件相对 `49b666f` 无差异。除新测试外未改任何原测试。
