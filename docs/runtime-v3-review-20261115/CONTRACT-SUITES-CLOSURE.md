# v3 19 组契约与历史失败清单闭环

**修复提交 `6b2a7319d800343b893df86c8221f6439fb986c7`：19 组完整实跑 564 passed、0 failed、0 skipped。原 33 项、原 147 项，以及其中此前标记待其他 lane 处理的 35 项，均在这次实际运行中通过。** 不是按符号存在、门名称或早期局部通过推断。机器证据见 [CONTRACT-SUITES-CLOSURE.json](CONTRACT-SUITES-CLOSURE.json)。

工作树 `/yzp/zhaozy/yangzepeng/0905/_demiflow/worktrees/gpt_v3_residency_20261115`，分支 `engine/gpt-v3-residency-20261115`。集成基线 `e6bdc2f9ea6ca1e5397315301e50a7db3edf9a59`；本树合并提交 `6015d77` 的文件树与该基线完全一致。未编辑或运行主仓，未 push、未安装环境、未操作生产服务。七个冻结文件逐字等于 `362d8528c27ae60a0d6ae0ccc986a0ff93847de8`，摘要在机器证据中。

## 实跑结果与边界

| 运行 | 实际版本 | 结果 | 原件 |
|---|---|---|---|
| 完整首轮 | 干净 `6015d77` | 553 passed / 11 failed / 0 skipped，146.61s | `/tmp/demiflow-v3-contracts-full-r1.log`、同名 `.xml` |
| 修复定向 | `6015d77` + 后来完整提交为 `6b2a731` 的补丁 | 15 passed，26.61s；11 个原红 + 4 个真实 Dataset 场景 | `/tmp/demiflow-v3-contracts-fixes-r2.log`、同名 `.xml` |
| 完整终验 | 干净 `6b2a731` | 564 passed / 0 failed / 0 skipped，138.05s | `/tmp/demiflow-v3-contracts-full-r3.log`、同名 `.xml` |

19 文件为 `tests/test_runtime_v3_{b,c,d,e,f,g,h,k,l,m,p,q,s,t,u,v,w,x,y}.py`。终验收集 564 个实际用例；三个可选依赖的 `importorskip` 均未触发。测试前进程内断言 `demiflow.__file__` 位于本树；测试期间不改源码、不提交。1744 个生产/测试 Python 文件的运行前后摘要一致；定向 15 项与最终提交的这 1744 个输入也全部一致。

终验原命令：

```bash
cd /yzp/zhaozy/yangzepeng/0905/_demiflow/worktrees/gpt_v3_residency_20261115
DEMIFLOW_RESOURCE_ROOT=/tmp/demiflow-v3-contracts-capacity-r3 \
PYTHONPATH="$PWD:$PWD/tests" \
/yzp/zhaozy/yangzepeng/0905/env/bin/python -B \
/tmp/demiflow-v3-contracts-full-r3.py \
> /tmp/demiflow-v3-contracts-full-r3.log 2>&1
```

运行器以 `pytest.main` 执行全部 19 文件，无 `-k` 缩选、无失败提前停止、无重跑插件改写结果；原始参数、源摘要、每个 testcase 状态和时长、日志/XML 摘要均在 JSON 中。所有临时文件和资源域均在本批自己的 `/tmp/demiflow-*`。

## 保留首轮 11 红及实际修法

| 首轮失败 | 真相与修复 | 保留/新增的断言 |
|---|---|---|
| B010 | worker 清理已正确汇报 `ExceptionGroup`，旧测试期待包装成 `DataflowError` | 检查捕获的是同一个原 `RuntimeError` 对象；全部已取得的 reader 均 close；mailbox 关闭；所有 owned kernel/reader 清空；重复 close 幂等 |
| G011 | **真实缺陷**：`SortOp.descending` 合法默认是 bool，`operation_facts` 却调用 `list(bool)` | bool 按 key 数量展开，tuple 保持原方向；错误长度/非 bool 拒绝；排序已有实际能力，原“拒绝 SortOp”负例改为真正未知 `LogicalOp`，未知能力仍拒绝 |
| L024、Y024 | 测试伪造 `requests/tokens/calls`，已不属于真实 `OperatorLLMUsage` 的七维公开字段；Y 还要求丢掉所有零值维度 | 直接用真实 `OperatorLLMUsage`；七维累计均核验，driver coordinator 与真实 worker 累计投影组合；重复读取不重新记账、不执行 pipeline；零值保留全部维度 |
| M026 | 当前已实现 offline 原生端口，旧测试仍要求所有 offline 注入被拒 | 单 offline 声明走验证过的同一 native port；不创建 worker 数据库、不发 HTTP；伪 port、混合 store、第二 journal、额外 retry 以及 Codex 的不兼容注入继续拒绝 |
| T005、T012–T015 | **协议缺陷**：segment/typed/schema 遍历新增后，物化 handle 暗中要求 `ArtifactStore._bounds`，但该私有字段不属于协议 | `StoredMaterializedHandle(..., bounds=...)` 显式接原预算；唯一生产入口传 `execution.services.bounds`。协议存储没有 `_bounds` 仍可读/保真/独立 owner pin/释放；manifest 超原控制预算时在读取前拒绝；历史 pickle 原有 bounds 字段照用 |
| W001 | **真实历史读取缺陷**：新 producer 封存 `sealed_at`、`observation_labels`，history reader 仍要求旧 exact keys | 只增加这两个已定义可选元数据；时间必须非负整型；标签必须属于原图 occurrence、非空字符串且唯一，并受原控制字节预算约束。未知字段、错 run/action/ref、伪标签仍拒绝；旧无字段 marker 读取相同原 plan；读取不改原 journal |

修改范围：四个生产文件 `plan_facts.py`、`dataflow_history.py`、`dataflow_handles.py`、`dataflow_terminals.py`；七份原契约测试文件。没有修改生命周期实现、请求预算权威、物理提交协议或冻结七件。T 构造参数是本次内部接口迭代；生产调用点与六个直接测试调用点全部同步，不新增默认预算，不依赖存储实现私有策略。

## 历史清单逐项结果

历史原件 `lane-test-delta.json`、`REGRESSION-NODEID-MAP.json` 保持不动，避免抹去当时失败事实。新 JSON 中 `historical_maps.original33` 和 `historical_maps.original147` 每项同时保留原记录、首轮实际 testcase、终验实际 testcase。

- 原 33 项：首轮 32 passed / W001 failed → 终验 **33 passed**。
- 原 147 项：首轮 145 passed / B010、Y024 failed → 终验 **147 passed**。
- 原 147 中历史状态 `other_lane_pending` 的 35 项：首轮 34 passed / B010 failed → 终验 **35 passed**。这 35 项均在本次 XML 中实际执行，没有以其他会话的回报或符号映射替代。

## 真实 Dataset 证明与原意图对应

除了契约组内 E 的真实 managed worker/count/native ACK 流程，定向验收额外执行以下未替换成 mock 的公开 API 场景：

1. `test_public_global_sort_complete_scope`：公开排序经过真实 worker、完整 scope 与结果读取。
2. `test_public_materialize_fidelity_readback`：真实物化、多次回读保持数据保真，不重新执行源。
3. `test_public_join_different_keys_from_materialized_sources`：真实物化句柄封存后跨 worker 读取，使用原预算，完成不同 key 的 join。
4. `test_replay_committed_result_reuses_seal_and_commit`：真实已提交结果恢复复用原 seal/commit，不另建执行或提交账本。

这些对应 I01 同一 Dataset、I07 原提交恢复、I08 有界资源、I12 真实历史展示及 I13 历史保护。它们不能替代所有 API、所有业务或产品 R1–R5 的整体验收。

## 明确未由本批证明

本批完整的是这 19 个契约组，不是仓库全部历史测试。H/Y 的 Ray provider 边界替身不算真实 Ray 集群，更不算跨宿主；本批没有新增物理 GPU、多机、外部 PostgreSQL 或生产数据库验证。此前其他批次的同机 Ray、工作台和模型证据仍应按各自提交/输入摘要逐项判断。

性能也没有在这里宣布通过：总控当前仍在处理原 120 秒宽行压力超时。本批无吞吐基准、没有通过增内存/worker/超时来换绿。产品浏览器、Notebook/Case/比较评测和全部真实业务仍由对应集成验收承担，不能用 564 个契约通过折算成两大意图“100%”。
