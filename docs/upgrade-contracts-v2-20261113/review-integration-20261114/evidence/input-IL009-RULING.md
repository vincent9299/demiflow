# IL009 最小裁决材料：`prompt_facts` 拒绝 `request_policy`

- 状态：**待裁决**（协调者未授权改动，因工单明令守卫不动）
- 涉及文件：`demiflow/operator_llm/flow_contract.py` → `prompt_facts`（约 246–256 行）
- 归属：**无派工归属**。`grep flow_contract dispatch-20261114/*.md` 无命中，198 张卡 + 6 个补充单元均不含此函数。
- 基线：`bbd93ffcc136c361093c5a4b6d79d0352c4c2557`（缺陷在基线即存在，非本轮引入）
- 两处独立证据：M 组 `M-KI-01`（真实公共 API 端到端）与 V 组 `V009`（封存事实层最小复现），互不引用、各自实测。

---

## 1. 最小复现（V009，纯本地，无外部/付费模型请求）

```python
from demiflow import data
from demiflow.execution.plan_facts import operation_facts
from test_map_prompt_async import PACK
from demiflow.operator_llm.pack import parse_prompt_pack

ds = lambda **kw: data.from_items([{"id": 1, "item": "1"}]).map_prompt_async(
    "enrich", config=parse_prompt_pack(PACK), inputs={"payload": "item"}, output="answer",
    concurrency=4, queue_depth=8, max_requests=64,
    options={"trust_env": False, "timeout_s": 5,
             "max_request_bytes": 1048576, "max_response_bytes": 1048576}, **kw)

operation_facts(ds()._plan.operations[-1])                      # OK
operation_facts(ds(request_policy={"min_concurrency": 1,
                                   "initial_concurrency": 1,
                                   "max_concurrency": 1})._plan.operations[-1])
# ValueError: Native prompt lowering/configuration copies differ
```

运行前置见 §5（IL012）：必须 `PYTHONPATH=<worktree>`，否则导入的是主仓。

## 2. 缺陷代码（M 工作树逐字读出）

`demiflow/operator_llm/flow_contract.py:246-256`

```python
def prompt_facts(actor):
    op, runtime = actor._operation, actor._runtime
    prompt = resolve_prompt(op.config, op.prompt_name)
    if (_value(prompt) != _value(resolve_prompt(runtime.config, op.prompt_name)) or _value(prompt) != _value(actor._prompt)
            or op.options != runtime.options or runtime.coordinator.max_requests != op.max_requests
            or actor.environment is not None or runtime._service is not None
            or runtime.request_gate is not None                      # <-- 这一行
            or runtime._clients or runtime._service_owner is not None or runtime._service_owners):
        raise ValueError('Native prompt lowering/configuration copies differ')
    return _native(prompt, op.inputs, op.output, op.outputs, runtime.options, op.max_requests,
                   actor.when is not None, actor.call_output, actor.error_output, runtime.token_budget)
```

调用链（M-KI-01 逐跳核对）：

1. `data/dataset.py:1574` — `request_gate = _resolve_request_gate(policy=request_policy, gate=request_gate, concurrency=concurrency)`
2. `operator_llm/inference.py:19-26` — `policy is not None` 时返回 `AdaptiveRequestGate(normalized)`
3. `data/dataset.py:1607` — `actor._runtime.request_gate = request_gate`（**两种形式都无条件赋值**）
4. `flow_contract.py:252` — `or runtime.request_gate is not None` → `:254` `raise ValueError(...)`
5. `flow_contract.py:41` — `normalize_prompt` 的拒绝集同样列入 `request_policy`（managed/sealed 路径）

即：**声明式 `request_policy` 必然产生一个 gate 对象，而守卫要求 gate 必须为 None**，两者结构性互斥。

## 3. 两处独立证据

### 证据 A — M 组 `M-KI-01`（真实公共 API 全链路）

- 路径：`Dataset.map_prompt_async` → `RunContext` → `materialize`，local mock transport，fixture key，**无外部/付费请求**。
- 脚本：`/tmp/demiflow-n1-probe7.py`
- 实测三态：

| 入参形式 | gate 类型 | 结果 |
|---|---|---|
| `plain_no_gate_policy` | `NoneType` | SUCCESS rows=2 req=2 |
| `request_policy={...}` | `AdaptiveRequestGate` | `RecoveryContractError: Native prompt lowering/configuration copies differ`，req=0 |
| `request_gate=<obj>` | `RequestGate` | 同上，req=0 |

- 被阻塞的 3 个既有测试（**原样保留，未削弱**）：
  - `tests/test_inference_execution.py::test_declared_admission_bounds_fresh_calls_and_excludes_replay[prompt]`
  - `tests/test_prompt_http_stream.py::test_concurrent_rows_pool_reuse_gate_lifetime_and_replay[False]`
  - 同上 `[True]`
- 存活意图由 45 个通过测试 + `test_prompt_stream_recovery.py` 6 passed 覆盖；被阻塞部分仅限 adaptive-gate 专有断言（`gate.peak`、`gate.fresh_responses`、`admission['adaptive']`）。
- 对照实测：`test_policy_and_explicit_shared_gate_conflict_before_http[prompt]` **PASS**（冲突在 `inference.py:20` 提前抛出），说明 policy 解析本身正确，问题只在守卫。

### 证据 B — V 组 `V009`（封存事实层）

- 复现即 §1，直接打 `operation_facts`，不经 transport。
- V 独立判定：`owner = UNOWNED (reported against M / operator_llm)`，并自行 grep 派工单确认**无卡拥有此函数**。
- V 的建议：`prompt_facts` 应当**把声明的 policy 与 lowering 出的 gate 做比对**，而不是要求 `request_gate is None`。

两处证据在不同层次（公共 API 端到端 / 封存事实函数）独立得到同一结论。

## 4. 工单冲突点（协调者为何不授权）

`dispatch-20261114/M.md` 第 11 行逐字：

> 优先复用N1已授权的在途候选；**prompt_facts守卫不动**。fresh预算与intent同事务，borrowed journal不能自关，unknown不重发。只替换外部transport做验收。

同时存在契约要求：

- `CONTRACTS.md:160`（C07）：**Prompt retains `request_policy`**
- `docs/prompt_http.md` `request_policy` 段落：同向

因此这是**工单与契约的直接冲突**，不是执行偏差：M 严格遵守了"守卫不动"，代价是 C07 声明保留的 `request_policy` 在 native prompt 公共路径上不可用。M 未越界修改组外文件，处置正确。协调者无权单方面解除工单明令，故升级裁决。

## 5. 建议改法（供裁决，二选一）

**方案 1（推荐，改引擎）**：`prompt_facts` 接受"由声明式 policy 派生且可封存"的 gate。
把 `or runtime.request_gate is not None` 换成对 policy↔gate 的一致性比对：gate 必须恰由 `op` 声明的 `request_policy` 经 `_resolve_request_gate` 派生，参数逐项相等；显式传入的共享 gate 对象仍拒绝（保持不可封存资源不得进封存事实的原意）。同步收窄 `flow_contract.py:41` `normalize_prompt` 的拒绝集。
- 保住 C07 与 `docs/prompt_http.md`，3 个被阻塞测试可原样恢复。
- 风险：`flow_contract.py` 无派工归属，需 GPT 明确指派装配负责人；不得顺手放宽其他拒绝项（`_service`/`_clients`/`environment` 等必须继续 fail-closed）。

**方案 2（改契约）**：宣布 native prompt 公共路径的声明式 admission 已退役。
修订 C07:160 与 `docs/prompt_http.md`，明确 `request_policy` 仅适用于 managed/sealed 之外的路径，并把 3 个被阻塞测试按"设计取代"重分类（参照 IL006 对 32 条 B/F 历史断言的处置）。
- 不动引擎，但**缩小了已声明契约**，与"同一封存计划真正约束执行"的意图有张力，需真人确认。

**不建议**：删除守卫、把并发降为 1、或放宽断言凑绿——违反本轮硬约束。

## 6. 裁决后必须做的复验

1. 在集成树（非单组工作树）重跑 §1 与 `/tmp/demiflow-n1-probe7.py`。
2. 恢复并按原断言跑上述 3 个被阻塞测试。
3. 确认 `runtime._service` / `_clients` / `_service_owner(s)` / `environment` 的拒绝行为**未变**。
4. 运行前置：`PYTHONPATH=<集成树>` 且断言 `demiflow.__file__` 在该树内（见 HANDOFF.md 的 IL012）。

---

证据原件：`/tmp/demiflow-upgrade-delivery-20261114/M.json` → `known_issues[M-KI-01]`；`/tmp/demiflow-upgrade-delivery-20261114/V009.json` → `needs_decision`。
台账：`/tmp/demiflow-dispatch-20261114/integration-ledger.json` → `IL009`。