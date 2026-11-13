# C 包设计稿：write_table create 中断恢复准入（design-first，待裁决）

基线：worktree c_runtime_20261113，HEAD 含桥/code-pin 载体修复。
状态：design-first。本稿只做设计，经总控裁决后再 implement。
范围：仅 workbench 真实入口 write_lance（LocalDatasetExecutor 原生执行）的
cancel→resume；不新增第二套调度/执行/恢复协议，复用现有 journal/receipt/封存绑定权威。

## 0. 缺口与已核实事实（总控代码级核实，本稿直接采用）

真实入口 flows/questions.py:select cancel→resume：attempt1 cancelled（6 slot 齐全、
acked=0、target.lance 不存在、runtime_commits 空），attempt2 failed，报
run_context.py:279 `This delivery-only entry needs its P3 native sink adapter before replay`。
该守卫由 14ab125 引入、main 无、不在 C 的 diff 里（既有占位，非终局设计）。

精确缺口链：
- manager.py:1019 `_native_execution_receipt` 的门槛在 :1211-1213：必须有 **prepared 的
  journal commit**（`execution_commits` 非空且 `facts[-1]['prepared']` 非 None）。
- cancel 发生在 commit 前 → runtime_commits 空 → `_native_execution_receipt` 返回 None
  → register_results 跳过该 output → resume 重入撞 run_context.py:279 守卫。
- manager.py:988-989 已 import reconcile_lance_create / reconcile_lance_overwrite，
  **全文零调用点**（未接线的既有权威）。

四项前提（总控已核清，本稿不再重新推导）：
1. 「确证不存在」的既有权威 = `inspect_lance(uri, None, storage_options)`
   （lance/storage.py:147）目标缺失时抛 `LanceResourceNotFound`
   （errors.py:52-53 文档逐字 "A physical Lance dataset is confirmed to be absent."）。
   LanceSink.prepare（lance_sink.py:30-35）已用此模式，且 create+base!=None 时抛
   LanceWriteConflict → **R-A（目标已存在）在 sink 层本来就 fail-closed**。
   **禁止** Path(uri).exists() 当存在性证据（lance/checkpoint.py:365 裸文件系统检查，
   无法区分不存在 vs 存在但不可读）。
2. 只把 LanceResourceNotFound 当不存在；inspect_lance 的任何其他异常（IO/权限/损坏）
   一律 fail-closed，不得降级成「不存在」。照抄 lance_sink.py:32 的 except 口径。
3. 时序可行：manager.py `_resume` :857 调 register_results(recover=True)，早于 :873
   begin_attempt，也早于 worker 重启与 run_context.py:279 守卫触发。
4. **分支A 不能靠 recovered 事件满足守卫**：execution_commits（run_journal.py:856-862）
   只读 runtime_commits 表；action_event（:451-452）"never infer commits from logs"，
   'recovered' 事件落 action_events，不产生 runtime_commits 行。故即使 reconcile 拿到回执
   并记 recovered，:279 的 `not execution_commits(action_id)` 仍成立、仍拒绝。
   分支A 必须走显式、以持久事实为据的准入分支。

## 1. 三分支判定表（A/B/C）与证据来源

准入仅对 `kind=='write_table'` 且 `mode=='create'` 的 replay 评估；其余形状字节不变。
输入：journal action declaration/intent 已 durable 携带 operation_id、request_hash、
spec.uri、spec.kind=create、spec.expected_version=null（总控探针实证，不新增身份权威）。

| 分支 | reconcile_lance_create(uri, request_hash, operation_id) | inspect_lance(uri) | execution_commits | 判定 | 动作 |
|------|------|------|------|------|------|
| A | 返回 receipt（marker=='demiflow-lance:'+operation_id） | — | 任意 | 证明我方已发布 | 恢复，**绝不重执行**；走现有 recover/receipt 路径 |
| B | 返回 None（未知） | 抛 LanceResourceNotFound（确证不存在） | 空 | 未发布且目标确证不存在 | 准入等价全新 create 重执行 |
| C | 返回 None | 返回 inspection（目标存在）或抛非 NotFound 异常（不可读） | 空 | 未知/存在/不可读 | **继续 fail-closed 拒绝** |

关键安全口径：
- reconcile 返回 None 是「未知」不是「不存在」（reconcile.py:114 "absence of evidence
  stays unknown"）。分支B 必须另有 inspect_lance 的 LanceResourceNotFound 正面证据，
  两者缺一不可。
- 分支C 涵盖「别人的数据」「存在但不可读/无权限/损坏」——只要不是确证不存在，一律拒绝。
- R-A 双保险：即便准入误判，LanceSink.prepare:34-35 仍在 sink 层对 create+已存在抛
  LanceWriteConflict。准入切片不替代 sink 守卫，是叠加的第一道闸。

## 2. 分支B「目标不存在」的正面证明手法（复用既有接口，不新造）

```
from demiflow.lance.storage import inspect_lance
from demiflow.errors import LanceResourceNotFound
try:
    inspect_lance(spec.uri, None, spec.storage_options)
    target_absent = False                 # 存在 → 分支C
except LanceResourceNotFound:
    target_absent = True                  # 确证不存在 → 分支B 候选
except Exception:
    target_absent = False                 # 不可读/IO/权限 → 分支C，fail-closed
```
与 lance_sink.py:30-33 完全同口径（只捕 LanceResourceNotFound）。区分「不存在」与
「存在但不可读」：后者落分支C。不用 Path.exists、不扫历史、不拿最新版本替代。

## 3. reconcile 接线位置：register_results(recover=True)，而非准入守卫

**选择：在 manager.py register_results(recover=True) 内补上未调用的 reconcile 权威。**
理由（职责分离）：
- register_results 是既有「恢复对账」点（:857，时序先于守卫），职责是**产出持久恢复事实**；
  准入守卫（run_context.py:279）职责是**消费持久事实做放行判定**。把 reconcile 放在
  register_results，守卫只读事实，不亲自做 Lance IO，保持守卫纯判定、可单测。
- 复用既有权威：reconcile_lance_create 已 import（:988），只需接线，不另造恢复协议。

具体接线（:1017-1021 现有 `_native_execution_receipt` 返回 None 之后）：
```
receipt = output['receipt']
if (not receipt or receipt['status'] != 'committed') and recover:
    receipt = _native_execution_receipt(run, output['action_id'])   # 现有：需 prepared commit
    if receipt is None:
        receipt = _reconcile_native_create(run, output, details)    # 新增：reconcile 权威
    if receipt is not None:
        run.action_event(output['action_id'], 'recovered', receipt)
```
`_reconcile_native_create` 仅对 write_table create 形状：从 intent 取 operation_id/request_hash/
spec.uri，调 reconcile_lance_create；返回 receipt 或 None。**对现有 22 passed 的影响**：
现有用例要么已有 committed receipt（:997-999 提前 continue），要么有 prepared journal commit
（_native_execution_receipt 命中），都不会走到新增的 reconcile 分支；新增分支只在
「无 receipt 且无 prepared commit」的 cancel-前-中断场景触发，故现有行为字节不变。

### 3.1 分支A 的持久事实必须落 runtime_commits（前提4 的落点，关键裁决点）

仅记 'recovered' action_event 不够（前提4）。分支A 要让守卫放行且 recover 不重发布，
必须把 reconcile 回执持久化为 **runtime_commits 行**（execution_commits 据此非空，
CommitCoordinator.recover 据此返回回执）。提议复用现有 journal commit 记录权威，把
reconcile 回执登记为一条 recovered commit fact（intent/prepared 由回执与 declaration 重建，
标记 recovered 来源），**不新增第二套提交权威**。

> 待裁决①：分支A 是「register_results 把 reconcile 回执登记为 runtime_commits recovered 行」
> （守卫自然放行 + recover 返回执），还是「守卫新增一个读 reconcile 持久回执的显式准入分支」
> （不写 runtime_commits，recover 路径另接 reconcile 回执）？前者复用 journal 单一提交权威、
> 守卫零改动但需确认 recovered commit 行的 prepared/intent 重建合法；后者守卫加显式分支但
> recover 需能消费非 journal 来源的回执。本稿倾向**前者**（单一权威、守卫不放宽），
> 请总控裁决 recovered commit 行的重建是否被现有 journal schema/校验接受。

## 4. run_context.py:276-279 守卫改法（只新增显式准入分支，不放宽）

现状：
```
if old is not None and (declaration.get('kind') in {'write_file', 'delivery'} or
        declaration.get('kind')=='run_stream' and not native_stream or
        declaration.get('kind') == 'write_table' and not self.journal.execution_commits(action_id)):
    raise RecoveryContractError('This delivery-only entry needs its P3 native sink adapter before replay')
```
改法：把 write_table 子句替换为一个**显式准入判定函数** `_write_table_replay_admitted(...)`，
仅当全部条件成立才放行，否则维持原拒绝：
1. kind=='write_table' 且 mode=='create'（从 declaration/sink spec 取）；
2. 分支A：execution_commits 非空（含 §3.1 登记的 recovered 行）→ 放行（recover 返回执，不重发布）；
   或分支B：execution_commits 空 **且** register_results 已持久化「目标确证不存在」事实
   （inspect_lance LanceResourceNotFound 的证据，随 recovered 对账落 journal）→ 放行全新 create；
3. 重执行计划必须通过既有封存绑定门 verify_loaded_plan / code-pin（resume 跑同一份封存计划）；
4. 任一不成立 → 维持原 RecoveryContractError 拒绝。

**字节不动**：write_file / delivery / 非 native run_stream 的拒绝分支原样保留；
write_table 仅在上述显式条件全满足时放行，绝不笼统放宽。分支C（目标存在/不可读）→
条件2 不成立 → 拒绝。mode!=create（append/merge/overwrite）→ 条件1 不成立 → 拒绝。

> 待裁决②：分支B 的「目标确证不存在」证据由谁持久化、存在哪个 journal 槽？
> 提议 register_results 在 reconcile None + inspect_lance NotFound 时落一条 durable
> 「create-target-absent」事实（绑定 action_id），守卫读它。避免守卫亲自做 Lance IO。
> 请裁决该事实的槽位与 schema（复用 source_binding 侧车槽，零 journal schema 变更）。

## 5. mode=overwrite 本轮后延的理由

create 可证：version=1 固定后继 + 唯一 operation_id marker + inspect_lance 确证不存在，
三者构成「未发布且可安全全新 create」的闭合证据。overwrite 需 expected_version 证明
（reconcile_lance_overwrite 要求 expected_version>=1，钉住 CAS 后继），且「目标存在」时
无法用 NotFound 证明未发布——必须靠 expected_version CAS 与 marker 双重证明，证据链更重。
本轮切片只做 create；overwrite 列为后续，需 expected_version 持久化与 CAS 证明设计。

## 6. 反例与黄金路径测试手法（走真实路径，不接受手写替身）

复用总控探针 /tmp/demiflow-coordinator-probe-f1/test_probe_resume.py 作红证据（不改它）；
新增 committed 测试用真实工作台入口 + 真实 RunContext/worker：
- **R-A（create 但目标已存在）**：真实 create 跑完一次（target 存在），再构造同 action 的
  resume（或模拟已写未 journal：真实写 Lance 但不清 journal commit）→ 断言准入拒绝
  （分支C），且 sink 层 LanceWriteConflict 双保险。最重要数据安全反例。
- **R-B（append/merge 非 create）**：mode=append/merge 的 write_table replay → 断言拒绝
  （条件1 不成立），不在准入切片内。
- **R-C（已有 commits）**：真实 create 走到 commit（runtime_commits 非空）后 resume →
  断言走现有 recovered/receipt 路径，行为不变，**不重执行**（acked 数不变、无重复发布）。
- **R-D（resume 计划/代码漂移）**：resume 时篡改业务函数（code-pin 变）→ 既有封存绑定门
  verify_loaded_plan fail-closed（"code-pin differs"），先于发布。
- **黄金路径**：真实入口 cancel（commit 前，target 不存在、runtime_commits 空）→ resume →
  succeeded、恰好 2 行正确结果、恰好 1 个 acked、dag_id 跨 attempt 稳定、sealed 仍可读、
  桥仍 available（载体修复后）。

teardown 后桥/ code-pin 载体存活已由本轮 test_workbench_bridge_readable_after_run_exit /
test_code_pin_source_archive_survives_run_exit 守护（红→绿已实证）。

## 7. 改动文件清单与行数估计（implement 阶段，待裁决后）

1. workbench/runs/manager.py：register_results 接线 reconcile（+_reconcile_native_create
   helper，分支A recovered commit 登记 / 分支B target-absent 事实）≈ +40/-3。
2. execution/run_context.py：:276-279 write_table 子句替换为显式准入判定
   （+_write_table_replay_admitted helper）≈ +25/-2。write_file/delivery/run_stream 字节不动。
3. tests/test_c_write_seam.py（或新 test_c_write_recovery.py）：R-A/R-B/R-C/R-D + 黄金路径
   ≈ +180。
不改动：承重七件、clean_orphans/run_journal/allocate_artifact 语义、chain.py:840、
worker 加载路径、LanceSink/CommitCoordinator 提交语义、E 测试。

## 8. 为何不会引入重复发布

- 分支A：reconcile marker 证明我方已发布 → 登记 recovered commit → recover 返回执，
  绝不重执行；execution_commits 非空使守卫放行的是 recover 而非 fresh create。
- 分支B：reconcile None（未知）+ inspect_lance 确证不存在（正面证据）+ execution_commits 空
  + mode=create + 封存绑定门通过 → 等价全新 create；目标确证不存在故无旧数据可覆盖。
  即便并发竞态，LanceSink.prepare:34-35 create+已存在抛 LanceWriteConflict 兜底。
- 分支C：未知/存在/不可读 → 拒绝，绝不盲目重启。
- 任一证据缺失即 fail-closed；不放宽 write_file/delivery/run_stream；不新增提交权威。

## 9. 待裁决清单（implement 前必须明确）

① 分支A 持久化形式：recovered runtime_commits 行（倾向，单一权威）vs 守卫读 reconcile 回执的
   显式分支。需确认 recovered commit 行 prepared/intent 重建被现有 journal 校验接受。
② 分支B「目标确证不存在」事实的 journal 槽位/schema（提议 source_binding 侧车槽，零 schema 变更）。
③ 准入判定函数读持久事实的边界（守卫不做 Lance IO，全部证据由 register_results 预产）。