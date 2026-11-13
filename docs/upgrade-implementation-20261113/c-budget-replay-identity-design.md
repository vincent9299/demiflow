# #42 默认 managed-image 中断恢复失败 — 重放字节身份设计稿

- 状态：**总控已裁决采纳 B-proper，已实现**（见 §11 实现后记）。本稿 §1–§10 为裁决前的设计记录，保留原貌。
- 工作树：`/yzp/zhaozy/yangzepeng/0905/_demiflow/worktrees/c_budget_20261113`，分支 `engine/c-budget-20261113`，基线 `de2d398`（已验收合并版）。树 clean。
- 失败用例：`tests/test_managed_image_execution.py::test_http_process_fault_recovery_never_regenerates_prepared_images[after_staging_before_prepared-False]`。
- 本稿不碰保护三件文档，不碰承重七件，不放宽守卫，不抹 reused，不改读取格式，不 merge/push。

---

## 1. 结论与推荐（按意图影响分级）

**推荐路线 B（治本），但必须是「内容寻址·最大共享」的值规范化编码器，不是总控探针里的朴素 canonicalizer。**

分级理由：

- gap③「默认范围内的数据正确性和失败处理」是承重项。它要求的不变量是：**同一逻辑 commit 的重放（re-prepare）必须预留相同字节，且该字节由行值决定，而非由生产代码如何共享对象决定。** 这是一条关键架构边界，不是外围完善。
- 路线 A 只把「request_id 这一个叶子在 decode 复用侧的共享」补成与活路径一致。它修的是症状，不是不变量：任何其它字段／其它 provider／未来新增字段一旦出现同类身份漂移就会再坏，且把「sink 字节核算正确性」耦合到「每个算子 decode 必须产出身份稳定的对象图」这一错误层上。**A 不治本**（论证见 §3）。
- 路线 B 把不变量落在**正确的层**：sink 拥有字节核算，就应拥有编码确定性。算子无需知道 pickle memo 的存在。
- 总控担心的 B「改变既有块字节」**对默认路径不成立**：我实测证明，恰当处理容器共享后，结构规范化编码把整行 **1072 → 1072 逐字节保长**（§2、§5）。朴素 canonicalizer 的 1105（+33）正是总控点名的「丢容器共享」坑，本稿用内容寻址最大共享避开它。

代价与边界我如实声明（§5、§8）：B 触及所有 native 行编码缝、每行多一次 O(节点) 遍历与约 2× 行图瞬时副本、对「活路径本就非最大共享」的其它行会改变字节（跨版本恢复被守卫拒绝，不加 shim）。默认 managed-image 路径已实测保长，落在 gap③ 范围内。

> 复杂度降级备选（供总控裁决，非并列推荐）：若总控判定完整结构规范化器的复杂度／blast radius 相对「实测仅叶子漂移」过重，可在**同一 sink 缝**改用「叶子按值驻留 + 容器按 id 保留共享」的简化变体（实测同样 1072→1072）。它修掉已证实的叶子漂移、保长、低风险，但不对容器级漂移治本。我仍推荐完整 B，因为总控对 B 的定义是「值规范化」这一通用不变量。

---

## 2. 根因复核（实测，自生成 pickle，仅 /tmp）

### 2.1 红证（de2d398，已实测失败）

`/tmp/demiflow-42-c/red_before.log`：`1 failed in 7.31s`，`RED_BEFORE_EXIT=1`。raise 链逐帧落在本工作树：

```
commit.py:70 sink.prepare
 → object_sink.py:411 self.rows.prepare(blocks, context)
 → block_sink.py:99 self.run.reserve_output('row_bytes', block['bytes'], ...)
 → run_context.py:377 self.journal.reserve_budgets(...)
 → run_journal.py:980 reserve_budgets_in_transaction
 → run_journal.py:1005 raise RecoveryContractError('Budget reservation changed')
```

### 2.2 字节漂移定位（与总控结论一致）

trace `/tmp/demiflow-42-diag-trace.log`：commit_id 跨 attempt 稳定、reservation_id 稳定（我在 block_sink.py:96-101 的 commit-keyed+ordinal 设计正确）。整行 attempt1=1072 → resume=1137（+65），逐字段：

| 字段 | attempt1 | resume | 说明 |
|---|---|---|---|
| instruction | 20 | 20 | 不变 |
| image | 309 | 309 | uri/sha256 一致，对象未再生 |
| error | 4 | 4 | 不变 |
| pixels | 88 | 88 | 不变 |
| **call** | **640** | **705** | **+65，唯一漂移源** |

call 唯一值差异 `reused: False→True`（顶层 + attempts[0]），必须保留。+65 来自 pickle 对象身份共享差异：活路径 request_id 六处共享一个对象；复用侧从 journal 读回变成两个对象，pickle 多写一次 64 字符串（SHORT_BINUNICODE+MEMOIZE 67B 替换一个 BINGET 2B = +65）。成因位置 `client.py:420-436` decode 的 `metadata={**(refs or {}), …, **record.get('transport_metadata',{})}`；活路径 record 在 `client.py:600-601` 用进程内共享对象构造。排除项 `_journal_refs`（client.py:453）无生产者。

### 2.3 规范化探针（我独立重跑 + 扩展，自生成 pickle）

`/tmp/demiflow-42-probe/leaf_intern.py`、`row_probe.py`。四种编码对同一对行：

**call 字段**（`demiflow-42-call-30888.pkl`=640 / `…30898.pkl`=705）：

| 编码 | attempt1 | resume | delta | 等长 |
|---|---|---|---|---|
| raw（现状） | 640 | 705 | +65 | 否 |
| naive-canon（总控探针，重建容器+驻留叶子） | 673 | 673 | 0 | 是 |
| leaf+id-memo（驻留叶子，容器按 id 保共享） | 640 | 640 | 0 | 是 |
| **structural（内容寻址最大共享）** | **640** | **640** | **0** | **是** |

**整行**（`demiflow-42-row-30888.pkl`=1072 / `…30898.pkl`=1137）：

| 编码 | attempt1 | resume | delta | 等长 |
|---|---|---|---|---|
| raw | 1072 | 1137 | +65 | 否 |
| naive-canon | 1105 | 1105 | 0 | 是（**+33，改字节**） |
| leaf+id-memo | 1072 | 1072 | 0 | 是 |
| **structural** | **1072** | **1072** | **0** | **是（保长）** |

逐字段 structural vs raw（attempt1）：call 640、error 4、image 309、instruction 20、pixels 88，**全部 `struct==raw_a=True`** → 默认行的活路径编码本就最大共享，故结构规范化逐字节保长。

值差异隔离：`raw b(reused=False)` 仍 705≠640（**强制 reused=False 修不好**，与总控一致）；`leaf b(reused=False)` 与 a **逐字节相等**（证明唯一值差是 reused）；reused 翻转**长度中性**（与总控一致）。所有编码 roundtrip 还原 == 原值。

### 2.4 与总控结论的一致性

- 一致：根因（pickle 身份共享）、+65 来源、reused 必须保留、reused 翻转长度中性、强制 False 修不好、commit/reservation 稳定、朴素 canonical 673/673。
- **一处细化（非矛盾，正是总控点名的坑）**：总控探针的朴素 canonicalizer 丢容器共享 → 673（整行 1105，改字节）。我证明**内容寻址最大共享**的结构规范化器把容器共享也按值规范化，整行 **1072→1072 保长**。即「B 必然改既有字节」对默认路径不成立——前提是正确处理容器共享（总控已要求「你要自己处理容器共享」）。
- 未发现其它不一致。

---

## 3. 路线对比 A vs B

### 路线 A（窄）：修 decode 复用侧对象共享

- 改动面：`client.py:420-436` decode（image provider 分支），让复用侧重建的 metadata 把 request_id 等 journal 派生字符串按值驻留成与活路径相同的共享图。
- 优点：blast radius 最小（一个函数）；保长（640/1072）；不动编码层；天然满足约束 6。
- **是否治本：否。**
  1. 症状特异：只补 request_id 这一处共享模式。其它字段／provider／未来字段同类漂移会再坏（whack-a-mole）。
  2. 错误层：把 sink 字节核算的正确性押在「每个算子 decode 产出身份稳定对象图」上。算子本不该知道 pickle memo。这是泄漏抽象。
  3. 不建立不变量：重放幂等仍依赖活路径与复用路径**偶然**共享一致，而非由值保证。
- 结论：能绿当前用例，但 gap③ 的架构边界仍敞开。按意图分级不推荐作为终态。

### 路线 B（治本）：BlockSink 行编码值规范化

- 改动面：`block_sink.py:54` 一处编码缝 + 一个有界规范化 helper。所有 native 行 materialization（rows / input_capture / staging_only）共用此缝，object_sink.rows 委托此缝（object_sink.py:411）。
- 优点：建立通用不变量「持久化行字节 = f(行值)」；正确层；对未来漂移免疫；默认路径实测保长。
- 缺点／代价：触及所有行编码；每行多一次遍历 + 瞬时副本；对「活路径非最大共享」的其它行改字节（跨版本边界，§5）；规范化器需深度/环路边届（§8）。
- **必须用 structural（内容寻址最大共享），拒绝 naive**：naive 整行 1072→1105 会破坏默认路径跨版本恢复；structural 1072→1072 保长。

---

## 4. 精确改动清单（改什么 / 不改什么）

### 改（路线 B-proper）

1. **`demiflow/execution/block_sink.py`** 新增模块级有界 helper `_canonical_row(value)`（约 40–60 行）：
   - 对 `dict/list/tuple/str/bytes` 图按**内容寻址最大共享**重建：相等子树共享同一对象 → 相等值产出相等对象图 → 相等 pickle 字节。
   - **保留 dict 插入序**（与现有 pickle 格式一致，满足约束 6 读回；也是默认行保长的前提）。
   - 叶子 `str/bytes` 按值驻留；其它类型原样透传（pickle 自行处理）。
   - 边届：深度上限 `_CANON_MAX_DEPTH`（建议 64，远超默认行 ~4 层）；用 id 栈检测环路。**超深或含环 → 回退原始 `pickle.dumps`**（保持现状、零回归；值规范化保证仅覆盖有界无环行，即默认路径，§8 声明）。
2. **`demiflow/execution/block_sink.py:54`** 一行：
   `raw = pickle.dumps(row, protocol=4)` → `raw = pickle.dumps(_canonical_row(row), protocol=4)`。
   - `:53` `check_size(row, max_row_bytes)`（对值）不变。
   - `:55-56` 编码后字节检查现在作用于 canonical 字节；因 canonical ≤ raw（最大共享 ⊇ 身份共享），不会新增膨胀；超限行仍拒绝。
3. **测试**（§6、§7）：新增 red-before-green 用例 + 规范化专属反例；既有 `test_s6_gate8_nondeterministic_reprepare_fail_closed` 必须仍绿。

### 不改（承重 / 范围最小化）

- `run_journal.py:1005` 守卫及 `:982-1016` 整个 reserve 逻辑——**逐字不动，严禁放宽**。
- `block_sink.py:96-101` reservation_id 语义（commit-keyed+ordinal）——不动，严禁改语义绕过。
- `client.py` decode / reused / refs / `_journal_refs`——**不动**（路线 A 的表面不采用）。
- `block_sink.py:185` `iter_rows` 的 `pickle.load`——读取格式不动（约束 6）。canonical 产物仍是合法 pickle，读回不变。
- 承重七件：`execution/raw_fetch.py`、`execution/queue_execution.py`、`execution/queue_sinks.py`、`execution/queue_source.py`、`execution/recovery_identity.py`、`planning/dag.py`、`resources/contracts.py`——逐字不动。
- `object_sink.py`、`commit.py`、`run_context.py`——不动（仅委托；修在编码缝）。
- 对象 artifact 编码（object_bytes，按 sha256 内容寻址）——不动（本就无身份漂移）。
- 保护三件文档、main ref `49527158…`、主仓 HEAD `e40a6dd`、主仓工作树——不动。
- 不加任何放宽守卫的兼容 shim；不抹 reused；不顺手重构无关部分。

---

## 5. 既有封存块字节边界声明

- **默认 managed-image 路径：保长，已逐字节实测。** 整行 structural canonical = 1072 = 原 raw attempt1；逐字段 `struct==raw_a` 全 True。故已封存的默认路径块在升级后重放仍预留 1072，守卫通过，**不破坏默认路径跨版本恢复**。
- **within-version 重放：恒幂等。** canonical 由值决定，reused 翻转长度中性 → 同 commit 多次 prepare 字节恒等，守卫通过。这是 gap③ 的核心场景。
- **其它行（活路径本就非最大共享）：会改字节，显式声明。** 若某行活路径 raw 编码含「等值但不同对象」的叶子/容器（非最大共享），structural canonical 会**缩短**字节（canonical ≤ raw）。此类行在旧码下已封存、新码下重放时，重放字节 ≠ 已封存预留 → 守卫按现状 raise `Budget reservation changed`（fail-closed，**不加 shim 接受双字节**）。
  - 范围界定：gap③ 是「默认范围内」恢复，默认路径已保长；此类非最大共享行的跨版本恢复不在本单范围，且守卫拒绝是正确行为（宁拒绝不静默改核算）。
  - 不引入版本号双轨、不放宽守卫、不重写已封存块。
- **naive 变体被拒绝**：整行 1072→1105 会破坏默认路径跨版本恢复，故不采用。

---

## 6. red-before-green 回归测试设计

1. **red 已在 de2d398 实测**：`/tmp/demiflow-42-c/red_before.log`，`1 failed in 7.31s`，链尾 `run_journal.py:1005`。实现前保持此红证。
2. **green 主用例**：修复后重跑同一参数化用例 `[after_staging_before_prepared-False]`，断言 `second.returncode==0`、`len(fixture_http['requests'])==1`（对象未再生）、`out.lance` 存在且行值与 clean 基线一致。命令（cd 进树 + 断言前缀 + 全量重定向 + echo $?，不接 | tail）：
   ```
   cd "$D" && PYTHONPATH="$D:$D/tests" /yzp/zhaozy/yangzepeng/0905/env/bin/python -B -m pytest \
     --import-mode=importlib -p no:cacheprovider -p no:randomly \
     -m "not slow and not network and not gpu and not ray" \
     "tests/test_managed_image_execution.py::test_http_process_fault_recovery_never_regenerates_prepared_images" \
     --basetemp=/tmp/demiflow-42-c --tb=short > /tmp/demiflow-42-c/green_after.log 2>&1; echo $?
   ```
   先断言 `demiflow.__file__.startswith("$D")`。
3. **编码层单元用例**（新增，建议落 `tests/test_s6_commit_window_quota.py` 或新 `tests/test_canonical_row.py`）：
   - 等值不同身份 → 等字节：构造两个值相等但 request_id 分别为独立对象的行（模拟活/复用），`BlockSink.append` 后 `_flush`，断言两块 `bytes` 相等、sha256 相等。
   - 保长回归：对默认行样本断言 canonical 字节 == 原 raw 字节（1072），锁住「不膨胀」。
   - roundtrip：`iter_rows` 读回 == 原值（约束 6）。
4. **全量回归**：managed_image 全套 + s6 quota 全套 + runtime/seam 相关组，确认无退步（尤其 §7 反例与既有 gate8）。

---

## 7. 反例：证明守卫仍有效（真正改字节必须 raise）

1. **既有反例必须仍绿**：`tests/test_s6_commit_window_quota.py:418-427 test_s6_gate8_nondeterministic_reprepare_fail_closed`——同 commit 先 prepare `[{"x":1},{"x":2}]`，再 prepare `[{"x":1},{"x":2},{"x":3}]`，断言 `pytest.raises(RecoveryContractError, match="Budget reservation changed")`。canonical 下两者值不同 → 字节不同 → 仍 raise。**这条直接证明规范化没有把守卫架空。**
2. **新增规范化专属反例**（锁住「值规范化 ≠ 掩盖真实变更」）：
   - 同 commit，先 prepare 行 R，再 prepare 行 R'（**改一个字段值**，如 instruction 文本变长 / pixels 改值），断言 raise `Budget reservation changed`。证明 canonical 只对「等值」幂等，对「改值」仍 fail-closed。
   - 对照：同 commit，先 prepare R（request_id 为对象 o1），再 prepare 值相等但 request_id 为独立对象 o2 的 R，断言**不 raise**、预留字节不变。证明修复恰好消除身份漂移、且仅消除身份漂移。
3. **守卫本体不动**：反例全部经由现有 `run_journal.py:1005`，不新增任何放宽分支。

---

## 8. 有限资源 8 条遵守（worktree AGENTS.md:46-53）

1. **增长方式与预算先行**：规范化是每行 O(行图节点数) 的一次遍历，无跨行累积、无全局排序、无缓存增长。行总量仍由 `max_output_rows`/`max_row_bytes`/`max_output_bytes` 既有预算约束，不变。
2. **按生命周期核算峰值**：编码瞬时峰值 ≈ canonical 副本 + pickle 缓冲 ≈ 2× 行图（行已被 `max_row_bytes` 界定）。这是 AGENTS.md:47 明列的「序列化副本」，纳入既有核算；不冒充 RSS 硬上限。canonical 副本在 `pickle.dumps` 返回后即可释放，不跨行驻留。
3. **分配前控制增长**：先 `check_size`（值，:53）→ 规范化 → 编码字节检查（:55）→ 才 buffer/flush/allocate。超限在分配前拒绝，不先完整物化再切。
4. **不可再拆对象有策略**：单行可独自超 `max_row_bytes`；canonical ≤ raw 不膨胀，超限行仍由 :55 **明确拒绝**（不截断业务结果）。规范化器自身**深度有界**（`_CANON_MAX_DEPTH`）、**环路检测**；超深/含环回退 raw（可验证边届，不无限递归）。
5. **端到端传递约束**：修在**终端 sink 编码缝**。reader/预读/算子/并发/跨进程序列化/回读/组批均不动；上游小批不在后续层被无约束合并。canonical 字节即被 reserve/store/读回的字节，约束一致传递；`iter_rows` 仍 `pickle.load`（读回格式不变）。引擎内存池/spill 之外，sink 自身预算（row_bytes/output_bytes/output_admission_bytes）不变。
6. **磁盘与并发准入/清理**：块文件准入仍走 `allocate_artifact` + `publication_guard` 同 fence/事务（block_sink.py:73-79），reservation 计数上限 100000 不变；规范化不新增文件句柄/临时文件/并发。异常/取消清理路径不变。
7. **稳定性约束下评估性能**：每行多一次 O(节点) 遍历 + 一次副本，换重放幂等。不以增内存/并发/临时空间换耗时；不产生大量微小文件（块大小/256 块 manifest 阈值不变）。默认行 ~5 字段、~4 层，开销可忽略。
8. **按边界行为验收**：用小预算触发——宽行/单条超限（:55 拒绝）、等值不同身份（幂等）、改值（守卫 raise）、深/环行（回退 raw）、真实进程 fault→resume（主用例）。保留既有批次/固定版本/文件边届契约（s6 gate1-9 全绿）。

### B 的规范化器边届（显式）

- 覆盖类型：`dict/list/tuple/str/bytes` 的有界无环图（默认行与所有 JSON-like 行 + ObjectRef dict）。其它对象原样透传。
- 深度：> `_CANON_MAX_DEPTH` → 回退 raw（零回归，声明）。
- 环路：检测到 → 回退 raw（pickle 本就支持环；回退保持现状，值规范化保证不覆盖环行，声明）。
- 不透明对象身份漂移：不在本单范围（默认行不含；未观测）。
- 保长性质：canonical ≤ 同值 raw（最大共享 ⊇ 身份共享）→ 规范化本身永不引入新预算拒绝。

---

## 9. 承重约束遵守清单

- 守卫 `run_journal.py:1005` 与 reservation_id 语义：不动、不放宽、不绕过。✓
- reused / 复用溯源：保留（canonical 对 reused 翻转长度中性，不改值）。✓
- 承重七件：逐字不动。✓
- 保护三件文档：不动、不入 commit。✓
- main ref / 主仓 HEAD / 主仓工作树：不动；不在主仓跑测试。✓
- 读取格式 `iter_rows` pickle.load：不动。✓
- 不顺手重构 client.py / BlockSink 无关部分；改动 = 一个 helper + 一行编码 + 测试。✓
- 不 push / merge / rebase 他人分支；合并由总控做。✓
- 测试只写 /tmp/demiflow-*；不扰训练 7095/7102-7105、control.py(50954)、PID 22682/44619/61381/63136；不占端口 55971/55972/59865/59866。✓
- 不嵌套派发；问题直报总控。✓

---

## 10. 待总控裁决点

1. 是否采纳 **B-proper（内容寻址最大共享值规范化）** 为终态？（我推荐；默认路径实测保长 1072→1072。）
2. 若判定完整规范化器复杂度过重，是否降级到**同缝叶子驻留变体**（同样保长，但不治容器漂移）？
3. §5 的「非最大共享其它行跨版本恢复被守卫拒绝」边界是否接受（不加 shim）？
4. 规范化器深度/环路**回退 raw** 的边届处理是否认可（零回归 vs 一律拒绝）？

裁决后我才实现。本稿仅设计，未改任何生产代码。

---

## 11. 实现后记（总控裁决采纳 B-proper 后）

裁决四点全部遵守：采纳 B-proper 终态、不降级叶子驻留、接受 §5 边界不加 shim、回退仅由「深度 > `_CANON_MAX_DEPTH`」与「id 栈检测到环」两个显式值决定条件触发，**无任何 `except` 回退**。承重要求 A–E 逐条落实：

- **A（memo per-call）**：`leaves/containers/stack/fallback` 全是 `_canonical_row` 的局部变量，无模块级/跨行缓存。唯一模块级新增是常量 `_CANON_MAX_DEPTH = 64`。
- **B（canonical ≤ raw 边界测试）**：已加 `test_s6_gate10_max_row_bytes_boundary`（恰好 fit 接受 / 略超拒绝）与 `test_s6_gate10_encoded_backstop_rejects`。未观测到任何 canonical > raw 的行形状。
- **C（dict 插入序）**：按 `node.items()` 原序重建，签名用有序 (key_sig, val_sig) 元组，插入序不同 → 签名不同 → 不折叠。
- **D（不动缝）**：`:53` `check_size(row,…)` 仍作用于**原始 row**（语义不变）；`:55-56` 拒绝语义不变；`reservation_id`、`iter_rows` 的 `pickle.load`、`run_journal.py:982-1016` 与 `:1005` 守卫、`client.py`、object_sink/commit/run_context、承重七件、保护三件——逐字未动（diff 仅 helper + 常量 + 一行编码缝）。
- **E（改动面）**：`block_sink.py` +71/-1（一个有界 helper + 一个常量 + 一行缝），`test_s6_commit_window_quota.py` +129（8 个 Gate 10 用例），本设计稿。无顺手重构。

### 两处如实声明的发现

1. **`:53` 是绑定预算，`:55` 是 backstop（既有性质，非本单引入）**：对所有 `check_size` 合法行形状，`sys.getsizeof` 求和 > pickle 编码长度，故 `:53` 先于 `:55` 绑定。两条边界都测了：真实绑定边界走 `:53`（`max_row_bytes_boundary`），并用**测试内** monkeypatch `check_size→0` 隔离出 `:55` 编码 backstop（`encoded_backstop_rejects`）。未改 `check_size`、未放宽 `:55`。

2. **`set`/`frozenset` 残留边界（§5 同类，非回归）**：`check_size` 允许 set/frozenset，但 `_canonical_row` 只规范化 `dict/list/tuple/str/bytes/标量`；set/frozenset 走不透明透传分支（原样返回 + 身份签名），其**元素不被值规范化**。因此「含 set 且 set 元素为等值不同身份字符串」的行，活/复用仍可能产出不同字节 → 守卫仍可能 raise。性质：(a) `canonical ≤ raw` 仍成立（set 部分与 raw 逐字节相同，其余最大共享）；(b) #42 行与全部受测形状均不含 set；(c) 含 str 的 set 另有跨进程 hash 序不确定性（PYTHONHASHSEED），是更深的既有重放问题，与本修无关。按约束 E 不扩范围，特此声明供总控裁决是否后续单独立单。