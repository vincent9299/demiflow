# 显式 shuffle seed 的兼容修复

原始目标仍为 INTENT.md 的同一 Dataset 语义/统一引擎（I01）、完整公开能力（I14）、有限资源（I08）及历史保护（I13）。本修复只恢复同一 typed 输入多重集、同一 seed 的可重复值序；不要求复刻旧 Ray Data 的具体排列，不调整引擎的身份、提交或恢复权威。

## 反例与版本

root 已实测旧 `267b914` 的 Ray range→map 30 行在两个新 action 中 `random_shuffle(seed=7)` 同序；当前原公式在 Local/Ray 的新 action 中均不同序。`e05b6b5` 保留该 30 行等序断言以及复用同一 shuffle 实例的变体，未改为无序比较。

新编译的 RandomShuffleOp 配置携带 `shuffle_order_version=typed-multiset-v1`。没有标记的封存配置继续调用原 `SHA256(domain, seed, occurrence, record_id)`；未知标记拒绝。原 K011 helper、RandomSampleOp、RandomizeBlockOrderOp、Record 和历史 codec 不变。已存在 seal 的 prepare 路径继续直接加载原配置，平台 revision 恢复检查不变。

新排序的第一遍主键为原 `_encode_payload(row)` 的完整 typed bytes，相同 bytes 再按 `(record_id, source provenance)` 隐藏排序，为同一原 Record 稳定分配 duplicate ordinal。第二遍键为 `(SHA256(new_domain, seed, SHA256(row_bytes), duplicate_ordinal), row_bytes, duplicate_ordinal)`。ordinal 在第一遍的连续相同 bytes 内递增，只保存上一个 key。完整 bytes 与 ordinal 为碰撞兜底，action/occurrence/record_id/到达顺序不进入第二遍业务主排序。同 action 的输入重排仍保留身份次序；新 action 的身份变化只能交换同值记录，不影响可见值序。完全相同行仍各保留一次，可分散到不同输出位置；原 Record 与全部 sidecar 原样随行。

相同输入的定义是该版本化 typed bytes 相同，**包括 mapping 插入顺序**。不宣称忽略字段顺序，不新增另一份通用 codec。原支持的 bytes、tuple/list、set/frozenset、非有限浮点、负零、日期/Decimal、NumPy 等仍使用原 codec。

## 共驻窗口与生命周期

G 为 `resident_limit(bounds, services)` 得到的实际 worker 授予，不提高计划的资源请求或 run 限额：

| 活跃对象 | 子窗口 |
| --- | --- |
| 第一遍输入 reader | 3G/8 |
| 每遍 build 的保留 run | G/4 |
| build 的当前 typed/record 编码 | G/4 |
| merge 的两个 reader、heap、key 解码 | G/4 |
| merge 的 Record 解码余量 | G/8 |
| 两阶段 run 引用及列表元数据 | G/16 |

第一遍 build 至多 15G/16。输入原始 refs/borrowed 描述符在输入关闭后仍可能由 kernel 持有，因此另限定其不超过 G/16；第一遍 merge→第二遍 build 加入此实际保留量后至多 G。最终输出保留 merge、排序引用和原输入描述符，至多 G/2，通过独立 `_shuffle_retained_bytes` 加入现有只读 `retained_resident_bytes` getter，供下游编码使用剩余额度。新 SortWindow 的 codec 窗按实际 typed 结构预先检查，merge fan-in 固定为 2。每遍 run 数由元数据窗限制，不能无限产生微小 run 引用。此为受管对象与编码分配预算，不冒充进程 RSS 硬上界。

原 G/64 frame 上限确实误拒了旧公式在同 1 MiB grant 下成功的 16 KiB 字符串行。保留该真实红例后，改为 G/16 单帧上限，两个 reader 共享原 G/4 窗，按实际存活比较 key、record bytes、bytearray 与编码前 `raw_workspace` 逐次准入；没有提高 G 或 merge 的 3G/8 总窗。中间合并输出编码与 Record 解码不同时发生，输出编码按实际 reader 保留量加编码峰值检查同一个 3G/8 总窗。新动态分支显式推进有限 heap，并在请求下一帧前删除旧 entry/frame；避免 `heapq.merge` 的最后一路快捷路径保留未计费旧帧。旧无标记分支仍用原 `heapq.merge`。单 run 三个宽帧、两路转一路的真实 frame owner 存活测试与原预算宽行正例均通过。

新分支输入 spool 在受限服务视图下读取；视图只缩小 bounds，委托原服务端口及实际 worker 的 `_FiniteServices.check_stop`，不重建/复制 RuntimeServices 映射。新 flag 的写入按原格式每个 fragment 的解码工作集预先分段，保留输入描述符余量，避免大量小行先形成读取窗口放不下的旧式大 segment。完整 segment 在解码列表前再次检查其外层及各 Record 文档的共同工作集；旧无 flag 分段保持。借用输入通过原 finite reader 的 `frame_call` 接线，seal 的 read/decode、manifest 总量/树遍历及 frame 读取全部进入同一输入窗口；原 port 描述符、seal members 和 manifest traversal 栈共同计入 retained。原始工件与 schema 不重写。

两遍均复用现有 build_sort_runs/merge_sort_runs 核心与 RunArtifactStore 的 put/read/release。原输入、第一遍 runs、第二遍 runs、合并中间工件和临时写入共同计入原 max_spill；额度不足按原权威拒绝，未增加第二份磁盘额度。释放只撤销本次排序引用，物理删除/退款仍由原 store GC 在原生命周期执行，不能把 unpin 描述为立即回收磁盘。

包装器在成功、异常、取消和提前关闭时显式关闭输入及两遍 merge iterator，释放两遍原始 runs；merge 自己释放其创建的中间 runs。借用输入及历史 artifacts 不由此包装器释放。store 写失败的 staging 继续由原写入 finally/崩溃恢复协议处理。

## 验收状态

实现及测试在独立叶 `/yzp/zhaozy/yangzepeng/0905/_demiflow/worktrees/gpt_shuffle_seed_20261117`，基线 `c3393e4`，分支 `fix/runtime-v3-shuffle-seed-20261117`。生产代码最后修改为 `3f8aee5`；后续只有测试夹具/断言和本说明更新。测试均在获授权后持 `/tmp/demiflow-i08-final-execution.lock` 串行运行，使用原 `env-ray` 环境及私有 `/tmp/demiflow-shuffle-*` 资源域，未运行性能基准，未修改主仓或 `tests/runtime_v3_support.py`。最后已释放锁，并以非阻塞 flock 成功确认。

汇总 `/tmp/demiflow-shuffle-verification-20261117-r1/summary.json` 收录 **56 个唯一通过用例、168 个完整 setup/call/teardown 通过报告**：专项 21 项、原回归 35 项。汇总逐例核对五个生产文件哈希与最终候选一致。每批都有实际 worktree 导入断言、前后完整源快照一致、原冻结七件逐字节相同、完整退出及 children 回执。Ray 私有集群观测到的 29 个子进程最终全部退出；所有已接受批次 `remaining_live=[]`。

以下路径均位于 `/tmp/demiflow-shuffle-`，各目录包含 `receipt.json`、`phase.json`、`result.xml`、`children.json`、`frozen.json`：

| 证据目录后缀 | 结果 | 覆盖 |
| --- | --- | --- |
| `public-final-20261117-r3` | 3 passed | 原30行两变体、union/flatmap 120行、2/4块精确等序 |
| `tail-spill-20261117-r1` | 4 passed | 单/双路宽帧存活、同1MiB/16KiB旧新成功、共享磁盘额度 |
| `kernel-regression-20261117-r3` | 15 passed，3个旧夹具失败另补修 | 双外排、稳定重复身份、1000小行owned spool、宽seal/manifest、四类关闭/失败、typed值、旧seal、过小窗及原K009–11 |
| `ack-20261117-r4` | 2 passed | 真实原ACK输出前缀中断恢复、精确原artifact_staging_bytes额度拒绝 |
| `ray-20261117-r1` | 1 passed | Local/Ray六次action精确同序，禁止Ray Data路径 |
| `borrow-regression-20261117-r1` | 20 passed | 原finite borrow全部原预算/语义 |
| `public-regression-20261117-r1` | 3 passed | 原块数/union/惰性子计划 |
| `spool-regression-20261117-r2` | 3 passed | 补合法服务夹具后的原spool三例 |
| `wide-regression-20261117-r1` | 6 passed | 原宽二进制join、非交换fold、生成seed、超限拒绝、parked consumer |

共享磁盘额度测试在第二遍触发原 `ExecutionBudgetExceeded`，明确断言 `budget == 'artifact_staging_bytes'`；失败后 staging 为空，已写 immutable objects 的原消费量仍准确保留。恢复测试在原 `_finish_publication` 完成且 journal 已 ACK、`0 < 输出前缀 < 30` 时中断，恢复禁止重编，原 seal 不变，最终完整值序与独立action一致。重复值身份由独立反向/乱序 Record 测试锁定。

保留的失败证据不是通过计数：`wide-red-20261117-r3` 为真实旧成功/新G/64拒绝宽行红例；`public-recovery-20261117-r1` 钉住实际 finite service 代理不能 dataclasses.replace 的接线缺陷，已由 `616aff6` 修复。早期 import/无效SHA夹具/未触发retire注入点及一次 `-k` deselection 的无效验收均保留原日志，未算作绿证据。

原 spool 三例在 `c3393e4` 已有 `operator.py:275` 的 `_check_stop(self._services)`，但 `tests/test_runtime_v3_finite_spool.py:18,40,55` 的 SimpleNamespace 服务均缺 check_stop。本轮协调方授权 `af7b993` 仅补该必需端口；数据、预算、断言和生产检查均未改变。`kernel-regression-20261117-r3` 的整批 exit1 没有被改写为 exit0；汇总只接受其中15个完整通过的case，另以修正夹具后的原三例完整通过回执闭环。
