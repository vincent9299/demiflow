# 已发布孤儿工件回收：实现与验收

本包在隔离树 `gpt_v3_artifact_recovery_20261115` / `engine/gpt-v3-artifact-recovery-20261115` 完成，基于原集成候选 `8e85e7a`（本树 merge HEAD `3263cb3`）。主仓、外部历史数据、服务及共享环境未修改。精确源码摘要、原承重七件基线摘要和测试节点见同名 JSON；JSON 的 HEAD 是测试时基线，工作区实现由逐文件摘要绑定。

## 实际闭环

从原 RunJournal 逐页读取全部历史/current/prepared/unknown 根，确认所有原资源分配已 `CONFIRMED` 退出，确认原 journal 服务线程及原生 PromptJournal 批写线程真正结束，再取得同一物理 inode 的独占锁。只有整份有界追根成功，且原对象登记/原 reserve charge/文件 inode/hash/size 一致，才回收不被引用的已发布对象。目录扫描、mtime、旧 fence、关闭请求、文件缺失本身均不构成退款证据。

原 journal 唯一记录 `prepare → 已验证 quarantine → 物理删除 → finish/refund`。每一代对象绑定首次收费 reservation；同 hash 被删除后再 put 使用新 reservation 重新收费。旧 settlement 回执逐字保留，重复 finish 不重复退款。

此包补上已发布孤儿对象回收，前一包 staging 恢复继续保留。它不是主动缩减历史的策略，也不在活 action 的 cut 之间删除对象。

## 文件/接口接线

| 文件 | 本包职责 |
| --- | --- |
| `execution/dataflow_object_lease.py` | 永久物理锁、只读协议提示 header、SH/EX 和同 inode 校验。Header 不授予 GC 权限，原 journal 才登记协议与资格。 |
| `execution/dataflow_gc.py` | 有界完整追根、候选校验、quarantine/reconcile、可选回收时间边界、每 RunContext 一个待回收预算。 |
| `execution/run_journal.py` | 原 journal 增量表、对象 charge_id、原 publication/transaction 共同锁、原回收状态与幂等退款。无第二数据库。 |
| `execution/dataflow_store.py` | put/pin/read 生命周期共享租约；read generator 在 store.close 后继续持有自己的 FD/SH；关闭及初始化失败释放自己的租约。 |
| `execution/dataflow_authority.py` | DriverArtifactPort 原恢复入口与协议绑定；DriverJournalService 真线程退出查询；原 PromptJournal 持有独立 SH。 |
| `execution/dataflow_services.py` | driver/worker 装配失败释放原 store 的新租约，保留首个异常。 |
| `execution/worker/dataflow_execution.py` | 原 worker close 结束 reader/kernel 后释放 store 租约。 |
| `execution/dataflow.py` | 原物理资源、journal service 关闭后收口；原 native journal 尚在时只登记延后，不提前关闭。 |
| `execution/run_context.py` | 自然退出及既有 draining finalizer 两个原 native.close 后调用点；不改变原 check_stop、ACK 或 draining 语义。 |

## 根与并发边界

- 原表：全部 `runtime_actions` 的 declaration/snapshot/result、`runtime_sources`、所有状态的 `runtime_commits.intent/prepared`、`runtime_receipts`、`runtime_effects`、原 child commit scope declaration、原 native calls request/response/error。
- owner pin 通过原 publication/settlement 找回完整 typed ref。对象 pin 不被当作叶子；同 hash 多 media alias 都被追踪。
- 原 StateArtifacts 验 size/hash/path，含 paged artifacts；typed kernel state 内字符串形式 record JSON 继续解码。
- 间接根：borrowed finite seal/producer manifest、SegmentForest 子树、record payload_ref/schema_ref、durable delivery、worker frontier、channel data_b64、terminal typed_result。
- 新 store 首次发布前写完整 header/fsync，原 journal 绑定同一 lock inode。旧已有 store 不补资格。已登记后 lock/header 丢失或替换，read/pin/put/new writer 均拒绝，不修补、不降级。
- 每个 read generator 的共享租约跨 yield/FD 存活；原 writer/store 关闭不等于该 reader 结束。root publication/pin 与 GC 互斥。活 writer、未确认资源退出、原 native journal 批线程活着都阻止回收。
- 所有旧 cut、prepared 和 unknown 根仍保留；没有“只看最新 cut”的优化。未知 media/载体、缺根、超预算、旧未归属 reserve 都保守停止新删除。

## 有界性与时间

单次最多 64 个候选；refs/states/待处理集合同时受原 max_artifacts 和 `min(max_state_bytes,max_resident_bytes/16)/2048` 约束。不是拿单个控制帧 64 KiB 限整个 collector。默认预算下 48 个活根加 1 个孤儿的真实用例已回收成功。

SQL 根逐行读，读取前检查 encoded 长度；JSON 在 loads 前估算容器/字符串膨胀，递归/步数/总读取字节有限；对象摘要按 64 KiB 分块核验。未知或无法在给定工作集内证明完整性的载体会保留对象与额度。

可选 GC 共享一个最长 5 秒截止，扫描、摘要及候选遍历均检查。按 RunContext 原 wall deadline 和 monotonic elapsed 取最小剩余；剩余不足该 allowance 加 1 秒则跳过。publication lock 不等待，避免新增原本 30 秒的锁等待。原 check_stop 仍执行且可判原任务超时。文件系统 fsync/内核 I/O 本身不宣称可被 Python 截止硬中断。

每 RunContext 只保存一个保守 Bounds，不随 action 数增长。GC 异常作为 blocked 记录，不覆盖业务异常。不触碰原预算、业务输出或历史身份。

## 验收证据

最终源码固定期间两个独立进程：

| 证据 | 结果 | 范围 |
| --- | --- | --- |
| `/tmp/demiflow-published-gc-final.xml` | 29 passed / 38.61s | 新 GC 原生 journal/store/RunContext、真实 subprocess crash、真实 Dataset、关闭超时边界。 |
| `/tmp/demiflow-published-gc-h.xml` | 79 passed / 16.95s | 原 H 工件/控制接口单元契约。H 自带 peer/Ray doubles，不冒充真实 Ray。 |

新增关键反例：

1. released owner 且无任何根的对象真实删除；原 charge 只退一次；同 hash 再 put 重新收费；旧 settlement 不改。
2. 原 CommitCoordinator ACK 加后续 pending intent，嵌套 state → borrowed seal → SegmentForest → 字符串 record → payload/schema 全部保留。
3. 活 read generator 即使 store.close 后仍阻止；活 writer、活 publication owner、原 batch journal 线程均阻止。
4. 五个真实 `os._exit` 窗口：prepare、rename、确认 quarantine、unlink、refund；每窗口重复两次，同 run 新 fence 恢复，原额度归零且不重复退款。
5. 真实 Dataset 输入→map→materialize→原 ACK→自动 GC→两次历史结果读取；注入的无根工件删除，真实结果保留。
6. lock/header/both 缺失、同名 inode 替换、并发 pin、未知媒体、缺失根、decode/trace 超限均不误删/退款。
7. 原 PromptJournal 批线程确实启动，控制服务关闭仍不回收；原 RunContext.close 真实 join 后才回收，native call 原账保留。
8. wall/monotonic 两种临近超时仍成功退出；原已超时仍失败；action close 临近时限保留对象与额度；活 publication 锁不等待。

开发中间证据：R1 14p/1f 为新测试错误 Record ID；R2 42p/1f 为新测试未先真实 ACK 就创建下一个 commit。都修正测试构造以遵守真实协议，没有放宽生产。R4 修正后 48p/116.81s，另覆盖原 artifact accounting/recovery、真实有限 join cut 后恢复、异常 traceback 活着时 FD 清理、native RPC 重发；它早于最后 GC 时间保护，不冒称等于最终源码。R5 28p；R6 五项时间边界通过，103 deselected 明确披露（不把被筛掉的 H 计入）。最终另跑完整 29+79。

## 未宣称的能力

不删除任何历史可达根；不能承诺历史无限增长仍占固定磁盘。活 action 内 GC、多个机器/共享 NFS 的锁一致性、GC 新版完整 API/Ray/browser 矩阵本包未验。依赖共享工件文件系统提供一致的 POSIX flock、hardlink/rename 与 fsync；没有该物理能力不授予回收权限。

全量追根超过原预算或遇未知载体会跳过，因此不会把所有长流、所有历史格式的回收率冒称 100%。本包交付是新协议下 quiescent store 可证明无根对象的真实回收与恢复闭环，待总控及独立 agent 复核后集成。
