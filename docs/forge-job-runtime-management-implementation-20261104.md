# Forge 作业运行态管理：v4 组合交付

2026-11-05（Asia/Singapore）。沿用最新正式执行/恢复/资源/受管生图基础，保留 R1—R4 和最新 DemiFlow 外壳。本轮按终审 R5—R7 补齐内置 actor、固定批处理及关系型性能证据。旧 v3 完整归档保留，当前发布待总控终审；用户授权总控通过后由原负责人合入、原服务发布及核验。

## 冻结与来源

隔离源码 `/tmp/forge-runtime-20261104-0oi_zj87/combined-demiflow`；当前证据 `/tmp/forge-runtime-20261104-0oi_zj87/combined-final-v4`。最新正式 dirty/staged 的逐文件基线为 `843e74326f7fc0ab56385054428601b9ac91b48b`，原状态及 2253 文件清单保留在 `combined-evidence/formal-base-*`。实现提交 `530b9cc52d2713bc79edc9a8b20bd18ff486caf7`，文档交付提交见 `release-manifest.json`。

1022 源码清单 SHA256 `851fc7fedf6a5e8e35ccb337331b9f35c0254a752c6ba00601b27a1cd5684ad9`；275 测试清单 `5aa0688d2cf2ac1741dc460393e9b070e0cf47a7d1e82da159fbd5e63bf2e137`。每组回归、性能前后核对，运行中不改源码或测试。相比 v3 仅 6 个 package 模块改变，全部改动及精确哈希见 `reused-evidence.json`；新增测试 1 份、修改测试 2 份（后一个为整批结束后的图片预算 fixture，初始/最终测试绑定单列）。不是把 v3 的 531 项自动登记为本版通过。

上游最终回执为 `upstream-final/coordinator_final_baseline_handoff.json` / `completion_receipt.json`，inventory `f16b8a2507d01e29c3957350d6c76733efcc519051c8db27f7df35209292b07d`；其 244 通过/1 callback 入口失败与原 DPC 41/2、本地 43 通过均保留各自来源。正式源码、index、原服务未因本次隔离返修改变。

DemiFlow 品牌、Harness 侧栏和顶部均保留。`shell-integration-verification.json` 核对上游六份正式摘要：index 和三份浏览器测试完全相同；app.js/style.css 叠加运行态接线的差异保留。本轮重新验证包外 11 资产和展开/折叠/手机视口；v3 全部 33 项浏览器及真实历史曲线证据保持原来源。

## 组合架构与资源边界

继续复用 runs → supervisor/execute → CheckedExecution → Dataset，与原生 RunContext.attach 共用同一个 RunJournal 和 attempt。原生 `runtime_reservations(name,reservation_id,amount)` 保存资源预算；Forge 动作事实的同名表冲突已消除，仅将后者改名 `forge_action_reservations(attempt,action_id)`。没有第二份执行事实、调度器或恢复引擎。原生 receipts、state/materializations、source ACK 和未确认动作保护均保留；不恢复已退役的显式 checkpoint/SQLite prompt journal。

新 `run_storage` 连接既有原生对象所有权。prepare 从检查报告封存最多 32 个可写对象位置；监督/准入只从 RunJournal 读取本 run 的命名空间，核对注册位置、完整 namespace_record 和严格的 32 位小写十六进制 native ID，禁止路径穿越及 symlink。work、native state/materializations/scratch 和自有外部对象共用 scratch 计量，遍历总计最多 65536 项/队列、24 层、2 秒，inode 去重；必要保护失败不能伪装成可选遥测缺测。

每个独立对象设备预留整份 scratch 额度，工作区同设备不重复扣；表输出设备另预留表输出额度。停止后的 native 状态、物化及保留对象计入历史准入；活跃运行的增长由 scratch/管理预留覆盖。history_bytes 是新准入时的历史容量检查，加运行中的有限增长保护，并非内核磁盘硬配额。Web 明确显示“表输出计量”，指 OutputWatch 的表文件累计逻辑增长；对象物理存储在 scratch/device 账目中，原生编码输出/对象预算继续由现有机制控制。

### Q3 历史准入候选（2026-11-07，稳定性尚待收口）

`history_admission` 每次完整重计保留路径的逻辑文件长度，不持久缓存或增加账本，不按 inode 去重。根及祖先逐组件 nofollow 打开；目录 FD 相对 DFS 同时核对目录身份与结构变化、journal 类型/身份/64 MiB 上限。65,536 项、4,096 run、24 层限制保持；逐项检查时间，FD/iterator 随深度增长并在正常/失败时释放。

2026-11-07 的预算裁决明确把计算与等待分开：从 history 入口到必要排空/清理，仅本次调用线程和至多四个临时 worker 合计 5 CPU 秒；等待上限为 `min(history入口+30秒, D_operation)`，5 秒墙钟仅作为慢检查标记。每个参与线程单独采样 thread CPU，线程退出时保存最终累计，返回前核对全部参与者已收尾；不使用共享 Web 的 process CPU，也不只计主线程。实际超界用量包含已退出 worker 与收尾 CPU。墙钟与 CPU 均为合作式检查，单个阻塞 syscall、停止证明和必要记账可以超过目标并保留实际结果，不能称硬实时保证。

公开 prepare/start/resume 从入口建立或继承 60 秒 `D_operation`，嵌套只用剩余期限，resume 的准备与 start 共用同一期限；组合 submit 可显式包在同一 scope，分开的公开请求各自计时。单次 `_capture` 使用原子调用上限与剩余期限的较小者，父进程只在其子环境写私有期限，子进程在父身份验证后绑定。ContextVar 在 finally 恢复；常驻 Web 不写全局环境。常驻 reader 的复用由唯一集成负责，不得把一次控制请求的期限绑定到 reader 启动环境；本补丁不增加逐消息协议。native 业务 worker/observer 不继承控制期限，原运行时间与 CPU/RSS/AS/FD 额度保持。

到期禁止新业务动作，但保留已提交事实：durably ready 的 seal/receipt 不因晚回复改失败或删除；退出成功的单次 worker 可排空有界终态回复，未完整关闭的回复仍拒绝。可能 fork 后保留 starting、unit、slot 及未知用量，不能自动重放或未经停止证明释放；resume 已提交 attempt2 即保留 attempt2，后续 start 可因剩余期限不足而拒绝。request_id 继续复用既有 receipt/index/reconcile，没有 ID 不承诺 exactly-once。cancel 保持独立通道和既有锁序。这是等待策略调整，不改变业务容量或计算额度，也不提供首次必成功或冷态 SLO。

同次准入最多四个临时 I/O worker、四个在途 run、四个标量结果；主线程和新增线程固定到同一已有允许 CPU，返回前恢复调用线程原 affinity。四个 job 共享单一 deadline 和受锁保护的总条目计数，不各得一份预算。每目录先在全局128 MiB保守字节配额内保存条目，关闭iterator后继续用保留父FD作相对DFS，避免每层同时常驻两份FD。名称/path的Unicode存储、DirEntry、配对对象、列表容量和后续stat对象均预留；消费/失败释放，极端宽名称/扇出可能先遇内存限制，明确resource_limit而非截断。metadata读取/解析/比较以短锁串行，遍历不持metadata锁。失败停止派发、取消未启动项并等待所有已启动项排空、FD/reader/线程/缓冲关闭后才返回，不保留跨请求pool或顺序/并行两套配置分支。

元数据仍以 RunJournal 为权威：每个短snapshot独立只读事务读取状态、native identity、manifest和registered roots。字段在SQL返回前限制字节，manifest至多8 MiB，roots至多64行，每行root_id/logical_root/physical_root/run_id分别至多64/4096/4096/32字节，且保留原生每记录8192字节上限；仅在已截限结果上排序。状态兼容原65,536字符边界（UTF-8最多四倍字节）；native identity至多1 MiB。解码前限制JSON深度64、结构标记数524,288，避免短字节文本制造过多容器。事务和连接在JSON解码及文件遍历前结束。完整验证后只保留storage必要字段，同时计算完整原始manifest/identity摘要并区分缺失；前后比较完整摘要、字段及namespace，投影不忽略原文改变，也不长期保留无关大值。

前后短快照的 namespace、native identity、允许位置或 attempt 变化时拒绝未知容量；任一快照 active 或 admitting 仍按 `max(actual,512 MiB)` 预留。prepare 默认另留 512 MiB，start 独立重检，host/device/scratch/evidence/output 和 CPU/RSS 预留不变。短 SQL 快照与路径检查不构成文件系统原子快照。

work/evidence/checkpoints 仅在 run 根跳过；外部自有 namespace 的同名目录照常计费。symlink、FIFO/socket/device、目录替换、条目消失或无法读取时不以零容量放行。旧无 runtime 表的 journal、无状态行以及新 prepare 的空目录继续按其历史语义核算，损坏/未知状态明确拒绝。namespace 仍复用同一注册位置、native ID 与完整 record 验证，不扫描整个逻辑对象库。

调整前四路候选完成110项相关回归；同一冻结运行源码的8/14/32/50-run有序只读测量均完成，52-run实际超过65,536项时明确resource_limit拒绝。32-run两次1.517/1.658秒，50-run两次2.792/2.632秒；原demo只读完整计费443,006,065 B，两次4.196/1.193秒，与基线完成扫描相同。这些是原5秒墙钟合同下的历史证据，不登记为新预算实现通过。OS缓存与文件系统延迟不受控，这些结果不构成冷态或硬5秒承诺。

组合压力同时覆盖四份近8 MiB manifest/近1 MiB identity、深度24对象树和129,305,856 B保守条目缓冲，另保持只读SQLite collector时FD峰值121、总task峰值5、RSS峰值192,626,688 B，未扩大原预算。附加collector的一次原5秒扫描超时拒绝；30秒只读资源诊断完整完成，其结果不代替原窗口成功。每次FD/缓冲释放、无Python扫描worker；原始返回瞬间内核task计数有时为2，连续正常/异常/超时准入未观察到扫描工作重叠，最终仅主task。

调整前私有公开API已验证连续prepare、两行成功输出、输出行数预算失败、部分提交后停止及attempt2恢复/再次停止，完整源码与停止证明另独立核验。首次测试脚本将全部执行限制到一CPU触发既有两线程保护、以及终态后过早检查租约的失败均保留；只修正私有验证脚本，平台执行预算与接口未改。此后原demo由原负责人验证了真实成功和取消/attempt2成功，但首次 prepare 的5秒拒绝保留；不能用之后的成功覆盖首次失败。本预算候选只做专属私有测试和有限生命周期，原demo首用可靠性由固定补丁审后再作预登记验证，Q3尚未关闭。测试见 `tests/test_forge_history_admission.py` 和 `tests/test_forge_operation_budget.py`；阶段交付材料分别绑定源码和原始结果。

清理先证明 Forge 子进程停止并持有原执行租约，再非阻塞取得原生 `.runtime.lock`，通过原生 claim_execution/clean_orphans/release_execution 栅栏清理死所有者的孤儿。不会执行 action、推进进度或创建新 attempt；prepared/committed 资产保留。锁被占用、损坏或清理失败时 cleanup=pending，整个 slot 与设备预留保留。显式 reconcile 成功后才可复用。

v3 真实跨设备补充 `cross-device/results.xml` 为 1 项通过，保留原版本证据且不加入 v4 计数：自有 `/dev/shm` 对象设备 162 与工作区设备 66318 分开预留（8388608 / 555745280 字节）。图片字节/hash、scratch、保留历史、cleanup=complete 均核对；相邻 100000 字节其他命名空间未扫描或改写。仅删除本次自有临时目录。此结果不代表任意远程对象设备或物理磁盘耗尽测试。


## R1—R4 的保留证据

以下实现及专项 fixture 从 v3 到 v4 未变，v3 的 102 项运行态集合和 R1 通知原样本补验按原版本保留，不加入 v4 计数。本轮另重跑运行态集成、原生资源、受管图片、停止/恢复、HTTP 与 Web 相关集合，覆盖新执行改动的交集。`reused-evidence.json` 逐项列出摘要、未变模块及适用性。

| 问题 | 现实现与证据 |
| --- | --- |
| R1 可选指标阻断控制 | latest/series 对有界 JSON、结构、类型、有限数值、SQL 身份校验；坏指标 unavailable，合法 partial/null 保留。40 项返修集合包含实际 supervisor 的 latest 路径。补充 `r1-notification-check` 逐一核对通知中的 tIme 样本、坏 JSON/字段/类型/NaN/超长、HTTP snapshot 200、compact cancel 200/stopping、首次停止意图不变；必要 RunJournal SQLite 错误照常传播。 |
| R2 starting 永久占域 | starting/index.publish 前持久化 unit 与父目录；同一 fd 的 dev/inode 验证。索引失败及三个启动前硬退出可恢复；尝试 Popen 后丢身份仍保留未知用量及租约，真实独立 session 后代停止后才收口。 |
| R3 清理残留漏账 | cleanup=pending 保留整个 slot，resume 不先创建新 attempt；设备预算拒绝新准入，显式清理后释放。v3 已覆盖 native 对象孤儿清理失败/重试和原生锁占用。 |
| R4 pending 永久占额 | 持准备、身份收据和执行锁，确认没有执行事实，关闭原 ID 的不完整准备；无目录、部分封存、空库/初始化库、过期和关闭中再退出均覆盖。未知执行仍 pending，同 ID 不创建第二 run。 |


## R5：具体内置 actor 的原生状态适配

DecodeDocument、EncodeImage、ParseWikitext 的处理状态都只存在于单行调用，配置已绑定原生计划身份；解析器/图片子进程和临时目录在返回前排空，没有跨行语义游标。三者显式 snapshot 返回空状态，restore 严格拒绝非空或错误类型。LanceLookup 的 URI/固定版本/策略也绑定原生身份；table/index 句柄与有界 LRU 可重建，astart 重新打开固定版本，restore 清空缓存，aclose 释放句柄。没有给任意 actor 注入默认空状态，也没有改变 OperatorStates 的拒绝规则。

原四条 Forge 成功路径重新执行；新增独立进程在首个可靠提交后硬退出，恢复同一 run/attempt 栅栏，从原 ACK 位置续接。断言全部输出、重复行/缺键/空值、原 commit ID、状态文件；lookup 在两次执行间推进表 head，仍读取固定旧版。生命周期测试验证句柄/缓存关闭及重建；图片实际编码结果核验 hash。

## R6：固定批处理的边界

原 flat_batch 默认预留 268445696 字节，超过 268435456，拒绝仍正确。保留原失败，新增明确 raises 的负对照；另设 1 MiB 行、2 MiB 批额度验证同一 flat_map→batch_map 完整扩增/物化，未放宽准入或删结果。

固定计数 batch_map 仅在 concurrency=1、flush_interval=None、无 group/prefix 时开放自动 materialize。采用现有 BlockSink/CommitCoordinator，在正常 EOF 将全部 worker 和尾批排空之后作一个终端提交，再关闭 actor；不在中途 source 边界强制 flush 或提前 ACK，不新增恢复账本，不序列化 pending 原始行。BlockSink 原有行/批/累计输出/scratch 约束继续生效；原生批处理新增累计字节校验，在调用批函数前拒绝超限。内存不随源总行数累计，临时输出占用由现有任务总额度约束。

提交前崩溃重新计算尚未提交的工作；prepared 或 sink 提交后失联恢复同一回执，不重复批函数。测试 538 个输入含重复/bytes/null，扩增 1076 行跨越 512 源边界，固定 7 行批次及 5 行尾批；核对顺序、多重性和所有结果。另测空源/单行、取消、累计输出行/字节上限、单行可入但整批超字节、计时/多 worker 拒绝。该策略没有中途进度恢复承诺，不扩大其他 P3 buffered/grouped/prefix 适配范围；有外部副作用的用户函数仍须遵循其原有幂等/原生提交契约。

## 22 项矩阵

矩阵分清本轮新证据与保留证据；旧样本不算当前测试通过数。完整用例 ID、JUnit、来源在 `acceptance-final.json` 与 `reused-evidence.json`。

| 编号 | 覆盖入口 | 证据与边界 | 本轮来源 |
| --- | --- | --- | --- |
| RM-01 | runtime_protocol、public_api、audit_runs | 已登记输入、只读前提 requires_prepare、未知字段/未登记入口/非法副作用拒绝；检查缓存不授予执行 | v3 原始证据；相关实现/测试字节未变 |
| RM-02 | forge_runs、run_boundaries、native_semantics、audit_runs | 源码/参数/平台/scope 与实际 worker 契约；无关文件变化对照；历史封存保留 | v4 相关子集重跑；完整旧集合保留 v3 |
| RM-03 | runtime_management、runtime_protocol、runtime_browser、runtime_review_fixes R4 | ID→run 先于准备，文件及目录 fsync，lost-ready 显式恢复，payload 冲突/过期；浏览器丢 HTTP 回复后同 ID 仅一 run | v3 原始证据；相关实现/测试字节未变 |
| RM-04 | forge_runs、resume、public_api | 重复 start、attempt CAS、null/bool/stale 拒绝、resume 同一身份；无等待队列 | v4 相关子集重跑；完整旧集合保留 v3 |
| RM-05 | runtime_management、runtime_integration、runtime_review_fixes R3 | 不同 TMPDIR/项目、CLI+HTTP、active=2、下调策略不接管既有运行、重叠输出、设备/inode 别名事实、分配块去重 | v4 相关子集重跑；完整旧集合保留 v3 |
| RM-06 | forge/native/builtins/managed-images | 原有 document/image/wiki collect、固定 LanceLookup 恢复成功；合法预算 flat_map→固定 batch_map 完整扩增与物化；原超预算 fixture 仍拒绝 | v4 相关子集重跑；完整旧集合保留 v3 |
| RM-07 | run_faults、resume、runtime_mock_http、managed_image_joint、runtime_foundation | 部分提交后停止、异步排空、HTTP 在途停止、原生对象取消/崩溃清理；未 prepared 的未知 action 仍拒绝重放 | v4 相关子集重跑；完整旧集合保留 v3 |
| RM-08 | runtime_integration、runtime_management、process_group、runtime_review_fixes R2 | 真实 DataFusion 新 session；分别丢 worker/supervisor；继承锁启动窗口、leader 先退、无关 canary；停止确认后终态 | v4 相关子集重跑；完整旧集合保留 v3 |
| RM-09 | runtime_management、runtime_protocol、public_api | 重复 stop 不刷新首次时间/事件，pidfd 身份不匹配不发 signal；旧 attempt 无权写当前状态 | v3 原始证据；相关实现/测试字节未变 |
| RM-10 | execution_recovery_*、runtime_builtin_recovery、managed_image_joint、resume | 原生 receipts/ACK/CAS；四内置 actor 从提交位置恢复；批处理 prepared/sink 回执丢失恢复不重调；未准备工作重新计算 | v4 相关子集重跑；完整旧集合保留 v3 |
| RM-11 | resume、runtime_management | 不完整记账/额度耗尽/stale/业务失败拒绝恢复；action/node 写前预留与终态空间；历史 attempt 保持 | v4 相关子集重跑；完整旧集合保留 v3 |
| RM-12 | runtime_management、performance | CPU 忙样本、首点 null、集合稳定差分、RSS/I/O 范围；对照 getrusage 资源；不声称连续真实峰值 | v3 原始证据；相关实现/测试字节未变 |
| RM-13 | foundation_observations、native_semantics、stream、runtime_builtin_recovery | occurrence/融合/扩增/过滤及生命周期；新增 lookup、批处理结果断言；缺测仍空，无全局百分比或 ETA | 计数原证据 v3；新增兼容结果 v4 |
| RM-14 | runtime_management、runtime_integration、runtime_review_fixes R1 | helper 退出、侧库损坏、关闭遥测、SQLite 满页、保留窗口、分 attempt 聚合；控制与真实结果仍有效 | v4 相关子集重跑；完整旧集合保留 v3 |
| RM-15 | run_faults、run_boundaries、output_watch | 行/输出/scratch/分配保护、监控故障与不可用用量；区别于可选指标缺测 | v4 相关子集重跑；完整旧集合保留 v3 |
| RM-16 | runtime_management、runtime_integration、runtime_foundation、cross-device | 默认准入、active 历史预留、封存/事实/诊断/SQLite 上限；native state/materializations/scratch 和自有对象计量；真实跨设备预留、保留历史、故障清理后复用 slot | v4 相关子集重跑；完整旧集合保留 v3 |
| RM-17 | runtime_integration、console、public_api | submit/control/attach 显式 CAS、旧 allow_runs GET 不迁移；control-only 停止；常规 operation slot 被占时短通道可取消；共享 10 连接占满时可返回 503 | v4 相关子集重跑；完整旧集合保留 v3 |
| RM-18 | runtime_management、run_browser、workspace_sync_browser、package | CLI/Web 多端、服务重启、条件 revision 包含 usage、历史 cursor 和分钟指标 attempt 隔离；大动作 payload 不进入轮询 | v4 相关子集重跑；完整旧集合保留 v3 |
| RM-19 | runtime_browser、web_redesign、audit_browser、operator_foundation_browser | 实际 Chromium；1440/390/320、键盘停止、丢回复重试、项目 tabs/紧凑页头、部分结果/缺指标、历史与草稿 | v4 相关子集重跑；完整旧集合保留 v3 |
| RM-20 | performance/results、comparisons、performance-plan | 保留 v3 三家族与关系型 +5.066%；v4 仅关系型三对预登记、交替顺序、全部样本，目标按实际结果判断 | v4 三对 + 明确标记的 v3 原样本 |
| RM-21 | package cold/runtime/builtins verification | 新 wheel/sdist 摘要；包外冷入口、CLI 停止/恢复、11 资产与三视口；新增五条实际内置/批处理路径 | v4 全部重建与重跑 |
| RM-22 | runtime_protocol | HTTP ray/gpu/arbitrary_script/config_run_pipeline 未知后端字段明确拒绝、零 run；没有真实 Ray/GPU 支持声明 | v3 原始证据；相关实现/测试字节未变 |


## 当前回归与包外验证

当前受影响范围 **253 个去重用例：252 通过、1 失败、0 跳过**，另 0 子测试不加到主数。唯一保留失败为原 flat_batch 超预算成功 fixture；新显式负对照和合法预算完整结果正向均通过。

| 集合 | 通过 | 失败 | 秒 |
| --- | ---: | ---: | ---: |
| builtins-final | 55 | 0 | 76.64 |
| native | 135 | 0 | 190.90 |
| forge | 39 | 1 | 559.48 |
| integration | 10 | 0 | 301.10 |
| managed-images | 13 | 0 | 549.25 |

首轮 builtins 的旧图片 fixture 局部执行器配置 128 MiB、CheckedExecution 配置 1 GiB，但隐式 RunContext 仍为默认 256 MiB；图片固定预留加流缓冲无法容纳，正确拒绝。保存整批后第一次仅调整局部执行器仍被拒绝，结果/fixtures 继续单列保留。最终测试显式绑定 1 GiB 原生 RunContext，与原检查额度一致；128 MiB 失败/零提交负对照保留。完整 builtins 集合重新执行。源码 1022 文件从首次冻结起未变，其余已执行测试文件也完全相同。初始/最终两份测试清单、修正 diff、时间和分组绑定见 `fixture-budget-correction.json`；没有把失败翻成旧批次通过。

旧两项 open_checkpoint 成功 fixture 失败和六项旧 checkpoint/sqlite_journal 排除保持为总控接受的退役入口负向证据；不恢复平行协议、不算成功。v4 原生退役入口否定测试重新执行。v3 的初始错误、R5/R6 开发中的测试脚本错误及修正全部保留，均不计最终通过。

新 wheel/sdist 逐项核对 1022 源文件。包外冷入口 3 项/12 子测试、CLI 8 次调用（含部分提交后停止及 attempt 2 恢复/停止）、11 个 HTTP 资产、1440/390/320 和折叠侧栏通过；导入源均在 wheel-extracted，没有 checkout 兜底。另五条包外真实路径 document/image/wiki/固定 lookup/flat_batch 全部核对输出，其中批处理由 4 行扩增成 8 行并保留尾批。最终 sdist 另精确核对全部本次变更文档/测试。

新故障/停止/受管图片证据与 v3 保留的跨设备、R1 通知原样本及全 Web 集合分别登记；`reused-evidence.json` 给出旧源摘要、未变模块和适用范围，不增加本轮主用例数。

共享 HTTP 总接入上限仍为 10，全部被占时 cancel 可返回 503；RM17 仅保证常规 operation slot 被占时短控制通道可用。实测 Linux x86_64，未在 aarch64 执行；没有真实模型/provider/GPU/业务数据执行。所有峰值为有限采样，不能表述为连续内核硬上限。

## R7：预登记性能与实际结果

测量前已固定三对、最多六次，顺序 off/on、on/off、off/on；同一三行自连接与每行 678749566 次确定性 CPU 计算，回归结束后执行，没有替换或第四对。旧 v3 关系型 +5.066% 原样保留。全部结果逐行一致；六次执行均 ≥60 秒。

| 对照 | 关闭/开启秒 | 墙钟差异 | 关闭/开启可靠 CPU 秒 | helper CPU 秒 | helper RSS MiB / FD |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 71.321 / 70.707 | -0.861% | 71.449 / 70.959 | 0.27 | 35.40 / 5 |
| 2 | 67.143 / 67.072 | -0.106% | 66.498 / 68.023 | 0.29 | 35.43 / 5 |
| 3 | 67.404 / 70.131 | +4.046% | 67.064 / 70.433 | 0.28 | 35.36 / 5 |

三对总墙钟比差异 +0.992%，配对中位差异 -0.106%。**按预登记规则，≤3% 目标未建立，完整结果交总控裁决，不以聚合值覆盖超标对。**

可靠 CPU 包含 supervisor 和已回收后代，helper CPU 为有限观察计数；主机 loadavg、worker RSS、FD、目录逻辑/分配块峰值、getrusage I/O、sidecar 大小/采样覆盖/探针耗时和测量自身扫描成本保存在 `performance/results.json`。未降低保护频率或可靠记账。共享负载不能被直接宣称为差异原因，三对测量也不是置信区间。所有单次终态在 `performance/*-final-state.json`，补充对比见 `performance-assessment.json`。

保留的 v3 sync −3.156%、stream +0.205% 和短任务数据仍标为 v3 来源，未重跑也不当作 v4 新测量。尤其 stream.py 本轮有批处理变化，不能据旧 stream 样本声称本版整类性能已认证。



对已有六样本进一步核对原生融合组观测（未增加测量）：第三对总墙钟增加 2.727 秒，其中融合组累计时间增加 2.360 秒，可靠 CPU 增加 3.369 秒；helper 观察 CPU 为 0.28 秒。前两对融合组差异分别为 −0.946 和 +0.869 秒，没有每对同向的持续墙钟差异。融合组计时包含读/解码/spill，不是单节点 CPU profile；这些数据只能定位差异主要落在已有执行组，不能据此证明来自遥测或共享主机噪声。当前无法完成因果归因，逐对 ≤3% 目标保留未达标，交总控裁决。原始拆分见 `performance-hotspot-analysis.json`。

## 发布与准确回退

`runtime.patch` 基于正式 dirty 快照 `843e7432…`，在全新基线解包目录实际应用并核对全部冻结源码及交付文件。`merge-readiness.json` 在正式仓只读核对目标原值、未修改源码及新增模块。`runtime-delivery.tar.gz` 与逐项 `bundle-manifest.json` 包含当前原始结果、制品及旧证据来源；不包含令牌/私人启动环境。

总控须对本组合及实际性能结论明确通过后，原负责人执行正式合入和原服务发布，无需重复业务许可。发布前再次校验 Review SHA/候选 SHA、正式漂移和既有 dirty/staged；保存私有启动参数/环境、原端口、登记/令牌/业务控制摘要和逐文件备份。仅替换审阅文件，保持 index。回退仅处理本次已写且后置摘要匹配的文件，拒绝覆盖并发修改，按原参数/环境/端口恢复服务。发布后核对全部源码、三个在线资产、登记/API/控制原值和 index。**当前尚未发布。**
