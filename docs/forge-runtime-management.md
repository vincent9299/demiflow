# Forge Local 运行管理 v1

本能力复用 `runs`、既有 supervisor、CheckedExecution 和 Dataset。支持已登记、实际检查通过的原生 Local Flow；原项目识别成功不会获得执行权限。PipelineBundle、Ray、GPU、任意脚本及旧 `--config` 协议不经此入口执行。当前 Linux 实现要求 `/proc`、flock、subreaper 和 pidfd；x86_64/aarch64 的 pidfd 系统调用有显式封装。它不是恶意同用户代码的安全沙箱，也不是 cgroup CPU/RSS 硬配额。

## 准备、启动和停止

`run admission ROOT ENTRY --params PATH` 只验证已登记参数和预算前提，返回 `requires_prepare` 及尚待检查项，不保存检查通过事实。Prepare 仍检查实际有效输出、源码/平台、固定输入、scope 和原生效果；start/resume 再核验现场绑定。

```sh
demiflow workbench run request-id
# 保存生成的 REQUEST_ID，以及精确的请求参数：
demiflow workbench run prepare ROOT ENTRY --params PATH --request-id REQUEST_ID --json
demiflow workbench run request ROOT REQUEST_ID --json
demiflow workbench run start ROOT RUN_ID --expected DIGEST --expected-attempt 1 --json
demiflow workbench run snapshot ROOT RUN_ID --attempt 1 --json
demiflow workbench run cancel ROOT RUN_ID --expected-attempt 1 --compact --json
```

ID→run 的文件和目录项先 fsync，然后才准备运行。重复 ID 和相同参数返回同一 run；不同参数冲突。失联后可以重发原 prepare 请求，也可查询 request；`pending` 需要显式 `request ROOT REQUEST_ID --reconcile` 核对已有封存，不生成新 run。若目录尚未创建或只有可证明未执行的部分封存，持有准备、收据及执行锁后清理私有残片并将原收据记为 failed；未知执行证据保留 pending。过期 pending 也可显式关闭，旧 ID 仍不可重放。失败保留失败事实。未知 ID 的时间必须在服务器当前时间前 24 小时至后 5 分钟内；已知 ID 可重放 7 天，过期后拒绝复用。每项目最多 4096 个收据、16 MiB，另有最多 16 个未完成收据。Web 在当前浏览器会话保存最多 16 个请求身份；关闭标签页后的恢复应使用已保存 ID 或运行记录。

start 捕获/校验明确的 attempt，重复调用不会启动第二个执行。resume 需要调用者观察到的 stopped attempt、技术恢复许可、完整累计用量和剩余额度；失联重试原 expected_attempt 不会新建 N+2。未知最终用量不能通过图表补成完整。重试前先读当前状态。

停止回执只表示已记录 `stopping`。首次请求时间不因重发刷新，随后显示 terminating/killing/confirmed。只有自有 worker 及独立 session 的原生后代已排空，才写终态；启动状态和域预留可见前，先 fsync 执行单元文件及目录；继承的执行单元锁和设备/inode 身份覆盖进程身份写入前的窗口。可证明尚未尝试启动时恢复为 launch_failed；一旦尝试 Popen，未确认的启动保留不完整记账。无法证明停止时保留 stopping、cleanup pending 和租约。已经停止但 cleanup=pending 时也保留整个域 slot，直至显式清理成功，避免遗漏残留 scratch 的设备预留。cleanup 未完成时 resume 明确拒绝，保持原 attempt 可供 reconcile 清理，不先创建无法准入的新 attempt。Supervisor 丢失后，GET 不自动修复；显式 reconcile 检查停止证明、原生提交标记和固定版本。部分提交、未知提交、未完成最终记账分别保留。

新准备使用 manifest v5 / `demiflow_forge_runtime_v1`。旧 v1–v4 journal 仍可读，保留其历史语义；旧 prepared seal 不会被新控制器接管或隐式升级，需重新 prepare。原日志和必要恢复证据不因版本迁移被删除。

停止后清理原生对象需持 Forge 租约、取得原生执行锁并使用原 RunJournal 的 owner fence；只回收未被 prepared/committed 保留的孤儿。锁/清理失败仍保留 cleanup=pending 和整个 slot，不启动第二个 run 或 attempt 来做清理。

## 部署域和预算

准备运行时，工作台在 manifest 的 `limits` 中保存合并后的有效上限，在 `limits_explicit` 中记录项目预算与本次请求实际填写的字段。显式填写与默认值相同也会留下记录。预览、准备和启动共用这组预算，恢复时沿用原运行身份。网页初始显示的默认超时仅在用户编辑后作为本次请求参数提交。

同一参与者域由 `DEMIFLOW_FORGE_DOMAIN` 指定，缺省 `/var/tmp/demiflow-forge-UID`，不随 TMPDIR、项目目录、CLI 或 HTTP 进程变化。路径必须绝对且无符号链接。域中的 `policy.json` 只接受下表字段；管理员显式写入配置，普通 GET 不建立或升级策略。修改策略不终止既有运行；新准入按当前总量检查。不要更换域路径来绕过存活租约。

| 字段 | 默认值 | 含义 |
| --- | --- | --- |
| max_active | 1 | 无等待队列；最高 16；未知所有者占用槽位 |
| max_rss_bytes | 8 GiB | 每运行 worker 树的阈值，加 supervisor 256 MiB、遥测 helper 128 MiB；另加全域检查树 768 MiB |
| cpu_threads | 7 | 每运行 5 个并行度预留：driver 1、native 2、控制/采样 2；全域检查另 2。此值是准入计数，不是内核 CPU 时间或全部库辅助线程的硬上限 |
| device_bytes | 16 GiB | 每个设备累计的 scratch、恢复证据、管理和输出预留 |
| history_bytes | 4 GiB | 每项目历史准入容量，最高 64 GiB；包含停止后保留的原生 state/materializations/scratch 及自有对象 |

例如 active=2，CPU 至少 12；内存至少两份有效 worker RSS、两份 384 MiB 控制开销，再加全域 768 MiB 检查树。默认 worker 为 4 GiB，两个默认运行需要至少 9.5 GiB 的域内存预算。检查阶段使用一个非阻塞全域租约，覆盖 prepare/preflight/resume/reconcile，租约传递到检查后代；受管检查树按 768 MiB 采样保护。普通只读平台检查和未参与此域的普通 Dataset/外部进程不计作已受部署域管理。

每项目保留原执行互斥。输出租约比较规范路径、祖先/后代和已有祖先的设备/inode，处理 bind alias；最终写入仍依赖原生 CAS。每运行按所属设备预留 scratch + evidence + 512 MiB 管理空间，并在每个表输出设备保守预留全部输出额度。另在每个独立可写对象设备预留全部 scratch 额度，同设备不重复扣 scratch。设备配置检查累计上限；实际剩余空间检查扣除已分配块及 admission 后的增长，避免再把已经占用的块完整扣一次。遍历有条目、深度、时间预算，未知覆盖拒绝新准入。不是物理分区配额，也不承诺外部进程不会耗尽磁盘。

默认一次运行保留原有 4 GiB scratch、2 GiB 累计输出、64 MiB recovery evidence/checkpoints、600 秒累计时限。输出提交不因失败或取消回滚。工作目录在执行确认停止后清理；恢复证据及必要控制事实保留。原生 state/materializations/scratch 及仅本 run 拥有的外部对象与 work 共用 scratch 计量；最多 32 个封存对象位置、65536 遍历项/队列、24 层和 2 秒总预算。停止后这些保留资产进入历史实际字节，活跃增长另受 scratch 预留及采样保护。历史容量是准入检查，非内核连续硬配额。历史准入按 inactive 实际字节与 active/待准入运行的完整 512 MiB 管理预留核算；准备新运行也预留完整管理空间。容量不足明确拒绝，需要显式历史管理，不自动删表、Notebook 或恢复证据。

| 管理类别 | 有界策略 |
| --- | --- |
| 业务源码与平台封存 | 业务 Python 源码按实际字节计入磁盘和历史容量。平台快照和配置等资源合计最多 192 MiB，全部封存文件合计最多 16384 项。复制时分块校验内容。 |
| 可靠 run.sqlite | 64 MiB 页上限，回滚旁文件另留相同量级；8 MiB manifest、16 MiB action facts、32 MiB node facts，加索引/控制余量 |
| 写前事实预留 | 每 action/attempt 8 × 256 KiB immutable fact 槽；因此累计最多 8 个 action/attempt 预留。128 actions / 32 attempts 是身份上界，不能绕过更紧的累计字节配额；超限在执行前拒绝 |
| 控制事件 | 每 run 最多 1024；重复 stop 不增加事件；单次分页 100 / 128 KiB |
| 运行日志 | 默认每 attempt 16 MiB，单 run 累计 64 MiB；只淘汰旧 attempt 日志并保留截断标记；读取最多 64 KiB，UTF-8 跨页字符不破坏 |
| supervisor 日志 | 每 attempt 64 KiB，32 attempts 不超过 2 MiB |
| 原生 query 诊断 | 每 run 16 个轮换槽，跨 query/attempt 共用；每槽日志 4 × 64 KiB，JSON 文档各 64 KiB；总量低于 16 MiB；stdout 在写入中轮转 |
| 遥测 SQLite | 主库 16 MiB，DELETE 回滚上界另留同量；单样本 2 KiB、分钟摘要 1 KiB；900 个原始点/30 分钟，最多 10080 个分 attempt 的分钟摘要/最近执行 7 天窗口 |

原生诊断位于运行自己的 diagnostics，作者资源解析仍使用封存 source，必要 IPC 放在 scratch/control。诊断失败可丢弃并标记，必要提交/结果事实超限则明确失败；不把可靠控制数据当作可丢日志。查询内层池仍随各 run 的 TMPDIR，外层负责 participating runs 总量，不修改普通 Dataset 的池语义。

RSS、目录及输出保护按约 0.2 秒采样；进程树最多 128 个进程、每进程最多 256 个线程。helper 每 2 秒采样、地址空间额外 64 MiB、FD 128，超过 5 秒未更新或 128 MiB RSS 阈值可终止 helper。采样有间隙和阈值超调，不替代主动 reader/writer 的有界设计，不宣称连续硬限制。

## 指标、HTTP 和 Web

`run snapshot` 从同一 journal 的小投影读取状态、输出事实及最多 64 个标量节点，最大 128 KiB。revision 包含状态/用量、动作、节点、遥测和活性，支持条件读取。服务器按 root/run/attempt 缓存 1 秒，最多 32 项/8 MiB，授权检查先于缓存；返回缓存和采样年龄。

CPU 单位是观察到的 worker/后代 CPU 秒除以经过秒，即“核”，不加已退出子进程累计时间。进程集合变化或首次采样的速率为 null/partial；RSS 是采样时各进程驻留和，可能重复共享页，不是独占物理内存。I/O 只报告 `/proc/io` 可见且身份集合稳定时的计数变化。短命进程、缓存与退出间隙不能据此推导全系统 I/O。`coverage=sampled` 说明进程集合稳定；I/O 不可读时仍单独返回 null，调用方不能由此字段推断 I/O 完整。吞吐按 action/node/occurrence 的最多 10 秒窗口，不合成全局百分比或 ETA。“表输出计量”卡片是 OutputWatch 的表文件大小累计增长（st_size 高水位），设备准入另用分配块（st_blocks），不混同为物理磁盘写入量；原生对象资产不在该表计数中，由 scratch/device 和原生输出预算核算。

`run metrics --resolution raw|minute --points 600` 最多返回 600 点、240 KiB；按 attempt 隔离分钟聚合。无指标显示 unavailable/null，不能显示成 0。可通过 `prepare --no-metrics` / `telemetry=False` 禁用可选遥测，保护和可靠记账仍工作。helper 退出、侧库损坏/满页只影响观测；输出保护/必要记账失败会阻止成功或保留未知用量。

Web 沿用已发布的 DemiFlow 品牌、Harness 侧栏与紧凑页头，保留项目列表、项目 tabs、固定引用和 Case 路径；桌面展开/折叠及手机抽屉共享原有导航。活动/已准备运行每约 2 秒读取小快照，单请求在途，隐藏暂停，错误退避最高 30 秒并带抖动。趋势/控制事件按需展开，每 5 秒刷新；历史 attempt 不自动切换。大型完整图和报告按原详情入口读取，标量快照截断会显式说明。

权限分为 run_submit（prepare/start/resume）、run_control（cancel/reconcile/准备回执核对）和 attach（创建结果别名）。旧 allow_runs 按原值映射三项，GET 不升级。通过显式 CAS 命令切换：

```sh
demiflow workbench console --registry REGISTRY permissions WORKSPACE_ID --expected REGISTRY_DIGEST --no-submit --control --no-attach
demiflow workbench console --registry REGISTRY serve --control-only
```

服务器开关和项目权限取交集；`--enable-runs` 与 `--control-only` 互斥。停止、小快照和控制事件使用独立的短操作通道，1 active + 最多 2 waiters，等待最多 1 秒；常规操作 1 active，等待最多 5 秒；连接总上限仍为 10。短通道只保证常规 operation slot 被占时可独立处理控制；10 个连接都被占用时仍返回 503，不承诺满连接下停止请求可接入。只读权限不授予停止权，移除登记后该服务不再接受项目操作；同一 OS 用户的本地 CLI 权限仍独立。

精确路由见 [HTTP API](forge-http-api.md)，多端例子见 [接入说明](forge-client-integration.md)。实现与验收证据见 [2026-11-04 交付记录](forge-job-runtime-management-implementation-20261104.md)。

## 当前组合支持范围

运行管理不扩展未适配算子的通用能力表。当前具备状态协议的 actor、受管 materialize/Lance、模拟 HTTP 与受管图片路径经过验收。本次补齐 document/image/wiki collect 与固定 LanceLookup 的具体原生状态适配，并恢复原成功路径。

固定计数 batch_map 的自动物化支持 concurrency=1、flush_interval=None 且无 group/prefix 的组合，整源排空后使用现有原生协议作单次终端提交。尾批、顺序、多重性和扩增结果保留；提交前失败重算未提交工作，prepared 后恢复已有回执。行/累计批字节、输出和 scratch 额度不变；超限拒绝。其他有缓冲/计时/并发分组算子仍需各自状态协议，不宣称全部 P3 完成。open_checkpoint 和 sqlite_journal 已退役。详细边界与实际结果见 [最终组合报告](forge-job-runtime-management-implementation-20261104.md)。


## 图管道入口（P1 候选）

项目内导航为开发、管道、数据、Readme。管道在同一页面组织流程/配置、图、检查问题、图下源码、运行记录、状态、日志、停止与结果；接入和权限使用原位区域。旧 checks/runs/run 深链进入同一工作台，不保留另一套运行页面。无入口、未支持或无提交许可时显示实际缺口，仍可读取已有记录。业务适配由项目维护者负责。

当前源码观察、检查报告和封存运行分别标明依据。配置来自登记 JSON，只有能追踪到允许字段的输入/输出和参数可保存；共享影响按实际参数路径关联节点。保存要求配置写入授权及服务上限，运行权限不会隐式开放文件写入；历史配置只读。保存、检查通过与真实准入是三个独立事实。

“运行”真实串联既有 prepare/start；原请求在浏览器会话内有界保存，响应丢失后核对原请求/运行，不能以新 ID 盲目重提。选择运行记录时，封存图、attempt、指标、节点证据、日志、控制和结果共同切换；历史 attempt 不自动跳向最新。后台仅订阅所选运行，隐藏暂停、错误退避；运行列表只按需刷新当前20条分页，筛选也只作用本页。

节点状态来自实际 action/node/occurrence 观测，未观测、共享融合范围和摘要未载入分别表示；不会把整次成功推为每节点成功。原始运行日志按 run/attempt 阅读。带版本的节点证据分页遇到变化时丢弃旧页重新读取，不混合快照。停止绑定已选 run/attempt，当前入口或参数文件被删除不会使它改为控制另一作业。

结果显示原生提交、固定版本和来自前代 attempt 的保留事实；部分提交不等于整次成功。恢复产生新尝试，页面继续保留旧尝试，由用户选择切换。结果可保留为新输入别名或继续加工；已提交的数据和旧历史均不会因重试、停止或页面切换而被删除。

数据页的目录可展开/收起；固定版本 Case 与阅读保留。Python/Jupyter、SQL、通用富输出和三部分可配置 Case 的正式执行、持久评测不属于 P1 已实现能力；它们仍按 E2 的 P2/P3 门槛推进。当前 P1 是独立候选，正式服务切换另经 Review，不因本文更新自动发布。
