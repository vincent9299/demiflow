# Forge 原项目识别优化交付核验（2026-11-04）

核验日期与时间采用 Asia/Singapore。本执行包只审查既有优化、复核原控制台交付并新增本报告；整体架构、优先级与最终 Review 由主会话负责。

本执行包核验完成：**成功解析的模块在一次 `discover` 内复用声明摘要**，逐入口判断保留；原控制台已经加载该实现，鉴权 HTTP 与页面检查结果相符，控制信息保持原值。失败解析仍可能重复读取，属于本轮未覆盖的性能路径，已报告并由主会话明确保留为后续范围；没有因此误报检查通过或扩大执行权限。

## 审查发现与代码依据

核验实现为 `demiflow/forge/project_discovery.py`，SHA256：

`da632a2e793fb890e44f81013a0708d9a870521126ebc55be25e44ef71bf90e6`

对照修改前副本，变更集中在调用内 `entry_sources`、`entry_source` 及其调用点。未改读取预算、项目归属选择、路径准入或原生运行准入。

| 审查项 | 依据与结论 |
| --- | --- |
| 成功模块复用 | `project_discovery.py:52,84–114`：按规范化的项目内源码路径保存摘要；成功路径只读取、解析一次。不保存原始源码、AST 或业务对象。 |
| 逐入口语义 | `project_discovery.py:116–150`：摘要包含顶层同步/异步函数的重复计数与首行位置；缺失、重复符号仍无效。旧协议的 `config` 继续只统计同步顶层函数，保持修改前行为。 |
| 文件变化 | 首次读取、解析前后，以及复用前，比较 `st_dev/st_ino/st_size/st_mtime_ns/st_ctime_ns`；底层 `read_bounded` 仍校验普通文件、读取长度及读取期间变化。变化产生 `entry_unreadable` 与 `changed during discovery` 诊断，不能将整个结果解释为完整识别成功。 |
| 调用间新鲜度 | 摘要字典位于 `discover` 局部作用域；下一次调用重新读取文件。没有长期缓存或业务代码导入。 |
| 路径边界 | 继续走 `local`、`relative_path`、`no_symlinks` 和原有受限读取；复用不新增项目外或隐藏控制路径授权。 |

发现 F-01：**失败解析没有复用结果**。`entry_sources` 仅在成功解析、节点检查及末次文件状态核对后填充。语法错误等异常返回后，后续入口仍会重新读取同一路径。本执行包的隔离复现使用 786,446 字节语法错误模块与 8 个入口，成功读取和尝试解析均为 5 次，成功读取合计 3,932,230 字节；其余入口遇到剩余字节预算不足，8 个入口全部明确标为 `invalid`。没有执行候选代码。

主会话已明确将 F-01 归为本轮未覆盖的性能路径，不作为误报通过或执行越权问题；当前优化说明也已限定成功解析模块。本执行包未修改实现或规范。后续若扩大优化范围，可评估在相同文件状态下复用有限的失败诊断，同时保留状态变化检查、单次读取预算口径与调用间重读；不应通过扩大预算掩盖失败路径开销。具体优先级由主会话决定。

## 资源边界与证据口径

- 读取限制保持 128 个文件、合计 4 MiB、单源码/声明 1 MiB，README/配置各 64 KiB；不同源码继续共用总读取预算。50,000 AST 节点检查保持原值。
- 新增驻留量随成功模块的顶层函数名称、计数和行号总量增长，并非常量空间；受成功读取总字节、文件数与每文件节点门槛共同约束。每次解析期间还同时存在当前原始字节、AST、当前及此前模块摘要。返回摘要后不保留原始字节或 AST。
- AST 节点检查发生在 `ast.parse` 之后，不能把它说成解析前分配上限。源码字节数、AST 节点数和摘要规模都不等于进程 RSS 硬上限；本执行包未新增或声称这样的保证。
- 新鲜度证据限于上述文件状态元组与实际读取校验；这不是锁定的文件系统快照，也不宣称能发现所有状态元组不变的改写。
- 对照样例的“成功读取字节数”从 3,933,690 降到 786,738，减少 80%，与 5 次降为 1 次一致。这不是物理磁盘 I/O、生产吞吐或总服务资源上限的测量。约 27 ms/6 ms 为依赖导入后 5 次调用的中位数；约 1.63 MB 为 Python 分配峰值，不包括原生分配及依赖导入。

## 测试与源码一致性

已阅读相关实现、5 项新增读取测试、原项目识别/浏览器测试及既有证据，没有重复全量回归。

| 证据 | 实际范围与结果 |
| --- | --- |
| `read-tests-before.log` | 修改前 1 项失败、4 项通过；共享大模块用例复现预算耗尽。 |
| `final-regression.log` | 优化隔离版本 47 项识别、检查、管理、HTTP、移除及浏览器回归通过；发生在最终配置/URI 边界修订之前。 |
| `latest-regression.log` | 最终边界修订合并后，29 项识别及真实浏览器复测通过。 |
| `discovery-read-comparison.json` 与对照脚本 | 786,738 字节、8 入口样例：识别从 5/8 到 8/8，成功源码读取从 5 次到 1 次；记录的修改后实现哈希与当前一致。 |
| 本执行包逐文件核对 | 最终测试副本与当前 `demiflow/`、`tests/` 的 1,202 个非缓存文件双向清单一致，合计 31,716,089 字节，差异为零；其中 Python 文件 942 个。 |
| 本执行包针对性补验 | 仅新增疑点的临时探针：F-01 失败路径；解析期间同长度改写并恢复 mtime、确保 ctime 发生变化；复用前同内容/同 mtime 的 inode 替换。后两项均得到变化诊断，未复用过期摘要。 |
| Demiurge 既有证据 | 阅读 53 项回归 `OK` 与跨层声明检查结果，未重跑，未修改 Demiurge 或业务文件。 |

原优化证据目录：[forge-optimization-20261104-ihy_7n_a](/tmp/forge-optimization-20261104-ihy_7n_a)。本执行包独立证据：[forge-discovery-delivery-20261104-016hfu8a](/tmp/forge-discovery-delivery-20261104-016hfu8a)，其中 `baseline-full-comparison.json`、`platform-files-before.json`、`edge-probes.json` 和 `probe_discovery_edges.py` 保存上述逐文件核对与补验。

## 原控制台服务交付

按要求仅通过 `wait_threads` 跟踪 `Review Forge handoff document (2)`（`01a1a550-95cc-7780-ab0c-e6f8a7cdec4d`），未向其发送消息或打断它。该聊天最终状态为 completed/idle，无报错、无待处理服务操作；完成后才进行本执行包的鉴权交付复核。

从实际监听 socket 查找 PID，并核对 `/proc/PID/cmdline`、`cwd`、启动 tick：最初为 PID 20434，15:45:16 启动，早于源码最后发布的 15:51:44。该审查聊天完成更新后，实际监听者为 **PID 64057，16:00:27 启动**，晚于优化源码写入；源码哈希仍为上文值。本执行包没有再次重启服务，没有使用旧 PID 操作进程。

两次核对的完整 argv 一致：使用 `/yzp/zhaozy/yangzepeng/0905/env/bin/python -B -m demiflow.cli forge console`，原 registry 路径、`serve --port 59865 --enable-runs`、原 token-file 路径全部保留，cwd 为 `/yzp/zhaozy/yangzepeng/0905/demiflow`。监听仍为 `127.0.0.1:59865`。启动信息分别保存在 `service-initial.json`、`service-after-peer.json`。

控制台登记与令牌从最初读取、另一聊天服务更新后，到本执行包 HTTP 复核后均一致，权限保持 0600：

| 文件 | 内容 SHA256 |
| --- | --- |
| `.forge/console.json` | `4bbb192fad38c07a4ee47905a38826002383a443af0522a4a056ee8a5b221a75` |
| `.forge/console.token` | `1dfc65bcc1322c9d2d1f656cf6923e545ca03d164baa541a6daf74da943f8d8d` |

仅保存哈希与必要文件属性，没有输出令牌原文，也没有在 URL 中传递令牌。HTTP 使用既有令牌作为鉴权头。

## HTTP 与页面结果

当前服务独立复核共 7 次 GET，0 次修改请求，0 次数据预览请求；单响应读取上限为 9 MiB。详细结果见 [http-verification.json](/tmp/forge-discovery-delivery-20261104-016hfu8a/http-verification.json)。

| 请求 | 结果 |
| --- | --- |
| 无鉴权 `capabilities` | 403 / `unauthorized`。 |
| 既有鉴权 `capabilities` | 200；HTTP 契约正确，Web 开启，原全局 `runs=true` 保留。 |
| 使用 55971 的 loopback Host/Origin 请求 `capabilities` | 200，与原端口结果相同；验证了服务对该转发 authority 的处理。没有更改或新建转发。 |
| `projects` | 200；仍只有原已登记项目 `shared/concepts/identity`，根目录与 registry 一致，项目 `allow_runs=false`。 |
| 原项目 `project-discovery` | 200；1 个旧协议入口 `recognized`，2 份原配置、1 个算子目录，9 条数据引用（6 可用、2 项目外、1 未解析文档引用），1 条未验证历史产物引用。整体 `partial`，覆盖 `complete=false`。 |
| 原项目 `context` | 200 / `loaded`；`project` 与独立 HTTP discovery 及本地同一公共发现入口结果逐项相同。原生 Flow 数量为 0，`runs_enabled=false`。 |
| 原项目 `project-status` | 200；整体与静态均为 `incomplete`，`changes=[]`；实际尝试源码前置检查 1 个入口，原生绑定/检查组数为 0，业务与未声明代码未认证。 |

`workspace_platform_status` 仍为 **stale**：历史执行快照没有因当前源码变化被重绑。当前派生检查依据与历史执行快照的状态独立；派生检查 `changes=[]` 不会使历史快照变为 current，也不表示完整 Flow、行为或业务审查通过。本执行包没有重新检查、重拍基线、重写旧证据或调用任何运行接口。

已查看另一审查聊天在 **16:01:18、服务更新后**保存的[真实检查页截图](/tmp/forge-project-recognition-20261104-qvefqyn8/browser/real-project-check.png)：页面明确显示“整体：尚未完成；静态检查：尚未完成”、`argparse` 依赖边界、旧协议未覆盖及项目外引用限制，与当前 HTTP 结果一致。早先的总览截图只作为呈现对照，不冒充最终服务的独立复测。

该聊天的 `acceptance.json` 记录真实固定版本表 v44392、1,026,275 行的受限分页/Case 验收及无脚本错误。本执行包没有重复读取这张表或将其加载进内存。截图与验收文件的哈希及来源保存在 `peer-artifacts.json`。本执行包没有单独启动浏览器或触发页面检查按钮。

## 保护核对与未验证项

本次独立 HTTP 复核前后，原 registry、token、业务项目的 `workspace.json`、原平台快照文件和当前派生 `project-check.json` 均逐文件哈希相同。未新建、登记或移除任何实际业务项目，未调用 pipeline/模型，未编辑业务源码、数据、Notebook、`training/`、Demiurge、共享环境、巡检进程或其他服务。本执行包唯一仓库新增文件为本报告，其余新增证据均在自有 `/tmp` 目录。

审查期间另有授权聊天更新了验收说明、优化说明和 roadmap；这些并行文档修改全部保留。本执行包未编辑这些文件，平台实现和测试文件保持一致；最终复核记录见本证据目录的 `final-verification.json`。

尚未独立验证用户本机 55971 的 SSH 转发端到端连接；服务端 authority 核对不等于客户端隧道实测。没有生产吞吐或 RSS 硬上限测量，没有重跑全量开发工具测试，没有补做业务行为、模型质量、完整 Flow 语义或整体认证。F-01 的性能改进仍待主会话决定后续范围；本报告不增加任何执行授权。
