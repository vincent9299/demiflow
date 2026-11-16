# 当前修复与验收状态（2026-11-16，继续实施中）

本页替代初审 `REVIEW.md` 的“当前状态”，不替代原始证据，也不宣称全部意图完成。代码仍只在隔离分支 `engine/gpt-runtime-v3-review-20261115`，工作树 `/yzp/zhaozy/yangzepeng/0905/_demiflow/worktrees/gpt_runtime_v3_review_20261115`。

## 最新集成与验收增量（本节优先于下文旧快照）

- 函数状态修复 `f90b82b` 已集成；总控在 `18b6421` 独立跑真实 Ray 6 项全过，单/四 CPU 的 closure、显式参数与原 ACK 恢复保持一致。2104 个 owner 输入及1791个独立验收输入逐字节核对 0 不符，见 [FUNCTION-RAY-INDEPENDENT-18B6421.md](FUNCTION-RAY-INDEPENDENT-18B6421.md)。
- Local/工作台/历史全组合固定版本 `f90b82b` 为 **109 passed / 1 failed**。唯一原慢消费者用例在原224MiB可驻留预算中被规划拒绝；原因是最终 thread_attempt 活引用程序仍按 owned 函数状态额外计快照。正在修，原测试/预算不动。此前106通过不能覆盖这项新回归。
- 本机四张真实 GPU 的 normal/failure/cancel 已完成 **3 passed / 0 skipped**，12个原allocation全部释放，见 [PHYSICAL-FOUR-GPU-391AE6B.md](PHYSICAL-FOUR-GPU-391AE6B.md)。两机CPU四项此前已闭环。
- 两宿主实际 GPU 绑定候选 `883cc5c` 已集成为 `e725af9`：Ray实际GPU grant先核宿主/UUID/内存，再在原DevicePool预约对应设备；资源层只接收注入的核验接口。CPU worker不见未授予CUDA，未知退出保留原计费，重建provider读取原journal核原actor。owner两宿主六张实际GPU normal/failure/cancel **3 passed / 168.92秒**；总控完整源审已完成，原始结果与journal尚待独立归档核验，不冒称最终验收。GPU远端actor-loss原ACK恢复正补验。
- 原同预算性能工作负载计划已冻结，见 [PERFORMANCE-PLAN-20261116.md](PERFORMANCE-PLAN-20261116.md)。正在完成真实baseline预热和测量harness；正式五轮对比未执行，不以GPU能运行代替性能达标。

下文保留前一阶段精确版本的事实；标为“在途”的旧状态以上述更新为准。最新组合仍未全绿，不宣布两大意图全部完成。

## 两条原始意图与目前判断

**产品 root**：统一、真正可用的工作台，让用户看懂、配置、运行 pipeline，分析、比较、评测数据。Demo、开发对话、Notebook Python/SQL、Case/媒体、历史保护仍属于产品，不能被引擎测试数代替。

**平台 root**：正确、稳定、高性能、通用、架构干净的统一数据计算平台，完整支撑实际业务。Local/Ray Core 是承载方式，同一 Dataset、真实图、有限资源、原生提交与恢复必须一起成立。原文、来源会话及 I01–I14 见 [INTENT.md](../unified-runtime-v3-20261114/INTENT.md)。

Dataset→真实 worker→数据交接→终端→原提交/ACK→恢复主链已经接通，真实硬件复验发现的跨机租约缺陷现已闭环，旧回归暴露的函数状态缺陷仍在修复，不能宣布完成。最新完整矩阵生产候选 **ab1b334** 的 Local/真实 Ray Core 各98 API、168例全通过；工作台实际执行图、取消/恢复、原宽数据5项通过。转换前共驻、恢复回读、提前关闭及旧终端断言迁移均已完成定向验收。随后 **5ff5da9** 仅修私有环境第三方依赖误分类，真实 Ray 转换13项与终端4项通过，未借用旧矩阵冒称新HEAD全量验证。

用户已授权本机4张H100及20机2张H100测试。真实本机 GPU 四项通过（使用一张卡）；两机真实 Dataset 原四例从 **1过/3失败** 修到 **4项全过**；正常写出、远程actor损失后的原ACK恢复、共享源union/repartition与非共享读取域拒绝均成立。旧失败来自共享锁身份错误使用各宿主不同的本地设备号。另旧 code-pin 组合复验暴露 **workers=1持久停车丢失函数 closure/default 状态**，相同8行输出变成8个1，workers=4输出1…8。函数状态仍在原所有者隔离树修复；跨机租约已集成 `3f989a0` 并补 `f9af1eb` 原等待诊断。CPU/GPU跨宿主绑定仍需完成，不直接撤销现行保护。

## 已验证的范围（每项保留自己的版本）

| 交付/验收 | 实际结果 | 证据与版本边界 |
| --- | --- | --- |
| Local 全公开入口及工作台 | 98 API、168案例全通过，107唯一执行；禁用Ray Data、真实浏览器图及取消/恢复通过 | [LOCAL-PRODUCT-FINAL-AB1B334.md](LOCAL-PRODUCT-FINAL-AB1B334.md)；4项547.35秒，生产/测试字节等于 `ab1b334`；保留原ACK/Lance/GC证据 |
| Ray 全公开入口 | 98 API、168原案例全通过；107唯一真实Ray Core执行、Ray Data导入0 | [RAY-API-AB1B334-VERIFICATION.md](RAY-API-AB1B334-VERIFICATION.md)；599.62秒，禁用门21.38秒；私有集群退出、FD原约束保持 |
| 宽数据、提交恢复与终端 | ab1原压力5项过；核心55项过；原stream受影响17项过；Local终端101项过 | [CORE-FINAL-AB1B334.md](CORE-FINAL-AB1B334.md)、[LOCAL-TERMINAL-FINAL-AB1B334.md](LOCAL-TERMINAL-FINAL-AB1B334.md)；原数据/预算/时限不变，集合不相加，窄余量不称完整性能达标 |
| 返回预算与GC争用 | 真实take_all进统一有界终端、iter_batches在decode前累计准入；原对象租约短暂争用不再误标已成功业务 | [返回预算](../unified-runtime-v3-20261114/review-20261115/TERMINAL-COLLECTION-BOUNDS.md)、[GC与supervisor](GC-SUPERVISOR-CONTENTION.md)；真实负例、计费/历史/FD原约束保持；转换前准入已在ab1最终集合验收，后续新问题另列 |
| 最终提交与无重复恢复 | 普通执行、持久交接完成晚于cut的两种真实竞态已修；完整结果恢复不再启动worker/追加提交/重发模型；合法前缀仍保持非final | [LATE-FINAL-CUT-CLOSURE.md](LATE-FINAL-CUT-CLOSURE.md)、[DURABLE-FINAL-STOP-CLOSURE.md](DURABLE-FINAL-STOP-CLOSURE.md)；普通修复95de52e、持久修复1d4281d |
| 工件GC与FD | 历史/unknown/prepared根、原物理租约、崩溃退款代际；原store准入真实句柄，排序输出和多view最小限均接通 | [PUBLISHED-GC-INDEPENDENT-REVIEW.md](PUBLISHED-GC-INDEPENDENT-REVIEW.md)、[ARTIFACT-FD-INDEPENDENT-REVIEW.md](ARTIFACT-FD-INDEPENDENT-REVIEW.md)；GC独立17项、最终FD独立11项，各自精确版本，不与其它集合相加 |
| 历史与工作台恢复 | 实际旧Git生成原生历史、当前只读与拒绝不兼容、原环境同RunRef恢复、1024/1025/1500 Prompt原身份；工作台唯一原生发布和固定版本 | [HISTORY-PROTECTION-AUDIT.md](HISTORY-PROTECTION-AUDIT.md)、[PRODUCT-INTENT-AUDIT.md](PRODUCT-INTENT-AUDIT.md)；当前v3旧中间状态适配未证明，V029仍NotRun |
| 原 33 个新失败 | 33/33 对应原 nodeid 通过 | [CONTRACT-SUITES-CLOSURE.md](CONTRACT-SUITES-CLOSURE.md)，完整 19 组 564 passed；原147、此前待验35也逐项映射 |
| 原 Local/stream 历史组 | 未改原断言的 106 passed | [ORIGINAL106-ACCEPTANCE.md](ORIGINAL106-ACCEPTANCE.md)，测试 tree 等于 `e6bdc2f`；不与564重复加总 |
| 默认普通管道 | 原31行案例约5.4秒，原先约93秒；真实默认4 worker得到独立source与后缀2 stage | [DEFAULT-SUFFIX-PLANNING.md](DEFAULT-SUFFIX-PLANNING.md)；保持任务重建、设备/显式隔离保护、单宿主容量与原预算 |
| 有限输入/恢复 | 原 seal 完整验证后借用；真实100×10 join中途ACK后恢复为1000行；不再重写整份输入载荷 | [FINITE-INPUT-BORROW.md](FINITE-INPUT-BORROW.md)；旧owned/pending兼容，完整成员/路由/摘要/预算反例 |
| 工作台计划与源码 | 同run/attempt原sealed plan、身份桥、prepare-only历史源码；修改当前代码仍看原快照 | [PRODUCT-LIFECYCLE-SOURCE-CLOSURE.md](PRODUCT-LIFECYCLE-SOURCE-CLOSURE.md)；root集成桥/Save组32项通过，浏览器三项在正确临时目录复验通过 |
| 原生 Save 与恢复 | 授权create/固定版本overwrite，原生发布/回执/ACK及发布后中断恢复 | [SAVE-PRODUCT-CLOSURE.md](SAVE-PRODUCT-CLOSURE.md)；root额外重验实际create和发布后恢复，不放开普通append |
| 失败与清理 | 真实启动失败、取消、业务失败叠加关闭失败，原异常可见，无成功ACK，FD4→4 | `/tmp/demiflow-final-control-probe.json`；独立失败链路，不代替全部资源峰值证明 |
| 数据保真与编码 | 原canonical字节保持，scalar/NumPy、负零、schema、block、引用、非canonical拒绝 | [CODEC-ENCODER-REUSE.md](CODEC-ENCODER-REUSE.md)，93项；没有payload缓存或跳过校验 |
| 后续真实Ray转换及终端 | 13项转换、4项终端通过 | [RAY-TERMINAL-5FF5DA9-CLOSURE.md](RAY-TERMINAL-5FF5DA9-CLOSURE.md)；依赖分类上限64模块/8MiB不变，原失败证据保留 |
| 真实物理GPU | 4 passed / 0 skipped，CPU→CUDA→CPU、final重放、取消/异常清理及中间ACK恢复 | [PHYSICAL-GPU-FINAL-5FF5DA9.md](PHYSICAL-GPU-FINAL-5FF5DA9.md)；1785输入独立字节比对全等，真实H100一张、5次原allocation全部RELEASED；不等于四卡或跨机GPU通过 |
| 真实两宿主 | 真实Dataset原四例4 passed；两host实际执行、远端actor损失后原ACK恢复、共享源union路由 | [PHYSICAL-HOSTS-CPU-CLOSURE.md](PHYSICAL-HOSTS-CPU-CLOSURE.md)；120.207秒，原红证据保留；1786输入及26份原journal归档总控独立核对，0不符 |

Local矩阵的旧汇总214是外层107条观察与案例层107条观察重复拼接，**实际为107次执行**。原始报告未篡改；测试记录器现按观察ID合并、保留两份原观察，独立业务调用即使参数相同也不折叠。本轮矩阵FD54→60通过原+8阈值，不能称零增长。

## 当前仍在处理的核心项

1. **函数状态在资源停车后丢失：真实业务语义缺陷。** 同一8行输入与同一算子，workers=1得到8个1，workers=4得到1…8。修复归原编译/函数状态适配器，原journal持有快照；必须隔离函数实例、默认参数和cells，不污染封存程序，不复制显式共享活对象。首轮正例修正不代表验收完成；旧F7中断/代码漂移拒绝、F9真实工作台及生命周期组合仍需通过。
2. **跨机共享工件租约身份：实际Dataset被错误拒绝。** 同一锁文件两机st_dev不同（196/170），inode和fsid相同，共享锁互斥实测有效。修复使用原journal版本化身份及原物理锁，保留本端fstat/lstat、替换/symlink防护；旧v1历史不静默升级。修复已集成，原v1保护及真实双机租约/GC通过；总控独立恢复与历史21项通过，旧竞态夹具按v2修正后4项通过。见 [CROSS-HOST-LEASE-CLOSURE.md](CROSS-HOST-LEASE-CLOSURE.md)。两机Dataset原四例在固定候选原样通过，总控已独立核对1786输入及26份原journal归档，见 [PHYSICAL-HOSTS-CPU-CLOSURE.md](PHYSICAL-HOSTS-CPU-CLOSURE.md)。
3. **跨宿主GPU绑定仍未交付。** 单宿主真实CUDA四例已通过；当前Ray适配仍只允许一台GPU宿主。实际Ray设备与原DevicePool预约必须一一对应，按宿主准入，不能只删除守卫。仅在授权的空闲设备和私有集群测试。
4. **已完成项不重复扩张。** 转换前共驻预算、实际恢复读取窗口、提前关闭、原GC与FD/历史保护均已交付定向证据。后续按上述修复影响补验，不以反复全量测试替代核心修复。
5. **诚实保留其它范围。** 外部PostgreSQL未连库，完整性能矩阵/长内核与真实主机故障仍未证明；工作台image/document Embedding授权、全部Notebook/Case产品验收不因引擎用例通过自动完成。正式门中disk_gc已接，其余16门原NotRun不能计为通过。

## 对齐意图，而不是用测试数量折算百分比

| 原意图 | 当前可依赖的结论 | 仍需保留的边界 |
| --- | --- | --- |
| I01/I02 同一Dataset、Ray Core承载 | ab1 Local/Ray各168例；无Ray Data调用，真实全矩阵禁止导入 | 转换/回读已定向验收；函数状态与跨机租约修复仍在途 |
| I03/I05/I06 交接、共享、多输入、及时背压 | 内存/持久增量/有限封存、实际共享图及join、限流与提前结束有真实专项 | 跨宿主完整路由故障矩阵未验 |
| I04 CPU/GPU独立 | 来源保持独立；纯CPU后缀可在真实容量允许时融合，GPU/显式隔离不被吸收 | 真实单卡张量峰值及释放有证据；多台GPU宿主仍明确拒绝 |
| I07/I10/I13 单一权威及历史 | 原RunJournal/CommitCoordinator/真实ACK与恢复，未知效果不重发，原封存/代码来源保护 | 不把局部历史兼容推定为任意旧版本兼容；完整GC需保历史根 |
| I08 有限资源和性能 | 原预算反例保持，31行退步、GC/FD、driver累计返回已修且真验 | 40k通过但余量小；转换前共驻与恢复回读已修，完整性能/峰值矩阵未交付 |
| I09 真实业务 | Local模型/队列/fetch/Save及恢复有实际链路；外部模型仅本地transport替身 | 外部PostgreSQL未连库，工作台image/document Embedding授权未开放 |
| I11 多机 | 真实两宿主Dataset、原生结果/ACK、共享源与actor恢复通过 | 真实两宿主Dataset及远端actor损失恢复通过；整机失效/网络分区未完成 |
| I12 产品 | 工作台看原执行图/来源、真实运行/结果/恢复，Notebook/Case已有专项回归 | 既有产品专项并非最新同HEAD全产品验收；引擎不代表全部产品root |
| I14 完整交付 | 逐公开入口及旧失败映射，显式保留未验项 | [旧17个正式门映射](FORMAL-GATE-EVIDENCE-MAP.md)中disk_gc现已接通；其余16门仍有NotRun，不返回常量伪造通过 |

## 历史与工作区纪律

主仓仍为 `e40a6dd`，原11处文档dirty保持；未在主仓运行测试、未push/合主仓、未接管在运服务。承重七件相对 `362d8528` 字节未变。

必须单独披露一次失误：诊断浏览器测试曾使用旧fixture默认目录，覆盖历史 `P1/screenshots/logical-source-unchecked.png` 一张图片。原字节未恢复；较早归档不能证明与原图相同，未冒充原件回填。主仓代码和业务数据未受影响。fixture默认已改为独立`/tmp`，后续截图显式隔离。详情 [BROWSER-EVIDENCE-INCIDENT.json](BROWSER-EVIDENCE-INCIDENT.json)。
