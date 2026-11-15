# 当前修复与验收状态（2026-11-16，继续实施中）

本页替代初审 `REVIEW.md` 的“当前状态”，不替代原始证据，也不宣称全部意图完成。代码仍只在隔离分支 `engine/gpt-runtime-v3-review-20261115`，工作树 `/yzp/zhaozy/yangzepeng/0905/_demiflow/worktrees/gpt_runtime_v3_review_20261115`。

## 两条原始意图与目前判断

**产品 root**：统一、真正可用的工作台，让用户看懂、配置、运行 pipeline，分析、比较、评测数据。Demo、开发对话、Notebook Python/SQL、Case/媒体、历史保护仍属于产品，不能被引擎测试数代替。

**平台 root**：正确、稳定、高性能、通用、架构干净的统一数据计算平台，完整支撑实际业务。Local/Ray Core 是承载方式，同一 Dataset、真实图、有限资源、原生提交与恢复必须一起成立。原文、来源会话及 I01–I14 见 [INTENT.md](../unified-runtime-v3-20261114/INTENT.md)。

最初断开的 Dataset→真实 worker→数据交接→终端→原提交/ACK→恢复链已接通。最新固定生产候选 **c04f2d0** 的 Local/真实 Ray Core 各98 API、168例全通过，工作台真实执行图及取消/恢复通过，原宽数据5项通过。take_all/iter_batches累计准入及GC与supervisor锁争用已修；恢复组合54过1失败为旧终端名断言，三处测试随真实take_all迁移后，实际恢复/共享图/收集17项、真实Ray共享图1项与公开接线55项通过。原33失败已闭环，所有旧红证据保持。当前继续修**格式转换前共驻预算**，并核实恢复结果回读窗口；后续补丁不借用c04矩阵冒称同HEAD全量验证。多台GPU宿主及真实多机/GPU边界仍在，不能称100%。

## 已验证的范围（每项保留自己的版本）

| 交付/验收 | 实际结果 | 证据与版本边界 |
| --- | --- | --- |
| Local 全公开入口及工作台 | 98 API、168案例全通过，107唯一执行；另禁用Ray Data、真实浏览器图及取消/恢复通过 | [LOCAL-PRODUCT-FINAL-C04F2D0.md](LOCAL-PRODUCT-FINAL-C04F2D0.md)；生产/测试字节等于 `c04f2d0`，原4节点542.25秒；实际17/20条GC intent下工作台状态正确 |
| Ray 全公开入口 | 98 API、168原案例全通过；107唯一真实Ray Core执行、Ray Data导入0 | [RAY-API-FINAL-C04F2D0.md](RAY-API-FINAL-C04F2D0.md)；候选`c04f2d0`590.43秒；独立禁用门23.86秒，私有集群正常退出，原FD阈值未改 |
| 宽数据、提交恢复与终端 | c04原压力5项过；恢复54过/1旧终端断言失败；测试迁移后真实恢复17项及Ray1项过，U公开接线55项过 | [INTEGRATED-C04F2D0.md](INTEGRATED-C04F2D0.md)；1776输入绑定，生产源码保持；原数据/预算/时限不变。旧失败保留，各集合不相加，窄余量不称完整性能达标 |
| 返回预算与GC争用 | 真实take_all进统一有界终端、iter_batches在decode前累计准入；原对象租约短暂争用不再误标已成功业务 | [返回预算](../unified-runtime-v3-20261114/review-20261115/TERMINAL-COLLECTION-BOUNDS.md)、[GC与supervisor](GC-SUPERVISOR-CONTENTION.md)；真实负例、计费/历史/FD原约束保持；格式转换前准入仍在修 |
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

Local矩阵的旧汇总214是外层107条观察与案例层107条观察重复拼接，**实际为107次执行**。原始报告未篡改；测试记录器现按观察ID合并、保留两份原观察，独立业务调用即使参数相同也不折叠。矩阵FD53→59通过原+8阈值，不能称零增长。

## 当前仍在处理的核心项

1. **Local队列竞态和工作台误标已闭环，保留旧红证据。** idle状态覆盖竞态修复后，c04完整168例与真实SIGKILL恢复通过；GC租约争用修复后，真实工作台成功/取消恢复全部通过。旧8e85e7a的167/168及55ace浏览器失败记录均保持。
2. **返回收集已修，转换前窗口继续修。** 原40k压力与14个真实/顺序收集反例通过。另已实测32KiB批预算在Arrow/Pandas约56KiB共驻后才拒；typed Pandas的1KiB窗口可先造4097字节中间Arrow仍成功。需按原schema、输入/输出/中间副本在构造前准入，不能仅改后验或乘统一常数。恢复replay提前积累的疑点在独立实证中，不提前判定完成。
3. **Ray接线已闭环到最新完整矩阵。** c04真实Ray168例通过、FD/私有集群退出原约束保持。同机Ray不当作多机；新转换/回读补丁只按影响追加真实定向验收并记录确切版本。
4. **工件GC及FD组合已实现并经过最终业务矩阵。** 只在物理退出和原journal证明下回收无任何历史/unknown/prepared根引用的工件；原退款代际不改旧收据。store窗口、sort末轮消费者与多view最小限均接通；正式disk_gc门已实际运行。其他16个原NotRun门不据此冒称完成，静止时回收不等于任意活任务GC或删除保留历史。
5. **设备/多机边界仍在。** 当前Ray部署适配允许一台GPU宿主（可有多张GPU），尚不支持多台GPU宿主的绑定。受控设备/同机Ray证据不是物理GPU推理或两个物理宿主故障恢复。测试资源尚未指定；不自动占用现有训练设备或生产集群。

## 对齐意图，而不是用测试数量折算百分比

| 原意图 | 当前可依赖的结论 | 仍需保留的边界 |
| --- | --- | --- |
| I01/I02 同一Dataset、Ray Core承载 | c04 Local/Ray各168例；无Ray Data调用，真实全矩阵禁止导入 | 新转换/回读补丁另做定向验收并保留版本边界 |
| I03/I05/I06 交接、共享、多输入、及时背压 | 内存/持久增量/有限封存、实际共享图及join、限流与提前结束有真实专项 | 跨宿主完整路由故障矩阵未验 |
| I04 CPU/GPU独立 | 来源保持独立；纯CPU后缀可在真实容量允许时融合，GPU/显式隔离不被吸收 | 多台GPU宿主仍明确拒绝；没有真实CUDA峰值证据 |
| I07/I10/I13 单一权威及历史 | 原RunJournal/CommitCoordinator/真实ACK与恢复，未知效果不重发，原封存/代码来源保护 | 不把局部历史兼容推定为任意旧版本兼容；完整GC需保历史根 |
| I08 有限资源和性能 | 原预算反例保持，31行退步、GC/FD、driver累计返回已修且真验 | 40k通过但余量小；转换前共驻与恢复回读继续核修，完整性能/峰值未交付 |
| I09 真实业务 | Local模型/队列/fetch/Save及恢复有实际链路；外部模型仅本地transport替身 | 外部PostgreSQL未连库，工作台image/document Embedding授权未开放 |
| I11 多机 | Core actor真实运行，声明与可读域协议存在 | 同机不冒充多机；未执行真实宿主失效矩阵 |
| I12 产品 | 工作台看原执行图/来源、真实运行/结果/恢复，Notebook/Case已有专项回归 | 既有产品专项并非最新同HEAD全产品验收；引擎不代表全部产品root |
| I14 完整交付 | 逐公开入口及旧失败映射，显式保留未验项 | [旧17个正式门映射](FORMAL-GATE-EVIDENCE-MAP.md)中disk_gc现已接通；其余16门仍有NotRun，不返回常量伪造通过 |

## 历史与工作区纪律

主仓仍为 `e40a6dd`，原11处文档dirty保持；未在主仓运行测试、未push/合主仓、未接管在运服务。承重七件相对 `362d8528` 字节未变。

必须单独披露一次失误：诊断浏览器测试曾使用旧fixture默认目录，覆盖历史 `P1/screenshots/logical-source-unchecked.png` 一张图片。原字节未恢复；较早归档不能证明与原图相同，未冒充原件回填。主仓代码和业务数据未受影响。fixture默认已改为独立`/tmp`，后续截图显式隔离。详情 [BROWSER-EVIDENCE-INCIDENT.json](BROWSER-EVIDENCE-INCIDENT.json)。
