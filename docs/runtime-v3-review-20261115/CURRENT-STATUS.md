# 当前修复与验收状态（2026-11-16，继续实施中）

本页替代初审 `REVIEW.md` 的“当前状态”，不替代原始证据，也不宣称全部意图完成。代码仍只在隔离分支 `engine/gpt-runtime-v3-review-20261115`，工作树 `/yzp/zhaozy/yangzepeng/0905/_demiflow/worktrees/gpt_runtime_v3_review_20261115`。

## 两条原始意图与目前判断

**产品 root**：统一、真正可用的工作台，让用户看懂、配置、运行 pipeline，分析、比较、评测数据。Demo、开发对话、Notebook Python/SQL、Case/媒体、历史保护仍属于产品，不能被引擎测试数代替。

**平台 root**：正确、稳定、高性能、通用、架构干净的统一数据计算平台，完整支撑实际业务。Local/Ray Core 是承载方式，同一 Dataset、真实图、有限资源、原生提交与恢复必须一起成立。原文、来源会话及 I01–I14 见 [INTENT.md](../unified-runtime-v3-20261114/INTENT.md)。

最初断开的 Dataset→真实 worker→数据交接→终端→原提交/ACK→恢复链已接通，普通及持久交接的晚到完成事件也已取得真正最终提交后才返回完整结果。Ray完整98入口曾在固定候选通过，原33个失败已闭环。已发布工件GC、文件句柄准入及排序下游交接现已合入隔离分支并有独立反例验收。**最新集成压力仍有2项失败：dacf6cb原5项为3 passed / 2 DEADLINE_EXCEEDED，不能用559f74e的旧5绿覆盖。** 正继续修吞吐及journal回执唤醒，最终Local/Ray/工作台组合尚未重跑。多台GPU宿主及真实多机/GPU边界仍在，不能称100%。

## 已验证的范围（每项保留自己的版本）

| 交付/验收 | 实际结果 | 证据与版本边界 |
| --- | --- | --- |
| Local 全公开入口 | 98 API、168 案例全部通过；80 正向、88 负向，4 个正式退役接口按原约定拒绝 | [API-MATRIX-LOCAL-E304640.md](API-MATRIX-LOCAL-E304640.md)；测试树生产/测试字节等于 `e304640`，2 个原 pytest nodeid，362.18 秒 |
| Ray 全公开入口 | 98 API、168原案例全部通过；107唯一成功执行均为Ray Core，Ray Data导入0 | [RAY-API-FINAL-CANDIDATE.md](RAY-API-FINAL-CANDIDATE.md)；精确候选`8e85e7a`，576.03秒；私有Ray所有者进程正常退出，原FD阈值未改 |
| 宽数据与收尾竞态（旧候选） | 559f74e原5项通过；8个通知竞态反例由红转绿。dacf6cb最新集成两项40k超时，尚未修毕 | [IDLE-COMPLETION-WIDE-ACCEPTANCE.md](IDLE-COMPLETION-WIDE-ACCEPTANCE.md)；压力候选`559f74e`，两种分区40k数据保持原256MiB/120s，耗时接近上限 |
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

1. **Local后续矩阵仍保留红证据。** `8e85e7a`代码的第二轮完整矩阵167/168案例通过，唯一`queue-explicit-True`在原600秒时限内未收尾；队列两条结果已settle。已修复worker发送idle时覆盖新控制状态的竞态；原队列两臂及真实SIGKILL恢复专项已过，不能提前将旧整矩阵改写为全绿。
2. **最新宽数据压力未过，继续修复。** 原两侧40000条8KiB、NULL/重复多重性、256MiB/120秒均未改；`dacf6cb`为3 passed / 2 failed（283.53秒），失败发生在真实计算尚未结束。完整原红：`/tmp/demiflow-final-v3-pressure.log/.xml`及`-evidence/verification.json`，1772个输入前后未变。`559f74e`旧5绿仅保留历史。`e0f6b8a`合并有界join读写及无变化分块复用，相关原19项通过；同4k profiling读写次数下降但总耗时34.21→34.51秒，尚无吞吐转绿证据。正修journal完成后未唤醒driver的2ms轮询接缝，不增加预算或改checkpoint阈值。
3. **Ray接线已闭环到完整同HEAD矩阵。** 首轮7失败及修法见[RAY-API-BOUNDARY-CLOSURE.md](RAY-API-BOUNDARY-CLOSURE.md)，`8e85e7a`完整168例通过。私有进程拥有Ray并真实退出，168案例结束FD均44、父进程原门12→12；未强关第三方FD、未抬阈值。后续idle/GC修改需按影响再验。
4. **工件GC及FD组合已实现，待最终业务矩阵。** 只在物理退出和原journal证明下回收无任何历史/unknown/prepared根引用的已发布工件；原退款代际不改旧收据。先前发现FD超配，已补store窗口、sort末轮真实消费者窗口与多view最小限，并经独立反例确认。`0860565`接通正式disk_gc门，真实Dataset、存活reader、pin、scratch超限和重复退款均实际运行通过；其他16个原NotRun门不据此冒称完成。静止时回收不等于任意活任务GC或删除保留历史。
5. **设备/多机边界仍在。** 当前Ray部署适配允许一台GPU宿主（可有多张GPU），尚不支持多台GPU宿主的绑定。受控设备/同机Ray证据不是物理GPU推理或两个物理宿主故障恢复。测试资源尚未指定；不自动占用现有训练设备或生产集群。

## 对齐意图，而不是用测试数量折算百分比

| 原意图 | 当前可依赖的结论 | 仍需保留的边界 |
| --- | --- | --- |
| I01/I02 同一Dataset、Ray Core承载 | Local/Ray均有真实完整入口矩阵证据；源码无Ray Data调用，Ray全矩阵禁止导入 | 后续Local队列时序回归及新修复须保留独立版本与最终收口 |
| I03/I05/I06 交接、共享、多输入、及时背压 | 内存/持久增量/有限封存、实际共享图及join、限流与提前结束有真实专项 | 跨宿主完整路由故障矩阵未验 |
| I04 CPU/GPU独立 | 来源保持独立；纯CPU后缀可在真实容量允许时融合，GPU/显式隔离不被吸收 | 多台GPU宿主仍明确拒绝；没有真实CUDA峰值证据 |
| I07/I10/I13 单一权威及历史 | 原RunJournal/CommitCoordinator/真实ACK与恢复，未知效果不重发，原封存/代码来源保护 | 不把局部历史兼容推定为任意旧版本兼容；完整GC需保历史根 |
| I08 有限资源和性能 | 原预算反例保持，31行退步已修，工件GC/FD及失败清理有真实证据 | 最新两项40k超时待修；小样本profile不是完整性能交付 |
| I09 真实业务 | Local模型/队列/fetch/Save及恢复有实际链路；外部模型仅本地transport替身 | 外部PostgreSQL未连库，工作台image/document Embedding授权未开放 |
| I11 多机 | Core actor真实运行，声明与可读域协议存在 | 同机不冒充多机；未执行真实宿主失效矩阵 |
| I12 产品 | 工作台看原执行图/来源、真实运行/结果/恢复，Notebook/Case已有专项回归 | 既有产品专项并非最新同HEAD全产品验收；引擎不代表全部产品root |
| I14 完整交付 | 逐公开入口及旧失败映射，显式保留未验项 | [旧17个正式门映射](FORMAL-GATE-EVIDENCE-MAP.md)中disk_gc现已接通；其余16门仍有NotRun，不返回常量伪造通过 |

## 历史与工作区纪律

主仓仍为 `e40a6dd`，原11处文档dirty保持；未在主仓运行测试、未push/合主仓、未接管在运服务。承重七件相对 `362d8528` 字节未变。

必须单独披露一次失误：诊断浏览器测试曾使用旧fixture默认目录，覆盖历史 `P1/screenshots/logical-source-unchecked.png` 一张图片。原字节未恢复；较早归档不能证明与原图相同，未冒充原件回填。主仓代码和业务数据未受影响。fixture默认已改为独立`/tmp`，后续截图显式隔离。详情 [BROWSER-EVIDENCE-INCIDENT.json](BROWSER-EVIDENCE-INCIDENT.json)。
