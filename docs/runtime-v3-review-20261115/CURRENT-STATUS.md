# 当前修复与验收状态（2026-11-16，继续实施中）

本页替代初审 `REVIEW.md` 的“当前状态”，不替代原始证据，也不宣称全部意图完成。代码仍只在隔离分支 `engine/gpt-runtime-v3-review-20261115`，工作树 `/yzp/zhaozy/yangzepeng/0905/_demiflow/worktrees/gpt_runtime_v3_review_20261115`。

## 两条原始意图与目前判断

**产品 root**：统一、真正可用的工作台，让用户看懂、配置、运行 pipeline，分析、比较、评测数据。Demo、开发对话、Notebook Python/SQL、Case/媒体、历史保护仍属于产品，不能被引擎测试数代替。

**平台 root**：正确、稳定、高性能、通用、架构干净的统一数据计算平台，完整支撑实际业务。Local/Ray Core 是承载方式，同一 Dataset、真实图、有限资源、原生提交与恢复必须一起成立。原文、来源会话及 I01–I14 见 [INTENT.md](../unified-runtime-v3-20261114/INTENT.md)。

最初断开的 Dataset→真实 worker→数据交接→终端→原提交/ACK→恢复链已接通，有实际进程、原始 journal 和失败恢复证据。当前核心未闭环的是宽数据性能、Ray 全入口发现的接线缺陷及其复验；此外，已发布工件 GC、多 GPU 宿主和真实多机/GPU 验证仍需明确收口，不能称 100%。

## 已验证的范围（每项保留自己的版本）

| 交付/验收 | 实际结果 | 证据与版本边界 |
| --- | --- | --- |
| Local 全公开入口 | 98 API、168 案例全部通过；80 正向、88 负向，4 个正式退役接口按原约定拒绝 | [API-MATRIX-LOCAL-E304640.md](API-MATRIX-LOCAL-E304640.md)；测试树生产/测试字节等于 `e304640`，2 个原 pytest nodeid，362.18 秒 |
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

1. **宽数据性能尚未过线。** 原测试两侧各40000条8KiB载荷，保留NULL/重复多重性，256MiB预算、120秒不变。`7c6d5b7`两参数仍超时；已完整读80002输入，分别hydrated76528/77408，不能把“接近完成”记为通过。原日志 `/tmp/demiflow-wide-event-codec.log`。正在去掉跨worker重复编码，并核对checkpoint进度不能随图深度重复计数。
2. **Ray首次全矩阵未通过。** 98 API中92满足，168案例161过、7失败。发现真实错误传递与模型声明环境变量接线问题，正在隔离修复；3个缺依赖案例属于原env-ray环境问题，使用独立临时环境补验，未修改共享环境。后续短组结果不能伪装为同HEAD全168绿。
3. **Ray资源所有者必须正确验证。** 纯Ray无DemiFlow对照同样在init/shutdown后保留17个进程级FD；连续Dataset无额外增长。应由私有子进程拥有Ray并确认其退出，不强行关闭C++内部句柄、不抬原FD阈值。
4. **已发布工件GC还缺实现。** 暂存/孤儿scratch恢复已验，不等于immutable published GC。任何回收必须覆盖全部current/history/prepared/unknown根、borrowed seal/manifest、segment、payload/schema引用与并发pin，不能只看最新镜像删除。
5. **设备/多机边界仍在。** 当前部署适配明确只接单GPU宿主；受控设备/同机Ray证据不是物理GPU推理或两个物理宿主故障恢复。测试资源尚未指定；不自动占用现有训练设备或生产集群。

## 对齐意图，而不是用测试数量折算百分比

| 原意图 | 当前可依赖的结论 | 仍需保留的边界 |
| --- | --- | --- |
| I01/I02 同一Dataset、Ray Core承载 | Local全入口实际进入统一驱动；源码无Ray Data调用，真实Ray矩阵禁止导入 | Ray失败项修复后需同语义复验 |
| I03/I05/I06 交接、共享、多输入、及时背压 | 内存/持久增量/有限封存、实际共享图及join、限流与提前结束有真实专项 | 跨宿主完整路由故障矩阵未验 |
| I04 CPU/GPU独立 | 来源保持独立；纯CPU后缀可在真实容量允许时融合，GPU/显式隔离不被吸收 | 多GPU宿主仍明确拒绝；没有真实CUDA峰值证据 |
| I07/I10/I13 单一权威及历史 | 原RunJournal/CommitCoordinator/真实ACK与恢复，未知效果不重发，原封存/代码来源保护 | 不把局部历史兼容推定为任意旧版本兼容；完整GC需保历史根 |
| I08 有限资源和性能 | 原预算反例保持，真实31行退步已修，失败/取消清理有证据 | 40k压力未过、published GC缺失，不能称高性能已完整交付 |
| I09 真实业务 | Local模型/队列/fetch/Save及恢复有实际链路；外部模型仅本地transport替身 | 外部PostgreSQL未连库，工作台image/document Embedding授权未开放 |
| I11 多机 | Core actor真实运行，声明与可读域协议存在 | 同机不冒充多机；未执行真实宿主失效矩阵 |
| I12 产品 | 工作台看原执行图/来源、真实运行/结果/恢复，Notebook/Case已有专项回归 | 既有产品专项并非最新同HEAD全产品验收；引擎不代表全部产品root |
| I14 完整交付 | 逐公开入口及旧失败映射，显式保留未验项 | [17个正式门映射](FORMAL-GATE-EVIDENCE-MAP.md)仍有NotRun，不返回常量伪造通过 |

## 历史与工作区纪律

主仓仍为 `e40a6dd`，原11处文档dirty保持；未在主仓运行测试、未push/合主仓、未接管在运服务。承重七件相对 `362d8528` 字节未变。

必须单独披露一次失误：诊断浏览器测试曾使用旧fixture默认目录，覆盖历史 `P1/screenshots/logical-source-unchecked.png` 一张图片。原字节未恢复；较早归档不能证明与原图相同，未冒充原件回填。主仓代码和业务数据未受影响。fixture默认已改为独立`/tmp`，后续截图显式隔离。详情 [BROWSER-EVIDENCE-INCIDENT.json](BROWSER-EVIDENCE-INCIDENT.json)。
