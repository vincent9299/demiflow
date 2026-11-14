# 第三轮反馈处理与架构图复审

2026-11-14。输入：Qwen第三轮/粒度复审、架构图v6、源码`bbd93ff`、设计`0677dc2`。完整图审见 [ARCHITECTURE-V6-REVIEW.md](ARCHITECTURE-V6-REVIEW.md)。

## 1. Q008装配权限：接受，且Local/Ray两处都必须改

两处真实调用在 `graph.py:546/979`。L017/L018都改为implement，范围限定封存前credit/prefetch联合求解与恢复验证。只改一张卡不能迁移另一条路线；这是对Qwen“只改实际调用的那一张”建议的必要修正。

`task_graph_rules.py`新增两条校验：新增非V函数有implement调用方；verify_preserve调用方不得声称接入新函数。对修订前188项计划实跑，恰好报Q008无可修改调用方、L017→Q008、L018→Q008三项。新增6项工具测试包含“只修Local仍红”的反例。检查不证明真实运行经过该函数，V018仍要求Local/Ray分别验证。

## 2. seal多态消费者：接受，补10张卡，不为覆盖重写19个方法

| 卡 | 类/方法 | 动作 |
|---|---|---|
| R023 | StreamMaterialization.seal | implement：当前cut独占转交与共同发布契约 |
| R024 | StreamMaterialization._commit | verify_preserve：唯一driver提交、回执后推进 |
| R025 | StreamMaterialization.finish | verify_preserve：未确认源/输出不完成 |
| R026 | WorkerStreamProgress.seal | implement：对接异步L034，保留worker无提交权 |
| R027 | GraphWorkerProgress.seal | implement：final回写、router barrier、状态引用与异步L035 |
| R028 | GraphWorkerProgress.request_cut | verify_preserve：driver cut命名及frontier |
| R029 | GraphWorkerProgress.note_driver_cut | verify_preserve：hint不冒充EOF |
| R030 | GraphWorkerProgress.align_cut | verify_preserve：InputGate对齐与reader等待 |
| R031 | GraphWorkerProgress._channel_cuts | verify_preserve：通道坐标与源坐标分开 |
| R032 | GraphWorkerProgress.note_contribution_acked | verify_preserve：正确回执才确认/解锁 |

共同字段、来源、同步副作用、thunk/await、final回写权、预算与失败行为已进入CONTRACTS C13及interfaces.pyi。graph receipt与chain CommitReceipt保持不同wire结构。L035增加checkpoint匹配要求；V012增加真实三类传输、EOF/hint/压力cut、错/迟到ACK和queue结算反例。

## 3. cut_id不是必须补一个新生产者：撤回这个多余参数

本地`_CheckpointBarrier`已经是FIFO传递的对象；它的arrivals按该对象、stage和worker_slot隔离。graph checkpoint_id属于跨worker提交协议，与本地对象不是一回事。旧R019新增`cut_id`没有合理的统一生产者，故直接删除这个草案参数，不让graph私有字段污染所有本地/chain路径。

新签名为 `arrive(self,stage,batch=None,*,worker_slot=None)`；终局对象、重复slot、错stage、相邻barrier不串用均入验收。graph的request_cut/align_cut仍显式列卡，保持原协议。

## 4. 粒度复审另外两点核验

- `L009.calls`的两个文件限定`:execute`确实解析到L010/L011；build_plan已按`file:symbol`建索引。“同名execute无消费者”的推断不适用于当前plan。`bridgeEntryFor`是既有外部helper，不是漏派的新函数。
- `_make_worker` AST中为1个外层函数+13个嵌套函数；若报告计14，应明确含外层。它是共享闭包状态的复合单元，不拆给14个agent。R015已明确唯一所有者，授权整个函数的scope guard不证明内部各分支正确。

## 5. 数量、状态与边界

本次新增10项，当前 **198项 / 54文件 / 29门**，其中196函数/方法与2声明单元；原21个V验收入口保留。只调整设计与设计验证工具；引擎、测试源码、主仓、架构图、全局架构文档、完整记忆文档以及Qwen三份未跟踪报告均未修改。

仍不能整包分发：buffer持久payload/参与者协议和Arrow真实grant/临时写出没有闭合。卡数增加、结构检查通过都不代表能力验收通过。`dispatch_ready=false`、业务/Ray/浏览器验收`not_run`保持。

## 6. 本轮验证口径

验证清单与摘要写在 `r3-evidence/verification.json`，源码/图只读核查在 `r3-evidence/source-audit.json`，旧计划接线报错在 `r3-evidence/wiring-before.json`。结果按真实执行回填，不将Qwen旧探针算作本轮重跑。

下一轮应集中完成两个实质阻塞，再验证真实组合；不继续为了目录/方法覆盖而无限加卡。图修订优先：提交权威与seal、默认入口、queue结算、既有资源机制、恢复时序及当前设计版本。
