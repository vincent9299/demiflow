# 对 Qwen 独立 Review 的裁决与修订记录

日期：2026-11-14；源码核对基线：`bbd93ffcc136c361093c5a4b6d79d0352c4c2557`。本文件为设计修订记录，不是运行时验收报告。

## 1. 结论与责任

**接受“修订后再分发”的总判断。原包不能保证各函数实现后能串成完整系统；此前“可以独立编码”的结论下得过早。** 函数名、字段类型、源码摘要和调用方列表是必要条件，但不能替代状态持有者、生命周期、预算事务、所有真实调用入口以及可执行验收的闭合。

两大意图保持不变：产品是统一工作台，让用户看懂、配置、运行 pipeline，分析、比较、评测数据；平台是正确、稳定、资源有界、高性能、通用且职责清楚的统一计算平台。产品 R.1—R.5/15 叶和历史完整架构继续独立保存；本次修订不删减未完成的能力来换取“100%”。

本次已做：保留原报告与摘要；复算 117 个历史失败的覆盖；补实际验收选择器、严格结果收集与冻结守卫；给旧卡加不可直接派发标记；逐条登记 Q01—Q57 的裁决；补七组关键重设计边界。**尚未做：将七组重设计全部写回 144 张任务卡和类型文件、真实组合实现及验收。当前 `dispatch_ready=false`。** 这些明确未完成项不能由设计检查绿色覆盖。

没有发消息或停止任何在途会话，没有改主仓/候选实现，没有使用主仓跑测试，没有推送或合并。旧卡的暂停分发只作用于这份设计包，不表示其他会话已经暂停。

## 2. 独立核验与需要收窄的结论

| 项目 | 本次独立证据 | 裁决 |
|---|---|---|
| Q06 新验收没有命令 | 原 `build_gates.py` 只把 `required_existing_files` 放入命令，21 个新函数只在元数据 | 成立；现在 21 个函数均有精确 node selector，缺文件即不通过 |
| Q53 历史盲区 | 从原审计 §1.2 解析 118 个 ID，扣 T 为 117；旧门缺 73 个，分布于 6 个文件 | 成立；原 ID、族、出处及源摘要已保存；新增 REGRESSION117 |
| Q27 action 生命周期 | 在真实公开 Dataset 调用到 LocalDatasetExecutor 的入口采样；take=false、iter_rows=false、count=true | take/iter 缺口成立；**不能说 count 自身没建 RunAction**。Qwen 在 Dataset 函数体执行前采样，无法证明函数体内部的状态 |
| Q28 count 嵌套 | `Dataset.count → checked.action → run.action` 已存在；delivery 内还会 `run.action` | 成立；修下层复用，不能再给 count 套第二层 |
| Q56 原生快照 | 真实 `StreamStageProgress.align` + 真实 BlockSink，阶段注册表放两条不同 worker 的 ScopedRow；只给 align 一个局部 batch，仍存下两条 occurrence | **“原生模型因 token=1 只保存首 worker”未被报告探针证明。** 实现读取整个 `stage.native_pending`。本探针不证明真实并发/崩溃恢复已经正确 |
| Q02/Q03/Q47 | 源码核对 `_Stage.__slots__`、grouped_worker、bind_stages 原保护；Qwen 的运行探针作为外部输入保留 | 接受状态持有者/聚合点缺失及不可先删保护；未把 Qwen 的实测冒称本会话重跑 |
| Q01 | 源码核对 CALL_RESERVATION、RunPromptJournal 原子预留和 EmbeddingActor._runtime | 接受预算交接缺失；拒绝免预留绕过守卫这条备选 |
| Q50 统一路径 | 结果正确不证明封存计划实际控制执行 | 接受验收缺口；**不把是否调用 `unified_chain_eligible` 当通用判据**，它是特定 worker 路线的准入缝，不是所有 driver backend 的唯一入口 |

独立探针：[action 边界](scope.json)、[原生阶段注册表](native-pending.json)；脚本分别为 [probe_scope.py](probe_scope.py)、[probe_native_pending.py](probe_native_pending.py)。第二个探针只隔离快照分支：语义状态适配为空，真实 align/BlockSink 未替换；随后主动终止隔离 action，不发布业务 commit。第一版探针因未提交 action 在退出时报错，已改成显式中止并重跑成功；不把这个探针清理错误算产品缺陷。

Q04 按作者撤回，不再计为一个有效 P0；Q57 单独保留。Q42 的“序号不同所以必有两个权威”也要收窄：本地 stage 输出序、源 frontier、物理 channel sequence 本来就是不同坐标。问题是缺少明确绑定和唯一提交路径，不能要求它们数值相等。

## 3. 七组关键方案的裁决

详细生产者/消费者、补任务位置、反例见 [关键修订规范](CRITICAL-REVISION.md)。这里给出不可随意改变的决定：

1. **公开 action / 下层 delivery / terminal 路由**：公开入口拥有一次 action；内部复用。生成器的消费、关闭、取消必须在 action 生命周期内。所有会读取/执行数据的 terminal 都要进入明确路线；纯元数据读取按证据声明为不执行，不能假报执行。验收检查实际封存布局与启动 backend/预算的对应关系。
2. **Embedding 请求预算**：复用 execution journal 的原子 reservation，不造 coordinator 或免费请求旁路。fresh 请求设置并清理原 ContextVar，实际预算扣账与 journal reserve 同事务；已保存响应不再次计 fresh 请求；unknown 不自动重发。
3. **buffer 状态与 barrier 聚合**：状态放在明确的每 stage/每 worker 持有者；不是临时局部变量的名字清单。普通 concurrent/grouped batch 要收齐参与者，native row-local 使用完整注册表的单次阶段快照，两种协议不能混用。buffer 层不发 commit、不拥有 ACK。
4. **工作台 occurrence**：暂不拆逻辑 DAG 顶点，不改冻结边端点或 dag_id。一个逻辑节点内部选择 occurrence；源码/参数/观测用二元选择键，物理摘要继续按 logical ID 取。来源可信等级和运行状态分别传递。
5. **执行程序及源侧代码绑定**：同进程 native driver 使用已有 code-pin-only 权威；发往 worker 的程序用已有 in-run pickle + code pin。源侧 callable 及其业务依赖纳入同一收集/归档/验证规则；不可仅 `describe(source)` 后声称代码保护闭合。
6. **现有 exchange 与资源下限**：InputGate/ChannelSequence/route_epoch 继续管通道边界；StageBufferAdapter 只管本地语义缓冲。预算上限小于协议所需 credit 下限时，重新编译一致的更小 credit 或明确拒绝，不能悄悄收紧到死锁，也不能用 floor 扩大已批准预算。
7. **F3 保护与逐 kind 放行**：保留已有 fail-closed；每一种并发、定时、分组、prepend buffer 只有在真实跨 cut 与故障恢复证据通过后才准入。不能先移除守卫再补测试，也不能改业务 concurrency=1 伪装恢复能力。

## 4. Q01—Q57 逐条处理账

“采纳”表示设计方向接受，不表示对应功能已实现或故障已关闭；“收窄”表示仅接受有证据部分。未列为“本轮已修”的生产设计项仍待新版卡落实。

| ID | 裁决 | 明确处理/状态 |
|---|---|---|
| Q01 | 采纳，拒绝免预算备选 | M 新增 fresh-budget 上下文与消费点，沿用 journal 原子预留；待卡落实 |
| Q02 | 采纳，收窄证据 | 旧 _Stage 无新字段本身不是设计无法实现的证明；真正缺口是字段持有者/读写者未闭合；交 R 完整装配 |
| Q03 | 采纳 | R 补 grouped barrier 分支与全参与者聚合；不能只改 adapter |
| Q04 | 撤回 | 保留撤回原因，关联 Q57，不重复计 P0 |
| Q05 | 采纳 | 区分普通执行与 durable 恢复；不能全局拒绝旧可用带锁/concurrency>1 actor |
| Q06 | 本轮已修命令盲区 | 21 个 V 函数加入真实选择器；未实现仍会失败/阻塞 |
| Q07 | 采纳 | logical 顶点不拆；occurrence 用节点内选择器；不动 dag.py |
| Q08 | 采纳 | ActionDecision 与 ExecutionPlacement recovery 同一个四值域；delivery_only 不冒充 durable |
| Q09 | 采纳 | 按交付方式分派已有两种 ProgramRef 生产者；同 Q46 |
| Q10 | 部分本轮已修 | L034/L035 异常栏已加 TimeoutError/取消；ACK 唯一键与清理仍待卡落实 |
| Q11 | 采纳 | 并发验证放拥有真实执行选项的准入点；状态分类本身不偷读全局模式 |
| Q12 | 采纳 | 不接受 E 候选删除 initial_state / ContextVar 值的断言；差异化身份必须保留 |
| Q13 | 采纳 | E 当前候选按函数重对账，不能把共同设计基线当候选 HEAD；待集成台账 |
| Q14 | 采纳 | W 增运行观测输入/绘制职责，来源标签不当状态 |
| Q15 | 采纳 | W 补 loadSource、chooseNode、请求参数与后端解析，二元选择键贯通 |
| Q16 | 采纳 | physical/stages 仍按 logical_node_id 查，选择 occurrence 不改键域 |
| Q17 | 本轮已修结果守卫 | skip/xfail/空收集均不接受；浏览器/Ray 环境尚未完成验收 |
| Q18 | 部分本轮已修 | 117-ID 门、冻结守卫已加入；原历史失败目前没有重跑清零 |
| Q19 | 采纳 | F1 仍 sealed_plan_facts.v1；v1/v2 分支属于 execution_bindings，不虚构 F1 v2 |
| Q20 | 采纳 | 产品开发历史、目录统计、媒体及评测冲突缺证据，保留原意图未完成；不凭门名算覆盖 |
| Q21 | 本轮已修类型描述 | request_gate=原 RequestGate/AdaptiveRequestGate/None，不等同 request_policy |
| Q22 | 采纳 | 未来共享 fixture/import/业务场景装配需单列任务；不能让 21 个单函数各造 fixture |
| Q23 | 采纳 | 非有限身份数值与其他后延项逐项归属；撤销“全部已分配”的笼统声称 |
| Q24 | 采纳 | F002/S011/Q006 按当前候选函数做三方比较；不因同文件在途就推翻正确窄补丁 |
| Q25 | 采纳 | 状态/OperatorSpec 使用既有 ProtocolError；计划载体才用 PlanContractError |
| Q26 | 本轮已修输入描述 | 历史只读验证载体/摘要/绑定；不要求当前环境 verify_seal，也不导入业务代码 |
| Q27 | 收窄 | take/iter/take_all 的生命周期缺口保留；count 在 executor 入口已有 action，见独立探针 |
| Q28 | 采纳 | iter_stream_rows 复用外层 action；不重建 count action |
| Q29 | 采纳 | _apply_plan 及所有执行型 terminal 必须有绑定消费职责；纯元数据操作单独标记 |
| Q30 | 采纳 | available_backends 由真实 executor 能力产生；不能默认空元组导致快路静默消失 |
| Q31 | 采纳并保留作者精确化 | operations 侧已有依赖保护不重写；补 source-side callable 依赖和归档集合 |
| Q32 | 采纳 | ACK 等待取既有 checkpoint/运行 deadline 的有效最小值，禁止另造 120s 默认；迟到/错 cut 回执不推进 |
| Q33 | 采纳 | ResourceProfile 真实字段 cpu_millis/memory_bytes/scratch_bytes；threads/rows 来自不同有据约束；内存驻留计入唯一预算 |
| Q34 | 采纳 | Arrow 调度/临时文件必须在实际分配前执行字节/磁盘准入；仅传 execution 参数不算实现 |
| Q35 | 部分本轮已修 | CODEC 已加入 s6 quota 回归；signed-zero、NaN 位模式与跨进程编码仍待实现/实测 |
| Q36 | 收窄 | id(source) 的 typed_cache 本身不证明重复执行被折叠；仍需真实双 input_path 执行/映射反例 |
| Q37 | 采纳 | 需按函数补实际异常和 IPC/磁盘/取消副作用；不能仅用包级模板生成错误清单 |
| Q38 | 采纳 | 复用 F002 候选方向，但先去秘密参数、核身份；不重写已正确事实 |
| Q39 | 采纳范围不实，收窄路径判据 | 未生产的 route 不标可用；不能将所有 backend 强塞到一个 materialize eligibility 函数 |
| Q40 | 采纳 | 独立 worker/Local-Ray/CPU-GPU/完整业务矩阵仍是总目标缺口；此轮接口修订不能结项整体平台 |
| Q41 | 采纳 | 按 commit 原参与者及聚合状态字节上限预检，不给每 stage 各发一份全局预算 |
| Q42 | 收窄并采纳交接缺口 | 不同 ordinal 坐标合法；必须有 cut/通道/状态的显式绑定和同一提交权威 |
| Q43 | 采纳 | credit floor 与资源 ceiling 联合可行性检查，不能分别各自 min/max |
| Q44 | 采纳缺口，收窄内存推论 | capture_source 全源等待成立；单凭它遍历全源不能证明全源驻内存。增量释放和首条延迟仍未完成 |
| Q45 | 采纳 | 本包 ID 用 task:M001；历史业务阶段用 business:M01；架构 A–E/U0–U6 不被新包名替代 |
| Q46 | 采纳 | 同 Q09；ProgramRef 键相同不意味着载体格式及加载方式相同 |
| Q47 | 采纳 | F3 guard 保留到逐 kind 证据通过，batch_ordinal 与满批/重放 cut 条件保留 |
| Q48 | 采纳为待测性能缺口 | 不逐行建段/fsync 的优化及证据未交付；不凭静态趋势报已测吞吐 |
| Q49 | 采纳为预算规模缺口 | reservation 条目规模要纳入有界性；不取消 Budget reservation changed 守卫 |
| Q50 | 采纳验收缺口，修正手段 | 检查封存 program/backend/profile 到真实执行的绑定及篡改负例；不是固定 seam 调用计数 |
| Q51 | 部分本轮已修 | 增门依赖关系；同 revision 的组合门证据必须齐，仍待新函数语义测试 |
| Q52 | 本轮已修明确冻结集合 | frozen-files.json 逐路径/摘要，守卫有红反例；旧冻结约束与新任务范围尚待新版卡逐函数收口 |
| Q53 | 本轮已修选择/判定盲区 | REGRESSION117 完整文件+117 必需 ID；T 单独 3 轮；没有重跑或声称 117 已绿 |
| Q54 | 采纳候选缺陷 | F002 语义字段白名单；不存 API key/Authorization/credentials；不以凭据轮换改业务身份 |
| Q55 | 本轮已修派工动作 | F001 改 verify_preserve，禁止无理由重写稳定 kind 权威 |
| Q56 | 收窄 | 并发 buffer 聚合问题保留；现有 row-local 注册表已全阶段读取，不能按假 batch 探针断言丢行 |
| Q57 | 采纳 | 工厂声明 adapter_present=True 表示平台延后适配能力，构造后必验实际实例；不声称实例已经验证 |

## 5. 验收修订的实际交付

- [gates.json](../gates.json) 现在 29 门。增加 REGRESSION117、HISTORICAL_TIMING；21 个新函数均在可执行 selectors 中。CODEC 补 `test_s6_commit_window_quota.py`。
- [历史 ID 清单](historical-failures.json) 保留全部 117 个确定性 ID 和单独 T；[原表](historical-failures-source.md) 不改写历史归因。未来改名/参数矩阵变化必须提交逐 ID 迁移说明与等价断言，不能删失效 ID 凑绿。
- [run_gate.py](../tools/run_gate.py) 执行 pytest 并产生逐测试结果；[gate_plugin.py](../tools/gate_plugin.py) 记录 setup/call/teardown、skip/xfail/收集错误。零测试、遗漏必需测试、意外 deselect、未收尾、超时、skip/xfail 都不接受；进程退出 0 不足以通过。
- 依赖门需同一集成 commit 另行通过；工具不伪造依赖门结果、不自动反复跑其他门。严格入口要求干净的隔离集成树，拒绝主仓，测试前后核冻结摘要，证据只写新的 `/tmp/demiflow-*` 目录。
- [冻结路径](frozen-files.json) 明列四个队列承重文件、planning/dag、execution/contracts/exchange、resources/contracts，共 7 个。本次明确保护集合不是对历史“六/七件”术语的倒推。recovery_identity/OperatorSpec/stream 的目标改动只能限指定符号，未批准整文件任改。
- 新增门守卫测试验证“pytest 退出 0 但 skip/xfail”也被拒、缺测试被拒、冻文件被改变红。**这些是验收工具的测试，不是 21 项业务验收通过。**

117 个原失败全部通过与“没有新增失败”还不足以证明原意图完成；真实同一封存计划、模型请求预算、取消排空、历史保护、产品可用性和性能指标必须有对应证据。门引用已有文件也不代表文件中的每条断言仍合理；改变旧错误行为时要显式迁移断言，不能保留要求错误行为的测试，也不能删除其原保护目的。

## 6. 可以交回 Qwen 的下一轮核对范围

请先核验 Q27/Q56 两处证据边界、Q50 的路径判据、Q01 原子预算的决定，以及修订验收是否仍有漏测/假绿可能。随后按 CRITICAL-REVISION.md 七组分别检查“调用者 → 真实状态/权威 → 消费者 → 故障反例”。

剩余设计交付必须同时更新 CONTRACTS、interfaces、任务卡、装配表和真实链路样例，逐个移除修订标记后才能重新发布。**现在不要将 144 张旧卡一口气发出去。** Qwen 的既有候选按函数吸收，不从零重写；已有在途工作不因本文件被自动叫停。

Review 次数仍诚实记录：之前完成的是 10 个维度检查；本次是对 Qwen 报告的定向复核与第一轮修订，未完成用户要求的 10 遍全量重审，不用工具检查数量替代它。
