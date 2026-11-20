# 取消平台身份运行/恢复门禁：最小完整设计 v1

状态：设计已完成，产品尚未修改，待父协调亲审后由原 C 作者实施。此次真人决定取代旧 C 的 S1/S2/H1 选择；不建立执行身份分区或通用 compatibility 服务。

真人决定：“我觉得不需要平台身份这个限制能力，业务pipeline能否运行为啥依赖平台身份呢，不需要”。随后授权按此方向尽快推进、方案 review 无问题后原 subagent 实施、按发版规范验收合流。当前以 EXECUTION-BASIS.txt、架构 v3 §5.1 第4项、P12 v2.1 及最新真人决定为依据；不复用旧产品 root/15 叶作新验收需求。

## 1. 固定来源与交付边界

- 新任务树：`/yzp/zhaozy/yangzepeng/0905/_demiflow/worktrees/gpt/engine/remove-platform-admission-20261120`；分支 `gpt/engine/remove-platform-admission-20261120`。
- 实际创建基线 main：`122edf4bd0c827338846abd0114f0b36cf8c1b6d`。旧 C `c2f643083834014d7f40e4c76c9ebd5c4a94883a`、旧 G、B 树未改。
- 只读对照首批固定 integration T：`a6ddb4a88af7a1525649aa31fe77210af3c97ce2`；最新只读 integration `b68bf815bee5c4e8ae27a0d0459e48c98db0e03c` 与 T 产品逐字相同；P12 产品来自 `53207a61e6b8cfb054de48f388eb8fe7f8eb7c0b`，最终 B `1423d2bf22f78a26e7b5ae6263ae434a61b62926`。
- 本设计涉及的引擎入口在 main→T 逐字相同；应用四文件 workspace/checks/manager/native_submission 有 P12 delta。实施前由父协调固定正式代码基线及依赖顺序；不能在 main 旧 API 上写完再覆盖已合入 P12。P12 的 table_resolver 注入、普通/固定输入解析、begin-before-reserve、owner 安全 publish 和 marker 收尾全部保留。
- E2 同时改 `execution/dataflow.py` 的 completed_live_nodes → lower_action 调用以传 history_revalidation（原约563行）；本任务仅改已有 seal 分支约415/426的版本判断，保留 E2 用途。B 发布分诊的 run_reader.py/compiler.py/test_upgrade_business_acceptance.py 本设计不改。
- 设计只写本树文档与自己的共享证据目录；无产品修改、业务执行、服务、模型、共享环境、四线、Fable、合流或部署。

## 2. 改前/改后与不变的真实合同

改前：任意包内 Python 改动可经平台全树摘要在 RunContext、已有 seal、worker、历史 capability、Ray 域探针拒绝；queue 把该摘要嵌入合同；Forge snapshot/工作区摘要/require 开发验收又能间接拒绝。

改后：平台源码摘要、发布版本、Forge 平台快照、开发工具或 sandbox 实现版本差异本身均不能拒绝业务新运行、启动或恢复。当前实现必须能解析原状态、读到真实原产物、遵守现有业务合同；不承诺所有旧格式都有执行适配器。

继续严格校验：RunRef/run_id 与目录、entry_identity、显式/有效 limits、截止时刻与累计预算、业务 UDF/依赖代码与 source/config、计划参数/输入固定版本、目标 URI/schema/overwrite CAS、权限/资源域、attempt/revision/fence、未知副作用与 commit/ACK、原 artifact SHA/成员集合、持久化 schema 与 actor 恢复能力。原 `describe`、`plan_identity`、business code pin 不改算法，不移类/模块，不重写旧 run digest。

运行未准入的拒绝仍是原合同错误，不制造成功、替代 run 或业务失败。P12 已发生的真实清理不回滚；后续 attempt/launch/reservation 顺序不变。

## 3. 真实消费者分类（完整条目见 SOURCE-CLASSIFICATION.json）

| 类别 | 实际入口/链 | 改动 |
|---|---|---|
| blocking | execution/run_context.py `resume`、`__enter__` | 删除当前全树摘要与原 platform_revision 的相等判断；恢复沿用原 configuration 原字节，只校验 run/entry/limits 与已支持 stream_protocol/format，不拿未知平台号作格式判断 |
| blocking | workbench/runs/manager.py `resume` | 删除中文“引擎实现已变更”前置拒绝；binding 缺失、权限、attempt、P12 owner/清理/预算保留 |
| blocking | execution/dataflow.py 已有 seal 分支；dataflow_services.py `_load_programs` | 删除平台比较；仍先验证原 seal、assignment、plan_id/revision、原 program/content pin，再加载业务程序 |
| blocking | execution/dataflow_history.py `validate_resume_compatibility`；dataflow_checkpoint.py `_recover_historical` | 删除平台不同的 reason 和当前全树求值；保留 `current_runtime` 为可省略的旧调用兼容关键字，不作为准入。原 schema、原 code pin 可读、checkpoint image、recovery 保证逐项验证 |
| blocking | execution/dataflow_artifacts.py `verify_host_domain` / `_probe_read_domain` | Ray 域探针只验证实际 node_id、原 challenge SHA、可达 store；不比较 runtime_revision，不在远端全树扫描。节点不符与产物损坏仍拒绝 |
| semantic-hash | queue_producer.py `configure/_resume` | 新 generation 写无 platform 的 producer-transform.v3；旧 v2 摘要按第4节兼容，保持原 hash 和作用域 |
| semantic-hash + frozen | queue_execution.py `validate_topology`、两个调用；dataflow_queue.py 调用 | 新 consumer.v3 无 platform，使用原业务 operations/result/settlement；由现有 queue_contract.py 中窄 helper 解析旧合同。精确冻结补丁草案附后 |
| semantic-hash, preserve | queue_source.py、queue.py、queue_commit.py、queue_history.py、dataflow_queue_recovery.py | 原完整 consumer 合同或原 transform hash 一经选定，继续传到 binding、worker、scope_id、commit_id、completion/ACK；这些摘要的相等校验不能删。预期这五文件不改，尤其 queue_source 仍冻结 |
| provenance-only (after above removal) | graph.py `_build_manifest`、chain.py `_build_sealed_manifest`；dataflow_seal.py `seal_dataflow`；worker/runtime.py Register；dataflow_services.py `_driver_hosts/_ray_hosts` | 保留原 manifest/platform/code-pin 来源字段及内部 artifact 关联。不把封存产物 hash 与当前平台源 hash 混为一物；仅注释更正“平台单独 pin 提供准入”的旧说法 |
| source utility | recovery_identity.py `platform_identity`；host/_surface.py 公开索引 | 原函数/原算法保留作显式来源诊断。首版新 RunContext 仅在首次创建配置时取一次来源摘要，恢复/worker/历史/队列不再求当前全树摘要（第7节解释成本） |
| blocking + semantic-hash | projects/workspace.py `_inspect_flow/_report_status/workspace_policy/verify_run_basis`；runs/manager `_prepare/verify_seal/_check_basis`；native_submission `inspect_submission/verify_basis` | 当前检查/新封存使用当前快照，旧 workspace 平台绑定只作来源；业务 basis 排除 platform，旧混合摘要按完整原声明作只读投影核验。不能只传 platform_check=False 因为旧 complete=False 与 workspace hash 仍会拒绝 |
| provenance/evidence status | projects/checks.py `check/status`、forge/platform_snapshot.py build/load/check/compare；platform_cli | 快照文件自身完整性仍校验，显式平台诊断仍可报告 stale。项目/flow 的平台漂移作为单列 provenance 状态，不进入运行必需的业务 changes/complete；不能冒称旧工程证据已由新工具重验 |
| hidden blocking + semantic-hash | conversations/admission.guard → development.project_tasks → ManagedTasks.status/_projection → tasks.current → Project.binding | require 路径改调用窄只读运行验收视图，第6节详述。开发 status/verify/review 原语义及旧签名不重签 |
| hidden blocking | managed._projection 对 review_authority.execution_identity 的比较；review_authority→sandbox.execution_identity | 运行时只验证原签名及当时 developer/verifier 同一实现事实，取消与当前 sandbox 实现的比较；开发/验证当次执行仍绑定真实 sandbox |
| integrity, preserve | forge.development registration 签名链、ReviewAuthority.validate、业务 snapshots；load_workspace snapshot path/digest；sealed source files；requirements/wheel/environment locks；worker grants | 它们认证原产物/权限/实际环境，不是当前平台版本比较，不整段删除。平台自身 distribution metadata 在 managed 环境比较中单列排除，第三方依赖/Python 实际环境、原封存包字节校验仍保留 |

`source_digest` 不是都为平台：location_index、analysis 的 source_snapshot、literal_source、业务 artifact 等真实数据身份全部保留。`platform_policy_digest` 是显式计划策略，不是全树发布身份。公开 API/对象 module/qualname/source_hash 与真正编译配置不在这次算法范围内。

## 4. queue 旧混合摘要的处理（最关键历史边界）

事实：`queue_pools.producer_contract/consumer_contract` 和 `queue_workers.consumer_contract` 只存 hash；producer `runtime_commit_scopes.declaration.transform` 仍只存 hash。consumer 的 task scope `declaration.consumer` 则保存完整 v2 合同（含原 platform）；本地 run runtime_identity 保存原 platform_revision。scope_id/commit_id/receipt/proof 已嵌入这些原值，不能替换成 v3 hash。

推荐在 `queue_contract.py` 增加两个领域内窄 helper；不用新表、迁移器、通用 compat 服务或修改 queue 历史。输入是当前完整业务声明、实际原 run/action、queue generation；输出是本次执行必须使用的原完整 consumer 声明或原 producer hash。校验先于 claims/网络请求/新 effect：

1. 未绑定 generation：构造 v3（与原 v2 相同业务字段，只有 protocol 升 v3、移除 platform）。producer/consumer 原有事务负责首次原子绑定；不同业务并发绑定仍由旧锁拒绝。
2. 已有 hash 等于当前 v3 hash：继续；平台无需参与。
3. 同一原 RunRef 恢复 v2：用原 run 的 platform_revision 填入当前 v2 完整声明，重建 hash 并与 queue 原 hash 严格比较；比较成功才使用原 v2 值。这是在检查完整业务合同，不是比较当前平台。业务 UDF、源、规范、operations/result/settlement 任何变化都会失败。不得凭“run 有旧 platform 字段”直接放行。
4. 新 consumer Run 加入原 v2 generation：从该 generation 已有 task acquisition/final scope 读取完整 consumer；验证 scope_id=identity(declaration)、queue/generation/task 对应、consumer hash 等于 pool 绑定；移除且只移除协议定义中的顶层 platform，比较其余完整业务字段。相同则携带原 consumer 字节继续，完成记录不重写。
5. 具体缺口：旧 generation 已绑定 consumer hash，但从未创建 task scope，且调用方没有原 RunRef；或旧 producer 由一个新 Run 接管、只有 producer hash，没有原 run 的 platform_revision。不可从 hash 分离原业务字段。返回“缺少原队列合同校验材料（仅存摘要），需原 RunRef/原完整声明”，不说平台不一致，不创建 scope、不修改 pool。旧原 RunRef 恢复路径可工作；此缺口也会在同平台且材料缺失时发生。不得自动猜版本、忽略hash、扫描其他任务或重建新 pool 冒充恢复。

读取只查指定 generation 与一个原 scope，使用现有 DB timeout/queue 任务上限；单合同沿原64 KiB上限，校验 JSON结构/协议/字段，原run配置也有上限。原 generation 不升级、不重签、不改 consumer/producer hash；新增 worker/attempt/正常新事实仍由原 owner 追加。

冻结具体范围：仅 queue_execution.py 的 `validate_topology(..., action=None)`、末尾合同构造和两个调用传 action，完整草案见 FROZEN-PROPOSAL.diff。`dataflow_queue.NativeAcquisitionService.configure` 同步传实际 action。`queue_source.py`、raw_fetch、queue_sinks、planning/dag、contracts/exchange、resources/contracts 不改；不编辑 FROZEN 值。父审具体范围、通过反例后，管理者按正式规范处理同批守卫记载，不能用摘要更新消掉红灯。

## 5. 工作台/Forge 的新运行与旧 basis

新检查用当前加载平台的 catalog/真实 capabilities，不要求 workspace 已手动 bind 到同一快照。`_inspect_flow` 读取当前 bounded snapshot，执行 FlowChecker，报告来源是本次 checker；检查前后确保本次实际来源没有 TOCTOU，再保存新报告。`_prepare` 封存同一当前 snapshot 的实际 runtime 文件与环境；旧 workspace.json 和其平台快照不被自动修改，旧报告不被标成“新平台验过”。current snapshot 在已有 operation budget 内共享，不反复全包扫描。

`_report_status` 的 completeness 指业务全部校验完成，不再取决于 platform_check；可保留调用关键字兼容，其结果单列 platform_provenance。原 snapshot 自身格式/digest/path 有问题仍拒绝为损坏原件，单纯与当前不同不拒绝。

旧 flow v3 已有 `binding.workspace_policy` 完整声明：先验证其 hash 与原 binding.workspace 一致，当前 policy 只把 platform 这个非业务字段替换成原记录值，再按原算法比较，或比较两份去 platform 的完整 policy；预算、注册、参数、locations、权限不省略。旧 v1/v2 缺当前执行合同仍按具体格式保持只读，不因为这个任务伪造适配器。

旧 native v1 的 binding.workspace_digest 只存混合摘要，但 run manifest.workspace 保存完整原 workspace：先核原 manifest.workspace 的 hash 等于报告绑定，再比较当前和原 workspace 除 platform 外全部字段。platform_digest 只证明原检查来源，不与当前平台比；报告和 manifest 自身 digest 仍原样验证。新 native 报告沿用现有格式，保存完整业务控制依据供同一逻辑使用，不需要改旧 digest。source/resources/params、actual inputs、targets、change scope 和 root 权限继续前后二次校验。

`verify_seal` 继续校验旧 run/runtime 包文件与原 manifest 文件清单，这是原封存内容完整性，不是要求当前发布匹配。既有 managed worker 从原封存脚本启动，不为了放行改旧脚本。环境比较排除平台 distribution 本身的版本/metadata；保留原 Python/第三方依赖/extras 的实际锁与可用性，不因平台名称借机豁免它们。缺模块、未知 pickle/state schema 按实际错误拒绝。

项目级 check 的 `checker_platform_digest/workspace_platform_status` 保留追溯，当前平台不同不进入运行 basis。显式 `platform check` 仍可给 stale 诊断；项目工程验收信息与业务是否可运行分开，不将 provenance 警告改名塞回 stale 门禁。UI 当前消费 flow/run 状态，未发现 JS 独立平台相等门禁；本任务不重做页面。

## 6. require 开发验收链：只读业务验收视图

此链必须同批处理，否则普通运行去门禁而 require 仍被平台更新锁死。现状 `tasks.current` 比 `digest({config: project.json SHA, tool: implementation_files + standards SHA})`；`_projection` 又要求原 sandbox receipt implementation 等于当前实现。签名真伪与“当前实现相同”是不同校验。

在现有 `ManagedTasks` 增加窄的 `runtime_acceptance(task_id)`，仅 admission.guard 使用；普通 `status/check/verify/evaluate_acceptance` 保持完整工程证据语义。复用原 lock、_validate、签名验证、proof、logs、任务/turn/请求、问题、scope/staging/root 校验，不创建新验收记录、不调用模型/执行器、不设全局绕过开关。

新任务开始时保存完整 binding_components/config 来源（有界原 project.json 与已有 tool清单，hash 必须等于原 state.binding）；后续 registration/implementation 签名链继续认证其变更。新验收同时保存运行消费所需的业务 snapshot 投影和其原 fingerprint 对应关系，纳入现有受保护 task/签名证据，不改源 fingerprint 算法。运行投影仅去掉明确由工具根拥有的版本/平台来源项；业务代码、配置、source definitions、验证命令、只读业务资料、用户授权/政策均保留。

旧任务读取顺序：从完整 binding_components（若有）或已验签 registration 的 project.after_raw/after_sha256、implementation revision 的 before/after_components 恢复配置依据；完整 source/review-input/原 readonly_baseline 作为原业务/平台分类证据。所有投影先认证原完整摘要/签名，再在内存中排除已证明为平台所有的项；不能按名字含 demiflow/platform 就抹掉业务字段。当前业务文件与原已验收业务 snapshot 相同、原签名/日志/acceptance/turn 完整且政策仍授权，运行可通过；当前 sandbox implementation 不再参与。

确切旧缺口：没有 signed registration/implementation revision、未保存 binding_components、且 project.json 未被原已认证源清单覆盖的旧任务，仅有不可逆 state.binding。无法证明项目配置没改，不能删除配置保护来放行。返回“历史验收缺少独立项目配置依据，无法核验当前业务配置”，保留旧 accepted 事实与历史只读；不把旧任务改为未验收，不自动重新签名/启动任务。若有完整依据，必须跨工具版本通过，不能因为旧格式一律拒绝。缺口也是材料不充分，不是平台版本不同。父审应具体裁定这条缺材料语义；无需重新询问 S2/H1。

运行视图不采用旧 status.ready 作为结论，否则旧工具检查会重新进入；也不删 run_policy=require 或直接相信 turn.accepted。双向反例必须证明只改工具/sandbox文件能通过，业务配置/UDF/授权/日志/签名变动仍拒绝。

## 7. 平台来源、持久化格式与回退

本次不新造平台身份算法。原 `platform_identity()` 全树函数保持公开兼容和显式诊断用途。最小首版仍在新 RunContext 首次创建 runtime_identity 时记录一次原摘要，理由：现有 K1 manifest 强制 source_digest 为真实 SHA，且来源有原件审计价值；取消恢复/每worker/queue反复计算，旧 configuration 直接复用。暂不为节省一次有界扫描引入伪零值、新 envelope 或 format 迁移。该扫描只产出来源、不比较其他运行/平台；其原文件数/字节资源边界保留。若父审认为连这一次来源计算也不值得，需明确采用 provenance 可缺的具体格式变更，不能隐式写假hash。本设计推荐首版保留一次，避免扩工。

新 queue protocol v3 是业务合同去除非语义项的局部版本；不改变 queue DB schema、已存在 scope/commit/proof、program/seal/wire 协议。旧 run、snapshot、report、task、signed receipt 的字节与摘要永远原值；恢复可按原状态机追加真实 attempt/新事实，历史保护验证分别比较不可变原件和合法追加，不要求实际恢复后整个可写 journal 文件仍同 SHA。

未知状态/适配器缺失的拒绝用具体 schema/缺失 artifact/recovery guarantee，不用 current platform hash 或“same engine”兜底。F1→v3 缺适配器、delivery_only/attempt_only 无计算恢复保证不是本任务新增承诺。保持原只读 reader。

回退：新版本已写 queue v3 后，旧平台会因不认识/不匹配真实 queue protocol 拒绝继续；不能改原合同倒灌v2。保留新版本可读原记录，回退发布按反向提交/两道闸走，不重置业务或预览。新 RuntimeContext 去门禁不改原format；新 managed 验收额外字段向前只读、不替旧签名作担保。

## 8. 有界实施清单与验证

一个整体交付，内部可分三提交但不能把只删外层作为已修：

1. 引擎/历史/worker/Ray 显式门禁与 queue 兼容。预计产品文件：run_context.py、dataflow.py、dataflow_services.py、dataflow_history.py、dataflow_checkpoint.py、dataflow_artifacts.py、queue_contract.py、queue_producer.py、queue_execution.py、dataflow_queue.py；chain.py 仅更正失效注释。其余来源写入消费者原则上不改。
2. workspace/checks/runs.manager/native_submission/forge.platform_environment 的业务 basis、当前检查/封存和 provenance；P12 调用注入不变。Forge snapshot 算法本身原则上不改，仅复用当前 builder。
3. require 验收 consumer：conversations/admission.py、forge/development/tasks.py、managed.py、project.py、registration.py 的窄读取/存证；不重写已有开发任务、不放宽签名/受保护政策。最后静态扫描所有 platform/runtime-version 关系与负例。

冻结源码仅上述精确 queue_execution 草案，FROZEN/守卫范围裁定必须父审；本阶段不修改其值。真人最新目标为 13:35 +08（约1小时）收口；父审固定范围后立即实施并优先给核心定向证据，不用长文排版或未受影响外围延后。实际实现与验证未完成就如实列未修，不将时间目标换算为通过；管理者两道闸/四线/发布依规范独立裁定。旧任务材料缺口不应变成“所有历史必须迁完”前置；按可证明记录形状分别验收。

| 验证组 | 正例（必须实际入口） | 必留负例 |
|---|---|---|
| 原生新运行/恢复 | 私有旧 producer 产出真实 v3 cut，新源码只加无关平台 .py；同 RunRef 正常恢复；新run可执行 | entry/UDF/helper/输入/limits 变动，RunRef冒认、completed run、缺原cut拒绝 |
| 已 seal/worker/checkpoint | 真实读取原 seal→driver→worker→恢复原cut；当前平台digest故意不同仍通过 | program payload SHA/成员/assignment/fence/state schema/source vector 损坏仍拒绝 |
| Ray 域 | 有界实际远端读或隔离connector探针，运行实现来源不同可证明同challenge | wrong node、wrongdigest、不可达域、超预算拒绝；未跑多机不称多机通过 |
| queue producer/consumer | 新v3跨平台新worker；旧v2原RunRef；有完整旧consumer的跨run接续；sink/explicit两入口；scope/commit/ACK原身份 | 业务transform/source/messages/result/settlement/pool/generation变化；缺原材料精准拒绝且无scope/claim；原已确认事实不改 |
| managed native/Flow | workspace平台旧snapshot，当前新inspect/prepare/start；旧原封存resume；显式平台rebind不改变业务basis | source/params/table固定ref/target/CAS/location grant/audit/permission/owner/预算变动；sealed文件损坏；P12 marker/reserve负例保留 |
| require | accepted业务不变，仅工具/sandbox源码变动，prepare/start/resume均可；签名链完整旧task兼容 | 项目配置/验证命令/UDF/helper/readonly业务/政策/turn/receipt/log变动拒绝；hash-only旧记录明确缺配置依据 |
| 历史 | 原snapshot/report/task/seal/program/旧queue合同与proof前后SHA逐项不变；只读路径零写；resume仅正常追加 | 禁止重签/重建替代run/改旧manifest摘要；未知schema真实拒绝 |

使用自己树/私有导出/自有小fixture，断言import来源，记录原producer SHA与新candidate SHA、全部原失败/skip/not-run。选择既有相关测试及新增有意义反例；不重跑未改 P12 312/40 或全仓四线。现有 W013/W014 平台拒绝断言改为真正执行链的跨平台可运行并保留其他负例，不用替身返回即声称恢复。queue/history/require 的范围是核心，不作为可选旁支。

## 9. 父审裁定点与当前未验证项

需父协调裁定具体冻结范围、实施基线/P12依赖、上述两类 hash-only 历史记录的证据缺口处理，以及保留一次来源扫描的最小取舍；它们是明确技术决策，不是再问用户选身份系统。用户产品方向和方案通过后的实施授权已具备，无新 S2/H1 问卷。

已验证：实际源码调用链、完整旧记录字段/摘要链、main→T交叉文件、静态分类、纯 JSON 摘要重建探针。未验证：未来实现、跨平台真实运行/恢复、queue/require反例、浏览器/多机Ray、发布两道闸。本设计不是修复结果，未称旧首批T已兑现去门禁。
