# D · P8 批次 3 逐对象退役设计 v1

状态：**仅设计，等待父协调组织 Fable 审阅；未修改产品或测试实现。**

| 字段 | 固定值 |
|---|---|
| 负责人 | D 原作者，后续修订和获准实施仍由本作者完成 |
| 基线 main / tag | `122edf4bd0c827338846abd0114f0b36cf8c1b6d` / `main-20261119-3` |
| 任务分支 | `gpt/engine/p8-batch3-20261120` |
| 专属树 | `/yzp/zhaozy/yangzepeng/0905/_demiflow/worktrees/gpt/engine/p8-batch3-20261120` |
| 本次范围 | 批次 3 的两个条件删除对象、两个保留对象；批次 4/5 仅限定设计 |
| 本次决定 | `timeout.py` 保留；`dataflow_control.py` 保留并完成测试对应；其余两项补齐十一门槛后才可删除 |

设计提交 SHA、文档 SHA-256、最终 clean 状态记录在仓库外的 `tasks/D/design-v1/HANDOFF.json`，避免文档自引用提交号。完整证据根为：

`/yzp/zhaozy/yangzepeng/0905/_demiflow/shared-review/architecture-upgrade-20261120/tasks/D/design-v1/`。

## 1. 意图、依据与不扩大范围

本任务直接承接 R.5.1 的复用与同批清理，受 R.2.3 的受支持恢复、R.5.2 的真实失败/清理/资源边界、R.5.3 的历史保护约束。平台长期意图是同一 Dataset 语义、Local/Ray 共用执行协议、A–E 状态归属明确、原生事实唯一。删除旧实现的实际收益是消除已被统一路径替代的代码和误用入口；SCC、文件数及行数下降不是完成标准。

原始 root 与完整 15 叶原文置于本目录 [ORIGINAL-INTENT.md](ORIGINAL-INTENT.md)，不改写。权威输入为总包 `USER-AUTHORIZATION.txt`、`CONTEXT.txt`、`REVIEW-HANDOFF.txt`、`TASK-D.txt`、`context-manifest.json`，以及后者固定的原始产品档案、平台 root/standing、AR v1.3、全局架构、架构报告第三版、P8 v5、P2 v4 及 P1/P2/P4 既有共识和正式发版规范。`SOURCE-CONTEXT.json` 记录路径及固定摘要。manifest 的全部副本已按字节数和 SHA-256 核对。

优先级：2026-11-20 本轮原话授权、已发布 `main@122edf4` 与正式 `docs/repository-rules.md` 覆盖历史暂停、旧工作位置和不用 subagent 的安排。旧 P8 的“批次 0–2 待验收”不是现状；这些批次已发布，本任务不重做。P1/P2/P4、原生运行、Notebook、版本准入和精确匹配保持。P12 不依赖 D，本任务不得拖 A/P12 发布。

不改变执行器、提交/恢复算法、身份范围或历史迁移承诺，不搬对象，不新建 Protocol/转发层/执行器来代替待删模块。不开发正式目录、integration 或其他人的树，不改业务、环境、Notebook、服务、冻结七件，不自行 Fable、合流、推送或部署。

## 2. 固定源码事实

基线分析时任务树 clean，HEAD 为上述完整 SHA。以下四份文件与旧测量基线 `74d2f9941377a0a2ca82e492d7be3a6b02e3d014` **全文相同**；这只使旧记录可追溯，不把旧调用测量改绑为当前运行证据。

| 对象及基线位置 | 固定事实 | 当前裁定 |
|---|---|---|
| `execution/native_commit_store.py:10` 的 `NativeCommitStore` | 19 行；9 个无实现 Protocol 方法；包内 AST 无导入者、源码 token 无消费者、无直接测试；不是 `@runtime_checkable` | 条件删除整个文件；先证明不是历史/类型依赖 |
| `execution/timeout.py:10,18,51,53` | 两个 frozen dataclass、一个默认实例、正整数 helper；包内无导入者、无直接测试；**`__all__` 明确公开导出三个名字** | 保留，第 8 条门槛不满足；不得先删 `__all__` 再称可删 |
| `execution/chain.py:310` 的 `unified_chain_eligible` | 只做 `cloudpickle.dumps(plan)`，对三类序列化异常返回 False；当前产品和 tests 均无使用方；清单仍记作批次 4，任务卡已明确将它提到批次 3 | 条件删除这一函数；不删 `chain.py`，不扩大到其他旧符号 |
| `execution/dataflow_control.py:24,91` | 产品 AST 无导入者；仅 `test_runtime_v3_h.py` 的 H045–H048 直接构造两类 transport；实际生产使用 `dataflow_hosts.ControlMailbox` | 保留源文件和四个测试；本批先逐断言对应，不宣称已经等价替换 |

固定源码 SHA-256：

| 文件 | SHA-256 |
|---|---|
| `native_commit_store.py` | `9c9b852e7f5def767efd588951d63ca0f54a97037e5b98bf514172915052ebbf` |
| `timeout.py` | `5296c5a49a17804ae9d4438a7c34b7a349ace6e65fa4fadf40e5eee77b6297a5` |
| `chain.py` | `e3a9103a0ec0aef6032474935bbde97c6218fd5ee213294e4a90ac96c9e6eee2` |
| `dataflow_control.py` | `6eff974f9267634590aee2b951cb09ca7d850a5559d5292ece862be34fb090f2` |

`chain` 当前导入者是 `dataflow_code`、`dataflow_seal`、`graph`、`kernels.task_scope`、`native_plan`、`worker.bootstrap`、`worker.execution`。它们使用活代码固定工具或其他旧符号，不使用待删 eligibility。按整文件“仍有人导入”或“旧循环没运行”裁定整个 chain 都不正确。

源码/配置/注册表搜索必须区分用途：目前命中包含 `tests/pre_v3_retention.json`、根 `BASELINE.json` 和历史 `docs/*` 源码摘要、旧方案/审阅原件。这些事实证据不能回改为“从未存在”。产品声明文件内部的定义/自有名字也不等于外部消费者。当前未见两个条件删除对象的活模块路径注册或动态启动字符串；变动前再核对。

## 3. 十一门槛：逐对象结论

详细机器表为 [OBJECT-GATES.json](OBJECT-GATES.json)。`事实成立` 表示指定源码/扫描范围内成立；`待验证` 不能算通过；`不满足` 表示当前保留。

| P8 v5 §7 | NativeCommitStore | timeout | unified_chain_eligible | dataflow_control |
|---|---|---|---|---|
| 1 不是点名公开兼容入口 | 成立 | 成立 | 成立 | 成立 |
| 2 产品无活导入/使用，含工作台/Forge | AST 零导入 | AST 零导入 | 零符号使用；模块有活使用 | AST 零导入 |
| 3 源码/配置/注册表无活字符串入口 | 当前范围成立；历史证据保留 | 自有公开导出存在 | 当前范围成立；旧审阅引用保留 | H045–H048 文档合同仍在，先对应 |
| 4 测量无经产品入口运行到 | 旧 74d2f99 为零；当前探针待补 | 旧记录为零，无需为删它补测 | 旧记录为零；当前探针待补 | 旧记录仅测试直接调用；生产对应待补 |
| 5 非被使用的类型/常量 | 它本身是类型；当前消费者零，历史引用待核 | 公开类型/常量，保留 | 不是类型/常量 | TYPE_CHECKING 的底层合同不是删此文件的依据 |
| 6 冻结文件中有调用方的函数不依赖 | 七件无目标 token/导入 | 七件无目标 token/导入 | 无符号使用；不改冻结件 | 未见直接依赖；不以此批准删除 |
| 7 业务仓不导入 | 已查范围零命中，边界见 §4 | 同左，但不消除公开契约 | 已查范围零命中，边界见 §4 | 同左，测试映射仍未闭合 |
| 8 不在公开导出 | 无包公开/显式 `__all__` 登记；实施复查 | **不满足：模块 `__all__`** | 无包公开/显式 `__all__` 登记 | 未见显式导出；仍有文档合同 |
| 9 删除后封存/旧历史结果不变 | 尚未删除，待前后对比 | 保留 | 尚未删除，待前后对比 | 保留；历史对应未证明 |
| 10 Ray 环境重复 4/9 | 待实施窗口；本轮未跑 Ray | 保留 | 待实施窗口；本轮未跑 Ray | 仅同进程 mailbox 测试不算 Ray |
| 11 每个牵连测试有旧到新处置 | 无直接行为测试，保留守卫/历史验收 | 无直接测试，保留 | 无直接行为测试；真实 chain/封存/预算/拒绝断言保留 | 四项逐断言映射见 TEST-DISPOSITION；全保留 |

**当前没有对象取得“十一条全部通过”的删除资格。** 设计批准不等于门槛补齐。实施只先尝试 NativeCommitStore、再尝试 eligibility；任何一项必要证据不足则该对象原样保留，清单写缺口，提交其余已满足对象。无收益的全历史追查不是强制前置，也不能把缺口换成通过。

## 4. 消费者范围与缺口

父协调 2026-11-20 明确本轮业务根为 `/yzp/zhaozy/yangzepeng/0905/demiwtg` 与 `/yzp/zhaozy/yangzepeng/0905/pan123`。授权限只读源码、相关配置/登记，不导入或执行业务、不查实际数据。

| 根 | 固定工作副本事实 | 实际检查与结果 |
|---|---|---|
| demiwtg | HEAD `8cdab2bcc36711c790c00e38db5ba77047d99b43`；561 项 dirty；已读 AGENTS 的项目/平台/历史保护条款与 `.demiforge/project.json` 元数据 | 当前存在 tracked 源码/识别配置 878 份，其中 749 `.py`；目标 token 零命中。347 份 tracked 源码/配置缺失，未恢复、未假装检查了当前内容 |
| demiwtg 登记根补扫 | 当前项目元数据 SHA-256 `a759451b0bbf5ef6936aab83a65010488dcc9f15461a4b0078450cb9e2f42b6f`；24 个登记根 | 根级 `.py` 及 `operators/flows/tests/tools/common/config/src/lib/schemas/scripts` 源码角色目录，共 720 份 `.py`，均未跟踪；7,533,727 字节，目标 token 零命中 |
| pan123 | HEAD `eba20e928a252c256900654c5444d445a5d79271`；1 项 dirty；无 AGENTS/可识别项目元数据 | tracked `pan123.py`、`patrol.sh` 两份；目标 token 和 demiflow 导入均无。未读凭据、token、日志、pyc |

注册根文件集合摘要为 `93b0508c00a4bc944385d7a42db7d8694b1d702375199c6b7afdcbf938a0e2f9`，逐文件 path/hash/tracked 属性在共享证据 `registered-source.json`。业务扫描前后 HEAD/status 相同，不保管或提交其 dirty。首次补扫递归枚举过慢，已仅中断自有扫描进程；原脚本/退出记录保留，改为增量遍历与源码角色限额后完成。它不是业务失败或通过测试。

未覆盖：其他未登记/未跟踪根、被排除历史源码目录、Notebook 代码与输出、未知外部安装/脚本、非字面量动态模块名、生产载荷。结论只对上述实际范围；不写“全业务零消费者”。历史测试使用平台登记的可复制夹具与既有回执，不读原生产运行。实施开工只需刷新这组有界证据及父协调确认的范围变化，不能为可选退役无限扩扫。

## 5. 改前→改后及职责合同

### 5.1 NativeCommitStore

改前是未被使用的结构化类型说明，描述 `execution_guard/begin_commit/prepare_commit/execution_commit/execution_commits/publish_receipt/confirm_commit/reserve_budgets/finalize_source_commit`，无任何存储实现或实例生命周期。唯一提交事实仍由 RunJournal、队列作用域和 CommitCoordinator 的现有实现拥有。

门槛全满足后的改后：删这 19 行文件；从 retention 的 `modules` 移到 `removed`，保留模块名、批次 3、决定依据和固定证据引用。不开新接口承接它，因为没有迁移调用方。RunJournal/队列的输入 fence/action/sequence/intent/budget、输出 fact/receipt、异常、事务、ACK/fence 与资源 owner 均不变。若发现消费者或历史按模块路径读取，则保留整个模块，不做“空类兼容”或别名。

### 5.2 unified_chain_eligible

改前函数只有纯序列化探针，异常类型是 PicklingError/TypeError/AttributeError，返回 bool；它自己的旧文案描述 legacy fallback，但当前无调用方。实际 chain 两个公开入口已直接组一次 materialize 请求，检查 active action 的 source/operations/provider 后交 `run_dataset_action`，不咨询 eligibility。

门槛全满足后的改后：只删这一个顶层函数及其函数内局部 imports。保留同文件所有活代码固定工具、两个兼容入口的完整签名/函数体、错误与预算断言，以及其余批次 4 函数。retention 的该符号从原 `batch:4` 移到 `removed/batch:3`，说明任务卡提批依据。模块顶层 imports/logger 不顺手清理，除非本对象唯一依赖有精确证明。

外部已有契约不变：必须有 native action context；foreign source/plan/provider 在 worker/owner 创建前拒绝 `INVALID_CONTRACT`；materialize 仍一次 v3 action、返回可重复读 handle；row/byte/worker 上限和失败无提交保留；资源生命周期仍归原 owner。删 eligibility 不意味扩大不可序列化对象的执行能力，也不增加新的 fallback。

### 5.3 两个本批保留对象

`timeout` 的完整输入输出保持：`resolve(params, deadline_at=...)` 计算请求值/宿主上限/剩余期限；`timeout_seconds<=0` 仍 ValueError，默认 7200、termination grace 2、结果最小 1 的现状不借机修订。它不等于运行时所有 deadline 的 owner，也不为退役重写其类型身份。

`dataflow_control` 的两类 transport 及 H045–H048 全部保留。生产 Local 入口是 `worker/bootstrap.py::_run_v3_worker` 创建 `ControlMailbox` 和单 socket reader；进程内/Ray 的 `worker/dataflow_host.py` 同样把 mailbox 注入 transport。`dataflow_hosts.LocalHostEndpoint`、`ControlFrameReader`、`HostedDataflowWorker` 承接帧、身份和回执行为。对照不成立的断言明确记缺口，不能将“都叫 request”当成完整等价。详见 [TEST-DISPOSITION.md](TEST-DISPOSITION.md)。

## 6. 文件与保留清单

获准实施的预期变更上限：

1. `demiflow/execution/native_commit_store.py`：门槛通过才删除。
2. `demiflow/execution/chain.py`：门槛通过才删除 `unified_chain_eligible`。
3. `tests/pre_v3_retention.json`：只变上述对象的状态/删除记录及已复核依据，不改无关 measured 数字，不把旧数据时间改成今天。
4. 本目录文档和必要的现有相关测试：若对应出现真实契约缺口，先以精确反例批准；不预先承诺删除任何 H 测试或新增只检查代码不在的测试。

明确保留五个公开兼容入口 `run_local_queue_chain/run_local_chain/run_ray_chain/run_local_graph/run_ray_graph`；`StreamStats`、`PlanContractError`、v1 dag、plan_interpreter、sealed_plans/sealed_plan_facts、checkpoint/history 需要的类与类型；七个冻结文件全部逐字节保持；保留批次 0–2 的 `removed` 历史。

`tests/test_package_dependency_ratchet.py` 是与 A 的潜在共享文件。当前两个删除候选没有引起其 `PRE_V3_MODULES`、`PLANNING_MAY_IMPORT` 或私有导入预算下降，**预期 D1 不修改该文件**。若精确依赖事实变化，提交具体差异给父协调安排唯一主写，不扩大预算。不修改 `tests/merge_gate.py`，尤其不碰 A 的 GUARDS；不改 FROZEN 摘要。

## 7. 身份、封存、历史与回退

| 契约 | 仅本次设计文档 | 获准的两个删除对象 |
|---|---|---|
| 平台 revision | 包内 `.py` 无变化，现有算法输入不变 | 删除包内文件或编辑 chain 会改变 `platform_identity()`；它不是 Git SHA，不承诺旧 hash 不变 |
| 对象模块/限定名 | 全部不变 | 被删对象消失；其余对象留原模块/限定名/方法字节码；chain 内仍存函数的**整文件 source_hash 会变**，不得称所有对象身份不变 |
| 封存字节/路径读取 | 不写历史 | 不重 dump 原 pickle、不重算旧计划/producer；被删对象若出现在受支持路径读取或类型历史中则该对象保留 |
| 历史只读 | 旧记录按原版本解释 | 登记夹具的历史读取、拒绝和文件摘要前后相同；需原引擎环境的运行继续使用原版本 |
| 恢复拒绝 | 保持现行检查 | 不同平台版本继续明确拒绝；不绕校验、不自动重放未知结果、不新增跨版本迁移承诺 |
| Forge snapshot | 独立于平台 revision；docs 不改变 forge 平台包源清单 | Forge 全平台源清单也可能变化；相关开发证据过期须按原规则重验，不重拍旧 snapshot 或宣称跨版本仍有效 |

依据为 `recovery_identity.py:76–99,113–117,309` 和 `forge/platform_snapshot.py::source_inventory/build_snapshot`。本批不改这些算法。若实际发现需要搬类、模块路径兼容或历史表示迁移，转 C 对应设计；普通源码改变沿用拒绝规则不等待 C 全部设计。

回退依托当前基线与每对象独立提交；父协调可在隔离树 revert 对象提交重新验证。源码回退不回滚生产表、旧 journal、封存计划或历史证据。本作者不回卷 main、不重写标签，不删除树或档案。

## 8. 获准实施后的阶段顺序

1. 父协调核实设计/Fable 阻塞清零后发实施指令。刷新自身 HEAD/status、已扫消费者范围和当前合流变化；不自动切换到未发布 integration。
2. 在固定基线与私有资源域补当前产品调用测量：正常 Dataset/chain 兼容路径、封存/预算拒绝、历史读取；观察所有包进程及 worker，明确仅导入、测试直接调用、经产品入口三种来源。旧监听器三处缺陷不得复用。若未建立可靠跨进程观察，不能以主进程零调用批准删除。
3. 在真实 Ray 环境和自有私有集群重复所需调用/历史检查，与本轮设计阶段分开记录；未获窗口、环境不可用、skip 或观测不足的对象保持。
4. NativeCommitStore 单对象变更→候选前后历史对照→retention/相关守卫→固定提交。任一门槛不满足，保留它并交缺口，不阻止另一对象的独立裁定。
5. eligibility 单对象变更→五个兼容入口与封存/代码固定/预算/错误/恢复验证→固定提交。保留 chain 其余符号和文档历史。
6. 只交 dataflow_control 的测试对应及批次 4/5 限定设计；不顺手实施它们。全部材料映射批准方案与实际 diff，原始失败/skip/not-run 原样保留，交父协调最终 Fable 审阅。
7. 合流、固定 T 四线与差异裁定、R/main 标签/正式目录/推送/归档由父协调和现有平台总控执行。D 的专项通过不代替发版总闸，预览换版独立。

## 9. 契约验收与本次实际结果

详细 nodeid、旧测试处置和失败反例在 TEST-DISPOSITION。最低删除验收包括：13 项 retention/包守卫；五个兼容入口翻译和真实 Local/Ray chain 的 5 项行为；原生历史读取/拒绝/原版本恢复；代码固定默认值/捕获 helper 的正负例；冻结七件摘要相同；已扫业务范围及受支持历史无目标路径依赖。候选代码仍以固定 SHA 运行、进程内断言 `demiflow.__file__` 位于本任务树。

本轮只跑有限基线，全部源自 `122edf4...`、`/yzp/zhaozy/yangzepeng/0905/env/bin/python`、独立 `/tmp/demiflow-D-design-*/` 资源域，4 个 pytest 进程顺序执行：

| 组 | 实际 collection/execution | 结果 |
|---|---|---|
| retention + package ratchet | 13/13 | 13 passed |
| H045–H048 | 4/4 | 4 passed；没有真实 Ray，仅 mailbox/fake connection |
| Local chain compatibility | 5/5 | 5 passed |
| history protection + recovery identity | 12/12 | 12 passed |

合计 34 passed，0 failed/error/skipped；不是全仓成绩。原始 stdout、JUnit、完整 argv/env/解释器/依赖/用例结果在共享证据 `test-receipts.json` 和四组 log/xml；`run_baseline.py` 为实际命令生成器。未运行当前 Ray、全量四线、删后历史对照、当前跨进程函数测量、实际业务或模型。旧 74d2f99 的测量及 `docs/engine-known-state/3640116.json` 仅引用原 SHA，不能当本轮通过。

## 10. 审阅所需结论

请审：两个条件删除对象的边界和补证步骤是否足够；timeout 保留和 dataflow_control 先对应是否守住公开/历史契约；业务范围的明示限制是否准确；chain 整文件身份变化是否按现行拒绝规则诚实处理；批次 4/5 的冻结与保留清单是否完整。

当前待父协调/Fable：设计审阅及实施指令、真实 Ray/调用测量窗口、受支持历史夹具删后验证。当前需真人裁定的仅后续批次 5 冻结无调用函数策略，详见 [BATCH4-5.md](BATCH4-5.md)；默认保留，不以用户休息为选择。没有请求扩大业务、服务或共享环境权限。
