# C：平台身份与历史读取／恢复策略设计 v1

状态：设计候选，等待总控及 Fable 审阅；D1、D2 尚待真人裁定。**未实施身份算法、封存格式、历史迁移或跨版本恢复。** 作者：任务 C 原作者 `/root/c_identity`；后续修订仍由同一作者负责。

基线：`main-20261119-3` / `122edf4bd0c827338846abd0114f0b36cf8c1b6d`。分支 `gpt/design/platform-identity-20261120`，隔离树 `/yzp/zhaozy/yangzepeng/0905/_demiflow/worktrees/gpt/design/platform-identity-20261120`。本设计的最终提交和 clean 回执在共享交接包中，避免提交号自引用。下文源码路径、行号均绑定此基线；探针源码和完整清单见同目录 `identity_probe.py`、`probe-results.json`。

## 1. 结论和需要决定的两项承诺

推荐分开管理**执行兼容身份、应用控制兼容身份、应用发布来源、Forge 工程快照**。执行兼容仍采用内容精确相等；不引入“版本号兼容”“只要测试通过就放行”的推断。第一版范围保守，所有底座 Python、供应商代码和执行资源均保留；工作台／Forge 的运行控制、封存、检查、权限和启动闭包仍受应用控制身份约束。不能按目录名称把这两个包全部认作 UI。

推荐历史策略为：已存在记录保留原算法、摘要、计划、RunRef 和原环境；当前版本提供已支持格式的有界只读读取；旧运行只在原匹配环境及已有恢复能力内继续。**不提供旧 run 到新身份的原地升级，不增加任意跨执行版本恢复。** 对采用新身份的新运行，只允许身份各必要分量完全相等时跨应用发布版本继续；应用版本变化本身既不是准入，也不是拒绝证据。

### D1：平台身份的范围（用户决定）

| 选项 | 产品承诺与代价 | 建议 |
|---|---|---|
| S1 继续全树身份 | 任何包内 Python 改动均可能使旧运行需原平台版本；仍有包外依赖／非 Python 资源不在此 hash 的局限。无需迁移算法，维护成本最低。 | 可接受的保守选择；其他任务继续按此交付。 |
| S2 分离身份、精确比较 | 新记录区分执行、应用控制、发布来源与 Forge 证据。SDK 运行可不受纯应用改动影响；受管运行还须控制闭包／环境相等。只有经证明独立的 UI／文档不影响恢复。首次上线是新记录的版本边界，旧记录不获得新承诺。 | **推荐。** 先保守全覆盖，再按具体收益缩范围；不做每算子裁剪。 |
| S3 按每个计划实际运行到的文件生成最小身份 | 理论上减少无关失效，但需要证明所有动态分支、lazy import、后端、插件、资源文件和子进程闭包。一次观测漏路径就误准入。 | 本轮不推荐，投入和风险超出收益。 |

S2 并不承诺所有名为“UI-only”的 `.py` 改动都可恢复：第一版应用控制身份仍保留全部应用 Python。静态网页样式／展示资源可按封闭清单单列；Python 文件未来排除需要逐文件证明。S2 改的是兼容范围，不能借已有普通源码修改授权直接实施。

### D2：历史与模块搬迁的承诺（用户决定）

| 选项 | 结果 | 建议 |
|---|---|---|
| H1 原记录原版本＋当前只读；新身份仅用于新运行 | 原 v1 摘要原样解释；模块移动后，新运行用新路径，旧历史的 JSON／固定数据可读，旧 pickle 的可执行恢复仍需原模块和环境。新 v2 记录只在精确匹配兼容分量下继续。 | **推荐。** 不自动迁移，不创建“替代成功运行”。 |
| H2 为点名的旧格式／模块加入受审迁移适配器 | 只对列名的来源版本、目标版本、数据／副作用域逐项证明；默认离线生成独立新产物并保留原件、关联和回退。恢复资格仍独立判断。 | 仅在真实历史需求出现时单独设计；本轮不承诺任何适配器。 |
| H3 任意模块重命名／版本升级后自动恢复 | 需要重建对象、依赖、状态、提交／ACK 与未知副作用等价证明；不能靠改 digest 或 import alias 达成。 | 不提供。 |

用户选择 S2/H1 后，技术方案按第 5–8 节实施；这不是批准扩大冻结件范围。已发现 `execution/queue_execution.py:126` 是冻结件中的平台身份消费者，启用覆盖队列的 v2 写入之前，需提交该具体变更的冻结件范围裁决与反例验收；不能只更新 FROZEN 摘要。没有该裁决可先完成双读／影子清单设计切片，但不得宣称 v2 全路径启用。S1/H1 无此新增冻结件需求。

## 2. 原始意图、已有共识与本轮事实分开

原始产品 R／15 叶逐字副本见 `original-intent.md`。任务直接服务 R.2.3（历史与受支持恢复）、R.5.1（唯一职责）和 R.5.3（历史保护）；通过稳定的准入支持 R.2.2、R.5.2。计算平台长期 root 是“把 DemiFlow 建成正确、稳定、高性能、通用且架构干净的统一数据计算平台，完整支撑用户的实际业务场景”。长期意图中的统一执行、唯一资源 owner、有限资源、unknown 不重派和用户掌控继续有效；2026-11-07 的暂停／未合入状态不覆盖本次 11-20 授权。

AR v1.3 的 C1–C6、唯一业务项目记录、工作台→Forge 单向依赖及底座不依赖两者保持。身份描述是记录中的准入依据，不是新运行账本、版本服务或资源 allocator。应用的历史展示、Notebook、Case 不以运行引擎在线为前提；本设计也不恢复已退出活跃入口的旧 Case UI。

P1：保留的运行类及原模块路径不能借目录整理搬迁；函数全文 source_hash 的差异需要真实解释，代码身份不按行号归一化。P2：注册键集合、类身份判断和封存路径不动，历史图不加严；生产拒绝只限已证明确切不变量。P8：公开兼容入口、读历史依赖保留；冻结不是默认可删。P12 v2.1 的 begin／marker 失败语义已通过设计审阅，C 不重开该方向。

本轮最新实际发布是 `122edf4`；架构 v3 数字原本绑定 `1855e00`，随后 `3640116` 增量有独立说明，不能自动改绑。本次实测包内 1,125 个 Python 文件、10,854,934 字节，旧平台 digest 为 `bedffb7dc807fc7a1127f588fa6a7d71b082f145a18596bf054d35c198c24b10`。本轮不检查或切换在线预览。发布回执的全量受测 T=`3640116f4f5cc573ccf68c82343c6d379b351437` 与发布 R=`122edf4` 分开；不称 R 的四线已同 SHA 全绿。

固定输入共 24 项，已逐字节核验 manifest 的字节数和 SHA-256，记录在 `probe-results.json.context_verified`；manifest SHA-256=`26b94ecee45cb19f6d60d6f6d7db868efcc8a3551ac78cffa10b5746d2a1c749`。正式发版规范依然要求隔离分支→提交合并前 gate→固定提交四线与差异裁定→main／正式目录→点名标签 push→归档；预览另行换版。

## 3. 当前身份链：实际输入、消费者与拒绝点

### 3.1 平台身份及传播

`execution/recovery_identity.py:309–326` 的 `platform_identity()` 以真实安装根 `demiflow/` 为基准，将排序后的全部 `*.py` 的**相对路径字节紧接文件内容**加入 SHA-256；不是 Git SHA，不含 pyproject 版本号，不含 CSS／JS／JSON，不含包外第三方二进制或解释器。上限 4,096 文件／64 MiB，Python 文件 symlink 拒绝，读取块 64 KiB。现有算法没有显式 algorithm 标签，`runtime_identity.format=1` 的 `platform_revision` 是字符串。算法按历史原样保留，不能用新的规范 JSON 摘要代算旧字段。

| 使用位置（唯一 owner） | 读／写及顺序 | 失配结果／边界 |
|---|---|---|
| `run_context.py:327–339,368–388`（引擎） | resume 读原 journal 后先比 revision；进入上下文再次比，随后才新 attempt／claim | `RecoveryContractError`：要求 original engine revision。进入上下文在比较前可能取得私有 runtime lease，必须收尾；不能宣称整个 __enter__ 零文件操作。 |
| `run_journal.py:721–728,793–804`（引擎） | 单一 runtime_identity 原字节文档；重复 claim 整份 configuration 必须相同 | `Immutable execution identity/limits differ`；不能为“兼容”覆盖旧文档。 |
| `dataflow_seal.py:559–566` | code pin v3 保存 journal 的 platform_revision 和每个程序 pin | 绑定到原 SealedExecution 的 content ref；旧 digest 不重算。 |
| `dataflow.py:415–427` | 已封存 action：先读图与相关静态绑定，再比 revision，随后验证程序 | `DataflowError(CODE_MISMATCH)`；不编译新计划替代旧计划。 |
| `dataflow_services.py:93–129,183–198` | driver 将 revision 放 HostDescriptor；worker 加载图／plan_id／revision 后，在程序 pickle 前比平台 | `CODE_MISMATCH`；plan_revision 失配为 `FENCED`。driver 和 worker 都要守，不能只修改 Web 前置检查。 |
| `dataflow_artifacts.py:172,185–203` | Ray 节点 challenge 返回真实 revision，driver 对 host 声明比较 | `CODE_MISMATCH`；只读 proof 不等于 worker 已完成执行。 |
| `dataflow_checkpoint.py:555–560` → `dataflow_history.py:466–550` | 历史恢复 capability 读取原 pin、plan、recovery image、domain/profile | `CapabilityReport(supported=False,reasons=…)`；平台相同也可能因 attempt_only、delivery_only、缺 image 或旧 native adapter 不存在而不支持。 |
| `workbench/runs/manager.py:889–906` | 当前授权先执行，然后原 journal revision 比较，之后才 worker resume／start | `ForgeError(code='conflict')`，中文说明原引擎版本；没有统一合同也拒绝。 |
| `queue_producer.py:127`, `queue_execution.py:126–128` | producer-transform／consumer 合同内也写平台 digest，再形成队列语义身份 | 不只是顶层运行 guard；漏改将使 v2 仍被旧全树 hash 间接钉住。后者为冻结件。 |
| `chain.py:1270`, `graph.py:109`, `worker/runtime.py:160` | 原封存 manifest platform.source_digest／worker artifact_digest_observed | 历史 ABI／旧封存事实沿用；不能把这些字段偷换为产品 semver。 |
| `execution/host/_surface.py:52` | P4 inspection 导出 platform_identity，工作台经该面调用 | 注册名字／方向保留；新接口需同批登记，不另造反向依赖。 |

`probe-results.json.source_audit.identity_consumers` 列出 recovery_identity 与 platform_snapshot 的全部静态 `from` 导入点及符号。另已用文本检索核对上述字段传播与 P4 字符串映射；这不是“所有动态调用已执行”的声明。recovery_identity 的非平台调用分布于 collect operators/reading，planning dag/dataset_dag/dataflow_compile，以及 checked/dataflow_api/dataflow_sources/dataflow_stream_lance/literal_source_identity/native_plan/plan_facts/stream_delivery/chain/queue/run_context/seal；它们消费对象、源、动作身份。修改平台算法不能顺带改变这些投影。

还有一份名称易混淆的 `execution/contracts/pipeline.py:23` 的 `PlatformRuntimeIdentity`，绑定 wheel SHA、requirements、ABI 等，`from_mapping` 仍有历史 `distribution == 'demiurge'` 限定。它不是本次 `platform_identity()` 的返回类型，也不是 v3 全树身份可直接换用的对象；保持已有消费者，不借 C 复活旧 bundle 执行能力。

### 3.2 对象身份与按模块加载

| 对象 | 当前身份输入 | 搬迁／改变的实际影响 |
|---|---|---|
| 普通函数 | module、qualname、整份源文件 SHA-256（≤8 MiB）、code_record 摘要、defaults／kwdefaults／closure | 同文件无关行也改变 source_hash；模块／限定名变化独立改变身份。code_record 不含文件路径和行号。 |
| 类 | module、qualname、类 `vars` 中直接 `inspect.isfunction` 的方法 code_record | 不是“整类完整源文摘要”；不能据此宣称类所有属性均被覆盖。搬模块必变。 |
| dataclass | `type.__module__ + qualname` 和字段递归描述，排除 runtime_contract／label | 类迁移改变 type 字符串；新增字段、字段语义亦可能改变计划。 |
| 实例／native actor | native 类型描述＋recovery_identity，或 callable 类型＋initial_state | 构造状态和原生专用合同必须保留；不能改成仅类名。 |
| Path／源／程序 | Path 是绝对路径；FileSource 内容清单；plan_identity 包括 kind/source/operations/sink/arguments；业务依赖闭包独立归档 | `None` 与 `{}` 不等价。根路径迁移不是纯字节等价。源码模块别名不能证明业务依赖相同。 |

`dataflow_program_pickle.py` / `dataflow_seal.py:188–233,370–416` 用 cloudpickle 保存程序：importable 类／函数可按原模块路径加载，闭包／局部对象可能 by-value；不能说所有 pickle 都 by-value 或所有类都按路径。加载前核对 artifact／program pin；加载后重算代码身份，不同拒绝 `INVALID_CONTRACT`；缺模块等反序列化错误转 `CORRUPT_ARTIFACT`。原模块缺失的独立夹具复现了 `ModuleNotFoundError`。历史展示不得为了“显示参数”调用这条可执行反序列化路径。

`chain._business_dependency_modules` 明确跳过平台、stdlib、site-packages，平台由 platform_revision 另绑，第三方环境原注释标为 deferred D5。`dataflow_code.business_sources` 补业务包 initializer，并以 64 模块／8 MiB 上限封存；worker mount_sources 后不替换已加载模块，后续 pin 验证负责拒绝。这意味着全树 Python hash 本身并不是完整 hermetic 环境证明。

### 3.3 Forge snapshot 与受管封存是另一层约束

`forge/platform_snapshot.py` 当前格式 v5；读 v1–v5 时保留原 discovery／standard version 和 scope。源清单包含包内**所有普通文件**（排除显式 cache／pyc/pyo），不是只 `.py`；有目录深度、条目、文件／总字节预算及前后 inode/mtime/ctime 检查。snapshot digest 覆盖 standard、catalog、platform、environment 四节摘要以及 format/scope/limits；created_at provenance 不影响 digest。

`platform_environment.py` 另外绑定包外 pyproject 原字节／安装 METADATA，以及 Python 实现／完整版本／cache_tag／系统／架构、选定 extras 的递归依赖元数据。`dependency_assurance='metadata_only'`：RECORD 文本的 hash 不是逐个验证安装的 native 文件，也不能保证未改 dist metadata 的二进制没有变化。严禁把此限制藏在“平台完全一致”里。

| 消费者 | 当前行为 |
|---|---|
| `forge/platform_cli.py` snapshot/check | 只写新 snapshot，拒绝覆盖；check 为 current/stale/incomplete/invalid。完整性不足不授予工程证据。 |
| `workbench/projects/workspace.py:57,156,716,1071,1309` | 初始化、读取、显式重新绑定；检查当前快照及前后稳定性；报告 status 标 platform_changed。重新 bind 不改旧报告。 |
| `projects/checks.py:25,81,154` | checker 绑定当前平台 digest；后续 current 判断继续比较，不把旧证据变新证据。 |
| `runs/manager.py:443–488,624–650` | 封存完整 `runtime/demiflow` 文件清单；最大 combined source/runtime 192 MiB。verify_seal 校验封存文件而非重新复制当前包，并比较环境。 |
| `projects/workspace.py:1720` | 非 native resume／start 的 verify_run_basis 仍要求原 report 当前且检查 complete。UI 资源令 snapshot stale 也可能阻断。 |
| `runs/native_submission.py:136–141` | native basis 比原检查所绑 workspace／snapshot 文档 digest，另检源码／资源；没有直接调用 check_snapshot。不能将两条路径的拒绝条件混为一条。 |
| `runs/worker.py` 与 `platform_worker.py` | 由当前控制器启动封存入口，子进程运行封存包；父控制器／原 worker 双方协议与环境都不能省略。 |

DemiForge development 自己还有 `development/tasks.py:131–147` 的 tree／readonly／sources 等 fingerprint 及标准／验证依据；不是所有开发记录都直接调用 platform_snapshot。C 保留这条独立工程生命周期，不能拿执行兼容 digest 替代其验收 fingerprint。

### 3.4 只读历史和恢复不是同一个入口

`dataflow_history.load_execution_version` 按原 journal 指向的 sealed_plan_facts.v1／dataflow_seal.v3 读取；`build_plan_view` 是内存投影，不调用当前编译器。未知／未来 schema 或无封存事实为 `INCOMPATIBLE_HISTORY`，hash 损坏为 `CORRUPT_ARTIFACT`，非法合同为 `INVALID_CONTRACT`，读取预算为 `BUDGET_EXCEEDED`。`workbench/runs/facts.read_sealed_plan` 转为明确 unavailable 原因；没有 facts 时声明图必须标明不是实际封存执行图。

`tests/runtime_v3_history_scenarios.py` 保存一个真正旧 producer（固定 Git `b4dd41722130be3898a8a98057541d843777906e`）的生成／读取／拒绝／原版本续行脚本；只读它的源码与 golden 本轮没有执行真实旧 producer。它区分协议／carrier 名称和引擎版本，保持旧 RunRef、seal、结果、commit／ACK，只由旧版本更新生命周期。这是后续验收的真实历史基准，不是任意历史格式已支持的证明。

## 4. 逐类变更结果矩阵

符号：`保留` 是按原 schema 有界只读、不执行用户代码；`可继续` 仅表示该类身份检查不阻断，**仍需**原 run 支持 checkpoint/final_commit、合法状态、原计划／业务代码／输入、当前授权、清理、剩余额度与同一资源 owner。`拒绝` 不伪造失败运行，不重写原摘要。

### 4.1 当前发布的行为（v1 全树身份）

| 变更 | 新编译／新运行 | 旧历史只读 | 原运行恢复 | Forge 工程证据 |
|---|---|---|---|---|
| 仅 `workbench/web` CSS／JS／HTML 展示资源 | 引擎身份不变；受管新检查需新 snapshot | 保留 | 引擎 hash 不阻断；非 native basis 的 current snapshot 可 stale；native 路径还取决于其原文档绑定及环境，不保证通行 | 包内资源变化→stale |
| 只改 Python 展示逻辑 | 当前源码重新编译；全树 revision 变 | 保留 | RunContext 原 revision 拒绝；Web conflict | stale |
| 编译规则／默认值 | 新行为依当前规则；可能 plan／对象身份也变 | 原 schema 读，不把新校验套旧图 | revision 拒绝；即使图 dag_id 相同也不放行 | stale |
| 运行内核／提交／恢复 | 当前新运行；按真实能力验收 | 保留；损坏／未知格式明确降级 | revision 拒绝，worker CODE_MISMATCH | stale |
| 数据协议／wire／codec | 新编译可能新 schema；需单独协议审阅 | 只读支持原 schema；新 unknown 拒绝解释 | revision 和协议准入均可能拒绝 | stale |
| 对象搬模块／改限定名 | 新对象 module/name/type 改；新 graph/code pin | JSON 图／结果保留；需要旧可执行对象的路径不在只读承诺内 | 先 revision 拒绝；绕过后仍可能 module 缺失、code pin 不同，不能绕过 | origin/file 变化→stale |
| 纯 repo `docs/`／tests | 平台 revision 不变（无包内 Python／资源变化时） | 保留 | 该变化自身不阻断 | 通常不影响 snapshot；若文件是被声明 readonly 依据则开发 fingerprint 仍会变 |
| 包内 `.md` 资源／标准文件 | Python revision 不变；若是运行或工程规则资源，行为可变 | 保留 | 仅平台 hash 无法检测；受管封存／snapshot 可拒绝 | stale；标准变另有标准版本语义 |
| 依赖／Python／打包 | Python hash 可不变；新能力按新环境 | 原 schema 与数据 reader 能力决定，不能承诺所有二进制仍可读 | 直接 SDK 单靠 platform hash **未覆盖**；受管 environment 常会拒绝。相同元数据下改二进制是现有 assurance 缺口 | pyproject／依赖元数据变化→stale；非元数据变化不一定检测 |

### 4.2 推荐 S2/H1 下的新记录行为（尚未实施）

旧 v1 记录始终走上表及原环境路线，**不适用下表的放宽**。

| 变更 | 新编译／新运行 | 旧数据历史只读 | 同 v2 原运行恢复 | 解释／错误分类 |
|---|---|---|---|---|
| 已证明仅静态 UI 展示 | 新应用发布来源改变；execution/control/environment 不变 | 保留 | 可继续；原 seal 完整性仍验，当前 Forge stale 不直接冒充原执行不兼容 | 当前新检查仍要求新 Forge 证据 |
| “UI-only” Python，未有逐文件闭包证明 | core 不变；control 变 | 保留 | 独立 SDK 可继续；受管运行拒绝 | `control_identity_mismatch`；不按名字排除 |
| 编译规则 | execution 变；新图按新规则 | 保留 | 拒绝；即便旧图无需重编也保守钉住编译／身份／恢复再降级路径 | `execution_identity_mismatch` |
| 运行内核／资源／提交／恢复 | execution 变 | 保留 | 拒绝 | 同上；不提供热切引擎 |
| 数据协议／codec／状态表示 | execution／协议清单变；未知 schema 新编译拒绝 | 原 schema adapter 支持才读，否则 unavailable | 拒绝，单独设计迁移 | `protocol_unsupported` 或 mismatch |
| 对象搬模块／改限定名 | 对象 pin 变，execution/control 按归属变 | 原只读 schema 保留，不需导入旧类 | 拒绝；原环境可继续原路径；不挂全局旧模块 alias | `CODE_MISMATCH`，必要时 `CORRUPT_ARTIFACT` |
| 纯 repo 文档 | 应用来源可能变，执行分量不变 | 保留 | 可继续 | 被运行读取的模板／schema／规则不是此类 |
| 包内执行资源／工程规则 | 执行资源→execution；控制／准入规则→control；纯展示→provenance | 保留 | 有关分量变则拒绝；工程验收证据另行失效 | 不把标准变化视为新执行自动获批 |
| 依赖／Python／wheel／打包 | 新 environment 或 execution 内容清单 | 已知 reader 支持才读 | 执行依赖、ABI、载入文件或未证实部署变：拒绝；只有可证明不影响执行／控制的包元数据改变才可继续 | `environment_identity_mismatch`／`environment_unverified` |

现有旧异常类别对 v1 原样保留。上表小写原因是 v2 **有界结构化 reason** 提案，映射至既有 `DataflowError('CODE_MISMATCH'/'INVALID_CONTRACT')`、`RecoveryContractError`、`ForgeError(code='conflict')`；不是未经审阅扩充冻结 wire 错误枚举。历史 unknown 仍 `INCOMPATIBLE_HISTORY`；corrupt 不降为兼容。

## 5. 推荐身份分工与闭包证明

### 5.1 四份事实不能合成万能版本号

| 事实 | 唯一 owner／保存位置 | 必须覆盖 | 消费者 |
|---|---|---|---|
| ExecutionCompatibility v2 | 引擎 identity 构造器；原 run journal／content-addressed code pin | 源码／依赖／运行资源／协议／算法清单 | RunContext、sealing、queue 合同、driver／worker／Ray、resume capability |
| ApplicationControlCompatibility v1 | 所属应用运行管理；同一 run manifest 的控制依据 | 权限／配置准入／封存／launch/resume/stop／预算和真实控制闭包 | 工作台的 prepare/start/resume；底座只消费独立 execution 身份，不反向导入应用 |
| ApplicationBuild provenance | 发布装配；manifest 的审计字段／已有发布回执 | 完整源码／资源构建 digest、包版本、Git／wheel 来源 | 展示、诊断与复现；单独不能授权执行 |
| Forge snapshot v1–v5／开发 fingerprint | Forge 已有 owner／项目已有文件或任务记录 | 原标准、平台源／catalog、环境、验证定义／项目范围 | 新检查、验证、Review、Freeze；不改成 execution digest，不改旧收据 |

不是建立四个服务或四套运行状态。以上为原记录中的不同证据，原 RunJournal 仍是执行事实唯一 owner。身份 hash 不替代权限、完整性来源或执行支持证明。

### 5.2 当前源码的闭包证据与安全的初始范围

本次探针解析全部 1,125 个 `.py`，包括函数内和 TYPE_CHECKING 导入、父包 initializer；固定字面量动态 import 纳入静态边。把 CLI 分发入口单列，底座保守源集合有 899 模块；其静态闭包仍为 899，**到 workbench／forge 的静态目标为 0**。应用控制种子 manager、run worker、native_submission、通用 worker、workspace、platform_worker 的闭包为 631 模块，其中 187 个应用模块。另有 38 个应用模块静态未达，里面仍包含 CLI、子进程、Notebook provisioner、conversations worker 等，不能当安全排除清单。

全包发现 32 个动态 import 调用，其中 27 个首参非字面量；此外 `_surface`／`_EXPORTS`／registry 符号字符串、固定子进程文件、资源读取和第三方 native 不在普通 import 图里。结果是**支持保守范围的静态证据，不是已证明的最小依赖闭包**。完整模块和文件 SHA 在 probe JSON，可复查到具体源码。

推荐第一版源集合按角色清单显式生成，而非简单目录过滤：

1. **底座执行集合**初值包含上述 899 个模块全部内容，含所有算子、SDK 声明、编译器、资源、旧历史 reader 和全部 vendored Python。`demiflow.cli` 是装配，归控制集合；包 initializer、P4 `_surface` 等即便包含上层路径文本仍保留底座集合。
2. **应用控制集合**初值包含全部 225 个 Forge／workbench Python 和根 CLI（部分业务无关 Python 因混用保守保留）。不能把 Forge 工具、worker、多用途 workspace.py、manager.py 或授权代码剔掉。所有同文件函数共用整个文件 hash；不做 AST 字节裁剪。
3. **非 Python 资源**逐一 role 清单：模型／数据协议／搜索 inventory、vendor 数据和运行模板属 execution；Forge contracts/standards、开发规范、启动 `.cjs`、部署／权限与 runtime JSON 属 control；纯网页展示资产才属 presentation。新文件或未分类资源默认包含在其较严格的 owner，若无法确定 owner 则拒绝生成 v2，而不是忽略。先纳入全部 vendor 资源，未来无收益不细分。
4. **环境闭包**以部署实际启用能力／extras＋backend 为范围，覆盖递归依赖、Python patch／ABI、native wheel 与真实执行脚本／插件来源。初期不从单次运行 trace 猜依赖。使用已有 wheel／安装清单和固定部署记录，必要时校验实际文件 hash；元数据声明只是一类证据，不可升级为内容证明。没有可验证来源的 editable 外部依赖／插件，v2 seal 明确 environment_unverified；不默认混用另一安装。
5. **封闭验收**：对每个受支持公开入口、每个种类登记符号、Local／同机 Ray、原生 source/sink、queue、thread 和 process 分别列动态加载、文件资源、子进程及环境依赖。导入／读取证据只用于发现漏项；静态完整列举、受控解析和保守集合一起证明覆盖。观测不到的分支保守纳入。外部未知 plugin 不能由 untrusted run 自声明“可排除”。

闭包命题为：允许的执行入口／动态解析合同能到达的源与资源均在 execution 集合或已封存的业务程序清单内；应用控制入口能到达的控制行为在 control 或 execution 内；第三方执行在 environment 内；唯一排除项的读取只能到展示／审计输出且不影响准入、执行、封存或恢复。调用者新增可执行边或资源时，分类检查失败，先扩大相应集合。**本阶段证明了当前 Python 保守集合的静态封闭与不能缩减应用目录的反例；非 Python／完整环境闭包尚待实现切片完成，未达到启用 v2 条件。**

这比声称“删除 workbench/forge 即安全”更保守，也比“没有全仓最小闭包就什么都不能做”更可实施。原 v1 路径继续运行；C 的未完成环境证明只阻止新 v2 开关，不影响已授权 A/B/D/E。

### 5.3 v2 内容表达和算法版本

新 `ExecutionCompatibility` 为有版本的、有限 JSON 事实：

```json
{
  "schema": "demiflow.execution_compatibility.v2",
  "algorithm": "sha256-canonical-manifest-v2",
  "scope_policy": "conservative-runtime-v1",
  "source_manifest_digest": "<sha256 of sorted path/size/sha256 records>",
  "resource_manifest_digest": "<sha256>",
  "environment_manifest_digest": "<sha256>",
  "protocol_manifest_digest": "<sha256>"
}
```

所有 manifest 原件按现有 ArtifactRef/content store 保存，字段闭合、UTF-8、规范 JSON 排序、路径用 POSIX 相对路径、不准 `..`／symlink／重复路径，排序不以 mtime 替代内容。摘要包含 schema/algorithm/scope_policy；digest 再散列完整上述文档。算法版本是解释规则；内容 digest 是精确实例，二者不可互换。改变范围算法须新 scope_policy／algorithm，不靠修改版本号字符串宣布兼容。

读取 budgets：Python 源默认保持旧 4,096／64 MiB；全部应用／资源封存不得超过现有合计 192 MiB；分节清单有记录数、单文件、总字节、深度和 deadline，64 KiB／1 MiB 小块读取；依赖清单受 Forge 现有 distributions／metadata ceilings 或更紧限制。扩大预算必须基于实测单独说明，不默增。超额／扫描中变化清楚拒绝，不缓存“上次通过”。读取期间保持描述符身份／前后状态核对；hash 的缓存只可绑定不可变文件快照，不能按 mtime 信任可变源码。

第一版 **不改变** `describe`／`plan_identity`／program code pin 的对象投影，也不搬类。ObjectIdentity 暂标为 `legacy-native-object-v1`（标签在新容器里，旧对象原值不改）；以后确需数据化稳定 type-id 单独设计，不在 C 偷加通用注册表。

新记录格式建议：runtime_identity `format=2`；新增 seal schema `demiflow.dataflow_seal.v4`、code pin schema `demiflow.dataflow_code_pin.v4`，包含 execution identity descriptor/ref 及 digest，原程序映射不变。新 format=2 中若沿用字段名 platform_revision，其值明确为上述 v2 execution digest，并以 format/descriptor 双重分派；旧 format=1 的同名字段仍为原全树 digest，绝不混比或重写。program payload schema 可继续 v3，logical／physical 的 runtime wire v3 可继续，只在证明字段／语义未变时保留；**不得往旧 closed schema 塞新字段**。HostDescriptor.runtime_revision 仍承载有界 digest，算法语义来自与 assignment 绑定的 code pin v4；Ray proof 必须按该已支持算法计算，再比 digest，不能用全树 v1 计算值冒充 v2。旧 loader 遇 v4 明确拒绝，不回退解释成 v3。

当前正常旧配置是 format=1／原 platform_revision 且无新 descriptor，选择原算法。其他已登记历史 carrier 按其原 validator 判定，不以本次新增字段要求加严；缺失旧字段不得伪造或补写。缺少 algorithm 不能猜测当前默认；format=2 缺字段、未知算法、双重矛盾声明全部拒绝。`platform_identity()` 的现有返回／字节算法保留供 legacy 解释；新函数从明确传入的协议版本选择，**不通过全局 current_run 偷换无参函数行为**。队列合同由其 parent/action 显式传 identity，仍需同批更新 validate_topology 的消费者并检查冻结范围。旧队列 protocol v2 按原字节读，新合同需新的 protocol 名，不能重算旧队列 contract。

## 6. 恢复、迁移和回退的具体时序

### 6.1 只读历史

授权与路径约束→有界读取原 manifest/journal→按原 schema 分派→校验原 ArtifactRef 和字段→显示原计划、参数、日志／commit facts／结果来源。整个路径不反序列化执行 pickle、不导入业务模块、不调用当前编译器、不“补齐”原缺失身份；unknown 保留原引用并显示 unavailable。结果表格式／reader 不支持时按资源明确不可读，不以数据不在内存冒充原运行失败。

必要的只读 SQLite／文件适配继续复用已有 run_reader／读取库；不能为了显示历史调用可能初始化表／迁移状态的写路径。验证用历史目录前后文件清单／摘要及写入审计，不只比较 plan_id。

### 6.2 原 v1 恢复

原 schema 解释→当前平台旧算法与旧 digest 比较→现有恢复条件→原业务 pin/输入/输出/commit/ACK 校验→原版本合法尝试。修改新 reader 的源码也会改变旧全树 digest；保留旧解析器不等于新 release 能继续旧运行。需要恢复时取得原发布／封存源码、依赖依据与固定数据位置，在获准独立环境中验身份；不将正式目录切回旧分支，不顺带重启当前预览。只保存 Git SHA 并不保存解释器、native 依赖或实际数据，缺失则如实不能恢复。

### 6.3 新 v2 恢复（仅 S2 获批并验收后）

1. 先当前权限和预期 run／attempt，读取原 format／schema；不创建 attempt 或预留资源来探测是否兼容。
2. 验原 seal、原 snapshot 的完整性和来源。执行代码来自固定原包／原内容引用，现编辑不替换；不会给旧 snapshot 重签 current。
3. 按身份算法核 execution＋environment；受管入口还核 control、原应用控制协议和 deployment capability。用户模型 endpoint／远端服务的真实性继续沿既有外部能力合同，不能把本地 hash 当远端行为保证。
4. 原 v2 的工程依据来自原封存；对新检查／新运行仍要求当前 Forge snapshot 和验证证据。**此处改变的是新 v2 resume 的准入分工**：不再把当前展示资源导致的 Forge stale 当作旧执行代码失配。当前权限、明确撤销／新安全准入规则仍会拒绝，不静默复用旧 grant。
5. 工作台 basis 不可简单跳过：原 entry、参数、源码／资源、授权输出、project/root、change scope、原运行合同与剩余额度按各 owner 验。将原先整个 workspace／platform digest 检查拆成“原封存工程依据完整性”和“当前操作授权／相容控制合同”，仅经受审字段清单排除当前平台展示 provenance；其他字段不得顺便放松。native／非 native 两条都验，旧 v1 保持原逻辑。
6. 原计划／状态镜像／commit／未知副作用／清理检查成功后，沿唯一 RunJournal 新建 attempt／claim fence；worker 在加载 pickle 前再次执行身份和 assignment 核验，避免父检查后源被换。校验期间变更拒绝。
7. 任何 mismatch 的诊断给有界 actual/expected 类型与 digest／原环境线索，不包含秘密，不把拒绝标成业务失败。通过 identity 仍不承诺 delivery_only／attempt_only 可恢复计算，也不重置预算。

### 6.4 模块搬迁和未来迁移

H1 不保留全局 import alias／转导入垫片，不在旧 pickle 上改 module 字符串。原历史 reader 使用持久 JSON 的白名单解码；若旧 artifact 确实必须 import 原类才能解释，就限定“原环境读取”或当前 unavailable。新对象用新路径，新运行取得新身份。

H2 将来仅接受点名 source_schema→target_schema adapter：输入只读原件，输出独立副本及 provenance、转换器版本／摘要／差异、可核对的原 digest；原目录和 journal 不变。若要把迁移后状态接到原 run 的新 attempt，还须独立设计 fence、commit、ACK、unknown／付费效果不重复与资源 owner 交接，不能凭格式转换成功授权执行。当前没有一个此类适配器获准。

### 6.5 回退

未启用 v2 写入时，撤回新 reader／shadow 计算不改变任何历史。启用后回退代码必须先盘点已产生的 v2 记录；旧版本应拒绝其未知 schema，不能把 v2 重写成 v1 来让旧代码接管。可保留支持 v2 的固定发布作只读／原环境控制，停掉新 v2 写入需明确产品能力公告；不提供两个执行循环或恢复开关。发布通过新的反向提交走发版规范，原 tags／提交／记录原样保留。服务降级与 Git main 回退是不同动作。

## 7. 后续可独立实施的切片与验收合同

这些是后续派发建议；本轮没有产品实施授权。每个切片从当时最新 main 建隔离树，先记录与本基线的差异，原作者保持设计连续性。范围变更重新审，不把本 v1 设计等同于产品已可用。

| 切片 | 范围／依赖 | 完成条件 |
|---|---|---|
| C0 当前身份报告与只读诊断 | 可在 S1 下单独做；不改 hash／guard／格式。复用现有错误，给原版本线索与已有历史读取能力 | actual/expected、算法分类及 unavailable 原因准确；报告不产生新 journal／运行；不成为 A/B/D/E 前置 |
| C1 保守角色清单＋影子计算 | D1=S2 后；新 versioned manifest builder、资源／环境闭包验证；旧算法保持实际准入 | 1125 Python 现有清单全覆盖；新增／动态资源分类失败反例；证明 core 不依赖应用；对 scope-policy／算法版本确定字节；资源有界。影子 digest 不授予恢复 |
| C2 版本读取／错误分派 | C1 结构通过；新增 format2／seal/codepin v4 的受控 reader 和新历史展示；保留原规则 | 旧 fixture 字节／摘要／结果逐项不变；unknown v4+、缺字段、双格式矛盾拒绝；读取不加载 pickle；原版本代码看到新记录明确不支持 |
| C3 全链身份传播与新记录写入 | C1+C2；用户 D1/D2；精确冻结件裁决；所有直接／间接消费者同批 | RunContext、seal、driver、worker、Ray proof、queue producer/consumer、history capability 与 host inspection 一起变；中途停留只读／shadow，不启用不完整 v2。原 v1 队列 protocol 和 run digest 原值保持 |
| C4 工作台控制身份与双 basis | C3；manager/native_submission/workspace/worker、Forge current 证据与历史依据分开 | native／非 native 的 UI-only、control 改动、explicit bind snapshot、授权撤销、父子版本不匹配均有实际入口夹具；不只测试 helper。原 v1 行为不变 |
| C5 发布／原环境恢复证据 | 固定候选与新旧组合；按 repository-rules 两闸 | 同进程／process／同机 Ray 实测，历史四线与差异；wheel/export 来源绑定；定版／预览分别处理；旧环境、原记录与新 v2 回退拒绝可复现 |
| C6 点名模块表示迁移（可后延） | 仅真实收益和 D2=H2 的具体另审任务 | 明确对象列表／消费者／历史 reader，不以 SCC／行数为理由搬；无全局 alias；有独立产物、原件保护和副作用证明 |

执行入口和 owner 保留：算法提供者只做有限身份计算、解析和比较；RunContext／RunJournal 决定引擎准入及状态；应用运行管理决定权限／封存／控制；Forge 决定开发证据是否 current。错误／取消／资源合同：扫描取消立即停止读取并关闭描述符，临时清单在私有 staging，失败不发布；源引用完整性与当前身份失配分别返回；验证 failure 不创建业务失败或另一个成功 run。没有统一 compatibility 服务或新的状态存储。

### 7.1 必须覆盖的验收矩阵（未来 C1–C5）

| 组 | 正例 | 失败／历史反例 | 证据不能替代什么 |
|---|---|---|---|
| I1 旧算法 | 固定 122edf4 与历史导出逐字节得到原 v1 digest；doc-only 不变 | 任意包内 Python 改动／增删／搬路径拒绝；格式未知不猜 | 只证明旧算法，没有新跨版本恢复承诺 |
| I2 新 manifest | 相同文件任意遍历顺序同 digest；允许 relocatable 相对根 | 资源增加未分类、缺包 initializer、动态注册符号、符号指向未知包、symlink、并发变更、超预算／取消 | AST 零边不能代替动态／子进程／非 Python 闭包 |
| I3 对象／封存 | 原对象 descriptor、program pin、逻辑／物理完整编码一致 | `None`/`{}`、同文件无关改动、module／qualname 移动、方法 code 变化、业务 helper／initializer 改动、缺原模块 | dag_id 相等不是全部 KernelSpec／程序字节相等 |
| I4 只读历史 | 原 v1/native facts、v3、未来 v4 图／结果有界读；原 RunRef／digest 保留 | unknown schema、损坏 ref、路径逃逸、超限、缺原模块时不触发 pickle；不 recompile | 测试夹具不是全历史覆盖；记录每个 producer SHA |
| I5 引擎准入 | 新 v2 同 execution/env 的有效 checkpoint 继续同 run 新 attempt | execution/env/protocol mismatch 在 attempt／claim／业务调用之前拒绝；worker 故意不同版本、Ray 节点 different revision 拒绝 | 同机 Ray 不是多机／GPU／外部 provider 证明 |
| I6 队列与副作用 | 新合同同 profile 保留 producer/consumer contract 与原 commit/ACK；原 v1 恢复同旧算法 | 只改应用不应在 queue 内残留 v1 hash；未知付费效果不重复；旧合同被改写拒绝 | 未获冻结裁决不能把 queue 写成“已完成” |
| I7 工作台双 basis | 仅展示资源变更，v2 原 seal／control/env 匹配，权限有效的受支持恢复 | UI.py 未排除→control mismatch；工程检查新旧区分；explicit snapshot rebind、参数／entry／scope／输出 grant 变更、撤权均按 owner 拒绝 | 引擎 helper 通过不能代替真实 manager→worker→sealed runtime 路径 |
| I8 P12 失败窗口 | begin 成功后 reserve 失败 marker 收尾；重试可达 | begin 拒绝不造 launch_failed／attempt；_resume 已发生的真实清理不回滚；publisher 失败不回滚源 | C 不覆盖或改写 B 的 P12 验收，只组合受影响路径 |
| I9 回退 | 新 writer 停止后仍以原 v2 release 读／控制已存在 v2；旧 v1 档案可恢复 | 旧程序读 v4 必须未知拒绝；不把 v2 digest 写回 v1；环境缺失不假称恢复成功 | 代码 revert 不等于服务／数据回退完成 |

测试均在固定任务提交／导出和私有数据域，断言实际 import 来源，记录解释器／依赖／fixture SHA、collection/execution、原失败／skip/error/timeout/not-run。受影响模块验证完成后交管理者做统一总闸，不要求每切片自己跑全仓四线。源码修订后只重跑受影响范围；全量发版按规范。

## 8. P12、P8、P2 可以独立推进，C 不是全任务前置

**普通源码改动影响旧全树身份，不等于本轮改变身份范围算法。** 已批准任务可以采用既有跨版本拒绝、保留原版本和历史只读路径；既不能说 hash 不变，也不需等待 D1/D2 才修改普通源码。

- **P12／任务 B**：按 v2.1 列出的 project/run 归属和 begin/publish 失败契约实施。平台 revision 与 Forge snapshot 变化如实记；不搬已封存的固定 worker 路径，不更改原格式／digest／按模块反序列化。若真实实现额外搬了被封存对象，逐项停下解决该对象，不扩成整批等待 C。DatasetRef 校验仍在原 helper，P4 名单预计零增减，projects 七核心与 registry/watch/flow_arguments 分别验，显式索引同步仅 CLI 的限制保留。
- **P8／任务 D**：在公开兼容入口与历史读取保护下，按已批准批次 3 范围退役确无消费者的旧实现；普通删 Python 的全树身份变化由既有拒绝承接。批次 4/5、冻结 queue_execution 的依赖裁定和按路径类的删除仍单独设计，不把旧静态“无仓内调用”变成可以破坏旧 artifact 的证据。
- **P2／任务 E**：生产仍处在告警基线；确定不变量按已准入范围逐项转拒绝，未知配置键不能一刀切。历史封存图按原版本读，不把新验证套旧记录；保留注册表键、类身份和真实编译输出约束。现有 pytest 插件只观察显式启用的驱动，不能声称覆盖所有 worker。算法不变时无需等待 C。

三者交付都列平台／对象／封存／历史读取／恢复／Forge snapshot 影响与原版本恢复线索；只要未改变算法、持久表示、模块路径加载或兼容承诺，就可走各自现行验收与发版。C 的闭包／环境／冻结件实施缺口只限制 C 的 v2 启用。

## 9. 本阶段实测、保留／修改清单与未验证项

修改仅 `docs/architecture-upgrade-20261120/C/` 的设计、意图原文副本、探针、JSON 证据；没有产品源码／测试／冻结件／AGENTS／业务文件／环境变更，没有 merge／push／部署。旧算法、对象描述、封存 bytes、历史记录、原标签及在线服务全部保留。最终 `git diff --stat` 和全文件 SHA 在交接中。

实际运行：

1. `identity_probe.py`，真实导入本树、Python 3.11.16，实际执行旧 `platform_identity` 和 `source_inventory` 于私有临时假包。断言现有 UI.py／compiler／kernel／protocol／Forge.py 修改都改变 v1；CSS／包内文档不改变 v1 但改变 Forge 源清单；pyproject 不在 v1；搬文件改变 v1。函数全文 source_hash／类 module 变更／缺原模块 cloudpickle 反例均按预期。静态清单包含文件 SHA 与所有直接导入消费者。
2. 10 项已有小型夹具检查，**10 passed，12.18 秒**：W002 历史恢复 capability、W003 历史视图、W013 SDK 拒绝、W014 Web 拒绝；Forge source drift、relocation/provenance、扫描中变化拒绝、sealed installed metadata、依赖闭包／extras、v1 snapshot 保留原 catalog／standard。原始 pytest.log／JUnit 位于共享 design-v1 evidence 子目录。W013/W014 的通过下游用替身，因此这里只证明前置拒绝合同，不称完整恢复已验。
3. `runtime_v3_history_scenarios.py`／prompt-identities.json 只读分析，未执行其旧 producer／续行；没有真实业务／付费模型／Ray/GPU 运行；没有测试未来 v2 实现，因为本轮没有该实现。

以上测试源 SHA 为 `122edf4bd0c827338846abd0114f0b36cf8c1b6d`；设计提交只新增文档／探针，运行源码与该 SHA 逐字节不变。正式合入闸／四线／浏览器不在设计阶段执行范围，均记 not-run，不使用这 10 个用例替代总闸。

待做项分层：

- 用户承诺待定：D1 的 S1/S2/S3、D2 的 H1/H2/H3；推荐 S2/H1。冻结 queue_execution 仅在未来 v2 队列传播切片的具体范围审阅后才可改，不影响设计交付。
- 技术尚未证明：完整非 Python／native 环境闭包、实际新 envelope/writer、受管双 basis 字段拆分、跨应用发布且执行身份相同的端到端恢复。实施阶段按清单验证，不能在本阶段写为通过。
- 设计评审待定：Fable／总控尚未审阅本版本。作者未调用 Fable，未擅自实施；交接后由父协调安排同主题会话。
- 可后延：最小逐算子依赖 footprint、稳定 type-id 改造、模块全量分包、任意旧版本迁移、工作台 Ray 提交。不存在“所有现有失败清零才可设计”的前置。

## 10. 固定依据与复查入口

共享包根：`/yzp/zhaozy/yangzepeng/0905/_demiflow/shared-review/architecture-upgrade-20261120/`。

- 本轮原话与权限：USER-AUTHORIZATION.txt、CONTEXT.txt、TASK-C.txt、REVIEW-HANDOFF.txt。
- 完整意图与原始档案：context/01-intent-message-block.md、02-original_task_intents.md、17-root-intent.json；本设计只摘出影响 C 的落点，完整固定原件仍是审阅依据。
- 体系／共识：03 AR v1.3、04 全局架构、09 架构 v3、10 P12 v2.1、11 Fable 通过、15 P8、16 P2 v4、19 P2 GPT 共识、21 P1 复审。
- 正式约束／当前版本：本树 AGENTS.md 与 docs/repository-rules.md；context/13-publication.json、14-CURRENT.json；旧暂停／旧候选／日期草案只作历史。
- 机器证据：同目录 probe-results.json；完整上下文 SHA 固定在共享 context-manifest.json；最终设计文件 SHA／候选 SHA／clean 状态固定在 tasks/C/design-v1/handoff.json。

评审请重点裁定：保守分离的收益与成本是否合适；queue 冻结消费者、受管 snapshot/basis 的额外拒绝链是否完整；新旧算法分派和 schema 组合是否不重写历史；“静态已证明／实施待验证／用户承诺”是否清楚分离。结论只针对设计候选，不替代用户 D1/D2 或正式发布总闸。
