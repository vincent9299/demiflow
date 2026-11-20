# G 声明层逐边收敛设计 v1

结论：18 条导入中推荐保留 17 条，仅媒体 provider 被动校验（E09）有一个小而可测的替代切片 G1。声明/实现混放的三处位置和 P1 六类运行对象继续保留，明确职责和守卫，不移动序列化类、冻结件或历史记录。没有需要用户新增裁定的兼容承诺；G1 是后续可独立评审任务，不是当前获准实施，也不作为首批发布前置。

固定基线：`main` / `integration` / `main-20261119-3` 均为 `122edf4bd0c827338846abd0114f0b36cf8c1b6d`。本树 `gpt/design/declaration-edges-20261120` 从该 main 新建；不复用 C 树。G 只写本目录设计/证据，产品、测试、依赖、打包和冻结七件未改。

## 授权、意图和依赖

最新真人“你先自己review吧”改变审阅安排：本轮由 GPT 评审，Fable 未完成不能说成通过。父协调对 C 固定 `c2f643083834014d7f40e4c76c9ebd5c4a94883a` 的对应普通源码/历史边界、对 F2 固定 `e0c3d7d9a18b729504707928a55a3257a50f152f` 的服务合同已通过；对应 receipt/hash 在 source-inventory.json。F 材料中的 pending 字样是原交付历史，当前状态以父协调 receipt 为准。S2/H1 新身份方案、具体冻结范围、跨版本恢复/迁移、F1工具归属均未在此获准；G 不等待 F1，也不默认为它选择新方案。

本设计沿用 C 的现行 S1：普通源码变化可能改变旧全树 platform identity，旧运行只能在原精确版本/环境按原规则恢复；新平台遇旧 revision 拒绝，原产物/历史只读保留，不重算 digest。P12、P8、P2 等沿用这些约束可以独立推进，C/G 不是全任务前置。对按模块路径序列化的类/冻结件则不动，不把普通文件变化与身份算法范围变更混为一谈。

对原始意图的贡献：支持 R.2 检查/运行/历史语义、R.3 固定数据来源、R.5 职责与资源保护；不把减少 import/SCC 数作为用户收益。完整 root、15 叶与贯穿原则在 ORIGINAL-INTENT.txt，逐字复制派发，不压缩改写。AR v1.3/全局架构是职责意图；架构 v3 五层图/清环建议不是已确认硬规范。

## 清单复核与口径

- 架构 v3 §4.2：9 条闭包内通向原 anchor SCC 的函数内导入，3 条种子跨包顶层导入，6 条通向环外模块的函数内导入；合计 18 条语句。独立 AST 在当前 main 找到全部原位置、提供者和符号；所有对应源文件 hash 与 F G-input 一致，无新增/消失条目。另从 G1 上游发现 parser:151 的 IMAGE_RESPONSE_SCHEMA 导入，明确作为邻接 G-ADJ-01 单列，不能并入旧18条口径或遗漏同批迁移。F 的 imported_names 是规范化名称集合，源码行内次序另存。
- 这些分组保留原 v3 的诊断标签，G 没有重算 SCC，也不把旧 297/263 静态模拟称为当前实现结果。复核的是完整指定清单，不宣称发现所有动态 import 或包初始化副作用。
- 三处混放：local_kernel 内三个关系声明类；data.local_relational；data.result_cache。最新引擎导入后两者分别是 2 条语句/10 个符号、6 条语句/9 个符号；与 v3 的 10/9 符号口径分开，没有据此虚报数量减少。
- P1 六类：QueueProducer、QueueAcknowledge、RawFetch、LanceLookup、StreamAdmission、StreamLanceSink。CacheCall/CachedMap/CachedSyncMap 属另列混放位置；它们不能冒充“环外六条”或追加第七个同类迁移目标。

[EDGE-TABLE.md](EDGE-TABLE.md) 给全部 18 项的可扫描结论；edge-decisions.json 每项均有真实 provider/consumer、输入输出/错误、owner、状态/资源/并发、身份/封存、旧入口处置、验收负例。保留原入口是一项明确决定，不是遗漏删除工作。[RETAINED-OBJECTS.md](RETAINED-OBJECTS.md) 单独覆盖三处混放和六类的所有字段。

## 从调用链得到的关键判断

1. 类型和值共享是合理方向。PromptPack、LanceQuerySpec、ResolvedWheel、PipelineExecutionTarget、PromptContractDiagnostic 不该为了形成“叶子包”复制或挪类。真实固定版本/对象校验比影子 Protocol 或 dict 更强；类方法/source 身份同样不能仅凭 module/qualname 不变就声称不变。
2. 声明目录内有真实 IO。LanceScanSpec 构造会通过 URI 规范化读取迁移 manifest；get 可以生成派生 SQLite，get_readonly 不等价。save_manifest 是明确持久化入口，调用者持有 run 事实和布局；G 不把它改叫纯函数或删除。PipelineBundleRef 发现与 digest 会读目录/源码，原代码也并非每个入口都有硬 byte/count 总界。保留这些边时如实记录现有预算与未保证项，不扩出无关预算改造。
3. 具体发现是 AST 读取。contracts.pipeline 引用 discover_pipeline_definition，后者经 inspect_pipeline_definition 读 AST；它本身不执行业务 import。真正 load_program 才 import 并实例化 PipelineProgram。F 的“不能把业务 import 当扫描”是注意事项，不是本边实际执行业务 import 的证据。
4. Pipeline.run 是显式运行入口。ProgramContext + execution_scope/_use_executor 的 lazy import 正在履行装配职责；删掉或搬到新 launcher 会增加 owner，不是边界改善。F2 的真实 journal/view/capability 注入继续原 owner，G 没有引入万能 runtime service。
5. 被动校验和 import 成本要分开。实测 image provider 校验新增 PIL/media 模块，部署校验经 services.__init__ 新增服务/执行支持模块；Thread.start、Popen、socket.connect 捕获均为零。前者独立校验很小，只有一个产品调用点；但真实 parser:151 还引用 media 的固定 IMAGE_RESPONSE_SCHEMA，必须同批把唯一常量归已有 model.py；后者涉及 package initializer 和多个嵌套政策，简单搬 import 没有真正净化，保留。
6. artifact.manifest_digest 的 JSON 编码不是 Forge 紧凑编码。manifest_id、storage sha、外置内容及 Verify 的用途不同；不为低层依赖改 hash。P1/P2 同一原生配置/状态分类规则也不复制。原 shared policy 属合理依赖，不因模块名有 execution 就判断必须删除。

## 身份、恢复与可实施性矩阵

| 决定 | 新编译/检查 | 旧历史只读 | 原版本恢复 | 新版本恢复旧 run | Forge snapshot |
|---|---|---|---|---|---|
| 本 G 仅文档/证据 | 产品树完全同 main，算法/输出不变 | 不读写真实历史 | 不运行、不改变准入 | 保留原拒绝 | 本设计不刷新/adopt |
| 17 边、三处混放、六类保留 | 原入口/类/参数/副作用不变 | 原值/路径保持 | 原条件仍适用 | 不新增承诺 | 不变更算法 |
| 后续 G1 窄函数转移 | 合法/非法配置和诊断等值；不加载 PIL/media；其他调用仍沿原实现 | 原 manifest/run/report/对象字节不重签；当前 reader 保持旧格式 | 留存 producing version/环境，按原可恢复性核验 | 普通源码平台hash不同仍拒绝；不可用 adopt 替代 | 新源码可能 stale，按原 adopt/check/new prepare，不改旧 seal |
| 将来搬类/换表示/碰冻结（未选择） | 必须精确对象/载体另审 | 未承诺自动迁移/alias | 未承诺 | 不授权跨版本恢复 | 不把新快照伪装原版本 |

G1 原/新模块函数身份变化只限所列内部 validator 迁移以及三个文件的 source_hash 连带影响；真实计划对象/类体不搬。实现阶段必须枚举实际 changed-source callable 和派生 IDs；不能用 shape-only 比较代替历史身份核验，也不允许刷新冻结快照掩盖变化。后续材料绑定该实现的 T，若文档另有 R，提供产品/tests/资源/依赖相同证明。

## 排序与后续任务

G1 是唯一有明确小范围收益的后续卡，详见 [FOLLOWUP-CARDS.md](FOLLOWUP-CARDS.md)。不新增模块，删除原 media validator/固定schema定义及旧导入并同批迁两个直接消费者，不留永久转发。此设计不启动它的实施；父协调可按已批准范围另派。

其余保留项由现有 P1/协议/固定版本/资源/恢复测试守住：只在邻接改动会触及合同或守卫缺失影响真实结果时补精确负例，避免为18行清单再造庞大测试体系。导入 guard 证明依赖形状，行为/资源/历史各有不同证据，不用一张静态图证明所有契约。已存在测试失败/skip不自动算通过。

## 验证、局限与交接

已完成：专属树与 main 基线核验；18条×实际AST/源码hash与F输入对账；真实调用/模块/类/冻结七件 inventory；小型真实被动配置与完整 parser 探针（plain/image/deployment接受，bool timeout拒绝，完整图片pack保持固定schema，记录加载模块及阻止资源启动）；两个既有 P1 guard文件 23 passed、0 failed、1 warning（禁自动pytest插件后既有 asyncio_mode 未识别），1.18秒 pytest时间。详细命令/解释器/私有资源目录/输出见 baseline.json，probe源码和结果同目录。没有付费模型、业务run、Ray/GPU、生产数据或在线服务。

inventory首个保存失败是 F 的 imported_names 顺序与源码书写顺序不同，修为按集合核对并保留真实 statement；原 AssertionError 日志仍在，不把它算产品回归。另一个最初扫描调用未留下完整终态，已记录为无成绩；最终脚本独立完整成功，未把不完整调用计入通过。

没有完成：G1实现/新无PIL验证、真实新旧版本运行/恢复、全部动态或外部消费者穷举、资源RSS硬界测试、业务浏览器/四线/合入闸/Fable。这些未验证不会成为17条合理保留的虚构阻塞，也不作为未实施G1的通过证据。来源/authority/manifest摘要和原始记录齐备，原始长期意图与最新执行状态分别引用。

所有交付仅本分支 docs/architecture-upgrade-20261120/G，外部交接 tasks/G/design-v1/handoff.json。不改 C/F/B 树、INDEX/CURRENT，不合流/push/发布。原作者继续保留 G 后续修订责任。
