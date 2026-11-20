# E1 实现与证据交接

2026-11-20。设计基准 `26af640628977b56f070e6f9eb923bed7c34f60f`；main / main-20261119-3 基准 `122edf4bd0c827338846abd0114f0b36cf8c1b6d`。实施授权为共享包 `tasks/E/IMPLEMENT-E1.txt`，父协调已按最新真人“你先自己review吧”完成 GPT 设计审阅。Fable 因额度中断，未完成独立通过。

E1a/b/c 已实现，候选提交交父协调终审，未合流、发布或切换预览。**生产仍只告警，E2 没有实现，也没有 enforcement 开关或可绕过模式。** 本文件补充获批 DESIGN.md；该文件中的原始 root、15 叶、取舍和原文保真规则保持逐字原文，未改写。

## 行为与责任对应

| 获批要求 | 最终实现 | 关键验证 |
|---|---|---|
| 完整扫描 total，而非只修 shape predicate | 原配置模块 `_scan/_findings/_node_findings` 同一规则源；native/publication 的集合成员判断先查文本；lance 引用先查非空 str；依赖形状错误只跳过关联规则 | 28 compiler-owned 字段 × 15 finite JSON 形状；lance `{}` / `[{}]` / `[]` / None / 数字 / bool；本节点与后续节点其他 findings 保留 |
| 真实内部异常不能假零 | 每节点非预期异常给 `check_error`，保留已产出的 finding 并继续后续节点；扫描索引异常同样记录 incomplete | 注入 shape 后真实 checker 异常；partial、terminal finding 与 check_completed=false 同时存在；异常正文不入日志 |
| 稳定分类与安全定位 | `_Finding` code/category/stage/node_kind/kernel_kind/occurrence/key；unknown key/kind 固定哈希；carrier_pair 仍 observation，并检查全部出边 | 未知键仍告警；256 字符合法键边界；不同边序同结论；秘密值不出诊断 |
| 成熟阶段、单一判定源 | `capture_graph` 在 capture、graph、budget、final 调 `report_configuration_stage`；整数边索引映射为 occurrence 对的惰性视图；final 包含 shared 且在 validate_graph 后、on_compiled 前 | 真实 compile callback 次序；configuration 对象未被报告复制/修改；完整语料阶段计数；共享 recovery actor 补全 SHARED_KEYS |
| 公共兼容返回与生产有界报告 | `configuration_findings` / `report_configuration` 保留 tuple[str]；checker error 现在显式非空。编译器改用无完整返回列表的 stage reporter | 全量返回、20 样例加剩余数；真实坏 shape/unknown 新图仍能编译返回，不新增拒绝 |
| 驱动插件如实观察 | 原 opt-in `configuration_schema_gate` 改读结构化 summary；只数 final 为 graph，阶段单独计数；父 PID handler 不推断子 PID | 保留确有反例的 reports_on_purpose；普通不编译单测仍允许零图，插件不宣称是全进程遥测 |
| 有限显式进程证据 | `configuration_schema_evidence.ProcessEvidence` 只有显式 with 安装 handler；每 case 固定预期 compiled/拒绝 code；完成阶段、进程角色、源码与 PID 绑定；退出 scope 后显式 write | 真实子进程 clean/finding/checker-error/编译后缺 completion；继承 handler 不能完成子进程 receipt；缺 case、缺 stage、无 scope、零 cases 均失败 |
| 固定语料不追绿 | `observe_corpus` 调原 compile_case/capture_case，用既有 baseline 清单和拒绝 code；另以原 compare 全字段对照 | 348 direct + 30 captured；323 compiled / 55 refused；全字段差异为空，函数身份夹具与 golden 未改 |
| 已有业务证据分层 | 只读既有 RefGen 迁移报告及七流程回执，记录可用事实和缺口 | 见 business-evidence.json；不重新运行真实业务/模型，不将旧版本与未观测子进程归为零 |

实际修改仅两份产品模块：`demiflow/planning/dataflow_configuration.py`（规则与报告 owner）、`demiflow/planning/dataflow_compile.py`（阶段调用者）。测试修改原 schema 和 opt-in gate，新增 evidence helper 与针对性测试。没有新增 registry、执行器、持久账本或后台任务，没有第二份配置规则表。原最终 `report_configuration(graph)` 编译调用已删除并改为同源有界 stage reporter；公共 wrapper 留给现有调用者。原 driver 插件解析消息字符串的逻辑已删除；不再依赖 “nodes checked” 文案。没有待删的产品旧路径。

## 固定源码验证

原件位于共享 `architecture-upgrade-20261120/tasks/E/implementation-v1/`。每轮原始 log、XML、receipt 保留，含完整命令、interpreter、依赖版本、PYTHONPATH、实际 import 路径、私有目录、timeout、源 SHA 和 clean 状态。私有资源以 `/tmp/demiflow-e-*` 隔离，既有 env 不改。先提交后测试，无 dirty 产品代码套用旧 SHA。

| 回执 | 固定源 SHA | 实际结果与后续处理 |
|---|---|---|
| e1a-01 | e23e9565504e583bff7537f39aa9f935a067b3b4 | 原四文件 56 passed / 1 failed；新隐私夹具的键超过 DTO 256 字符，失败发生在进入 checker 之前。产品逻辑未追绿更改 |
| e1a-02 | 5fa62c3ca87d13ae30e566ad87d94c3c51eff576 | 仅修夹具至合法键长，配置文件 36 passed |
| e1b-01 | 3bd47091e12a146115512976760945a9ab8fb4a6 | 78 个 JUnit testcase，3 个含失败；终端 14 failed / 76 passed：2 个新 ActionRequest 夹具漏 sink/programs，另为已知 cold-import 单测的 12 个 subtests。schema/主语料/inventory/compile inputs/依赖 ratchet 与其余证据测试通过；不称整轮通过 |
| e1b-02 | ee128b04b70f94b23f941d922c4af869bdbd7787 | 补齐直接请求夹具和 limit，证据文件 11 项及原 legacy reduce grant/seal 用例 1 项，共 12 passed |
| e1b-03 | 9ba87378a0f6e3db62072b16c6874382a81e8b94 | 增加 inherited PID 负例、将 missing-completion 改为真实编译后退出；证据文件 12 passed |
| corpus-01 | 9ba87378a0f6e3db62072b16c6874382a81e8b94 | 固定原 baseline root，378 case；323 compiled / 55 原拒绝；图 wire hash/字节数/dag、声明、报告绑定、标签、拒绝完整文本零差异；capture/graph/budget/final 各354，0 findings / 0 checker errors；明确唯一 compiler PID 完成回执 |

`e1b-01/receipt.json` 的自动计数保留原值：它的 failures 统计 XML failure 节点含 subtests，passed 统计 testcase 元素，不能互加当同一口径。`source-and-receipt-audit.json` 对原 XML 和终端原文分别列出，不覆盖历史回执。公开冷导入失败由任务包 CONTEXT 第四节第7项登记，E 分支相关测试、data/__init__ 与 exports fixture 相对 base 无差异；交父协调/任务 A 处理，不在 E 修改或豁免。

产品源码自 3bd4709 起未改，ee128b0/9ba8737 仅修补新增测试；最后提交只含 E 文档。共享 `validation-binding.json` 将每轮实际源 SHA 的所有产品 .py 与测试输入 blob 逐项对照最终候选，避免把后修测试说成已在前一轮通过。7 冻结文件和 merge_gate/GUARDS、golden、函数身份夹具均未改。建议父协调将 `test_dataflow_configuration.py` 与 `test_configuration_schema_evidence.py` 的正常/负例纳入后续合流守卫；本作者未改 GUARDS。

## 资源、失败和历史

扫描保留 occurrence 索引和全部出边消费者 O(V+E)，固定四次成熟扫描。shape 检查还会访问配置序列/成员；其成本按实际访问的有界 JSON 大小计，不把包含大 members 的单个键当 O(1)。本轮无排序、无配置/整图序列化复制。每阶段生产只保留20条样例和有限 code/category 计数；公共兼容 tuple API 仍有 O(F) 全量结果空间。原 DTO JSON 深度/键数/长度和 compiler 节点/边 bounds 保持，未新增缓存或 worker。预先存在的 typed-field lru cache 未扩展其合同。

证据每进程最多4096个声明 case，每 case 固定四阶段、每计数分组至多64个短 code；最多汇总64个明确 PID。单 receipt 磁盘预算8MiB；超限写明确 incomplete 小回执，manifest 超限在安装前拒绝，绝不截断成通过。8MiB 是文件预算，不是进程 RSS 承诺；JSON 编码前存在上述有限计数表及编码缓冲。每次汇总顺序读取一个至多8MiB receipt，不积存所有原件。文件使用独占新建，已有结果不覆盖；handler scope 退出恢复 level、移除自身 handler。子进程崩溃/取消/超时无 receipt 一律不完整；工具不接管或终止其他任务进程。

过程记录仅 fixture 名、固定拒绝 code、计数、源路径/提交、PID/角色和明确角色依据；不复制配置值、模型正文、异常正文。不是安全签名/远程遥测协议，外部调用者必须提交本轮明确进程清单并将真实 case 结果交给 observer；任意伪造回执不属于可信测试前提。角色 not_a_compiler 必须声明依据且不允许有 compile 观察，不能等同“0图通过”。顺序 observer 不支持线程交织的并发编译，遇到顺序冲突视 incomplete；本轮固定语料为串行。

普通平台 .py 变更会改变现有 platform revision；没有声称 hash 不变。DTO、codec、factory module/qualname、native_model_declaration 和写入配置不变，完整固定语料字节等价。平台 revision 与 Forge snapshot 仍分开，跨版本恢复拒绝/保留原版/历史只读按既有规则，不改变身份范围或承诺。schema 未接入 sealed 读取路径，两个历史声明重编译入口仍仅告警。原 legacy reducer 用例验证原 grant/seal 保持；未将此单例夸大为完整跨版本恢复验证。

未运行四线总闸、Ray/GPU、真实模型与业务、浏览器/在线预览、跨版本恢复全套、max_nodes/max_edges 最大规模压力或 RSS/性能基准。主语料中真实小 bounds 拒绝保持，但不代替最大规模压力。E1 最终审阅、合流 gate 和发布四线由父协调/平台总控依规范执行。

## E2 尚缺的证据

现有本轮语料已覆盖四成熟阶段，完整编译产物对照已完成；本轮补充 shared/recovery actor 是编译夹具，不是业务运行。RefGen 历史七流程和10个 Dataset/Lance/恢复测试属于旧源码 e993cfd 且另有 dirty UI，没有本次阶段、checker completion 或明确子进程观测回执。它们只帮助确定业务范围，不能成为本候选零报告凭据。

仍需父协调在原授权范围内明确必需真实管道/配置/执行角色；每个会编译的实际进程绑定同一候选和四阶段 completion，未编译角色给实际调用链依据。缺此证据时保留生产告警。E2 的确定不变量允许集合、坏新图在副作用前拒绝、两个历史声明重验入口显式 report-only 和历史负例仍须另行实施/审查；当前交付不含这些变更。没有新增必须叫醒真人的 E1 决策。
