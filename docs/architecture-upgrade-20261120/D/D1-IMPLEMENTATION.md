# D1：条件退役的最终保留交付

2026-11-20。本轮获准范围已经完成逐对象处置：**两个候选均保留，产品删除0项。** 这不是“批次3已经退役”，也不是缺门槛仍批准删除。父协调在低成本证据评估后明确同意按原始收益原则保留，不申请Ray窗口、不新建跨进程测量体系；D不拖A/P12。完整原始root/15叶及取舍原则保持在本目录 `ORIGINAL-INTENT.md` 原文；本轮结果不改写它。

基线 `main@122edf4bd0c827338846abd0114f0b36cf8c1b6d` / `main-20261119-3`；设计提交 `afd2e2b8905d1953ad0d17b5c0cb63c72811e67e`，GPT父审通过的是条件方案，不是删除资格。当前实施授权为共享 `tasks/D/IMPLEMENT-D1.txt` 及父协调本轮保留裁定。Fable未完成，不冒充独立通过。

## 逐对象结果

| 对象 | 最终处置 | 明确未满足的删除门槛 |
|---|---|---|
| NativeCommitStore / native_commit_store.py | 原19行及9方法全部保留；retention只补保留依据 | 4当前可靠跨进程产品测量；5受支持历史/外部类型依赖未证明；9删后历史对照；10真实Ray重复4/9 |
| chain.unified_chain_eligible | 原函数与chain全文件字节保留；retention只补本次单符号评估依据，旧batch4字段不重写 | 4当前跨进程产品测量；9删后封存/代码固定/历史对照；10真实Ray重复4/9 |
| timeout.py | 原文件和三个 `__all__` 导出保留 | 第8条公开导出条件不满足，不先删导出来制造资格 |
| dataflow_control.py / H045–H048 | 源码和四用例原样保留，既有逐断言映射继续适用 | wrong-reply、store、identity owner等未等价；不能把fake transport/同进程mailbox说成真实Ray |

两候选各有独立处置提交和完整十一条表，见 `D1-NATIVE-COMMIT-STORE.md`、`D1-UNIFIED-CHAIN-ELIGIBLE.md`；机器清单见 `D1-OBJECT-GATES.json`。所有 `scoped_fact` 仅表示已声明范围的静态事实，不表示十一条已全通过。没有直接测试可删，原测试全部保留。

## 低成本复用评估及停止依据

已找到P8修正版 `/tmp/demiflow-claude-p8/site/sitecustomize.py`，只读保存原件和摘要，没有运行。其三个旧缺陷确已修正；但每函数达到400次walk或48个来源便停止记录，打开/写日志失败静默，无声明PID、开始/完成/崩溃对账。它不能直接证明当前所有子进程均完整观察，也不能把有限采样零命中当无产品调用。

登记历史夹具可以复用 `test_runtime_v3_history_protection.py` / `runtime_v3_history_scenarios.original_native_history`，原Git版本b4dd417的生成/读取/拒绝/原版本继续运行有明确预算；其旧进程会将PYTHONPATH替换为原package，不会自然继承当前site监听。原Ray记录只有43文件的模块加载，没有函数级测量。当前真正私有Ray fixture有address=local、6CPU、100MiB object store和唯一临时根，装配本身可复用，但不是当前两对象的已完成调用/删后历史证据。Ray worker还使用独立runtime_env，主进程安装监听不证明worker被观察。

要补全删除资格，需要验证每个相关进程的监听安装/覆盖、源绑定、完成/崩溃与日志错误对账，并做候选前后Local/Ray历史及chain身份对照；这超出删除少量旧说明/序列化探针的本轮收益。父协调同意停止，不再补合成探针、环境探测或实际Ray运行。不是以skip、未到窗口或旧回执当通过；没有申请窗口、连接或停止任何集群。

## 事实刷新与证据边界

在父协调免除进一步扫描前，已依初始实施派发启动现有三个有界只读scanner；均正常完成，未在裁定后再扩扫。源码固定afd2e2b，四目标文件与base及旧74d2f99相同；NativeCommitStore无包导入者，eligibility无当前token消费者，chain仍有7个包内导入者，timeout公开导出仍在，冻结七件摘要一致。

业务范围沿用先前批准的demiwtg/pan123及24登记源根。demiwtg HEAD8cdab2b、561项dirty，878份tracked源码/相关配置（749py），347份缺失tracked项；pan123 HEADeba20e9、1项dirty，只读2份源。登记现役根补扫720份py均为untracked，7,533,727字节，集合SHA256为93b0508c00a4bc944385d7a42db7d8694b1d702375199c6b7afdcbf938a0e2f9。目标零命中，HEAD/status/实际文件清单均与设计证据相同；没有导入执行业务、读取数据/秘密、恢复缺失文件或更改dirty/staging/Notebook。

未覆盖外部/未知消费者、排除历史根、Notebook、未登记源根、非字面量动态模块名和生产载荷。零命中不外推全业务。pan123仍未见相关平台消费者。旧context manifest中的三份live授权文件在最新GPT授权后已更新，scanner如实记录三项不匹配；这是A审已登记的来源差异，不回改旧索引。本轮读取最新授权并单列当前hash，原意图/架构材料保持。

共享原始证据在 `tasks/D/implementation-v1/`：feasibility-assessment、原监听器副本、scanner-provenance、baseline-source/references、business-source/registered-source、refresh-interpretation、最终固定提交与测试回执。设计34项有限基线保持原122edf4绑定，不冒称本轮重跑、当前调用测量或删后验证。

## 变更、验证和身份

本轮仅新增D1处置文档/机器清单并更新retention中这两个对象的reason。其余ledger状态、batch、users/importers、旧measured字段、removed历史逐项保持；未改产品和测试Python、GUARDS、七冻结件、golden或历史夹具。最小相关验证为现有 `test_pre_v3_retention.py` 的6项源码守卫，固定候选执行的实际结果见最终HANDOFF，不将文档说明当测试回执。

无包内Python变化，所以现有platform_identity源码输入、对象module/qualname/source_hash和chain全文件均保持；不是对一般源码删除也声称身份不变。封存格式、读取者、恢复拒绝、原版本选择与历史只读规则不改；没有生成或覆盖真实历史工件。Forge平台snapshot与项目watch仍不同，本轮不重写业务认证，文档/测试文件可能影响具体项目watch，不能一概声称所有Forge任务快照不变。

未跑当前调用测量、删后历史、Ray、四线、真实业务/模型、GPU/多主机、服务或浏览器。无产品删除无需为删除资格继续重型验证；这些not-run不计通过。普通JSON、diff/清单与输入字节对照加相关6守卫足以核验本次保留文档/ledger变更，发布仍由总控按正式规范另行处理。

## 批次4/5保留边界

`BATCH4-5.md` 仍仅限定设计，本轮未实施其任何符号退出、搬移、解冻或替换；批次0–2不重做。保留WorkerChainExecutor等旧worker待定闭包、bootstrap活v3分支、RayControlConnector及继承关系、chain全部活代码固定工具/两个兼容入口、其余历史符号、StreamStats/has_stream、PlanContractError/v1路径和必要历史类型。

冻结 `write_queue_datasink` 及stream_view/Dataset._run_stream相关依赖仍默认保留；后续如需退役冻结死函数或改译公开语义，须独立范围裁决/审阅，当前没有真人选择，不因休息推定。既有方案A/B/C及真实Local/Ray失败/恢复验收仍待后续任务，不成为本轮D1或A/P12前置。

当前无新增必须真人裁定项。后续若再考虑删除，必须重新固定对象/消费者/证据，补齐十一条；本轮保留结论不是预先授权删除。未操作其他树、正式目录、integration、CURRENT/INDEX、环境或服务，未派下级、Fable、合流、push、部署。
