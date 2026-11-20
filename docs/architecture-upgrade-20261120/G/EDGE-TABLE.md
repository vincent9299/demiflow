# G 声明层逐边决策（固定 main@122edf4）

18 条逐项如下；每项的输入输出、状态/资源/并发、历史与负例见同目录 edge-decisions.json。编号保持 F G-input，分组是 v3 诊断标签，不是新规范。

| ID | 当前位置 / 提供者 | 决定 | 原因 |
|---|---|---|---|
| 01 | demiflow/data/plan.py:12 → demiflow.operator_llm.model | 保留 | 迁出 PromptPack 无业务收益，反会改变序列化类型；保持 F2-PROMPT 原 journal/view 注入。 |
| 02 | demiflow/data/sources.py:9 → demiflow.lance.model | 保留 | 另造声明影子会令真实校验/路径/数据身份分叉；保留同一个构造规则。 |
| 03 | demiflow/execution/contracts/pipeline.py:16 → demiflow.pipeline | 保留 | 包级入口还承载既有兼容导出；改变 import 名本身无实际所有权收益。若后续治理包初始化，须单独量实际加载并同批更新出口。 |
| 04 | demiflow/data/native_options.py:35 → demiflow.planning.dataflow_policy | 保留 | 提取副本会分裂 CPU/GPU/自定义资源规则；现 P1 冷声明路径已不加载运行 owner，SCC 边不是必须删除的执行耦合。 |
| 05 | demiflow/execution/contracts/artifact.py:204 → demiflow.execution.artifacts | 保留 | 替换 Forge/content_digest 等近似 helper 会直接破坏旧内容引用；归属已清楚，保留窄函数导入。 |
| 06 | demiflow/execution/contracts/artifact.py:221 → demiflow.execution.file_ref | 保留 | 仅两个旧链路 owner 的兼容 helper，搬五行会改既有调用而不会减少状态 owner；P8 决定旧链路未来，G 不替 P8 迁移/删除。 |
| 07 | demiflow/lance/storage.py:36 → demiflow.execution.location_index | 保留 | F2 中立 location 合同允许合理依赖；换目录只会扩大身份/消费者范围，没有已证收益。 |
| 08 | demiflow/lance/storage.py:54 → demiflow.execution.location_index | 保留 | 保留已存在中立定位服务，不造第二位置注册表。缓存坏可原地重建派生物，不得修改源或返回不同表。 |
| 09 | demiflow/operator_llm/model.py:37 → demiflow.operator_llm.media | 替代（仅后续 G1） | 35 行左右独立校验只用 stdlib，唯一产品调用点明确；去掉配置检查对图像库的加载有具体可测收益，成本小。不是为了降低 SCC。 |
| 10 | demiflow/operator_llm/model.py:53 → demiflow.services.deployment | 保留 | 保留一次唯一规则。只搬 deployment_config 仍经 services initializer 加载 owner，不能虚报已净化；完整包初始化拆分比 G1 广，暂无需求/收益证据，不排实施卡。 |
| 11 | demiflow/pipeline/core.py:97 → demiflow.pipeline.context | 保留 | 删除 ProgramContext 会改变公开 program 能力；搬入口只会增加 launcher，合理 lazy import 保留。 |
| 12 | demiflow/pipeline/core.py:98 → demiflow.execution.run_context | 保留 | 该位置就是声明对象显式转执行的边界；不是应禁止的被动声明执行。 |
| 13 | demiflow/execution/contracts/operator.py:119 → demiflow.execution.operator_policy | 保留 | 复制接口 duck type 会绕过状态恢复准入；虽位于 execution 仍是窄政策合同。 |
| 14 | demiflow/execution/contracts/pipeline.py:64 → demiflow.execution.requirements | 保留 | 搬 dataclass 无当前使用收益且改变已记录路径；仅环境数据不是运行权限。 |
| 15 | demiflow/execution/contracts/pipeline.py:147 → demiflow.execution.requirements | 保留 | 与 F2 native/environment身份一致；抽空验证会使封存环境脱钩，保留比搬路径更安全。 |
| 16 | demiflow/execution/contracts/pipeline.py:214 → demiflow.execution.target | 保留 | Protocol/hasattrs 替代会放宽原准入；移动类会影响历史/外部公开路径，留原处。 |
| 17 | demiflow/operator_llm/model.py:28 → demiflow.operator_llm.errors | 保留 | 错误类型就是合理跨模块合同，搬迁只改身份与所有调用者，无收益。 |
| 18 | demiflow/operator_llm/model.py:164 → demiflow.schema | 保留 | 复制schema解释可能让检查与执行不同；窄共享函数合理保留。 |

邻接 G-ADJ-01：parser.py:151 → media.IMAGE_RESPONSE_SCHEMA，属于完整 G1 所必需的额外调用者迁移，不是 v3 原18条之一。只迁 validator 不足以隔离真实 parser。详见 edge-decisions.json 的 adjacent_findings。
