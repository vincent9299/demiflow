# P4 工作台与引擎接口收口

基线：`14e78e0f9240aa981c88d6a376b542a8608a3768`（产品 `74d2f99` + Claude 编译契约 `2b54d1c`）。
状态：实施和定向验证完成，待总控独立验收；不代表 Claude 整条改造验收或部署。

本次服务于 R.2.1 的真实节点源码/参数/Prompt，以及 R.2.2–R.2.3 的受管运行，
收口 R.5.1 的私有依赖。仅在独立副本实施，不涉及 P5/P6 进程重构、Ray 产品入口、
全新运行 SDK、第二个 Journal、业务迁移或服务部署。

## 受管 Local 宿主接口

工作台继续直接装配 `RunJournal`、`RunContext.attach`、`local_execution` 和
`CheckedExecution.bind_context`，不引入纯转发启动器。它们是此宿主场景的公共
surface，不意味着执行器内部字段、任意 context 查询或全部模块成为公共接口。

`RunContext.attach(journal, *, entry_identity=None, limits=None, check_stop=None)`
复用宿主现有 run/attempt 和 Journal。宿主保留运行状态、取消、seal、预算与结果登记；
引擎保留 scope/资源生命周期及原生提交事实。退出 scope 不新造一次运行。

`CheckedExecution(..., report_digest=None, targets=None)` 是现有检查报告绑定接口；
受管工作台必须显式传入封存 manifest 的 `report_digest`，不探测版本、不省略绑定。
该参数不是新一份报告；检查、receipt 和恢复合同保持。

`local_execution` 在全部现有参数（包括 `planning`）之外接受：

- `resource_root=None`：相对 Dataset/Prompt 资源根，默认仍为当前目录。
- `diagnostics_root=None`：原生诊断根，默认仍回退到当前 run 的 `diagnostics/native`。
- `block_size=256`：执行器块大小，与 `batch_rows=8192` 分开；必须是正整数。

路径在构造期规范化，不主动创建资源/诊断目录。配置错误在进入执行 scope 前失败。
工作台分别传入封存 source、run 的 diagnostics、剩余 limits 的 batch_rows，替换原来的
三个私有赋值。普通调用默认不变，`planning` 的 max_parallelism 校验保留。
执行器仍在 finally 中关闭；Journal、权限、预算、取消、恢复和登记逻辑不迁移。

语义测试：`tests/test_local_execution_host.py` 覆盖明确配置、默认回退、同一 Journal、
报告 digest、异常退出清理和资源启动前的参数/规划预算拒绝。实际受管运行沿用
`tests/test_forge_runs.py` 与 `tests/test_forge_run_faults.py`，由最终组合验收记录选定结果。

## 身份和历史约束

不更改冻结 `CompiledDataflow`、graph/声明/报告身份、Journal schema 或恢复 identity。
平台源码 hash 的变化继续阻止不兼容历史恢复；旧记录按原封存数据读取。
业务源码/参数摘要、seal、结果登记及权限守卫均不得绕过。

## 真实编译来源接口

`capture_pipeline(entry, arguments, *, on_compiled=None)` 把可选宿主回调传给
`lower_action` / `capture_graph`。回调消费 `CompilationProvenance`：
`action_id`、`dag_id`、`bindings: tuple[OriginBinding, ...]`、`terminals`、`unavailable`。
每条绑定只有 `kind`（source/operation）、瞬时 `object_id`、最终 `occurrence_id`。
object ID 仅在同一预览进程中与已有弱引用连接，绝不进入封存图或任何持久身份。

生成时点在 `decorate_graph_capture`、codec window、`bind_shared_actors`、最终图
digest 和 `validate_graph` 全部完成后。alias 索引只在编译器内解释，取最终 graph 节点；
同一 operation 对象的多个 occurrence 不合并。只保留原有实际编译这一次，既不再编译，
也不根据 AST 或函数名重建图。无 observer 时不调用来源构造器、不复制任何 alias 索引。

先检查最多 256 节点、1024 条绑定，再迭代 alias；超限返回空绑定和明确 unavailable，
不先复制大表、不截断后冒充完整映射、不拒绝合法图。callback 异常原样传播使预览失败；
capture ContextVar 以及 PlanCallsites profiler 均在 finally/exit 中复原。回调是受信宿主
接口，不是业务代码或模型工具执行入口，普通执行默认关闭。

工作台保留公开 SDK 调用入口的被动观察，冻结其真实参数及精确源码位置。同步回调时，
终端仍位于活动 SDK 调用内，从已捕获的 SDK 记录取得调用点，不读取编译器栈帧或局部变量。
已有 profiler、死弱引用或引擎展开节点缺少来源时显示 unavailable。材料读取核对同一
action/dag，并继续核验项目目录、源码 hash 和精确 AST 调用范围。AST 仅定位文本。

## Prompt 材料接口

`execution.program_materials.program_prompt_materials(declaration, *, max_depth=4, max_prompts=32)`
只消费实际 `ProgramDeclaration`，返回 `ProgramPromptMaterials(occurrence_id, state, prompts, reason)`。
state 是 none/available/unavailable，后者保留“确实引用了材料”的 UI 语义。

每个 `PromptMaterial` 提供带路由的 name、已经选中的 `PromptDefinition`、原声明的
inputs/output/outputs/max_requests 及被动的普通函数 predicate 引用。不归一化或改变
合法绑定形状，不返回 operation/settings/service/environment，不重开 Prompt 文件。
工作台继续产生全部原 UI 元数据、脱敏、源码认证和文本/JSON 预算。

包装解释属于 execution，复用 `dataflow_native.model_variant`；未修改工厂类或
Claude 的 CompileInputs、注册/降级机制。仅按真实类型读字段，不实例化程序、Actor、
模型或服务，不调用 predicate、repr、任意 iterable。总包装访问至多 161、深度至多 4、
Prompt 总数至多 32、名称至多 2048 字符、单 pack 至多 256 定义；超限材料 unavailable，
图继续保留。大模板与 Schema 只保留原值引用，现有单文本 64 KiB、单材料 128 KiB、
总材料不足 4 MiB 的宿主预算不放宽。这些只读视图限本次编译生命周期，不得修改。

语义测试位于 `test_compile_provenance.py`、`test_program_materials.py`，既有真实 SDK
调用/共享前缀/源码 hash/Prompt `when` 测试位于 `test_workbench_operator_contracts.py`、
`test_workbench_plan_details.py`、`test_plan_capture.py`。全量编译语料差分和最终浏览器、
受管运行抽验由总控在固定交付 SHA 上独立进行，不以当前局部通过代替最终验收。

## 本任务交付证据

使用现有 `/yzp/zhaozy/yangzepeng/0905/env/bin/python`，从本独立 clone 运行，
测试临时目录均为 `/tmp/demiflow-p4-*`，未安装依赖或改共享环境。

| 定向组 | 结果 | 原始证据 |
| --- | --- | --- |
| Local 宿主接口 + 工作台边界 | 16 通过 | `/tmp/demiflow-p4-host-results.xml` |
| 计划捕获、Prompt 材料、公开 API 关联、边界、编译依赖 | 57 通过 | `/tmp/demiflow-p4-materials-results.xml` |
| 新来源/Prompt 合同 + 包依赖、CompileInputs、native variant | 59 通过 | `/tmp/demiflow-p4-contracts-results.xml` |
| 最终新增合同（含真实 cached Prompt、shared actor、超限保图） | 18 通过 | `/tmp/demiflow-p4-contracts-final-results.xml` |

上述组含重叠用例，去重后为 11 个文件、127 项全部通过，不把重复执行累加为覆盖。
初始化宿主测试 fixture 的早期失败已修正（必须先建立 Journal attempt/state）；
材料边界 fixture 使用新声明时必须重新传可冻结的 code_identity，未为测试放宽产品合同。

13 个关键文件逐字节与 `14e78e0` 比较不变：冻结 dataflow/codec、CompileInputs、
native/model lowering 与模型工厂、graph adapter/shared actor、RunJournal、RunContext、
CheckedExecution、recovery identity 和工作台 runs manager。证据为
`/tmp/demiflow-p4-freeze-check.json`。同一真实请求 observer 开/关还验证了 graph 字节及
完整 CompiledDataflow 的相等性；没有改写已有编译语料或结构基准。

局限：本任务未声称全量 357 项语料差分、浏览器或真实受管提交/取消/恢复已完成。
这些由总控在固定交付 SHA 上独立抽验；本任务已测试正常与异常 scope 退出、同一
Journal/attempt、报告参数、预算拒绝以及只编译/只读材料不执行业务和模型的边界。
