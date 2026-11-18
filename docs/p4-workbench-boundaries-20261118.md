# P4 工作台与引擎接口收口

基线：`14e78e0f9240aa981c88d6a376b542a8608a3768`（产品 `74d2f99` + Claude 编译契约 `2b54d1c`）。
状态：实施与定向验证中，尚未经总控独立验收；不代表 Claude 整条改造验收或部署。

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
