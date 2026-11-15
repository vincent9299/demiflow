# 真实公开 API 完整验收

在固定代码 `3d5f4dd425dd67c5328fe3e07e576c694c5f7471` 上，正式
`test_runtime_v3_acceptance.py::test_api_matrix` 通过，随后类型保真、有限段和
控制连接清理专项通过：**12 passed，553.88 秒，零失败、零 teardown error**。

- 98 个计划入口均达到各自声明的验收口径；其中 4 个已退役 checkpoint
  入口验证的是保留拒绝，并非恢复旧执行路径。
- 168 个实际案例：80 个正例、88 个负例；负例严格校验原错误边界。
- 214 次真实 `execute_action` 调用，原 RunContext/RunJournal/CommitCoordinator
  及真实 Local worker 执行；Ray Data 导入尝试为零。
- `failed`、`not_run` 均为空，307 条加载模块路径指向本隔离树。
- 原 FD 门未变：baseline 53，call 与 teardown 都为 59，均通过既定 `+8`
  容差；另外的专项强持有异常与执行对象，逐个检查已退出 worker 的原
  `driver_socks` 清空，覆盖三次真实失败、有限阶段退休和原生写出。

完整逐入口/逐案例/调用/提交证据在
[API-MATRIX-FINAL-20261116.json](API-MATRIX-FINAL-20261116.json)，命令、代码摘要、
冻结七件校验、日志/JUnit 摘要和 FD 阶段记录在
[verification](API-MATRIX-FINAL-20261116.verification.json)。原始临时证据为
`/tmp/demiflow-api-final-v4-evidence/`、`/tmp/demiflow-api-final-v4.xml` 和同名日志。
外部 pytest 观察插件只在测试 call 完成后复制证据，避免 fixture 清理目录
后丢失；不替换任何 Dataset API、执行函数或验收断言。

本轮包括：原生写出正确传递 Arrow 类型及可空 receipt、空输入在真实 EOI
提交空表、有限输入按实际编码字节组段并兼容旧 pending 快照，以及 Local
仅凭原物理退出证据关闭控制连接。也已合入父分支的原生额度传递与原
journal 中暂存总额度核算。历史红证据保留，没有改写成通过。

这表示本计划的 **Local 公开 API 案例集** 完成，并不代表任意参数组合、
所有外部数据库、Ray 全 API、多机故障、GPU、性能基准或整个产品 root
达到 100%。共享上游 prepend 在默认四 worker 下的调度退步已单独交父分支，
未通过提高该测试预算掩盖；它不是这个 168 案例集合的覆盖项。

本路始终在隔离分支，未改主仓、未推送、未停止外部业务。
