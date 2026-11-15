# 实际并发、状态所有权和恢复限制

本次仍是隔离分支上的实施进展，不是整体交付。两大 root 和 I01–I14 保持不变。

## 修复

- 同一个 worker kernel 按声明创建有限调用槽；异步生命周期共用持久事件循环，线程调用使用有界线程池。每个调用至多缓存一条输出，生成器不预先收集成列表。输入顺序决定调用身份和输出交付；checkpoint 必须等待调用与传输全部排空。
- 修复槽释放后继续阻塞等待下一调用、没有补入新输入造成的并发死锁。取消先等待在途调用退出，再关闭 actor；未确认退出不冒充清理完成。
- `callable_scope=worker` 每槽独立实例、独立状态及调用序号；槽轮转顺序随 checkpoint 保存。共享自动 JSON 状态且并发大于 1 时，在任何业务调用前拒绝。工厂在 worker 构造后分类，声明豁免不等于无状态。
- `restrict_recovery` 是唯一 driver journal 上的固定控制命令，只能收紧当前 action 的恢复保证。无法恢复的 actor 在首次调用前持久记录 `attempt_only`，工作台复用现有事实读取；部分恢复拒绝，最终已提交结果仍可读取。恢复启动前检查所有槽的镜像，而不只检查第一个。
- 算子内存请求纳入声明并发、每个独立状态所有者以及输入/输出批次同时存活的工作集；原生 Prompt 的 GPU 请求不因并发调整丢失。

## 证据与旧测试迁移

`/tmp/demiflow-concurrent-v3-2.log`：11 passed，包含真实异步/线程三并发、输出顺序、独立 actor 跨 cut 恢复、原生模型七个场景。

`/tmp/demiflow-concurrent-v3-3.log`：扩展组合 28 passed、2 failed。两个失败是新增测试错误引用 `budget_value` 和改名后遗漏的局部 fixture；已分别改为现有 `budget_consumed` 与同一个工厂，在 `/tmp/demiflow-lanes-concurrency-1.xml` 中均通过。原来五个历史恢复/状态反例本轮全部通过。最终组合仍须绑定最后提交重跑。

旧测试中的 pytest 顶层类改成局部可封存工厂，避免把 pytest 注入的模块路径当作业务运行环境；中断点从不再被调用的旧 `stream._arun` 迁到真实 `DataflowExecution.pump` 非最终提交之后。共享状态拒绝断言、首次调用前拒绝、工作台恢复限制、完成结果可重用均保留；独立 worker 的断言加强为准确序列 `[1,1,1,2,2,2]`。有状态 filter 现由独立串行 stage 执行，测试改为锁定真实串行计数，不能继续假设上游并发会隐式折叠进它。

全 lane 迁移仍未完成：`/tmp/demiflow-lanes-concurrency-1.xml` 首 129 项为 94 passed / 35 failed 后停止。集中暴露旧 fake DTO、空 ResourceProfile、不合法 durability、旧 worker 启动和 cut 接缝。它们没有被跳过或算作通过，后续逐项迁到真实契约并保留负例。
