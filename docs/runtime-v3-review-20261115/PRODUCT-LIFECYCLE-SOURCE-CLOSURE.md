# 工作台历史源码、默认资源准入与实际 actor 生命周期

本包在隔离分支 `engine/gpt-v3-artifact-recovery-20261115` 修复四个实际产品缺口：prepare-only 历史声明图无法读源码、默认 flat_map→batch_map 预算拒绝、actor 类与既有实例的启动/关闭事实未显示。基线是 `2072bfa`（已合 root `69602d5`，包含本路 `93bad20` 的 Embedding 与真实调用校验）。不修改主仓、不推送、不调整共享环境或服务。

## 用户看到的变化

- 历史记录中的声明图明确显示“声明投影结构（未执行）”。选节点能读当时封存的源码；准备后修改当前实现，历史仍显示原函数。真正运行的封存执行图仍通过原身份桥定位，缺桥/冲突仍拒绝，不把声明图提升成执行证据。
- 合法的 flat_map→batch_map→materialize→write_lance 能按默认配置完成。默认行、批上限为控制信息与同时驻留的输入留出空间，并把实际值写进原 manifest；真实执行读取相同值，内存总额不增加。
- 用户明确指定的行、批上限保持不变。只有仍使用默认值的字段参与调整；不存在可行配置时明确拒绝。部分字段显式、部分字段默认时，搜索只在 `row <= batch` 的合法范围内进行，不把过小批上限误当成资源过大。
- actor 的 created/started/closed 来自实际 worker 的原生生命周期步骤。真实启动或关闭失败显示失败；未发生或无法验证的步骤不推断成功。仅声明一个类不会制造运行事实。

## 接线与权威边界

`AsyncInstance` 在原调用/清理步骤完成或失败时记录常数大小的计数，不持有另一份 actor，不改变原 state owner、生命周期调度、调用参数或代码身份。无可选用户 hook 时，完成的是平台原生生命周期步骤，并不声称调用了不存在的用户函数。

中途事实走现有 `progress`。关闭阶段原 mailbox 已收尾，最终事实作为原 `Drain`/`Quiesce`/`Cancel` host 回包的附加 `WorkerEvent` 运输；K3 receipt 本体未修改。Local process、Local thread、Ray Core 使用相同事件解析和验证。最终列表受原控制字节预算约束，单次线性累计，不反复序列化整个前缀。

Driver 在 allocation/boot/attempt/fence 尚有效、资源尚未退休时复用原事件验证，再按原封存身份桥投影到 `CheckedExecution`，由原 `RunJournal.observe_nodes` 持久化。阶段退休只强制刷新非终态事实；最终节点终态仍由原 action 收口决定，原 journal 的终态不可覆盖规则保持。观察收尾失败不能遮蔽先前业务/关闭异常。

这些生命周期值是各 occurrence/partition **当前 worker boot** 的实测投影；新 boot 替换该分区的当前事实，不声称是全 attempt 累计创建/关闭次数。`closed` 表示实际原生 actor 关闭步骤结果，不等同于进程退出或资源退款证明；物理资源退出与返还仍归原 ResourceManager。被旧 boot 篡改的最终事实即使对应 actor 确实执行过 aclose，也不能显示为已验证关闭。

工作台原先强制 `workers=1` 还会让扩展调用在 parked durable credit 上被拒绝。本包复用 residency `2b9f988` 同一行改动：使用统一 `LocalKernelOptions` 原默认 4，仍由原物理计划进行资源准入；没有扩大 memory/RSS、修改 credit 或放开不可容纳调用。

## 关键验收

- 原 `test_managed_native_families` 六族完整执行：async、flat_batch、显式有界 flat_batch、actor 类、actor 实例、iterator；输入→实际变换→materialize→Lance，保留原精确行值、多重性及 actor 启动/关闭断言。默认与显式 caps 另核对封存 manifest。
- 真实 process 反例：启动失败、业务失败加关闭失败、单独关闭失败、取消、空输入不创建类实例；保留实际文件中的创建/启动/关闭顺序与具体错误。篡改最终事件 boot 后原 fence 拒收，不能伪造 closed。
- 真实浏览器读取 prepare-only 历史源码，同时修改当前函数名确认未回读当前文件；相邻 29 项原身份桥匹配、缺桥和冲突保护继续执行。
- 真实同机 Ray Core：原生 SaveLance 确认回执，以及 Prompt→sort→take_all、原请求额度与原 ACK；禁止以 Ray Data 接替。不是多机/GPU验收。
- 原线程恢复测试原先通过 `_retire_phase` 注入故障；最新合法融合可能不经过该点。已改为原 CommitCoordinator 确认的非 final cut 之后中断，使用 checkpoint_rows=2，保留 caller 默认改变后仍复用封存 thread 模式、完整六行、原 PID 的断言；不是通过禁用融合让旧夹具碰巧命中。
- 部分默认预算的真实工作台正反例：显式 row=12 MiB、默认 batch 收紧后完成；显式 row=16 MiB 无可行范围时拒绝；显式 batch=16 MiB、默认 row 收紧后完成；显式 batch=16 MiB 且总内存=128 MiB 时拒绝。固定字段和总内存均核对未变，失败不发布输出。

完整命令、原始日志/XML、每轮输入摘要与最后结果见同名 `.verification.json`。中间失败保留：旧退休 hook 不再必达；新非法参数测试最初期待下层错误，实际被更早的公共 guard 拒绝；部分默认预算第一次真实复现红。修复后对应反例单独复验，不把这些旧红删除或计成通过。

## 合并注意与未声明完成项

- root 正修改 `ControlMailbox` outbox 唤醒与 `bootstrap._run_v3_worker` 选择器性能；保留双方，尤其本包 command loop 中捕获 executor、最终事实附加回包的三处。
- residency 的 Save 产品包与本包只在 worker 默认行相同，checked/manager 是不同函数/代码段。
- regression 的有限输出游标与非阻塞 EOI 改 `OperatorKernel.finish`/GraphRunner step/frontier；本包不覆盖这些区域，只增 `_create_instance` observer、metrics payload 与关闭事实。
- 本包不证明全项目已 100%。Embedding image/document 的工作台对象读取授权、多机/网络分区/GPU/全 API Ray 矩阵、全局性能仍需各自证据；本次未用已有引擎正例替代这些产品或环境缺口。

## 本轮结果

按同一 nodeid 去重并保留修复顺序，最后已验证 84 项通过、0 项遗留失败、0 项跳过。各组不是同一时刻运行：Local 初组 45 通过/1 旧注入点失败，修复后的 tail 9 通过/1 新负例错误消息期待不符，公共拒绝独立 1 通过，部分默认预算组 5 通过；浏览器与桥 31 通过，真实 Ray Core 2 通过。各版本差异及对应输入摘要已记录，未把早期失败冒充当时全绿。承重七件与 `49b666f` 逐字节一致。
