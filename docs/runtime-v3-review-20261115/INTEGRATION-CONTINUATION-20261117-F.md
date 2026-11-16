# 原生队列恢复、代码固定与发布预算：继续验收 F

当前集成代码为 `f48bfa3f5a22084a0492cce6bffdcd4c36a11860`，隔离分支 `engine/gpt-runtime-v3-review-20261115`。主仓不修改、不测试、不合并、不推送。本页是本轮事实，不是全平台完成声明。

产品目标仍是看懂、配置、运行真实 pipeline，分析、比较、评测数据；平台目标仍是同一 Dataset/运行时在 Local 与 Ray Core 上正确、有界、可恢复且高效执行，保持原提交、资源与历史权威。以下工作直接修复实际业务路径，未创建第二套队列、提交账本或资源分配器。

## 已集成内容

1. `c227093` 已包含大行模型原生发布空间、输入封存与真实 Ray literal 恢复。原生发布组合在 `6e9d31c` 为 16/17 通过；剩余 Blob 负例被新的输入准入提前拒绝，保留原数据与 R/B、把夹具 M 从 384KiB 调到 400KiB 后，在 `c227093` 真正命中其原定的“转换前拒绝，writer 构造为零”。该单条通过，未把两个版本拼成一次 17/17。
2. `f772689` 至 `48e6722` 合入历史 owned SIGKILL fixture 与方法闭包代码固定。实际序列化的 class/instance/partial、类方法、原生槽位、静态/类方法与属性捕获进入同一有界依赖遍历；不调用用户 descriptor/metaclass lookup。摘要与源码归档复用同一依赖集。保留原 hash 公式；旧工件不回写。
3. `35798bb`、`90ee9b2` 迁移原 Ray 验收观察点：保留真实 actor SIGKILL、同 RunRef、原 ACK 与业务调用次数；Local/Ray 按真实发布前缀、稳定 record_id、最终源游标比较。物理 park 切点可以不同，继承贡献必须逐字来自原 ACK。
4. `f48bfa3` 合入队列恢复候选 `adcbb2d`。原数据流 cut 持有最多原 max_inflight 个 driver claim 引用。恢复先经原 QueueDatasource 对账，再经原 QueueChannel 验证/续用原窗口租约，过期租约只走原 claim 的准入、epoch 和预算路径。worker 只拿引用，不拿 token。
5. 已 ACK 的 RawFetch 继续由原 CommitCoordinator 复用。原生 final 已 ACK 而数据流丢回复时，只核验原完成证明、原工件和原行摘要，不重复 claim/finalize/计费。恢复重演若产生 `False→0` 这种 Python 相等但原生 JSON 类型不同的行，按原 `row_sha256` 拒绝，不把旧结果冒充新结果。
6. 原生 claim 窗口占满、下游尚未结算时，数据流使用原 cut/驻留轮换推进，不再必须等 60 秒普通 checkpoint 定时器。保留源 EOI、原 claim/finalize/ACK 权威及七件冻结。

## 验收与版本边界

机器证据见同目录 `INTEGRATION-CONTINUATION-20261117-F.json`：总控独立核验 12 份回执的全部输入摘要与各自 Git blob，零不符；各批输入不变、完整阶段、无存活子进程。不同 HEAD 不加总成最新版本全量通过。

| 验收 | 固定代码 | 结果 |
| --- | --- | --- |
| Local/Ray 模型、私有 cache、停止、严格恢复、literal ACK/GC/恢复组合 | `ae23afa` | 70 passed |
| 原生大行发布组合 | `6e9d31c` | 16 passed / 1 提前拒绝的夹具失败；上述原负例在 `c227093` 单独通过 |
| 真实 Ray 大 literal 原 ACK/GC/恢复 | `6e9d31c` | 1 passed，原 270 条类型化记录 |
| 最终 actor/class/partial 方法捕获与原 F7B/序列化 | `170071f` | 36 passed；含真实 SIGKILL、未改源码恢复成功、改依赖后零新业务调用 |
| 工作台首次运行与 default/kwdefault 代码保护 | `e0ed573` | 3 passed |
| 原 Ray actor death 恢复 | `4593a9a` | 该节点通过；同批 SEM 旧观察点失败保留 |
| 原 Local/Ray SEM | `952e8d2` | 1 passed；真实 2/12 个物理 cut，最终同一有序 5 行与源 10/EOF |
| 原两条 Queue SIGKILL→同 RunRef 恢复 | `796c79c` | 2 passed / 106.54s；原结果、HTTP 次数、quota/物理退出断言保留 |
| Queue 完整业务与 checkpoint progress、原租约边界 | `c34c0d3` | 23 passed / 1 新测试误用 process 观察器检查 thread；业务与原生 ACK 断言已通过 |
| 租约/预算负例和真实 thread/process 丢 final 回复恢复 | `32fcfb3` | 10 passed / 35.21s；分别核真实 thread/process 清理，修正上述观察器错误 |
| 两种 Local host 的 final ACK 复用及类型变化拒绝 | `adcbb2d` | 4 passed / 63.66s；HTTP 仍一次，原结果、epoch、claimed/settled 和退出证明保持 |

Queue 五个生产文件在集成 `f48bfa3` 与已验候选 `adcbb2d` 逐字节相同。另一路 agent 已独立只读复审恢复与权威链路，未发现新的 P0/P1 阻塞。仍需与另外两个运行时候选合流后的组合验收。

## 尚未收口，不能称 100%

- 图像部分完成后的协作取消仍保留间歇性失败证据。已定位清理阶段 RPC 抛 ServiceStopped 却未回错误 reply 的确定缺陷；两条负例复现后候选已修，37 控制项通过，原 130 行/130 次请求/两 cut 的恢复与真实 Ray 仍在最终收口。不能用两次碰巧成功覆盖旧红。
- 原 64/128MiB 宽 join、80KiB sort→sort→Lance 在独立候选已通过原业务断言；新的按物理 phase 预留发布空间、compact typed row 与真实 writer 开销核算尚待最终交付/集成。旧 v1 与旧 grant 不改写，不增加 R/B/M。
- 当前合流的 Local/Ray/工作台最终组合、原 U3 安静窗口、受影响 GPU、正式 I08 性能矩阵与 V035 安全历史余额未完成。I08 未跑，不称性能达标。
- 三条 Stage2 固定真实生产源用例未运行；其余原 Ray 历史节点逐项核余额。PostgreSQL 没有测试 DSN；两机旧 SSH 授权入口认证拒绝，当前新网络/整机故障验收未完成。已有历史双机/GPU证据不冒充本版本完整重验。
- 主仓 HEAD `e40a6dd`、原 11 项 dirty（18 个展开文件摘要）和七件冻结均再次独立核验未变。
