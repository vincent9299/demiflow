# 资源核心验收：V023 双后端通过，V012 Ray 仍阻塞

固定隔离树 `engine/gpt-v3-artifact-recovery-20261115`，被测提交 `6d64117`；全部生产 Python 与总控 `abeaa7b` 逐字节相同。主仓、共享环境、默认资源池没有写入或测试。源摘要、四组原始证据目录索引、JUnit 及真实资源清理见同名 JSON。此结论只覆盖表中实际执行结果，不宣称全部意图或性能闭环。

| 门 | Local | 同机真实 Ray |
|---|---|---|
| V023 memory_peak | 通过，48.990 秒 | 通过，70.863 秒 |
| V012 fairness_deadlock | 通过，86.104 秒 | 失败，131.720 秒（包含集群启动/回收） |

Local 两门 2 passed / 135.23 秒；Ray 两门 1 passed、1 failed / 203.33 秒，零 skip/xfail。全部在各自 `/tmp/demiflow-*` RESOURCE_ROOT、RESOURCE_HOME、FORGE_DOMAIN 中执行，进程内验证导入本树；Ray 为私有 6 CPU head、真实原 actor/资源权威，业务 worker 请求零 GPU；head 未显式禁用硬件 GPU 发现，不称集群声明了 0 GPU。原承重七件相对 `362d8528` 字节未变。

## V023 实际覆盖

两后端各执行相同 12 个情形，其中 10 个走真实 Dataset，两个补足原 RunContext/CommitCoordinator/LanceSink 的精确 cast 缝；不是旧报告拼接。每组 9 次实际 `execute_action`（不足预约在 launch 前拒绝），零 Ray Data 导入尝试。每组原 journal 27 个 allocation 全部 CONFIRMED，Local 原池 27 个均 RELEASED；真实子进程列表为空。Ray 使用原 Ray authority，不能用 Local 池为空冒充其释放证据。

1. 32769 字节载荷超过 32768 单行上限，callback 从未运行，无结果提交。
2. 8×8 KiB 输入共享分支到慢消费者，完整 16 条多重性正确。原 worker ChannelSetState 实际 reserve 观测保留行/字节上界；不是 driver 无采样的空列表作为证明。
3. Typed-null Arrow 转换：16 条 fixed-list 1024-double 需要 131072 字节，原 batch 4096，分配前拒绝；proxy pool 仅 128 字节固定开销，未分配超限目标。原反例修前实际分配 133184 字节才失败。同预算正例 8 条完整 schema/value 均核对，回调后结果 count=8。
4. 状态+输入+输出共驻：原状态 131072 字节、输出约 65886 字节与输入同时存在，原 worker profile 917504 字节，16 条结果完整；总预算 1 MiB 的负例在 launch 前拒绝，未抬预算。
5. Dataset→Lance 转换正负例及 cast 正负例：原 fixed-binary 131072 的负例在构造/转换前拒绝、目标不存在。修前原 cast 实际分配 2097216 字节；修后只见 64 字节已有空值 bitmap，无目标大缓冲。
6. 精确等宽 cast：4300 个 uint64→float64，两份各 34400 < 65536，但同时 68800 > 65536；当前在 cast 前拒绝，目标 Arrow 分配峰 0、无发布/ACK。原实现红对照实际先分配 34432。相同 65536 预算下 3000 行正例，输入/输出各 24000，完整值摘要与 schema 正确，原 Lance 提交回执 ACK 成立。

Arrow proxy pool 观测实际 Arrow 分配；Python 受管对象通过原 check_size/grant/ChannelSetState 核对。两者均不冒充操作系统 RSS/VMS 硬隔离，也不声称任意第三方内部分配已完整受管。输入 Arrow 已有缓冲和目标转换同时驻留计入准入，不以单独两次检查代替总和。

## V012 真实剩余问题

Local 的原 64×4 KiB、1 worker、16 MiB、120 秒、checkpoint 1000ms 保持。原私有池另有真实 holder 持有 63000/64000 cpu_millis，graph 仍完整输出 128 条；holder 一直存活到业务完成，之后通过原 process-unit 停止并释放。该子例 47.507 秒；原 16384×4 KiB、90% 热点键、128 分组校验 25.153 秒；8 MiB 小磁盘明确资源拒绝 4.197 秒，无最终结果提交。34 个 graph allocation 全部 CONFIRMED，另 1 个 holder 也真实退出并 RELEASED。

Ray 原 head 6 CPU，真实 holder 持有 5 CPU，仍只留 1 CPU；同样 64×4 KiB / 1 worker / 16 MiB / 120 秒 / 1000ms，**122.393 秒后超时，0/128 终端结果，消费者未启动**。48 次真实 actor allocation 全部最终 CONFIRMED，holder 物理退出，释放后 CPU=6，自有子进程为空；清理正确不等于业务成功。后续 Ray 热点键/小磁盘两个情形未执行，明确 not_run，不能由 Local 结果代替。

总控已交 GPU/性能 lane 修 Ray phase/checkpoint 调度，保持原预算和 interval。当前观察是 actor 启动耗时大于切片间隔，需原 cut/state 因果核验，尚不在本报告把推断作为最终根因。另保留一份不经 pytest fixture 删除的原固定红现场，交该 lane 只读诊断；不重复跑未变的 12 个内存情形。

## 测试工具纠错与历史证据

首次最终 Local 组业务均通过但 teardown 检测 FD 56→77，定位为测试证据导出 `sqlite3.connect` 只退出 transaction context、未关闭 FD。`6d64117` 用 `contextlib.closing` 显式释放，并新增 12 个真实 SQLite 备份、关闭 GC 的 FD 零增长反例；原 FD 守卫未削弱。此后上表最终两后端四门全部无 teardown/FD 错误。

之前 115 项原 C/finite/public 回归完整通过，原凭证 `/tmp/demiflow-resource-terminal-reg.xml` 和前报告字节绑定保留；不把它们称为本提交重新执行。之前慢 42 行恢复原正确性案例在停驻修复组合后也通过，仍不构成 I08 五轮性能完成。`RESOURCE-PREALLOCATION-20261116.md` 保留分配红→绿与旧 callback-prefix 断言三文件对照：合法非最终零输出 ACK 不等于目标发布；无最终/无 native 结果、原版本不变及不重试断言均保留。

产品意义：宽行、慢消费者和有限资源下，真实 Dataset 结果与原提交/恢复权威相接；超预算在不可逆分配/发布前拒绝。引擎意义：使用原 allocator、worker grant、journal、ACK 和真实 Local/Ray 载体，未增加第二账本。Ray 单槽调度仍直接妨碍统一后端可用性，必须修好再关闭 V012。
