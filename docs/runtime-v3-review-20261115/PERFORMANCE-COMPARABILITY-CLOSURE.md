# 性能对照必须先证明部署与存储可比

真实漏洞是基线源码放在 `/tmp` 本地 xfs、候选源码放在 `/yzp` 的 dpc 共享文件系统，却把耗时差归因为代码收益。实际只读取证仍观察到 `/tmp` 设备 `259:14`、本工作树设备 `0:196`。旧 Lance 启动 7.92% 的因果结论已撤回，原测量和功能证据继续保留。

修复提交 `3ea123b` 只改 `tests/runtime_v3_performance_scenarios.py` 和 `tests/test_runtime_v3_performance_harness.py`。原机器计划 JSON、九类负载、独立预热、五轮、原基线角色和 20% 阈值逐字未改，生产和冻结七件未动。

每次真实运行在计时前后观察源码 checkout/实际包、harness、试验根目录、run、三个私有资源域、temporary 和 TMPDIR。Linux 的 stat/statvfs 与有界 mountinfo 读取提供设备号、fsid、文件系统、挂载来源/根/选项及证据摘要；主机、boot 和 mount namespace 分开记录。尚未创建的目录明确记录其最近实际父目录，计时后重新验证，不把相同路径字符串当相同存储。不同工作树和试验目录名称允许不同，比较的是对应位置的物理条件。

已有的正式 Ray 集群在计时外逐节点做同一只读观察：最多 8 个实际节点，原 NodeAffinity 硬绑定、零 CPU/GPU 请求、单次任务、30 秒总等待。原 Ray API 提供实际 node_id，节点集合必须覆盖该轮真实节点；driver 的观察不能代替远端。失败只取消本探针的任务，不停集群或其它服务。本轮没有为了该改动启动远端集群，远端适配器仅完成单元检查，真实远端取证等待下一次已授权性能运行。

计时明确从 RunContext.start 之前开始，到公开结果消费、原 ACK、run/executor 关闭之后结束。业务 worker 导入/代码检查在内；子进程启动、driver 初始导入、输入摘要、HTTP/Ray 观察器 setup/teardown、源码快照和文件系统探针在外。实际 start/stop 单调时钟与 elapsed 绑定；profile 诊断不准入正式比较。

正式比较在已知 driver 源码位置不可比时便不启动十三轮。实际回执缺身份、探针失败、任一路径存储不同、计时前后或跨轮漂移、远端覆盖缺失，均为 **not_comparable**，不生成 p50/p95 比值或净代码收益。业务功能状态保留；不可比不冒充“性能退步”，也不能通过 V034。文件系统匹配只是必要条件，仍需原固定输入/预算/版本、清理及最终协调的测量环境，不能据此声称缓存/主机背景负载完全相同。

## 证据

父版 `a354f0e` 的纯比较器收到完整合成五轮回执、仅候选源码 FS 改为 dpc，仍返回 passed/ratio=1；新比较器返回 not_comparable、空 comparisons。`/tmp/demiflow-storage-comparison-counterexample.json` 保留两边结果、父版源码摘要和真实本机挂载观察。合成回执只证明检查器漏洞，绝不是引擎性能通过证据。

固定 `3ea123b` 专项 **49 passed / 5.87 秒 / 零失败与跳过**：48 项检查器或只读探针验证，另 1 项原有真实 Local small_data 使用真实 Dataset、原 ACK、资源清理并记录前后存储。新反例覆盖相同 FS 类型但不同设备、数据或临时目录差异、挂载选项、主机/boot、未知身份、轮内/轮间漂移、错路径、计时边界、profile 和远端缺口。两臂一起篡改数据/预算、缺轮次、改基线及超过原 20% 的既有反例保持。

`/tmp/demiflow-storage-guard-r1-evidence/verification.json` 的 2,198 项输入与提交归档逐字一致；测试前后无源码/HEAD 漂移，冻结七件一致，零遗留子进程。`PERFORMANCE-COMPARABILITY-VERIFICATION.json` 固定原回执、真实 Local trial、挂载证据和输入摘要。正式九类五轮矩阵未跑，不能把这 49 项称作 I08 性能达标。
