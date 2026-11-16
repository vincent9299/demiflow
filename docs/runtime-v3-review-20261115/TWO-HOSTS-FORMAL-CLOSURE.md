# 两台真实宿主上的正式 V026 验收

固定集成提交 `f5669fbf0ec9e4a5c45c95efdb420557edf16fd1`，正式 `test_runtime_v3_acceptance.py::test_two_hosts` **1 passed / 154.083 秒，0 skipped**。这一个门内部直接执行原四个真实业务场景，再执行工件内容冲突反例；不是五个独立 pytest 通过数。

## 用户可依赖的结果

- 同一 Dataset 通过真实 Ray Core 在两台不同物理宿主执行，12 行经过 source/transform 后写入原 Lance sink，原提交与最终 ACK 完成。
- 远端本任务 actor 在一个非 final ACK 后被真实杀死；再次运行沿同一封存 plan_id、原 ACK 前缀恢复，最终仍为完整 12 行。没有重新编译另一份计划。
- 共享上游、双分支 union 和 repartition 实际输出 16 行，重复多重性正确，业务 worker 实际覆盖两个节点。
- 两机不共享的 `/tmp` 读取域在业务执行和 ACK 前被拒；两个宿主上路径和长度相同、字节不同的 challenge，由原校验拒绝为 `CORRUPT_ARTIFACT`，没有提交。
- 原读域验证产生 9 条真实证明；两节点共观察到 5 个非空 worker 代码缓存。缓存路径按原 allocation 隔离，没有把 driver 的 import 路径当作 worker 已装载证明。

原 RunJournal、CommitCoordinator、资源分配、封存身份与读域校验保持唯一；没有新增第二份执行或提交账本。测试中的不同字节只写入本任务私有 `/tmp/demiflow-*` 路径，结束删除该文件。

## 证据与边界

[独立核验回执](TWO-HOSTS-FORMAL-PRIMARY-VERIFICATION.json) 将全部 **1866 项 Python 输入摘要**逐一与固定 Git 提交比对，零不符；运行前后源码不变。原始输出位于 `/tmp/demiflow-root-two-host-formal-r2/`，包括两台宿主实际 node/hostname、原业务观测、读域证明、正式 XML 和私有集群退出记录。

本机 `ray_initialized_after=false`、`children_after=[]`；远端退出码 0、`children=[]`。仅清理本任务启动的集群和 worker，没有停止其他服务。

第一轮 `/tmp/demiflow-root-two-host-formal-r1/` 的四个业务场景已通过，但新增错误字节夹具用了不同长度，先触发长度拒绝，未走到预期摘要拒绝。`f5669fb` 仅修测试为等长错误字节，第二轮完整正式门通过；生产防护没有放宽，第一轮红证据保留。

故障范围是**两台真实宿主中的本任务 actor 丢失**，未测试整台机器掉电或网络分区。这也不替代小预算公平调度、全量历史回归或同预算五轮性能验收，三者另行收口。
