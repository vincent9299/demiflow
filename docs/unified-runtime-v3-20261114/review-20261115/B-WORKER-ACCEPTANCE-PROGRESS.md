# B001–B020 worker 生命周期与验收收口

在 `engine/gpt-v3-artifact-recovery-20261115`、`gpt_v3_artifact_recovery_20261115` 隔离树实施。GC 核心已独立提交 `0367648`；本批只改 `worker/dataflow_execution.py::DataflowWorkerExecutor.step`、`tests/test_runtime_v3_b.py` 与本回执，不改其他 lane。

## 红基线与真正的生产修复

原 B 门实跑 **16 failed / 4 passed**（`/tmp/demiflow-artifact-b-baseline.xml`）。夹具自己维护了一套 DTO，而且 `_install_fakes` 只在生产模块导入失败时安装伪实现，已经无法表达现行 `load_sealed_dataflow`、stage 所属程序、原生 journal RPC 和 graph runner。没有把这些伪实现重新插回生产调用链。

另复核发现真实生命周期缺口：`DataflowWorkerExecutor.step` 直接访问仅在 `start` 创建的 `_runner`，未激活调用得到 `AttributeError`；取消之后显式 step 还可能继续读输入。总控明确授权最小修复：

- 未激活或已关闭的 step 明确 `FENCED`。
- 已取消的 step 返回 0 processed / end_of_input，保留未确认 effects，不读取 source，不伪造提交。
- 控制锁只包住准入检查，不包住 reader/kernel 执行。真实 reader 阻塞时 cancel 仍可及时返回。

## 验收更新的精确边界

保留全部 B001–B020 nodeid。使用真实 `prepared` 编译/封存，真实 DTO、RunArtifactStore、WorkerJournalClient、DriverJournalService.dispatch、program verification、ControlMailbox、DataflowWorkerExecutor、WorkerGraphRunner 与 WorkerRuntime。只以 `DirectTransport` 适配进程内网络边界；fixture lease 向原 journal service 登记，不宣称该单元门完成了真实 host allocation。真实 host/Local/Ray 验收另有集成门。

- B001–004：构造不读源；原封存读取不构造 driver RunJournal；错误 boot/plan/code-pin 拒绝；真实 cut image 恢复；缺 buffer/channel ref fail closed；激活前 FENCED；阻塞 reader 与 cancel 并发。
- B005：耗尽真实 channel credit 后 producer 不发 data，consumer 不发生付费调用；归还额度后真实 payload 交接。foreign producer partition 在 peer 解码前拒绝，合法交接后真实 ACK 释放 producer credit。
- B006–008：没有 driver cut 请求时 worker 不发 cut；错误 revision/participants 拒绝；空侧也贡献 cursor 与真实 channel/state 工件；重复 receipt 幂等，冲突/未知 cut receipt 拒绝。现行协议允许 driver 在结束时发送 final request，因此旧“任何 final=True 都拒绝”的夹具假设改为“worker 不自行产生 cut，final 来自经过校验的 driver 请求”。
- B009–010：取消后 0读取且保留未知效果；一个实际 kernel close 失败时其余 acquired reader/邮箱/缓存仍收尾，第二次 close 幂等。
- B011–017：真实 WorkerRuntime code pin、限额、K3 grant、wire assignment、Verify→Restore→Activate、Drain/Cancel；未激活 Cancel 进入 RETIRED，不能重新 Activate；不把 driver authority 挂到 worker。
- B018：缓存按 assignment 私有子目录拥有。配置 parent cache 与自动 temp cache 都清理本 assignment 目录，其他 allocation 的 sentinel 保留。by-value source 可以不创建 code cache，所以配置路径测试预放本 assignment 的旧 cache，再验证实际清理；不要求空程序强制造文件。
- B019–020：实际 bootstrap 入口/schema/pin 拒绝；真实 socketpair 的 Register→Cancel 保持有界退出。

## 最终证据

命令（本树，`PYTHONPATH=$PWD:$PWD/tests`，`DEMIFLOW_RESOURCE_ROOT=/tmp/demiflow-artifact-capacity-b3`）：

```sh
/yzp/zhaozy/yangzepeng/0905/env/bin/python -B -m pytest -q \
  tests/test_runtime_v3_b.py \
  tests/test_runtime_v3_concurrent_execution.py \
  tests/test_runtime_v3_public_execution.py::test_public_map_take_commits_actual_records \
  tests/test_runtime_v3_public_execution.py::test_partial_cut_restores_input_kernel_and_terminal_prefix \
  tests/test_runtime_v3_public_execution.py::test_public_async_lifecycle_same_loop_and_cleanup \
  --basetemp=/tmp/demiflow-artifact-b3 \
  --junitxml=/tmp/demiflow-artifact-b3.xml
```

**28 passed in 56.08s**，无 skip/xfail。七承重文件逐字等于 `49b666f`。本批未重跑整套 Ray（GC 前批真实 Ray 已过，本批生产变化只增加 step 激活/取消准入）。主仓、保护文件、共享环境和在运服务未操作。
