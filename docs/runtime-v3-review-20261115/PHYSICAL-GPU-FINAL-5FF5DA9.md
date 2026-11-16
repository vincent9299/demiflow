# 本机真实 GPU 验收

提交：`a8544ba1daabd28aba4c4d736a5114d0d2d806f2`；隔离树 `gpt_v3_artifact_recovery_20261115`，分支 `engine/gpt-v3-artifact-recovery-20261115`。生产代码与 root `5ff5da9` 相同，仅新增 `tests/test_runtime_v3_physical_gpu.py`。承重七件相对 `362d8528` 字节不变。

正式固定候选：4 passed、0 skipped、0 failed，74.65 秒。1785 个 Python 输入摘要、HEAD、工作区状态前后全等；实际 import 来自该隔离树。完整命令、输入摘要及每例记录在 `verification.json`，XML `/tmp/demiflow-physical-gpu-final.xml`，日志同名 `.log`。

## 实际物理范围

当前主机有四张空闲 NVIDIA H100 80GB HBM3。本次只使用真实编号 0，UUID `GPU-b1e12243-75a0-c769-ce2c-3c7ac9d8ef71`；inventory 的 UUID/容量来自 nvidia-smi。使用现有 `env` 的 torch 2.13.0+cu130 / CUDA 13.0，不安装环境、不使用假 GPU inventory。另三张卡从未由本测试占用。

真实公开 Dataset 输入 → CPU map → GPU map → CPU map → take_all/iter_rows → 原 RunJournal/CommitCoordinator ACK。GPU worker 从原 grant 得到一个实际设备；worker 不能再创建 GPU authority；CPU 两侧可见设备均为空，PID 与 GPU worker 不同。

## 四项证据

1. 正常和 final ACK 重放：3 条实际 CUDA 算术结果正确。恢复时 `_launch_phase` 若被调用即失败；实际无新 worker、无新 CUDA 调用，原提交事实逐字保持。
2. 业务异常：在第 2 条 CUDA 运算/同步后真实抛错。原 GPU allocation 归还、进程退出、NVML 无残留。
3. 取消：4096 行源大于整条流水线有限 credit；第一条 GPU 结果到用户后立即关闭 iterator。实算 2 条后终止，源未结束，原非 final cut 已 ACK，设备归还。
4. 中间提交恢复：原非 final ACK 后测试抛出受控异常，真实关闭第一批 worker 后以同 RunRef 恢复。首 worker 处理 0、1；恢复 worker 处理 2…31；原 ACK 保留、最终 32 条精确原序、最终 ACK 确认。这里使用受控异常，不冒称 SIGKILL 或机器断电测试。

所有 5 次物理 GPU allocation 都有原 cleanup evidence 且状态 RELEASED；所有观测 CUDA PID 已退出，device pool charged 为空；最终 nvidia-smi 四卡均 0 MiB/0%。详情与原 JSON 摘要在 `physical-observations.json`。

## 有限资源和证据边界

每次 GPU map 只对 4096 个 float32 元素执行 CUDA 算术并 synchronize。正式证据中最大 torch allocated peak=49152 bytes，reserved peak=2097152 bytes。前置 NVML 采样观察到 CUDA 上下文约 616 MiB，这与张量/缓存口径不同，不能混称硬显存峰值。

公开 gpu=1 当前预约整张物理卡（85520809984 bytes），不是可配置的分数显存分配。本测试显式为 torch worker 申请 2 GiB 主存预约，全 run 给 8 GiB 有限上界；这是原资源准入，不宣称 RSS/cgroup/CUDA driver 硬隔离。没有扩大任何生产默认值。

不覆盖：四卡并行吞吐、Ray GPU、多机 GPU、网络分区、GPU 长内核强制取消、主机断电。其它 lane 的证据不可与本轮混算。

## 夹具调整记录

初轮总 run 默认内存不足以容纳显式 2 GiB worker 预约，补了同 run 的明确 8 GiB 上界；未改生产准入。pytest assert 重写影响 by-reference 函数字节，跨进程业务函数改为 if/raise 保留校验。torch.__version__ 原是 TorchVersion 子类，观测字段转普通字符串。取消源 128 行可被全部有限预读而到达 EOI，改成 4096 行才真正覆盖 source 未结束的 nonfinal close；未删除非final/原ACK/物理清理断言。前置红日志全部保留在 `/tmp/demiflow-physical-gpu-*`。
