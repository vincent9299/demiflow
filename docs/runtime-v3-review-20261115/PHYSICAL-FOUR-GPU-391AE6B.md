# 本机四张真实 GPU 并行与隔离验收

固定提交 `391ae6b19e936bbf99758b8a8a82b370576db38c`；隔离分支 `engine/gpt-v3-artifact-recovery-20261115`。生产代码与 root `3f989a0` 相同，只新增 `tests/test_runtime_v3_physical_four_gpu.py`。无主仓、环境、服务或生产代码改动。

正式 3 passed，0 skipped，0 failed，74.25 秒；1788 份 Python 输入摘要与 HEAD/dirty 状态前后全等，实际导入隔离树。完整命令/摘要见 `verification.json`；XML `/tmp/demiflow-four-gpu-final.xml`，日志同名 `.log`。

## 真实链路

同一公开 Dataset 声明共享 CPU 前驱，分为四个独立 GPU map 节点，经 union 汇合再进 CPU map，最后原 take_all / RunJournal / CommitCoordinator。每个 GPU 节点原 `gpu=1`，没有假 inventory、没有模拟 CUDA、没有旁路执行器。inventory 从实际 nvidia-smi 读取四张 H100 80GB 的 UUID/容量。

GPU 回调首先进行 4096 个 float32 元素的真实 CUDA 算术并 synchronize，然后停在有界测试 gate。只有 driver 侧同时观察到四个真实 CUDA PID、四个不同 UUID、原 DevicePool 的四个 active allocation 与 PID 内回报的 allocation_id/可见设备逐一对应，才释放 gate。

每一场景都实际观察到四张卡各一个 CUDA 进程，NVML 各 616 MiB；各回调 `torch.cuda.device_count()==1`。CPU 回调可见设备为空、PID 与 GPU 回调不同。正常场景四条结果数值与分支标识完整，原 final ACK 确认。

## 三个场景

- normal：四个真实 GPU worker 同时存活；CUDA 结果经 CPU 汇合输出，原 ACK 成功。
- failure：四卡全部完成实际 CUDA 并确认同时预约后，一个业务分支抛错；四份原预约全部释放，四个 CUDA PID 均退出。
- cancel：同样在四卡实际计算/预约已确认的时刻，经公开 `RunContext.cancel(ref)` 请求取消；有界回调放行后由原控制和清理路径收尾，四份原预约全部释放。

三场景合计 12 份原 GPU allocation 均 `RELEASED`，有原 cleanup evidence；原 pool charged 为空，全部 CUDA PID 已退出。最终 nvidia-smi 四卡均 0 MiB/0%。原事实及各证据文件 SHA 在 `physical-observations.json`。

## 有限范围

每个 GPU worker 显式申请 2 GiB 主存，总 run 12 GiB、最多 12 workers，原限制未修改。GPU 是每节点独占一整张实际卡；每节点张量峰值 49152 B、torch cache 2 MiB，NVML CUDA context 616 MiB 分开记录。这是实际有限测试载荷与原准入，不能冒称硬 VRAM/cgroup/RSS 隔离。

本轮证明四个显式 GPU 逻辑节点的同机并行与隔离；不声称单个 map 自动扩为四副本，不声称跨 host GPU，不是吞吐基准或长 CUDA kernel 强制取消证明。单卡 final/nonfinal ACK 恢复由前一份 `a8544ba` 的真实证据单独证明；本轮不混算恢复次数。

本lane已释放所有卡，后续双主机 GPU 由总控另行协调。无全局 ray stop，没有接管或停止其它服务。

原证据摘要同目录 `PHYSICAL-FOUR-GPU-391AE6B.json`。完整1788文件前后摘要保存在所列 verification.json，摘要文件记录其SHA-256。总控已独立对照root生产3f989a0，0不符；后续f9af1eb未冒称本组已重跑。

CPU前驱的单一PID仅证明worker身份一致，不能由此证明调用次数为1；共享上游执行一次的语义另由原专项验收，本四卡报告不借PID推导次数。
