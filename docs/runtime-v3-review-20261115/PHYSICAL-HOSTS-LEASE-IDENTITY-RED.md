# 两台物理宿主 CPU 验收：共享工件租约阻塞

生产候选：`5ff5da9e9cad3d595d84212b44c1cbd787e08dc4`；执行树 HEAD：`4b4dea15dbff91447593cfb9b2be06b215457810`。生产代码逐字相同，本批只新增可选验收测试与证据。不是两进程模拟两节点，不使用 Ray Data。

## 已确认与未确认

- 两个真实不同宿主分别为 `job-09b54ebad3bd-20260629073159-5584d65864-wcm7n`（`10.233.68.27`）和 `job-bb1b7400ff7f-20260828124445-68db7f5c68-hfxcn`（`10.233.122.251`）。各自 `/tmp` 不共享，共享源码目录可读。
- 双方共享 `env-ray` 的 Python 3.11.16、Ray 2.60.0、PyArrow 25.0.1、NumPy 2.3.5、cloudpickle 3.1.2。1036 个引擎 Python 文件的聚合摘要均为 `51bc6d8591e27261c00e4ebd8684e0760d099412c615bf701e42ed0e8c0e1034`。
- 双向直接 TCP 和私有双节点 Ray Core 实际任务成功；两端各 2 CPU、0 GPU，真实任务返回不同 hostname/node ID。临时认证值只存在于内存及 SSH 标准输入，不进入证据。
- 原 Dataset 四条测试：**1 passed、3 failed，27.060 秒**。通过项是“不共享的 `/tmp` 读取域在执行前拒绝”。三个正向用例均被相同真实租约身份缺陷挡在 worker 装配阶段，不能据私有集群成功宣称 Dataset 跨机成功。
- 正常 source→map→Lance ACK、杀本测试远程 actor 后沿原 cut 恢复、共享上游 union/repartition 多重性三项仍未验到成功。未发付费模型请求，未跑 GPU 推理，未模拟整机或系统网络故障。

## 核心缺陷

`dataflow_object_lease.ObjectLease` 把原 journal 登记的 `(st_dev, st_ino)` 同 worker 看到的值直接比较。同一个共享锁文件，在本机设备号为 `196`，远程为 `170`，inode 完全相同。三条用例分别观察到 inode `144115188562201620`、`144115188561641659`、`144115188562132391`，双方 `f_fsid=3`、nlink=1、size=0。双方读取的原 header 均是驱动登记的 `[196, inode]`。

真实 worker 因而抛出：`FENCED: Published object lease differs from original journal`。该错误不是挂载不可见，也不是代码不同。附加真实锁探针证明：本机持有同一共享文件的排他锁时，远程非阻塞共享锁确实被阻止。

修复需要版本化的跨宿主身份，同时保留每台机器自身 fstat/lstat 对替换、路径和持有 FD 的核验、原 journal 权威及 v1 历史边界。不得简单忽略设备号、关闭 GC 守卫或改测试断言。已交原协议 owner 实现，本提交不修改生产。

## 后续只读检查

- `WorkerGraphRunner` 在 Ray Core 下已经使用 `ray.util.get_node_ip_address()` 创建 peer 服务，`127.0.0.1` 仅是 Local 分支；当前未发现该处误用 loopback 的代码缺陷，但本轮 Dataset 尚未走到真实跨机 payload 交接。
- `DataflowRayConnector.placement_options` 使用真实 node ID 的 `NodeAffinitySchedulingStrategy(..., soft=False)`；读取域挑战也硬绑定指定节点并比对 runtime revision 与内容摘要。正向实际 placement/payload 仍须在租约修复后验证。
- 配置 GPU inventory 的 Ray 路径明确要求恰好一个 GPU 宿主，两个 GPU 宿主现为 `UNSUPPORTED_CAPABILITY`。这是源码限定，不能将本轮 CPU 集群或本机 GPU 证明计作跨机 GPU 完成。

## 重现与证据

测试：`tests/test_runtime_v3_physical_hosts.py`，只有显式 `DEMIFLOW_PHYSICAL_HOSTS_ROOT` 及自有双物理节点集群才启用。观察原 placement/ACK；故障只针对本测试 actor；不替换生产协议。

完整自有集群启动/关闭、环境、命令及原始输出：

- `/tmp/demiflow-physical-hosts-20261116/preflight.py`、`preflight.json`
- `/tmp/demiflow-physical-hosts-20261116/cluster_smoke.py`、`cluster-smoke.json`
- `/tmp/demiflow-physical-hosts-20261116/run_r1.py`、`run-r1.log`、`r1/result.xml`
- `/tmp/demiflow-physical-hosts-20261116/lease_probe.py`、`lease-probe.json`

配套 JSON 保存精确测试节点、完整失败栈、1785 条执行输入摘要、原始文件摘要、双方实际宿主/节点及关闭事实。测试前后 `changed_inputs=[]`。两个私有集群均正常关闭：远程进程 exit=0、远程子进程为空、本机 `ray.is_initialized()==False`、本机子进程为空。没有调用全局 `ray stop`。

唯一共享测试根为 `/yzp/zhaozy/yangzepeng/0905/_demiflow/physical-host-tests/cpu-6d81795387bc3bdb`，附 owner 清单。当前暂留原失败工件供修复对照；完成后只清理该自有目录，保留最小原始证据归档。主仓、共享 Python 环境、其他服务均未修改。
