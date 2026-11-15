# 资源等待、长任务授权与数据保真修复检查点

本批是继续实施，整体仍未完成。产品 root：统一、真正可用的工作台，看懂、配置、运行真实 pipeline，分析、比较、评测数据。平台 root：正确、稳定、高性能、通用、架构干净的统一计算引擎。当前优先修复直接阻断真实 Dataset 链路的行为，不以局部计数替代两个 root 的完整验收。

## 实现与边界

- Local 复用原 LocalCapacityPool、ResourceManager、RunResourceState。独立 run 默认按同用户/主机/cgroup 共享部署域，容量来自 CPU affinity/cgroup 与物理内存/cgroup；`DEMIFLOW_RESOURCE_ROOT` 可指定部署域。嵌套工作者使用父分配 envelope。测试全部显式使用本任务 `/tmp/demiflow-*` 域，未接管生产 pool。这是逻辑资源准入，不冒称硬 RSS/CPU 隔离。
- 同一 allocation 在容量暂不足后继续重试，不另起身份、不重复收费。原生资源状态在收到明确 PENDING 后释放长期 I/O claim，改为有界退避；未确认的未知 provider 结果没有这个例外。启动在所有参与者获准前不激活业务；超过准入期限明确失败、收尾。
- 从未拿到容量的请求不再因“没有 reservation 可释放”而无法确认退出。已有 bound allocation 仍须匹配原物理退出证据，不释放未知占用。
- 当前 driver 仍在一个 cut 内保留所有参与者：若总驻留超过容量，业务执行前明确拒绝，避免按纸面分期方案进入永远无法完成的等待。**有限通道跨阶段分期驻留仍未实现，不能据此宣称调度优化完成。**
- 长任务通过原有控制通道续期，要求 driver 继续 pump 且保持原 run/attempt/fence。worker 校验分配/boot/所有者和严格递增期限；过期授权不可复活。driver 的每次 journal 命令也检查实际登记且未过期的 worker。续期不重试业务、不扩大请求预算。
- Ray 的读取域探针真正运行在指定节点，核对该节点代码版本、节点身份和本 run 的 durable challenge。放置按每台主机 CPU/内存预算计算，经 Ray Core NodeAffinity 绑定实际节点。去掉未曾创建的 `ray-object://<digest>` 假位置。**当前仍只实测本机 Ray 集群；GPU/跨物理机没有验收，不能宣称完成。**
- NumPy 原始 dtype/shape/字节通过纯数据 codec 保存，包含标量、数组、负零、空数组、复杂数与日期。只接纳基础 data-only dtype，禁止 object/structured dtype；内部数组以不可变 bytes 为底层，业务回读复制，避免 caller 修改已封存 Record。公开批次→worker 交换→materialize→中断恢复真实通过。未改原有值的编码公式。
- 分组转排序前，配置在 JSON 边界完整解冻；已提交 channel cursor 的 GC 读取兼容正式只读映射。RawFetch 的 ACK 已由原生 journal 永久确认，driver 改保留计数，不再额外无限累积 commit_id 列表。
- K/X/D/T 验收逐步改用真正的 DTO、sha256 身份、typed state codec 与原生 sink 入口。原预算、拒绝、损坏、重复、GC、故障恢复反例保留。更早拒绝的非法 DTO 将构造放进原拒绝断言；没有关闭正式校验、添加 xfail 或删除负例。

## 证据

所有 pytest 显式绑定本工作树与 tests 导入路径，测试数据留在 `/tmp/demiflow-*`。无在运行 worker 时改代码或提交。

| 文件 | 实测结果 |
| --- | --- |
| `/tmp/demiflow-resource-progress-2.xml` | 28 passed：共享容量、真实 PENDING 后原 allocation 启动、超时无业务副作用、短 TTL 长调用续期，以及原 Local provider/资源一致性用例。 |
| `/tmp/demiflow-ray-native-v3-5.xml` | 1 passed：本机真实 Ray Core，远端读取探针及节点绑定后 Prompt→sort→提交，Ray Data 入口被禁止。 |
| `/tmp/demiflow-checkpoint-r3.xml` | 80 passed：D/K/X/T 全部用例，加资源 5 项与 NumPy 2 项。D004 含真正子进程退出的提交恢复窗口；未用 mock 提交代替。 |
| `/tmp/demiflow-connected-v3-3.xml` | 84 项组合：83 passed，1 failed。失败为旧控制测试未登记实际 worker lease，被本次新增授权校验正确拒绝。 |
| `/tmp/demiflow-control-r4.xml` | 上述控制测试登记实际 lease 后 1 passed。原重复请求、冲突拒绝、未读回复背压、读后重发断言保留。不冒充上表一次全绿。 |

## 尚需继续

队列生产端的零/多输出 source-unit、EOS 和恢复；搜索/文档/managed 模型与 Agent 多轮组合；剩余 lane 与历史回归；多机/GPU 与分期驻留；崩溃后孤儿 staging 的安全回收；工作台完整产品场景与最终意图对照。原指定 33 项已在前批通过，本批没有以“33 项通过”掩盖更大范围仍未完成。
