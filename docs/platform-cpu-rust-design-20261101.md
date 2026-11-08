# 平台 CPU 性能分析与 Rust 实验计划

范围为 demiflow 平台请求链路；不修改业务 pipeline、共享模型环境、已有服务或
GPU 2/3，不 commit/push。本轮开始保存 108 个既有改动文件至
`/tmp/demiflow-cpu-rust-20261101/baseline`，所有实验使用独立目录与自有进程。

## 先测量，再决定替换边界

1. 用真实本地模型测量 HTTP 服务、算子 runtime、journal 写入与离线重放。
   覆盖不同长度和冷热缓存，区分首次准备/启动、正常推理与重放。
2. 同时记录端到端 wall time、各进程 CPU 时间、线程 CPU profile、阶段 wall/CPU
   时间、请求分位数、缓存状态和显存。线程等待不能计作 CPU 工作；嵌套阶段不
   直接相加。诊断 profiling 与无 profiling 对照分开，记录探针开销。
3. 根据真实平台函数的独占 CPU 时间选择一个热点，说明外部模型/库耗时的边界。
   先写出 Rust 最小设计并复核，再执行实验；不以语言偏好预选替换整个 runtime。
4. 实验保持业务 API、请求身份、输出、质量门槛、预算、取消与恢复语义。Rust
   只替换所选组件的一份实现，不新增调度器、后台队列或服务代理。
5. 分别报告函数微基准、CPU 受限链路和真实 GPU 链路。没有端到端显著收益时
   保留实验与结论，不把候选实现默认接入生产路径。

## 资源与验证边界

每次仅启动一个模型实例，显式 GPU 1（空闲检查与既有协作锁），模型沿用已验证
的固定两通道、图缓存 4 GiB、allocator 64 GiB/JEV 74 GiB。HTTP 并发最多 4，
CPU 对照最多 8；单次阶段最多 4096 行，固定 200 行输入样本，单输入最多 1 MiB，
总输入最多 32 MiB。原有服务端/token/质量限制继续生效。

统计按阶段聚合，延迟样本每函数最多 4096 个、函数最多 128 个；CPU profile
只针对显式测量窗口，输出最多 50,000 个函数记录。单个模型任务超时 20 分钟。
Rust 工具链/构建缓存仅放实验目录，编译并行度最多 2；实验磁盘总预算 4 GiB，
profiling 证据目标小于 512 MiB，不下载模型或重建共享环境。

最终设计/实现 review、数据与是否采用 Rust 的决定见
[实验 review](platform-cpu-rust-review-20261101.md)。

最终必须包含当前源码绑定的基线、候选/原实现交替对照、精确输出比较、相关
CPU 回归、资源清理和工作树保留审计。冷缓存/不同缓存热度或 profiling 与非
profiling 的数据不直接用来宣称 Rust 加速。

## 测量后的设计 review（实现前，2026-11-01）

实际 JEV 请求的 worker 采样以 vLLM、Torch、Triton、FlashInfer、模型算子为主；
不能把 CUDA 同步轮询消耗的进程 CPU 时间算作平台 Python 热点。平台 graph
cache、编码预算、HTTP 回复不是目前的大头。GPU worker 不进入本轮 Rust 范围。

发现两个可独立处理的点：

* `transport_context` 每次请求都构造并校验 `DeploymentService`，即使租约已存在。
  将构造移入首次 bind 的分支，复用原有 `_service_owners`；仍逐次检查冲突配置、
  ensure_ready 和请求准入，不增加缓存，也不提前启动模型。
* schema `_validate` 是重放链路中独占 CPU 靠前的平台函数，但绝对占比有限。
  真实 1000 行重放中，轻量计时约 17 ms / 705 ms 客户端 CPU；真实 JEV
  runtime 200 行约 6.4 ms / 520 ms CPU。Rust 实验选它，不能预先声称提升 GPU 吞吐。

Rust 仅做已编译 schema 的无副作用接受谓词，通过 PyO3 直接读取 Python 对象，
避免 JSON 再编码。支持精确内置 JSON 类型；不支持的 schema、子类/Mapping、
失败实例均返回 Python 原校验器，保留错误类型、文本、顺序和所有现有语义。
字符串按 Unicode 字符数计数，整数不转浮点，数值比较沿用 Python 比较语义。
编译最多 12 层、512 节点，快路径遍历最多 65,536 节点；超限回退，无新增全局缓存。
实验持有一个独立 schema 快照，每次先检查当前 schema 与快照一致，避免可变
schema 的陈旧缓存漏检。快照/编译状态只属于本次 benchmark 生命周期。

review 结论：边界可保持干净，允许实现 benchmark-only PoC；不修改模型执行、
输入、journal 身份/存储、取消、队列或服务接口。Python 原实现、仅消除多余
类型判断的 Python 对照、Rust 三路都测；编译收益不冒充语言收益。须做差分测试，
覆盖无效响应、错误兼容、Unicode、bool/int、超大整数、NaN/Infinity、可变 schema
及预算回退。若真实链路收益小于噪声，不引入正式依赖或默认开关。
