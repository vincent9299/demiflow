# pplx CUDA Graph 集成复核（2026-11-01）

范围：pplx 单 GPU 独立请求、CUDA stream 与精确形状图缓存。显式启用，batch
仍关闭；不改变 BF16、权重、原生分块、选项读出和概率校准。方案在
[pplx-cuda-graphs-design.md](pplx-cuda-graphs-design.md)，配置见
[model-services.md](model-services.md)。未 commit/push。

## 架构和实现 review

- 配置仍只有 `model.deployment.options.cuda_graphs`，解析惰性。HTTP 准入、GPU
  资源锁、supervisor、按行预算/journal/重试仍复用原有实现，没有第二套行执行器。
- `PplxGraphAdapter` 仅包装原生 logits 执行；原生 `decide`、encoder、`_batches`
  和校准继续生效。预编码结果只在当前请求中复用，不做跨请求结果缓存。
- 每条 lane 有固定 CUDA stream 和常驻执行线程；参数共享，可变模块状态独立。
  提交前必须获得 lane 与工作区预算，一个 lane 至多一个任务，线程不另存待执行队列。
- 捕获、检查、销毁使用独占门闩；正常执行允许多个 GPU reader。关闭拒绝新请求、
  排空已获槽请求、释放全部图，再结束执行线程。
- 首次扩大测试确实发现生命周期错误，而非仅检查输出：同 stream 图淘汰会触发
  Torch 清理共享 cuBLAS workspace，可能使其他图访问已释放地址。现按当前
  Torch Inductor 的方式将 workspace 捕获到图私有池，独占清理外部缓存引用。
  后续连续关闭/重建还曾发生原生进程崩溃；将底层库线程局部状态的生命周期
  绑定到 lane 后，重建、跨调用线程和缓存回收纳入重新验收。未修改安装的 Torch。
- 缓存策略初版过度捕获，800 次调用花约 49 秒捕获，不能作为可用性能结果。
  默认阈值改为 8；有界频率优先、同频 LRU，低频新长度不能替换更热的图。
  缓存条目、字节、热度表、累计捕获次数和时间均有限。
- 默认新图精确对照，之后每 64 次重放再对照；失败时本次返回原生值，丢弃全部
  图并停用图加速。`verify_every: 1` 支持逐次对照。抽查及有限样本不构成对
  所有未来输入的数学证明；不能将其表述为全域、全版本质量认证。

## 已完成的原生/适配器验收

只使用空闲 GPU 1；每 0.2 秒 NVML 采样时核对 GPU compute PID，确认是本探针。
GPU 0、2、3 和既有服务未被操作；GPU 0 原有 d3 后来由外部退出，并非本任务停止。

输入为原有 100 条真实身份判断、100 条分流/多问题测试、36 条由业务文本构造的
更长上下文和 1 条图片 fixture。共 237 条、144 种 `(token 长度, 读出数)`，
106–7,538 tokens。没有预选 8 个形状、补齐长度或截断输入。长上下文扩展是
边界样本，不能冒充额外采集的真实线上长文本分布。

- 默认配置：200 + 600 + 600 次混合调用，完整 answers 与 usage 均逐项原生一致。
- 扩展质量：237 × 3 = 711 次调用精确一致；受有限缓存/热度策略影响，实际
  捕获 27 种形状、54 张图，38 次淘汰。其他形状使用原生路径。
- 64 MiB 缓存无法容纳图时，12 次原生回退一致，没有尝试超预算 capture。
- >8,192 token 请求在 GPU 执行前明确拒绝，不截断，实例继续健康。
- 注入错误图输出：当次返回原生值，缓存归零，parallel 降到 1；后续 12 次
  原生调用一致。这一人为注入计数不属于模型自发数值变化。
- 原生 2-chunk 请求的分块和输出一致，未改变内部 batch 计算。
- 为排除“该长度只测试了回退”的漏洞，另外按形状清空缓存强制冷捕获：
  144 组共 288 次比较；139 种符合图条件的文本形状都实际捕获并精确一致，
  长度 106–2,826，其中 16 种有多个问题读出。其余为图片或超过图长度上限，
  保持原生。该轮专测覆盖与释放，不是吞吐配置。
- 四流上限另外验收 144 次：答案和 usage 完全一致，15 次捕获、7 次淘汰，
  无质量降级。默认仍为两流；四流通过质量测试不代表吞吐必然更高。
- 本轮原生 GPU 过程峰值：Torch reserved 59,382 MiB、NVML 61,101 MiB；
  Torch 限额 65,536 MiB。缓存上限单独为 4,096 MiB，实际计费未超限。

原生混合基准 9.98 条/秒，默认冷启动 10.45 条/秒；后两轮为 9.78 和 11.09
条/秒，仍在捕获新图，不能称为充分热态吞吐。捕获期间 p95 会升高，不能只
给热图命中吞吐。CPU 回归 208 项通过，含部署、服务生命周期、批量禁用、
SystemOne 图片、prompt、journal、重放、缓存边界、关闭排空与故障降级。

## 正式服务验收

通过正式 `prepare → DeploymentService → model_worker`，原生环境只读接入，
运行时为 Python 3.12.15 / pplx-decider 0.3.0 / Torch 2.14.0+cu130 /
Transformers 5.17.0。各轮重新建立 HTTP 连接线程。2,724 次 HTTP 质量比较的
答案、概率和 usage 全部与原生一致，batch 始终关闭，parallel=2。

| 负载/阶段 | 请求数 | 条/秒 | p50 / p95（秒） | GPU 平均 / 中位数 |
|---|---:|---:|---:|---:|
| 完整 237 条扩展集，冷启动 | 237 | 8.07 | 0.177 / 0.696 | 64.06% / 58% |
| 完整扩展集，后续 3 轮，仍捕获 | 711 | 8.53 | 0.173 / 0.750 | 72.48% / 76% |
| 完整扩展集，再 3 轮，仍捕获 | 711 | 10.35 | 0.166 / 0.679 | 80.19% / 92% |
| 12 种常见形状的 71 条输入，预热 | 213 | 22.89 | 0.063 / 0.260 | 88.62% / 94% |
| 相同热点，持续供给且无新捕获 | 852 | 26.07 | 0.062 / 0.207 | 96.16% / 98% |

热点阶段持续 32.69 秒，41.36% 采样值为 100%；期间仍进行了 6 次原生质量
抽查。热点子集与完整扩展集不同，不能用它们计算加速比；也不能与此前 8 形状
原型直接比较。正式路径这批混合数据还有冷形状和长输入回退，未达到持续 100%。
HTTP 过程峰值 NVML 为 61,015 MiB，Torch reserved 为 59,296 MiB；缓存最多
32 条，所记录的计费字节始终低于 4 GiB，累计捕获预算仍有效。

同一服务再完成 1,000 条带独立 smoke_id 的身份调用：journal 恰好 1,000 请求、
1,000 响应、1,000 usage，HTTP/transport 错误均为 0，用时 88.38 秒。关闭服务
后，禁止任何 DeploymentService.bind，再回放 1,000 条，输出相同、没有新增
请求，耗时 0.53 秒。这一 GPU smoke 直接使用生产 AsyncOperatorLLMRuntime；
Dataset/map_prompt_async 的连接与调度另外由 CPU 回归覆盖。

原始正式服务结果：`/tmp/demiflow-graphs/service/report.json`、`service.log`、
完整 `prompt.yaml` 和 `calls.sqlite`。

## 有限资源和性能短板

默认 32 个图条目覆盖所有 lane；4 GiB 缓存的计费口径为图私有池 reserved
字节及静态张量逻辑字节，普通 allocator 的碎片归总限额。64 GiB 总限额约束
Torch allocator，不包含驱动及所有第三方分配，至少保留 2 GiB 设备余量。
不能把这两个值说成整进程 NVML 显存硬上限。

冷门形状、CPU 编码/原生 kernel 发射、图捕获暂停、有限缓存，以及较长输入的
原生回退仍限制吞吐。累计捕获预算耗尽后不再学习新图，这保证开销有限，也
意味着输入分布长期改变时需要重新评估部署。后续应先用真实输入分布测命中率、
吞吐与 p95，再评估有限预热或原生融合 kernel；任何 kernel/精度变化都重新
执行质量验收，不能因“原生 fast path”就默认与当前概率一致。

NVML 的 GPU 利用率衡量采样期内执行 kernel 的时间比例，100% 不代表全部
SM、Tensor Core 或显存带宽均达到峰值。解释依据见
[NVIDIA NVML 文档](https://docs.nvidia.com/deploy/nvml-api/latest/api/structnvmlAccountingStats__t.html)。
性能目标是质量保持、资源受限时的有效吞吐和可接受延迟，持续 100% 不能对
所有输入分布及到达率承诺。

## 复验入口与原始证据

- CPU：`tests/test_pplx_graphs.py` 及本轮相关 15 个测试文件；完整结果在
  `/tmp/demiflow-graphs/cpu-final-lifecycle.xml`。
- 原生：`/tmp/demiflow-graphs/native/report.json`、`native-final.log`、`cases.jsonl`
  和 `baseline.json`（后两者在 `native/` 内）。首次失败日志也已保留。
- 全形状强制捕获/四流：`/tmp/demiflow-graphs/all-shapes/report.json`；实际运行
  文件为 `/tmp/demiflow-graphs/all_shapes.py`，日志 `all-shapes.log`。
- 可复用 GPU 质量脚本：`benchmarks/pplx_cuda_graphs.py`，显式指定空闲卡、权重、
  审核过的 JSONL 和新输出路径，在 pplx Python 环境运行。默认不进入 CPU 回归。
  `--cold-shapes --streams 1` 强制逐形状捕获；普通模式检查混合调用/调用线程更换。
  运行时快照为 `/tmp/demiflow-graphs/implementation-sha256.json`，验收后核对一致。
- 本轮起点的 86 个 dirty 文件保存在
  `/tmp/demiflow-graphs-baseline-20261031T161541`，未覆盖既有改动。
