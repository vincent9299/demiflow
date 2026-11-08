# JEV 正式并行后端：实现复核与 GPU 验收

本轮承接 [前次诊断](model-runtime-latency-review-20261101.md)，按
[后端设计](jev-runtime-design-20261101.md) 完成正式接入。证据目录为
`/tmp/demiflow-jev-integration-20261101`，包含失败实验，不覆盖历史基线。

## 实现与架构复核

`deployment.performance: optimized` 现在为四个本地模型选择各自的图执行后端。
JEV 使用一个托管 HTTP worker、一个进程内 vLLM、同一份权重；原生 HTTP 配方
仍保留。prepare、action/persistent、服务身份、算子与 journal 沿用平台原接口。

- 公共 `cuda_graphs.py` 继续承担 FIFO 准入、固定执行通道、工作区预约、图缓存、
  淘汰、质量退化和关闭。没有增加一套服务调度器或业务队列。
- `jev_runtime.py` 封装 vLLM 私有状态、元数据复制与固定版本的 FlashInfer 修复；
  `jev_graphs.py` 管理原生对照、捕获和独立重放。模型协议的格式化/校准只有一份。
- 默认两条固定 CUDA stream/线程；各通道独占 GDN state、KV、输入和读出索引。
  已验证图同时重放，只有权重共享。原生 runner 与捕获持有独占执行门，不能与
  重放重叠；原生冷路径仍为一个 forward。`batch=false`，没有跨请求合批。
- 全局 Python ForwardContext 只在独占阶段使用；图重放不执行该 Python 状态。
  读出保留原生 `compute_token_logprobs` 的 gather 宽度和 FP32 路径，校准未改。
- 首次/周期对照同时检查有效 hidden state 和 processed logprob，要求精确相等。
  不一致时返回该次原生结果、清空并禁用图、把实际并行能力降为 1；CUDA 执行
  错误停止接单。禁止将质量失败悄悄当作已验证并行。周期抽检与回归是验证范围，
  不是对所有未来输入的数学证明。

固定版本：vLLM 0.31.0、Torch 2.13.0 对应已审阅 commit、StartLux 1.1.0、
FlashInfer 0.7.0.post1、CUTLASS DSL 4.7.1，单 SM90 GPU、bf16、文本输入。
FlashInfer 两个 SM90 模块源文件校验通过后，才在本次拥有的 worker 内隔离 scratch。
不修改安装包、权重或已运行服务；版本不匹配明确拒绝 optimized，不能盲目打补丁。
未来模型可以复用公共执行策略，但必须提供对应后端与质量验证，不能承诺任意
新模型自动获得同样加速。

## 资源复核

默认 JEV allocator 74 GiB，图缓存 4 GiB/32 项，最多 128 次捕获/60 秒累计捕获
预算，热度门槛 8；图最长 4096 tokens，整行最多 8192 tokens。时间预算限制是否
开始下一次捕获，不会中断正在执行的 CUDA 捕获。缓存、形状历史、HTTP 在途数、
固定 stream/线程均有上限。

每条通道状态先计费再分配，上限 1 GiB；本次两条通道合计 1,335,099,392 bytes。
每份输入/元数据复制最多 128 MiB，原生问题展开也检查整行 token 和编码字节预算。
超限明确拒绝或按已声明策略走原生，禁止截断。allocator 上限包含它管理的权重、
缓存和状态，不等于驱动/所有第三方分配的硬上限；启动保留设备余量并测量 NVML。

cuBLAS 工作区按固定通道释放绑定，不能全局清除仍被原生 vLLM 图引用的工作区。
所有图内索引张量存活到图销毁，淘汰前排空 GPU。未采用反复创建临时 stream 或
无限扩大缓存来规避生命周期问题。

## 本轮定位并修复的 Torch 崩溃

正常 Python allocator 下，多次混合长度/缓存淘汰实验在 Python 整数分配或
OrderedDict 遍历中发生 segfault。开启 `PYTHONMALLOC=debug` 后通过不能算修复；
改为原生线程统一捕获仍失败，排除了这种线程改动作为解决方案。

实际安装的 Torch 源码
[Module.cpp](https://github.com/pytorch/pytorch/blob/cf30153c4c131c8164ee7798e5022d810682e2cb/torch/csrc/cuda/Module.cpp#L741)
在 `THCPModule_memorySnapshot` 中，用 `THPObjectPtr` 接管了
`PyTuple_GetItem` 返回的借用引用。两项和三项 pool 参数都受影响；销毁该包装器
会错误减引用。Python 3.12 的小整数为 immortal，早期小图池 ID 掩盖问题；图池
ID 超过 256 后可破坏 Python 堆。JEV 的原生图及通道工作区清理也消耗图池 ID。

无 GPU 的 [最小复现](../benchmarks/torch_snapshot_refcount.py) 在三个现有模型环境
全部确认：一次调用后，两个大整数引用计数均从 11 降为 10。受影响的本机版本为
JEV Torch 2.13.0+cu130，以及 pplx/AMOS/d3 Torch 2.14.0+cu130。额外引用保活，
诊断后补回实测缺失的引用；不修改包。`snapshot-refcount.json` 保存逐环境结果。

正式修复统一在 `graph_pool_bytes`：读取 allocator 的
`reserved_bytes_by_private_pools`，避免有缺陷的 filtered snapshot，仍按私有池
实际 reserved bytes 计费；统计缺失则失败，不能按零字节放行。四个后端均使用
该公共函数，没有扩大显存预算。[上游候选补丁](../benchmarks/patches/torch-cuda-snapshot-borrowed-reference.patch)
另行保留，已在对应源码副本通过应用检查；未编译或安装到共享环境。

GPU 最小压力测试完成 400 次捕获/重放/销毁，最后 pool ID 为 400，每次计费均与
安全的无过滤底层快照一致；最多保留 8 张图，128 MiB allocator 上限，峰值
23,068,672 bytes。随后正式 JEV 服务在普通 allocator 下完成下述完整回归。
该缺陷解释本轮长时间运行崩溃，**不能用它解释历史数值漂移**。

## 正式服务 GPU 结果

`service-v6-fixed/report.json`：普通 Python allocator、GPU 1（NVIDIA H100 80GB
HBM3，驱动 590.48.01），200 条不同业务行
反复混合并更换 HTTP 连接线程；与独立原生启动保存的当前 200 行基线比较。
所有答案、概率和 usage 要求完全相等，没有放宽容差。压力配置仅把热度门槛改为
2、累计捕获预算改为 180 秒，以更早覆盖捕获/淘汰；其他预算保持默认。

| 阶段 | 行数 | 客户端并发 | 吞吐（行/秒） | p95（毫秒） | GPU 利用率中位数 |
|---|---:|---:|---:|---:|---:|
| 多形状冷缓存 | 200 | 2 | 8.35 | 609.0 | 48% |
| 多形状预热 | 600 | 2 | 13.78 | 325.2 | 57% |
| 多形状稳定阶段 | 2000 | 2 | 17.30 | 250.1 | 64% |
| 高频输入预热 | 200 | 2 | 36.32 | 109.0 | 98% |
| 高频输入单请求 | 300 | 1 | 31.65 | 31.0 | 91% |
| 高频输入并发请求 | 600 | 2 | 36.50 | 99.6 | 98% |

3,900 次性能请求零差异；非法请求 422 后正常行继续成功。高频阶段来自这批
业务输入中的 50 行/6 类形状，只作为热缓存测量，没有为这几类形状硬编码实现。
并发高频吞吐比单请求高约 15.3%，响应延迟也上升；GPU 100% 的采样占比约 19.5%，
不能描述成始终 100%。长尾原生回退、质量复核和捕获仍限制全局收益。

性能阶段累计 94 次捕获、66 次淘汰，峰值图缓存 4,165,990,764 bytes，未超过
4 GiB；Torch allocator 峰值 75,491,180,544 bytes（70.31 GiB），NVML 峰值
71,415.375 MiB（69.74 GiB，两者采样方式不同），无图质量失败或执行退化。

随后通过真实 `AsyncOperatorLLMRuntime` 执行 1,000 行身份 smoke：1,000 请求、
响应和 usage 记录，零 HTTP/transport error，耗时 67.25 秒。关闭服务后重放
1,000 行，输出一致、请求计数不变、0.573 秒；测试把服务绑定改为立即报错，确认
重放完全来自 journal。smoke 用于链路/身份/持久化验收，不冒充额外原生质量基线。

### JEV 边界与实际 GPU 并行

`boundaries-final/report.json` 使用同一份最终源码和 GPU 1 独立启动。选项数
1/3/7/16/63/128/255，实际输入长度 94/114/154/256/820/1656/3434，以及额外
2262/4262/7062 token 输入，各重复三次，答案和 usage 与原生精确一致；长于
图预算的输入按原生路径执行。10262 token 输入和极大编码请求在 GPU 提交前
拒绝，没有截断。

同形状不同内容反复缓存复用，周期 hidden/logprob 对照通过。热图 profiler
记录 stream 33/37 共 3376 个 kernel，2895 对跨 stream 时间区间重叠，证明
单 GPU 上确有独立 forward 重叠；重叠对数不是吞吐倍数。

图缓存降至 64 MiB 后不再捕获，原生答案保持一致。故意把重放 logprob 加 0.125，
逐次质量检查捕获失败，返回原生正确结果，清空/关闭图、能力降至 `parallel=1`；
之后不同请求仍精确匹配原生。测试结束关闭自有 runner、释放 GPU。

### 持久服务完整生命周期

`persistent/report.json`：通过真实 CLI `prepare → start → status → logs → stop`
启动单组件 optimized JEV，配置使用默认图预算/热度策略，独立 revision 和测试
服务目录。4 个客户端并发执行 200 条业务行，答案、概率和 usage 全部精确匹配
当前原生基线；能力声明 `parallel=2`、`batch=false`，无图退化。显式 stop 成功，
测试确认 GPU 1 已无计算进程。加上前述 action/journal，两个生命周期均已验收。

### 公共修复的三模型回归

`{pplx,amos,d3}-pool-fix-v2/report.json`：在 GPU 1 顺序执行，每个模型重新建立
200 条原生基线，再运行冷缓存/混合/排队各 200 条；客户端并发分别为 2/2/4。
各 600 条优化请求全部精确匹配完整原生答案/usage，没有内部 mismatch、禁用或
执行失败。新原生基线还与上一轮独立启动保存的 200 行逐项相同。

| 模型 | allocator 峰值（GiB） | 图缓存峰值（GiB） | 捕获次数 |
|---|---:|---:|---:|
| pplx | 52.24 | 3.13 | 36 |
| AMOS | 55.27 | 3.17 | 36 |
| d3 | 55.79 | 3.83 | 27 |

保持公共默认的 64 GiB allocator、4 GiB/32 项缓存及两条执行通道。该轮重点
验证公共计费修复和质量，只有 600 条预热/执行；不能与前次 1,600 条长测的热度
状态直接配对宣称尾延迟改善。前轮 FIFO/减少预热的性能对照仍见前次诊断报告。

## 历史跨启动数值漂移的结论边界

历史旧基线与当前基线仍有 80/200 行不同（2 行标签不同，最大概率差约 0.01562）。
本轮没有重写旧基线。已定位的原生敏感点是第一层 `in_proj_ba` BF16 GEMM：
同一输入和权重，工作区改变导致 split-K 与非 split-K kernel/归约路径不同；
实际投影 tensor 和完整模型指纹支持该机制，协议格式化与校准没有改变。

额外控制实验：从进程启动设置 4 MiB 工作区、优先 cuBLASLt，分别得到相同的
另一组结果（相对当前 43 行不同、相对历史 101 行不同）；禁用 BF16 reduced
precision reduction 与当前 200 行完全相同；禁用 split-K 的尝试被当前非 Lt
后端拒绝，不计作成功。输入、字典顺序、格式器和实际业务请求核对一致。

这些实验排除了若干猜测，但没有完整复现历史 200 行。旧进程未保存层 tensor、
kernel、工作区轨迹，**历史那一次究竟由什么触发切换仍未证实**。不能声称已根治
所有跨启动变化，也不能把修复 FlashInfer scratch 冲突等同于解决历史漂移。
当前并行质量结论绑定已审阅运行时和当前原生基线；数值环境/依赖/适配器进入
执行身份，旧 journal 和旧基线保持原样。

## 无 GPU 回归与操作边界

16 个相关测试文件共 269 passed（75.12 秒），覆盖服务生命周期、prepare/身份、
算子/journal、图片 state、HTTP、并发/FIFO、质量退化和资源边界。新测试检查
JEV 双重放可以同时进入执行门、质量失败如实退化、展开请求提前拒绝、元数据
复制预算与别名、单组件端口、运行时变更失效，以及安全的图池计费。

GPU 测试只使用 0/1，自有任务启动前检查空闲/协作锁。GPU 0 中途出现外部图像
服务，一次 pplx 加载因此 OOM，失败保留，后续转 GPU 1 顺序执行。没有停止该
外部任务、既有 pplx/AMOS 或 GPU 2/3 任务。未 commit/push，未修改模型安装环境。
工作树保留情况由本轮开始的 101 文件快照及最终增量审计记录核对。

最终证据索引：

- `runtime-source-final.json`：服务源码 SHA-256，adapter version
  `3852ce3d491fee254f326a57c00bbc4a4a623f8725d2b4e69c97cccf96c8ee20`。
- `cpu-final.xml` / `cpu-final.log`：269 项相关无 GPU 回归。
- `service-v6-fixed/`：正常 allocator 的长时间 HTTP、1k journal 与无服务重放。
- `boundaries-final/`：新长度/选项、资源/质量故障与 GPU profiler 原始 trace。
- `persistent/`：默认配置的持久服务 CLI 起停和 200 行精确对照。
- `{pplx,amos,d3}-pool-fix-v2/`、`three-model-native-restart.json`：公共修复回归。
- `snapshot-refcount.json`、`pool-accounting.json`、`torch-patch-check/`：Torch 根因证据。
- `workspace4-fresh/`、`no-bf16-reduce/`、`blaslt/`：额外数值控制变量实验。
- `baseline/`、`turn.diff`、`preservation-audit.json`：原始脏工作树与本轮增量。

实现/预算/质量退化和两种生命周期的剩余接入工作已完成；历史旧启动的原始数值
路径触发条件仍没有足够证据证明，按上述限制保留，不标记成已解决。
