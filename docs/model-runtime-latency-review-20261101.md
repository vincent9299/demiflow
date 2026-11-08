# 运行时数值与尾延迟复核（2026-11-01）

后续 JEV 正式并行接入、公共 Torch 图池修复和完整服务验收见
[JEV 后端复核](jev-runtime-review-20261101.md)。本文保留当时的实验与开放状态，
其中“尚未接入正式后端”由后续验收更新；历史跨启动触发的证据缺口仍保留。

本轮承接 [设计复核](model-runtime-latency-design-20261101.md)。只修改 demiflow 平台，
原有未提交内容按本轮开始的 97 文件 SHA-256 快照保留；仅运行自有 GPU 0/1 测试进程。
没有修改已安装的模型环境、权重或 StartLux/vLLM 源码，没有启动/停止既有服务。
以下测试均采用概率零容差，不能代替带标签的业务准确率验收。

## 合入工作树的实现与架构复核

- `BoundedExecution` 在原有 Condition 准入点维护 FIFO ticket：先来的合法请求先取得
  GPU lane 和 workspace 预算；只给已准入的请求提交固定单线程 executor。
  服务仍负责 `max_inflight`，ticket 不存第二份载荷，不新增后台请求队列或调度引擎。
  等待中关闭/失败会移除 ticket 并唤醒其余调用；活动 GPU 工作排空后才销毁缓存。
- pplx 与通用 NativeGraphAdapter 的静态输入额外预热从三次减至一次。捕获前已经
  有原生执行，一次静态布局预热仍覆盖 lazy 初始化。保留捕获独占、输入/输出
  生命周期、私有图池、cuBLAS 工作区隔离、原生对照和首次/周期 `torch.equal`。
- 增加进程累计/最长 lane 准入等待观测。该时间不包含 HTTP、编码和获得 lane 后
  的图捕获等待；不以这一指标冒充完整请求延迟。
- 数值环境身份增加 `CUBLAS_`、`CUBLASLT_`、`FLASHINFER_`。准备清单增加 Torch
  版本源码、vLLM GDN/linear/context 源码、FlashInfer GDN/scratch 源码及 cuBLAS/CUTLASS 包版本的记录。
  可只读挂接的通用 vLLM 不强依赖存在 JEV 源文件；缺失本身被记录。
  变更后的身份不能沿用旧质量证据。此记录仍不是所有动态 GPU kernel 的完整哈希。

没有增加捕获后台线程、修改默认缓存/显存上限，或把未验证的 JEV 实验接入 profile。
FIFO 会产生大请求阻挡后续小请求的取舍；它消除准入插队，不保证每项指标都改善。
模型专用数值实验保留在证据目录，公共执行层没有加入 JEV 条件分支。

## 三模型 GPU 对照

同一 GPU 1，先后使用本轮开始的源码快照与最终实现。每个模型 200 条固定业务形态
样本建立原生基线，然后冷缓存 200、混合 1000、排队 400 次执行；问题含 choice/noul、
不同选项数量、单/多问题和不同输入长度。固定输入与随机种子，固定 2 个 GPU lane，
使用默认 32 图 / 4 GiB 图缓存 / 64 GiB Torch allocator 预算。

三个模型的优化前后原生基线跨启动完全一致；优化后各 1600 次答案与 usage 全部精确
一致，内部图校验无 mismatch、无降级。下表为单次配对测量，不是统计置信区间。

| 模型 | 阶段 | 吞吐前→后（条/s） | p95 前→后（ms） | p99 前→后（ms） | 捕获累计次数前→后 |
|---|---|---:|---:|---:|---:|
| pplx | 冷缓存 200 / 2 clients | 9.92→11.41 | 500.0→263.4 | 745.8→530.3 | 5→4 |
| pplx | 混合 1000 / 2 clients | 11.04→12.26 | 594.6→450.7 | 804.7→600.0 | 49→56 |
| pplx | 排队 400 / 4 clients | 11.75→13.75 | 844.9→611.9 | 1326.8→712.3 | 66→71 |
| AMOS | 冷缓存 200 / 2 clients | 10.77→10.78 | 220.0→364.7 | 708.4→518.3 | 4→5 |
| AMOS | 混合 1000 / 2 clients | 10.84→12.17 | 589.7→423.1 | 722.1→569.1 | 65→64 |
| AMOS | 排队 400 / 4 clients | 13.98→13.67 | 716.1→574.2 | 858.4→779.6 | 77→79 |
| d3 | 冷缓存 200 / 2 clients | 5.53→5.84 | 846.9→671.1 | 1254.8→1198.6 | 12→12 |
| d3 | 混合 1000 / 2 clients | 7.54→7.62 | 364.5→357.6 | 762.1→642.7 | 25→25 |
| d3 | 排队 400 / 4 clients | 7.88→7.88 | 821.1→678.5 | 997.9→714.8 | 25→25 |

AMOS 冷缓存 p95 上升（捕获由 4 次变为 5 次），排队吞吐下降约 2.2%；其混合负载
p95/p99 与排队 p95/p99 改善。不能据此宣称所有阶段都加速。D3 持续吞吐基本不变，
主要改善 p99。捕获仍会暂时阻塞其他 lane，长尾没有被完全消除。

最终测量的 Torch allocator 峰值分别为 pplx 52.68 GiB、AMOS 55.73 GiB、d3 55.60 GiB。
缓存都在 4 GiB 内；这不是整个进程所有第三方显存的硬上限。

另对三个最终图后端重新执行：故意破坏重放输出后返回原生正确结果，清空/禁用图，
保留 `parallel=2`；64 MiB 小图预算下使用原生；超输入预算在 GPU 分配前拒绝。
两条 stream 的 GPU profiler 显示真实 kernel 时间重叠，分别有 8139、7232、12972 对
跨 stream 重叠区间。这证明实际并行存在，不能将重叠对数解释为吞吐倍数。

AMOS 还通过正式 `services prepare` → `services benchmark --parallel 4` → 自动释放
自有服务：200 单条 + 200 并发单条，标签/概率/usage 无差异。此冷缓存 CLI 测量
单条 10.34、4 客户端 10.29 条/s；排队和捕获会抵消并发收益，不用它宣称加速。

## JEV：已定位的数值敏感实现

多次原生独立启动的 200 条结果均与上一轮新基线一致，与旧基线仍有 80 条答案不同。
同一输入/权重/启动配方下，插入逐层 tensor 指纹探针不改变答案；固定 case 51
覆盖 48 层 GDN 与 16 层完整 attention。

仅改变 `CUBLAS_WORKSPACE_CONFIG=:16:8` 后，最早的分歧出现在第 0 层
`QwenGatedDeltaNetAttention.in_proj_ba` 的 BF16 线性投影：QKV 输入完全相同，b/a
不同，然后 GDN 输出不同。该配置复现 case 51 的旧概率
`0.9808759631442909`；默认值为 `0.979667646768647`。但全体 200 条仍有 100 条
与旧基线不同，所以不能把复现一个值写成完整复现旧启动。

从实际编译路径 `extern_kernels.mm` 截取输入 `[254,5120]`、权重 `[96,5120]` 和
输出，指纹与完整模型第 0 层 b/a 完全对应。独立 GEMM 回放确认：

| 工作区与对齐 | profiler 中的 kernel | 与默认 native 投影不同元素数 |
|---|---|---:|
| 32 MiB，正常对齐 | `nvjet_sm90_tst_64x16_64x16_2x4_h_bz_splitK_TNT` + split-K reduce | 0 |
| 128 KiB，正常对齐 | `nvjet_sm90_tst_64x8_64x16_2x4_h_bz_TNT` | 33 / 24384 |
| 32 MiB，人为偏移 2 bytes | CUTLASS align1 GEMM | 21 / 24384 |

128 KiB 投影最大差 0.0625，b/a 完整指纹与上述小工作区整模型实验一致。
每种配置进程内重复 10 次均一致。这定位了一个原生 GEMM 算法/归约选择的数值
分歧源，排除了 demiflow 请求拼接、概率校准作为这项控制变量差异的来源。
人为不对齐只用于说明敏感性，并非已证明原服务出现这种地址。

以同一输入/权重的 FP64 乘积再舍入 BF16 为参考，默认 split-K 有 20 个元素不同，
非 split-K 有 29 个元素不同；两者最大绝对误差相同。禁用 split-K 并非在该样本上
更准确，更不能因此放宽服务的原生精确一致门槛。

安装的 Torch commit `cf30153c4c131c8164ee7798e5022d810682e2cb` 中，
[CublasHandlePool.cpp](https://github.com/pytorch/pytorch/blob/cf30153c4c131c8164ee7798e5022d810682e2cb/aten/src/ATen/cuda/CublasHandlePool.cpp)
按 handle/stream 管理工作区，Hopper 默认 cuBLAS 32 MiB、cuBLASLt 1 MiB；统一
工作区时 Lt 上限受 cuBLAS 限制。诊断使用实际安装版本，未直接套用未来 main 文档。

运行中另扫描 9 组工作区设置，每组 200 条：没有一组完整复现旧基线；恢复默认
重新精确匹配新基线。该扫描在现存 vLLM 图初始化后进行，已捕获图不会自动按新
工作区重新选 kernel，不能与从进程启动设置环境的实验等同。

**历史那一次启动为什么选择了另一条数值路径，仍未闭环。** 当时未保存逐层
tensor、kernel/工作区轨迹；本轮发现的敏感实现不能代替缺失的历史证据。没有
修改旧基线、调宽容差或把不一致结果写成质量通过。

## JEV 单卡并行实验

### 原生实现中的共享临时内存缺陷

独立 GDN 算子测试先用相同输入，100 次并行通过；改为不同输入后，在第 1～2 次
就失败，各自串行仍正确。相同输入会掩盖互相覆盖，不能作为并行验收。完整 GDN
core 也复现，且不需要 demiflow HTTP、适配器、模型权重或全模型调度。

具体实现是 FlashInfer 0.7.0.post1 的 `utils.py::_get_cache_buf`：缓存键只有
`(name, device)`。SM90 的 `delta_rule_cp_sm90.py::_get_cp_workspace` 与 tensor map
分配复用这些全进程缓冲区，不同 stream/图捕获会拿到相同地址。实际 case 51 的
CP 路径使用以下五个共享名字：

- `gdn_cp_sm90_t`：1.5 MiB；
- `gdn_cp_sm90_local_transfer`、`gdn_cp_sm90_local_state`、`gdn_cp_sm90_fixed_state`：各 6 MiB；
- `gdn_cp_prefill_tensormaps`：16,896 bytes。

这会让独立请求覆盖对方的中间状态。普通非 CP 的 SM90 路径也按同样方式共享
`gdn_prefill_tensormaps`。仅复制模型 KV/state、换线程/cuBLAS handle 或另建图池
都无法隔离这些已在全局缓存里的地址。

候选修复让上述工作区通过 `torch.empty` 按调用分配，Torch 在捕获时把它们纳入
各自图池，并管理 CUDA 生命周期。不替换 kernel，不改变精度、形状、公式或
归约顺序。普通 native 调用依靠 allocator 复用；原有全局名字缓存不再被使用。
新增内存必须计入各执行实例/图的预算，不能把“去掉共享”当成没有成本。

可审阅文件：

- [SM90 候选补丁](../benchmarks/patches/flashinfer-sm90-gdn-workspace.patch)：仅涉及
  FlashInfer 的 SM90 CP/non-CP 包装层；已在临时源码副本中应用、通过语法检查，
  并把该副本仅加载到自有测试进程，254 和 2048 token 的不同输入各并行 100 次
  精确通过；Torch 峰值分别为 132 MiB、320 MiB。
- [不依赖模型权重的复现脚本](../benchmarks/jev_gdn_reentrancy.py)：显式选空闲 GPU，
  合作资源锁，Torch 4 GiB 上限，最多 2048 token / 1000 次测试；修复模式只改变
  自己的进程，单次 scratch 分配还限制为 256 MiB，不修改安装文件。

原始 FlashInfer GDN 与完整 GDN core 的不同输入测试都复现并行失败；
修复后的独立 FlashInfer GDN 连续 100 次精确一致。便携复现脚本原实现第一次
并行就失败，修复模式和实际补丁源码模式各 100 次精确一致。正式安装目录未修改。

### 整模型可行性与开放边界

临时 vLLM worker 实验共享权重，复制每个 lane 的输入、attention metadata、
GDN state、KV 与 StartLux Plan；保留原生 NULL_BLOCK_ID=0，实际状态从 block 1
开始。每 lane 的元数据/输入上限 128 MiB、状态/KV 上限 512 MiB，同时只保留
两张图，Torch allocator 上限 74 GiB。两个固定捕获线程，两个独立 CUDA stream。
捕获时顺序切换 ForwardContext；重放不再执行其全局 Python context 逻辑。

修复前：原样模型回放、状态隔离后的 native、单图和双图串行都精确一致，但同时
重放失败。最初诊断保留的中间 tensor 引用可能被 Inductor 显式复用，不能据其
事后内容判断最早分歧层；最终阶段探针使用捕获内快照修正了这个诊断限制。

修复后：200 条原生答案/usage 与当前新基线保持精确一致。case 51 的两张整模型
图独立重放及并行 20 轮，全部有效 hidden-state 数值精确相同；GPU profiler
确认 stream 33/37 各执行 1752 个 kernel，有 2717 对跨 stream 时间区间重叠。
30 轮双请求的纯 GPU 诊断计时为串行 1.768 秒、并行
1.697 秒，仅是单形状可行性数据，含探针，不能当成正式服务吞吐提升。

`jev-parallel-varied-v4` 通过 102 对不同输入：200 条业务样本的主问题，以及
743/1223 token 的两个扩展输入与不同短请求配对；100 种实际 token 长度，范围
114～1223。每对分别验证隔离 native、捕获后单图、并行 3 轮，有效 hidden state
全部精确相等。每次只保留两张图，单 lane 状态/KV 最大 462,028,800 bytes，
Torch allocator 峰值 70.328 GiB，未增大 74 GiB 预算。这里不把模型 forward
测试冒充并发 HTTP 读出测试；多问题的完整原生答案仍按最初 200 行基线校验。

短输入保留原生 piecewise 的输入布局，只比较有效 token，没有额外 padding；
长输入按实际 block 数复制并检查预算，超限直接失败。前两轮扩展实验在返回
原生短图时发生 illegal memory access；反复创建 stream/线程会轮回 Torch 的
有限 stream 池，可能与既有图共享 stream/workspace 生命周期。去掉手动工作区
清理仍可复现；固定两条 stream、两个线程并保留到进程结束后，全部 102 对通过。
这是诊断 harness 的生命周期限制，不能把此前错误归因于请求质量或 FlashInfer
补丁无效；也不能将这个实验控制变量当作对整个 vLLM 生命周期的正式审计。
公共 Torch 后端原本已经使用固定 lane/线程/stream，不需要引入另一套机制。

`jev-parallel-extra` 又覆盖多问题样本中的另外 20 个问题，10 对全部通过，包含
103～106 token 的更短输入，以及 1008～1012 token 的长输入。合计覆盖原始
200 行的全部 220 个问题和两个扩展输入，共 112 对、672 次并行 forward 对照，
109 种长度，103～1223 token；全部有效 hidden state 精确一致。两次独立启动
分别使用 GPU 0/1，200 行原生答案/usage 都与当前基线一致；历史旧基线的 80 行
差异仍存在。单次固定形状 profiler 证实 kernel 重叠，多形状测试确认输出和预算，
没有将这些实验计时当作正式服务吞吐。


**这证明存在修复后单卡独立 forward 并行的可行路径，尚未把临时实验冒充正式
JEV 并行适配。** vLLM 的全局 ForwardContext、runner 输入和可变状态仍需由正式
后端完整管理；上线还需要多形状缓存/淘汰、采样/读出、HTTP 生命周期、故障与
journal 的端到端验收。公共框架没有引入上述私有对象复制和诊断 patch。
正式能力仍为 `parallel=1`、`batch=false`，CPU 请求准备/读出流水线继续支持。

历史跨启动漂移与本节的并行 scratch 冲突是两项问题；修复并行冲突不能据此
声称已经解释或修复旧启动的全部数值变化。

## CPU 与证据

最终 15 个相关测试文件：262 passed（68.22 秒）。包含 FIFO 无饥饿、关闭后
ticket 清空、失败清理、图质量退化、资源
预算、数值环境身份、图片 state、服务生命周期、算子并发与 journal 回归。
随后补齐无 CUDA 后缀的 `nvidia-cublas` 与 CUTLASS 包名，部署测试 17 passed。
四个 profile 的 `_versions` 在三个真实安装环境中只读采集通过：cuBLAS 均为
13.1.1.3，JEV CUTLASS DSL 为 4.7.1；未安装或升级包。

证据根目录：`/tmp/demiflow-latency-20261101`。

- `baseline/manifest.json`、`baseline/files/`、`turn.diff`：本轮原始工作树与增量。
- `performance-comparison.json`、各模型 before/after `report.json` / `baseline.json`。
- `{pplx,amos,d3}-boundaries/report.json` 与 `trace.json`；`amos-cli-report.json`。
- `jev-native-2/`、`jev-workspace16/`、`jev-projection-actual/`、`gemm-isolation/`、
  `jev-workspace-sweep/`：逐层指纹、实际投影 tensor、GEMM profiler、控制变量数据。
- `jev-parallel-poc-v*/`、`jev-parallel-varied-v*/`：失败也保留，不以单图或串行正确替代并行验收。
- `jev-parallel-varied-v4/{pairs,report}.json`、`jev-parallel-extra/`、
  `jev-parallel-summary.json` 与所用辅助脚本：不同输入的完整模型验证。
- `cpu-final-v2.xml`、`cpu-final-v2.log`、`cpu-provenance-final.log`、`runtime-identity-final.json`。
- `portable-gdn-{native,fixed,source-patch}.json`、`upstream-patch-check/`：原生失败、
  候选修复和实际补丁源码验证；`jev-parallel-poc-fixed/parallel-overlap.json`。
