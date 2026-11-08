# Triage 全量吞吐问题 · 定位交接（2026-11-03）

- 状态：待定位（平台性能问题，非业务正确性）
- 机器：**20 机器**（`ssh -F /yzp/zhaozy/yangzepeng/0905/.devkeys/config gpu32520`，2× H100 80GB）
- 业务：297,506 个 gap 概念的三分类 triage（choice，ENTITY / GENERIC / SCENE_OR_JUNK）
- 当前：pplx + amos 两个模型并行跑全量，**只打标签、不删除**
- 相关平台文档：`docs/model-services.md`、`docs/four-model-performance-review-20261031.md`、`docs/jev-throughput-review-20261101.md`

---

## 1. TL;DR

- 每个模型稳态约 **22.4–22.6 条/秒**；`request_concurrency` 从 4 提到 8 **没有提升**（22.5 → 22.4）。
- 稳态 GPU 已接近满载：多次采样 **98–100%**，功耗 **645–695 W**（H100 上限约 700 W）。
- 单请求延迟（最近 300 条）：pplx p50 **0.315 s**、p95 **0.363 s**；amos p50 **0.318 s**、p95 **0.419 s**。
- 结论倾向：**不是服务准入并发不够，而是模型执行/输入准备本身接近上限**；需要定位时间花在 prefill、readout、CPU 编码还是 HTTP/journal。
- 一个明确可疑点：pplx engine 日志反复出现
  `causal_conv1d is not installed` → **回退参考 PyTorch 实现（正确但慢）**。

---

## 2. 复现环境

### 2.1 机器与服务

```text
20 机器: gpu32520 (10.127.48.252:32520)，2× H100 80GB
  GPU 0: pplx-decider-v1.2
  GPU 1: amos-1-large
共享 /yzp (VPS)，本地机器 4× H100 不参与本轮
```

### 2.2 业务配置

```text
configs:
  demiwtg/shared/concepts/identity/configs/triage_pplx.yaml
  demiwtg/shared/concepts/identity/configs/triage_amos.yaml
deployment:
  profile: pplx / amos
  performance: optimized          # CUDA Graph，2 streams
  request_concurrency: 8
  lifetime: action
  gpus: [0] / [1]
  python: 现有 .pplx-env / .decision-env
prompt:
  triage_concept, systemone choice, 3 options
  criteria + instructions 共约 300 input tokens/请求
驱动:
  runs/identity_gap_triage_20261101/run_triage_platform.py
  map_prompt_async concurrency=8, queue_depth=8
  journal 复用、checkpoint 落盘
```

### 2.3 数据

- 输入：`model_scores__identity_gap_20261031.lance`（297,506 个未覆盖概念）
- journal：
  - `runs/identity_gap_triage_20261101/pplx_full_journal.sqlite`
  - `runs/identity_gap_triage_20261101/amos_full_journal.sqlite`
- 已完成约 67%（pplx）/ 64%（amos），**跑批进行中，定位时请勿重启/打断**；等结束后 GPU 0/1 空出。

---

## 3. 实测数据

### 3.1 吞吐 vs 并发

| 并发（request_concurrency / Dataset concurrency） | pplx 条/秒 | amos 条/秒 |
|---|---:|---:|
| 4 | 22.5 | 22.7 |
| 8 | 22.4 | 22.6 |

增加并发没有收益；GPU 已接近满载，说明不是准入队列不足。

### 3.2 GPU / 功耗（并发 8 稳态，连续采样）

```text
GPU 0 (pplx): 98–100%，649–692 W
GPU 1 (amos): 98–100%，645–695 W
显存: 52.4 GB / 56.0 GB
```

早期采样曾出现 38–57%，是模型加载/CUDA Graph 捕获阶段；稳态后基本 98–100%。

### 3.3 单请求延迟（最近 300 条响应）

| 模型 | p50 | p95 | max |
|---|---:|---:|---:|
| pplx | 0.315 s | 0.363 s | 0.452 s |
| amos | 0.318 s | 0.419 s | 0.481 s |

按 Little's law：8 并发 / 0.315 s ≈ 25 条/秒，和实测 22.5 接近，说明服务已经接近该输入分布下的上限。

### 3.4 冷启动/重启成本

- 数据准备（读 29.7 万打分 + 100 万概念名称）：约 4–6 分钟；
- 模型加载：约 1–2 分钟；
- CUDA Graph 重新捕获：前几分钟吞吐偏低，之后回升；
- 每次重启合计约 8–10 分钟。

---

## 4. 与平台 review 的对照

`docs/four-model-performance-review-20261031.md` 的“服务层性能”：

| 模型 | 原生条/秒 | 优化混合条/秒 | 优化单客户端（热输入） | 双客户端（热输入，GPU 94%） |
|---|---:|---:|---:|---:|
| pplx | 9.21 | 13.08 | 25.04 | 30.59 |
| amos | 9.62 | 12.15 | 23.76 | 30.77 |

注意：review 的热输入只有 50 条高频输入、且是构造样本；本轮是 **29.7 万真实业务名称**，prompt 固定约 300 tokens。两者不能直接相除，但 22.5 vs 25/30 的差距值得定位。

---

## 5. 已知可疑点

1. **`causal_conv1d` 未安装**（pplx engine 日志）：
   `causal_conv1d_fn is falling back to its reference PyTorch implementation ... much slower`。
   该模型是混合线性注意力/GDN 结构，conv 回退可能显著增加 CPU/GPU 时间。amos 也可能同样回退（待确认）。
2. **prompt prefill 占比**：每次请求约 300 input tokens，且 criteria/instructions 固定；如果 runtime 不做 prefix/state 缓存，每条都要重新 prefill。两个模型 p50 ~315 ms，需要确认 prefill 占多少。
3. **CUDA Graph 覆盖**：曲线在稳态 98–100% 说明图在跑；但不同输入长度会触发新形状捕获/回退，需要看 `/v1/models` 的 `capabilities.execution`（hits/captures/evictions/verify、queue wait）。
4. **CPU 编码/HTTP/journal**：concurrency 8 仍不提升，可能是 CPU 准备与 GPU 交替造成空档；需要 per-stage profile。
5. **单客户端 vs 双客户端**：review 里双客户端能到 ~30/s；本轮单客户端 8 并发。是否可以双客户端（不同行范围、共享服务）需要平台确认。

---

## 6. 定位建议（给 Codex）

### 6.1 先量化时间分布

在 20 机器上，等本轮跑批结束后（GPU 0/1 空闲）：

```bash
# 200 条真实业务 case，单条 vs 并发，optimized vs native
cd /yzp/zhaozy/yangzepeng/0905
PYTHONPATH=demiwtg:demiflow env/bin/python -m demiflow.cli services benchmark \
  --config demiwtg/shared/concepts/identity/configs/triage_pplx.yaml \
  --cases /path/to/cases-200.jsonl --parallel 2 --report /tmp/pplx-bench.json
```

- 用 `cProfile`/`py-spy` 看客户端编码、HTTP、journal 占比；
- 用 `nsys`/`nsight` 或 torch profiler 看 prefill / decode / readout / graph replay；
- 拉 `/v1/models` 的 `capabilities.execution`（命中、捕获、淘汰、校验、queue_wait）在稳态前后的变化；
- 对同一批输入，比较 `performance: native` vs `optimized`，量化图加速实际贡献。

### 6.2 验证 `causal_conv1d`

- 在**独立副本环境**（不改共享 env）安装 `causal_conv1d`，跑 200 条对照；
- 对比 p50/p95、吞吐、GPU 利用率；确认收益再讨论是否更新共享环境（需用户授权）。

### 6.3 验证并发/客户端模型

- request_concurrency 2/4/8/16，单客户端与双客户端；
- 双客户端需保证行范围不重叠、journal 不冲突；
- 记录 GPU 利用率、p50/p95、吞吐，判断是否还有排队/空档。

### 6.4 检查 prompt/prefill

- 统计 criteria+instructions 的 token 数、每条输入 state 的 token 数；
- 确认 runtime 是否支持 prefix cache / state cache；若支持，评估对固定 300-token 前缀的收益（不改 prompt 语义）；
- 若 prefill 占主导，讨论是否值得为固定前缀做缓存（平台能力，需质量对照）。

---

## 7. 期望产出

1. **瓶颈结论**：prefill / decode / readout / CPU / HTTP 各占多少；22.5 条/秒是否是当前输入分布下的合理上限；
2. **`causal_conv1d` 回退的实际影响**（有/无对照，独立环境）；
3. **低风险优化建议**（按收益/风险排序），明确是否需要改平台、是否影响质量/一致性；
4. 最小复现命令与测量数据，不修改业务 prompt/语义，不启用 batch。

---

## 8. 边界

- 跑批进行中（约 1 小时），**不要重启/打断**；结束后 GPU 0/1 可用。
- 共享 `.pplx-env` / `.decision-env` 不直接改；需要装包先在独立副本验证并申请授权。
- `.devkeys/config` 只读使用，不修改、不复制到系统/root 目录。
- 生产 batch 保持关闭；不改模型权重、精度、校准。
- 20 机器与本地机器共享 `/yzp` 存储，注意路径一致。

---

## 9. 相关文件

```text
demiflow/docs/model-services.md
demiflow/docs/four-model-performance-review-20261031.md
demiflow/docs/jev-throughput-review-20261101.md
demiwtg/shared/concepts/identity/configs/triage_pplx.yaml
demiwtg/shared/concepts/identity/configs/triage_amos.yaml
demiwtg/shared/concepts/identity/runs/identity_gap_triage_20261101/run_triage_platform.py
demiwtg/shared/concepts/identity/runs/identity_gap_triage_20261101/pplx_full_journal.sqlite
demiwtg/shared/concepts/identity/runs/identity_gap_triage_20261101/amos_full_journal.sqlite
```
