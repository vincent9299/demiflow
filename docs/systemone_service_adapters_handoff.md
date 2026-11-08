# 身份补齐业务接入交接：平台服务 + `map_prompt_async`

- 状态：平台优化告一段落，进入**业务验收**阶段
- 日期：2026-11-01
- 读者：DeepSeek（业务接入/验收）、平台 owner、业务 reviewer
- 权威平台文档：[model-services.md](model-services.md)、[jev-throughput-review-20261101.md](jev-throughput-review-20261101.md)
- 本文替代 2026-10-31 的“SystemOne 服务托管、适配器与批量”计划；那份计划中的适配器/合批实现**不再执行**（原因见文末）。

---

## 0. 一句话

用现有 `demiflow.services` 部署 + `map_prompt_async` 接入真实业务；**小批验证 → 逐步放量**；复用平台并发、缓存和 journal；**合批保持关闭**；业务侧不重写调度、重试或模型执行逻辑。

---

## 1. 平台现状（已实现，可直接用）

### 1.1 部署与生命周期

四个本地模型统一通过 `demiflow.services` 的 `deployment` 声明托管：

```yaml
model:
  name: AutoTrust-JEV-27B-v1.1
  deployment:
    profile: autotrust-jev          # autotrust-jev | pplx | amos | d3 | vllm
    weights: /yzp/.../models/AutoTrust-JEV-27B-v1.1
    revision: immutable-weights-v1
    gpus: [0, 1]
    lifetime: action                # action | persistent
    performance: optimized          # 已验证的加速配方；native 保持原行为
    request_concurrency: 2
```

- 命令：`demiflow services prepare|doctor|start|status|logs|stop|validate|benchmark`。
- 外部 URL 仍可用（原有 `base_url`），但新业务应优先用 deployment；**不要手工 `start_*.sh` 绕过平台**。
- 单副本默认并发接收、有限排队；`replicas: 2` 才是**两个独立实例真正并行**（每副本一张卡）。
- `performance: optimized` 使用有界 CUDA Graph（JEV 实测吞吐 +14.1%、p95 −43.9%；四模型回归通过），**不改变权重、精度、校准**。
- JEV 原生 HTTP 路径可显式 `request_pipelining: true`（CPU/GPU 流水重叠，`parallel:1`、`batch=false`），与合批互斥。

### 1.2 质量门槛与合批（重要）

- **生产合批保持关闭**：pplx/JEV 的原生合批/并发单条对照出现标签差异（pplx 最大概率差 0.0559、JEV 0.0294），未达到“与单条等价”的验收条件。见 [GPU review](model-services-gpu-review-20261031.md)。
- 平台保留了 `batch_evidence`/`services validate` 机制，但**本阶段不启用**；不要配置 `systemone_batch`、不要申请 batch 证据。
- 提速只用已验证的两种手段：`performance: optimized`（单副本图加速）和 `replicas`（多副本真并行）；JEV 可加 `request_pipelining`。
- 三个并发概念不要混：`parallel`（真实独立执行）、`max_inflight`（HTTP 准入/排队）、Dataset `concurrency`（行并发）。

### 1.3 算子与存储

- `map_prompt_async` 已支持 SystemOne `noul` 与 `choice`（choice 返回 `choice` + `probabilities`）；journal、预算、重试、request 身份沿用原有语义。
- `demiflow services` 的 prepare 会登记端口/GPU；`action` 部署由活跃 runtime 持有，最后一份 lease 释放后关闭；`persistent` 由 supervisor 跨 run 持有。
- 命中 journal 的请求不会启动/加载模型。

---

## 2. 业务当前状态（接续点）

### 2.1 身份表

```text
identity_assessments.lance   v44390   728,769 行
  assessed 728,667 / insufficient_evidence 102
summary.lance                v17      complete=true（scope=有本地文档的概念）
```

- 全量概念 1,026,275；已覆盖 728,769（71.0%）。
- **未覆盖 297,506 个概念没有本地文档**，是当前业务缺口。

### 2.2 三模型分诊/打分（只打分，未删除任何样本）

```text
triages__identity_gap_20261031.lance          297,506   AutoTrust 三分类
crosscheck__identity_gap_20261031.lance         9,512   AutoTrust + pplx + amos
crosscheck_rest__identity_gap_20261031.lance   95,082   AutoTrust + pplx + amos
```

- AutoTrust 分布：GENERIC 197,921（66.5%）、ENTITY 70,369（23.7%）、SCENE_OR_JUNK 29,216（9.8%）；low_confidence 33,742。
- 95,082 补充交叉中“强正向 ≥0.8”救回 17,787。
- 原计划的 192,912 条强正向补分**已暂停**（约 18%，checkpoint 在 `/tmp/crosscheck3`）；`model_scores__identity_gap_20261031.lance` 尚未生成。
- **没有应用任何删除**；`drop__...lance` 只是候选分析。

### 2.3 平台改动状态

- 平台优化暂停；不再继续做适配器/合批/性能改造。
- 已有实现、回归证据和边界以 `docs/model-services.md` 及其链接文档为准。

---

## 3. 接入方式（业务侧怎么做）

1. **配置**：在现有 prompt pack 的 `model` 中加 `deployment`（profile/weights/revision/gpus/performance/request_concurrency），保持 prompt、schema、模板不变。
2. **准备**：

   ```sh
   demiflow services prepare --config prompt.yaml
   demiflow services doctor  --config prompt.yaml
   ```

3. **调用**：保持 `map_prompt_async`，`concurrency` 从 1–2 开始（≥ 副本数），不配置 `systemone_batch`：

   ```python
   result = rows.map_prompt_async(
       'triage_concept', config=load_prompt_pack('prompt.yaml'),
       inputs={'payload': 'payload'}, output='answer',
       concurrency=2, error_output='error', call_output='call',
   ).checkpoint(...).take_all()
   ```

4. **证据**：journal 沿用现有 run/journal；新业务用**新 run 目录**，不覆盖历史。
5. **GPU**：显式声明 `gpus`；准备/启动前确认卡空闲并遵守协作锁；**不要占用/重启其他任务的卡**（当前 0/1、2/3 都有其他进程，见第 8 节）。

### 3.1 状态字符串必须与历史基线逐字节一致（实测教训）

同一批 300 条、同一模型、同一 criteria/instructions，只改 state 的序列化方式：

| 方式 | pplx 标签一致率 | 最大概率差 |
|---|---:|---:|
| 平台模板 `{{ payload \| json }}`（Jinja：紧凑、排序、尾换行） | 0.922 | 0.270 |
| 业务侧预生成 `json.dumps(..., ensure_ascii=False)`（历史直连格式） | **1.000** | **0.0** |

决策头模型对 state 的字节格式敏感；历史直连基线用的是 `{"name": "...", "aliases": []}`（带空格、保留插入顺序），而 Jinja `tojson` 输出 `{"aliases":[],"name":"..."}\n`。

做法：**模板用 `{{ state }}`**，业务侧预先生成与历史完全一致的 state 字符串作为输入列；不要用 Jinja JSON 过滤器重建状态。amos 同样适用（对齐后 300/300、最大概率差 0.0）。

---

## 4. 第一步：小批真实业务验收（必须先做）

**样本**：从 297,506 个未覆盖概念中分层抽 200–500 条（ENTITY / GENERIC / SCENE / low_confidence 各若干），带真实 `name`/`aliases`。

**流程**：

1. 用平台 deployment 起服务（`lifetime: action`，单副本，`performance: optimized`，`request_concurrency: 2`）；
2. 同一 prompt（三分类 `choice`）跑 `map_prompt_async`，`concurrency=2`；
3. 对照此前直连 HTTP 的同一批结果（标签、三类概率、顺序）；
4. 检查 journal：每行一条 request/response、无重复、无缺失；
5. 故障恢复：中途取消/重启后再跑，验证 journal 复用与断点；
6. 端到端耗时：含 prepare/加载/推理/写 checkpoint，分别记录冷启动与稳态；
7. 记录 GPU 利用率、显存、p50/p95；确认**没有开启 batch**。

**通过标准**：

- 输出与直连基线一致（标签一致；概率差在既定容差内，记录实际值）；
- journal 行数 = 输入行数，无重复发送；
- 取消/重启后不重复计费、不丢行；
- 端到端时间可接受，且能解释冷启动占比；
- 没有任何 `systemone_batch`/`batch_evidence` 配置。

通过后再按 1k → 1万 → 全量逐步放量；每档记录耗时、失败率、GPU、journal。

---

## 5. 边界与规则（业务侧）

1. **不重写**调度、重试、并发、模型执行；平台能力不够时提**最小复现**（输入、配置、日志、journal 行），由平台修。
2. **合批关闭**：不配置/不申请 batch 证据；需要吞吐用 `replicas` 和 `performance: optimized`。
3. **不删除**任何概念/样本；新增结果写新表/新 run，不覆盖 `identity_assessments` 历史版本。
4. **不手工起停服务**绕过 `demiflow.services`；外部 URL 只作为兼容路径。
5. **不占 GPU 2/3**、不重启其他任务；使用前确认卡空闲与协作锁。
6. 平台改动（如需）单独走平台仓流程，不在业务仓打补丁。

---

## 6. 后续业务决策（接入验收后）

| 决策 | 选项 |
|---|---|
| 强正向 19.3 万条补分 | 续跑 `/tmp/crosscheck3`（约 5–6 小时）或放弃（已有 AutoTrust 分） |
| 合并打分数表 | 生成 `model_scores__...lance`（297,506 行，一行不删） |
| ENTITY（70,369） | 走 Wikipedia 搜索/API 取真实文档，再跑身份审核（平台路径） |
| GENERIC（197,921） | 不搜索；如需描述，单独出“模型生成定义”产物（不写身份表） |
| SCENE_OR_JUNK（29,216） | 明确无关候选；按“强支持才保留”规则处理，但**不删除原始数据** |
| 身份表 scope | 是否把新获取文档的概念并入 `identity_assessments`（新版本 + summary） |

---

## 7. 验收清单（reviewer 用）

- [ ] `demiflow services prepare/doctor` 输出正常，端口/GPU 与声明一致；
- [ ] 200–500 条小批 `map_prompt_async` 输出与直连基线一致；
- [ ] journal 行数/身份/重放正确，无 batch 配置；
- [ ] 取消/重启恢复验证通过；
- [ ] 端到端耗时、GPU、p50/p95 记录；
- [ ] 未覆盖历史表/未删除样本/未占用其他任务 GPU；
- [ ] 放量计划与每档验收记录。

---

## 8. 当前环境事实（2026-11-01）

```text
identity table v44390, 728,769 行；summary v17
未覆盖概念 297,506
triage 297,506；crosscheck 9,512 + 95,082；强正向补分暂停在 /tmp/crosscheck3
GPU 0/1/2/3 当前均有其他进程（非本任务）；无 8102/8103/8104 服务
平台文档：docs/model-services.md、docs/jev-throughput-review-20261101.md
```

使用 GPU 前先确认空闲和协作锁；不要停别人的进程。

---

## 附：已废止的平台优化计划（历史）

2026-10-31 曾计划给 `demiflow.services` 增加 systemone 适配器与 `/v1/systemone_batch`、由算子侧合批。该计划**不再执行**：

- 平台已实现 deployment/profile/replicas/CUDA Graph/pipelining，业务可直接用；
- 合批在质量门槛上未通过（pplx/JEV 标签差异），生产保持关闭；
- 算子侧合批会把模型族逻辑放进平台，边界不清晰，已被 deployment + 适配器方案取代。

历史设计仅作背景，不作为实现依据。
