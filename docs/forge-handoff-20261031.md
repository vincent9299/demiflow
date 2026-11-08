# demiflow Forge 交接：契约开发平台、当前实现与生产验证计划

日期：2026-10-31。目标读者：继续后续开发和预生产验收的 Codex / 维护者。

本文描述当前真实代码和验证状态。续接时仍须先读 `AGENTS.md`、本文及 `docs/forge-contract.md`、`docs/forge-authoring.md`、`docs/forge-lifecycle.md`；若文档与当前实现不一致，以当前实现和实际测试为准。

## 1. 目标与不可退让的边界

Forge 的目标不是简单的脚手架或测试运行器，而是**契约驱动的 Pipeline 开发与生命周期平台**：

```text
内置 ContractBundle + 当前平台 ContractContext
  → 受限开发
  → Hard Check + Contract Tests
  → 独立 Review
  → Freeze
  → 按授权运行与后续验证
```

开发、测试、Review、Freeze 和运行必须绑定同一契约与实现 revision。Agent 可以实现业务函数、配置、测试和 README，但不能随意新增执行器、模型客户端、算子注册体系、隐藏子流程、未声明模块或契约外 API。

当前边界：

- 独立实现，运行时**不 import、不读取、不依赖** DemiForge、Demiurge 或 demiwtg。
- 旧项目只作为一次性参考；不做兼容垫片、跨仓 `tool_root` 或旧 Candidate/Role/DemiOrch 外壳。
- 真实执行直接复用现有 `demiflow.data` / Dataset，不创建第二个执行引擎。
- 业务多样性通过 Request、Definition、operator 和 RunConfig 表达，不允许项目自行替换工程结构。
- 新能力必须通过新的、显式版本化的契约扩展，不能为了让某个任务通过而临时放宽 v1。
- 静态检查和测试不是操作系统安全沙箱；Review 是证据，不是自动业务质量证明。
- 未提交 Git；保留工作树中原有 dirty/untracked 修改，不 stash、clean、restore 或覆盖。

## 2. 当前仓库与工作树

仓库：`/yzp/zhaozy/yangzepeng/0905/demiflow`

分支：`feat/pipeline-cli-mvp`

Forge 主要新增内容：

```text
demiflow/forge/
examples/forge_text_map/
integrations/coding-agent/demiflow-forge/
docs/forge-contract.md
docs/forge-authoring.md
docs/forge-lifecycle.md
tests/forge_support.py
tests/test_forge_*.py
```

修改了 `demiflow/cli.py`、`pyproject.toml`、`MANIFEST.in`、`README.md`、`AGENTS.md`。

以下既有修改不是 Forge 工作的一部分，后续必须保留，除非用户另行授权：

- `demiflow/execution/stream_lance.py`
- `demiflow/operator_llm/systemone.py`
- `docs/prompt_http.md`
- `tests/test_prompt_systemone.py`
- `docs/sqlite_journal_throughput_handoff.md`

## 3. 平台架构

### 3.1 模块职责

| 模块 | 当前职责 |
| --- | --- |
| `contract.py` | 加载内置 ContractBundle；从当前 demiflow 公开 API 生成签名、公开文档和实现摘要 |
| `definition.py` | PipelineDefinition、OperatorDefinition、Request/RunConfig、静态 AST 解析、operator 签名/config 校验 |
| `manifest.py` | 从 Definition 和 operator 生成 Manifest、语义摘要和实现摘要 |
| `scaffold.py` | 从事务性临时目录生成项目，完成后原子迁入目标；骨架默认不能通过验收 |
| `checks.py` | 布局、AST、依赖、禁止行为、request/definition、context/manifest/run config 检查 |
| `runner.py` | dry-run 与 Freeze 后的真实本地 Dataset 执行 |
| `verify.py` | 有界快照、check、dry-run、受控 pytest、测试前后变更比较及验证报告 |
| `lifecycle.py` | status、Review、Freeze、证据过期与日志损坏检查 |
| `store.py` | 项目外证据目录、原子记录和非阻塞项目锁 |
| `process.py` | 无 shell argv、最小环境、进程组、超时、有界 stdout/stderr |
| `snapshot.py` | 有界文件/目录项、流式 SHA256、变更检测 |
| `schema.py` | 封闭 JSON Schema 子集、数据校验、JSON Schema → Arrow schema |
| `io.py` | 排他写入、原子文本/JSON、fsync、symlink 防护 |
| `cli.py` | `demiflow forge` 命令组；旧 bundle CLI 保持独立 |

没有 `agent_loop.py`、`scheduler.py`、`orchestrator.py`、`operator_registry.py`、Candidate/Role 兼容层。

### 3.2 项目布局与所有权

```text
<project>/
├── project.json          # contract ID/version/context digest
├── request.json          # 原始业务需求；必须与 Definition 一致
├── contract.lock.json    # 生成的冻结契约上下文
├── pipeline.py           # 唯一静态 PipelineDefinition
├── manifest.json         # 只能由 Forge 生成
├── operators/            # 一个普通同步行函数，首版无嵌套模块
├── tests/                # 真实业务行为测试
├── run.example.json
├── README.md
└── datasets/             # 运行期本地 Lance，不参与源码 revision
```

候选实现只能修改 `pipeline.py`、`operators/`、`tests/`、RunConfig 和 README。`request.json`、绑定、locked context、Manifest 和证据不能用于“修改后通过”。

证据默认写到项目外：

```text
<parent>/.demiflow/forge/<project-name>/
```

`datasets/`、`runs/` 和登记的缓存目录不参与源码快照；其余目录和文件统一受 512 项、单文件 2 MiB、总 16 MiB 上限约束。

## 4. v1 契约是否写死，支持什么

**是的，当前 v1 是明确且有意固定的最小契约，不是通用 DAG 或插件系统。**

权威声明位于 `demiflow/forge/contracts/pipeline-v1.json`：

```json
"nodes": ["read_lance", "map", "write_lance"]
```

代码还有对应的硬约束：

- `contract.py` 要求节点恰好是这三个；
- `definition.py` 要求一个 Lance 输入、一个 map、一个 Lance 输出，且恰好三个线性节点；
- `manifest.py` 只允许 map 引用的唯一 operator；
- `runner.py` 只执行现有 `data.read_lance(...).map(...).write_lance(...)`；
- `checks.py` 将 `map_prompt_async`、`agentmap_async`、动态 import、隐藏 I/O 等列为禁止行为。

因此当前只支持：

```text
单个固定版本 Lance 输入
  → 一个普通同步 fn(row, **config)
  → 单个本地 Lance 输出
```

当前不支持：

- `map_prompt/map_prompt_async/agentmap_async`；
- 多输入、多 map、多输出、分支、循环、修订链或任意 DAG；
- 模型、网络、外部生产写入；
- 生产调度、分布式恢复、跨机器并发控制；
- RunConfig 输出 `expected_version`；
- 通用生产执行超时和资源硬限制；
- 自动语义 Review、固定评测或自动演进。

v1 的写死是为了防止首版重新长出一套不受控框架，不是因为最终平台永远只能有这三个节点。扩展时应新增版本化 ContractBundle 和对应 Definition/校验/Runner 行为，保留 v1 项目与验收语义，**不要直接把 v1 改成支持所有能力**。

## 5. 当前生命周期与使用方式

主要命令：

```bash
demiflow forge schema request
demiflow forge rules --json
demiflow forge new REQUEST --out PROJECT --json
demiflow forge context PROJECT --json
demiflow forge describe PROJECT --json
demiflow forge manifest PROJECT --json
demiflow forge check PROJECT --json
demiflow forge dry-run PROJECT --config RUN_CONFIG --json
demiflow forge verify PROJECT --json
demiflow forge status PROJECT --json
demiflow forge review PROJECT --file REVIEW --json
demiflow forge freeze PROJECT --json
demiflow forge run PROJECT --config RUN_CONFIG --json
```

生命周期门禁：

- `check`：静态契约、布局、API、依赖、Manifest 等；
- `verify`：check + dry-run + 受控 pytest + 测试前后快照；
- `review`：绑定当前 revision，通过时不得包含 blocker，区分 self/independent；
- `freeze`：要求当前 Verify 和 Review 通过；
- `run`：要求当前 check、Verify、Review、Freeze 全部有效；日志损坏也会阻断；
- `status`：实时报告 `draft/implemented/tested/reviewed/frozen` 及 `missing/stale/failed/corrupt/passed`。

真实运行只接受项目内 `datasets/` 的相对 `.lance` 路径和固定正整数输入版本。输出 schema 从 JSON Schema 派生 Arrow schema，因此合法空输入也能写空表。operator 异常、schema 错误和写入不确定性不自动重试或转换成业务通过。

现有 `demiflow run --root ... --backend ...` 是不可变 bundle 的旧命令，与 `demiflow forge run` 不同，没有合并或改写旧语义。

## 6. 已完成的验证

### 6.1 Forge 聚焦与真实用例

- **25 项 Forge 测试全部通过**。
- 覆盖 Request/Definition/schema、Manifest、scaffold、禁止行为、超时、日志洪峰、秘密隔离、测试修改源码、Review/Freeze 过期、日志损坏、并发锁、路径逃逸、symlink、真实 Lance 正常与空输入、operator 异常等。
- `examples/forge_text_map` 完成真实输入 → trim/uppercase → Lance 输出。
- 真实 CLI 动线通过：`new → manifest → check → verify → review → freeze → run`。
- wheel 构建成功；在仓库外隔离 venv 安装后，同一完整生命周期和真实输出核对通过。
- `smoke_standalone.py` 与 `git diff --check` 通过。

### 6.2 全仓回归

最终带详细日志的全量结果：

```text
1865 passed, 17 warnings in 1488.37s (0:24:48)
pytest_exit=0
```

两个此前不稳定的时间敏感测试本轮也通过。警告为既有 Lance 弃用及多进程提示，不是失败。

### 6.3 Chromium

为解除浏览器测试阻塞，已在工作区安装 Chromium、Headless Shell、FFmpeg：

```text
/yzp/zhaozy/yangzepeng/0905/.cache/ms-playwright
```

17 个浏览器相关用例全部通过。运行相关测试需要：

```bash
export PLAYWRIGHT_BROWSERS_PATH=/yzp/zhaozy/yangzepeng/0905/.cache/ms-playwright
```

**Chromium 不随源码或 wheel 分发。** 普通 Forge 用户不需要它；原生浏览器搜索用户按 `docs/native-search.md` 安装：

```bash
pip install '.[search,search-browser]'
python -m playwright install chromium --no-shell
```

## 7. 生产前仍须明确的限制

当前测试证明 v1 的实现和约定范围，不证明通用生产适用性：

- `forge run` 仍是本地 v1 执行器，不能直接读外部生产表；
- 项目锁只协调本机 Forge 调用，不是分布式锁；
- `append` 不自动构成业务幂等；
- 测试子进程可信但不是安全沙箱，不能阻止其主动联网、读其他文件或消耗资源；
- Review 是人工/Agent 署名审查，不能代替业务语义与训练效果验证；
- 当前没有模型节点、生产恢复、固定评测演进或生产 SLA 证据。

生产使用要区分：用 Forge 约束生产 Pipeline 的开发，与直接用当前 `forge run` 执行生产任务。后者需要按实际生产要求另做契约和运行时验收。

## 8. 下一项真实业务验证：训练数据合成题目

目标业务入口：`demiwtg/training/sample_refgen_question_references`。

建议首个预生产需求固定为：

> 从固定合格 GT 合成短／中／长三档训练题目，独立题审，最多一次修订并独立复审，交付完整题目记录；本批不做参考匹配、向量采样或训练。

对应现有 `produce + stop_after=questions`，不是全量参考和训练链。

### 8.1 首轮输入

- 固定 `gt_candidates_source` 与 `gt_summary_source`，均含 URI 和正整数版本。
- 上游摘要必须 `role=gt`、`processing_complete=true`，并准确指向合格 GT 表。
- 首轮只选 **2 个合格 GT、6 个题位**。
- 每个 GT 具有概念身份/版本、目标图片及固定 SHA、必要上下文和来源。
- `expected_questions=6`，任何失败、拒绝或无题都不得缩小分母。

### 8.2 业务链

1. 模型调用前验证上游完成状态、GT 身份、图片 SHA 和本轮范围。
2. 每个 GT 生成 short/medium/long 三道题。
3. 作者和题审是独立请求；作者自检不能进入题审输入。
4. 有效 `reject/uncertain` 最多修订一次，并再次独立题审。
5. 通过题锁定；同 GT 的上游身份/内容问题按现有规则阻断，不从其他 GT 补题。
6. 输出全部 6 个题位及摘要，区分成功、业务拒绝、无法出题和技术失败。

### 8.3 输出与非目标

- 每题一行，保持现有 `sample-refgen-questions-v2` 字段契约，不为实现方便扩正式字段。
- 保留原题、修订题、题审、状态、`train_eligible`、真实 prompt/模型调用与来源。
- 摘要保存输入分母、状态计数、固定输出和 `question_processing_complete`。
- 不做参考匹配、向量编码、采样、训练、共享主表更新或全量 3,379 GT。
- 不用旧响应冒充新请求，不以试跑成功宣称训练收益或全量题目质量。

## 9. 对 Forge v2 的建议范围

完整题目生产需求不能塞入 v1 的单 map，也不能为了赶进度把模型调用藏进普通 operator。

建议先设计新的版本化契约，至少包括：

- `map_prompt_async` 模型节点、版本化 Prompt Pack、模型/revision 和独立调用账本；
- 多阶段作者 → 独立题审 → 有限修订/复审；
- GT 图片只读引用、SHA 校验、图片与渲染输入字节上限；
- 每个节点独立的预算、并发、超时、请求身份和失败语义；
- 题目阶段表、最终输出与 summary 的明确写边界；
- 新契约版本的 Definition、Manifest、静态检查、dry-run、Runner 和迁移规则；
- v1 项目保持原语义，不做隐式升级或兼容垫片。

实现前应先读 demiflow 的真实公开模型接口与文档，不复制旧 DemiForge Guidance。业务参考还需读 `demiwtg/PIPELINE_SPEC.md` 和目标 README；业务仓默认只读，除非用户另行授权修改。

## 10. 后续验证顺序

### 阶段 A：离线契约与故障验证

使用临时 Lance、fixture 图片和模拟模型响应覆盖：

- 正常生成与独立题审；
- `reject/uncertain` 的一次修订和复审；
- 二次拒绝后停止，不增加隐式修订；
- 无题、业务拒绝、技术失败、格式错误和合法空结果；
- 完整分母、锁定保护、输出 schema 和 summary 门禁；
- 相同请求复用、语义变化请求不复用；
- 实际渲染请求中的作者/题审上下文隔离；
- 模型额度、并发、超时、空阶段不启动服务。

### 阶段 B：2 GT 真实 canary

只有阶段 A 通过且用户明确授权真实模型调用后执行：

- 核对实际模型、prompt、revision 和请求参数；
- 每请求只包含声明的 GT 图片和材料；
- 逐题人工核对图片、概念知识、题面、答案泄漏、长度和可生成性；
- 恰好 6 个题位，状态与完成门禁正确；
- 独立 Review 无 blocker，才允许扩大第二批。

### 阶段 C：预生产评估

- 扩大小批真实 GT 和业务边界，不直接全量；
- 注入违规实现，目标应拦截项 100% 被拦截；
- 统计 Agent 首次 Verify 通过率、人工返工、契约绕行和漏检；
- 核对实际吞吐、资源、失败恢复和输出一致性；
- 明确回滚、监控、告警、责任人和故障处理方式。

### 生产放行建议

至少需要：真实数据对照无差异、无未解决 blocker、重复运行符合幂等/追加约定、失败不冒充成功、资源在预算内、canary 和回滚方案完成。若生产需要外部表、复杂模型链、分布式恢复或跨机器控制，必须先完成相应契约和运行时能力，不能绕过 v1/v2 边界上线。

## 11. Codex 续接检查清单

1. 阅读 `AGENTS.md`、本交接、三份 Forge 文档及目标业务 README。
2. 核对当前 Git 状态和未提交修改，保留所有既有工作。
3. 先运行 Forge 聚焦测试和真实 example，不急着修改 v1。
4. 明确下一任务是新增版本化模型/多阶段契约，不是给 v1 塞隐藏模型调用。
5. 先提交契约设计、测试边界和非目标，再实现；避免第二套执行器、注册表和兼容层。
6. 真实模型、生产数据、共享服务、全量运行和 demiwtg 修改分别需要明确授权。
7. 交付前重跑 Forge 测试、真实用例和全仓回归，检查既有 dirty 文件未受影响。
