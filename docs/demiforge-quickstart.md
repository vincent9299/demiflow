> 历史文档。当前实现见 [Forge 开发辅助方案](forge-guided-coding-redesign.md)。

# DemiForge 最小闭环

当前版本：`1.0.0-candidate.10`。Python 3.10+、POSIX、源码检出或正常解压安装的 wheel 均可使用。
项目 launcher 与 dev core 只依赖标准库；公共 Click CLI 使用已安装平台依赖，不调用模型。
使用公开 CLI/项目 launcher；`development` 内部 Python API 不是稳定集成接口。

candidate.8 新增 [DF-03 性能与精简编排](../demiflow/forge/development/standards/pipeline-v1.0.0-candidate.8.md#efficient-and-minimal-orchestration)：审查全量扫描、关联、物化和重复执行；用已有回执统计，避免为启动校验另扫全表。该要求由语义审查和针对性测试确认，未宣称自动识别所有性能问题。

本版使用 `demiforge-project-v3`：一个业务项目拥有多条具名流程、共享代码算子和 Prompt 定义。
按 `projects.<id>` 登记项目归属；`flows` 下逐条登记实际 `{path, symbol}`，不设项目默认入口。
推荐项目内 `flows/ + operators/ + prompts/`，已有明确登记的其他文件名仍可用。
不再要求把不同业务流程硬合成一个 `run(stage=...)`，也不为每条流程复制算子或 Prompt。
每条流程只有一份接线必需的业务参数及当前值；不要求参数文件、参数方案或配置下拉框。
算子默认值、Prompt 定义、引擎/部署设置分别维护，运行记录封存当时实际值。

[新规范 DF-02](../demiflow/forge/development/standards/pipeline-v1.0.0-candidate.10.md#df-02-project-flows-and-reusable-definitions)
规定归属、复用、参数和迁移边界。[project-multiflow.json](../examples/development/project-multiflow.json)
展示两条流程共享一份算子目录和 Prompt 目录；不执行业务代码即可检查所有入口和静态依赖。
源码、Prompt 和历史保护保持；没有因支持多流程而放宽 watch、资源预算或跨入口依赖限制。
当前升级是规范与开发工具，新的算子库和单份参数页面仍按独立产品设计实施。

历史 project-v1/v2 和 candidate.1–9 保留原绑定。[candidate.9 示例](../examples/development/project-workspace.json)
继续验证当时的一项目一入口契约。显式迁移时更新项目规则、角色登记和工具位置，创建新任务；
不覆盖历史快照、认证、Notebook 输出或已有 dirty，不批量移动生产代码和数据。

candidate.5 的 DF-03 收敛业务模型入口：demiflow 使用 `map_prompt`、`map_prompt_async`、
`agentmap_async`，固定评测只需 `map_prompt_async`；不在业务回调中直连模型或自建模型 Actor。
既有向量/损失等专用入口需要单列平台迁移缺口，不能重命名成统一入口冒充完成。
这是版本化规范与审查要求，未宣称程序能完整扫描或隔离所有模型调用。

## 规范和归属

当前候选标准是 [candidate.10](../demiflow/forge/development/standards/pipeline-v1.0.0-candidate.10.md)，
包含 DF-01 到 DF-07 的要求、来源、例子和验收方式。项目配置不能关闭其中任何条款。
具体 Dataset/模型 API 是平台适配；业务表、字段、prompt 正文和阈值归项目。
demiwtg 的 PIPELINE_SPEC.md 仍是现行强制规范，本次只提炼候选，不删除原规范或
将所有旧流程标为合规。遇到冲突先解决，不能择易执行。

DF-02 的 [README 持续维护要求](../demiflow/forge/development/standards/pipeline-v1.0.0-candidate.5.md#readme-maintenance-for-every-change-2026-10-12)
要求每次 pipeline 变更同步更新对应 README 正文，并在交付前审查与最终实现的一致性。
当前由署名语义审查确认，程序未自动判断更新是否充分、准确。
旧标准文件及项目绑定保留；现有项目显式升级 `standard` 后创建新任务，不能通过 refresh
重置旧任务的验收依据。工具版本变化同样会使旧任务的工具绑定失效，不能沿用旧回执。

## 接入项目

1. 参考 [多流程示例](../examples/development/project-multiflow.json) 创建项目配置；
   [旧布局示例](../examples/development/project.json) 保留用于旧标准：绑定版本、
   当前规则、全部相关源码根、只读平台路径，以及已审阅的 fixture 测试命令。
   `tool_root` 相对业务仓根或为绝对路径，须指向实际加载的 demiflow checkout 根，
   或正常安装的 `demiflow` 包目录（不是整个 site-packages）。配置采用 stdlib 可解析的 JSON。
2. 运行 `python3 -B -m demiflow.forge.development --project /path/to/business init
   --config /path/to/config.json`。不覆盖既有配置；生成 `.demiforge/project.json`、
   `.demiforge/forge` 和本地任务记录的 ignore 规则。
3. 在项目 AGENTS.md 放一个入口，要求 pipeline 开发使用当前配置和此流程。
   将唯一正文所在的 `demiflow/forge/development/skill` 链接到 Codex 的 skills 目录。
   wheel 中也在相同包资源位置，配套脚本可直接运行；若导出部署副本，记录来源版本与完整资源哈希，不能将副本当第二维护源。
   安装位置不等于当前聊天已加载；新聊天中确认 skill 可发现，也可直接读其 SKILL.md。
4. 登记业务项目的 `path`、`flows` 映射、`operators`/`prompts` 路径列表、`notebooks` 列表和可信验证命令。
   每条流程一个独立入口模块和明确函数符号；项目没有默认执行入口。未用角色可用空列表。
   所有角色与文档/测试必须在 watch 内；缺失实现可 start，不能通过 check。
5. v3 采用 candidate.10。已有 v1/v2 项目先继续原版本；迁移须显式更新完整登记与项目规则，
   不能只改默认版本或沿用旧通过状态。`--project-name` 选择开发任务所属业务项目；旧 `--pipeline` 别名保留。
6. `snapshot_limits` 可显式设置 `total_bytes`、`file_bytes`、`notebook_bytes`、`entries`、
   `seconds`。默认分别为 256 MiB、16 MiB、256 MiB、20,000 项和 120 秒；
   硬上限分别为 16 GiB、256 MiB、1 GiB、200,000 项和 600 秒。先统计完整监测范围，
   再选择预算；增大文件哈希预算不放宽 Python AST/上下文读取限制。不缩小 watch 或删除 Notebook 输出绕过失败。全文件流式哈希，
   Notebook 解析只保留 code/tags（1 MiB、每容器 4,096 项、深度 64、30 秒），输出仍受哈希保护。

从项目根开始：

```bash
python3 .demiforge/forge start --id orders-fix --project-name orders \
  --goal '修复已确认的订单输入边界' --write orders/operators/quality.py \
  --write orders/tests --write orders/README.md \
  --non-goal '不改平台、不执行生产、不安装共享依赖'
python3 .demiforge/forge context --id orders-fix
python3 .demiforge/forge context --id orders-fix --read standard --page 1
# 按 context 的页数逐页读取全部必读来源，再开始编辑。
python3 .demiforge/forge note --id orders-fix --kind progress --text '已补边界用例，待验证'
python3 .demiforge/forge refresh --id orders-fix --reason '更新了目标 README，重新核对上下文'
# 阅读变化的来源，再执行 check / verify。
python3 .demiforge/forge check --id orders-fix
python3 .demiforge/forge verify --id orders-fix --allow-tests
python3 .demiforge/forge review --id orders-fix --file /tmp/orders-review.json \
  --reviewer '开发者姓名，self-review'
python3 .demiforge/forge handoff --id orders-fix --accept
```

Review JSON 的每一条都必须经过实际审查，不是自动填充的通行证：

```json
{
  "DF-01": {
    "status": "pass",
    "reason": "实际审查结论、范围和限制，不是重复规范原文",
    "evidence": ["orders/README.md", "orders/processing.py"]
  }
}
```

实际提交必须包含 **全部 DF-01..DF-07**；上例只展示单条结构，不是可直接通过的文件。
仅 DF-05 对无模型/prompt 的任务允许说明依据的 `not_applicable`。
Review 是署名人的语义判断，不是工具自动证明，更不自动等于独立评审。

续接前运行 `list` / `history --id orders-fix` 只读查看目标、原范围与当时结果，
同绑定任务再用 `context --id orders-fix` 检查当前来源和证据。旧 accepted 不代表当前通过。不要为消除越界记录另建任务基线。未解决问题用 `note
--kind question` 记录，会阻止接受；确认后用 `resolve --question <note_index> --reason ...`。
长期决定回写业务 README；`note --kind decision` 记录来源和理由，改变决定会使旧证据失效。

## 项目配置不是规范开关

- `sources` 是必读来源映射。`project-rules` 必须存在；pipeline README、入口和统一标准
  自动加入。可用 `"lines": [1, 35]` 声明**显式节选**，行号闭区间；节选不能冒充全读。
  完整源文件仍参与版本指纹。更改选段或必读来源属于项目配置变更，需要重新审阅接入。
- `watch` 包括相关业务源码、测试、prompt、配置和共享帮助代码；允许改动必须在其中。
  基线是 start 时的内容，不是 Git HEAD，保留并识别已有脏文件。目录标记、新增、删除、
  未跟踪文件也参与比较。工具不判断谁改的；其他聊天同期改动同样可能阻止验收。
- 目录快照记录软链接本身的地址摘要，不跟随目标；图片展示链接、断链、目录环不会仅因软链接身份阻止登记。
  链接新增、删除、改指仍参与变更检测及条目/时间/字节预算；目标内容不属于此链接的源码取证。
  实际必读源码、配置、状态及直接声明的只读根仍保留原路径校验，不把数据链接当作已检查的源码。
- `readonly` 为只读平台源码和契约路径。直接声明的根路径必须可读，符号链接拒绝。
  只对登记源进行摘要比较，不能阻止别的 shell 写入；不要声称整个共享机器被保护。
- 测试 `argv` 不经过 shell，仅支持 `{project}`、`{tool}`、`{scratch}` 三个路径替换。
  `env` 显式传入配置，默认不继承凭证；HOME/TMPDIR 重定向至任务临时目录，禁用 bytecode
  和 pytest 自动插件发现。可信命令仍能主动联网/读取本机文件，不构成安全沙箱。
  审阅命令及其测试代码、数据隔离和副作用是接入责任，不因名称为 test 就自动安全。
- 每命令 1..600 秒超时，合并输出上限 1 MiB；超时/超量失败并杀掉该进程组。
  不限制全机 RSS、磁盘量、外部服务或主动脱离进程组的程序。开发任务不授权这些行为。

## 证据与限制

`.demiforge/tasks/<id>/task.json` 持久保存当前基线、读页回执、决定、检查、测试和审查。
`verification/<invocation>/` 保存独立日志和临时 fixture 数据；`handoff.md/json` 为当前交接。
它们忽略 Git，留在本机供新聊天恢复；跨机器必须显式导出经审阅且不含秘密的记录。
不要把自动清理任务目录当作普通验收步骤。

验收绑定工具代码、标准全文、项目配置、所有监测文件、只读文件、上下文选段和任务决定。
代码、测试、prompt 或相关配置变化后旧测试和 review 都过期。上下文变更必须 refresh
并重新阅读；refresh 不重置修改基线。工具/标准/配置变更不允许旧任务继续接受。
日志丢失/变化也会拒绝接受。文件摘要是本地可信工作流的证据，不是防篡改签名。

当前程序检查：必读页回执、声明路径边界、只读变更、目录/入口、Python 语法、
notebook 隐藏定义、README 规范入口。candidate.9/10 在同次观察中复用本地静态导入依据，
检查缺失模块/符号、包初始化、受支持的再导出及通过 helper 的反向/跨入口依赖。
它不执行候选 import，也不探测安装环境；外部绝对 import 明确列为未检查。
这些检查不证明 Agent 已理解规范，也不证明动态 import、反射、任意文件读取、
数据语义、模型调用、prompt 质量或业务效果。

快照默认预算为每文件 16 MiB、单次源树 256 MiB、20,000 项、120 秒；既有
`snapshot_limits` 可在硬上限内明确配置。candidate.9 静态取证另外受不超过 20,000 项/引用、
16 MiB 单个 Python、256 MiB 累计源码、每文件 200,000 token/AST 节点、16 MiB 派生元数据
和 120 秒限制，并服从更小的声明预算。树深最多 64；重叠 watch/角色对同一文件只哈希一次。
超限明确失败，不截断后标完整。上下文每源最多 1 MiB，每页 8,000 字符。
历史手工改动、权限变更、解释器/第三方包升级和未登记目录不受源码指纹完整覆盖；涉及这些
变化时重新确认环境并重新验证。本版仅支持本地 Python/POSIX 项目。

### candidate.9/10 的本地源码覆盖

策略由任务实际绑定标准选择；混合项目中的 candidate.1–8 任务仍使用原路径与剪枝规则。
现代路径允许 `runs/datasets` 名称，角色、所有者、watch、readonly 和写权限仍分别校验。
`.git/.demiforge/__pycache__/.pytest_cache/.venv/venv/node_modules/.ipynb_checkpoints`
继续受保护。普通 watch 范围继续完整哈希，Notebook 的输出和元数据也保留完整字节摘要。

在宽泛 watch 下，`runs/datasets` 只是默认剪枝候选。明确登记的入口、算子、Notebook、
`sources` 配置文件和可静态定位的本地依赖，只穿透到必要文件及其包初始化/目录。
声明为算子目录的作者子树、明确 watch 到候选目录内的子树按其实际范围遍历；不会因为一个
文件例外展开兄弟历史或表目录。例如 `orders/runs/settings.json` 登记为 source 后会完整
哈希该文件，仍不进入兄弟 `orders/runs/history/`。`sources.lines` 只裁上下文，不能裁摘要。

`context`、`check` 和 `handoff` 的 `source_coverage` 列出精确来源、作者子树、
默认剪枝候选、外部导入及 `unverified_ranges`。未声明配置或源码/产物混合边界不明时，
覆盖为部分、check 非通过；不能仅凭目录名认定这些内容是运行产物。写范围与未核验区域相交
时 start 拒绝；必要本地源未 watch 时也不能建立原始基线。已 watch 的缺失实现或语法错误
可建立 draft，机械检查仍失败。需要补全声明时只使用原有 sources/watch/角色字段，保留真实
任务原值并由独立审阅决定登记变化；工具不会自动修改业务登记或读取生产数据。

取证摘要与同次快照逐项比对；文件、初始化包、namespace 或缺失状态在观察期间变化会拒绝
发布结果。每次操作重新取证，verify 在命令后再次观察。新增必要依赖会进入当前 fingerprint，
不会补拍 baseline；覆盖边界或范围外文件变化仍阻止验收。refresh 仅更新上下文与回执、清除
旧证据，原 baseline/readonly baseline 保持不变。此实现改变工具 binding，旧任务继续只读
查看，不能自动续接或继承旧 accepted。

## 当前工具归属与历史查看

唯一实现位于 `demiflow.forge.development`，公共入口为
`demiflow forge dev --project /absolute/business ...`。源码环境也可用
`python3 -B -m demiflow.forge.development` 或项目薄 launcher。公共 CLI 自身仍依赖
Click 和现有平台库；stdlib 入口不需要 YAML、Arrow 或 Click，不安装/修改共享环境。
不再从 Demiurge 导入工具或回退旧目录；DemiOrch、Candidate/Role/Profile 不属于迁入范围。
项目 launcher 在解析 JSON 或导入平台前，按 Project 配置相同的 256 MiB 上限做字节准入；
只读取已准入的文件长度加一字节，拒绝超限、特殊文件或读取中发生变化的配置。
项目任务 JSON 保留旧序列化语义，原子文本落盘复用平台唯一 `forge.io` 实现。

9 份标准逐字节保留，用 `importlib.resources.files("demiflow.forge.development")`
读取 `standards/`、`skill/SKILL.md` 与 `skill/scripts/forge.py`。支持源码及正常解压的 wheel；
不宣称直接 zipimport/zipapp 支持。binding 按稳定逻辑名绑定有限 dev 源码、父包、公共
I/O/轻导入依赖、launcher/CLI 适配与实际标准全文，不递归扫描环境。`{tool}` 展开为
经核对的 `tool_root`，不会另推旧仓位置。

```bash
python3 .demiforge/forge list --offset 0 --limit 20
python3 .demiforge/forge history --id orders-fix
python3 .demiforge/forge history --id orders-fix --artifact task --page 1
python3 .demiforge/forge history --id orders-fix --artifact handoff --page 1
```

`list` 与 `history` 不实例化 Project、不锁文件、不写任何记录、不执行源码或配置命令。
当时结果仅显示为 `as_of_result`，当前证据恒为 `unverified`；binding 相等也只表示
工具/配置身份匹配，未重新检查源码、日志和接受门禁。原上下文正文未存入旧任务时明确
标记缺失，只展示原路径与摘要，不用当前文件冒充原文，不补回执。

历史查看每次最多枚举 10,000 个目录项、读 32 MiB（单文件 8 MiB），每页最多 50 个
任务，JSON 深度 64、结构 token 200,000、输出 1 MiB、时间 10 秒；超限明确拒绝。
artifact 每页 8,000 字符，目录/字节预算内不读取日志、scratch 或业务数据。
它是本地读取预算，不是 OS RSS/权限沙箱。源码、配置、标准和包位置变化使旧绑定过期；
原 baseline/readonly baseline、未解决事实和当时 accepted 保留原字节。
本轮没有 continuation，不能为续接重拍 baseline，也不要求所有旧任务先结项才切工具。

F-01/F-02 的设计和实际验证分别见
[源码覆盖设计](demiforge-source-coverage-design-20261104.md) 与
[实施交付](forge-source-coverage-implementation-20261104.md)。这不构成当前业务认证。自测：
`python3 -B -m unittest discover -s tests -p 'test_forge_development*.py' -v`。

### 旧标准的固定三格 Notebook（candidate.2–6）

仍绑定这些旧版本的 Notebook 按 DF-02 固定为运行编排、运行监控、最终结果三格，分别使用
`pipeline-run`、`pipeline-monitor`、`pipeline-results` 标签。通过运行 ID 或命名配置切换批次，
不为新运行追加格子；后两格只读。第三格采用“汇总 → 明细”，从总分、维度分及有效样本数进入逐题详情，并可返回同一汇总。
candidate.3/4/5/6 第三格默认内嵌可离线交互的 srcdoc HTML，完整 HTML 与媒体快照仍归选定 run；
结果输出只嵌入一份有界快照，外部报告链接作为备用，不能替代格内结果浏览器。
用户要求整理旧 Notebook 时，先按原字节完整归档并记录 SHA；不得直接丢弃历史输出。
工具可在总快照预算内流式记录大历史 Notebook 的摘要。candidate.1/2 的 Notebook 上限仍为
16 MiB；candidate.3/4/5/6 的序列化 HTML 上限为 64 MiB，完整 Notebook 上限为 96 MiB，
Python 格源码上限仍为 16 MiB。工具检查 Notebook/源码大小，HTML 大小及真实离线交互须提供行为验证。

候选标准 `1.0.0-candidate.2` 固定三格 notebook；旧 `candidate.1` 文本与绑定仍保留，项目显式升级后新建任务。格 2 的处理漏斗逐算子分列“已处理完（输入行）”“待处理（输入行）”“产出（输出行）”，并说明粒度、分母、在途、失败及缺失遥测；不可用产出数反推处理数。

### 单份参数与产品边界

流程入口的具名参数及其默认值/类型是业务参数契约。产品展示该契约的一份当前值，不列配置方案。
复杂值需要明确结构；未知类型不猜字段。修改参数使旧编译预览过期；历史运行只读当时的封存值。
算子库与管道引用同一代码或具名 Prompt 定义；Prompt 文件可含多条定义。
工具目前检查角色归属、源码覆盖、静态依赖和证据绑定；不自动证明参数是否必要、Prompt 语义正确，
也不声称新的产品表单/算子库已经实现。产品方案见 [算子库与多流程设计](forge-project-flows-product-20261118.md)。
