# Forge development F-01/F-02 端到端交付（2026-11-04）

当前状态：**终审返修已合入并重新提交 Review**。本轮修复本地受限/超深导入降为 external、
readonly 遗漏范围内明确配置两项复现；最终为 128 项完整回归通过。当前产物与证据见文末
“终审返修交付”，下列原章节保留上一轮交付记录。

已按主会话 Review 的[源码覆盖设计](demiforge-source-coverage-design-20261104.md)完成并合入。
F-01 的明确本地缺失导入现在产生非通过结果；F-02 的 candidate.9 作者源码可按真实声明穿透
`runs/datasets`，无法核验的混合范围明确保持部分覆盖。没有改写标准来迁就实现。

本报告是平台工具交付和自测记录，提交主会话 Review；不表示任何真实业务任务已认证。
此前已批准的冷启动兼容修复另见[迁移交付补充](demiforge-consolidation-implementation-20261104.md)。

## 实际变更

| 文件 | 结果 |
| --- | --- |
| `demiflow/forge/development/coverage.py`（新增） | 每次操作内唯一的有界本地静态取证：模块、符号、依赖、身份、摘要、诊断和外部边界。 |
| `demiflow/forge/development/project.py` | 按实际 pipeline 标准分派；复用原快照的精确路径/作者子树选择器；同次观察与身份检查。 |
| `demiflow/forge/development/checks.py` | candidate.9 结构检查复用同次事实；缺失/不支持/未覆盖均非通过，不再运行第二份导入扫描。 |
| `demiflow/forge/development/tasks.py` | start/current/context/check/refresh/verify/review/handoff 贯通覆盖依据与指纹；交接 JSON 保留完整清单，Markdown 显示覆盖摘要。 |
| `demiflow/forge/development/resources.py` | 有限 binding 清单加入新 helper；保留冷启动修复已加入的 Data facade 绑定。 |
| `tests/test_forge_development_source_coverage.py`（新增） | 46 项受控临时 fixture 用例。 |
| `tests/test_forge_development_wheel.py` | 隔离安装中额外运行新覆盖用例，检查所有 demiflow 模块来源；保留原 53 项回归。 |
| `docs/demiforge-quickstart.md`、`demiflow/forge/development/skill/SKILL.md` | 同步工具覆盖、部分范围、资源与历史任务说明；Skill 的业务工作流保持原授权边界。 |
| 本报告（新增） | 汇总实际实现、最终产物、证据和保留项。 |

前九个目标文件在合入前逐项核对保存的 SHA，两个新增源码/测试目标确认不存在；只原子替换
这些文件。首次隔离基线 `baseline.json` 留存，`expected-target-baseline.json` 只额外承认
本聊天已获批、已正式交付的冷启动 resources 变化。合入后发现并补强 watch/readonly 重叠的
身份一致性：第二次快照不能用新身份覆盖第一次的旧身份。补充使用本聊天已交付摘要做 guard，
增加真实竞态用例并重建、重验最终产物。两个阶段的清单均保留。

正式 `pyproject.toml`/`MANIFEST.in` 已有其他获批发布的变化，本轮未覆盖它们。最终构建重新
捕获当时最新正式包，而非从初始隔离副本回写其他人的文件。

## 可核对的行为

实际任务绑定 candidate.9 才使用新路径和覆盖策略；candidate.1–8、混合项目中的旧任务、
不传新策略的低层 snapshot 保留原规则。现代任务的 project-wide watch 继续包括所有已登记
现代作者角色；所有权、共享算子、跨入口和 operator→entry 门禁分别保留。

静态检查保留 Import/ImportFrom、相对级别、名称、别名、来源和行号。明确相对或已声明/
实际本地模块的终端、中间包、必要符号缺失均非通过；普通函数、类、赋值及 import 绑定
不被误当成同名子模块。初始化包、真实 namespace 子模块、受支持的再导出和 helper 继续
进入同一闭包；无法解析的循环、动态导出、文件作为包父级、同名模块/初始化包歧义均非通过。
属性/下标赋值不虚构模块级名称。外部绝对 import 单列为 uninspected，不扫描安装环境；
未声明空同名目录不足以判定外部库已被本地替代。全程不执行候选 import 或 find_spec。

普通 watch 保持完整字节哈希。入口、算子文件、Notebook、sources 配置和实际静态依赖只
穿透必要祖先；明确算子目录和明确 watch 的候选内子树按其声明范围读取。兄弟历史/表目录
不因文件例外展开。测试同时监测 scandir 和 os.open，未知历史哨兵未打开、未下探。
未知内容记为 `unverified_ranges`，不冒充缺失标记或内容摘要；必要的真实缺失目标仍保留
None，目录保留原 directory 标记。未单列配置的复现明确部分覆盖、check 失败。

同次观察将已解析 Python 字节摘要与快照比对，复核源码、初始化包、namespace、缺失状态
及上下文身份；跨 tree/readonly 快照的相同物理路径不能被后一次身份悄悄替换。
上下文选段不减少配置完整摘要，Notebook 的保存输出/元数据仍参与完整字节哈希。
context/check/handoff 都提供派生 `source_coverage`，现代 excluded_directories 只列技术保护目录。

start 允许已 watch 的缺失实现/语法错误 draft，后续 check 保持失败；必要源未 watch，或写范围
与未核验范围相交时不能捕获一个自称完整的原基线。新增必要依赖加入当前快照/指纹，始终不
补拍 baseline。未知边界的删减/扩展不能洗掉原范围事实。refresh 更新上下文并清空旧证据，
不移动 baseline/readonly baseline；verify 在可信命令后重新取证，fixture 改必要源码会失败。
新 helper 进入 binding，旧任务继续拒绝当前验收，原记录只读保留。

## 资源与能力边界

复用既有 snapshot_limits 默认值和硬上限，没有提高额度或缩小 watch。默认每个普通文件
16 MiB、源树 256 MiB/20,000 项/120 秒；Notebook 完整摘要按已有 notebook_bytes 流式读取。
静态取证受不超过 20,000 个计量事实/引用、单个 Python 16 MiB、累计源码/Notebook 代码
256 MiB、每次解析 200,000 token/AST 节点和 120 秒限制，并服从更小的声明预算。
Notebook 投影代码额外限制为 1 MiB。路径深度 64、模块/再导出链深度 64、路径长度 4096。

静态事实计量字符串有 16 MiB 上限；每次快照的选择器/未知边界另有 16 MiB 上限，条目和
时间继续受控。只保留有界符号/边/摘要，解析后丢弃 raw/AST；队列先去重、计量再增长。
重叠 watch/角色不重复哈希同一文件；readonly 保留独立快照和原写保护。小 entries/bytes/
AST/time 预算、特殊文件、符号链接及技术控制路径均测试为显式失败。
这些是主动读取/计算/元数据准入，不是 OS RSS、全机磁盘或安全沙箱保证。

不证明任意动态 import、反射、动态再导出、任意文件读取、外部库可用性、namespace 与安装
环境合并、扩展模块、业务效果或执行语义。明确剪枝边界只能报告缺口，不能自动辨认其中配置
与产物。仍需真实声明、可信 fixture 和人工语义 Review。没有新建配置协议、执行器或框架。

## 验证与产物

最终结果：

| 最终检查 | 结果 |
| --- | --- |
| 正式源码完整环境（含最终 wheel） | **120/120 通过，20.628 秒**。 |
| 正式源码纯标准库环境 | 120 项中 **116 通过、4 项按依赖/未指定 wheel 条件跳过，5.217 秒**。 |
| 新覆盖用例 | 46 项均通过，包含在上述完整回归及安装验证中。 |
| `/bin/python3 -I -B` 独立解压 wheel | 原 53 项 + 新 46 项，共 **99 项通过**；禁止 Arrow/YAML/Click/Demiurge 回退，并核对所有已加载 demiflow 模块来源。 |
| wheel 公共入口与冷启动 | 单独安装路径下的 launcher/CLI、基线/验证/历史查看 smoke，以及五种完整环境冷启动均通过。 |
| 原生 catalogue 回归 | **24 项通过，15.01 秒**；禁用无关 pytest 插件后有 1 条 asyncio_mode 配置警告。 |
| wheel/sdist 内容 | development 全部 25 个文件/资源与源码一致，九份标准 SHA 与冻结 fixture 一致。 |
| 历史/业务控制核对 | 427 个历史文件无差异，8 个业务控制文件合入前后无差异。 |

45 项初版边界用例后，增加 watch/readonly 重叠竞态，最终共 46 项。覆盖缺失终端/中间包/
from-child、普通符号/别名、namespace/init/再导出/循环、隐式初始化反向依赖、缺失 legacy
入口引用、共享算子、外部环境边界、任意角色名称、受限穿透、部分覆盖、完整配置/Notebook
字节、源码变化与 fixture 变化、原始 baseline、旧版分派、预算及文件身份。

候选构建和正式构建都只使用现有 Python/setuptools 离线构建，没有安装或修改共享环境。
最终源码快照 `final-verified-source/` 来自正式仓，枚举 1,670 项、1,460 文件、40,054,349 字节；
捕获预算 20,000 项/160 MiB 总量/32 MiB 单文件/30 秒。交付核对时 1,000 个正式包文件与该快照无差异，正式 pyproject/MANIFEST 也一致。

最终产物（该目录中的文件为本次最终交付；其他 candidate/final 目录是保留的过程证据）：

| 产物 | 字节 | SHA256 |
| --- | ---: | --- |
| [demiflow-0.3.0-py3-none-any.whl](/tmp/forge-source-coverage-implementation-20261104-sl8h0a3t/final-verified-dist/demiflow-0.3.0-py3-none-any.whl) | 13506698 | `2a799f647e600c355849eab4645f61023616c325f6fcb9b28d7bec26738cac96` |
| [demiflow-0.3.0.tar.gz](/tmp/forge-source-coverage-implementation-20261104-sl8h0a3t/final-verified-dist/demiflow-0.3.0.tar.gz) | 14215390 | `208fa970471a8d9229389349baa74ddf7b95fc346e54ec7475c7727b17075d7c` |

wheel 中 25 个 development 包/资源文件逐字节匹配构建源码；wheel 和 sdist 的九份标准均
匹配冻结 fixture。sdist 含新 helper、新测试、Skill 和 quickstart。完整逐文件源码摘要在
`final-verified-source-manifest.json`，产物摘要在 `final-verified-dist-validation.json`。
本报告作为构建后验证记录单独保存，不以自身摘要循环定义构建内容。

## 发布前置与保留证据

先以 wait_threads 观察 Web 当前发布。总控随后明确解除前置：首版及项目列表/tab 已正式
发布并核验，允许本聊天完成范围内合入；对应
`/tmp/forge-project-tabs-20261104-5ef7zu7y/evidence/release.json` 的状态为 published。
最后一次 wait 见 Web 已开始另一个界面调整 turn，不再据此重复等待已解除的发布前置。
未向 Web 发送消息。完整依据在 `publication-gate.json`。

427 份此前已枚举历史 task/handoff 文件逐字节仍与迁移前摘要一致，8 个业务登记/launcher
控制文件在本轮合入前后相同；未枚举任务 scratch 或生产数据来完成此核对。
九份标准全文、业务登记、历史 baseline/readonly baseline、原 accepted/问题记录未改写。
本轮只动 development 及对应测试/文档，未改原生 Forge 检查器/执行链、Web、服务或真实业务源码。

完整工作证据：`/tmp/forge-source-coverage-implementation-20261104-sl8h0a3t/`。
重点索引：

- `baseline.json`、`before/`：原隔离依据，完整保留。
- `adopted-approved-cold-fix.json`、`expected-target-baseline.json`：已批准冷启动变化的显式承认。
- `formal-merge-manifest.json`、`formal-final-merge-manifest.json`、`overlap-identity-correction.json`：
  逐项合入前/后摘要；`owned-changes.diff` 保存相对于各目标合入前字节的实际差异。
- `publication-gate.json`：发布前置解除依据。
- `preservation-premerge.json`、`preservation-final.json`：历史和业务控制文件只读核对。
- `verified-development-stdlib.log`、`verified-development-with-wheel.log`、
  `verified-native-catalogue-regression.log`：最终测试日志。
- `final-verified-source-manifest.json`、`final-verified-source-budget.json`、
  `final-verified-dist-build.log`、`final-verified-dist-validation.json`、
  `verified-package-consistency.json`：正式源码、构建预算、离线构建、归档内容和摘要。
- `delivery-evidence-manifest.json`：本报告、变更文件和最终证据文件的独立摘要索引。

旧任务没有 continuation 或重基线；若需续接，由主会话按原事实另行 Review。本轮没有借 F-01/
F-02 修复追认任何历史检查、业务认证或生产执行权限。

## 终审返修交付（2026-11-04，当前版本）

依据[终审裁决](forge-source-coverage-review-20261104.md)，上一轮 120 项测试没有覆盖的两项
问题均已复现、修复并合入，现重新提交终审。原架构、标准及历史任务继续保留。
上一轮产物和证据是过程记录；本节列出的代码、结果与制品为当前返修交付。

### 两项修复与实际结果

1. **保留本地身份，再执行受限/超限判定。** `coverage.py` 先识别已登记本地根；对其他
   名称，仅在最多 64 段、安全前缀中查找已有本地文件/初始化包证据。已知本地导入不会因
   后缀包含 venv/__pycache__ 或超过 64 段而成为 external。module 在探测受保护路径前
   返回明确 DF-03 受保护/深度预算诊断，并保存本地解析失败状态。`import orders.venv.missing`、
   `from orders.__pycache__ import missing` 和 orders 下 65 段导入完整经历 start、全部 context、
   check 后均为非通过、coverage incomplete、external 清单为空；verify 拒绝，handoff 不能接受。
   另覆盖 from-child、相对 import 及实际本地初始化包前缀。通过 os.open/scandir 哨兵确认
   受保护目录未打开、未扫描；普通未登记外部 venv 的完整生命周期继续通过并明确 uninspected。

2. **readonly 继承范围内的精确来源。** `project.py` 的 tree/readonly 复用同一取证对象派生
   的精确来源集合。readonly 索引其在项目内的真实根，只将位于这些根中的已声明/已取证路径
   投影为该 readonly 根的快照标签；不会把整个普通 watch 或无关来源作为 readonly 例外。
   `sources.settings=platform/runs/settings.json`、`readonly=[platform]` 现在同时具有 tree 和
   readonly 完整摘要，初次 check/verify 通过。修改配置后 fingerprint 变化，refresh 保留
   原 baseline/readonly baseline，check 明确报告 readonly changed，verify/handoff 不能接受。
   有兄弟 history/table 时只读取明确配置，历史仍为 unverified/nonpassing；打开和遍历哨兵
   均未触发。绝对根、含 .. 的合法根及末尾斜线根均验证；选择器匹配采用 Path 规范形式，
   原 readonly 标签继续保留。相同文件在 tree/readonly 间变化仍触发首次身份冲突。

不调整动态导出分支：`from_symbol` 的 AST 与返修前相同；专门加入真实 clean.py 与包级
`__getattr__` 返回 42 并存的用例，结果仍是 unsupported dynamic local export，且不执行候选。

精确来源数量继续受不超过 20,000 项及声明预算控制；readonly 根索引/投影字符串合计
最多 16 MiB，复用本次观察剩余 deadline。按路径祖先查找根，避免将全部根与全部来源做
笛卡尔遍历；超限明确失败。既有快照、Python、Notebook、符号链接和只读保护预算保持。

### 回归与最终制品

| 返修最终验证 | 结果 |
| --- | --- |
| 最新正式源码完整环境，含最终 wheel | **128/128 通过，24.385 秒**。 |
| 最新正式源码纯 stdlib 环境 | **124 通过、4 项按依赖/未指定 wheel 跳过，7.090 秒**。 |
| 独立 wheel 安装 | **107 项回归及 smoke/五种冷启动通过**，包含于上项完整环境验证。 |
| 原生 catalogue 回归 | **24 项通过，14.32 秒**；禁用无关 pytest 插件后 1 条 asyncio_mode 配置警告。 |
| 当前覆盖测试 | **54 项通过**，两项问题均有完整生命周期验证。 |
| 内容/保留检查 | 9 份标准、25 个 development 包/资源文件一致；427 个历史文件及 8 个业务控制文件无差异。 |

新增 8 个测试方法，覆盖测试由 46 增至 54 个；完整 development 由 120 增至 128 个。
独立 wheel 子进程运行原 53 项与当前 54 项，共 107 项，禁止 Arrow/YAML/Click/Demiurge
回退并核对所有已加载 demiflow 模块来源；五种完整环境冷启动、launcher/CLI、真实 fixture
验证和历史只读 smoke 同时通过。

原独立 probe 未修改。仅在本聊天证据目录复制并替换导入/临时 fixture 路径，分别针对候选
和最终正式源码重跑：三个本地受限生命周期均非通过，readonly 配置 complete 且有摘要，
动态导出仍非通过。新增用例在修复前失败的日志亦完整保留。

| 当前最终产物 | 字节 | SHA256 |
| --- | ---: | --- |
| [demiflow-0.3.0-py3-none-any.whl](/tmp/forge-source-coverage-review-fix-20261104-1a9eob1a/final-dist/demiflow-0.3.0-py3-none-any.whl) | 13509423 | `bd4312decd1be482cd4b7e83dbddece5d18eeb4ab5d599135300dea1a46b1aee` |
| [demiflow-0.3.0.tar.gz](/tmp/forge-source-coverage-review-fix-20261104-1a9eob1a/final-dist/demiflow-0.3.0.tar.gz) | 14232294 | `ae4b19b7ef7fdf8f860a84116bcc2b4383dbddeda6eb957f52999905aa6472c4` |

最终包从合入后的最新正式仓捕获并离线构建，未安装或修改共享环境。捕获共 1,680 项、
1,469 个文件、40,452,411 字节，仍受 20,000 项/160 MiB 总量/32 MiB 单文件/30 秒限制。
1,000 个包文件及正式 pyproject/MANIFEST 与该构建快照一致。wheel 的 25 个 development
文件/资源、wheel/sdist 的 9 份冻结标准和 sdist 的新测试均逐字节核对。
本报告是构建后验证记录，制品内报告为构建时版本，当前交付索引以本节为准。

### 变更、基线和证据

本轮代码/测试仅改 `coverage.py`、`project.py`、`test_forge_development_source_coverage.py`，
另更新本报告。合入前四个目标 SHA 全部匹配本轮原始 baseline；按目标原子替换，原正文
保留为上一轮记录。没有修改原生 Forge 检查器/执行链、Web、服务、共享环境或业务登记。
427 个原历史 task/handoff 文件仍与迁移前摘要一致；8 个业务控制文件合入前后相同，
原 baseline/readonly baseline、历史 accepted/问题记录未重写。9 份标准全文保持不变。
工具 binding 正常随实现变化，旧任务继续只读，未创建 continuation 或重拍基线。

当前证据目录：`/tmp/forge-source-coverage-review-fix-20261104-1a9eob1a/`。

- `baseline.json`、`before/`、`formal-merge-manifest.json`、`delivery-owned-files.json`、
  `owned-changes.diff`：本轮原字节、合入 guard 和完整实际差异。
- `reproduced-before-fix.log`、`affected-after-fix.log`、`dynamic-export-branch-unchanged.json`：
  修复前复现、受影响测试及动态分支保留依据。
- `independent-probe-adapted.py`、`independent-probe-after-fix.log`、
  `independent-probe-final.py`、`independent-probe-final.log`：独立复现的路径适配和实际结果。
- `final-development-stdlib.log`、`final-development-with-wheel.log`、`final-native-catalogue.log`：
  最终源码、stdlib、独立安装和原生 catalogue 回归。
- `final-source-manifest.json`、`final-source-budget.json`、`final-dist-build.log`、
  `final-dist-validation.json`、`final-package-consistency.json`：最新正式源码与最终产物摘要。
- `preservation-before.json`、`preservation-final.json`：427 个历史文件和 8 个业务控制文件核对。
- `delivery-evidence-manifest.json`：当前报告、变更源码及上述最终证据的独立摘要索引。

本节更新原交付，未追认任何真实业务认证；最终验收结论仍由主会话 Review。
