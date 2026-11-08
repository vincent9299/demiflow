# DemiForge 归属收敛实施与复核（2026-11-04）

状态：**A/B/C 与主会话补充批准的最小导入时机修复已实施，最终回归/构建通过；待主会话最终 Review。** 不删除 Demiurge。

## 实际范围与差异

实施依据为 [原方案](demiforge-consolidation-plan-20261104.md)。仅迁当前 DemiForge；DemiOrch、Candidate/Role/Profile、调度、沙箱、冻结协议与运行资产未迁入。Demiurge 只读，无 push、旧仓/业务目录删除、共享环境安装、正式服务更新或生产任务中断。

当前改动为 19 个现存文件、1 个已安装 Skill 软链接及新 development 包/资源、6 份 dev 测试、标准哈希 fixture、3 份迁入文档、2 份示例和本文。仓库已有大量 dirty/untracked 工作，本文不把其他改动归为本任务。

| 范围 | 实际变化 |
| --- | --- |
| `demiflow/forge/development/` | 原 7 个 core 文件迁入；checks/notebook 原字节保持，未实施 F-01/F-02。新增 `resources/io/launcher/history/click_entry/__main__`；runner 仅将 `{tool}` 改用经核对的实际 tool_root。 |
| `demiflow/__init__.py` | 正常 `__getattr__/__dir__` 延迟公开对象；`__all__` 不变，实际对象仍来自原模块。首次请求公开对象时保持 data 优先的既有初始化次序。stdlib dev 不触发重型导入。 |
| `demiflow/forge/cli.py` | 只增加 `forge dev` 薄注册，将原参数和退出码交给唯一 argparse main。原 `forge check/verify/review`、workspace/flow/run 门禁未修改。 |
| `pyproject.toml` / `MANIFEST.in` | 打包全部 9 份 Markdown 标准、唯一 Skill 正文和配套脚本；源码与正常解压 wheel 使用相同资源。 |
| 资源/绑定 | `importlib.resources` 定位；tool_root 只接受实际加载的 checkout 根或 demiflow 包目录，拒绝整个 site-packages/任意目录。有限逻辑文件名绑定 dev、父包、CLI 适配及公共轻依赖/I/O，不递归哈希环境，不用临时资源路径伪装旧绑定。 |
| 原子写入 | dev 保留原 JSON `default=str`、NaN 与无末尾换行语义；唯一 fsync/replace 实现复用既有 `forge.io.atomic_write_text`。 |
| 当前文档/示例 | quickstart、业务开发定位、源码覆盖设计移入当前仓；后者明确原行号/SHA 为设计依据、F-01/F-02 未实施。示例保留旧/新标准版本，只切工具位置。 |

原源文件/配置、Git 状态、历史哈希及具体 patch 均保存在证据目录。`diff-existing.patch` 是相对任务前真实文件的差异；`diff-migrated-core.patch` 是相对只读旧 core 的差异，不用 Git HEAD 冒充任务基线。

## 消费者切换

| 项目 | 原 tool_root | 新 tool_root | 处理 |
| --- | --- | --- | --- |
| `demiwtg` | `../Demiurge` | `../demiflow` | launcher/config；当前 AGENTS、PIPELINE_SPEC、common_concepts README 链接。 |
| `demiwtg-bagel-review-20261016` | `../Demiurge` | `../demiflow` | launcher/config；当前 AGENTS 与规范链接。 |
| `.codex-worktrees/image-top1-20261022` | 旧仓绝对路径 | `../../demiflow` | 仅两个薄工具文件；冻结业务及 prose 不改。 |
| `_demiflow/bagel_input_grounding_paired_20261014/checkout/demiwtg` | 旧仓绝对路径 | `../../../../demiflow` | 仅两个薄工具文件；冻结业务及 prose 不改。 |

后两处是在本轮删除前定点核验中确认的旧可执行入口。四份配置均按原字节仅替换 tool_root 值；schema、逐项 standard、sources/watch/readonly/verify、snapshot_limits 和业务字段逐项一致。两项 candidate.9 与旧项目的其他标准均未升级；现有业务阻塞事实未移除。

根 AGENTS 和 `.codex-home/skills/demiforge` 已改指包内唯一 Skill。当前规范只换权威链接位置，标准版本、章节和全文不改；归档、旧任务、冻结记录、训练和 Notebook 输出不批量替换。

## 历史只读与未完成事实

`list` / `history [--id ...] [--artifact task|handoff|handoff-md --page N]` 不实例化 Project、不进入 tasks.locked、不建立控制目录、不写回记录、不读取当前业务源码或执行配置命令。

- 当时结果字段为 `as_of_result`；当前证据始终 `unverified`。新 binding 与旧记录不等显示 stale；相等也只表示工具/配置匹配，不复用源码/日志/Review 通过。
- 原上下文正文没有存入旧 task 时明确显示缺失；只返回原路径和摘要，不以当前文件替换原文，不补读页回执。
- 原 baseline/readonly baseline、未解决问题、旧 accepted 与失败/越界事实保持原字节。迁移不重拍 baseline、不自动 continuation，也不要求全部历史任务先结项。
- 旧绑定的 refresh/current 拒绝复用，并指向只读 history 和另行 Review 的原基线保留方案；不再建议直接从当前树重开该任务。
- 每次枚举最多 10,000 项、读 32 MiB（含工具绑定读取，单文件 8 MiB）、分页 1..50 项、JSON 深度 64、结构 token 200,000、输出 1 MiB、10 秒。artifact 每页 8,000 字符；超限明确拒绝。不是 OS RSS/权限沙箱或历史动态效果认证。

已复核 427 份原 task/handoff/日志文件的完整哈希一致；只读旧仓 tracked 状态一致，受保护的既有 Forge 文件未变化。该哈希清单不声称遍历了所有 scratch、数据与历史资产。

## Review 补充与验收状态

已补主会话指出的 launcher 配置读取问题：模板从 Project 的 `MAX_TOTAL` 生成相同 256 MiB 上限，打开后先校验普通文件与长度，再最多读原长度加一字节；超限在 JSON/平台导入前拒绝，读取中变化也拒绝。四个实际入口都来自同一模板。每个实际入口的普通 list 通过；复制到自有临时项目后，256 MiB + 1 的稀疏配置均以退出码 2 拒绝，替换 JSON 解析器证明未调用解析。

主会话进一步 Review 并明确批准了冷启动最小补丁：把 `pipeline/core.py` 顶层私有 `_use_executor` 导入移入原 `Pipeline.run` 调用处。仅移动导入时机，执行逻辑和接口不改；新增文件基线后才实施。该文件也纳入有限工具绑定。根公开对象第一次被请求时维持既有 data 优先初始化，直接 pipeline/execution/data 导入与根 API 各自使用新进程验证，保留对象身份、`__all__` 和导入前可发现性。

最终测试、最终 wheel/sdist 及资源哈希见下方验收与制品段。此前冷启动失败与补丁试验保留在证据目录；早期构建不再作为当前制品。

## 最终验收与制品

| 验收 | 实际结果 |
| --- | --- |
| 原 53 项适配回归 | 原测试方法全部保留；只适配 import/资源/launcher 路径及旧绑定错误文案。包含在最终源码与隔离 wheel 两轮通过中。 |
| 完整 dev 套件 | 现有业务解释器下 **74 项通过，34.693 秒**。包括 3 项公开 API/薄 CLI/冷启动测试和 1 项隔离 wheel 集成。 |
| stdlib 入口与边界 | `/bin/python3` 下 **70 项通过、4 项按安装条件跳过，4.197 秒**；明确阻止 YAML/Arrow/Click/旧包导入的入口用例通过。 |
| 无源码兜底的 wheel | `/bin/python3 -I` 只加入自有 wheel 解压目录，拒绝旧包和 YAML/Arrow/Click；9 份标准、Skill/脚本、init/context/check/verify、`{tool}`、两个薄入口和旧 accepted/stale 历史只读通过；集成内再次执行原 53 项，全部通过。 |
| 源码与 wheel 冷启动 | 每种顺序独立新进程：pipeline、execution、data、根 Pipeline、根星号导入；公开身份、`__all__`、导入前 `dir()` 可发现性均通过。wheel 的所有已加载 demiflow 模块均来自临时安装目录，旧包拒绝导入；平台第三方依赖来自既有解释器，未安装/修改环境。 |
| 既有平台兼容 | `test_forge_cli.py` / `test_dataset_api_contract.py` **19 项通过，32.01 秒**，包括临时 fixture 的实际本地运行。未接触生产项目/数据/服务。 |
| 包资源与实际源码 | 最终 wheel 的 **32 个 dev/启动/公共辅助文件**与当前源码逐字节一致；sdist 中 9 标准与 Skill/脚本共 11 份资源一致。支持正常解压 wheel，不宣称 zipimport。 |
| 消费者/历史保护 | 四入口普通 list 及逐一超限前置拒绝通过；四配置仅 tool_root 字节变化；427 份原历史记录/日志哈希一致；既有 Forge 检查器/运行器/Web 无本任务改动；额外 core 仅按补充授权移动私有导入。 |

构建仅写自有临时快照，使用既有 setuptools，未联网补包、未安装到共享环境。当前环境原无 `bin/demiflow` console script，未因迁移部署该脚本；产品命令已注册，可用既有解释器 `python -m demiflow.cli forge dev ...`，stdlib 使用项目 launcher 或 `python -B -m demiflow.forge.development ...`。

复现工作目录为本仓：

```bash
/bin/python3 -B -m unittest discover -s tests -p 'test_forge_development*.py' -v
DEMIFORGE_TEST_WHEEL="/tmp/demiforge-consolidation-implementation-20261104-2n0uwgm0/dist/demiflow-0.3.0-py3-none-any.whl" \
DEMIFORGE_TEST_FULL_PYTHON=/yzp/zhaozy/yangzepeng/0905/env/bin/python \
../env/bin/python -B -m unittest discover -s tests -p 'test_forge_development*.py' -v
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 ../env/bin/python -B -m pytest \
  tests/test_forge_cli.py tests/test_dataset_api_contract.py -q
```

最终制品（自有临时目录，尚未发布）：

| 制品 | 字节 | SHA256 |
| --- | ---: | --- |
| `demiflow-0.3.0-py3-none-any.whl` | 13,432,551 | `50b4fc060730862694a06048768088fb65a7e93e0d5ac886c2cce38a6fc21537` |
| `demiflow-0.3.0.tar.gz` | 12,830,731 | `442615effcf3f8f3a0425cb7dbd4fee5789ed9ebb412672025cf0b603e0729b6` |

包内 Skill 与实际 launcher：

| 文件 | SHA256 |
| --- | --- |
| `demiflow/forge/development/skill/SKILL.md` | `9778ab888c05299a03ba93d46304b5a9f6882ce1f4b3821abfde2afc02710b97` |
| `demiflow/forge/development/skill/scripts/forge.py` | `86dd3ed33d45f84fe176604b97872d9076628ac54b77772a547804e7f7f12ba3` |
| 四份相同实际项目 launcher | `ec05077687e207de1424fda708352aa40532ba4192b08b7df30ba2616b650ca6` |

最终测试日志 SHA256：

| 日志 | SHA256 |
| --- | --- |
| `demiforge-development-final-stdlib-tests.log` | `d9a31ed8598cffc59179c962c1814c1267fa0363d3d3379bc459efa04fb2a433` |
| `demiforge-development-final-environment-tests.log` | `3337f6e28e73732799930d5a0a54a6e716bdcfb55ac6a362548a6be6db4d79fa` |
| `demiforge-existing-cli-data-final-tests.log` | `e85041ddc992cf927d51effb2bd00c79112ef6de5df7a8cde7a9be7aac8f837a` |

逐项源码/测试摘要见 `implementation-source-test-hashes.json`；构建资源对照见 `build-artifact-hashes.json`。

## 标准资源原字节哈希

以下与只读旧仓、源码资源及已构建制品逐项比较；唯一 candidate.1 文件仍名为 `pipeline-v1.md`。

| 文件 | SHA256 |
| --- | --- |
| `pipeline-v1.0.0-candidate.2.md` | `1b166b4b4289be809f685d9bcb5e8cb451917918761aaa7479f616b51490b4bb` |
| `pipeline-v1.0.0-candidate.3.md` | `d072039aa106d052f680f57c181cd0a99ed0573d27128e8061d84dcfdc75f335` |
| `pipeline-v1.0.0-candidate.4.md` | `539f9a05618e3b4f6a71428a23864003a0327eba639c93c885360e85f3f74ad4` |
| `pipeline-v1.0.0-candidate.5.md` | `396c9fb4db0bb61387ff77feb26cfb20b4f4a76d681398f18cfdc84064636f0e` |
| `pipeline-v1.0.0-candidate.6.md` | `2abd2945f79084ad0806e89fa770f20d668eb180868aa18b19b520f423fa7fd5` |
| `pipeline-v1.0.0-candidate.7.md` | `400cb43cf220816401e2f1ba72e68f7b03405e7592deac8164bcdc1517a8ea4c` |
| `pipeline-v1.0.0-candidate.8.md` | `f832af006509fec2f911f9e05975e7ffb26e8944a5794b63bbde8dae5bd13b6a` |
| `pipeline-v1.0.0-candidate.9.md` | `dd8e310063075fb44a9f7dcc7f7c3182e99a5d041ed687ccfb8940e40132bb88` |
| `pipeline-v1.md` | `d557524065ed0d7ff7ebb803c4237f013fedb2d69e6e9b546b9a6d338690682f` |

## 删除前定点核验与剩余门槛

本轮读取有明确根、文件/总字节/目录项/时间预算，完整路径、摘要、排除与失败均在证据目录；以下数字不相加冒充无重叠的全工作区盘点。

| 范围 | 实际结果与边界 |
| --- | --- |
| 已注册 worktree 控制入口 | 核对时 14 个登记根中 13 个现存；只有已切的四处有 `.demiforge` 工具配置/launcher。缺失根为 prunable 记录，不修剪登记。 |
| 当前主项目定点源码 | 10 个明确业务/接线/测试/工具根读 2,079 文件、37,386,137 字节，声明范围完成，无旧依赖机器引用；运行/冻结/归档等排除列明。另一次宽泛主根扫描在 50,001 项停止，不能当作 collect 或整仓全覆盖。 |
| 当前 training | 含未跟踪、实验脚本的声明源码范围读 5,056 文件、54,256,966 字节、54,737 项，完成；Demiurge 命中只在既有验证快照 JSON。明确剪去 source/platform/review snapshots、运行数据、环境/vendor 等，未改 training。 |
| 其他 worktree 源码 | image-top1：5,381 文件；native-search：1,487 文件；冻结 checkout：2,547 文件，均完成其声明的源码范围。native-search 仍有既有日志/制品 ABI 名称兼容，未改成外部源码导入。 |
| Python 静态核验 | 上述已读范围去重 8,412 个 Python 文件全部可解析，未发现真实 `demiurge`/`demiorch` import；被排除/大文件/符号链接、动态 import 和外部环境未由此证明。训练已读 Python 源码摘要复核无变化。 |
| 旧工具进程 | 可见 /proc 命令行的有界探针未匹配旧工具；不代表远程进程、已加载模块或所有历史任务已结束。未中断任何进程。 |

仍由主会话控制：最终 Review；备份覆盖真实 dirty/untracked 资产且可恢复；未读/排除范围按实际消费者另行核对；具体历史 runtime wheel 保存位置与当前消费者仍未全局确认。历史 ABI/distribution/logger 字符串保留，不当作对本地旧源码的 import。F-01/F-02 后续另做，不能用归属迁移宣称它们已修复或当前业务已认证；需要跨工具续接时另行审查原基线保留方案。

本次不以静态零命中、旧 accepted 或现有 wheel smoke 授权删除 Demiurge，也不更新正式服务、Web 或主会话 roadmap。

证据目录：`/tmp/demiforge-consolidation-implementation-20261104-2n0uwgm0/`。主要文件：`before-manifest.json`、`history-before.json`、`consumer-switch.json`、`preservation-and-consumer-validation.json`、`launcher-byte-admission-review-fix.json`、`build-artifact-hashes.json`、`implementation-source-test-hashes.json`、`test-log-hashes.json`、各源码 audit JSON、`targeted-static-import-audit.json`、`final-old-tool-process-probe.json` 和两个具体 diff。


## 冷启动兼容补充交付（2026-11-04，主会话明确批准）

本节是归属收敛 Review 之后的冷启动兼容修复。上文制品/哈希保留当时事实，当前代码与制品以本节为准。主会话已删除 Demiurge；本修复只用 demiflow 当前源码、包资源与现有迁移证据，不依赖旧仓兜底。F-01/F-02 仍在独立隔离副本，待 Web 当前发布完成后再合入。

最小依赖方案先写于隔离证据 `minimal-dependency-plan.md`。原循环是 `lance.schema -> data.flow_contract -> eager data facade -> lance.schema`。当前将 data facade 改为正常 PEP 562 延迟导出，并删除根包的 data 预热分支；不再依赖隐含初始化顺序。`lance.schema` 与 `data.flow_contract` 的实现逐字节保持，公开对象仍来自原定义模块，42 个 data 导出、原 __all__ 顺序和 dir 可发现性保留。

主会话进一步 Review 并明确批准了 `forge/platform.py` 的受限声明解析：仅识别根/data 两个已知物理 facade、已有有限映射、最多三层和 256 字符目标名；canonical 目标必须来自实际 demiflow 包。它不调用任意模块 getter、类描述符或候选代码，不扩大公开 namespace，不改变检查/执行/提交门禁。未解析或外部目标仍非通过；原生发现的构造器/描述符不执行、预算与 shadow 拒绝回归保留。

正式合入前，七个目标逐项匹配保留的原基线，新文件确认不存在，所有其他修改保留。合入后从当时最新正式包、测试和示例做了有界快照（20,000 项/160 MiB/单文件 32 MiB/30 秒上限），实际 1,486 项、32,071,035 字节；与已验证候选的全部 992 个包文件一致。

| 正式修复文件 | SHA256 |
| --- | --- |
| `demiflow/forge/platform.py` | `ba10787e8edc346ea6a04799eaf28a69298ad8b6660b5608d7cb54b92ed8197c` |
| `demiflow/data/__init__.py` | `77b83b05d32ab25e422bd04132ecc1b83ee9b2d55f59792cefc864d2b9013ac0` |
| `demiflow/__init__.py` | `496d2cf0828fe2b73b2c5f738270e4827e04e155695ffa4c2dd239b1ed647e92` |
| `demiflow/forge/development/resources.py` | `8ad3445e049eb40ceb2e44970d0eb664eac62e3bbed510e2ac01711a7cb8a4dc` |
| `tests/test_forge_platform_discovery.py` | `3c82617a5b764a503637b4aa95d0475f9362636aeca0e4a763da67c09025eb89` |
| `tests/test_package_cold_imports.py` | `9b23fd21f4f19c84bd36d21fc130735aa74f85609c242f7bcbd3730c8e1d4b1b` |
| `tests/fixtures/data-public-exports.json` | `f0fcd9f12d23bdd87e569cca622f3debbf84f2aba6da2124f24b4bca8eec3cdc` |

两个未变实现分别是：

- `demiflow/lance/schema.py`：`dfee278c286f9cce375465101794518dc7c1db77478b0493b6cf44eacfcabd5a`。
- `demiflow/data/flow_contract.py`：`085e11f226e4f60e5b06a9bdc3e708abbb73928a62a4f372b2d604256d8771d8`。

验收结果：

- 正式源码独立冷启动矩阵 12 种入口、42 个 data 公开对象身份/原 __all__/dir、固定版本 metadata 通过（3 项测试，29.426 秒）。精简 schema/契约/dev 导入明确阻止 YAML、Arrow、Click、第三方 lance 与旧包，仍通过。
- 正式源码原生发现、既有 CLI 与 Dataset 契约共 43 项通过（26.04 秒）；原 dev 74 项全部通过，stdlib 下 70 项通过、4 项安装条件跳过。
- 不含源码兜底的 wheel 在独立新进程通过相同 12 种入口、42 个对象身份、固定版本 metadata 与 stdlib 入口，所有已加载 demiflow 模块均来自自有安装目录。
- 四个真实本地提交/对账 fixture 在冻结源码通过（208.81 秒），在解包 wheel 通过（216.74 秒）：A/B 与固定输出续接、独立多输出、丢 receipt 的固定版本对账、错误 marker 不晋升与登记恢复。临时表/运行目录均自有，不访问业务、生产、模型或服务。
- 正式重建 wheel 的每个条目与通过上述 wheel fixture 的候选制品逐字节相同；又从正式重建制品做了全新安装并重跑冷入口矩阵。最终源码/资源与 9 份冻结标准摘要逐项一致，sdist 同样保留标准全文。

| 正式重建制品 | 字节 | SHA256 |
| --- | ---: | --- |
| `demiflow-0.3.0-py3-none-any.whl` | 13,433,113 | `5c660570296922fc1e10fd1f403f8c152892ac67b715eb8ae982228cf2dfbdd3` |
| `demiflow-0.3.0.tar.gz` | 13,282,723 | `27573cb2ae3ca99ca553a3496591c3754c1b982313b4c2485a7d8ad84a83c95c` |

本轮没有 push、更新正式服务、重绑真实项目快照、修改业务/历史或标准；未修改 Web。早期失败日志保留：资源设置与 schema_hash 原协议前缀断言已修正；一次在验证中更新隔离包导致原平台快照正确 stale，随后冻结包并用全新 fixture 完整重跑通过，没有放宽门禁。

证据目录：`/tmp/forge-cold-import-fix-20261104-ooe87idz/`。正式合入清单 `formal-merge-manifest.json`、最新源码 `formal-latest-source-manifest.json`、制品 `formal-artifact-hashes.json`、最终 wheel 矩阵 `final-wheel-cold-matrix.json`、正式冷入口/回归日志 `formal-cold-tests.log` / `formal-public-regression.log`、源码/wheel 真实 fixture 日志 `real-fixture-source-final-tests.log` / `real-fixture-wheel-tests.log`，以及具体补丁和受保护实现原 SHA 均在其中。
