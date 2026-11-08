# Forge 检查依据修复（2026-11-04）

本执行包落实跨层审查 F-03/F-04/F-05，以及主会话对 L-02 的覆盖范围决定。平台实现只修改 `demiflow/forge/source.py`；未扩展 CLI、检查协议、项目声明或执行权限。最终架构与集成验收由主会话 Review。

## 工作树基线与实际改动

修改前已读取工作区根和本仓库 `AGENTS.md`、`docs/forge-cross-layer-review-20261104.md`，并保存目标文件当前副本及 SHA256。Forge 源码与指南原本为未跟踪文件，因此下面的差异依据当前文件副本，不能用 Git HEAD 代替。

证据目录：`/tmp/forge-check-evidence-fix-20261104-7hs060a2/`。

- `baseline.json`、`before/`：目标文件及相关只读源码、既有测试的修改前副本和哈希；新增测试及本文在基线中明确为不存在。
- `actual.diff`、`after.json`：相对上述副本的实际差异及最终文件哈希。
- `isolation-manifest.json`、`fixed-inputs.json`：隔离验证的原始输入清单与本执行包更新的输入。
- `baseline-tests.log`、`fixed-tests.log`、`native-tests.log`：修复前复现、首次针对性验证和原生入口补验日志。

| 文件 | 修改前 SHA256 | 修改后 SHA256 |
| --- | --- | --- |
| `demiflow/forge/source.py` | `91c1efb180cc4794b59eaf532066b30193c7a1e06c149308724f86ff117eb352` | `97997a1d3b248624f0f7d5fb889d803626be53c8d77b8732397e08c4797983be` |
| `docs/forge-workspace.md` | `dde637eebfe393969a4233a7a1fbbd5085346e04c6ef7d5aa3ec5e87d1b9ba79` | `f5ef75dfa1052ba7f14001beaad785320d836fbd263e4992ccd9b1e9d6293438` |
| `docs/forge-project-discovery.md` | `005d7d407fed1de1c88b17513be8ebabd419753a56a697bb5658645e319bb368` | `6a5e27a3415293f999e5845e9e59e521e27d585e1ff9594e4507e7574557e2ec` |
| `tests/test_forge_check_evidence.py` | 不存在 | `9c7b45ff412a3ce857b590870951e73e29ecbabc12b614ada38a59790b3d239c` |

另新增本文。`project_check.py`、`flow_check.py`、`operator_check.py`、`workspace.py`、`workspace_worker.py`、`workspace_cli.py`、`project_discovery.py` 和基线中既有测试均未修改，最终逐文件复核哈希。

## F-03：读取依据独立于解析与检查成功

原来 `SourceTree.load()` 在读取、语法解析和 AST 节点预算检查全部完成后才写入 `files`。失败依赖虽然影响诊断，却不进入报告内容依据；只修复该文件，旧报告仍可能返回 `incomplete`、`changes: []`。

现在在受限读取成功后立即将实际相对路径和 SHA256 写入同一 `files` 映射，然后继续执行原有总字节、AST 及源码检查。`files` 表示读入的内容依据，不表示这些文件已通过检查。语法失败仍为 `FL-01 / incomplete`；节点或总字节超限仍为 `FL-10 / incomplete`。语法诊断保留行号，语法与预算诊断使用实际文件路径，包括 `helpers/__init__.py`。

源码证据：`source.py:79` 的诊断路径参数、`source.py:127` 的受限读取及 `source.py:132` 的摘要登记先于 `source.py:137` 的解析和 `source.py:140` 的节点检查。

现有消费者无需修改：

- `flow_check.py:180` 将 `tree.files` 写入 Flow binding；`workspace.py:1164` 发布前复核同一批内容，`workspace.py:1369` 在状态查询中复核摘要。
- `project_check.py:63` 使用现有 `SourceTree` 做原项目源码前置检查，并在第 71 行合并其文件摘要；第 163 行起复核保存的源码依据。

因此依赖修复后，公共 `workspace.project_check_status()` 会返回 `stale` 和 `source_changed:<实际文件>`；已绑定原生入口的旧 Flow 报告也会 stale。查询不改写历史诊断或项目检查记录。重新检查原项目源码前置条件成功，整体仍为 `incomplete`，没有补造 Flow、行为或 Review 证据。

资源与边界保持原值：默认最多 1024 个源码文件、单文件 1 MiB、总读取预算 16 MiB、单文件 50000 个 AST 节点、合计 200000 个 AST 节点；原项目前置检查仍收紧为 128 文件、4 MiB、10 秒。新增持有的是已有读取内容的摘要，数量由同一文件预算控制；没有全目录扫描、全局缓存或第二套检查器。总字节超限仍在解析前拒绝，该文件原本就已受单文件上限约束地读入；本次仅在拒绝前保存其内容摘要，不宣称改成了分配前总量硬限制。

缺失文件、符号链接和单文件超限仍沿现有受限读取机制返回未完成，不写入虚假的内容摘要。没有增加失败目标登记协议；未读到的内容仍属于未覆盖事实，修复后需要显式重新检查，旧状态不保证检测到这种修复。

## F-04/F-05：修正当前工作区指南

`docs/forge-workspace.md` 的正式接入示例移除了 `workspace check` 和 `check-status` 的 `--json`。两条命令原本就固定输出 JSON，CLI 实现与默认输出均未改变。真实 `CliRunner` 调用经现有 worker 获得项目检查 JSON；自有 fixture 缺少完整认证证据，退出码均为 1，符合命令语义，不再是选项解析错误的退出码 2。

算子实际定义改为按项目内本地源码、声明、归属和依赖解释，不要求目录名为 `operators`。模型配置改为按原生节点明确声明、项目归属和安全相对路径绑定，不要求 `flows`。源码依据是 `operator_check.py:239` 的实际本地定义检查，以及 `workspace.py:1258` 的资源安全路径与受限内容读取。明确保留旧 v1 及规定固定目录的历史版本规则，未改旧标准或 skill。

## L-02：按实际闭包说明内容覆盖

`docs/forge-project-discovery.md` 现在区分入口/参数与实际读取的依赖闭包，以及仅被声明但未进入闭包的算子。后者保持已有 `operator_checks: declared_path_existence_only`：仅报告角色路径存在，不能声称已审查全部内容，也不能保证内容变化使报告 stale。

没有增加全部声明算子的递归审查，也没有缩小原有实际检查闭包。回归以已声明 `unused.py` 的内容变更验证该边界；入口实际读取的 `processing.py` 仍进入报告并参与失效检查。未读目标、首个错误之后的依赖均不被当前内容摘要认证。

## 验证与结果

统一使用已有 `/yzp/zhaozy/yangzepeng/0905/env/bin/python -B`。为避开并行 Web 改版导致的平台快照竞态，验证运行在证据目录的 `isolated/`。隔离副本包含 1204 份文件、31,780,953 字节，复制限制为最多 4096 项/文件、单文件 8 MiB、总量 64 MiB；不复制缓存和字节码，复制前后逐文件核对状态和哈希。平台包（包括当时 Web 静态文件）只做内容复制，验证期间不编辑 Web。

新回归共 14 个选定用例：

- 4 个公共 API/worker 语法失败场景：原项目/原生绑定 × 普通模块/包初始化；核对内容摘要、实际诊断路径、修复前未完成、修复后 stale、旧记录不被状态读取改写。原生分支还查询真实 Flow 报告。
- 1 个原项目公共 API/worker AST 超限场景：约 100 KiB、50000 个列表元素，超过原有单文件节点预算；失败文件仍进入依据，修复后 stale。
- 3 个收紧预算场景：单文件 AST、累计 AST、总字节；保存读取依据后仍报未完成，失败模块未进入成功模块映射。
- 3 个读取拒绝场景：缺失、符号链接、单文件字节超限；没有内容依据，不通过。
- 1 个文件数预算场景：第二份依赖在读取前被拒绝。
- 1 个 L-02 未读算子覆盖边界场景。
- 1 个两条真实 CLI 命令的输出/退出码场景。

另选已有 14 个相关用例：`test_closure_budgets` 的 6 个预算、`test_package_initialization_and_real_definition_ownership`、`test_all_initialization_is_checked_without_execution` 的 5 个初始化场景、`test_complete_example_and_variants`、`test_partial_source_check_is_bound_to_originals_and_declarations`。它们验证原有合法闭包、定义归属、禁止执行的初始化、预算拒绝与项目证据失效继续成立。

验证记录：

| 阶段 | 结果 | 证据 |
| --- | --- | --- |
| 修复前源码运行语法与 AST 两个公共复现 | 2 个预期失败，均因失败依赖缺少摘要 | `baseline-tests.log`，21.12 秒 |
| 首次针对性运行 | 26 passed；2 个原生 fixture 因参数文件缺少现有格式声明提前停止 | `fixed-tests.log`，159.59 秒 |
| fixture 按现有 v2 参数格式补全后，仅补验上述原生分支 | 2 passed，12 deselected | `native-tests.log`，74.73 秒 |

共 28 个不同选定用例最终分批通过。首次验证中的两个 fixture 问题没有引发平台实现或协议变更；保留原始失败日志。修复 fixture 后只重跑受该分支影响的用例，未重复整个测试库。

首次针对性命令（工作目录为 `isolated/`）：

```bash
/yzp/zhaozy/yangzepeng/0905/env/bin/python -B -m pytest -q -p no:cacheprovider \
  --basetemp=/tmp/forge-check-evidence-fix-20261104-7hs060a2/fixed-fixtures \
  tests/test_forge_check_evidence.py \
  tests/test_forge_flow_checks.py::test_closure_budgets \
  tests/test_forge_flow_checks.py::test_package_initialization_and_real_definition_ownership \
  tests/test_forge_flow_checks.py::test_all_initialization_is_checked_without_execution \
  tests/test_forge_flow_checks.py::test_complete_example_and_variants \
  tests/test_forge_project_discovery.py::test_partial_source_check_is_bound_to_originals_and_declarations
```

原生分支补验命令：

```bash
/yzp/zhaozy/yangzepeng/0905/env/bin/python -B -m pytest -q -p no:cacheprovider \
  --basetemp=/tmp/forge-check-evidence-fix-20261104-7hs060a2/native-fixtures \
  tests/test_forge_check_evidence.py -k 'syntax_failure and native'
```

## 操作范围与未验证项

原跨层审查目录 `/tmp/forge-cross-layer-review-20261104-fpm6r4m3/` 只读。没有改动业务项目、数据、Notebook 输出、`training/`、Demiurge、共享环境、README、roadmap、Web 源码、静态资产服务或实际登记清单；没有启动模型、执行业务 pipeline、重拍或重绑业务快照、更新基线来取得通过。测试中的工作区与公开声明都在自有临时目录；已有针对性项目识别测试只创建自有两行 Lance fixture，不执行 Flow。

本次未重启正式服务，端口、入口和令牌未变。没有进行浏览器/Web 集成、正式服务热更新、生产/模型行为、全部执行故障或全库验收；由主会话统一安排后续集成。历史检查报告不会回填新内容摘要，应在实际采用当前实现后显式重新检查；本文不授权重绑实际业务快照。
