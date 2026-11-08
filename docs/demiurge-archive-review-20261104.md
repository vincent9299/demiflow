# Demiurge 归档、删除与删除后核验（2026-11-04）

**已完成。** 在迁移实施报告最终回归/构建通过、本会话独立复核及运行检查通过后，于 **2026-11-04 18:13:12 +08:00** 删除精确目录 `/yzp/zhaozy/yangzepeng/0905/Demiurge`。删除后 22 项入口/资源/历史查看烟测全部通过，旧目录未被重建。四个消费者共 149 份任务的 565 个控制文件及内嵌原始 baseline/readonly baseline 哈希保持一致。

报告更新：2026-11-04T18:20:33+08:00（Asia/Singapore）。机器证据使用 UTC。最新用户指令已将本会话扩展为备份→独立核验→删除→删除后验证，不再要求等待主会话另发删除指令；本次据该授权执行。实施会话原报告的“不删除/待主会话 Review”描述其职责边界，不覆盖本会话后续明确获得的删除授权。

**GitHub 备份已在删除之前实际确认。**

- 仓库：[vincent9299/Demiurge](https://github.com/vincent9299/Demiurge)。
- 备份分支：[`archive/demiurge-pre-consolidation-20261104T085745Z`](https://github.com/vincent9299/Demiurge/tree/archive%2Fdemiurge-pre-consolidation-20261104T085745Z)。
- 提交：[`2428140bdbb9e4051585b040e760087e69a0f895`](https://github.com/vincent9299/Demiurge/commit/2428140bdbb9e4051585b040e760087e69a0f895)；根 tree：`ca23aa476a10c9be5bcab59fea1ba535cb1037db`（[固定源码树](https://github.com/vincent9299/Demiurge/tree/2428140bdbb9e4051585b040e760087e69a0f895)）。
- 父提交：`057bc69275c7bbe11ce27986f10091d00cdde2b6`。备份包含原有全部 4 个历史 commit，连同备份 commit 共 5 个。

上传包含全部 226 个 tracked 与 12 个非 ignored 未跟踪文件，共 238 文件、1,797,373 字节；原 HEAD 之外有 8 个已修改、12 个新增文件（2,857 行新增、70 行删除），覆盖用户既有源码、candidate.4–9、Skill、文档、示例及测试。20 个 ignored Python 缓存只做本地保全。审阅上传清单与凭据/私有数据模式扫描均无命中；这是启发式检查，不宣称形式证明。没有把既有修改归为本次创作。

备份提交在私有 Git 副本和专用 index 中生成；源分支、工作树和 index 未被 checkout/stash/reset/clean 或改写。只向已有 origin `git@github.com:vincent9299/Demiurge.git` 新建上述分支，无 force-push、main 改写、PR 或仓库可见性变更。随后用 ls-remote 确认 ref，再从 GitHub 向空的独立 Git 存储 fetch，无 alternates，重新计算 commit/tree SHA-1，逐个比对 238 个 blob 的路径、执行位、大小和 SHA-256；全部一致，独立 `git fsck --full` 通过。该远端确认时间为 **2026-11-04 17:03:55 +08:00**。

原有全部本地 refs 为 `refs/heads/main`、`refs/remotes/origin/HEAD`、`refs/remotes/origin/main`，均指向原 HEAD；没有其他分支、tag 或 stash。三者全部在备份父历史中可达。完整 `.git` 的 ref 名称、reflog、配置、对象与原 index 还由完整 tar 保留；新增备份 ref/commit 另由 GitHub 和全 refs bundle 保全。

**远端末次重查的准确限制。** 删除前再次联网查询时，SSH 返回 `Permission denied (publickey)`，HTTPS 无可用凭据，未声称此次重新查询成功，也未改 remote、凭据或共享环境。此前 GitHub 留底已独立 fetch/tree/fsck 确认且由用户 Review；删除前本会话再次完整哈希旧仓，324 个文件及全部路径/权限与归档完全一致，238 个可提交文件还与该独立取回的 GitHub tree 逐字节一致。因此没有新增需补推内容，沿用已确认的远端备份证据；没有以本地归档冒充一次未完成的 GitHub push。

**完整私有归档和可恢复性。**

归档目录：`/yzp/zhaozy/yangzepeng/0905/_demiflow/demiurge-consolidation-20261104/archive-20261104T085745Z`，权限 `0700`。没有在工作区根创建第二套 Demiurge；私有目录中的 Git 副本和恢复校验副本是证据产物，没有接入当前入口。

| 产物 | 完整路径 | 字节 / 权限 | SHA-256 |
| --- | --- | --- | --- |
| 完整原目录 | [Demiurge-complete-20261104.tar.gz](/yzp/zhaozy/yangzepeng/0905/_demiflow/demiurge-consolidation-20261104/archive-20261104T085745Z/Demiurge-complete-20261104.tar.gz) | 1,148,156 / `0600` | `c4cb9cfba6e16b8df7570b81433da7539746bf8121158d79a9c826bdf5c61ea7` |
| 全 refs Git bundle | [Demiurge-all-refs-20261104.bundle](/yzp/zhaozy/yangzepeng/0905/_demiflow/demiurge-consolidation-20261104/archive-20261104T085745Z/Demiurge-all-refs-20261104.bundle) | 619,026 / `0600` | `52f921ea9a80e68fb9498edcbc6d7e8ef8e6eb4364a553961d6dda4675726daf` |

tar 覆盖 **324 个普通文件、2,657,755 字节、101 个子目录**：工作树 258 文件（含全部 dirty/untracked 与 20 个 ignored 缓存）、`.git` 66 文件。共 426 个 tar 成员，含根目录。未跟随 symlink、未复制外部环境；源树无 symlink/特殊文件/嵌套挂载，没有 runtime wheel、业务表、模型或外部数据位于该删除树内。原 Git 存储独立，无 commondir、alternates、shallow 或 partial-clone/promisor 依赖。

已逐成员核对路径、字节、SHA-256、权限、uid/gid；还实际解压到 `/yzp/zhaozy/yangzepeng/0905/_demiflow/demiurge-consolidation-20261104/archive-20261104T085745Z/recovery-check`，全部 324 个文件内容/权限一致，恢复后的 fsck 通过，dirty/staging 状态与初始逐字节相同。bundle 的 `git bundle verify` 通过，包含完整历史。删除前再次确认 tar/bundle 哈希与 `0600` 权限。恢复步骤见 [RECOVERY.txt](/yzp/zhaozy/yangzepeng/0905/_demiflow/demiurge-consolidation-20261104/archive-20261104T085745Z/RECOVERY.txt)；当前源路径已不存在，恢复时使用新的私有目录，不自动覆盖当前 demiflow 或业务项目。

删除前源目录仍为原 `main`，HEAD、三个 refs、remote、staged/unstaged diff、索引条目与原 `.git` 文件均保持原样；8 个修改、12 个未跟踪和 20 个 ignored 文件无遗漏。原 index SHA-256 在备份前和删除前均为 `7a1ed2f2e98c702b0223add98e804c8f77511ccdb384a4b8763cc93b1218a210`。删除后这些状态由完整归档恢复，不再宣称被删除的工作树/index 仍现场存在。未观察到旧仓并行缓存变化，不需要补充归档。

**迁移完成与独立核验。**

已读取 [迁移实施报告](/yzp/zhaozy/yangzepeng/0905/demiflow/docs/demiforge-consolidation-implementation-20261104.md)，核对最终 wheel `50b4fc060730862694a06048768088fb65a7e93e0d5ac886c2cce38a6fc21537`，将关键机器证据、报告和 wheel 保存到 `/yzp/zhaozy/yangzepeng/0905/_demiflow/demiurge-consolidation-20261104/archive-20261104T085745Z/migration-evidence-reviewed`。独立比对 32 个 dev/启动/公共辅助文件，实际源码与受测 wheel 全部一致；删除前又确认其未变化。实施报告记录最终 dev 74 项通过、stdlib 70 项通过/4 项条件跳过、既有 CLI/数据契约 19 项通过，及无旧仓兜底的 wheel 与冷启动导入回归。本会话另做下面的实际核验，不用旧通过记录替代当前核验。

| 消费者 | 当前 tool_root | 登记数量 | 本会话核验 |
| --- | --- | ---: | --- |
| `demiwtg` | `../demiflow` | 21 | launcher/help/list/history/原 task 分页通过 |
| `demiwtg-bagel-review-20261016` | `../demiflow` | 23 | 同上 |
| `.codex-worktrees/image-top1-20261022` | `../../demiflow` | 1 | 同上，原 handoff 查看通过 |
| `_demiflow/bagel_input_grounding_paired_20261014/checkout/demiwtg` | `../../../../demiflow` | 20 | launcher/help/list/history/原 task 分页通过 |

四份配置与实施任务保存的原配置逐项比较，除 tool_root 外所有业务字段一致；均解析到实际 demiflow checkout。四份 launcher 都正常导入 `demiflow.forge.development.cli`，没有旧包回退。根/当前消费者指引与已安装 Skill 已切换，`.codex-home/skills/demiforge` 解析到 `demiflow/demiflow/forge/development/skill`，当前 canonical Skill 和脚本可读。

删除前 **22 项全部通过**（2026-11-04 18:04:32 +08:00），删除后同样 **22 项全部通过**（2026-11-04 18:15:05 +08:00）。包括四个真实项目的 help/list/history/原始记录分页、公开 `python -m demiflow.cli forge dev` help/list，以及源码和自有 wheel 解压安装中的 9 份标准、Skill/脚本资源。九份标准逐字节匹配原仓归档哈希；stdlib 资源/项目探针未加载 YAML、Arrow、Click、NumPy、Torch 或 Ray，平台公开 CLI 使用既有解释器，无共享环境安装。

每个探针以新进程运行，拒绝导入 `demiurge`、访问原 Demiurge 目录或写业务项目。删除后确实没有原目录可用，也未创建软链、转发或副本作为旧路径回退。历史查看标明 `read_only: true`、`current_evidence: unverified`，保留当时结果与 stale 绑定；原 task/handoff artifact SHA 与原文件一致。不重拍 baseline、不补写回执、不自动 continuation，不把旧 accepted 当成当前通过。

**删除操作与运行条件。**

最终检查扫描 297 个可见本地进程、12,650 个 FD，并检查 cwd/exe/cmdline/environ/maps，旧路径或旧开发命令命中为 **0**。权限拒绝限于系统 apt/http 进程的部分字段；该时点观察不宣称覆盖远程进程或证明所有历史任务结项。首次检查曾只命中短时 bash 命令文本；当次保留目录，进程自然退出后以独立脚本重查通过，没有中断用户任务。

移除前确认字面绝对路径、非 symlink、目录 inode/device 未变、无挂载及嵌套挂载，使用 fd 防 symlink 攻击的 `shutil.rmtree`。仅删除精确目标及其内部文件，未删除其他路径；完整归档、bundle、私有恢复副本和新 canonical Skill 均保留。操作证据记录删除后 `lexists` 为 false；删除后所有入口烟测结束仍为 false。

本会话只新增/更新本报告与私有证据、执行授权的旧仓移除；没有改迁移/业务/平台实现、AGENTS、Skill、业务登记、Notebook、training、共享环境或现有服务，没有运行真实业务 pipeline/模型或修改生产数据。实施任务自身的已授权改动范围见其报告，不混记为本会话实现。

删除后重新哈希四个消费者共 **149 个 task.json 所在任务的 565 个直接控制文件、60,345,575 字节**，与本会话烟测前基线的路径、内容、权限与 mtime 均一致，内嵌 baseline/readonly baseline 因 task.json 全文件哈希而完整保护。没有新增、丢失或改写控制文件。该清单不重遍历嵌套 verification fixture/运行产物；烟测禁止对整个业务项目写入，且只执行 help/list/history/资源读取。

**保留的历史与边界。** 两处额外 worktree 的冻结业务 prose、历史任务/输出中的旧路径、旧 ABI/distribution/logger 名称原样保留；它们的实际可执行入口和资源已切断旧源码依赖。未把字符串零命中当全局证明，也未改写这些历史事实。具体旧 runtime wheel 的全工作区分布没有因本次被全局认证；待删树本身没有 wheel，且无源路径运行占用。F-01/F-02 本轮未修复，设计已迁到 demiflow；本次删除不宣称业务被正式认证或所有后续改进完成。

关键可核验文件：

- [删除决定及最后门槛](/yzp/zhaozy/yangzepeng/0905/_demiflow/demiurge-consolidation-20261104/archive-20261104T085745Z/evidence/deletion-decision.json)、[精确删除结果](/yzp/zhaozy/yangzepeng/0905/_demiflow/demiurge-consolidation-20261104/archive-20261104T085745Z/evidence/deletion-operation.json)、[最终运行检查](/yzp/zhaozy/yangzepeng/0905/_demiflow/demiurge-consolidation-20261104/archive-20261104T085745Z/evidence/predelete-runtime-final.json)。
- [迁移独立 Review](/yzp/zhaozy/yangzepeng/0905/_demiflow/demiurge-consolidation-20261104/archive-20261104T085745Z/evidence/deletion-migration-review.json)、[删除前 22 项烟测](/yzp/zhaozy/yangzepeng/0905/_demiflow/demiurge-consolidation-20261104/archive-20261104T085745Z/evidence/predelete-smoke.json)、[删除后 22 项烟测](/yzp/zhaozy/yangzepeng/0905/_demiflow/demiurge-consolidation-20261104/archive-20261104T085745Z/evidence/postdelete-smoke.json)、[任务/基线保护](/yzp/zhaozy/yangzepeng/0905/_demiflow/demiurge-consolidation-20261104/archive-20261104T085745Z/evidence/postdelete-history-preservation.json)。
- [原始目录清单](/yzp/zhaozy/yangzepeng/0905/_demiflow/demiurge-consolidation-20261104/archive-20261104T085745Z/evidence/source-baseline-manifest.json)、[GitHub 独立取回验证](/yzp/zhaozy/yangzepeng/0905/_demiflow/demiurge-consolidation-20261104/archive-20261104T085745Z/evidence/remote-verification.json)、[远端文件树](/yzp/zhaozy/yangzepeng/0905/_demiflow/demiurge-consolidation-20261104/archive-20261104T085745Z/evidence/remote-tree-manifest.json)、[原 refs 覆盖](/yzp/zhaozy/yangzepeng/0905/_demiflow/demiurge-consolidation-20261104/archive-20261104T085745Z/evidence/all-local-refs-coverage.json)、[完整归档校验](/yzp/zhaozy/yangzepeng/0905/_demiflow/demiurge-consolidation-20261104/archive-20261104T085745Z/evidence/archive-verification.json)、[实际恢复校验](/yzp/zhaozy/yangzepeng/0905/_demiflow/demiurge-consolidation-20261104/archive-20261104T085745Z/evidence/recovery-verification.json)、[bundle 校验](/yzp/zhaozy/yangzepeng/0905/_demiflow/demiurge-consolidation-20261104/archive-20261104T085745Z/evidence/bundle-verification.json)。
- [删除前原版报告](/yzp/zhaozy/yangzepeng/0905/_demiflow/demiurge-consolidation-20261104/archive-20261104T085745Z/evidence/archive-review-before-deletion-20261104.md)（SHA-256 `16609538aa2ef62642ad035ea952de9533c2ec8b29d4f15815e10ce50b79c771`）、[恢复说明](/yzp/zhaozy/yangzepeng/0905/_demiflow/demiurge-consolidation-20261104/archive-20261104T085745Z/RECOVERY.txt)、[产物/证据校验和](/yzp/zhaozy/yangzepeng/0905/_demiflow/demiurge-consolidation-20261104/archive-20261104T085745Z/SHA256SUMS)。原版报告的“源目录保留/等待后续删除指令”是当时状态，已由本报告和后续明确授权取代。
