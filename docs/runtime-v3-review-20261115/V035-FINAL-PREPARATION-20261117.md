# V035 最终冻结前：逐节点证据与执行准备

固定审计版本：`dbeecf7816327ce270726d361b65e3cae694e166`。仅只读源码、Git blob 和既有回执；本次没有执行测试或 pytest collection，没有修改主仓、root 工作树或生产文件。机器清单见 [V035-FINAL-PREPARATION-20261117.json](V035-FINAL-PREPARATION-20261117.json)。root 后续 region/终端改动不属于本快照。

结论：不能把原来的 3,431 个“未观察到”全当成漏测；补齐 XML 后，其中 1,138 个有真实历史通过记录。但也不能据此宣布最终全绿：当前 V035 要求**整个最终输入摘要字典相等**，本次找到的旧回执没有一份满足固定版本全部输入相等。只补漏项会改善覆盖，却不能自动使 V035 通过。

## 1. 清单与证据边界

| 项目 | 本次只读核对结果 |
| --- | --- |
| 固定版本源码边界 | 2,952 个输入，49,895,710 字节；从 Git blob 读取，没有导入平台或运行源码 |
| 当前测试文件 | 818 个，每个都找到实际 collection/执行中出现过的 selector |
| 较早完整 collection | 8,119；较新的完整 collection 是 `34159ce` 的 8,220，均不是本版本完整清单 |
| 合并后的已知 selector | 8,714 个函数仍存在；其中 8,332 个 selector 有与当前**测试文件字节**相同的实际 collection/执行记录，覆盖 817 文件 |
| 尚不能确认参数展开的部分 | 382 个旧 selector；`test_execution_recovery_context.py` 没有当前测试文件字节的 collection 证据；最终统一 collection 必须重做 |
| 三阶段来源回执 | 384 份：来源前后相同，按固定版本原 `complete_phases` 检查；没有把失败、跳过、重试、重复 call 当通过 |
| XML | 1,191 份；只作历史执行观察，不伪造 setup/call/teardown，也不改成当前版本通过 |
| 当前版本全部输入相等回执 | 0；当前版本完整 collection 也未取得 |

8,714 不是本版本 pytest 实际完整参数化数量。AST 只核对符号存在，兼容 unittest 类和导入的测试函数，不生成参数 ID。回执已有的真实参数 ID 才进入机器清单。旧计划的 44 个显式替代映射保持原参数后缀；其它改名不能凭名称相似自动认定等价。

历史三阶段记录中的 4,756 个 selector 对应当前测试文件字节，477 个对应较早测试字节；另 1,170 个只有历史 XML 通过观察。上述集合仍跨生产版本，且本次没有逐个把 384 份历史回执的全部输入再与各自 Git 提交重算，因此不升级原来证据的可信边界。历史数据仅用于找漏项与安排顺序。

父清单中的 3,431 个“未观察到”现在拆为：1,138 个找到 XML 通过、2,292 个仍无观察、1 个只有非绿观察。新增和当前化 selector 另计，不能把两个清单的数字直接相减成当前通过率。

## 2. 真正需排入补验的节点

从当前测试字节确认的 selector 中，排除 V035 自身这个不可递归执行的 meta 节点，还有 **2,301 个未取得历史 passing selector 证据**：2,292 个未观察到执行，9 个只有非绿或跳过。精确列表位于 JSON 的 `missing_evidence_batches`，按文件边界、每批不超过 80 个 selector 分成 37 批。

| 分组 | 节点数 | 执行前条件 |
| --- | ---: | --- |
| 引擎/资源/恢复/队列基础 | 1,079 | 私有四域、PIDFD、原池；不能只凭 cpu 标签视为无副作用 |
| 模型/搜索/服务 | 377 | 逐文件确认本地 HTTP/transport 替身、服务退出和原预算；不凭名称调用真实外部模型 |
| 产品/Forge 后端与工具 | 505 | 原项目/会话目录、子进程、Git fixture 归属检查 |
| 浏览器/内核/产品交互 | 228 | Chromium/Playwright、Notebook 环境与私有证据目录；先处理下述共享写点 |
| 真实 Ray | 17 | 私有集群先建立再记录 fixture FD 基线，保持原 FD 阈值，最终 shutdown/子进程确认 |
| GPU 标签需区分真硬件与 mock | 13 | 不能把服务配置里的 GPU 字段一律当实际 CUDA 需求；按源码区分 |
| PostgreSQL | 74 | task-private DSN/namespace；缺少环境保持未验，不把 skip 算通过 |
| 离线 wheel | 1 | 从最终候选构建并绑定 `DEMIFORGE_TEST_WHEEL`，不能用旧包凑通过 |
| 正式组合门 | 7 | 按各门实际依赖：双宿主、性能、终端、源快照、缓存、排序聚合、关系语义 |

这是排程提示，不是对 248 个文件已经完成 fixture 安全审查的宣称。只有 8 个原始缺口节点有既有文件审查条目；其余必须读本文件和导入 fixture 后才能由既有 gate 标为 reviewed。机器批次全部保留 `not_dispatch_ready`，没有擅自跳过环境审查。

优先的 9 个确切节点：

- `tests/test_local_relational.py::test_checkpoint_cached_map_failure_resume`
- `tests/test_local_relational.py::test_checkpoint_sync_and_mixed_prefix`
- `tests/test_local_relational.py::test_async_checkpoint_preserves_source_context_across_feed_chunks`
- `tests/test_local_relational.py::test_checkpoint_async_in_running_loop_resume_and_version`
- `tests/test_local_relational.py::test_sort_and_join_cleanup_on_consumer_error`
- `tests/test_document_embeddings.py::test_native_document_payload_and_replay`
- `tests/test_runtime_v3_node_loss.py::test_actual_unknown_paid_actor_loss_never_reissues[embedding]`
- `tests/test_runtime_v3_producer_execution.py::test_same_run_producer_recovers_zero_and_many_outputs[postgresql-dataset_cut]`
- `tests/test_runtime_v3_producer_execution.py::test_same_run_producer_recovers_zero_and_many_outputs[postgresql-native_unit]`

前五条中四条仍写退休 `checkpoint/map_cached`，cleanup 仍观察旧 `local_relational.tempfile`。root 已指定下一隔离任务保留原 selector 和业务能力，将它们迁到真实 Dataset v3/原 RunContext/ACK；不能改成只验证“退休接口报错”。Document 及 unknown-paid embedding 由 root 最终组合复验；正式 V027 的真实场景可作后者历史语义证据，但不伪称这个独立 selector 已通过。两条 PostgreSQL 仍是环境缺口。

初扫看起来有 18 个非绿节点。源码复核后，另外 9 个来自 F5/Arrow/legacy-stream/L017/A005 的旧参数展开；当前新 ID 已有记录，不列成新增缺陷。机器清单仍保留这些旧 ID 供最终 collection/等价映射复核，没有删除原红证据。

## 3. 必须先处理的 fixture 写入风险

三处是**硬编码共享目录写入**，四私有域环境变量拦不住，当前批次不可直接执行：

| 文件 | 证据 | 最小处理 |
| --- | --- | --- |
| `tests/test_forge_workbench_project_checks_browser.py:49` | 向共享 P1 写 `project-check-status-*.json` | 只迁证据输出到 tmp_path，保留延迟状态/最新检查产品断言 |
| `tests/test_forge_runtime_core_flow.py:103` | 向共享 P1 写 `registration-failure-R2.json` | 同上，保留原注册失败界面与无伪成功断言 |
| `tests/test_forge_workbench_adopt_browser.py:74` | 向共享 P1 写 `CFG2-deterministic-final.json` | 同上，保留原单次写、配置字节、刷新后草稿状态断言 |

另有两类需明确准备：`test_forge_workbench_source_observation.py:19` 在 import 时读取 `FORGE_WORKBENCH_EVIDENCE`，默认仍是共享 P1/R9，必须**导入前**指定 `/tmp/demiflow-*`。`test_stage2_unified_plan.py:77` 指向 demiwtg 的真实 `concepts.lance`，不可被当成普通临时输入；先审需要的源切片和只读边界，使用受控案例，不写生产项目。

Chromium 的多个旧 fixture 有固定安装路径，其它依赖 `FORGE_CHROMIUM`。测试证据目录包括 `FORGE_ACCEPTANCE_DIR`、`FORGE_OUTPUT_EVIDENCE`、`FORGE_WORKBENCH_EVIDENCE`，须显式私有。四域和 TMPDIR 必须在 pytest 进程内校验，并确认 demiflow 实际 import 根；不能用 editable 安装悄悄测主仓。

## 4. 最终执行顺序及避免无效重复

1. **先完成生产收口及上述 fixture/五条语义迁移，再冻结候选。** 在生产持续变化期间跑外围全量，得到的整字典摘要会立即失效。
2. **只在冻结候选重新完整 collection。** 用原 inventory 脚本覆盖默认环境及真实 Ray 等必需补充环境；逐文件对齐 818 文件和原 577 文件基线，解析导入跳过、重复节点、382 个旧参数 ID。collection 不计通过。
3. **先跑上述九项与实际受影响的 Dataset/模型/队列/提交恢复/Local-Ray/工作台组合。** 若出现生产失败，修复后重定冻结版本，避免先耗费外围批次。
4. **在同一冻结输入字典上执行已审批次。** JSON 37 批只是目前真实漏证据的优先队列。原 V035 是全仓门，要真正绿色还必须覆盖全部最终 collected leaf；历史 6,000 余节点不能被转记为当前通过。已经在同一冻结字典取得的回执可直接归并，不重复执行。
5. **正式 I08/真实硬件门保持原方法。** 性能独占窗口，原数据规模、预算、40 ACK、同文件系统五轮；不能用本次历史对账或单次宽行通过代替。无 PG/离线包/指定环境时明确环境未验，完整交付结论保留这一边界。
6. **最后执行原 evaluate 与 V035 外层裁决。** 每个 leaf 恰有一次完整 setup/call/teardown，重复/重试/xfail/隐式 deselect/来源不符均不接受；V035 自身仅作 meta，不能递归运行自身。失败开发回执保留，但不得混入最终候选集合覆盖后来的结果。

不改 gate、不改旧回执、不生成假参数节点、不用测试文件没变化推导生产依赖没变化。节约时间的正确方式是冻结后一次执行、充分复用同输入回执；本次历史归并帮助把优先级放在真实缺口上。

## 5. 复核与可追溯附件

JSON 记录每个文件当前 sha256、全部已知节点按证据等级分类、37 个精确补验批、128 个已不在此版本或符号已变的历史/候选节点、原44显式映射、环境风险，以及原始分析文件和脚本摘要。

完整分析：`/tmp/demiflow-v035-final-preparation-20261117/audit.json`，包含所有 384 回执、1,191 XML 的路径/摘要及逐节点引用。只读脚本：`/tmp/demiflow-v035-readonly-audit.py`。读取总量约 340 MiB，单文件32 MiB上限；两个超大旧 collection 回执未整份展开，其压缩 inventory 已显式引用。没有把读取上限内未发现的证据当作绝对“从来没跑过”。

本报告不缩小两大 root：模型、队列、提交恢复和 Local/Ray 属于平台；浏览器、Notebook、分析比较、Case、配置运行、历史来源属于产品。引擎专项通过不能替代产品全域。
