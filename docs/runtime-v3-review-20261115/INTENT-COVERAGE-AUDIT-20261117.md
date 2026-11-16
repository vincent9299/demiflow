# 两大原始意图覆盖复核：最终收口还差什么

审阅根树 `gpt_runtime_v3_review_20261115`，源码快照 `79bc411c7723539bb09c4f39c291599be0043159`（初读为 `612dd0b`，后核新增 EOF 差异）；本报告独立写在性能树 `091b043` 之上。依据 I01–I14、架构总意图 v1、CURRENT-STATUS 最新段、INTEGRATION-PROGRESS 及实际 receipt。源文件/证据摘要、精确最终 selector 见同名 JSON。根树和主仓均未写入；没有全量或重性能复跑。唯一新增执行是总控指定的 fanout 旧版本单条基线，环境纠正在文末明示。

**结论：本轮未发现新的、已证实的公开 Dataset 双引擎或 Local/Ray 核心能力断线；不能据此宣称没有任何遗漏。真正挡住完整交付的是以下四组。产品的看图/配置/运行、Notebook 分析、Case 比较和评测仍在验收范围，不能用引擎通过数替代。**

## 1. 可操作的剩余项

| 优先级 | 问题与直接证据 | 最小收口，保留的原约束 |
|---|---|---|
| P1 / I05–I08 | U3 原六目标慢分支尚未证明独立推进。最新总控反馈 `79bc411` 的 gap 为约 0.0176，小于原 0.1；本审计未把这条反馈冒称自身复跑。此前多切/停车已按实际共驻修正，不继续引用旧 21 participant 数字判现状。 | 保留 60 行、六 reader、慢分支每行 5ms、gap>0.1 与原预算，定位上游吞吐和不必要串行的因果；只改原 compiler/placement/runtime。原六个 U3 selector 在修后取得固定版本证据。 |
| P1 / I08 | 快 CPU 原 20k→10k 仍约 13 秒，旧参考约 1.872 秒；宽行 owner `87cc0c3` 两原例约 99.53/98.28 秒、23 项组合及真实 Ray 恢复通过，但不是正式五轮/最终合流版本性能门。 | 同文件系统、原数据/预算/ACK 频率五轮交替矩阵；先明确主要慢处，不能把小幅候选收益或单次低于 120 秒当整体达标。原 40k×8KiB 双侧、1/2 分区、256MiB pool/2GiB RSS 不变。 |
| P1 / I05、I08、I14 | 19 条 join-chain 旧红并非 19 个结果错误，但也并非只删旧统计即可关闭。原 receipt 的 16 条先通过结果/重复数/折叠顺序再失败于旧 `datafusion` 数量；改写可见 key 的一条先通过结果再失败于旧数量。另两条 thread/process 在 `change=False` 的旧数量断言失败，**`change=True` 尚未执行到**。 | 按原生计划/分区事实迁移观察点，完整跑同 key 和改 key 两臂；不削弱值、多重性及 reducer 顺序。分区复用的性能承诺单独保留到 I08，不凭结果相等宣称优化存在，不为凑旧数量恢复旧引擎。 |
| 验收阻塞 / I01、I07、I09、I12–I14 | 最新公开入口、actor、overlap、末行 EOF、结果登记和 kernel 修改发生在早期完整 API/业务/产品证据之后；原 74 待合流项/19 join 项不是全仓 inventory。缺最终受影响面组合和 V035 完整裁决。 | 先定最终候选，再按 §3 小集合补当前连接处；未变化深层证据按原版本复用。合并精确 inventory 的 passed/failed/not_run/skipped/等价迁移及源摘要，不把各版本通过数相加成当前全绿。 |

另有一个**测试口径修正**：fanout 恢复在发布重叠时读取未 ACK 后缀可以合法重放。固定旧 `091b043` 的同一原断言仍红，证明不是新 EOF 改动引入；80 条结果和原 ACK 前缀均先通过。应读取原 source_vector，分别锁定已确认前缀不重做、未确认后缀允许重读、最终结果不重不丢。不能简单删除读取断言或承诺每个纯回调物理 exactly-once。

## 2. I01–I14 对照与已有证据边界

| 意图 | 源码/已有证据及本次判断 |
|---|---|
| I01 同一 Dataset | Dataset 的 take/count/iter/materialize/schema/write 等动作进入 make_action_request/run_dataset_action；公开 run_stream 进入同 execute_action。98 API/168 case 有绑定版本证据，当前变更按 §3 补 affected groups；不等于所有参数组合全覆盖。 |
| I02 Ray Core | Ray executor 是同一 action 适配器；生产 Python 搜索无 Ray Data 执行 import，真实 Ray API/业务/恢复证据有禁止 Ray Data 观察。私有可选依赖环境补过三项，不冒充重新跑过全 98 矩阵。 |
| I03 三交接 | 原 finite-complete/durable/memory 和原 scope/ref/route 验证保留；新 join 投影未新增提交权威，恢复丢弃进程内 capability 后完整重验。最新优化的原 finite恢复 Local/Ray 证据可按候选复用；需合流后检查其与 source/transport 组合。 |
| I04 调度/CPU-GPU | 真 CPU→GPU→IO、同机四 GPU、两宿主六 GPU、原 grant/释放均已有证据。当前 U3 是 CPU 分支调度/吞吐问题，不重称 GPU 能力仍未实现；证据也不代表 MIG 或任意显存硬隔离。 |
| I05 共享/路由 | 真实 shared-owner、不同同值实例、class factory、原绑定/恢复、Local/Ray 已有实现与固定证据；仍需关闭上述 U3 和未执行 changed-key 两臂。 |
| I06 及时/有界 | 原 source credit、finite成功封存、publication overlap 与 callback drain 已合。fanout 不能要求未确认纯后缀崩溃后从不重读；不以此放松实际输出/ACK。 |
| I07 原提交/恢复 | worker 不另开提交权威；Lance/KV/Datasink、模型 unknown、原 journal/ACK、原 state image/pack、source/code pin 有真实故障证据。最近生命周期与 source EOF 有影响，选最终共用缝补验，不必再把所有历史窗口全量重跑。 |
| I08 性能/资源 | 内存峰/同时副本、Arrow拒绝前分配、state/scratch/释放已验；**高性能未闭环**。小数据成功或 40 ACK 正确都不能替代正式性能门。 |
| I09 实际业务 | Local/真实 Ray 已跑获取→队列→原生 agent 两轮工具调用→Lance→后处理/ACK，含非 final 恢复、explicit ACK、unknown 与预算负例。旧模型旁路/私有缓存缺口已经后续修复，不复活为新问题。最近共享 runtime 修改后补当前整链连接证据。 |
| I10 唯一权威 | 公开入口仍汇入原 compiler/worker/exchange/resource/journal；工作台从封存图与原观测投影，不重编“第二张执行图”。没有从源码找到本次新增第二账本。 |
| I11 真多机 | `f5669fb` 正式双宿主原业务/读域/同长异字节拒绝/actor-loss 有原始证据；不混同同机 Ray，也不宣称整机断电/任意网络分区均验过。当前本地 CPU/有限输入改动不要求机械重占双机六 GPU。 |
| I12 产品 | 实际 sealed plan/physical/identity bridge/run-attempt observations 仍接线；管理 union/嵌套 materialize、Notebook 原 SQL/Broker/Jupyter、Case 比较/标注 CAS/历史/多角色字段都有证据。旧产品报告“图像/managed GPU 缺接线”已被后续 fdea56c/301b426 关闭；不当新缺口。当前最终最小选择必须包含真实浏览器、Notebook 和 Case，见 §3。 |
| I13 历史 | prompt enum1024/1025/1500身份、旧代码/来源拒绝、固定 Lance transaction、搬迁路径、未知效果不重发均有证据。历史只读兼容不等于旧执行可继续；最新结果登记改动应补真结果固定版本链路。 |
| I14 完整交付 | 仍不能宣称完成：四组 blocker 未清、全仓 ledger 未最终闭合。旧冻结 helper 文件存续不等于新执行仍旁路；新增公开调用或可达路径才应立缺陷。 |

没有将以下内容误判成新增核心缺口：私有 Dataset._run_stream 的旧兼容实现（公开 run_stream 已迁）；LanceRun 未导出、无当前调用/测试且已有明确 legacy 说明；checkpoint facade 在 require_action 处拒绝；analysis.runtime 的 unknown-role 拒绝并非缺 SQL。它们的死实现清理可按 I14 独立跟踪，不为其恢复已退休能力。

产品全 root 仍包括导航/context、读图配置运行/停止恢复、数据分析/Notebook、Case 选取比较评测、历史与有限读取。本次没有做每一个 UI 控件的新品全验，也没有以“引擎不负责”抹掉原产品承诺。源码变更审计显示 301b426 之后 workbench 生产仅 domain/manager 两文件变化；Notebook/Case 实现无 diff，其旧深层 CAS/UI 证据可保留，当前 pipeline→固定结果→Case 接缝仍须验证。

## 3. 最小最终验收选择（不是现在再全跑一遍）

按最终合流 diff 选；某组已在最终相同源码通过则直接复用，不重复执行。所有测试仍需实际 import 根、四个精确 DEMIFLOW 私有域与 TMPDIR、完整原三阶段、明确环境/零隐式跳过。

1. **已知红项**：原六 U3；test_local_join_chain 全 19 selector，含前述两个 change=True；fanout 恢复绑定真实 ACK 后的原 selector。不要用只收集或新小替代集关闭它们。
2. **公开 API 的变化面**：test_actual_api_behavior_group 的 sources/transforms/relations/terminals/writers/native/models，在 Local/真实 Ray 执行。其余 readers/documents/queue/acquisition/retired 组复用原 API 证据前核实现与声明未变；若受影响再加相应组。每个子组自身诚实保留 not_run，不冒充全 98 完成。
3. **实际业务/效果**：正式 test_model_effect_recovery + test_queue_full_business，Local/真实 Ray 对应原固定场景；原三模型 unknown 与已确认调用不重发、预算不重收、队列与结果/ACK 是必须的观察点。若最终变动已由同版此门覆盖，不额外再跑局部等价模型矩阵。
4. **提交/恢复共用缝**：原 finite_join_output_checkpoint_restores_exact_prefix_and_multiplicity（Local/Ray）、shared_actor_real_ray_cut_recovery、recovery_output_prefix_resumes_without_reinvoking_actor、KV after_sink_commit_before_receipt、Datasink completion_callback_failure_keeps_ack_and_resume_does_not_rewrite。精确文件/nodeid 在 JSON。原多机/硬件和其余已覆盖窗口保留其版本。
5. **产品六个连接点**：managed union+nested materialize 的每个 occurrence；浏览器真实 sealed physical facts；源码/参数/数据图加载；删除当前源码后的原 attempt 停止；真实 A/B receipts→Case 比较→固定版本续读；原 Notebook/Jupyter SQL。JSON 给出六条现成 selector。Case 标注 CAS/历史、移动端/深层 Notebook 响应丢失原证据可复用，不偷偷取消产品验收。
6. **I08 与最终账本**：遵循已定 PERFORMANCE-PLAN，最终同版五轮/同文件系统/同预算。整理 V035 exact inventory 和每项可复用边界，明确所有剩余失败/未跑；CURRENT-STATUS 与 JSON 中旧 pending/unmerged 候选条目需随新证据调和，而不是并列为新卡点。

这份集合是针对目前最后变化面的收口建议，不是另设新的一轮全量 review，也不是用约二十个函数代替历史全仓账本。

## 4. 本轮新增旧基线实验与隔离纠正

总控要求对 fanout 的旧版本执行原单条 `[True]`。第一次直接复用旧 execute_batch，仅显式三 DEMIFLOW 域，第四个 DEMIFLOW_LOCATION_CACHE 未设置；该次虽得到同红，**排除正式四域验收**。未动断言/代码，未清理任何共享记录；该 literal ItemsSource 不涉及 location manifest，不能据此泛化其他测试隔离。

随后显式四域 + TMPDIR，且在同一 pytest 进程中断言准确路径与 import 根/干净 `091b043`，仍为 **1 failed / 9.27s**，原第88行读次数断言失败、结果 multiset/原 ACK 前缀先通过、源码前后相同。

正式 `/tmp/demiflow-fanout-prior-eof-091b043-four-domains/receipt.json`，SHA256 `ba756a50a8ebfeae7804f1e191a8bdff4b844ff8216897e01e1c835a5ca31b6f`；同目录 isolation.json/run.py 保留有效环境、PID及完整入口。原三域红 `/tmp/demiflow-fanout-prior-eof-091b043/receipt.json` 原样保留且不计正式证据。
