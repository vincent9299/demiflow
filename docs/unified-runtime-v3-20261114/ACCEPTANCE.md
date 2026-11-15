# 验收：证明真实默认入口、模块组合与用户意图

所有运行门目前 **not_run**。设计结构检查与工具自身测试只证明任务包没有对应类别的漏项，不能填入下面任何实现通过栏。本基线测试文件清单见 `evidence/regression-files.json`，当前 577 个；最终以集成版本完整 collect 为准，新增测试必须追加，不得冻结旧数量后忽略新增文件。

## 必交证据

每个实现会话交：工作树、branch、HEAD、全部commit、文件diff、卡ID状态、完整命令、退出码、逐nodeid三阶段结果、明确not_run、进程内所有已导入demiflow模块的绝对路径、测试前后源码/配置/测试摘要。dirty交付必须附逐文件摘要与patch，不能只贴HEAD。最终集成树重新收集，GPT与Qwen独立Review，GPT统一修复；各lane单测不能代替组合验收。

命令必须形如（环境解释器使用已具备依赖的独立env，不修改共享env）：

```bash
cd /absolute/isolated/worktree
PYTHONPATH="$PWD" /absolute/env/bin/python -m pytest tests/test_runtime_v3_acceptance.py::test_no_ray_data
```

fixture在进程内断言demiflow及已加载子模块均来自该工作树。裸pytest和临时脚本不带树绑定的结果无效。所有输出只写 `/tmp/demiflow-*`。不得用提高预算、缩小数据、将并发改1、增加manual materialize、删除guard、重试覆盖首次失败使门转绿。

`gates.json`为机器清单；每张叶卡都有required nodeid，30个集成函数各有独立门与环境要求。`tools/evidence_guard.py:evaluate`要求完整collection和setup/call/teardown、所有required被执行、无skip/xfail/deselect、无重复/重试报告、源码前后摘要一致、路径绑定正确。测试执行负责人负责按此输入schema导出报告；没有报告不得凭控制台passed行填写通过。缺环境记not_run，不能以环境跳过标全绿。

## 30个组合场景

精确实现行为和反例见 `tools/scenarios.py` 与对应V卡；表格由该文件生成，不能手写另一套口径。

| 卡 | 场景 | 必须证明 | 环境 |
|---|---|---|---|
| [V006](tasks/V006.md) | `test_no_ray_data` | Local与真实Ray公共from/read→map/filter/batch→sort/repartition/shuffle→全部terminal，import hook阻止ray.data且记录execute_action/plan_id | local, ray |
| [V007](tasks/V007.md) | `test_api_matrix` | 枚举plan.api全部公开入口；有效入口有正向语义例和错误参数例，4退役入口拒绝 | local, ray |
| [V008](tasks/V008.md) | `test_shared_diamond` | 一个有计数效果的共享上游分两支后union/join；同一函数两次独立调用作对照 | local, ray |
| [V009](tasks/V009.md) | `test_incremental_first_row` | 可seek慢来源首行后阻塞后续；下游第一条应在来源耗尽前到达 | local, ray |
| [V010](tasks/V010.md) | `test_three_exchange_modes` | 同一图分别memory/durable/sealed，验证发布/读取/成功seal/空scope/失败scope | local, ray |
| [V011](tasks/V011.md) | `test_cpu_gpu_isolation` | CPU准备→GPU模型→IO分别profile与进程，模型驻留复用和有界预取 | local, ray, gpu |
| [V012](tasks/V012.md) | `test_fairness_deadlock` | 慢分支、热点key、小slot、小磁盘和部分资源分配，消费者保留容量 | local, ray |
| [V013](tasks/V013.md) | `test_relational_semantics` | inner/left/semi/anti null/重复/倾斜；非结合reduce；两侧异步前缀无需materialize | local, ray |
| [V014](tasks/V014.md) | `test_sort_shuffle_aggregate` | 多key/null排序、固定seed全局shuffle、hash/rebalance、count/mean/std/custom aggregate | local, ray |
| [V015](tasks/V015.md) | `test_source_snapshots` | Lance版本、文件替换、SQLsnapshot、one-shotspool、from_iter异常与关闭 | local, ray |
| [V016](tasks/V016.md) | `test_multiworker_barrier` | 3worker/2输入/空分支/EOI/未齐batch/flatmap部分输出，中断恢复 | local, ray |
| [V017](tasks/V017.md) | `test_route_recovery` | hash/rebalance/target广播在中间cut后崩溃，恢复相同后缀路由/顺序 | local, ray |
| [V018](tasks/V018.md) | `test_commit_crash_matrix` | intent前后、prepare后record前、prepared后publish前、publish后receipt前、receipt后ACK前逐点杀进程 | local, ray |
| [V019](tasks/V019.md) | `test_model_effect_recovery` | 真实PromptActor/Embedding/Agent实现+本地transport，在发送前/已发/确认后断线 | local, ray |
| [V020](tasks/V020.md) | `test_actor_state_lifecycle` | 无钩子累加类、worker独立状态、共享状态并发、partial、锁、astartTask、close失败 | local, ray |
| [V021](tasks/V021.md) | `test_cache_materialize` | 原7个cache/asyncmaterialize失败全部真实复验，None/list/bytes/wide rows/取消/生命周期 | local, ray |
| [V022](tasks/V022.md) | `test_queue_full_business` | 真实获取/校验/搜索→enqueue→claim→RawFetch→纯转换→QueueResultSink原子ACK→历史receipt读取→模型→Lance→后处理；显式ack_queue合法尾部另验 | local, ray |
| [V023](tasks/V023.md) | `test_memory_peak` | 单行超限、宽行、Arrow等宽cast两副本、fanout、慢sink、状态+输出同时驻留 | local, ray |
| [V024](tasks/V024.md) | `test_disk_gc` | durable/sealed通道容量耗尽、多crash、多个reader、effect/sink pin、取消reader | local, ray |
| [V025](tasks/V025.md) | `test_remote_journal` | worker无driver数据库目录、禁止sqlite连接，实际跨进程effect/artifact/control请求 | local, ray |
| [V026](tasks/V026.md) | `test_two_hosts` | 两个不同node_id/host_id真实Ray worker、不同cache根、共享store实读challenge | two_hosts |
| [V027](tasks/V027.md) | `test_node_loss` | 杀producer节点/actor拥有者，未确认ObjectRef失效但已提交cut durable refs可恢复 | two_hosts |
| [V028](tasks/V028.md) | `test_driver_restart` | driver进程杀死后新owner获取fence，原worker迟到消息/网络分区/续约 | local, ray, two_hosts |
| [V029](tasks/V029.md) | `test_history_identity` | v1/v2完成结果只读、兼容恢复、不兼容拒绝，enum1024/1025/1500历史原公式 | local |
| [V030](tasks/V030.md) | `test_workbench_real_plan` | 真实prepare/start/show/cancel/resume，union与嵌套物化桥，CPU/GPU/exchange/恢复限制展示 | browser |
| [V031](tasks/V031.md) | `test_product_regression` | 现有Notebook/Python/SQL、Case配置/标注/历史、开发对话、source查看回归 | browser |
| [V032](tasks/V032.md) | `test_bounded_terminal` | take0/take1/take_all(limit)/iter.close/summary以及无穷队列来源的有限消费 | local, ray |
| [V033](tasks/V033.md) | `test_artifact_security` | 错误code pin、跨域、包路径穿越、symlink、checksum、过期grant、大小谎报 | local, ray |
| [V034](tasks/V034.md) | `test_performance` | 固定预算/输入/revision，快CPU、慢模型、CPU→GPU、100x100展开、不均衡模型、热点key、大图、sealed、小数据9类 | local, ray, gpu, two_hosts |
| [V035](tasks/V035.md) | `test_all_regressions` | 收集全仓test文件/nodeids，按环境分组执行，原44失败逐条裁定和替换断言映射 | all |

## 历史回归必须完整分配

`HISTORICAL_ALL`覆盖所有文件及其全部收集nodeids。不是要求缺GPU/PostgreSQL/浏览器的机器假跑；执行时每nodeid归入可满足依赖的环境，记其真实结果。长期无环境的必交项仍是未验，不能算100%。577是本次文件数，历史报告的598不能直接挪作当前事实。

- `historical-dispositions.json`逐条列出原44个失败。前37个是历史契约预期，需要分别保留当前行为断言、说明变化和映射替代新断言；四个退役checkpoint入口仍应拒绝，不能为旧用例复活退役API。
- 后7个是已知真实cache/async materialize缺陷，必须保留原用例并修到通过。测试参数、宽行、取消和并发不得弱化。
- 原生delivery barrier、stream_map生命周期、身份enum>1024、Arrow双副本、负零容器、worker code-pin、队列finalize与所有cut参与者均为必需反例，不得仅选之前的936个nodeid。
- 实施引入的失败与基线失败分开证明；基线对照只在隔离树。每条报错都属于一个owner，不能标“历史问题”后无人处理。

## 从用户操作到真实执行的证据链

一次真实工作台prepare→start→选择run/attempt→查看图/源码/参数→节点观测→输出数据→cancel/resume，记录report_digest、action_id、dag_id、plan_id、occurrence、stage/partition、allocation/boot、channel/sequence、cut/commit、result version。身份桥每个report节点必须在当前展示图；切到历史显示当时版本；没有逐节点观测的融合组明确标注。浏览器截图/交互记录与journal事实同快照，不能用静态HTML图代替。

至少一个混合流水线：CPU清洗→异步模型→join/union→排序/聚合→持久写出，两后端同一业务代码；中间不人为materialize补能力。另验获取/队列原生结算→历史receipt→模型/数据写出的完整业务边界。

## 多机、GPU与性能

两个不同Ray node_id/host_id、不同cache根、同一store真实challenge；worker无法读driver journal路径。单机两个actor只能通过ray门，不能通过two_hosts门。真实GPU profile、设备与显存驻留必须有设备证据，mock只验证调度分支。PostgreSQL只用明确授权的隔离测试库，不接生产。

性能9类：快CPU、小数据、慢API、CPU→GPU、100×100展开、不均匀模型、热点key、大图、finite-sealed。每类固定输入摘要、预算、版本、环境；warmup单列，至少5次测量，报告每次数据与p50/p95、首条、吞吐、峰值RSS/内存池/磁盘/FD、复制、取消延迟。基线必须在独立树；旧不支持能力不能伪造比较值。

硬门：输出语义一致、主动资源上界及清理成立、慢源首条早于全源EOI、CPU/GPU不同受管组、没有无限队列。性能回退超过20%时提交瓶颈证据及修复，不用扩大预算掩盖；特殊场景的代价必须由最终Review明确接受，不能由agent自行豁免。数字是工程验收阈值，不是用户原话中的性能承诺。

最终状态分开：design_ready、implemented、local_verified、ray_verified、two_hosts_verified、gpu_verified、product_verified、history_verified。任何必交环境缺证据，整体保持未全部验收。
