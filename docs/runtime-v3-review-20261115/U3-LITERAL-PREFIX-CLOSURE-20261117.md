# U3 原慢分支与提交恢复收口

交付生产 `0c8be49` + `c378c5b`；固定验收 HEAD `c378c5b51ee30b82f41a5117f51201ea62c94ba9`，隔离分支 `engine/gpt-v3-u3-timing-20261117`。主仓未写入/测试。仅修改 `planning/dataflow_graph_adapter.py`，以及 `dataflow_place.py` 原 specialize 调用多传 hosts 的一行；新增 `tests/test_u3_graph_prefix.py`。本树另取总控已有测试修正 `67039c4`/`096187e`，不需再次派发它们。

**原 60 行、六 reader、慢分支每行 5ms、gap>0.1 的测试已在固定候选连续三次无探针通过。Local 35 项组合全过；真实 Ray Core 广播/定向原中断恢复两项三阶段全过，0 跳过，私有集群关闭、无子进程残留。** 这关闭该 U3 受影响范围，不等于 I08 全性能或整个产品已完成。

## 因果与实现

旧 `79bc411` 原图带轻量首末 callback 时间探针：共享祖先供应60行跨度约0.3883秒，快分支跨度约0.378–0.409秒；慢分支自身原5ms×60约0.30秒，被更慢的上游供给掩盖。诊断探针有开销，仅用于归因，不作为收益百分比。图的六个 branch 原本已独立，不能靠调整完成时间或减数据制造 gap。

新计划仅在原编译器里做两件事：

- 真 `ItemsSource`/`RangeSource`、单分区、普通同步 Map/concurrency1/inline、无 actor/native资源声明的线性祖先前缀，可共置到一个原生 worker。逐项保留所有逻辑 occurrence、kernel、source cursor、状态 image 和输入/输出窗口；CPU requests **求和**，内存驻留/state/output也完整相加，并检查原总预算和一个真实host是否容纳。流式Map内部仍有原有界call executor，因此不借CPU max假称完全串行。未知/文件源、async/class actor、并发或thread/native声明、多分区、隔离要求、单host资源不够均不合并。
- 平台自己的 `_identity`/`_unwrap` router 使用普通 MapOp，去掉无业务作用的每行异步call队列。仍保留六个真实reader分区、原route/credit、所有原记录与snapshot；用户ancestor/target的执行语义不变。

marker只用于新图编译。恢复直接读原sealed布局，不重编图、不新增journal、不改cut/ACK协议、不扩大额度。新中断用例显式 checkpoint_unconfirmed_rows=8，在原第8行ACK后中断，恢复沿原source cursor，24个callback各一次、两份最终分支结果精确，原ACK前缀和dag_id/plan_id保持；一般的未确认纯计算后缀仍允许重放，不承诺跨崩溃所有物理callback恰一次。Range另锁原offset.next、4/8游标和精确16条输出。

## 验收与保留的红证据

| 固定证据 | 结果 |
|---|---|
| `/tmp/demiflow-u3-prefix-final-r2/receipt.json` | 35 passed/124.96s，含原六U3、原tag边界、18新prefix边界/恢复/Range、四种真实路由 |
| `/tmp/demiflow-u3-prefix-slow-repeat-1/receipt.json` | 原slow单条再次通过，call12.546s |
| `/tmp/demiflow-u3-prefix-slow-repeat-2/receipt.json` | 原slow单条再次通过，call12.567s |
| `/tmp/demiflow-u3-prefix-ray-warm-final-r2-evidence/verification.json` | 原broadcast/target恢复，两项setup/call/teardown均通过；原三CPU私有Ray、原FD+8检查、shutdown/children[] |

精确nodeid、源摘要、回执SHA、FD事实与Ray原场景证据在同名JSON。正式命令在pytest进程核实际树与四个DEMIFLOW私有域（含LOCATION_CACHE）、TMPDIR；源码前后固定。原U3 helper仍核真实worker、原数据、唯一ACK、cursor、外来文件保护、实际GC退款和容量释放。

未改写的红证据：

- 第一版仅source+ancestor的组合是29通过/2红：原slow gap0.08195仍不达标，新增Range测试误借Items专用offset.index断言。增加平台plain router，Range改为检查其真实offset.next；未改旧slow断言或用户程序。
- 新恢复试验最初在adapter模块内放“不重编”探针，原code-pin正确识别该模块变动并拒绝；将观察点放在driver既有compile调用缝后通过，未放松code-pin。
- 第一次Ray两个业务call都通过，但首次环境初始化导致fixture FD53→65，原+8检查报错，不能称整批通过。按总控已审harness将同样三CPU私有Ray环境在fixture前建立，保留原FD阈值/真实资源清理，正式两项三阶段均通过。
- warmup首次临时目录嵌套导致AF_UNIX路径>107字节，在pytest前失败；改为直接`/tmp/demiflow-u3-*`目录。该次没有测试结果，不计验收。

七冻结件逐字匹配 `49b666f`。没有修改root/主仓、共享环境、KV join区域、记录codec、公共Record校验、提交/恢复权威或任何原预算。原正式五轮性能矩阵、快CPU与最终整体意图裁决仍由总控继续，不能用本页的局部通过替代。
