# 五条关系计算历史用例：原生提交/恢复迁移

交付树：`_demiflow/worktrees/gpt_v3_perf_mainpath_20261116`，分支 `engine/gpt-v3-relational-history-20261117`。基线 `907bd7e`，最终测试候选 `de70b34ca6acb20db51e7edf2ab25873670159a4`。没有生产修改；只改 `tests/test_local_relational.py` 原五个函数，新增 `_native_relational_history_fixtures.py` 私有 worker 观察辅助。

正式验收：**5 passed / 37.27 秒**。原 inventory/execute_batch 记录每节点唯一 setup、call、teardown；四个私有域及 TMPDIR、进程内实际 import 根、前后全部输入摘要均验证。回执 `/tmp/demiflow-relational-history-r3/receipt.json`，SHA256 `9345d4b4c2b66e044d66047adec0765d0db1fd0a278a998eac0ee0dba48a27c5`。全部回执源码摘要另与固定 Git 提交逐字节独立比对，零不符；机器证明见 [RELATIONAL-HISTORY-MIGRATION-20261117.json](RELATIONAL-HISTORY-MIGRATION-20261117.json)。

## 能力保持与机制迁移

`checkpoint/checkpoint_async` 是退休的结果路径 facade；**`map_cached` 仍是支持的公开能力**，继续使用原 CachedMap、SQLiteKV 缓存、cache version 及错误诊断。本次没有把缓存换成普通 map，也没有改成只断言退休 API 报错。

| 原 selector | 保持的真实能力与新观察点 |
| --- | --- |
| `test_checkpoint_cached_map_failure_resume` | 原4行平方结果；第2号输入一次业务失败，错误仍写入原缓存 `*.error.json`；原run未完成、已有真实ACK；变更v2拒绝且原ACK/调用不变；v1同RunRef恢复，0号成功调用不重发；最终原materialize manifest为final且ACK；完成结果重读不重调/不追加原commit；另一个新RunRef再次执行，确认确实命中原公开缓存，而非仅完成结果快捷读取 |
| `test_checkpoint_sync_and_mixed_prefix` | 原同步map/filter后得到n=3，再经原异步map_cached得到doubled=6；真实materialize及最终ACK；固定物化结果可复读，原commit不改变 |
| `test_async_checkpoint_preserves_source_context_across_feed_chunks` | 原7行、真实两行source/batch窗口；来源open/pull/close在原SourceRunView内，同run_id/目录/attempt且无driver journal/reserve权；关闭一次，原driver RunContext与ActionObserver在返回后保持；不再patch已退休stream._FEED_CHUNK |
| `test_checkpoint_async_in_running_loop_resume_and_version` | 已运行的asyncio loop内，经原公开materialize与to_thread上下文桥执行；第一轮原业务错误、第二轮同RunRef恢复，1号缓存调用不重发；未完成原run拒绝v2且原ACK不变；完成后固定结果重读无调用；纯同步n=3也能在同notebook loop完成；未创造第二执行器 |
| `test_sort_and_join_cleanup_on_consumer_error` | 保持原50×50重复键join、chunk_bytes=1、下游reducer原RuntimeError；真实原RunArtifactStore scratch必须实际创建，fd关闭、路径/owner消失，原staging settlement/reservation均归零，无遗留未结算staging；结果无伪成功、没有最终发布；原ThreadResourceProvider给CONFIRMED，原capacity allocation为RELEASED、CPU占用归零 |

其余原测试函数的 AST 与 `907bd7e` 逐个相同；没有改全文件其它已绿方法、oracle、参数规模或断言。

## 两轮红证据与纠正

1. `2a7790c`，r1：2通过、3失败。失败来自本次测试把 `final` 误读为 prepared 顶层字段；真实字段在原 `prepared.sinks.materialize.manifest.final`。纠正观察路径，未改生产。
2. `cf557a0`，r2：2通过、3失败。业务结果与ACK已正确，但迁移测试试图再次 resume 已完成 RunContext，原保护明确拒绝。保留守卫，改为在**未完成**的原RunRef验证版本漂移/恢复，在完成后通过原 MaterializedDataset 读取固定结果。未重开completed attempt，未放宽历史保护。
3. `de70b34`，r3：同固定候选五条全部通过。

以上原红回执均保留并记录摘要。没有把两条已绿加三条别的版本冒称五条同版本通过，正式结论只来自r3。

## 集成

可按顺序取 `2a7790c`、`cf557a0`、`de70b34`，或审阅 `907bd7e..de70b34` 的两文件窄diff后集成。文档提交独立，不改变被验源码。四域环境、完整命令、runner摘要、源Git字节验证和原红证据均在配套JSON。

这关闭的是五条历史能力观察迁移；不宣称全库V035、Ray、性能或产品全意图已完成。V035清单里的这五条可登记本次绑定版本通过，仍遵守最终候选完整输入一致要求。
