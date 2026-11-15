# 原106项完整回归：106 passed

在隔离树 gpt_v3_regression_20261115 的 2df1570 完整执行。该提交与 root e6bdc2f 的 tree 完全相同：38296f0cd55ffc4741e85f53d0012c28deecce9b。测试期间无源码修改。

原94 passed / 6 failed 的 XML 精确对应 tests/test_stream_map.py 80项 + tests/test_local_kernel.py 20项（test_dataset_local.py 不存在），本轮追加 test_stream_grouping_lookup.py 6项。无缺失旧nodeid、无skip、无xfail，也未筛掉慢测例。特别核对：两个原100项文件与2177a2e逐字节相同，本轮没有调整数据、时间、资源或结果断言。

结果：106 passed / 559.85s；XML /tmp/demiflow-regression-original106-e6bdc2f.xml，日志同前缀.log。精确命令、进程内导入树绑定、文件摘要和全部106 nodeid保存在相邻JSON；资源与basetemp均独立/tmp/demiflow-regression-*，未修改/测试主仓、未push。

## 原六项失败逐条结果

- `tests.test_stream_map::test_default_map_remains_lazy_and_single_inline_stream_stage`：passed，7.884s。
- `tests.test_stream_map::test_slow_consumer_bounds_wide_rows_and_inflight_calls`：passed，1.818s。
- `tests.test_stream_map::test_prepend_rejects_configured_maps_but_accepts_plain_map`：passed，1.545s。
- `tests.test_stream_map::test_nested_relations_support_stream_options_without_eager_source_access[thread]`：passed，12.071s。
- `tests.test_stream_map::test_nested_relations_support_stream_options_without_eager_source_access[process]`：passed，36.519s。
- `tests.test_local_kernel::test_narrow_chain_batch_boundaries_and_limit_match_serial`：passed，147.387s。

## 意图对照与限制

同一Dataset实际worker已覆盖线程共享/生命周期/取消/异常、原catch/drop计量、有限背压、公开terminal、共享/嵌套多输入、原输入固定版本、类型/重复多重性、原128/192KiB预算、提前关闭与清理、模型/写出失败、group定时/压力及Lance lookup缓存隔离。真实恢复与Ray分别由此前原协议专项支撑；此106项不是整个仓库或全部产品的100%验收。

31行窄链用例（两次执行）为147.387s，虽然结果与上界断言通过，但不是可接受的性能证据。旧100本轮累计523.841s、旧诊断520.994s，不宣称提速。root负责的40k/120秒压力仍未通过；下一步先验证其16c92ad Local mailbox唤醒修复对31行耗时的影响，再消除有限输入在同原artifact store内重复编码/复制。
