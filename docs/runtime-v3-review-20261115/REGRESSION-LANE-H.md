# H 宿主与单一权威回归

H79 项在真实已集成 DTO/authority 上 **79 passed，0 failed，16.21 秒**。日志 `/tmp/demiflow-regression-h2.log`，JUnit `/tmp/demiflow-regression-h2.xml`。进程内导入断言绑定本隔离树；`DEMIFLOW_RESOURCE_ROOT=/tmp/demiflow-regression-capacity-h2`、`PYTHONPATH=$PWD:$PWD/tests`，解释器 `/yzp/zhaozy/yangzepeng/0905/env/bin/python -B`，pytest `--basetemp=/tmp/demiflow-regression-h2`。

本批仅改 H 测试夹具/错误旧断言，未改变任何生产代码。承重七件未动。单元的 Ray carrier 是显式 fake，不宣称真实集群验证。

核心改动：去掉对第二份 `_effects` 账本/伪 Ray locator 的期待，验证原 RunJournal 和单一 allocator；合法 SHA/预算名/已注册 lease 与 scope grant，使反例抵达真正被测的身份和预算分支；Ray prepare 先于 activate，cancel 后也不自行释放资源；actor不可达是 UNKNOWN_EFFECT 而不是空 poll 成功；保留未知、过期、跨action、资源上界与无worker数据库的断言。64字节真实预算写72字节的反例已替换旧“声明max_bytes较大即拒绝”断言，检验真实逐块staging准入。

原19路147失败的每个nodeid、原错误、迁移依据/修法和当前状态记录在 `REGRESSION-NODEID-MAP.json`：其中112个本路已复验通过；B16/E19由其它路继续处理，未把它们当通过。F/G/P/Q/S/U/W/Y 216项组合与本H79项总计295项通过；完整19路集成由主控在各路汇合后执行。
