# 默认窗口与恢复状态预算修复

基线 main e3b645b。修复的是默认有限内存下的四条功能回归，不扩大任何用户预算，不重算历史封存计划。

## 原因与行为

- fit_default_stream_windows 遇到任意 finite_complete 输入就返回，遗漏该有限阶段下游仍需同时驻留的组件。reduce 及流式后缀因此只能被拒绝或全部落盘；prepend 的前缀不能先到下游，造成公开流式合同失败。
- 行/批默认窗口按驻留缩小时，纯无可变状态节点的 max_state_bytes 也按同比缩小。该字段同时约束整个 cut 的所有恢复镜像，四个停车阶段的约10 KiB镜像超过缩小后的9830字节，运行到第三个cut才报错。
- 现在复用同一incremental组件划分，按每组真实驻留与原PublicationReservation调整隐式窗口。有限输入仍隔开天然阶段；单worker合置仍核算所有成员。没有可变状态需求时不把整个cut额度当作payload slack缩小。
- 显式row/batch/state、总内存、复制与codec最小值、控制和原生发布预留均保留；不可行仍由原solver拒绝。resume绕过默认窗口拟合，原物理计划和身份不改。

## 验收

原四项：test_public_worker_budget_uses_actual_phases_and_native_allocation[1]、test_prepend_streams_without_waiting_for_upstream，以及test_nested_relations_support_stream_options_without_eager_source_access的thread/process变体，基线4 failed；修后11项组合全过。

扩大原有流式、预算、恢复及发布边界：117项中116通过；另一37项中35通过、1跳过。两条失败均为旧清单已有的负例：默认窗口现在可适配，测试却期待拒绝。两条均改为显式32KiB row/batch ceiling，使4个并发调用确实无法放入128KiB，保留“启动前拒绝、零launch”断言。修后相关5项通过，含新增有限输入下游预算与无源读取反例。

独立review：native-final-ack lane只读核对组件划分、分阶段publication预留、显式边界及历史路径，无阻断发现。合流后的组合证据由总控另列，不把分支测试冒充最终合流测试。

原始回执与输入摘要：/tmp/demiflow-regression-closure-20261119/{budget-red,budget-green1,budget-expanded,budget-boundaries,budget-final-targets}。首次失败保留，不覆盖改写。
