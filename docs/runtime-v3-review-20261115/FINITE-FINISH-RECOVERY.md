# 有界 finite 输出、原输出前缀恢复与内存通道前沿

隔离工作树 gpt_v3_regression_20261115；未修改/测试主仓，未 push。前置 d623cd2 为异步 EOI 死锁修复；本树另取 root 的 afa2529→6b94524（source-first quiesce）和 9a68bcc→da0d7ca（实际下游 ACK/输出触发原 cut）。

## 两处真实缺口

1. 原 GraphRunner 在一次 finish 调用中遍历全部 join/sort 等输出，step 无法处理 pause/cut，有限输入不等于调度推进有界。现在保留当前有限生成器，每 step 至多交付 max_records 个此类输出；取消时关闭其原 scratch 资源。结构性有限 kernel 在原 kernel image 中记录 finish_cursor(port, emitted)，恢复读取原 spool、重算确定性的结果并跳过已提交输出前缀，不对前缀再路由/发布。节点 EOI 只在 iterator 与在途调用实际耗尽后传播。没有新增调度/提交账本。

2. 原 memory_incremental 把每一个消费过的 delivery_id 存到跨 park 去重集合，规模随总结果行数增长。真实 16×16 join 在 max_pending_events=64 的第 65 条就拒绝，虽然实际传输信用仍有限。memory 本来由同 producer 有序交付：ThreadHost 直接有序入队、Local 同 socket、有序 Ray receive；driver 禁止同 delivery 在途重复，ACK 后释放原 peer。现保留每个原(edge, producer)的 routing ordinal 高水位，重复或倒退在读取 peer/执行业务前硬拒；原 durable_incremental 的逐 delivery 重放/去重协议未变。高水位保存在同 worker-frontier，空间按已封存输入 membership 有界，不按行数增长。

## 历史、效果与有限保证

- 只在确定性结构 kernel 的原输入上恢复输出前缀；用户 reduce 的活跃 Python fold iterator 不宣称可快照，保持 checkpoint_ready=False，继续排空后才提交。没有将未知/付费效果重放变成普通重复计算。
- 旧历史无可选 finish_cursor/memory_frontiers 字段保持原解释。新 cursor 的 port、非负输出位置、已闭合输入必须一致；越界/外来 port/未闭合输入均拒绝。
- 已满足消费者的有限后缀，只依据原 closed_outputs/finished 节点事实裁剪；不能因合法 limit/提前关闭的未消费后缀误拒恢复。
- 每步有限输出不等于任意第三方函数或 join 首次建索引都可常数时间完成；原时间/取消/provider 清理边界继续生效。
- 未扩大原数据/内存/事件预算，没有移除 durable 恢复或行数/多重性断言。

## 可复验结果

- /tmp/demiflow-regression-finite-finish-r2.xml：1 passed（100×10 join 中途 ACK 后中断/恢复），1 failed（64 pending ceiling 下 memory 第65条错误增长）。最早 r1 是两处测试夹具错误：RunContext 不公开 max_pending_events；36条小结果在收到第5个事件前已排完，未触发预期 partial cut。已改在原 resolver 收紧 sealed structural Bounds，使用1000条有真实中间输出的场景，不把 r1 称作生产红基线。
- /tmp/demiflow-regression-finite-finish-r3.xml：5 passed；1 failed 仅因真 worker 事件文件自动包含 pid 而测试预期遗漏，重复投递已被正确拒绝且实际业务只调用1次。现断言真实 ThreadHost pid。
- 完整 Local 组合 /tmp/demiflow-regression-finite-finish-combo.xml：51 passed、1 Ray 环境跳过，226.81s。新 finite8、EOI3、K、finite segments、durable 停驻/丢ACK/恢复、live input 历史。
- 真 env-ray /tmp/demiflow-regression-finite-ray.xml：2 passed，47.95s。私有 Ray Core 中原 CPU2 grant、map→join→limit→take/run_stream、原 ACK 与 durable 停驻通过；Ray Data 阻断保持。全部 finally shutdown，未访问共享 Ray。
- 上述组合之后仅补合法剪枝 frontier 的恢复判断及 route_epoch 精确整数校验。最终源码 /tmp/demiflow-regression-finite-finish-final.xml：12 passed，52.68s；finite 全8、EOI全3、原 parked limit满足反例1。JSON 单独绑定最终源码及证据，未把先前组合宣称为相同字节的复跑。

所有命令使用 PYTHONPATH=$PWD:$PWD/tests、对应 env/bin/python 或 env-ray/bin/python -B /tmp/demiflow-regression-runner.py；runner 在进程内断言 demiflow.__file__ 属于本树。basetemp 与资源目录 /tmp/demiflow-regression-*。承重七件相对49b666f无差分。

## 剩余项

40k宽 join 的原120秒性能约束由root的codec/SpoolPort优化继续验证；本提交不把该压力列为通过。root的finite_fusion及其它会话新变更尚未在本树合入，最终全包集成验收由root完成。
