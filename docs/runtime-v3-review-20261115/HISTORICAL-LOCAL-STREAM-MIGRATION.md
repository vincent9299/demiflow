# 历史 Local / stream 观察迁移

本批只改变历史测试对执行者的观察方式及已授权的入口迁移，原数据维度、预算、顺序、重复多重性、生命周期与错误保护保持。原128/192KiB正例未放大预算。当前提交是可合流候选，完整复验须合入总控的小预算修复及 c29978a 的真实早停限制后执行；不能据本提交宣称完整旧套件通过。

| 原用例 | 契约与迁移 | 当前证据 |
|---|---|---|
| local fused map/reduce、join全how/thread/process | 原预算/结果/组内顺序保留；退休peak_pending_tasks改为实际peak_active_workers<=workers，原worker使用证据来自PID/TID ready | tiny修复由总控已复验，候选仍待统一基线组合 |
| failure_and_early_consumer_close | parent reads list换worker文件；原<=25、closed_early、failed、临时资源清理保持；失败action异常跨出RunContext，避免吞掉失败又要求成功结束 | c29978a独立真实同例两mode绿，待本树复验 |
| Lance固定版本/投影、join+reduce顺序、map_batches+limit、写失败旧版本保护 | 数据全部保持；不存在的DataFusion报告换真实graph/kernel观测 | /tmp/demiflow-regression-local-nontiny-r1.xml 7passed/3failed；3红=上述early2+closed1，生产已有窄修 |
| hash_prefilter_cannot_replace_exact_keys | 旧Bloom不再执行；真实thread worker中的join_spool哈希全部碰撞，精确键比较仍必须给原结果且UDF不可越过 | 同上通过，实际52次hash |
| context/configuration | U002已确认跨executor声明可组合，保留两个来源/声明身份与真实结果；嵌套/非法mode/闭context仍拒 | 闭context修复后原用例也在native-stream-r1 8项组合通过 |
| unsupported_shuffled_child_plan | 已交付shuffle不再当unsupported；原factory声明lazy且两个独立输入真实各扫描一次 | 同上通过 |
| default_map_remains_lazy | 只撤driver主线程归属；原顺序、唯一inline调用线程、队列1保留，通过真实worker PID/TID绑定original ready | 新探针字段修正后待复验，queues真实指标由总控补 |
| callback_cancellation[1/1000×inline/thread] | parent events换真实worker事件；仍原CancelledError，实际call1/close1，原allocation确认退出，零ACK | /tmp/demiflow-regression-stream-lifecycle-r1.xml 4passed，其它3项失败单列 |
| lance_checkpoint_writer_failure → native_lance_writer_failure | 退休checkpoint独立负向/零业务；原400行/2实例/Schema拒绝与消费后I/O失败移到真实write_lance，原LanceSink/提交协议；版本1及旧数据不变，资源CONFIRMED，ACK中无native publication | 首轮writer真实保护过，仅新增总ACK=0探针误判中间checkpoint，已校正；Schema按write_lance已存在ValueError+明确extra消息，未放宽字段检查 |

Native writer测试的注入边界是原LanceSink.prepare中的批次迭代：先消费并写真实Arrow批，然后抛受控OSError，原提交权威完成失败清理。中间cut允许封存worker结果（native=null）；不得把它误认为Lance已发布，也不得禁止原协议的合法checkpoint。
