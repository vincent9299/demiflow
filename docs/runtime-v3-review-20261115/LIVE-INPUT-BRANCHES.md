# 原 stream / Local 六项红例收口

原始完整诊断：2177a2e 的 test_stream_map.py + test_local_kernel.py **94 passed / 6 failed，524.35s**。所有原128/192KiB预算、early-close两mode原≤25读取、日期/bytes/重复键关联、组内顺序、固定Lance版本及失败旧结果保护通过；未放大预算或删除异常断言。

| 真实缺口 | 修复与证据 |
|---|---|
| slow consumer 的 IterableSource closure含lock在thread宿主被当pickle源 | 仅明确thread stream的IterableSource复用原run/action/attempt/fence registry；来源保持独立worker与attempt_only，manifest只存原稳定code/occurrence/capability |
| 放开上述来源后，source提前读完32行，违反原≤8 | 原外部增量ExchangeSpec的credit_rows收紧到实际consumer声明queue_depth；原并发仍由kernel管理，不另建配额。原≤8、1<peak≤3、active0、32条结果全部通过 |
| shared union/thread/process两worker无法放置 | 在已有2-worker约束下把同一增量suffix DAG共址；只额外允许明确union的relational节点，保全部输入port/shared source唯一occurrence，CPU/memory/state按原stage相加，不改kernel恢复/effect声明 |
| prepend两worker及普通flat_map→filter→map_batches→limit在parked credit上展开失败 | 同一窄共址保原kernel的任务/batch/状态边界，源不合并，finite prefix仍走原port。没有提高credit或放大预算。narrow原例在graph-seams-r1通过，prepend及全部八种nested动作在最终组合通过 |
| queues缺失 | 合入root786d93a的真实output handoff投影；default inline真实workerPID/TID/顺序/queue1、slowconsumer原全部断言通过。该指标明确为output handoff，不冒充外部input queue峰值 |

新增3个真实恢复/拒绝用例：活来源完成最终ACK后registry已释放仍能读原结果且零source重读、原commit列表不变；真实partial cut ACK后停止，下一attempt拒绝且零新增source调用；明确process宿主带lock封存前拒绝、零读取。来源始终独立owner，没有复制活lock或将未完成attempt伪造为可恢复。

最终组合 **22 passed / 116.82s**，涵盖新增3项、原慢消费者/默认inline/prepend/nested两mode、residency原suffix10项与真实metrics。首轮曾将workers4的prepend也多合并成2worker，已收窄为只有2-worker紧约束才遍历分叉，原workers4的4真实allocation/2节点尾部/原finite port/profile总额断言全保留并通过。

完整100项没有在最新候选上再次全跑：旧XML与各修复定向XML逐nodeid全部列在JSON，不把不同行为/不同revision的通过数相加冒充一次全绿。主仓未改/未测试；冻结七件对49b666f无差异；40k原性能红仍由总控处理，不计入本批修复。
