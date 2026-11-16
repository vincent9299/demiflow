# 恢复后旧 idle 误触发 cut：窄修与实际验收

2026-11-16。交付生产候选 `03f516b9de00068ddaf44279b96b392ae2e180d7`；在既有 `7159715`/`4a49ed4` 基础上，依次取 `16dcaf1`、`03f516b`。只在 `gpt_v3_residency_20261115` 隔离树修改和测试，未推送、合主仓或修改共享环境。

本项对齐平台 I03/I08：有限容量下实际推进、有界交接和原生恢复。**关闭的是原 V012 的 partial-slots 失败及同根控制竞态；完整正式 V012 还包含 hot-key/small-disk，需总控在集成树组合执行。不是完整性能矩阵达标。** 产品的图、运行记录与恢复继续消费同一引擎事实，没有第二套工作台执行状态。

## 原缺陷与第一手因果

原场景真实占用 Ray 六个 CPU 中的五个，留一 CPU/一 worker 给 Dataset；64×4 KiB 共享源→两个实际有状态 `SlowBranch`→union→iter_rows，应得到128条。原内存16 MiB、checkpoint_rows=64、interval=1000 ms、timeout=120 s 均不变。

`16dcaf1` 单独运行仍失败：120秒预算内只有0条终端结果、48次ACK、49次worker分配。首个源窗口读24行后，每次重开源只读1行，继而轮换两个分支与union，重复五次物理启动。

只读六阶段诊断 `/tmp/demiflow-phase-clock-timeline-r4/timeline.json` 将原因钉到真实事件：第六阶段原消费ACK归还后，427.465秒发出 resume，427.495收到启动期间滞留的旧 idle。此时本阶段未请求 checkpoint、距本阶段时钟起点不足1秒。driver 将旧 idle 当成当前静止事实，立即申请 cut，worker 只来得及多读一行。旧 worker 内 `_idle_sent` 竞态保护无法失效已经离开 worker 的消息。

同时纠正诊断：第三阶段是 union，输入增加24行、输出0行属于真实缓冲进展，**并非无进展空cut**。不把它误判为分支B或定时器错误。

## 修改边界

1. `16dcaf1` 只在原ACK已持久化、所有旧 owner 已退租、前沿无新进展的阶段交接中，剔除不执行业务的 K1/K2 准入耗时。一次性在第一个 K3 activation 之前开始原 checkpoint interval；构造、激活和实际业务仍计时，任务120秒总期限不变。脏前沿、未ACK、旧owner尚在及准入失败均不能重置时钟。
2. `03f516b` 在现有 endpoint/runner 中各保存一个当前 boot 的有界活动水位。data/输入EOI/暂停/恢复等实际活动入原队列时单调递增；纯ACK、receipt、lease续期不制造活动。原 Local帧、Ray RPC、thread 调用使用同一水位语义。
3. worker 在原 condition 锁内决定 idle 时同时固定该水位。消息发送期间若新 resume 到达，旧消息仍带旧值；不得临时把旧静止事实贴上新值。
4. driver 先保留原 allocation/boot/attempt/fence 核验，再只接受等于当前活动水位的 idle；旧消息不触发 cut，未来/非法水位拒绝。初始无活动为0；缺失水位的旧启动观察不能覆盖已经发出的活动。原最终EOI、cut、真正消费ACK、credit及退租权威不变。

水位只属于当前存活控制连接，不写入 plan、record identity、snapshot、journal 或恢复协议；没有新账本、资源分配器或调度器。最大值受63位正整数限制。原七个冻结件与 `362d8528` 字节全等。

## 固定候选实际结果

| 原 partial-slots 场景 | Local | 真实 Ray Core |
| --- | ---: | ---: |
| 正确结果 | 128/128 | 128/128 |
| 业务耗时 | 40.639秒 | 92.247秒 |
| 首条结果 | 32.174秒 | 68.530秒 |
| 原ACK/实际分配 | 22/22 | 36/36 |
| cleanup | 全部CONFIRMED | 全部CONFIRMED |
| 全部子进程退出 | 是 | 是，私有Ray亦退出 |
| 无进展且未完成阶段的cut | 0 | 0 |

每次恢复的源增量：Local为32、22、10；Ray为24、11、13、13、3。两侧最终都恰好64个源记录，不再陷入每次1行的循环。holder在整个业务期间保持原预约，结束后才退出释放；没有增加资源或缩小数据。

证据：`/tmp/demiflow-phase-clock-idle-{local,ray}-r1/verification.json`、`case/proof.json`、`cut-audit.json`；原RunJournal、worker快照、credit观察均保留，cut审计逐个核载体摘要。这是各一次真实边界验收，不是五次配对性能统计。

## 回归与反例

- 旧 `16dcaf1` 的实际 `_handle_event` 通过AST加载到新迟到idle反例，真实失败于 `assert not ex._idle`。只替换测试进程中的原方法，未改候选文件；`/tmp/demiflow-idle-old-handler-red.log` 保留原函数字节摘要。
- 固定 `03f516b`：原控制批量、worker idle竞态、park窗口、phase时钟、durable ACK前后断点/丢ACK/拒绝无证据恢复、最终EOI等 **74 passed / 3 deselected，139.26秒**。`/tmp/demiflow-idle-final-local-evidence/verification.json` 绑定1861份输入，运行前后不变。
- 同候选真实Ray：原 `test_ray_core_durable_parking_uses_same_protocol` 与原512行 `test_real_ray_terminal_acks_use_bounded_batch_rpcs` **2 passed / 33.39秒**。`/tmp/demiflow-idle-final-ray-evidence/verification.json` 同样绑定1861份输入；children=[]，ray_initialized_after=false。
- Local组的3个 deselect 分别是上述2个实际Ray用例和名字含ray的 `receive_many` 纯契约用例；该纯契约已在先前57项单元组合通过，不把deselect冒称Ray未测或已测。两组正式节点数不含此前重跑的57项。

剩余：总控集成后执行真正完整V012 Local/Ray，继续I08九类基准及既有产品整体收口。首条结果仍晚、Ray还有明显启动开销；不得据本项宣布两大意图100%完成。
