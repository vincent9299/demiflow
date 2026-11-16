# 正式恢复与生命周期门接线

三项原正式门已通过 Local 与真实 Ray Core：`test_multiworker_barrier`、`test_actor_state_lifecycle`、`test_driver_restart`。没有修改原验收断言、设计阈值或引擎来配合测试。

- 固定 `a19e78a`：Local **4 passed / 87.00s**（三门 + 四路 channel 组件），Ray **3 passed / 151.13s**（三门）；全部 setup/call/teardown 通过，无 skip。
- 两套正式验收都绑定同一份 **2123 项源码/测试摘要**，前后不变；冻结七件仍等于 `49b666f`。
- 原完整证据：`/tmp/demiflow-residency-formal-local-evidence/verification.json`、`/tmp/demiflow-residency-formal-ray_core-evidence/verification.json`。同目录 `scenarios/` 保存原门实际结果。机器清单和原门事实已收入 `NATIVE-RESIDENCY-GATES.json`。

## 真实覆盖

| 原门 | 实际取证 |
|---|---|
| multiworker_barrier | 原 nonfinal ACK 包含 **9 个独立 worker 的同一 cut 贡献**；BatchMapOp 镜像真实保留 **3 行未满批次**（flat_map 展开产生）；两个 join 端口与空过滤分支 EOI；中断后恢复得 9 条完整笛卡尔结果，保留原 ACK 前缀，最终 source vector 全部 EOI |
| actor_state_lifecycle | 没有 snapshot/restore 钩子的累计类恢复后 total=10，日志证明已提交前缀不再执行；3 callable worker 各自保留 owner/state/轮转；共享 auto_json 状态并发在业务调用前拒绝；partial 原生调用成功；带锁与 astart Task 的 owner 在成功、失败、取消、close 失败下均按 stop→后台 drain→close 收尾，某 owner close 失败没有阻止另一 owner 关闭 |
| driver_restart | 复用现有真实 Dataset driver SIGKILL 工具；新进程恢复原 Lance 提交，版本仍为 1、输出与计费不重做；新 attempt/fence；原 worker 实际发送过的旧事件在新 driver 被原权威 FENCED 拒绝 |

Ray 场景使用独占的短路径私有 Ray Core 实例、0 GPU，隔离在自有子进程内以释放 Ray 的进程级资源；父 fixture 仍检查自己的子进程和 FD。它不是 fake Ray，也不是多机声明。Local 显式选择 process：`execution='thread'` 的内部 callable 线程池不等同于借用 driver 对象的 thread host。此前探索中借用 host 被正确拒绝恢复，已通过声明正确的 host 模式修正测试，没有放宽生产恢复规则。

## 第四项 route_recovery：仍为部分覆盖

公开 Dataset 已实跑 `join(hash) → repartition(shuffle=True) → map_async`，中断于原 nonfinal ACK 后恢复出完整 8 行；原 physical 确有 hash/rebalance。另以原 RunJournal/ArtifactStore、真实 channel/route/credit 实现覆盖 hash/rebalance/target/broadcast：恢复 routing ordinal、sequence、未消费缓冲、未归还 credit；相同 reserve 幂等、release 幂等；后缀目标/投递 ID/顺序与连续执行一致；旧 route_epoch/plan_revision 均 FENCED。

**这不证明公开 Dataset 可以产生 target/broadcast。** 当前 compiler 没有其公开 lowering。Local (`68f31a5`，17.59s) 与真实 Ray (`7bfd9d0`，28.65s) 的正式 route 门均实跑到该明确缺口后抛 `NotRunError`；pytest 显示的各 1 failed 不计为通过。没有 monkeypatch 编译路由造绿，也没有把底层四路组件冒充四路端到端。

部分事实：`/tmp/demiflow-residency-route-final-{local,ray_core}-evidence/scenarios/`；原返回值、缺口和失败均收在 JSON。最初 helper 漏写 `shuffle=True` 的输入错误仅为测试构造错误，其红日志保留于 `/tmp/demiflow-residency-route-partial-local.log`，不是生产缺陷。

## 集成

本 lane 新提交：`95cb3a6`（四个 delegate + 原生场景）、`57ce257`（Ray 场景子进程）、`a19e78a`（四路组件测试）、`68f31a5`（公开路由测试声明）、`7bfd9d0`（保留 Ray 部分事实）。`219ff25` 是总控 `92d9474` 的 cherry-pick，不应重复集成。

三门正式验收归属 a19e78a；其后仅修改独立 route 测试声明及 NotRun 证据转交，未改已验的 actor/barrier/driver 业务或生产文件。这里不把较早证据偷偷重新绑定到交付 HEAD。

本轮仍未宣称两机网络分区、完整历史回归、浏览器/Jupyter 或全部产品能力完成。主仓、共享安装与生产服务未作为写入或测试目标，无 push、无合主仓。
