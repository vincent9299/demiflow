# 有限计算与检查点协调计时边界修复

交付代码：`aae8c409dca38f8b522c323d1ca7cd6612b8bb53`；基线 `1997f7a`。
隔离树：`/yzp/zhaozy/yangzepeng/0905/_demiflow/worktrees/gpt_finite_checkpoint_admission_20261117`；分支 `engine/gpt-finite-checkpoint-admission-20261117`。

## 结论与意图

原热key reduce在正常外排/归并期间被原60秒checkpoint协调时限误杀，原因属实，已修复。本改动服务于产品的真实运行/正确恢复，以及引擎的稳定、有界和统一执行；不改变结果、提交权威、未知效果、历史绑定或资源额度。此项功能闭环不代表高性能目标已完成：原完整I08五对性能门由总控另行执行，本分支未运行或宣称通过该门。

## 最小修法与边界

保留原source-pause时刻启动的60秒协调额度。仅在cut尚未开始、且所有非idle参与者都提供当前activity generation和boot下实际不可重放finite reduce finish证据时，排除这段有限业务等待；已消耗的协调额度不补满。原RunContext总120秒始终独立约束。普通未响应/未排空参与者仍消耗原60秒；一个忙worker不能替沉默peer免责。cut、发布及终端停止不享有此排除；发布超时仍排空并保留原真实ACK。

- `WorkerGraphRunner`只在两处既有完整输入EOI后的有限finish调用口围住原iterator。pause之前/之后进入finish都可通过原控制通道回报；不伪造idle，不把reduce变成可快照或可重放。
- 新`checkpoint_wait`是既有WorkerEvent中的有界瞬态类型，只有generation、单调sequence、partition、occurrence四字段，无业务数据、快照或额外账本。更新原DTO/codec枚举及host early-event路由。
- driver先经过原run/attempt/fence/allocation/boot/plan_revision校验，再验证partition和真实封存reduce/finite_complete输入。陈旧generation或乱序sequence不改变当前前沿；未来generation及越界归属拒绝。idle与busy互斥；ACK、resume、新checkpoint和wave切换清除旧状态。
- 控制通知在condition锁外发送。finish失败时，附带通知失败不能替换原用户错误。
- 新状态规模只随当前worker和封存图增长；事件仍受原控制队列/帧上限约束。无新allocator、coordinator或durable identity格式。

## 实测与版本

| 版本 / 证据目录 | 实际结果 | 含义 |
|---|---|---|
| `1997f7a` / `/tmp/demiflow-finite-checkpoint-baseline-r1-evidence` | 14 failed | 新真实Dataset 1/2 worker案例均复现原coordination超时；另12项既有phase-clock fixture在缺partition_groups处失败。不是14个新生产缺陷。 |
| `a9d5031` / `/tmp/demiflow-finite-checkpoint-r1-evidence` | 20 passed / 27 failed | 两个真实Dataset、原alignment超时及publication超时保留ACK均已通过；失败是15项本轮新单元fixture缺字段和上述12项旧fixture，全部保留证据。 |
| `80c16e0` / `/tmp/demiflow-finite-checkpoint-r2-evidence` | 151 passed / 1 skipped，36.44s | 完整窄组：finite等待、checkpoint行数/时钟、原两条timeout反例、控制批量和DTO/codec。skip是该解释器未装Ray。 |
| `aae8c40` / `/tmp/demiflow-finite-checkpoint-ray-r1-evidence` | 22 passed / 0 skipped，37.95s | 新错误保留反例及原finite全部专项，真实Local 1/2 worker、真实Ray均获原final ACK和物理退出；前组skip的Ray bounded ACK用例也实际通过。 |
| `aae8c40` / `/tmp/demiflow-finite-checkpoint-hot-key-r1-evidence` | 2 passed，83.49s | 未修改原测试：16384×4KiB、64MiB、120秒总时限、60秒coordination正例76.62s通过；8MiB磁盘反例仍真实拒绝。该时间是功能测试耗时，不能作为正式性能中位数。 |

这些集合有重叠且跨版本，不相加为最终HEAD的全量通过数。真实Local/Ray专项把原run目录、commit_id、sequence/status、busy/clear控制事件、actual结果及物理退出写进原pytest回执properties；可直接核验对应原RunJournal。

旧`test_runtime_v3_phase_checkpoint_clock.py`只补实际wave partition_groups和已有resume observation_rows=0；原输入、时限和所有时钟断言保持。新公开业务反例以1.8秒有限fold对1秒coordination确定性锁根因，原真实压力另用原60/120参数验证。基线业务运行在同1997提交的独立`/tmp` checkout，仅作功能红对照，不用其耗时算性能比。

## 来源与交接

每组通过显式PYTHONPATH、进程内import断言、四个私有资源域及TMPDIR/D8运行；没有修改主仓、root集成树、共享环境或承重七件，未push/合主仓。五组源摘要分别逐条与其Git提交核验：2990/2991项，均0不符；运行前后源码/HEAD一致、children_after均为空。完整机器核验见同名JSON以及`/tmp/demiflow-finite-checkpoint-source-audit-20261117.json`；baseline外置测试文件摘要另绑定`a4e14c9`。

集成范围 `1997f7a..aae8c40`：5个生产文件、1个新测试文件、1个旧fixture文件。root其它分支同文件的terminal `_push/_publish` 和末尾 `_OutputSubscription` 不属于本改动。交给总控做合流独立验收和正式性能复测；此叶树已无在运测试/业务。
