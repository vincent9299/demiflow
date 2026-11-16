# 图、工厂恢复与真实 Ray 关闭的合流复核

本轮仍对齐产品的真实执行图与历史、引擎的统一执行/有限资源/原提交恢复；不以专项通过宣称全部完成。
工作树为 `gpt_runtime_v3_review_20261115`，分支为 `engine/gpt-runtime-v3-review-20261115`。

## 已合流的实际变化

- 旧 graph 入口现在执行共享祖先及每个真实 target，Local/Ray 都复用同一 v3 执行、原资源权威和原提交/ACK。调用者的预算、路由和恢复身份进入原封存计划。
- 三 worker 下的图通过原驻留求解器组合可共驻阶段；只读原封存 DAG 的 durable 准入窗口。混合组在源暂停时使用原 checkpoint 排空消费者，不改 checkpoint 行数/时间，不越过 credit 或全局窗口。
- 纯声明型线程工厂恢复原实例状态；借用外部 list/锁的工厂保持原共享身份。分类被动检查 `__init__`、`__new__`、`__call__` 及方法，受类层数/成员数上限约束。不能把 pickle 成功当作独立所有权。
- scope-close 失败后的物理退出仍由原 ResourceManager retirement/reconcile 完成，原失败可见；未知退出不释放收费。不创建第二份资源或提交账本。

## 总控亲验

| 固定版本 | 原生验证 | 结果 |
| --- | --- | --- |
| `6e1aab8` | L016 全部 28 个原 nodeid，私有真实 Ray、真实 SIGKILL/恢复、资源关闭失败及 unknown 收费 | 28 passed，119.40 秒，84 个 setup/call/teardown 全通过 |
| `3a7215f` | 全回归工具与证据反例守卫 | 70 passed，13.58 秒 |
| `a19cc42` | 合流后的 Local chain、工厂状态与共享身份、图真实 target、混合组 credit 边界 | 19 passed，55.95 秒 |
| `a19cc42` | 合流后的真实 Ray chain 与 L018 原四 nodeid 的真实封存/credit/历史重放 | 9 passed，78.55 秒 |

四组均绑定本隔离树导入、固定输入摘要、冻结七件、原三私有 `/tmp/demiflow-*` 域；运行期源码未变，结束没有子进程。Ray 初始化后的进程级 FD 有运行库残留，不宣称零增长。

对应原始回执：

- `/tmp/demiflow-root-l016-final-r2-evidence/verification.json`
- `/tmp/demiflow-root-regression-guards-r1-evidence/verification.json`
- `/tmp/demiflow-root-graph-factory-integrated-r1-evidence/verification.json`
- `/tmp/demiflow-root-graph-chain-ray-r1-evidence/verification.json`

总控另外逐字节复核 owner 的图 Local 46 项/Ray 8 项（各 2,197 输入）、工厂原完整 117 项（2,891 输入）、补充 `__new__` 14 项（2,898 输入），分别与其实际提交对象比较，零不符；所有三阶段及九份图证明附件核对通过。见 `GRAPH-FACTORY-PRIMARY-AUDIT.json`。这些集合有重叠，不相加冒充独立总数，也不冒称全部在最终 HEAD 重跑。

## 保留的红基线与修正

L016 全量 r1 为 25 passed/3 failed。两项新观察器只观察 stage retirement，遗漏最终 scope close；修订后同时检查原最终 ACK、真实 worker drain、原 scope terminal、物理 kill 和 connector close 顺序。第三项新夹具从没有提交的启动失败恢复，原历史保护正确拒绝；改为原两行 ACK 后的真实 worker 失败，第二 attempt 再失败，并要求原提交前缀完全不变。没有放开无日志证明的恢复。

工厂最初的 `3da12f7` 单独候选破坏外部共享事件列表，未单独接收。原 80 项 stream_map 和 22 项 sync_map 在完整分类修复后全部通过；总控再发现只实现 `__new__` 的构造绑定缺口，补真实共享/无副作用反例后才合流。

## 未完成项与性能结论

全库 selector 目录已收集到 8,000 余项，收集不等于通过；残余安全分组仍在实际执行。最终全公开 API/工作台组合尚需匹配最后的生产候选，外部 PostgreSQL 等未验项继续披露。

两项原 40 KiB 驻留能力回退与终端旧 tail 读取窗口的共驻预算问题正在独立修复，不藏入图已通过结果。GPU/两宿主既有证据保留原版本，本批没有重验或扩张其结论。

性能正式门仍未通过。同文件系统终端编码对照为 13.1904→12.9893 秒，约 0.20 秒差异小于观察波动；候选还增加共驻缓冲超过原预算，已经撤回，未集成生产。此前 `/tmp` xfs 对 `/yzp` dpc 的启动/小帧比较保留原值，但已撤回净代码收益归因。主要优化方向仍是批处理、工件粒度及合理驻留，不能用减 checkpoint、跳校验或增加预算换速度。
