# 工作台成功提交被 GC 竞争误标失败

候选 `55acec6` 的真实浏览器运行暴露此问题。原运行 `r0de753fad91e407db8f89b24` 已有两个 ACK：第一个为空进度，第二个为 final，原生 Lance receipt 的 committed_version=1、input_rows=written_rows=2。固定 v1 实际两行正确，16 个 GC intent 全部完成；监督进程却因周期用量记账失败，把 attempt 标为 failed。

原栈为 `workbench.runs.worker.supervise → record_usage → RunJournal._transaction → shared_access → ObjectLease`。共享锁使用不等待的 `LOCK_SH | LOCK_NB`，与工作进程正常的可选 GC 独占锁竞争时立即抛出 `FENCED: Published objects are being reconciled`。这不是工作台图绘制失败，也不是实际业务回执未提交。原失败现场保存在 `/tmp/demiflow-regression-final3-evidence/browser-failed-run/`，整次四节点验收保持原来的 **3 passed / 1 failed**。

修复只为原 journal 的共享对象租约增加最多 5 秒的有界等待，发生在获取数据库连接和开始事务之前，与原 SQLite 竞争等待上限一致。GC 的独占获取仍不等待；工件读写默认行为未放宽。等待后重新校验原锁 inode、发生等待时的协议 header，并继续执行原 attempt 校验。超出等待预算在事务开始前返回 CAPACITY_PENDING；原 inode/header/attempt 不匹配仍拒绝。没有新增持久账本、扩运行时限或绕过原 GC 根证明。

验收：

- 真实 RunJournal 用量记账与原 `_exclusive` 竞争的固定反例：原实现 **1 failed**，准确复现上述 FENCED，日志 `/tmp/demiflow-gc-journal-red.log`。
- 首轮修复与原 published GC/FD 组合 **39 passed，41.32 秒**，`/tmp/demiflow-gc-journal-fixed.xml`。包含原崩溃退款代际、历史根、活读者和句柄预算反例。
- 最终新增边界文件 **4 passed，2.10 秒**，`/tmp/demiflow-gc-journal-boundaries.xml`：竞争后原记账成功且 FD 不增长、陈旧 attempt 仍拒绝、等待有界且无 FD 遗留、等待期间 header 被替换仍拒绝。

以上为隔离树开发验证，不代替修复后的真实工作台浏览器复验；最终组合证据另记。`take_all`/批收集预算由另一独立修改负责。主仓和在运服务未修改。
