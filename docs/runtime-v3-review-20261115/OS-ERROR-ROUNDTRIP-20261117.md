# 原始存储错误与队列生命周期验收闭环

代码候选：`889368b6f7c79914d794eb4a30d6a65b7056838e`；基线：`fc40e29f2884ec7169a91284aa645ac8f1fcdc6d`。
工作树：`/yzp/zhaozy/yangzepeng/0905/_demiflow/worktrees/gpt_error_roundtrip_20261117`；分支：`engine/gpt-error-roundtrip-20261117`。
提交顺序：`a1a255c` → `889368b`。生产范围仅 `demiflow/execution/dataflow_failures.py`；测试范围仅 `tests/test_queue_postprocessing.py` 和新增 `tests/test_runtime_v3_os_error_transport.py`。

## 原始验收及问题

固定 fc40e29 原生提交恢复组 35/35 通过，105 个原测试阶段，73.09 秒；SQLite／队列组 48 通过、4 失败，156 个阶段，208.03 秒。两组均固定源码、四个私有资源域和私有 TMPDIR，无遗留子进程；没有更改原预期。

两条 `test_raw_storage_failures.py::test_partial_writes_complete_or_fail_before_prepare[full/zero]` 是真实回归。原生写入分别产生 `OSError(ENOSPC, ...)`、`OSError(EIO, ...)`，经服务错误回包、worker 错误回包两跳后变成 `OSError(message)`，`.errno` 丢失。原测试先在 errno 断言失败；其后无 pending、零 prepared 和一次 HTTP 的断言在原红基线没有执行，不能提前声称通过。

两条 `test_queue_postprocessing.py::test_stateless_actor_field_binding_and_projection_settle[sqlite-False/True]` 已通过原业务字段、verified receipt、queue_drained、一次 HTTP 断言，最后失败于 `events == ['astart', 'astop', 'aclose']`。测试观察的是已退休的 `_MapWorker._hook`，没有观察统一执行器实际生命周期。

## 修复

固定白名单中的原生 OSError 族现在携带有限数据字段：32 位有符号 errno、最多 2048 字符的 strerror、最多 4096 字符的 filename/filename2，或最多 4096 字节的原始路径字节（以受界十六进制编码）。接收端验证字段集合、精确类型和长度后，只构造原固定白名单类型。两个回包都保留 errno、原异常类型及受支持的文件字段。

没有 errno 的历史 message-only 错误继续按原方式读取；未知异常类仍映射为固定 DataflowError，不根据远端类型名称导入模块，不传输任意 args/pickle。超长或非数据 optional 文件字段不参与对象重建，只保留原有的受界诊断文字；errno 不因这些字段被丢掉。

生命周期测试改为真实 actor 方法向私有事件文件记录 `astart → astop → aclose`，其 `__call__` 直接复用原 `PureAnnotation.__call__`。所有原业务值、验证、单次 HTTP、显式／sink 两种结算断言保持。第一版观察类用了继承，被原冻结无状态准入守卫正确拒绝；随后仅将测试类改为直接 object 类，没有放宽守卫。该次红证据保留。

## 验收与来源

- `/tmp/demiflow-root-finite-v035-native_commit_recovery-r1/receipt.json`：35 passed；SHA256 `d8b054e61fea9ceef9a24acda2c83bba16c5374ef88c803a3039c4efb45acbbf`。
- `/tmp/demiflow-root-finite-v035-sqlite_queue_business-r1/receipt.json`：原 48 passed / 4 failed；SHA256 `9b88ef0b10f652432324b94832f05807ab76a9089cc85c8ae03705e2f7cb4ee7`。
- `/tmp/demiflow-os-error-roundtrip-r1/evidence/receipt.json`：候选 a1a255c，46 passed / 2 fixture failures。两条真实原始存储失败已转绿，后续无 pending、零 prepared、一次 HTTP 断言均实际执行通过；OS 双跳、字节路径、有界反例、未知类、历史错误、HTTP 脱敏、恢复／预算／取消／服务停止／容量等待等直接相关错误契约通过。
- `/tmp/demiflow-os-error-roundtrip-r2/evidence/receipt.json`：候选 889368b，仅重跑上轮两条未绿生命周期用例，2 passed，21.30 秒；真实回调顺序与业务断言通过。

总计 **48 个不同 selector 最近一次验收为绿**，并非 48 项全部在最后一个 HEAD 重跑。r1 → r2 唯一源码差异为 `tests/test_queue_postprocessing.py`，全部生产代码摘要一致；原 46 绿没有重复执行。两批均有完整原三阶段、四个私有域、源摘要前后不变和 `children=[]`。机器证明见配套 `OS-ERROR-ROUNDTRIP-20261117.json`，原完整回执保留在以上路径。

未重跑原 SQLite／队列批中的另外 48 个已绿 selector，未跑性能、U3、GPU、Ray 或全量历史。后续组合由总控执行。七件冻结文件逐字等于既有基线；主仓只读核对为 e40a6dd、原 11 项 dirty；没有修改主仓／总控树，没有 push。
