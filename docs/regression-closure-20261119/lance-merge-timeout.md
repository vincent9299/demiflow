# Lance merge 大样本超时定位

本次没有修改产品或原测试。旧记录中 `test_lance_merge_strategy.py` 整文件未在
900 秒完成，不能把其中 18 项都记为失败或通过。

## 已核实的原因

原历史目录保存的数据库表明：

- 正常 100,010 行用例：失败前已有 427 个 ACK；最后封存 source cursor 为 41,256。
- 末尾重复用例：文件级截止前已有 194 个 ACK；最后 source cursor 为 19,343。
- 两者均尚未到完整源 EOI、原生最终 merge；目标还没有发布新版本。

在当前 e3b645b 产品代码（本任务 11d8e86 只改测试）上，保持原 100,010 行输入及所有
默认预算，做了 60 秒有界进度采样。source cursor 与 ACK 数持续递增，不是静止死锁。
采样主动以明确诊断异常结束，因此这份结果不是该用例的验收通过。

`source_readers.py` 的读取上界为
`min(256, max_batch_bytes // max_row_bytes)`。默认两者都是 16 MiB，因此扫描为 1 行；
业务 `batch_size=8192` 被该安全上界收紧。Arrow carrier 虽启用，此时也只是每行一个
共享对象。每行原生读取、编码、artifact 登记、传输与周期提交累积为低吞吐，原默认
600 秒执行时限不足以处理 100,010 行。它不是 `_UniqueMergeKeys` 溢写后停住。

这是当前默认预算/读取策略的性能限制。本轮遵守“性能先保持，后续优化”，不取消
最坏单行字节预留，不调高默认或测试运行时限，也不缩小原大数据反例。

## 验证和范围

- 原文件除两个大数据用例外：**16 passed / 2 deselected**，56.42 秒。涵盖策略选择、
  值与保留列、版本保护、重复/空键/缺失键拒绝、并发 CAS 和隔离守卫。
- 对同样 **100,010 行**直接运行原生 merge（诊断，不能替代 Dataset 全链）：
  真实跨 100,000 键阈值写入 SQLite、全部更新、保留列与旧版本不变、临时目录清理，
  约 0.40 秒。末尾追加第 100,011 行重复首键：真实磁盘索引拒绝、未提交目标版本、
  清理目录，约 0.36 秒。
- 两个原 Dataset 大数据用例仍是**未完成完整验收**。原测试和旧失败/超时证据保留。

证据目录 `/tmp/demiflow-lance-merge-audit-20261119/`：
`historical-last-commits.json`、`probe/progress.jsonl`、`probe/result.xml`（主动结束诊断）、
`nonlarge/result.xml`、`native/result.json` 与完整执行脚本/日志、`verification.json`。
