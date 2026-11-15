# 真实执行观测与 task 恢复反例收口

生产观测只投影真实 worker 事件及原提交结果。worker 原 ready 事件携带实际进程 PID、runner 原生线程 TID；driver 验证原租约后接收，同 allocation 身份改变拒绝。runner TID 不冒充每个调用池线程 TID。活跃峰值从成功 activate 回执计入，到已排空并退休时移除；EOI/ready 到达顺序不用于推算并发。历史 worker 明细按 max_partitions 和控制字节限额截断，实际已观测 boot 总数及截断标记仍保留，不随分期轮转无限增长。

公开 metadata/stats 包含实际节点行计数、真实工作进程、已准入上界（明确不是观测峰值）、执行引擎 dataflow_v3、完整/提前完成/失败状态。cleanup 的内部 cancel 不覆盖原业务错误类型；执行准备失败同样可见。没有生成旧 DataFusion query 文件、peak_pending_tasks、延迟或队列峰值等虚构统计。

## 两组历史 task 测试迁移依据

- 三种 callable 形态的正常业务仍要求 `[1,2,3,1,2,3,1]`、每行执行一次、原输入不变。class 在 2-worker 有限分期下可因原生恢复重建本地资源；改验每进程构造/关闭严格平衡且调用只发生于活跃实例中。原任意构造器外部副作用没有 exactly-once 承诺，本测试不建立第二去重账本。
- 两种 partial-task 原生 ACK 恢复保留原 workers=4→2 收紧反例：封存的是三个 worker 必须同时驻留的 memory cohort，当前容量 2 必须在业务开始前快速拒绝。events 和原 journal ACK 完全不变；随后原 RunRef、可行 workers=4 下用改过的当前 batch_rows=999 恢复，仍按封存 batch_rows=5 的业务状态继续，已提交前缀不重复。

## 验证

进程内 import 路径断言均指本隔离工作树；源码与原始 XML 摘要在 EXECUTION-OBSERVATIONS.json。最终观测专项 5 passed / 17.30s。此前同一 task 测试字节的组合 9 passed / 103.41s（其中 4 个旧观测用例，不能把两次相加称为 14 个独立测试）。承重七件与 49b666f 字节相同。

仍有明确未完成观测：stream catch/drop、stage latency、queue peak、stage policy 展示；对应原断言未删除。Local native_resources 参数与线程宿主在总控/另一实现路推进，这次不宣称其全量回归完成。主仓、共享环境、生产服务未修改。
