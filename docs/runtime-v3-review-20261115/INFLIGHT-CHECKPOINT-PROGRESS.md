# 原生在途输入与 checkpoint 组合验收

## 本批完成

- row-local Prompt/Embedding 的并发调用快照保存有界输入、slot、原 invocation scope、已交付输出数量；不保存线程、Future、连接或第二份请求账本。一般可变 UDF/Agent 仍需先结束调用再快照。
- 恢复保留原 scope 和 slot，已交付输出不重复发出；输入、slot 集合或身份损坏在启动调用前拒绝。原生请求仍由原 journal 判定已知/未知：未知不能重发。
- worker 报告暂停就绪后冻结输出前沿，待原提交 receipt/resume 再继续；全局 final 还要求各真实 worker 结束，不能只用 source EOI 宣告完成。
- ObjectSink.prepare 已把本代对象移交给 prepared。删除 driver 无条件 reset，避免抹掉提交期间新进入下一代的对象。
- 修复真实 native response RPC 丢弃原 journal references 的接线错误。首次执行和恢复返回相同字段结构，防止 Arrow struct schema 漂移。

## 真实验证

历史 known_inflight 测试改为在封存的 worker 测试 factory 内，仅延迟真实模型响应之后的 Python 调用完成。它仍使用原 PromptActor、HTTP、原 journal 和对象 sink；不依赖 driver monkeypatch 能跨进程传递。慢调用保持在途时，另一槽处理至第 8 个请求且跨多个真实 checkpoint；随后只在所有已发送请求已有完整 journal 响应时中断，避免把未知请求误当成已知恢复。

恢复后断言全部 12 条业务结果、无错误行、HTTP 总共 12 次、各行 Arrow schema 相同，且实际 cut 的 kernel image 含 pending 输入和原 scope。未知请求保护保留：第一次诊断故障点过早而触发的 UncertainPromptCall 没有被代码放宽。

| 证据 | 结果 |
| --- | --- |
| `/tmp/demiflow-inflight-r4.xml` | 56 passed：在途调用数据快照/损坏反例、真实上述恢复链、K/M 全组。 |
| `/tmp/demiflow-inflight-full-r5.xml` | 75 passed：完整图片业务与进程崩溃窗口、公开 Dataset、Agent/Codex、写表及工作台取消恢复组合。 |
| `/tmp/demiflow-inflight-ray-r5.xml` | 2 passed：本任务私有真实 Ray Core 模型/排序及队列/fetch/提交/ACK/历史链；Ray Data 禁用。 |

所有测试均绑定本隔离树导入，数据/资源域位于 `/tmp/demiflow-*`。本批不能替代尚在进行的分期驻留、安全 GC、其它 lane/产品完整验收，也没有物理多机/GPU性能证明。
