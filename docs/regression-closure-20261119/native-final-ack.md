# 晚到原生 EOI 验收修正

基线：`e3b645b`。本提交只修改 `test_runtime_v3_late_final.py` 的故障注入，不改引擎。

旧用例在第一个非最终切点开始后扣住切点命令。它把 source / embedding 的命令放行，
却要求所有 worker 已完成后才向 terminal 放行。d729ca3f 关闭了控制命令重新启动
业务轮询的漏洞：embedding 已封存的 pending 结果需要原切点全部提交并 resume 后才能
继续；缺少 terminal contribution 又使提交无法完成。旧注入因而要求合法冻结的业务
先越过自己的封存边界，形成循环等待，并不能证明真实 driver 出现活性退化。

运行取证持续 23 个采样点确认：source / embedding 已封存同一非最终切点；embedding
仍有 pending invocation；terminal 未收到切点、未封存；三个 worker 业务和控制队列均空。
原用例两变体超时，原 `idle_cut` 两项保护通过。

现在只延迟原 `CommitCoordinator._finish` 中已经 prepared 的发布完成：真实 driver
依据原 prepared journal 事实启动有限的 publication overlap，恢复 worker，真实 HTTP
embedding 请求完成并经真实数据、EOI 和 ACK 路径到达终端，然后放行原发布。
没有扣住 cut、伪造 EOI、改切点身份、手动 resume 或放宽运行时限。

原断言保留：恰好两个真实 ACK、第一切点非最终且身份不变、第二切点最终且身份不同；
仅最终 image 可以成为结果；显式停止/自然完成两种情况；同 run 恢复不再启动 worker、
不新增提交；输出完整且只请求模型一次。补充断言确保完成发生在真实 publication
future 未结束、driver 已授权 overlap 的窗口内。

证据位于 `/tmp/demiflow-native-final-fix-20261119/`：

- `baseline/result.xml`：旧用例 2 failed，idle-cut 保护 2 passed。
- `probe/states.json` / `probe/result.xml`：未改源码的状态采样与原失败。
- `final/result.xml`：late_final、idle_cut、checkpoint_overlap 三文件 17 passed。
- `mutation/result.xml`：测试进程中仅移除 95de52e 新增的 final-recut 分支，原[False]
  用例在“提交数应为 2”处失败，实测只有 1 个 ACK。源码文件未改，证明新版用例仍能
  检出它原本要保护的最终 ACK 缺失，不能把 live result 误当成可恢复最终结果。
- `updated/result.xml` 保留开发中的一次 hook 误拦已 ACK 恢复事实的失败，不算通过；
  最终 hook 仅延迟 prepared 的首个发布，恢复照常检查原 ACK。

因此旧清单中这两条 `product_regression` 应更正为“故障注入违反封存后推进前提”。
这不撤回 d729ca3f 的 idle-batch 保护，也不扩大为整个引擎已无活性问题的结论。
