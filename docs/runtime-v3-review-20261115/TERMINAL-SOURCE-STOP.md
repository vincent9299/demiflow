# 单 worker 有界终端停止的源证明

生产基线为 `c04f2d0`（本树源码相同，报告提交前 HEAD 为 `0117482`）。真实 `from_arrow` 单行字符串/Arrow JSON 扩展类型，`take(1)` 与 `take_batch(1, pyarrow)` 在 `workers=1, worker_mode=thread` 下均复现 `FENCED: source stop lacks downstream completion proof`。红基线为 **4 failed，6.85 秒**，`/tmp/demiflow-terminal-stop-red.xml`。

原因是同一 worker 内的终端 `_close_inputs` 关闭源，不需要跨 worker 的 `input_satisfied` 回报；driver 已主动发送 pause/satisfy，却只认可跨阶段下游关闭证明。因此合法的源 pruned 回报被误判。故障与 JSON 转换无关，普通字符串对照同样失败。

修复仅改 `execution/dataflow.py`：driver 在实际向当前 endpoint 发送 pause/satisfy 后，记录该 allocation、boot、attempt、fence、plan revision 和原 source split 的停止授权；EOI 仍经过原 run/epoch/boot/分配校验，只有确切授权的原 split 才能借此接受 pruned。该授权只是允许停止，不代表读尽输入，不写入 `observe_input_satisfied`，不改原 cursor、不重写已提交 cut、更不把合法前缀涂成 final。原逻辑下游完成与自然 EOI 继续通过各自证明进入同一 checkpoint 权威。只增加当前执行内有界授权集合，没有新持久账本。

定向验证：

- 上述 4 个真实类型/终端用例由红转绿，并断言发生 pruned 时提交仍非 final；不是仅断言函数返回。
- 新真实 worker EOI 反例同时验证：旧 boot、旧 attempt、旧 controller epoch、不同 plan revision、错误 fence、外来 split、仅 `_stopping=True` 无授权、授权属于旧 boot，均拒绝；恢复原真实回报后原 ACK 成功。构造反例时仅对已验证控制 JSON 使用原 `_thaw_json`，未放宽 DTO 公共验证。
- 新增 5 项与原 `test_parked_incomplete_take_keeps_original_prefix_receipt` 最终 **6 passed，7.95 秒**，`/tmp/demiflow-terminal-stop-final.xml`。原 range10000.take2 的不完整源、非 final、terminal_satisfied、原 ACK/cursor 断言未改。
- 同一生产候选此前组合的既有 5 项 late-final、shared-prefix、logical-limit/native-write、unknown-model-no-replay 共 8 项均通过；在 `/tmp/demiflow-terminal-stop-green2.xml` 原样保留。

## 明确保留的未闭环问题

该组合原结果为 **12 passed / 3 failed，104.89 秒**。一条是新增负例夹具重建不可变控制 JSON 的问题，已修正且上述最后 6 项已验；另两条为新发现的真实 `iter_rows` 提前关闭问题：单/双 thread worker 读取 range1000 的首行后 close，在订阅尚有未消费交接时等到原 20 秒上限，不能生成前缀 ACK。两条原断言与红证据保留，独立测试为 `tests/test_runtime_v3_terminal_close.py`，交 residency 分支修 Handle/subscription 收尾，不能据本提交宣称已闭环。该区生产代码本提交未碰。

所有测试使用本隔离树 `PYTHONPATH=$PWD:$PWD/tests /yzp/zhaozy/yangzepeng/0905/env/bin/python -B`，过程内断言 DemiFlow 模块来自本树；临时目录和资源域均为 `/tmp/demiflow-terminal-stop-*`，没有修改或运行主仓，没有削弱原测试或扩大预算。完整 API/产品验收仍绑定其原候选，本提交只主张以上定向证据。
