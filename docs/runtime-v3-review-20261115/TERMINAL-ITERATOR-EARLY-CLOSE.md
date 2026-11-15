# 普通迭代器提前关闭：原前缀提交闭环

## 真实缺口与最小修复

在隔离树 `engine/gpt-v3-residency-20261115`，依赖读取准入 `47f4cfa`、
原 source-stop 授权修复 `ee0277d`（本树 `6c553b3`）、原测试 `34462ff`
（本树 `1963f07`）复现：`data.range(1000).iter_rows()` 取首行后 `close()`，
真实 thread host 的 workers=1/2 均等待原 20 秒上限，未取得任何 prefix cut。
未增加预算、改输入或缩短/放宽断言。

原因是 Handle 先等待提交，再释放 subscription；未消费的 terminal delivery
持有真实 worker credit，原检查点要求 credit 清空后才允许贡献，因此互相等待。
而且仅清空当前队列仍不足：已经发送的真实输出可能在 close 后才到达。

修复仅三处：

- 正常 GeneratorExit 先发原 `satisfy_terminal`，再关闭 subscription 释放当前
  transport credit，随后等待原 CommitCoordinator 的真实 prefix receipt。
- subscription 关闭在本 action 执行对象留下瞬态 closed 标志；不是新持久账本、
  source completion 或恢复证据，也不改变原 sealed plan 身份。
- 后续 terminal envelope 仍经过原 lease、boot、attempt、plan 和 terminal membership
  校验；对真实 worker 已 spool 的输出登记原 delivery 后立即走原 `_ack`，不再
  放入已关闭的消费队列。这个 ACK 只释放传输所有权，不等同于业务用户读过该行。
  持久 prefix 仍包括 worker 实际已产生并由原 cut 封存的输出，不声称恰好等于
  用户 `next()` 次数。未读取完整源，未把 prefix 重标 final，未回滚已有原生效果。

## 验证

| 固定运行 | 结果 | 证据 |
|---|---|---|
| 原两个真实 earlyclose 反例，生产修复前 | 2 failed / 41.53s | `/tmp/demiflow-terminal-close-red.xml` |
| 同两例 + source-stop 5 条正反例 | 7 passed / 9.00s | `/tmp/demiflow-terminal-close-green.xml` |
| 最终 close 4 + stop 5 + T 契约 15 + late-final 5 | 29 passed / 27.99s | `/tmp/demiflow-terminal-close-combo.xml` |

新增两条 late-delivery 测试只延迟原 worker 第二个 terminal envelope，保留原 peer、
record、序列和 delivery，直到真实 subscription.close 后再交给原 driver。
断言过期 boot 被拒且不 ACK、合法原消息恰 ACK 一次、无剩余 delivery/队列、
原 journal commit 为 acked、cut 非 final、结果行数小于 1000，以及原 worker
endpoint/活跃 allocation 集合已释放。原两个测试继续断言真实 source cursor 未到 EOI。
已有 late-final 组验证完整输入必须取得真正 final cut，native HTTP 不重复，以及
不完整停止仍保留 prefix，避免该改动混淆两类完成语义。

测试期间源码固定；JSON 记录本树 HEAD、1779 个 Python 输入摘要、XML 摘要与逐 case，
源码摘要在最终组合运行中采集并在结束后逐个核对无变化（不是声称开跑前已采集）。
承重七件逐字对齐 `362d8528c27ae60a0d6ae0ccc986a0ff93847de8`。主仓未编辑或测试。

## 明确边界

本包只闭合正常迭代器停止读取的生命周期和传输信用；不修改读取窗口公式。
独立复审已发现 `47f4cfa` 的固定 `4 * (max_row_bytes + 1)` reserve 在默认
96 KiB 驻留/24 KiB row 下使 read window 为负，属于已确认、仍由另一 owner
修复的问题，不能由本次 29 项通过掩盖。转换准入也由另一包负责。
本包没有新跑全 Local/Ray API、跨机器或 GPU；最终合流仍需覆盖上述窗口修复。
