# 空闲通知并发修复及原宽数据验收

本项服务于I07提交恢复、I08稳定与性能，以及产品真实运行完成状态；不把结果已经返回等同于最终提交完成。生产修复为`559f74e`，只改变`WorkerGraphRunner.step`通知发布的并发顺序，未修改原提交协议、checkpoint阈值、数据预算或超时。

## 缺陷与修法

原代码先发送EOI/idle，再写`_idle_sent=True`。发送时可能遇到有界mailbox背压；另一控制线程在这段时间收到pause/resume/park/receipt并将`_idle_sent`清为False，旧发送返回后又把它覆盖为True。driver等待新空闲通知，worker以为已经发送，最终提交可能一直等到超时。

修复在原`_condition`锁内确定并预留此次通知标志，实际发送仍在锁外。新控制命令可以正常进入并作废该标志，旧发送结束后不再覆盖它。没有持锁等待mailbox，也没有增加后台线程、无界队列或替代提交者。发送失败仍使任务失败，不伪造ACK。

新增双线程反例将四种真实`receive`命令分别放在EOI/idle发送未返回的窗口，共8种交错；验证控制线程仍可进入、随后确实产生新的idle、EOI保持一次、无新控制时不重复通知。旧实现8/8失败，修复后全通过。连同原有限输出恢复与checkpoint进度测试共**27 passed / 27.41s**。

## 实际运行证据与边界

- 修复前，真实4k join/map/take_all的性能诊断已产生4000条结果，worker最后约108秒中94秒等待条件变量，driver仍泵到原120秒超时。其最后非final ACK覆盖3715行。带时序观察再次运行28.26秒正常结束，因此这是时序敏感现象，不声称每次稳定复现。
- 同版本Local完整入口矩阵在`queue-explicit-True`达到原600秒超时，另167案例通过；队列两条结果已settle。该矩阵红证据保留在`/tmp/demiflow-regression-final2-api-evidence`。它与idle缺陷机制相符，但不把八个组件反例单独当作整矩阵已通过。
- 修复后原队列两臂、producer中断恢复和SQLite消费者真实SIGKILL后恢复已有独立专项通过；完整组合仍按最终集成版本收口。PostgreSQL环境未提供，环境skip不算通过。

## 原宽数据5项全部通过

精确候选`559f74ef5d7655f67b04ffd482e8b7a6a72c7a87`，源码和测试1762项摘要运行前后全部相同，HEAD与工作树全程未变；实际导入来自隔离树。日志`/tmp/demiflow-idlefix-pressure.log`，XML及原始取证文件见同名前缀。

| 原测试 | 结果 | pytest耗时 |
| --- | --- | ---: |
| opaque join / tuple、bytes、set、NULL保真 | passed | 3.061s |
| 前序窄查询后宽载荷批次约束 | passed | 8.770s |
| 不可拆的9.75MB载荷join与sink；负向拒绝保留 | passed | 10.949s |
| 两侧40k×8KiB，分区参数1 | passed | 119.160s |
| 同输入与预算，分区参数2 | passed | 120.134s |

整组**5 passed / 264.24s**。后两例各完整读取80002输入，检查全部载荷、NULL不匹配和重复多重性，原256MiB内存、2GiB RSS保护、2线程及120秒动作期限保持。pytest耗时包含动作之后的图、manifest和取证核验；动作本身仍经过原120秒硬检查。

这证明本次原压力门通过，**不代表性能余量充足**，也不是五类业务完整基准或GPU/跨机证明。此前两参数超时的`/tmp/demiflow-final-pressure.*`原证据未覆盖。机器可读记录见[IDLE-COMPLETION-WIDE-ACCEPTANCE.json](IDLE-COMPLETION-WIDE-ACCEPTANCE.json)。

## 工作区边界

工作树`/yzp/zhaozy/yangzepeng/0905/_demiflow/worktrees/gpt_runtime_v3_review_20261115`，分支`engine/gpt-runtime-v3-review-20261115`。测试仅使用本次`/tmp/demiflow-*`目录。主仓仍`e40a6dd`、原11处文档改动保留，承重七件与`362d8528`逐字相同。没有push、合主仓或操作生产服务。
