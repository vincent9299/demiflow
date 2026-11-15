# EOF 等待与定时输出：真实死锁修复

隔离工作树 gpt_v3_regression_20261115。未修改/测试主仓、未 push。

## 缺陷与修复

原分组反例把 timer 从 30ms 改为 500ms（上游回调等待下游仍为 2 秒）即可稳定触发 TimeoutError：GraphRunner._edge_eoi 同步 kernel.finish → ConcurrentCalls.finish 占用唯一 business runner，下游分组 poll 无法继续。这是实际能力缺陷，未增加 timeout/预算或放宽断言。

EOI 现在仅记录原输入事实。runner 每轮继续轮询原 kernel；调用在途时延后应用 EOI，最后未满批次可非阻塞发起，确认所有输出排空后才向下游发 EOI。直接调用 kernel.finish 仍保留同步合同。`finished_ports` 是原 worker-frontier.v3 的可选字段：标识已执行 finish 的输入边，和原 kernel 在途快照共同恢复，避免最后一批再次调用。旧历史缺少字段按旧同步完成规则解释；新字段只允许已收到所有 producer EOI 的本 worker 输入边，无新增提交权威或业务身份。

## 真实验收

- 红基线：/tmp/demiflow-regression-eoi-timer.xml，1 failed / 12.93s，栈确定上述阻塞点。
- 新反例：行调用、最后 partial BatchMap，两种模式原 2 秒业务等待、实际下游 500ms timer、全部行与调用次数保持。
- 新恢复：最后未满 embedding 批次 EOI 已执行、HTTP 仍在途时封存原非 final cut；模型响应已写原 native journal 后 os._exit(77)；同 RunRef attempt 2 恢复，仍只有一个 HTTP、一次请求预算、一条结果，原 ACK 保留。
- 组合：/tmp/demiflow-regression-eoi-combo.xml，42 passed / 161.52s；包含 EOI 3 项、已有 batch/group 定时与真实恢复、调用槽恢复和 K kernel 合同。
- 原分组/lookup 加补充 timer 7 项也通过，/tmp/demiflow-regression-eoi-timer-green.xml。

命令均使用 PYTHONPATH=$PWD:$PWD/tests、/yzp/zhaozy/yangzepeng/0905/env/bin/python -B /tmp/demiflow-regression-runner.py，进程内断言 demiflow.__file__ 在本工作树；临时资源与测试目录全部 /tmp/demiflow-regression-*。

## 尚未闭环

此提交处理异步 EOI 排空；finite-complete join 等 finish 生成器仍会在一次 step 内输出全量。根协调者发现其阻断中途 cut 的压力问题，下一独立补丁处理有界 finish 与原有限输入恢复游标；这里不把 40k 压力列为通过。
