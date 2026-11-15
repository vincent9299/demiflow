# 返回集合与组批预算闭环

隔离分支：`engine/gpt-v3-artifact-recovery-20261115`。代码基线与总控候选 `55acec616b2c6c7e2297b070d5e6b6001217fa81` 相同；本树合并 HEAD 为 `8749b943e907fce46a4b8b1e6287ee181f71d2a0`。最终验证时 HEAD + dirty 源码摘要逐项记入同名 JSON，提交后源码字节不变。未修改主仓、冻结七件、预算上限或提交/ACK 协议。

## 实际缺陷与修法

1. `Dataset.take_all(None)` 用 iter_rows 自己 append，真实 192 KiB Driver envelope 下 48 × 6 KiB 行仍全量返回。现在公开入口用真实 take_all request，复用唯一 `collect_rows`；没有第二次执行、第二个 collector 或 allocator。
2. `collect_rows` 原来 decode/append 后才检查累计额度。现在下一行的行数与 `Record.encoded_bytes` 准入先于 schema 读取、payload 解码和 append；失败明确取消/关闭原 subscription，保持原异常。
3. `iterate_batches(batch_size=128)` 原来已读取全部 128 个宽行才拒绝。现在每个 Record 解码前检查当前累积窗口，受原 max_batch_bytes 和 max_resident_bytes 中较小值约束；超限拒绝，绝不把用户要求的批大小改小来凑成功。正常行数、drop_last、schema 边界与格式转换规则保留。
4. take_all 原 limit+1 预准入误拒 `limit=max_output_rows` 的合法精确结果。内部 `verify_complete=True` 只收集 limit 行；额外 Record 证明溢出时不解码/append，明确 ValueError/limit。空结果的 limit=0 仍实际验证为空，不能等同于 take(0)。
5. 原 worker 若先将探测行写入 terminal block，max_output_rows=3 的第四行会先触发通用预算错误。`WorkerGraphRunner._push` 对 sealed take_all 的显式 limit，用原 `_terminal_rows` 在解码/编码/落盘前判溢出。该计数从原 terminal manifest 恢复、flush 不归零；没有新增账本。单 partition 已超过全局 limit 是充分反例，跨 partition 合计仍由 Driver 的同一 collector 判定。None、take、迭代、写出及其他预算错误不改变。

这是原记录编码额度的前置累计准入，不宣称它等于任意 Python 对象的 RSS。`_convert_batch` 保留原 Arrow/Pandas 转换后共驻检查；第三方转换的实际分配峰值不是本补丁新增的分配前硬保证。调用者自行保存多次 yield 的返回批属于调用者持有的数据，不会伪称已被引擎回收。

## 身份、恢复与产品

外层 `checked.action('take_all', source, plan, arguments={'limit': 原值})` 不变。新的 runtime terminal 如实封存成 take_all，不再冒充 iter_rows/take。新计划因此可以有新的 dag/plan identity；历史 seal/dag/pin 不重写。

原 `platform_identity()` 摘要覆盖全部平台 Python，`RunContext.resume` 继续要求原 revision。旧候选历史不能拿本次修复后的代码静默恢复；这不是通过修改历史身份制造兼容。工作台仍读取原封存 plan/bridge，只会对新运行展示真实 take_all terminal。

## 真实反例与最终验收

红基线：

- `/tmp/demiflow-terminal-red.xml`：真实公开 take_all 在 192 KiB 下未抛超限；精确 3 行/额度 3 被原 limit+1 拒绝。该轮另一个 batch case 最初用了非公开的 list 格式，是夹具错误，**不作为缺陷证据**。
- `/tmp/demiflow-terminal-batch-red2.xml`：修正夹具为公开 pandas 后，原逻辑实际读取 128/128 行才拒绝，EOI 前拒绝断言红。
- `/tmp/demiflow-terminal-green2.xml`：16 passed，另 3 上限/4 行 case 真实暴露 worker 先触发 terminal disk/row budget 的错误类型问题；已由第 5 点修复。

最终同一生产源码：

| 检查 | 结果 | 证据 |
|---|---:|---|
| `tests/test_runtime_v3_terminal_collection.py` | **14 passed / 16.28 s** | `/tmp/demiflow-terminal-green3.xml`、`.log` |
| 原 Arrow/Lance/Parquet → worker → materialize → take_batch/iter_batches/schema 3 项；公开 terminal API 场景组 1 项 | **4 passed / 94.51 s** | `/tmp/demiflow-terminal-adjacent.xml`、`.log` |
| 原 T 终端叶测试（独立 Python 进程，明确含 doubles，不冒称真实完整引擎） | **15 passed / 0.77 s** | `/tmp/demiflow-terminal-leaf2.xml`、`.log` |

新专项含真实 process worker PID 验证、小内存成功/超限、typed tuple/bytes/负零、空源、显式 limit 0/2/3、精确 max_output_rows 成功与溢出、原批大小/drop_last、源关闭，以及 4 个实际 DTO 的窄 decode-before-admission 序列反例。公开 batch 超限仍未读到完整大输入时拒绝，不通过缩小输入或 batch_size 过关。

最终计 **33 个 pytest 项通过、0 skip**；不等于全平台 33 个历史失败均由本补丁解决，也不代替总控固定候选的 Local/Ray/产品完整验收。本轮未新增或重跑多机/GPU、付费模型、全量性能矩阵。
