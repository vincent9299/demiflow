# 跨 worker 同一记录只编码一次

本批在根 `7c6d5b7` 上收口实际热路径：原 `_output → route_record` 对载荷编码一次，随后每个 `_publish` 又对同一冻结 Record 编码。新增内部 `_route_record_document` 将二者组合；调用者得到原 `RoutedRecord` 元组与唯一 canonical raw，同一次广播所有 reader 共用不可变 bytes，不建立缓存。

## 保持的契约

- 公开 `route_record(record, *, spec, state)` 签名与行为保留。新旧路径共用原 metadata 验证、一次 ordinal 领取、目标选择、delivery identity，仍调用 `_validate_record`，未省略 parent_record_ids、schema/ref 与 encoded_bytes 校验。
- 原 codec 只增加内部可选 `payload_max_bytes=None`。默认原行为不变；联合路径把 inline payload 的校验/编码长度限制在 `min(channel_window, max_row_bytes)`，完整 document 仍受 `max_row_bytes` 约束。合法的“payload恰好占满credit、metadata额外占用仍在row预算内”可以通过。外部 payload_ref 保持原 channel window 验证及原 document 编码，其载荷读取预算仍由原 reader 管理。
- `_publish` 的新 `validated_raw` 只由已完成全部验证的联合调用传入，并保留 bytes类型/row长度边界；默认terminal仍原样编码。finite frame格式、控制消息、ACK、credit、工件所有权、原路由及计划身份未修改。

## 同一字节候选的验证

原 runner 断言从本隔离树导入，命令：

```sh
PYTHONPATH="$PWD:$PWD/tests" DEMIFLOW_RESOURCE_ROOT=/tmp/demiflow-regression-capacity-route-final /yzp/zhaozy/yangzepeng/0905/env/bin/python -B /tmp/demiflow-regression-runner.py -q tests/test_runtime_v3_route_document.py tests/test_runtime_v3_codec_encoder.py tests/test_runtime_v3_x.py::test_x004 tests/test_runtime_v3_finite_finish.py::test_finite_join_output_checkpoint_restores_exact_prefix_and_multiplicity tests/test_runtime_v3_durable_residency.py::test_parked_limit_propagates_real_input_satisfaction --basetemp=/tmp/demiflow-regression-route-final --junitxml=/tmp/demiflow-regression-route-final.xml
```

**57 passed / 19.36s**。覆盖五路由/恢复ordinal/原delivery identity、原wire逐字节、可选schema与block、typed值、五路由坏parent元数据、schema/encoded_bytes/缺键/目标错误、三个mode的payload与document独立预算、外部ref、真worker sort→async map→结果→ACK、真join100×10中断恢复、parked limit。

整行编码次数的独立观察：三reader广播原“route一次 + publish三次”共4次，新路径1次，各reader的原wire完全相同。测试中的真实worker观察只比较raw与原编码，不改变实际生产调用。

## 微基准（不冒充全链路）

`tests/runtime_v3_route_document_benchmark.py` 在同一codec与预算下比较原两步路径和联合路径；无payload缓存，三个交错顺序轮次，中位数：

| 输入 / 路由 | 原CPU秒 | 联合CPU秒 | CPU加速比 |
|---|---:|---:|---:|
| 小记录 / forward（10000次） | 0.682811 | 0.556089 | 1.228× |
| 小记录 / 3reader broadcast（10000次） | 1.306916 | 0.796946 | 1.640× |
| 左右各8KiB / forward（2000次） | 0.287593 | 0.185582 | 1.550× |
| 左右各8KiB / 3reader broadcast（2000次） | 0.549942 | 0.227419 | 2.418× |

原始样本与wire摘要：`/tmp/demiflow-regression-route-benchmark.json`；汇总及最终源码/测试XML摘要：`ROUTE-DOCUMENT-ONCE.json`。微基准只证明该交接环节收益，不宣称整条40k/120秒流程已通过，根协调者在同原预算下复验。此次未新跑Ray。

承重七件相对 `49b666f` 无差异；没有改原测试、扩大预算、写主仓、push或共享环境变更。测试和基准完成后才提交。
