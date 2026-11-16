# Arrow 承载最小契约：先复用现有 Record

这是源码复核后的候选方案；替代早先默认新增 ArrowRowPayload DTO 的假设。未实施，等待 root 裁决。

## 可以复用的部分

- `Record.payload_ref` 可指向整个不可变 Arrow IPC batch；`schema_ref` 保留原始字段与 metadata。
- Lance `source_position` 已有 version / fragment_id / row_offset。把 batch 的起始 position 与行数存入其 IPC schema metadata，offset = record.row_offset - batch.start_row_offset。global filter/projection 扫描用现有 global row_offset，普通 fragment 扫描用实际 fragment_id；无需往 source_position 增字段或改变 record_id。
- Record/SourcePoll 仍逐行业务消息；每行的 sequence、cursor、credit rows=1 不变，不能用 batch 冒充一行业务数据。
- `SourceCursor.offset` 是受界 JSON。需要批内恢复时，可在新的 cursor 内保存 batch_ref + 下一个 row offset；旧 cursor 无这些字段仍按既有固定版扫描恢复。pending_refs 仍保存原 transfer document，不引入第二本 checkpoint。
- `BlockPartition` 不参与解释 Arrow 数据，其原输出分区语义保持。

## 无法零修改复用的硬证据

1. `exchange/dataflow_bridge.py:_validate_record`、`_decode_record_document` 要求 **encoded_bytes == payload_ref.size_bytes**，且允许的 data media 无 Arrow batch。
2. `kernels/dataflow.py:_input_row_bytes` 把整个 ref.size_bytes 当一行；`_materialize_row` 要求 ref.size_bytes <= max_row_bytes 并调用原 typed-row decoder。
3. `dataflow_handles.py:iter_rows`、`dataflow_sinks.py:NativeTerminalSink._blocks` 对 ref 也直接 `_decode_payload`，不知道 batch offset。
4. `dataflow_gc.py` 的 `_LEAVES/_STRUCTURED` 是封闭集合，新 Arrow MIME 未登记会 fail-closed，不能只让 reader 新发一种 ref。
5. terminal 聚合、output budget 和 channel envelope 取 Record.encoded_bytes；若每行强行填整个 batch 大小，虽然保守但重复 N 倍计量并拒绝原本合法业务。不能以此宣称性能与能力闭环。

## 请求裁决的最小变化

不扩 frozen Record DTO。增加版本化 MIME `application/vnd.demiflow.arrow-batch.v1`，明确此 MIME 下：

- payload_ref.size_bytes = 实际 IPC 工件大小，进入原磁盘存储/传输计量。
- encoded_bytes = 该业务行的有界逻辑载荷计量（具体 Arrow 行物理/编码上下界需先定义并验证）；旧 typed-row MIME 保留原相等校验。
- batch 工件 metadata 固定 source_id/split_id、version/fragment_id/start_row_offset/row_count、schema digest；读取时核实原 source_position 落在工件范围内。batch 内不混 source/split，混源 union 仍逐 Record 携带各自 ref，禁止凭 row_offset 单独匹配工件。
- batch 的实际 retained parent buffer 由原 worker grant 另行预留，至多一个活动 batch/有限已准入窗口；channel 的小引用不能绕过这个持有期。不是新增 allocator。若无法明确预留生命周期，应停止该方案，不能靠事后 RSS 检查补洞。
- 透明 select/rename 可对完整批做 Arrow select/rename，封存一次新的共享 IPC ref，所有行仍逐行引用；不得每行写小 IPC 文件。Python UDF 才转行；其新输出回原 typed-row 路径。

## 必须一同接入的消费点

source_readers.LanceReader / _BaseReader pending、dataflow_sources.spool_record；exchange codec；kernels/dataflow._input_row_bytes/_materialize_row 与透明投影；record_schema；NativeTerminalSink._blocks；DataflowMaterializedHandle.iter_rows/保留释放；dataflow_gc 引用遍历；source/channel/operator checkpoint snapshot 的 ref 可达性。原 store、source seal、DataflowExecution、CommitCoordinator 和 ACK 继续唯一拥有权威。

原纯 copy、typed schema、Local/Ray、批内 cut/resume、早关/取消、混源、宽行与慢消费者预算反例都必须实跑。不能只修 spy，也不能 driver 重扫源表绕过这些消费点。

当前列序/merge 提交不依赖此候选方案，可先独立交付。
