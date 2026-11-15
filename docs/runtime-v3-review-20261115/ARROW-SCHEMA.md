# Arrow 类型与批次语义

原始意图 I01/I08/I09/I13：同一 Dataset 经输入、worker、交换、物化和原生提交后，不能因转成 Python 行而把 int32 / large_string / 嵌套字段 / nullable / schema metadata 改成推断类型。公开 local_execution(batch_rows=3) 也必须控制未显式指定 batch_size 的 map_batches。

隔离树 `gpt_runtime_v3_review_20261115` 的修复：Record 增加可选 schema_ref，指向原 ArtifactStore 的有界 Arrow schema IPC。业务 payload、record_id、source_position、sequence 保持原语义；无类型 sidecar 的旧 wire/document 不增加字段，旧字节不重写。schema sidecar 不建立第二套账本。每 reader / kernel 只缓存最近输入及输出 schema，独立字节、结构上界；跨调用并发访问加锁。

Lance、Parquet、Arrow/Pandas materialized input 提供原实际 schema；纯 filter / 路由保持原 Record，select/drop/rename 投影类型，任意行 UDF 输出重新成为未声明类型的普通行。Arrow/Pandas batch 返回值保存其实际输出 schema，生成器逐批处理。schema 变化分批，累计行数与值不变。批转换和 native sink 检查输入与转换结果同时驻留额度。

物化结果的内部 typed_rows 直接读取原 record artifact 与 schema ref，公开 rows 仍只返回业务值。take_batch / iter_batches / schema / native Lance sink 消费同一元数据。worker 终端不再从单个 null 行推断整个列类型。新计划封存原 batch_rows 默认值；不修改历史语义身份公式。session.stats 投影真实 dataflow 状态及 dag_id / plan_id，不标注未实现的旧 columnar 快路径统计。

验收过程与确切边界：

- `/tmp/demiflow-schema-r1.xml`：13 passed / 4 failed。4 条新夹具错误地捕获整张 Arrow Table，被原语义身份拒绝；改为仅捕获 schema。
- `/tmp/demiflow-schema-r2.xml`：5 passed / 1 failed。Parquet 的 native list child 名称从 item 规范化为 element；夹具改为严格绑定文件原生 pq.read_schema，仍检查 metadata 和类型，不放宽 equals。
- `/tmp/demiflow-schema-r3.xml`：233 passed / 1 failed。真实 Arrow/Parquet/Lance → worker map_batches → materialize → take_batch/iter_batches/schema，以及固定 Lance version + filter + global limit + batch_rows=3 + generator output + native write，全部通过。余下 T004 旧测试载体未定义可选 schema_ref。
- `/tmp/demiflow-schema-r5.xml`：6 passed / 70.10s。修复 T004 可选字段读取；同时验证 recorder 首次 API import 不泄漏包装、三处真实 durable park 恢复、模型 worker usage。r4 误选不存在 nodeid，无用例执行，不作通过证据。

运行均显式设置本树 PYTHONPATH、独立 /tmp/demiflow-* resource/forge/temp/junit 路径，使用 env/bin/python -B -m pytest。测试期间源码与 HEAD 冻结。

不能扩大声明：这不是整条执行的零复制 Arrow batch transport；业务记录仍走统一有界行传输。空结果没有携带行 schema 的额外来源，空 Dataset schema 仍需后续单独核验。未据此宣称任意 Arrow extension dtype 可序列化、跨物理宿主、GPU 推理或性能验收通过。未来 published artifact GC 必须遍历 Record.schema_ref（含 pending cursor / kernel snapshot / terminal materialization 中的 record documents），不能只保留 payload_ref。
