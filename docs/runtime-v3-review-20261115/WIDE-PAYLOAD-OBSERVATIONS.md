# 原始宽行规模与真实 join 边界

前三类真实行为已验证：bytes/tuple/set/None 及重复匹配保真；24 条 1MiB 行经过 narrow reduce 后变宽再 join 仍保真；约 9.75MB 原二进制行在默认 16MiB batch 下因共驻开销拒绝，未发布表，随后原 256MiB 总内存、显式 16MiB row/32MiB batch 下成功写完整原始 payload、多重性、large_binary schema 和版本1。

旧 DataFusion query/result.json/worker_result.json 已无实际执行者，不伪造这些报告。测试探针在真实 worker 的原 join_spool 内只记录输入、回读及实际 key/offset/header 索引，验证索引含精确键和位置、不携带完整 payload，且计费不超原 index_limit；观察原 sealed physical bounds。原先首条新探针误预期 5 次 hydration，实际匹配的 left 行在两次命中间复用所以为4；本项已复验通过，没有改业务结果断言。

两条 40k 大规模测试仍保留：每边超过 256MiB，原 256MiB pool/2GiB RSS/120s，2 threads 上限及1/2 partitions声明。其预算绑定迁到公开 native_resources，原 dataset_session monkeypatch 不再影响统一路径。当前 native join 是串行受控 kernel，不伪造 DataFusion 的内部线程/分区计数；真实 sealed policy 与 worker 收到的限制均断言。actual-byte SpoolPort 合入后已实跑，两臂均在原120s期限抛 DEADLINE_EXCEEDED（2 failed/244.59s），**性能未验收通过**。partitions2 原 journal 最新 cut51 已ACK，但首个来源仅22190/40001行、EOI=false，completed_stages为空，尚未进入join。原store418对象/250831061bytes、最大segment约4.99MB，微文件问题修复已生效；不能把当前吞吐缺口误判为旧微段或join索引失败。完整原始路径与摘要见JSON，总控接手真实热点修复，预算和业务断言未改。

配套 JSON 列出精确通过 nodeid、XML摘要、最终源码摘要与大规模两例的真实红证据。128/192KiB 小记录历史失败仍单列为平台问题，未改成放宽预算的通过项。
