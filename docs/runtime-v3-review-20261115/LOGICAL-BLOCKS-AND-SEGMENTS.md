# 真实逻辑分块与增量结果封存

本批把已有公开参数接到原统一执行器：`Dataset.repartition(num_blocks)`、`target_num_rows_per_block`、hash keys/sort、`random_shuffle(num_blocks)` 和 `local_execution(partitions=...)`（仅 shuffle 默认块数）。使用原 RunArtifactStore、RunJournal、CommitCoordinator、资源 allocator；Local/Ray Core 同一 kernel。N 是真实逻辑结果块数，允许少于 N 个 worker 顺序完成；不宣称 N 路同时驻留。

## 新增/迭代契约

- `BlockPartition(occurrence_id: sha256, index: int >= 0, count: int > 0)`，要求 index < count。`Record.block_partition` 默认 None；None 不进入旧 wire/identity。传输文档只有实际分块记录才新增 `bp`。业务 record_id、sequence、source_position 不替换、不伪造。
- Repartition 使用原 finite_complete 输入 spool，源耗尽后赋真实块标签。无 shuffle 平衡连续分配，多出的行依次放前面的块，保留原输入顺序。指定 N 可以包含空块；target_rows 空输入返回一个空块，非空为 ceil(rows/target)。超 max_partitions 在执行前/确定行数后拒绝。
- hash 使用现有 typed group key 的 SHA256 前 8 字节取模，同键同块；sort=True 时块内按 key 排序。keys/sort 必须 shuffle=True，sort 必须带 keys。排序走原有界外排序，不在 driver 收全量行。
- row-local map/filter/flatmap 保留标签；batch 遇块边界先 flush 并保留该块。join/union/global sort 重组后清除旧块标签。RandomizeBlockOrder 按块重排并重新编号，原输入 ordinal 仅参与排序键，块内行顺序保留。scalar 终端始终全局一份。
- worker sink manifest 保留原 v3 schema，新增可选 `partitions`（各项 partition_id/rows/bytes/data_ref，可带 block_partition）；原顶层 data_ref 单分区历史仍可读取。schema_ref 保留在总层。combine 校验分区唯一性、数量和总计，按 logical index 组合。
- 新 `application/x-demiflow-terminal-segments.v1` data_ref 指向原 Store 中不可变二叉 forest。节点字段 schema/height/rows/bytes/children，每个 child 是 ref/height/rows/bytes。当前 roots 最多 O(log segments)，单个索引受 control budget，遍历约束深度/节点数/循环和父子总计；内部结点为二叉，叶段只用原 records media。旧直接 data_ref 兼容。

## 生命周期与预算

`TerminalBlocks` 最多持有一个原 ScratchStream 文件。块切换或 checkpoint 只写当前增量尾段，关闭原 scratch；恢复直接沿原不可变段继续追加，不复制旧前缀。存储仍由原 journal 先准入，真实 rows/bytes/分区数有界。块/forest 的常驻元数据另按保守对象费用在原 stage grant 与 max_state_bytes 内准入，超限失败。

Store pin 仍由原 commit/handle owner 发出，读取/写出/恢复统一展开段树。没有新账本，也不改变提交/ACK 含义。新 terminal 原始数据写入由每 cut 重写累计前缀改成仅写增量；每 cut 对历史 refs 的 pin 遍历仍可能累计 O(n²)，本批明确没有完成该元数据性能优化。其他算子的既有 spool/镜像成本也不在这条性能声明内。

## 验证与边界

精确命令、源码摘要及 XML 见 LOGICAL-BLOCKS-AND-SEGMENTS.json。

- 最终 Local 新分块全组 + 40 KiB 真实 durable 驻留：12 passed / 1 skipped（普通环境无 Ray）；130.67s。
- 独立 env-ray 私有 Ray Core 集群：1 passed，12.60s。Ray Data 的 from_items/from_arrow/read_datasource 全部设为必抛，真实 Dataset 仍输出 3 块、[3,2,2] 行以及全部 7 行。finally shutdown。
- C/K、空 typed schema、原 durable 崩溃/ACK/恢复、宽行 10 MB join 写 Lance、排序/随机种子组合均按失败范围逐一修复复验；记录保留初始失败，不声称第一次全绿。
- 明确验证 7→3 块、2→5（空块）、0→4、target_rows→5、local shuffle 默认 4 块、hash 同键/块内排序、多重性、batch 边界、全局 count、原 journal 最终 commit 后恢复同一分区 manifest、block randomization、65 次增量 append/中途 forest restore。
- 七承重文件与冻结基线逐字一致；主仓未改/未测，无 push，无主仓合并。

本批不代表全部两大意图已经完成。真实 thread 宿主/不可序列化 live callable、历史 pin 成本、性能全面评测及多机故障验证仍按单独任务推进；同机 Ray 不冒充多机。
