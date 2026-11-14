# 全部现有公开 API 的迁移归属

按基线 AST 枚举，不能用新增能力清单漏掉旧入口。data 是来源 facade，Dataset 是计算对象，签名保持。四个checkpoint入口继续明确拒绝；不恢复已退役的协议。下面“保留表面”也必须通过G-API，不是保留旧执行路线。

| API | 处置 | 直接叶任务 | 执行装配/验收 |
|---|---|---|---|
| `Dataset.join` | migrate | U001 | G-API |
| `Dataset.reduce_by_key` | migrate | U003 | G-API |
| `Dataset.exclude_keys` | migrate | U005 | G-API |
| `Dataset.group_batches` | migrate | U004 | G-API |
| `Dataset.map_cached` | migrate | U016 | G-API |
| `Dataset.lookup` | migrate | U017 | G-API |
| `Dataset.union` | migrate | U002 | G-API |
| `Dataset.checkpoint` | retired_preserved_rejection | F 公共装配；原函数保留 | G-API |
| `Dataset.checkpoint_async` | retired_preserved_rejection | F 公共装配；原函数保留 | G-API |
| `Dataset.checkpoint_lance` | retired_preserved_rejection | F 公共装配；原函数保留 | G-API |
| `Dataset.checkpoint_lance_async` | retired_preserved_rejection | F 公共装配；原函数保留 | G-API |
| `Dataset.map` | preserve_surface_migrate_execution | F 公共装配；原函数保留 | G-API |
| `Dataset.flat_map` | preserve_surface_migrate_execution | F 公共装配；原函数保留 | G-API |
| `Dataset.map_async` | preserve_surface_migrate_execution | F 公共装配；原函数保留 | G-API |
| `Dataset.batch_map` | preserve_surface_migrate_execution | F 公共装配；原函数保留 | G-API |
| `Dataset.search_web` | migrate | U021 | G-API |
| `Dataset.read_object` | migrate | U022 | G-API |
| `Dataset.decode_document` | migrate | U023 | G-API |
| `Dataset.encode_image` | migrate | U024 | G-API |
| `Dataset.parse_wikitext` | migrate | U025 | G-API |
| `Dataset.fetch` | migrate | U026 | G-API |
| `Dataset.register_documents` | migrate | U027 | G-API |
| `Dataset.read_documents` | migrate | U028 | G-API |
| `Dataset.enqueue` | migrate | U032 | G-API |
| `Dataset.ack_queue` | migrate | U033 | G-API |
| `Dataset.admit_rows` | migrate | U019 | G-API |
| `Dataset.deduplicate` | migrate | U020 | G-API |
| `Dataset.lookup_lance` | migrate | U018 | G-API |
| `Dataset.prepend` | migrate | U029 | G-API |
| `Dataset.save_lance` | migrate | U030 | G-API |
| `Dataset.run_stream` | migrate | U031 | G-API |
| `Dataset.map_batches` | preserve_surface_migrate_execution | F 公共装配；原函数保留 | G-API |
| `Dataset.map_prompt` | migrate | U010 | G-API |
| `Dataset.search_vectors` | migrate | U015 | G-API |
| `Dataset.map_embeddings` | migrate | U013 | G-API |
| `Dataset.document_embeddings` | migrate | U014 | G-API |
| `Dataset.pack_prompt_inputs` | preserve_surface_migrate_execution | F 公共装配；原函数保留 | G-API |
| `Dataset.map_prompt_async` | migrate | U011 | G-API |
| `Dataset.agentmap_async` | migrate | U012 | G-API |
| `Dataset.filter` | preserve_surface_migrate_execution | F 公共装配；原函数保留 | G-API |
| `Dataset.limit` | preserve_surface_migrate_execution | F 公共装配；原函数保留 | G-API |
| `Dataset.select_columns` | preserve_surface_migrate_execution | F 公共装配；原函数保留 | G-API |
| `Dataset.drop_columns` | preserve_surface_migrate_execution | F 公共装配；原函数保留 | G-API |
| `Dataset.rename_columns` | preserve_surface_migrate_execution | F 公共装配；原函数保留 | G-API |
| `Dataset.add_column` | preserve_surface_migrate_execution | F 公共装配；原函数保留 | G-API |
| `Dataset.random_sample` | preserve_surface_migrate_execution | F 公共装配；原函数保留 | G-API |
| `Dataset.sort` | migrate | U006 | G-API |
| `Dataset.repartition` | migrate | U007 | G-API |
| `Dataset.random_shuffle` | migrate | U008 | G-API |
| `Dataset.randomize_block_order` | migrate | U009 | G-API |
| `Dataset.materialize` | migrate | U034 | G-API |
| `Dataset.take` | migrate | U035 | G-API |
| `Dataset.take_all` | migrate | U036 | G-API |
| `Dataset.take_batch` | migrate | U037 | G-API |
| `Dataset.require_columns` | migrate | U044 | G-API |
| `Dataset.count` | migrate | U040 | G-API |
| `Dataset.collect_summary` | migrate | U048 | G-API |
| `Dataset.aggregate` | migrate | U041 | G-API |
| `Dataset.sum` | preserve_surface_migrate_execution | F 公共装配；原函数保留 | G-API |
| `Dataset.min` | preserve_surface_migrate_execution | F 公共装配；原函数保留 | G-API |
| `Dataset.max` | preserve_surface_migrate_execution | F 公共装配；原函数保留 | G-API |
| `Dataset.mean` | preserve_surface_migrate_execution | F 公共装配；原函数保留 | G-API |
| `Dataset.std` | preserve_surface_migrate_execution | F 公共装配；原函数保留 | G-API |
| `Dataset.iter_rows` | migrate | U038 | G-API |
| `Dataset.iter_batches` | migrate | U039 | G-API |
| `Dataset.schema` | migrate | U042 | G-API |
| `Dataset.columns` | migrate | U043 | G-API |
| `Dataset.size_bytes` | migrate | U045 | G-API |
| `Dataset.stats` | migrate | U046 | G-API |
| `Dataset.execution_metadata` | internal_diagnostic | U047 | G-API |
| `Dataset.write_datasink` | migrate | U049 | G-API |
| `Dataset.write_lance` | migrate | U050 | G-API |
| `Dataset.write_parquet` | migrate | U051 | G-API |
| `Dataset.write_json` | migrate | U052 | G-API |
| `Dataset.write_csv` | migrate | U053 | G-API |
| `MaterializedDataset.release` | migrate | U054 | G-API |
| `MaterializedDataset.num_blocks` | migrate | U055 | G-API |
| `read_queue` | preserve_surface_migrate_execution | F 公共装配；原函数保留 | G-API |
| `read_queue_records` | preserve_surface_migrate_execution | F 公共装配；原函数保留 | G-API |
| `read_legacy_queue_records` | preserve_surface_migrate_execution | F 公共装配；原函数保留 | G-API |
| `read_document_receipts` | preserve_surface_migrate_execution | F 公共装配；原函数保留 | G-API |
| `from_items` | preserve_surface_migrate_execution | F 公共装配；原函数保留 | G-API |
| `from_iter` | preserve_surface_migrate_execution | F 公共装配；原函数保留 | G-API |
| `read_records` | preserve_surface_migrate_execution | F 公共装配；原函数保留 | G-API |
| `range` | preserve_surface_migrate_execution | F 公共装配；原函数保留 | G-API |
| `from_arrow` | preserve_surface_migrate_execution | F 公共装配；原函数保留 | G-API |
| `from_numpy` | preserve_surface_migrate_execution | F 公共装配；原函数保留 | G-API |
| `from_pandas` | preserve_surface_migrate_execution | F 公共装配；原函数保留 | G-API |
| `read_parquet` | preserve_surface_migrate_execution | F 公共装配；原函数保留 | G-API |
| `read_json` | preserve_surface_migrate_execution | F 公共装配；原函数保留 | G-API |
| `read_csv` | preserve_surface_migrate_execution | F 公共装配；原函数保留 | G-API |
| `read_text` | preserve_surface_migrate_execution | F 公共装配；原函数保留 | G-API |
| `read_binary_files` | preserve_surface_migrate_execution | F 公共装配；原函数保留 | G-API |
| `read_images` | preserve_surface_migrate_execution | F 公共装配；原函数保留 | G-API |
| `read_sql` | preserve_surface_migrate_execution | F 公共装配；原函数保留 | G-API |
| `read_datasource` | preserve_surface_migrate_execution | F 公共装配；原函数保留 | G-API |
| `read_lance` | preserve_surface_migrate_execution | F 公共装配；原函数保留 | G-API |
| `vector_search_lance` | preserve_surface_migrate_execution | F 公共装配；原函数保留 | G-API |

## 参数与返回契约

完整原签名及行号在 plan.json.api 中。原 map/async/batch/关系/模型配置不重写；新增执行支持不能通过静默忽略参数实现。Ray native_options 由 normalize_native_options 唯一转换，逐参数测试验证num_cpus、num_gpus、memory、concurrency、partition等；ActorPoolStrategy等Ray Data执行对象须明确迁移诊断，不能继续实例化RayData。
