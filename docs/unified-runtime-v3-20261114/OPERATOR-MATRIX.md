# 算子/来源/终端映射与参数冻结

不是重新设计业务参数。下表字段来自基线AST（含继承），编译器必须逐字段保留；CallableSpec/类/函数/聚合放代码工件，资源参数放profile，其余逻辑配置按C02进入身份，不能直接json.dumps全部操作。

| LogicalOp | 真实字段 | 执行与恢复 |
|---|---|---|
| `MapOp` | `callable: CallableSpec`; `native_options: NativeOptions &#124; None`; `stream_options: MapStreamOptions &#124; None` | invoke_unary / incremental；保持原顺序、字段与调用语义；未知state拒绝伪恢复 |
| `FlatMapOp` | `callable: CallableSpec`; `native_options: NativeOptions &#124; None` | invoke_unary / incremental；保持原顺序、字段与调用语义；未知state拒绝伪恢复 |
| `MapBatchesOp` | `callable: CallableSpec`; `batch_size: Optional[int]`; `batch_format: str`; `zero_copy_batch: bool`; `native_options: NativeOptions &#124; None` | invoke_batch或原生adapter / bounded batch；未满buffer/timer/输出cursor入cut |
| `BoundMapOp` | `callable: CallableSpec`; `inputs: Mapping[str, str]`; `output: Optional[str]`; `outputs: Optional[Mapping[str, str]]`; `native_options: NativeOptions &#124; None`; `stream_options: MapStreamOptions &#124; None` | invoke_unary / incremental；保持原顺序、字段与调用语义；未知state拒绝伪恢复 |
| `OperatorLLMMapOp` | `prompt_name: str`; `config: PromptPack`; `inputs: Mapping[str, str]`; `output: Optional[str]`; `outputs: Optional[Mapping[str, str]]`; `native_options: NativeOptions &#124; None`; `options: Mapping[str, Any] &#124; None`; `max_requests: int &#124; None`; `node_id: str` | invoke_unary或M原生adapter / incremental；副作用按原协议；不能推断pure |
| `FilterOp` | `callable: CallableSpec`; `native_options: NativeOptions &#124; None` | invoke_unary / incremental；保持原顺序、字段与调用语义；未知state拒绝伪恢复 |
| `AsyncMapOp` | `callable: CallableSpec`; `concurrency: int`; `queue_depth: int &#124; None`; `catch: Tuple[type[BaseException], ...]`; `label: str &#124; None`; `hard_timeout: float &#124; None`; `execution: str` | invoke_unary或M原生adapter / incremental；副作用按原协议；不能推断pure |
| `DeduplicateOp` | `callable: CallableSpec`; `concurrency: int`; `queue_depth: int &#124; None`; `catch: Tuple[type[BaseException], ...]`; `label: str &#124; None`; `hard_timeout: float &#124; None`; `execution: str`; `on: Tuple[str, ...]`; `key: str`; `output: str` | invoke_unary或M原生adapter / incremental；副作用按原协议；不能推断pure |
| `AgentMapOp` | `callable: CallableSpec`; `concurrency: int`; `queue_depth: int &#124; None`; `catch: Tuple[type[BaseException], ...]`; `label: str &#124; None`; `hard_timeout: float &#124; None`; `execution: str`; `environment: Any` | invoke_unary或M原生adapter / incremental；副作用按原协议；不能推断pure |
| `BatchMapOp` | `callable: CallableSpec`; `max_batch: int`; `flush_interval: float &#124; None`; `concurrency: int`; `queue_depth: int &#124; None`; `catch: Tuple[type[BaseException], ...]`; `label: str &#124; None`; `hard_timeout: float &#124; None` | invoke_batch或原生adapter / bounded batch；未满buffer/timer/输出cursor入cut |
| `EmbeddingMapOp` | `callable: CallableSpec`; `max_batch: int`; `flush_interval: float &#124; None`; `concurrency: int`; `queue_depth: int &#124; None`; `catch: Tuple[type[BaseException], ...]`; `label: str &#124; None`; `hard_timeout: float &#124; None`; `model_contract: Mapping[str, Any]`; `inputs: Mapping[str, str]`; `output: str` | invoke_batch或原生adapter / bounded batch；未满buffer/timer/输出cursor入cut |
| `StreamGroupBatchesOp` | `callable: CallableSpec`; `max_batch: int`; `flush_interval: float &#124; None`; `concurrency: int`; `queue_depth: int &#124; None`; `catch: Tuple[type[BaseException], ...]`; `label: str &#124; None`; `hard_timeout: float &#124; None`; `on: Tuple[str, ...]`; `output: str`; `chunk_bytes: int`; `max_groups: int`; `buffer_bytes: int` | invoke_batch或原生adapter / bounded batch；未满buffer/timer/输出cursor入cut |
| `StreamPrependOp` | `callable: CallableSpec`; `concurrency: int`; `queue_depth: int &#124; None`; `catch: Tuple[type[BaseException], ...]`; `label: str &#124; None`; `hard_timeout: float &#124; None`; `execution: str`; `source_dataset: Any`; `max_rows: int`; `max_row_bytes: int` | invoke_unary或M原生adapter / incremental；副作用按原协议；不能推断pure |
| `LimitOp` | `limit: int` | invoke_unary / incremental；保持原顺序、字段与调用语义；未知state拒绝伪恢复 |
| `SelectColumnsOp` | `columns: Tuple[str, ...]` | invoke_unary / incremental；保持原顺序、字段与调用语义；未知state拒绝伪恢复 |
| `DropColumnsOp` | `columns: Tuple[str, ...]` | invoke_unary / incremental；保持原顺序、字段与调用语义；未知state拒绝伪恢复 |
| `RenameColumnsOp` | `names: Tuple[str, ...] &#124; Mapping[str, str]` | invoke_unary / incremental；保持原顺序、字段与调用语义；未知state拒绝伪恢复 |
| `AddColumnOp` | `column: str`; `callable: CallableSpec`; `batch_format: str`; `native_options: NativeOptions &#124; None` | invoke_unary / incremental；保持原顺序、字段与调用语义；未知state拒绝伪恢复 |
| `RandomSampleOp` | `fraction: float`; `seed: Optional[int]` | invoke_unary / incremental；保持原顺序、字段与调用语义；未知state拒绝伪恢复 |
| `SortOp` | `keys: Tuple[str, ...]`; `descending: bool &#124; Tuple[bool, ...]`; `boundaries: Optional[Tuple[int &#124; float, ...]]` | ordering / finite_complete；固定seed与全局范围；纯输入可重建 |
| `RepartitionOp` | `num_blocks: Optional[int]`; `target_num_rows_per_block: Optional[int]`; `shuffle: bool`; `keys: Optional[Tuple[str, ...]]`; `sort: bool` | repartition_records / incremental hash或rebalance；路由计数入cut |
| `RandomShuffleOp` | `seed: Optional[int]`; `num_blocks: Optional[int]` | ordering / finite_complete；固定seed与全局范围；纯输入可重建 |
| `RandomizeBlockOrderOp` | `seed: Optional[int]` | ordering / finite_complete；固定seed与全局范围；纯输入可重建 |
| `VectorSearchOp` | `callable: CallableSpec`; `concurrency: int`; `queue_depth: int &#124; None`; `catch: Tuple[type[BaseException], ...]`; `label: str &#124; None`; `hard_timeout: float &#124; None`; `execution: str`; `query: str`; `output: str`; `search_spec: Mapping[str, Any]` | invoke_unary或M原生adapter / incremental；副作用按原协议；不能推断pure |
| `SearchWebOp` | `callable: CallableSpec`; `concurrency: int`; `queue_depth: int &#124; None`; `catch: Tuple[type[BaseException], ...]`; `label: str &#124; None`; `hard_timeout: float &#124; None`; `execution: str`; `requests: str`; `output: str`; `checkpoint: str &#124; None`; `checkpoint_operator: str`; `checkpoint_lease_s: float` | invoke_unary或M原生adapter / incremental；副作用按原协议；不能推断pure |
| `RegisterDocumentsOp` | `callable: CallableSpec`; `concurrency: int`; `queue_depth: int &#124; None`; `catch: Tuple[type[BaseException], ...]`; `label: str &#124; None`; `hard_timeout: float &#124; None`; `execution: str`; `request: str`; `output: str` | invoke_unary或M原生adapter / incremental；副作用按原协议；不能推断pure |
| `RegisterDocumentsBatchOp` | `callable: CallableSpec`; `max_batch: int`; `flush_interval: float &#124; None`; `concurrency: int`; `queue_depth: int &#124; None`; `catch: Tuple[type[BaseException], ...]`; `label: str &#124; None`; `hard_timeout: float &#124; None`; `request: str`; `output: str` | invoke_batch或原生adapter / bounded batch；未满buffer/timer/输出cursor入cut |
| `ReadDocumentsOp` | `callable: CallableSpec`; `concurrency: int`; `queue_depth: int &#124; None`; `catch: Tuple[type[BaseException], ...]`; `label: str &#124; None`; `hard_timeout: float &#124; None`; `execution: str`; `request: str`; `output: str` | invoke_unary或M原生adapter / incremental；副作用按原协议；不能推断pure |
| `SaveLanceOp` | `callable: CallableSpec`; `max_batch: int`; `flush_interval: float &#124; None`; `concurrency: int`; `queue_depth: int &#124; None`; `catch: Tuple[type[BaseException], ...]`; `label: str &#124; None`; `hard_timeout: float &#124; None`; `uri: str`; `stage: str`; `key: str`; `output_ref: str &#124; None`; `mode: str` | invoke_batch或原生adapter / bounded batch；未满buffer/timer/输出cursor入cut |

## 多输入与来源

KeyedSource持有PlanInput的source+plan，join/exclude_keys用left/right；reduce_by_key/group_batches用单端口但complete-input requirement不同；UnionSource.inputs按构造次序编号，不按schema合并。完整字段见下表。

| Source/结构 | 字段 | 归属 |
|---|---|---|
| `ItemsSource` | `items: Tuple[Any, ...]` | S；多输入结构G展开，K计算 |
| `RangeSource` | `count: int`; `native_options: NativeOptions &#124; None` | S；多输入结构G展开，K计算 |
| `FileSource` | `format: str`; `paths: Tuple[str, ...]`; `options: Mapping[str, Any]`; `native_options: NativeOptions &#124; None` | S；多输入结构G展开，K计算 |
| `SqlSource` | `sql: str`; `connection_factory: Callable[[], Any]`; `options: Mapping[str, Any]`; `native_options: NativeOptions &#124; None` | S；多输入结构G展开，K计算 |
| `LanceSource` | `query: LanceQuerySpec`; `native_options: NativeOptions &#124; None` | S；多输入结构G展开，K计算 |
| `MaterializedSource` | `handle: Any`; `row_count: int &#124; None` | S；多输入结构G展开，K计算 |
| `IterableSource` | `factory: Callable[[], Iterable[Any]]` | S；多输入结构G展开，K计算 |
| `PlanInput` | `source: SourcePlan`; `plan: Any`; `executor: Any` | S；多输入结构G展开，K计算 |
| `KeyedSource` | `kind: str`; `left: PlanInput`; `on: tuple[str, ...]`; `right: PlanInput &#124; None`; `right_on: tuple[str, ...]`; `how: str`; `suffix: str`; `reducer: Any`; `initial: Any`; `chunk_bytes: int`; `max_rows: int`; `output: str`; `reducer_contract: dict &#124; None`; `reducer_kwargs: dict &#124; None` | S；多输入结构G展开，K计算 |
| `UnionSource` | `inputs: tuple[PlanInput, ...]` | S；多输入结构G展开，K计算 |

File格式支持Parquet/CSV/JSON/JSONL/text/binary/images/records，Reader按format与原options选择；SQL固定snapshot/keyset或明确spool；Lance query保留投影/filter/search/limit/版本，global top_k由全局终端合并；自定义Datasource计划的ReadTask用TaskReader；QueueDatasource是C16服务专用，历史queue/doc读取用HistoryReader。

## 终端与返回值

所有原签名在API-MATRIX/plan.api。take/take_all→list；take_batch→原block格式；iter_rows/iter_batches→持有action的迭代器；count→int；aggregate→原聚合输出；schema/columns→原schema/列；materialize→MaterializedDataset；write_datasink/write_lance保留原WriteResult/表语义；write_parquet/json/csv保留None返回与既有格式，不承诺后端目录布局字节相同。run_stream保留原显式恢复约束，checkpoint旧入口继续拒绝。metadata只读不能触发UDF。

native_options业务资源映射必须在P.normalize_native_options精确列出：num_cpus→cpu_millis（有限非负且粒度校验），num_gpus→GPU资源声明，memory→resident request，concurrency→replica上下限；业务batch_size仍归kernel；block/partition参数按原字段名翻译。Ray Data ActorPoolStrategy/内部TaskPool对象给明确迁移错误，不实例化Ray Data。未知参数不能静默忽略。
