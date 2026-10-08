# Dataset 关系执行

业务只使用 `from demiflow import data` 和 Dataset API。join、reduce_by_key、group_batches 的本地执行统一由平台内部的 DataFusion 关系内核处理；普通脚本、`data.local_execution(...)` 和本地 Pipeline driver 使用同一路径。业务不创建原生会话，不写执行 SQL，不导入 `demiflow.execution` 内部组件。旧公开引擎会话入口和 Options 导出已删除。

## 使用方式

```python
from demiflow import data

concepts = data.read_lance("/work/qid_concepts.lance", version=1)
requested = data.from_items([{"qid": "Q1"}, {"qid": "Q2"}])
selected = concepts.join(requested, on="qid", how="semi")
selected.write_lance("/work/selected.lance", mode="create")
```

同样的 API 可做分组；任意 Python reducer 不被假定为可交换或可结合，也不会自动改写成 SQL 聚合：

```python
counts = concepts.select_columns(["image_n"]).reduce_by_key(
    "image_n",
    lambda state, row: {
        "image_n": row["image_n"],
        "concept_n": (state or {}).get("concept_n", 0) + 1,
    },
)
print(counts.take(10))
```

只有终结动作才执行。读取、关联、回调、写入的业务声明与原来一致。异步或模型调用仍走原流式执行器；先按既有要求物化异步结果，再进行同步关联。

## 原生执行和 Python 边界

- 固定本地 Lance 表上的连续等值关联、投影和重命名尽可能组成同一原生计划。任一输入达到 100 万行时，平台把长关联链按最多两次关联分阶段物化，各阶段先后释放内存，业务不需要插入中间表。普通 join 不增加最终全局排序，输出顺序不保证；DataFusion 为执行关联选择的内部排序仍由引擎决定。宽字段保留 Arrow 列式形式，纯关联直接写表时不会逐行转成 Python 字典。
- 不透明的 Python map/filter/flat_map、非标准读取特性和动态字典是明确的执行边界。先按已有执行上下文处理一次，再以有界批次保存键、稳定序号和无损 Python 载荷。私有载荷列使用 Arrow LargeBinary 的 64 位偏移，避免关联扩增或重新组批后触发 Binary 单数组约 2 GiB 的偏移限制；输入小批次本身不能保证关联结果仍低于此限制。DataFusion 对这些键排序、关联，平台恢复原对象，不按前几行猜整个表的 schema。此内部格式不改变业务输出字段，也不放宽资源保护。
- reduce_by_key 和 group_batches 的键排序交给 DataFusion。任意 Python reducer 按原组内顺序执行，复用已经编码的键，一组独立复制 initial；不做未经声明的并行部分聚合。直接同键 join→reduce 可共用一个原生查询。map 改键后重新计算键，不复用旧分区。
- local_execution 的 process 模式中，若固定类型关联之后只有不扩增行数的分区内同步回调，且写表声明了 schema，平台可在原生子进程中按配置的 batch_rows 执行这些回调；仍位于 DataFusion 引擎外，每批重新反序列化回调声明，再构造回调实例；闭包、绑定方法及 callable 对象都保持任务状态边界。显式 thread 模式继续在原进程的线程任务中执行；flat_map 继续使用流式 Python 任务，避免把不受限的展开结果集中成一个 Arrow 批次。回调异常按原异常类型返回，失败不提交业务表。
- 其他纯 Python 窄变换继续使用现有线程/进程执行器。此改造没有把 Python 字节码编译成原生表达式；也没有让 Ray 的执行器自动切成单机 DataFusion。

保留原契约：canonical JSON 键区分整数、浮点、布尔与字符串；含 null 的连接键不匹配，分组键可为 null；重复键保留笛卡尔积；semi/anti 保留左侧多重性；左连接未匹配的右字段在 Python 行里仍缺省。内部存在标记和源行地址保留字段存在性及下游分组所需的来源次序，不进入业务输出；普通 join 不承诺输出按键或输入序号排序。列名碰撞、缺字段和回调异常继续失败，不能以 SQL null 替代缺字段错误。

## 资源、失败与持久化

开发与评审遵循 [有限资源约束](../AGENTS.md)。下面描述的是当前各层的实际保护范围，不能由某个池、行数限制或 spill 推导整条 pipeline 的内存保证；跨层尚未实现的字节准入、回读/解码和单对象边界在工作区平台 TODO 的 DF-016 跟踪。

`read_lance(...).exclude_keys(other, on=...)` 用于在原扫描顺序中排除已有键。它要求未附加变换的本地 Lance 扫描，支持其固定版本、列选择、过滤、limit 和批次配置，不接受 SQL projection 或 native options；变换接在排除之后。左侧重复行分别保留，含 null 的键不匹配。关联键必须存在于底层源表，可以不包含在最终返回列中。

该节点先以有界批次保存键、Lance 内部行 ID 和扫描序号，再由同一个受管理的 DataFusion 引擎计算仅含行 ID 的排除结果，按原扫描序号排序。正文等非键字段不进入关联/排序的临时载荷表。存活行从同一固定 Lance 快照按批回读，平台适配的 `_take_rows` 接收的是行 ID，不把它误作偏移或物理地址；删除、压缩与固定版本按该契约验收。每批最多 `min(scan_batch,8192)` 行，缺省 1024，单个大行仍可能很大。只读 ID 的前置扫描及 ID 临时表有实际开销，不能称为完全无物化或固定总内存。提前关闭、异常及正常结束均回收临时目录；诊断分别记录 `lance_key_scan`、`keys_only_exclusion`、`lance_payload_read`。普通 `join` 的输出顺序不保证；这里保留扫描顺序是 `exclude_keys` 的显式契约。

平台以 Linux 进程和文件锁管理内部查询。共享执行名额按用户及 cgroup 隔离，同一组中不同工作目录的 Dataset action 仍使用同一个资源池。默认最多两项并发，每项最多 8 线程/2 分区、8 GiB 原生池、24 GiB worker RSS 阈值和 128 GiB 查询临时目录阈值；小容器降低线程、名额和预算。单查询及排队期限各为 3,600 秒。原有 local_execution 的 workers/memory_bytes 仍管理 Python 任务，不冒充原生进程 RSS 上限。

RSS、磁盘和 cgroup 是采样保护，不是内核硬限制。原生池不包含所有 Lance、Arrow、Python 分配。Python 中间载荷按最多 8,192 行或约 8 MiB 编码载荷分批；平台根据当前关系实际携带的已编码输入最大载荷行宽之和，把该查询及结果回读的目标批行数进一步降低，使不透明载荷的估计单批约不超过 8 MiB。inner/left join 累加两侧载荷，semi/anti 只计输出侧；重新编码以新载荷宽度替换旧输入，其他分支此前出现的宽行不压小本分支批次。该估计不含全部 Arrow 开销和类型已知列，且单个巨大行仍可能超过预算，不能替代 RSS 保护。Python 回调和最终 writer 不属于原生 worker 的 RSS 统计，不能把报告解释为整条 pipeline 的硬内存上限。

查询失败、超时、取消或资源保护不自动改用 Python 关系算子，不自动增内存，不重跑回调。内部临时结果在 action 正常结束、失败或提前关闭时回收；SQL、物理计划、资源和失败记录保存在执行根的 `_demiflow/dataset_native/queries/`。父进程异常退出时查询子进程随之终止；主机/进程崩溃可能留下未发布的临时目录。

私有 Lance 写入沿用不超过 8,192 行的查询批次目标，并限制每批 Arrow 逻辑字节不超过 8 MiB。小批直接传给 writer，不再拼接、复制或要求整除文件行数；较大批次只做零拷贝切片，单行超过字节预算时明确失败。报告记录 `writer_batch_rows` 和 `writer_batch_bytes`。切片可能保留父缓冲，Lance 编码器另有缓冲，因此这些准入预算不是进程 RSS 上限。固定行数的 Python 回调使用分块 Table 保持原分组，完整组超预算时报错，不静默改变回调范围。

业务目标仍由标准 Dataset writer 提交，沿用锁、expected_version、不确定提交和恢复规则。内部引擎只接收固定输入和私有输出，不直接写业务目标。`execution_metadata()` 和 local session 的 stats 提供原生查询位置及 Python 边界，可用于验证实际执行后端；不要求业务为选择引擎修改代码。

## 依赖与兼容修复

平台依赖固定 DataFusion 54.0.0、pylance 12.0.0+demiflow.arrowfix1；当前验证环境为 PyArrow 25.0.1、CPython 3.11、Linux。安装平台会安装原生关系依赖，业务不选择引擎 extra。工作区环境已安装 0.3.0；此前已加载旧模块的 notebook/常驻 Python 进程需在方便时重启后使用新版，升级不主动中断运行中的任务。已有 Python 算子、数据引用、业务 schema 和公共历史版本不因内核切换改写。

早期兼容层通过 `pa.concat_arrays` 复制嵌套切片并对齐文件行数，绕过 Arrow Rust 的 offset 缺陷，但会增加内存与复制。2026-10-03 已回补上游两项修复并安装重新构建的 wheel，删除 `aligned_record_batches` 及二次幂／文件对齐约束。125 万行非对齐跨文件逐值校验通过；历史完整 10,648,274 行验收属于旧兼容路径，不算本次新 wheel 的全量验收。

后台周期堆栈采集曾在本环境触发 SIGSEGV，相同 Python 业务变换不加载 DataFusion 也可复现；已取消周期采集，保留 fatal handler、父进程监控及批次进度。相关 CPython 报告 [116008](https://github.com/python/cpython/issues/116008)、[158200](https://github.com/python/cpython/issues/158200) 是定位参考，并非已证明本机 C 层缺陷完全相同。

旧显式接口的大表性能记录属于迁移前验证，不能直接作为当前 Dataset 编译路径的速度。本次 Dataset 接入的测试和真实阶段回执位于工作区 `_demiflow/dataset_native_20261001/`，两个 QID pipeline README 记录实际验收范围与结果。


### 根因与上游修复路线（2026-10-02）

原版 `pylance==12.0.0` 的 Cargo.lock 固定 `arrow-data/arrow-array==58.4.0`；这是编译进 Lance wheel 的 Rust 依赖，与 Python 环境的 `pyarrow==25.0.1` 分开。再次独立复现：16 行 PyArrow 嵌套 struct 切片触发 `arrow-data-58.4.0/src/data.rs` 的 `end <= self.len()` panic；32 行父子均可空的布尔 struct，在每文件 10 行时触发 `null_bit_buffer size too small`，错误路径为 Lance `StructStructuralEncoder::maybe_encode → pushdown_nulls`。两者都不依赖 DataFusion，也不是图片载荷或数据量不足以溢写造成。

已核对两个已合并的 Apache Arrow Rust 修复及实际代码 diff：

- [#10835](https://github.com/apache/arrow-rs/pull/10835)，提交 `c44f8d4c2fa6da9e990b273da60fa8a4add14419`：struct 的累计切片偏移只传给孩子，父 offset 归零；父 nulls 只按本次相对偏移切片。防止重建时对孩子再切一遍。
- [#10709](https://github.com/apache/arrow-rs/pull/10709)，提交 `b54d812d6c666f4a3a38bf6ae9fc2e5420cab480`：去掉 `ArrayData::validate` 中用数据 offset 检查独立 NullBuffer 长度的错误检查。位图自带 offset，由 NullBuffer 保证其内部边界；原始 `null_bit_buffer` 构造时的真实尺寸检查仍须保留。数据 offset 与 null bitmap offset 不同是合法状态，不应强迫所有数据复制成 offset 0。

已按上述路线回补 `arrow-data==58.4.0`，重新构建并安装 `pylance==12.0.0+demiflow.arrowfix1`，其余 Rust 依赖保留 Lance v12.0.0 的锁文件版本。单独升级 PyArrow 不会改变这些内置依赖。构建使用 4 个编译任务、20 GiB RSS／80 GiB 临时盘保护，实测峰值约 5.65 GiB RSS；Rust 33 项、平台 44 项相关测试通过，安装后另复核 8 项。两项原始崩溃复现直接交给 Lance，无业务兼容层。补丁、安装和重建方法见 [补丁说明](../patches/lance-12.0.0/README.md)。

上游原始 PR、回补补丁、源码校验、构建日志与 [检查回执](../../_demiflow/lance_arrow_repair_20261002/patch_check.json) 保留。已撤掉缺陷兼容层；按字节准入、背压、单行超限策略及 writer/RSS 保护继续存在，DF-016 中的完整资源改造尚未完成。

## 本次 Dataset API 实测

固定概念装配阶段 1840.78 秒，10,648,274 行全字段、列表顺序和键唯一性对账通过；图片关系阶段 299.56 秒，30,114,261 行同样通过。独立对账另计，两项原生查询有 47.75 秒实际执行重叠。概念历史同阶段为 4,045.93 秒；图片关系历史同输入为 144.41 秒，当前通用 Python 边界仍有明显成本。不能把这些局部重放当作全流程加速，也不能用旧显式 SQL 的成绩替代 Dataset 路径。公共两表版本保持 @1；详细口径与回执见两条 QID pipeline README 和控制目录 summary.json。

## 显式字符串 CSV/TSV 的原生关联（2026-10-02）

`data.read_csv` 在本地支持 Arrow `read_options/parse_options/convert_options`。单文件、固定表头、全部显式声明为 string、`strings_can_be_null=False` 的 CSV（含 gzip、制表符、引号和单元格内换行）可以直接进入原生关联；文件大小、mtime 和 inode 在查询前后检查。默认未提供 Arrow 选项的旧 CSV reader 行为保留。其他 Arrow 配置走受支持的标准 reader 路径，不静默忽略选项。

涉及这种 CSV 输入的单字符串键关联使用原字符串比较，需要后续有序分组时才使用对应的 JSON 分组键；未命中的源记录不经过 Python 键 UDF。源文件行序用顺序编号保留以供后续有序分组使用，普通 join 的输出次序不保证。空字符串保留为空字符串，Lance 的 null 键仍不匹配；原生 CSV 返回的空单元格 null 仅在这条全字符串读取契约下恢复为空字符串。

此类流式文件关联由平台启用原生 hash join 规划，仍使用同一个受管理的内存池、RSS/临时空间/超时保护。固定 Lance 表的既有排序关联策略保持。业务仍只提交 Dataset API，不创建引擎会话。`tests/test_csv_native.py` 验证四种关联、重复/空/null 键、中文、引号、换行、gzip、TSV 和坏记录；与原有 Dataset native 检查共 22 项通过。全量来源的运行结果另见 QID 图片 pipeline 的固定回执。

## 多来源 union 的工作进程边界（2026-10-02）

连续调用 `union` 时，没有后续转换的子 union 在 Dataset 计划中展平，保持输入顺序及原分支转换范围。原生关系执行器对无转换的 union 直接串接各输入，不再调用 Python identity-map 执行层。此前深层 union 会同时保留多层工作进程池；QID 来源汇总实跑曾出现 OpenBLAS 每进程初始化 64 个线程而创建失败，最终抛出 BrokenProcessPool，cgroup 的 oom/oom_kill 均为 0。

带有实际 map/filter 等操作的分支边界继续保留；不将转换移动到其他输入，也不回放用户回调。15 个来源的连续 union、带转换的分支范围及既有 native 契约共 27 项检查通过，记录位于 QID 图片 `raw_supervision_20261002` run 的 `union_engine_tests.log`。该次生产重跑还在进程启动环境中将 OpenBLAS/OMP/MKL/NumExpr 线程数设为 1，限制 Python 工作进程的数学库线程；这不改变内部 DataFusion 的 CPU 配置，也不修改主机全局环境。

## Arrow 批次校验接 Lance 写入（2026-10-02）

本地 `read_lance(...).map_batches(..., batch_format='pyarrow').write_lance(..., schema=...)` 的单批次算子路径保留 Arrow 列。此前即使回调接受 Arrow，执行层仍经过嵌套列转 Python 行、pickle 溢写、再转 Arrow；现在使用原有受限任务池执行回调，按 Arrow IPC 传递结果。源固定版本、过滤、投影、limit 和全局批边界保留；线程/进程选择、任务并发上限、顺序、异常清理及不重试回调的约束继续有效。回调可返回 Table、RecordBatch 或批次生成器。需要其他算子、未声明写入 schema 或特殊 native/zero-copy 选项的计划继续走原路径，不扩大已支持的执行语义。

嵌套空值/空列表、跨 fragment 批边界、固定版本、空结果、进程和线程、失败不提交及既有纯复制共 7 项检查通过。32,768 行真实 QID 宽表的全部 20 个字段一致；同样两进程下，原路径 11.480 秒、Arrow 路径 3.948 秒（含进程启动）。该 2.91 倍是小规模复制校验步骤的对照，不是全量 pipeline 加速比。回执位于 QID 图片 run 的 `arrow_batch_sink_tests.log` 和 `arrow_copy_benchmark.json`。

## 大文件读取后的内存准入（2026-10-02）

cgroup `memory.current` 同时包含进程内存和文件缓存。只用这个值做准入/终止判断时，Lance 大表读取填满缓存后，即使存在大量可回收页，也可能长时间不启动查询或误触发 cgroup guard。平台现从 current 中仅扣除 `max(0, inactive_file - file_dirty - file_writeback)`，使用保守的工作集估计；活跃文件缓存、脏页、回写页、匿名页和内核内存仍计入。统计不存在或不完整时退回原 current 值。

准入锁、任务槽预留、单进程 RSS、临时空间、超时和额外内存余量均保留。该计算是采样估计，不是内核内存隔离，也不保证所有 inactive 页可即时回收。查询诊断写入 `cgroup_memory_policy=current-minus-clean-inactive-file-v1`。不写 sysfs、不清主机缓存、不修改其他任务。回归包括文件缓存占满时启动真实查询、脏页压力下拒绝准入，以及既有并发、超时、RSS/空间保护和进程生命周期。

## Lance 合并共用资源管理（2026-10-02）

上文的 8 GiB 池和两项并发描述关系查询。符合条件的固定 Lance 部分列补丁现在由平台自动选择列重写，并复用同一组准入名额：Lance 内部池最多 12 GiB，超过 200 万行的补丁最多独占两个已有名额，按实际名额取得进程 RSS 预算。原生计算子进程只准备未提交事务，由原 writer 核对版本并提交；业务 Dataset API 不增加策略或引擎参数。详细条件与实测见 [Lance 合并说明](lance_merge.md)。

这不是整个 pipeline 的低内存保证。排序的一部分中间数据可落盘，部分归并及片段写入缓冲仍不可溢写，池外分配也不受该池直接约束。当前实现的资源不足行为是提前失败并保留旧版本，没有自动无限增内存或重试。要在较低预算下完成同一全量任务，仍需验证仅关联键/行号、按块回读载荷及分片准备后统一提交的执行方式。


## 2026-10-03 普通关联不附加全局排序

用户明确要求普通 join 不隐式附加输出排序。此前平台在关联后生成的 `ORDER BY 关联键、左序号、右序号` 已移除；canonical 键的等值语义、空键、重复多重性、缺省右字段与回调不重放保持。之前依赖 join 输出排列的测试改为完整行多重集对账，不能依赖碰巧观察到的引擎输出顺序。

`reduce_by_key/group_batches` 仍按其顺序契约准备连续分组。大范围的这类必要排序先固定私有中间结果，只让排序键和 Lance 行 ID 进入最终排序，再在受 RSS/空间/时限保护的同一 worker 中按有界批次回读载荷。键排序批次与载荷回读批次分别限制，回读使用行 ID 而非物理地址。单行与池外分配仍受已有准入/保护边界约束，不将 8 GiB 引擎内存池等同于进程全部 RSS。文档业务仍只调用 Dataset API，未修改 DataFusion 源码。

本次验证与全量恢复状态记录在 `_demiflow/document_embeddings_20261003/`；源码修改本身不代表全量宽表已经恢复或完成。

此次回归：普通关系、CSV 和回调相关 56 项检查通过；另以 20,000 条、每条约 4 KiB 的不透明载荷（约 80 MiB）在 32 MiB 引擎池下完成必要分组排序，逐条核验内容及各组首尾顺序，SQL 只选择行 ID。回读前仅有行数及已观测编码大小的估计，不能保证分配前 Arrow 字节上限；回读取得的最大逻辑字节与交给下游的最大批字节分别记录为 `max_lookup_bytes/max_batch_bytes`，后者受 8 MiB 准入约束。切片可能保留父缓冲，RSS 保护仍不可省略。此结果不代表 DF-016 的统一字节准入已完成。
