# 显式 shuffle seed 的兼容修复

原始目标仍为 INTENT.md 的同一 Dataset 语义/统一引擎（I01）、完整公开能力（I14）、有限资源（I08）及历史保护（I13）。本修复只恢复同一 typed 输入多重集、同一 seed 的可重复值序；不要求复刻旧 Ray Data 的具体排列，不调整引擎的身份、提交或恢复权威。

## 反例与版本

root 已实测旧 `267b914` 的 Ray range→map 30 行在两个新 action 中 `random_shuffle(seed=7)` 同序；当前原公式在 Local/Ray 的新 action 中均不同序。`e05b6b5` 保留该 30 行等序断言以及复用同一 shuffle 实例的变体，未改为无序比较。

新编译的 RandomShuffleOp 配置携带 `shuffle_order_version=typed-multiset-v1`。没有标记的封存配置继续调用原 `SHA256(domain, seed, occurrence, record_id)`；未知标记拒绝。原 K011 helper、RandomSampleOp、RandomizeBlockOrderOp、Record 和历史 codec 不变。已存在 seal 的 prepare 路径继续直接加载原配置，平台 revision 恢复检查不变。

新排序的第一遍主键为原 `_encode_payload(row)` 的完整 typed bytes，相同 bytes 再按 `(record_id, source provenance)` 隐藏排序，为同一原 Record 稳定分配 duplicate ordinal。第二遍键为 `(SHA256(new_domain, seed, SHA256(row_bytes), duplicate_ordinal), row_bytes, duplicate_ordinal)`。ordinal 在第一遍的连续相同 bytes 内递增，只保存上一个 key。完整 bytes 与 ordinal 为碰撞兜底，action/occurrence/record_id/到达顺序不进入第二遍业务主排序。同 action 的输入重排仍保留身份次序；新 action 的身份变化只能交换同值记录，不影响可见值序。完全相同行仍各保留一次，可分散到不同输出位置；原 Record 与全部 sidecar 原样随行。

相同输入的定义是该版本化 typed bytes 相同，**包括 mapping 插入顺序**。不宣称忽略字段顺序，不新增另一份通用 codec。原支持的 bytes、tuple/list、set/frozenset、非有限浮点、负零、日期/Decimal、NumPy 等仍使用原 codec。

## 共驻窗口与生命周期

G 为 `resident_limit(bounds, services)` 得到的实际 worker 授予，不提高计划的资源请求或 run 限额：

| 活跃对象 | 子窗口 |
| --- | --- |
| 第一遍输入 reader | 3G/8 |
| 每遍 build 的保留 run | G/4 |
| build 的当前 typed/record 编码 | G/4 |
| merge 的两个 reader、heap、输出编码 | G/4 |
| merge 的 Record 解码 | G/8 |
| 两阶段 run 引用及列表元数据 | G/16 |

第一遍 build 至多 15G/16。输入原始 refs/borrowed 描述符在输入关闭后仍可能由 kernel 持有，因此另限定其不超过 G/16；第一遍 merge→第二遍 build 加入此实际保留量后至多 G。最终输出保留 merge、排序引用和原输入描述符，至多 G/2，通过独立 `_shuffle_retained_bytes` 加入现有只读 `retained_resident_bytes` getter，供下游编码使用剩余额度。新 SortWindow 的 codec 窗按实际 typed 结构预先检查，merge fan-in 固定为 2，frame 为 G/64；宽行兼容性尚待原预算实测，不能仅凭这条上界宣称原有效输入均已通过。每遍 run 数由元数据窗限制，不能无限产生微小 run 引用。此为受管对象与编码分配预算，不冒充进程 RSS 硬上界。

新分支输入 spool 在受限服务视图下读取；新 flag 的写入按原格式每个 fragment 的解码工作集预先分段，保留输入描述符余量，避免大量小行先形成读取窗口放不下的旧式大 segment。完整 segment 在解码列表前再次检查其外层及各 Record 文档的共同工作集；旧无 flag 分段保持。借用输入通过原 finite reader 的 `frame_call` 接线，seal 的 read/decode、manifest 总量/树遍历及 frame 读取全部进入同一输入窗口；原 port 描述符、seal members 和 manifest traversal 栈共同计入 retained。原始工件与 schema 不重写。

两遍均复用现有 build_sort_runs/merge_sort_runs 核心与 RunArtifactStore 的 put/read/release。原输入、第一遍 runs、第二遍 runs、合并中间工件和临时写入共同计入原 max_spill；额度不足按原权威拒绝，未增加第二份磁盘额度。释放只撤销本次排序引用，物理删除/退款仍由原 store GC 在原生命周期执行，不能把 unpin 描述为立即回收磁盘。

包装器在成功、异常、取消和提前关闭时显式关闭输入及两遍 merge iterator，释放两遍原始 runs；merge 自己释放其创建的中间 runs。借用输入及历史 artifacts 不由此包装器释放。store 写失败的 staging 继续由原写入 finally/崩溃恢复协议处理。

## 验收状态

实现及测试在独立叶 `gpt_shuffle_seed_20261117`，基线 `c3393e4`。测试尚未运行，等待 root 的共享测试窗口授权；静态 AST 解析不代表运行验收。

专项测试覆盖原 30 行两种 action 用法、union/flatmap 重复与乱序输入、真实 store 双遍 spill/有限 fan-in、第一/第二遍取消、输出关闭、磁盘写失败、typed 值、旧公式/无标记 seal 直接加载、过宽帧拒绝、Local/Ray 同序和原非最终 ACK cut 恢复。原 `tests/runtime_v3_support.py` 由 root 管理，本叶不修改。

审查修订另加入同组 Record 反序/乱序的完整身份次序、实际 OperatorKernel 的只读 getter/保留量回调、G=max_batch=1MiB/max_row=4096 下1000小行填满旧 segment 的正例，以及已有 sort runs 时读取合法256-producer seal和512-child manifest的预算边界。

授权后的测试顺序（未启动）：先运行 `tests/test_runtime_v3_shuffle_seed.py -k 'not same_typed_input_order_on_local_and_ray_core'`，再运行其中真实 Ray 用例；旧路径回归为 `tests/test_runtime_v3_k.py::test_k009`、`::test_k010`、`::test_k011`、`tests/test_runtime_v3_finite_borrow.py`、`tests/test_runtime_v3_finite_spool.py`、`tests/test_runtime_v3_wide_order_seed.py`、`tests/test_runtime_v3_blocks.py::test_real_target_rows_and_shuffle_partition_setting`、`tests/test_local_union_plan.py::test_union_accepts_shuffle_and_keeps_each_independent_source`、`tests/test_local_kernel.py::test_supported_shuffled_child_plan_is_lazy_and_keeps_distinct_sources`。现有用例的输入与预算均不改。
