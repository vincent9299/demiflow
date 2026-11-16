# 原生 CSV 选项、读取与恢复闭环

固定代码 `f28e0ac1c296e3bc9e11b9b3e2317bd1fb46a2d8`：Local 54 项通过，真实同机 Ray Core 1 项通过；没有 skip、xfail、重复阶段或测试中源码变更。两轮都绑定同一份 2,202 个输入摘要、冻结七件、完整命令、实际 import 路径和三个私有资源环境变量，测试后无存活子进程。完整 nodeid/三阶段回执及历史失败见同名 JSON。

## 原问题与修复

历史回归的五个原 CSV 用例全部实际复现。`0df4884` 已补原生 CSV 配置的语义身份，但 `FileReader` 仍固定 `csv.DictReader`：gzip 的 UTF-8 BOM 留在 id 字段名内，TSV 分隔符和 literal quote 选项被忽略，后续关联找不到 id。不是仅修改测试期望。

- `recovery_identity.describe` 仅扩展精确 Arrow `ReadOptions`、`ParseOptions`、`ConvertOptions`：完整字段、原生类型、ISO8601 parser 顺序、回调与 partial 绑定。原 primitive/长列表/历史 prompt enum 身份路径不改。
- `dataflow_compile._source_semantic_facts` 将这些原生声明投影到已有配置结构；旧普通 JSON options 表示不改。
- `FileReader._native_csv_rows` 使用真实 `pyarrow.csv.open_csv`，直接执行原有读、解析、转换选项；保留压缩识别、BOM、分隔符、引号、空串/空值、列类型、projection、跳行、编码及 invalid-row handler。每次向统一数据流交一行，同时携带原生 Arrow schema；含路径列时 schema 同步更新。
- 未传原生选项的 CSV 仍走原有字符串读法，`001`、空串和字面 `NULL` 不被自动推断成其他值。
- 源游标仍是原 `file_index/record_index`，记录身份、序列及 pending 规则不另建一套。压缩输入从固定文件重读到原位置，原生解析选择与首跑一致。
- `source_callable_roots` 的 CSV 分支将 invalid-row handler 接入原有业务依赖遍历；partial 的模块捕获完整保留，源归档只解开 partial 来定位原函数文件。代码固定、传输归档和 worker 加载校验仍使用原权威。
- `dataflow_sources._options_digest` 仅对新支持的精确 Arrow CSV 声明复用 `describe`。旧路径用 stdlib pickle 会在局部/partial 回调携带模块时直接报错；普通旧 JSON/SQL options 的原 pickle 摘要逐字保持。不会改写已保存的历史 snapshot；不匹配的历史仍拒绝使用。

## 资源口径与边界

显式 `ReadOptions.block_size` 影响 Arrow 的第一批类型推断，不能为了过预算静默缩小。打开文件前要求两个原生输入块的字节额度小于 `min(max_batch_bytes, max_resident_bytes − max_row_bytes)`，为 decoded batch 留出空间；无法满足时先拒绝，没有打开原生 reader。

保留至多一个已返回的 RecordBatch，下一次 `read_next_batch` 前显式释放前一批。检查 `get_total_buffer_size()`，把 dictionary 和 slice 保留的父缓冲算在内，再连同两个输入块额度核验；逐行转换前检查该行的 Arrow 字节，发布前继续经过原 `_BaseReader` 编码行上限。不能先整表 `to_pylist` 再切小块。

**实际限制：这些检查不是 Arrow C++ 内部的硬分配器，也不是 Python/进程 RSS 硬上限。** `block_size` 约束输入批处理粒度；原生 parser、转码、解压器和 schema 推断仍可能临时分配，decoded-buffer 检查发生在 Arrow 返回批次之后。原 worker 的进程资源保护继续有效；本补丁不宣称解决第三方预分配硬额度这一更广的问题。`open_csv` 自身为单线程流式读，即使原 `use_threads=True` 也不声称获得多线程解析。

## 验收内容

1. 原 `tests/test_csv_native.py` 五个业务用例全部保留：四种 join、多重性、原字符串、空值、换行、gzip BOM、中文、literal quote、TSV 严格宽度。四种 join 的原 oracle 没有删改。已替换的 DataFusion `plan.txt` 断言改为读取真正执行的 v3 封存 source/relational 图、核对 dag/plan 身份与原 ACK；宽度负例在整个失败 scope 外断言，确认 action 无结果且无 final cut。
2. 新原生身份 31 项：全部可见选项进入摘要、实际 cloudpickle roundtrip、不同 parser/类型/回调不合并、旧普通 options 摘要和历史 primitive/长列表保持。
3. 新 reader 7 项：真实 gzip 拉取、Latin-1、跳行、nullable bool/int/projection/path schema、压缩游标恢复后原记录身份与 sequence 相同；旧字符串语义；显式 block 超预算在 open 前拒绝；数字解码膨胀和宽行在发布前拒绝；两种业务 callback（global、partial 捕获）实际处理坏行，归档覆盖同一 helper 集合，helper 改动后真正 `verify_dataflow_program` 拒绝加载。
4. 原恢复身份 7 项、历史 prompt 大 enum 4 项通过，未用本补丁改写旧 native prompt 字节。
5. 真实 Ray Core：压缩 TSV → typed native CSV → join → 原 driver ACK；人为在完成 action 后中断 run，再用同一 RunRef 恢复；结果及多重性相同，原提交记录逐字不变。测试显式禁止恢复时创建 worker，并以原 Ray Data blocker 验证没有调用 Ray Data。此证据仅为同机 Ray，不代称多机 CSV 或性能门通过。

## 失败历史与交付

| 固定版本 | 真实结果 | 处置 |
| --- | --- | --- |
| `0df4884` | 5 failed / 30 passed | 原 CSV 实际读取失败，身份测试通过 |
| `98f519f` | 1 failed / 40 passed | 原 TSV 负例吞掉异常后 scope 正确拒绝完成；移动异常边界并保留失败无发布断言 |
| `c229c6e` | 1 failed / 48 passed | 新 partial 正向反例揭露 snapshot stdlib pickle 无法携带模块；修原生 options 窄分支 |
| `f28e0ac` | Local 54 passed；Ray 1 passed | 同一代码版本，完整三阶段，0 skipped、0 changed inputs、0 存活子进程 |

父树按 `3a525c6` → `0df4884` → `98f519f` → `0372111` → `c229c6e` → `f28e0ac` 集成，再取本文档提交。没有修改主仓、共享环境、运行服务或冻结七件；输入、测试、Ray 临时目录和证据只落独占 `/tmp/demiflow-*`。本交付关闭 CSV 功能、身份与已确认结果恢复缺口，不以 55 项局部通过声称全部平台或性能验收完成。
