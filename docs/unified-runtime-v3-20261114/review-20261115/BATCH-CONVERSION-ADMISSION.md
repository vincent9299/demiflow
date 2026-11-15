# 终端列式转换：分配前预算准入

## 交付范围与原始意图

本包解决 I08「有限资源」的一处实际断点：真实 Dataset 经统一执行器取得行后，`take_batch` / `iter_batches` 原来先构造 Arrow / Pandas，才判断输入与输出是否共驻超限。改为在第三方载荷构造之前，依据原批次、原 schema、原 Bounds 估算并准入。超限明确失败，不缩小用户指定 batch，不改类型、列、顺序、null、schema metadata，也不新增 allocator、journal 或执行入口。

可写树为 `gpt_v3_artifact_recovery_20261115`，基线 `daaae37fd3171131862e5eb75dbf9990063568b0`。源文件只涉及：

- `execution/batch_conversion.py`：新纯估算 helper。
- `execution/dataflow_terminals.py::_convert_batch`：构造前调用；保留构造后兜底。
- `tests/test_runtime_v3_batch_conversion_admission.py`：独立反例和真实 Local / Ray 公开入口。

没有修改正在并行推进的恢复预读与累计 collection credit。该边界由 residency / 总控另包负责，不能用本包证明其完成。七个承重文件相对 `362d8528` 字节一致。

## 真实红例

此前只读取证在 `/tmp/demiflow-conversion-readonly/`。两条主要证据：

1. 真实 Dataset 两行各 14,000 字符；`max_batch_bytes=32768`，Python 输入 28,574 字节，Arrow 输出 28,008 字节，合计 56,582 字节。原逻辑在分配后才拒绝。Pandas 同类路径输入加输出 56,820 字节。
2. typed Pandas 单行 `None`，原 schema `fixed_size_binary(4096)`，批预算 1,024 字节。中间 Arrow 实际 4,097 字节，而输入加最终 DataFrame 仅 410 字节，原逻辑甚至返回成功。按输入乘固定倍数无法正确界定这种 schema 扩张。

第一次新测试运行有两个夹具问题，均未作为生产缺陷：原生 PyArrow 自身不支持某个 nested fixed-binary → Pandas 组合，替换为其确实支持的 nested fixed-list/null 布局；宽输入初稿用 `from_items` 把 28 KiB 字面量封进小 state budget，尚未到达转换，改用真实 worker 内生成的 `from_iter`。所有预算反例、第三方构造未调用断言保持。

## 可核对的估算原则

上界始终是 `min(max_batch_bytes, max_resident_bytes)`；没有调大 Run 限额。先计入 Python 行、行容器和列表容器。估算遍历受原 `max_depth`、`max_nodes` 及有限工作窗口约束，不把无界展开藏在估算器中。

Arrow 按 schema 的布局计算：validity、offset、fixed-width、binary payload、子数组、字典索引和 memo。固定宽度与固定 list 即使输入是 null，也计入声明布局；不会按非空输入大小假定输出小。每个普通缓冲单独计入几何增长、旧/新分配和收尾空间，含原生 primitive builder 的最小容量。不是对整个输入统一乘一个常数。

String / binary view 按 16 字节描述符和真实非 inline heap 计算。超过 12 字节的值会启动 PyArrow 的 32 KiB heap block，即使最终 compact 后很小，也在构造前保留这段空间。UTF-8 长度按原字符精确计算，不额外复制整个字符串。List view 同时包含 offsets 和 sizes；REE 不假定压缩率，按最多每行一个 run 计算 run ends 与 value buffers。

typed Pandas 同时计入中间 Arrow 的峰值和 DataFrame 的 object staging、输出列、consolidation、index / column 及嵌套对象；原有 schema coercion 仍由同一个原生构造实现。普通 Pandas 根据实际列与值计算。NumPy 遍历真实列形状和 dtype promotion，Unicode 采用整列最大宽度；不使用均值，不借 Arrow schema 隐藏 NumPy 实际仍会输出的列。

没有声明 schema 的 Arrow 路径仅在已准入的指针列表上推断类型元数据，再计算载荷布局；估算阶段不先创建结果数组。未知扩展/布局在无法给出界限时明确 `UNSUPPORTED_CAPABILITY`。已核实的标准 `JsonType` 按其 string storage 估算，实际转换仍保留原 Json schema；没有把全部 `BaseExtensionType` 当自定义扩展一律禁用。

保留的构造后检查防范实际结果与估算不一致；Arrow 使用 `get_total_buffer_size()`，Pandas 使用 deep memory，NumPy 使用数组 nbytes。它们是兜底，不是新的分配前机制。

## 已验证能力与诚实边界

测试覆盖 Arrow / Pandas / NumPy 宽输入的「构造函数尚未被调用即拒绝」及同输入足预算成功；fixed binary、fixed list、struct/null 扩张；Unicode 列宽、NumPy shape/dtype/负零；decimal、dictionary、map、嵌套 bytes→text；标准 string/binary view、list/large-list view、REE、Json。真实公开入口经过原 Dataset → worker → 原 journal ACK，未 mock 执行器。Ray 使用私有本机 Ray Core，`_RayDataBlocker` 确认没有使用 Ray Data。

未知用户扩展没有安全估算声明时明确拒绝；原生本身不支持的 union 等布局不伪造支持。这里界定的是框架管理的载荷与已识别转换工作区，不是任意第三方版本、全局内存池、解释器或进程 RSS 的硬保证。没有重跑多主机、GPU、生产数据库、付费模型或全量性能矩阵；也不声称产品完成度为 100%。

总控另行发现的一行 `take_batch` 提前终止竞态属于 driver；本包不通过改变输出/limit 绕过。Json 原公开 `iter_batches` 能力已由总控独立证明，本包真实 Local 两进程 / Ray 的三个含 null 值也保持原 Json schema 和字符串。

## 分阶段验证与源码绑定

机器可读证据：`BATCH-CONVERSION-ADMISSION.json`，记录当前源摘要、XML / 日志摘要、全部实际 nodeids，以及哪些运行绑定最终源码。

| 阶段 | 结果 | 范围 |
|---|---:|---|
| 初版准入 + 原回归 | 47 passed / 125.19s | 28 新用例 + 独立选出的 19 个旧 typed / empty / public batch 回归；标准 view / REE / Json 增补前 |
| 初版真实 Ray | 7 passed / 62.33s | 三格式 × 两公开终端、typed null schema；增补前 |
| 标准布局首轮 | 16 passed / 20.42s | view / REE 增补，Json 前 |
| 最终源码 Local | 27 passed / 24.42s | 全部新增标准布局含 Json + 相邻宽输入 / nested / dictionary / unknown-extension |
| 最终源码真实 Ray | 6 passed / 48.32s | 六种标准布局的真实公开 Arrow + Pandas |

前几轮不冒充最终源码完整验收，计数也不相加称为唯一测试总量。最终两轮执行期间源代码冻结；新增标准类型后按总控要求仅跑受影响范围，没有重复全量旧 47 / Ray 7。

可复跑最终 Local 选择：

```sh
PYTHONPATH="$PWD:$PWD/tests" DEMIFLOW_RESOURCE_ROOT=/tmp/demiflow-conversion-layout-final-capacity \
/yzp/zhaozy/yangzepeng/0905/env/bin/python -B -m pytest -q \
 tests/test_runtime_v3_batch_conversion_admission.py \
 -k 'standard_views or public_standard or inline_view or wide_input or nested_types or dictionary_map or unsupported_extension' \
 --basetemp=/tmp/demiflow-conversion-layout-final \
 --junitxml=/tmp/demiflow-conversion-layout-final.xml
```

真实 Ray 选择同文件 `::test_public_standard_layout_schema_and_pandas`，使用 `env-ray/bin/python`、`DF_CONVERSION_PROVIDER=ray_core` 和独立 `/tmp/demiflow-conversion-layout-ray*` 资源/输出目录。全过程未修改主仓、共享环境或已有服务。
