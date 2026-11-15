# Typed codec：复用固定编码配置，保持原 canonical 字节

隔离基线 `d693dbf`（合 root `e304640`）。生产只修改 `execution/exchange/dataflow_bridge.py`，19 行新增、4 行替换；不改 operator、runner、driver 或承重七件。

## 改动

- 复用原 `JSONEncoder` 配置：`sort_keys=True`、`ensure_ascii=False`、紧凑分隔符、**`allow_nan=True`** 与原 Mapping default 全部保持。遍历、循环引用检查使用每次调用的局部状态，未缓存任何用户值。
- `None`、精确 bool/int 的直接字节路径与原 JSON 相同。int 子类仍走原 encoder，不执行用户的字符串转换；浮点没有新快路，负零、NaN/Infinity 完全保留。
- 原 record 固定七个字段名的前缀只构造一次，删除每行重复编码固定键的工作。未知私有 helper 键仍走原编码路径。
- 已经有界的 payload 字节拼接方式、预算累计、严格类型验证、canonical 反查和历史身份均不改，没有添加完整 payload 副本。

## 独立微基准

脚本 `tests/runtime_v3_codec_benchmark.py` 在独立模块中载入 `e304640` 原 codec，先验证旧/新 wire 一致，再交替执行各 5 轮取中位数。小行每轮 10,000 次，约 200 KB 宽文本每轮 100 次；不会把不同数据量之间的耗时互相比较。

| 情形 | 原实现 | 本实现 | 加速比 |
|---|---:|---:|---:|
| 窄行 document 包装 | 0.1391 s | 0.0413 s | 3.37× |
| 窄行完整编码 | 0.2681 s | 0.1624 s | 1.65× |
| 窄行完整解码/验证 | 0.5969 s | 0.4784 s | 1.25× |
| 宽行 document 包装 | 0.0019 s | 0.0009 s | 2.05× |
| 宽行完整编码 | 0.0519 s | 0.0510 s | 1.02× |
| 宽行完整解码/验证 | 0.0768 s | 0.0752 s | 1.02× |

这是固定字段和小记录编码热点的实测收益，不表示 40k 宽 join 压力已经通过。宽文本自身编码仍占主导，不为扩大数字更换数据或关闭验证。

## 验收

固定生产源码运行 **93 passed / 35.70 秒，0 failed，0 skipped**，原 XML `/tmp/demiflow-codec-encoder.xml`。完整命令、源文件摘要、精确 nodeid、微基准每轮耗时与历史 wire 摘要见同名 JSON。

覆盖新增 26 项、原 codec cost、X codec/channel、全部 scalar/NumPy typed 数据、原 materialize→中断→同 RunRef 已提交重读，以及 borrowed finite 原路由/损坏/预算用例。新增验证包括：

- 负零、NaN/Infinity、Unicode/控制字符、大整数、不可变容器逐字节等于原 JSON。
- inline/ref × 有无 Arrow schema × 有无 block partition 的 8 种原 wire 组合；日期/时区/Decimal/set/bytes/NumPy 原类型内容保真。
- 精确大小边界、非 canonical 字节拒绝；循环对象、非数据对象、孤立 surrogate 与 int 子类仍沿原拒绝/编码语义。
- 8 线程、512 个独立值共用 encoder，结果逐条等于原实现，未留下跨调用缓存。
- 原硬编码历史 payload / semantic identity 字节测试通过；未修改原测试、预算、断言或历史文档。

测试/微基准期间未改 Python 或 HEAD；主仓、生产服务与共享环境未改。尚未宣称完整性能门、多机或 GPU 验收完成。
