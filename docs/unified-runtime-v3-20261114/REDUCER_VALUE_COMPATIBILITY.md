# Reducer 业务值契约勘误（2026-11-17）

本勘误恢复原公开 `Dataset.reduce_by_key` 的输出能力。旧版 `267b914`
允许 reducer 返回 list、scalar、dict，并将结果原样交给 `take_all` 或后续普通
`map`。v3 将所有 `Record.inline` 限为 Mapping，意外收窄了这项能力。
这里补充当前实现的业务值边界，不重写原设计文档或历史封存计划。

- `Row = Mapping[str, object]` 继续表示按列、按键访问的行。列操作、关系输入和
  field-bound map 仍使用该契约；普通回调可接收 reducer 输出的原值。
- `Record.inline` 可承载现有 typed record codec 支持的非 `None` 数据值：
  字符串键 Mapping、list/tuple、set/frozenset、bool/int/float/str/bytes、
  已支持的时间/Decimal 值和受限 NumPy 值。闭集以原 codec 校验为准，
  不加入任意 Python 对象、对象数组或 pickle 搬运。
- reducer 的最终状态为 `None` 时仍不输出。`False`、`0` 和空容器是实际输出；
  嵌套 `None` 是合法数据。`inline` 与 `payload_ref` 仍恰有一个。
- list 与 tuple 的区别、bytes、负零及既有类型信息通过原冻结/解冻与 typed
  codec 保留。业务值不套保留字段包装；调用者修改已读结果不影响保存的 Record。

Local 与 Ray Core 共用同一 Record、kernel、Exchange、终端和恢复路径。
业务值传输使用既有 `demiflow.record.v3` typed document；控制面的
`encode_wire` 仍维持原严格 JSON 投影边界，不承载这些新增的根业务值。
按列的 schema、Arrow/NumPy batch 转换等操作保留各自输入要求。非 Mapping
及混合结果不推断列名；物化元数据保持未知，默认 `schema()`/`columns()` 对
实际非表格值明确报错，`fetch_if_missing=False` 仍返回未知。

Mapping 的既有编码字节与顶层字符串键检查保持。typed tags、document 版本、
record/source/graph/physical 身份、历史 seal 和原提交/ACK 均不改写。
现有深度/节点/行/状态/驻留/临时空间预算、前缀读取和资源释放路径继续生效；
不增加资源额度，也不新增执行或恢复权威。

回归覆盖见 `tests/test_runtime_v3_reducer_values.py`：真实 Local/Ray 的原值与
回调输入、物化后读取、原 final ACK 重放、物化结果的 nonfinal ACK 前缀恢复，
以及类型、空值和资源拒绝边界。该清单描述验证范围，实际运行结果以对应 revision
的测试证据为准，不用用例存在代替通过结论。
