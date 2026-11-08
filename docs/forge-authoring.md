# Forge 开发接口

## Request 与项目生成

先查看 `demiflow forge schema request`，编写 `demiflow_forge_request_v1`，再执行：

```bash
demiflow forge new request.json --out my-pipeline --json
```

Request 说明名称、目标、原始需求、行粒度、输入输出 JSON Schema、map 名称/版本/config/config_schema、输出模式、副作用、行数与测试超时以及完成条件。未决问题和不支持的能力会在生成前拒绝。初始 operator 和测试带未完成标记，不能通过检查。

## Definition 与 operator

`pipeline.py` 只能包含 `PipelineDefinition` 的静态字面量，三个节点固定为 `read_lance`、`map`、`write_lance`。operator 是普通同步函数：

```python
from demiflow.forge import operator


@operator(
    name="normalize_text",
    version="1.0.0",
    description="Trim and optionally uppercase text.",
    input_schema=INPUT_SCHEMA,
    output_schema=OUTPUT_SCHEMA,
    config_schema=CONFIG_SCHEMA,
)
def normalize_text(row, *, uppercase):
    text = row["text"].strip()
    return {**row, "text": text.upper() if uppercase else text}
```

实际生成项目要求装饰器参数也是静态字面量，不能用模块常量代替。函数接收一个 `row` 和关键字配置；input/output schema 必须与 Pipeline 完全一致，参数及默认值必须与 config_schema 对齐。修改实现后运行 `demiflow forge manifest my-pipeline`，不要手改 Manifest。

## RunConfig

```json
{
  "schema_version": "demiflow_forge_run_v1",
  "inputs": {"source": {"uri": "datasets/input.lance", "version": 1}},
  "outputs": {"result": {"uri": "datasets/output.lance"}},
  "max_rows": 1000
}
```

首版只接受项目内 `datasets/` 的相对 `.lance` 路径，输入版本必须是正整数，行数不能超过 Definition 预算，输入输出不能相同。`dry-run` 只展开计划，不读取 Lance、不导入 operator、不写输出。

## 真实执行

Freeze 后，`forge run` 逐行校验输入输出，调用现有 Dataset API，并用输出 JSON Schema 派生 Arrow schema，因此合法空输入也能创建有明确字段的空表。operator 异常、schema 错误和写入不确定性原样失败，不自动重试或转换成业务通过。

完整可运行示例在 `examples/forge_text_map/`。
