# 原生算子工作区示例

这是可重复运行的机制示例，不代表业务质量验证：3 条 case 经 `flat_map` 展开为 6 条，再由普通异步 actor 标注，物化后写入固定版本 Lance 表。

接线仅在 `flows/expand.py`；参数在同目录 `demo.json`；普通函数和 actor 在 `operators/variants.py`，不继承平台基类。修改 `prefix` 即可使用同一份接线做另一组实验。

创建并执行：`python examples/forge_operator_foundation/make_demo.py /tmp/native-operator-demo`。目标工作区必须尚不存在。

打开：`python -m demiflow workbench view /tmp/native-operator-demo`。浏览器依次进入“运行记录”、选中一次运行、点击 `flat_map` 或 `map_async` 节点查看真实计数，再点击“查看结果”逐条看 Case。文本和结构化字段、固定表版本、封存源码可独立查看，无需 Notebook 或 agent。

通过 SSH 使用时，将服务器输出的端口转发到本机：`ssh -N -L <本机端口>:127.0.0.1:<服务端端口> <服务器>`，在本机使用同一访问 token。开启新实验操作需在 view 命令显式加 `--enable-runs`；只读查看不执行算子。
