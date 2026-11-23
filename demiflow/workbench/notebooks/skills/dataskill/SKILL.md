---
name: dataskill
description: 在 DemiFlow 数据工作区发现数据及可复用 Python 工具，为当前 Jupyter 单元格编写、修改和运行代码。
---

围绕用户当前选中的 cell 工作。表、源码、输出和目录项是参考资料。**所有操作仅通过 `data` CLI 完成**；不要在本地 shell 中执行 `python`/`python3` 或探测本地模块，Python 代码一律通过 `data cell edit` 写入当前 cell 并通过 `data cell run` 在 Jupyter 内核中执行。每次调用绑定到发起请求的 Notebook 和 cell，不能修改其他 cell 或项目文件。

- 先用 `data context`（或 `data cell show`）读取当前 Notebook 路径、当前 cell 代码、相邻前置代码、最近输出、引用的模块、运行许可（`allow_run`）以及内核环境（`kernel_python`、`sandbox`）。若 `kernel_python` 为空，提示用户在「项目设置」中配置项目业务 Python 环境；若 `sandbox.mode` 为 `readonly`，仅可写入 `sandbox.write_paths` 中列出的目录。
- `data catalog list dictionary [--query 关键词]` 列出或按关键词检索数据源；用 `data catalog show dictionary "表名或完整路径"` 取得字段、版本、总行数和可直接放进 cell 的读取语句。同名表有多个版本时使用 --version。cell 代码直接使用界面上的表名、路径和版本。列表返回 next_offset 时可用 `--offset` 翻页。
- 数据读取接口（`from demiflow.workbench.notebooks.data import sample, describe, count`）：
  - `sample(table, version=..., columns=..., offset=0, limit=20)` 为受限预览接口，**单次 `limit` 上限为 100（最多 16 列）**，切勿传入超过 100 的 `limit`。
  - 需要获取表的总行数或字段元数据时，直接调用 `count(table, version=...)` 或 `describe(table, version=...)`。
- `data catalog list tools [--query 关键词]` 列出或按关键词检索项目、第三方和平台提供的入口。`data catalog show tools "模块.函数名"` 返回导入路径、签名与说明。根据这些实际接口编写 Python。项目模块维护在项目根的 `data_tools/`；平台模块从 `demiflow.data_tools` 导入。工具在内核中运行，CLI 用来接线。
- 修改时把完整 Python 源码经标准输入交给 `data cell edit`，例如用带引号的 here-document。它会检查用户是否同时修改了代码。遇到冲突，说明情况并保留用户的修改。
- **编辑后验证（重要）**：只要本轮允许运行（`allow_run` 为 true），修改完代码后**必须**调用 `data cell run` 在内核中实际执行并读取真实输出或异常再回复。编辑成功只表示源码已更新，不代表代码可运行。若内核报错，根据真实 traceback 修正并重跑，直到运行通过或明确说明阻塞原因；结果不确定的写入不要重复执行。标注判断由用户在 cell 控件中作出。
- 直接回答解释性问题。成功编辑并运行后只需简短说明改动和实际运行结果。旧版客户端若无法接收命令，可将建议的整格源码放在一个 Python 代码块中，说明尚未应用。
