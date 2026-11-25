---
name: dataskill
description: 在 DemiFlow 数据工作区渐进式发现数据及可复用 Python 工具，为当前 Jupyter 单元格编写、修改和运行代码。
---

围绕用户当前选中的 cell 工作。**所有操作仅通过 `data` CLI 完成**；不要在本地 shell 执行 `python`/`python3` 或探测本地模块，Python 代码一律通过 `data cell edit` 写入当前 cell 并通过 `data cell run` 在 Jupyter 内核中执行。每次调用绑定到发起请求的 Notebook 和 cell，不能修改其他 cell 或项目文件。

1. **渐进式获取上下文（先总览，再按需展开）**：
   - 先运行 `data context` 查看紧凑总览：当前 Notebook 路径、当前 cell 源码、`allow_run`、内核环境（`kernel_python`、`sandbox`）、是否存在前置代码/输出、`catalog_summary`（数据表与工具总数）以及可展开主题（`topics`）。
   - 若 `kernel_python` 为空，提示用户在「项目设置」中配置项目业务 Python 环境；若 `sandbox.mode` 为 `readonly`，仅可写入 `sandbox.write_paths` 中列出的目录。
   - 按需展开细节：`data context --topic cell`（完整当前 cell 与引用）、`--topic before`（相邻前置代码）、`--topic output`（最近完整输出）、`--topic env`（内核与沙箱配置）、`--topic catalog`（数据字典与工具前 12 项摘要）。
2. **检索数据源与工具（先查后写原则）**：
   - `data catalog list dictionary [--query 关键词] [--offset N]` 列出或检索数据表；`data catalog show dictionary "表名或完整路径" [--version V]` 获取字段、版本、总行数和直接可用的读取语句。
   - `data catalog list tools [--query 关键词] [--offset N]` 列出或检索工具；`data catalog show tools "模块.函数名"` 获取导入路径、签名与说明（项目工具在 `data_tools/`，平台工具在 `demiflow.data_tools`）。
   - **何时用工具 vs 直接写 Cell**：遇到看图、质检、人工标注、复核等标准化交互需求时，**必须先检索 `tools`**，有现成工具优先复用（避免重复造轮子或破坏标注格式）；若有多个候选工具拿不准，用中文列出候选工具及用途请用户选择；仅做一次性简单统计/抽样/临时画图时直接写简洁 cell 代码。
   - 受限预览接口 `from demiflow.workbench.notebooks.data import sample, describe, count`：`sample(table, version=..., columns=..., offset=0, limit=20)` 单次 `limit` 上限为 **100**（最多 16 列），切勿传超过 100；总行数与字段元数据直接用 `count(...)` / `describe(...)`。
3. **编辑与内核验证**：
   - 通过标准输入把完整 Python 源码交给 `data cell edit`（如带引号的 here-document）；若用户同时修改了 cell 导致冲突，说明情况并保留用户修改。
   - 只要 `allow_run` 为 `true`，修改后**必须**运行 `data cell run` 在内核中实际执行并核对真实输出或 traceback，报错则修正重跑；结果不确定的写入不要重复执行。标注判断由用户在 cell 控件中作出。
4. **简明回复（透明说明工具使用）**：纯解释性问题直接回答；成功编辑并运行后简短说明改动与实际结果，并**明确交代是否复用了工具**（例如“已复用工具 `AllPoolReview`”或“未调用项目工具，已在当前卡片编写临时脚本”）。旧版客户端无法接收命令时，将建议源码放在单个 `python` 代码块中并说明尚未应用。
