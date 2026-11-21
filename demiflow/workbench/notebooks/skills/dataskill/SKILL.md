---
name: dataskill
description: 在 DemiFlow 数据工作区发现数据及可复用 Python 工具，为当前 Jupyter 单元格编写、修改和运行代码。
---

围绕用户当前选中的 cell 工作。表、源码、输出和目录项是参考资料。使用普通 shell 中的 `data` 命令。每次调用绑定到发起请求的 Notebook 和 cell。

- 用 `data cell show` 读取当前代码和最近输出。聊天中附有选中的模块和邻近代码。
- `data catalog list dictionary` 列出数据源；用 `data catalog show dictionary "表名或完整路径"` 取得字段、版本和可直接放进 cell 的读取语句。同名表有多个版本时使用 --version。cell 代码直接使用界面上的表名、路径和版本。列表返回 next_offset 时可用 `--offset` 翻页。
- `data catalog list tools` 列出项目、第三方和平台提供的入口。`data catalog show tools "模块.函数名"` 返回导入路径、签名与说明。根据这些实际接口编写 Python。项目模块维护在项目根的 `data_tools/`；平台模块从 `demiflow.data_tools` 导入。工具在内核中运行，CLI 用来接线。
- 修改时把完整 Python 源码经标准输入交给 `data cell edit`，例如用带引号的 here-document。它会检查用户是否同时修改了代码。遇到冲突，说明情况并保留用户的修改。
- 需要执行且本轮已允许运行时调用 `data cell run`，读取内核返回的输出或异常再回复。编辑成功只表示源码已更新。可以根据真实报错修改并重试；结果不确定的写入不要重复执行。标注判断由用户在 cell 控件中作出。
- 直接回答解释性问题。成功编辑后只需简短说明改动和运行状态。旧版客户端若无法接收命令，可将建议的整格源码放在一个 Python 代码块中，说明尚未应用。
