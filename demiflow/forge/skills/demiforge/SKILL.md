---
name: demiforge
description: 用 DemiFlow 公开 SDK 解读、新建或修改 Pipeline 项目中的 Flow、共享算子和 Prompt。用于查询项目个性化配置与开发规范、创建标准文件、检查源码与执行项目验证的开发任务。
---

# DemiForge 项目开发

1. **渐进式获取本项目上下文（先总览，再按需展开）**：
   - 先运行 `demiflow forge context --root .` 查看紧凑总览：已登记管道（含中文标题、摘要、入口路径与参数文件）、个性化目录（`locations`）、业务 Python 与沙箱配置、角色目录统计与 SDK 索引。先根据标题直接定位目标入口与参数，避免盲目全仓 grep。
   - 按需展开细节：`--topic sdk`（完整 SDK API 手册）、`--topic rules`（强制开发条款）、`--topic files`（全部角色文件清单）、`--topic flow|operator|prompt`（对应范例与正反例）、`--api SYMBOL`（单个 API 精确签名）、`--path REL`（聚焦文件及下游消费者）。纯解读任务到此即可回答。
2. **新建或修改标准文件**：标准角色目录为 `flows/`、`operators/`、`prompts/`、`configs/`、`data_tools/`、`notebooks/`——Flow 运行参数与业务/部署配置统一放在 `configs/`（不新建 `parameters/`，已迁移入口清理 `parameters/` 中的未登记旧文件），默认运行产物平铺在 `data/`（活跃参数不再将默认输出指向历史 `runs/`，历史 `runs/` 仅作只读保留），可读运行日志在 `logs/`，内部状态在 `.forge/`。项目根目录保持洁净，不散落业务配置（`*.json`/`*.yaml`）、Notebook（`*.ipynb`，应移入 `notebooks/` 并保留输出）或运行脚本（`*.py`）；`flows/`、`operators/`、`configs/`、`prompts/` 仅保留活跃入口、已声明复用模块或有效引用文件，废弃的非数据文件（旧脚本、旧配置、旧参数、旧 Prompt）应直接删除清理，切勿迁入或打包（`.tar.gz`/`.zip`）到 `docs/`；Notebook 查看/分析/标注工具统一放 `data_tools/` 并声明 `__demiflow_data_tools__`。新项目用 `demiflow forge init --root .`（接纳已有源码加 `--existing`）。新增文件用 `demiflow forge add flow NAME --root .` 或 `add operator NAME --root .`；新建 Prompt 先查 Prompt 主题，再通过 `add prompt NAME --root . --from FILE` 创建。已有文件按需求直接修改。
3. **改后自查与业务测试**：每次代码修改后，运行 `demiflow forge check --root . --path REL`（全项目验收运行 `demiflow forge check --root . --json`，会同时检查全项目 DF-02 目录整洁度；指定入口可加 `--entry REL.py:SYMBOL` 及 `--params REL.json`），根据诊断立即修正并复查。
   - **静态检查边界速查（切勿写临时 `_probe.py` 循环盲探检查器）**：Flow 接线层仅支持公开 `demiflow` API 与对静态参数/配置的纯 Python 校验（`if`/`raise`、`and`/`or`、`in`/`not in`、算术、静态推导式、纯内置函数及 `dict`/`str` 只读方法），严禁在 Flow 接线期做任意磁盘 I/O 或用运行时数据决定拓扑；行算子仅允许纯计算/数据依赖（`json`/`hashlib`/`math`/`re`/`html`/`io`/`pathlib`/`typing`/`pyarrow`/`PIL`/`torch` 等），严禁导入宿主机副作用模块（`os`/`time`/`subprocess`/`socket`/`urllib`/`requests`/`shutil` 等，触发 `OP-05`）。项目源码须**自包含且仅依赖公开 SDK**，严禁导入 `demiflow.execution.<内部子模块>` 等平台内部实现，严禁跨项目 `import` 其他业务项目源码（触发 `DF-02`）。
   - **业务环境自闭环**：仅使用 `forge context` 报告的本项目业务 Python 环境在项目根目录运行 `python` / `pytest`（允许导入 `demiflow` SDK，不依赖平台宿主第三方包）。
   - 若 `forge context` 显示本项目尚未配置业务 Python 环境，**切勿**在宿主机搜索 Python/venv/pytest；完成源码修改与 `forge check` 静态检查后，明确提示用户在项目设置中配置该项目独立的 Python 解释器。
4. **严格遵守读写与沙箱边界**：读写仅限本项目根目录、授权可写目录及 `$TMPDIR`（**临时文件必须写 `$TMPDIR`，不要写宿主机 `/tmp`**）；严禁探测、列举或绕过父目录、兄弟项目、宿主机 `/tmp`、凭证文件或平台内部环境。项目运行凭证遵循 `.env`（本地真实密钥，不入 Git、受保护不可写）+ `.env.example`（入库变量名模板，值为空）规范。受限沙箱内 `.forge/`、`tables/`、`workspace.json`、`.env` 与项目根目录受宿主保护（勿直接跑 `forge init`）：如需创建缺失的标准目录、登记/更新 Flow 参数绑定、绑定输入 Lance 表、同步根目录 `.env.example` 模板（`sync_env_example`），或将根目录散落的配置/Notebook/查看工具归位至标准目录、清理废弃的非数据文件，调用宿主工具 `manage_project_layout`（支持 `ensure_dirs`、`register_flow`、`bind_params`、`bind_table`、`sync_env_example`、`move_path`、`remove_path`；其中 `move_path` 完整保留 Notebook 输出，`remove_path` 严禁删除 Notebook、业务数据、`runs/` 及已登记文件）。读取项目外输入数据时，在工作台内使用 `read_input` 工具（`list`/`find` 查目录，`read` 用 `offset`/`bytes` 读文本，`inspect` 仅用于 Lance/Parquet 表结构与样本）。
5. **独立评审与交付**：交付新增或实质修改的 Flow、算子、Prompt、跨管道输入或输出语义前：
   - 在终端/独立环境运行 `demiflow forge review --root . --entry REL.py:SYMBOL --goal "本次业务目标"`（复用参数，可附 `--evidence REL` / `--base REF` / `--path REL`）。
   - 在工作台开发会话中，独立评审由工作台宿主自动执行或通过宿主工具 `request_pipeline_review` 触发，不要在受限沙箱内直接跑 `demiflow forge review`。
   - 对影响交付的阻断项修正并复核；纯解读或文字调整可直接回答。
6. **登记新入口**：需向工作台公开新入口时，按权限使用 `demiflow forge add flow --root . --register --entry REL.py:SYMBOL`（可配 `--params`）；已有入口修改沿用原登记。结束时分别报告实际修改、静态检查、测试结果与尚待解决的限制。
