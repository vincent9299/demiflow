---
name: demiforge
description: 用 DemiFlow 公开 SDK 解读、新建或修改 Pipeline 项目中的 Flow、共享算子和 Prompt。用于查询项目个性化配置与开发规范、创建标准文件、检查源码与执行项目验证的开发任务。
---

# DemiForge 项目开发

1. **先通过 CLI 获取本项目个性化配置与规范**：运行 `demiflow forge context --root .`，查看项目根目录、个性化目录（`locations`）、授权可写路径、业务 Python 运行环境状态、已登记入口与角色目录。已有目标可加 `--path REL`；查编写范例加 `--topic flow`、`--topic operator` 或 `--topic prompt`；查精确 SDK 签名加 `--api demiflow.data:Dataset.filter`。根据出处阅读本次相关源码、参数和测试。纯解读任务到此即可回答。
2. **新建或修改标准文件**：新项目用 `demiflow forge init --root .`（接纳已有源码加 `--existing`）。新增文件用 `demiflow forge add flow NAME --root .` 或 `add operator NAME --root .`；新建 Prompt 先查 Prompt 主题，再通过 `add prompt NAME --root . --from FILE` 创建。已有文件按需求直接修改。
3. **改后自查与业务测试**：每次代码修改后，运行 `demiflow forge check --root . --path REL`（指定入口可加 `--entry REL.py:SYMBOL` 及 `--params REL.json`），根据诊断立即修正并复查。
   - **业务环境自闭环**：仅使用 `forge context` 报告的本项目业务 Python 环境在项目根目录运行 `python` / `pytest`（允许导入 `demiflow` SDK，不依赖平台宿主第三方包）。
   - 若 `forge context` 显示本项目尚未配置业务 Python 环境，**切勿**在宿主机搜索 Python/venv/pytest；完成源码修改与 `forge check` 静态检查后，明确提示用户在项目设置中配置该项目独立的 Python 解释器。
4. **严格遵守读写与沙箱边界**：读写仅限本项目根目录、授权可写目录及 `$TMPDIR`；严禁探测、列举或绕过父目录、兄弟项目、宿主机 `/tmp`、凭证文件或平台内部环境。读取项目外输入数据时，在工作台内使用 `read_input` 工具。
5. **独立评审与交付**：交付新增或实质修改的 Flow、算子、Prompt、跨管道输入或输出语义前：
   - 在终端/独立环境运行 `demiflow forge review --root . --entry REL.py:SYMBOL --goal "本次业务目标"`（复用参数，可附 `--evidence REL` / `--base REF` / `--path REL`）。
   - 在工作台开发会话中，独立评审由工作台宿主自动执行或通过宿主工具 `request_pipeline_review` 触发，不要在受限沙箱内直接跑 `demiflow forge review`。
   - 对影响交付的阻断项修正并复核；纯解读或文字调整可直接回答。
6. **登记新入口**：需向工作台公开新入口时，按权限使用 `demiflow forge add flow --root . --register --entry REL.py:SYMBOL`（可配 `--params`）；已有入口修改沿用原登记。结束时分别报告实际修改、静态检查、测试结果与尚待解决的限制。
