> 历史文档。当前实现见 [Forge 开发辅助方案](forge-guided-coding-redesign.md)。

# Forge v1 契约与项目边界

本文对应旧 `forge new/check/verify/review/freeze/run` 的 v1 项目协议。当前原项目管理、任意合法目录布局及只读识别见 [工作区指南](forge-workspace.md) 和 [原项目识别](forge-project-discovery.md)；下述固定骨架与三节点约束适用于 v1，不作为原项目登记要求。

`demiflow.forge` 是独立的契约驱动开发层。内置 ContractBundle 与当前 demiflow 的公开 API 共同生成 `contract.lock.json`，开发、测试、Review、Freeze 和运行都绑定这份上下文。运行时不读取 DemiForge、Demiurge 或业务仓规范。

## 首版能力

首版只有一个线性结构：单个固定版本 Lance 输入 → 一个普通同步行函数 → 单个 Lance 输出。模型、网络、外部写入、任意 DAG、跨 Pipeline 编排和自动演进不在契约内，不能通过新增 helper、注册表或隐藏模块绕开。

## 所有权

项目固定包含 `project.json`、`request.json`、`contract.lock.json`、`pipeline.py`、`manifest.json`、`operators/`、`tests/`、`run.example.json` 和 `README.md`。业务实现只能修改 `pipeline.py`、`operators/`、`tests/`、RunConfig 和 README，而且 Definition 必须继续匹配原始 Request。

`project.json` 和 `contract.lock.json` 绑定契约；Manifest 只能由 Forge 生成。额外根模块、嵌套 operator 包、未引用 operator 和非法文件类型会被拒绝。`datasets/` 与 `runs/` 是运行目录，缓存目录按契约剪枝，其他源码均参与 revision。

## 平台上下文

Forge 从当前 `data.read_lance`、`Dataset.map`、`Dataset.write_lance` 读取真实 signature、完整公开 docstring、源文件和 SHA256。上下文不是凭记忆维护的 API 说明。对象缺失、非公开、缺少文档或上下文被修改都会使检查失败。

operator 只能使用契约列出的纯计算 import。动态 import、`eval/exec`、文件/网络/模型/Dataset 调用、额外执行入口和明文凭据会被静态检查拒绝。静态检查不是任意 Python 的安全证明，业务语义仍需要行为测试和 Review。

## 证据边界

验证记录、Review 和 Freeze 默认保存在项目外的 `.demiflow/forge/<project-name>/`，不参与源码 revision。任何受管源码、测试、Request、Definition、上下文或 Manifest 变化都会使旧证据过期。

测试子进程使用最小环境、独立进程组、超时和有界日志，但不是操作系统沙箱：不禁止受测代码主动联网、读取其他文件或消耗整机资源。Forge 的真实运行只访问项目内 `datasets/`，不提供生产资源、分布式恢复或外部写入保证。
