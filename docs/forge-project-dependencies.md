# Forge 项目依赖

业务项目以根目录的 `pyproject.toml` 作为唯一依赖声明，直接运行在显式配置的项目 Python 环境中（不创建隐藏 `.forge/venv/`，也不维护独立的 `.forge/deps.lock.json` 锁文件或手工摘要审批）。

- **A 类依赖（`[project].dependencies`）**：安装到项目显式 Python 环境中的标准 PEP 508 分发包。
- **B 类依赖（`[tool.demiflow.packages]`）**：只读的外部 Python 源码包，支持 `vendored`（受管副本位于 `packages/<包名>`）和 `local_ref`（指向宿主已授权读范围内的外部绝对包目录）。
- 无 `[tool.demiflow.packages]` 表的旧项目沿用原行为；仅使用 A 类包的新项目可通过空 `[tool.demiflow.packages]` 表启用依赖管理。

```toml
[project]
name = "example-project"
version = "0.1.0"
dependencies = ["numpy>=2"]

[tool.demiflow.packages]
research_model = { source = "local_ref", path = "/approved/research_model" }
helpers = { source = "vendored", path = "packages/helpers" }
```

## 统一 Forge CLI

```sh
demiflow forge dep list --root /project [--python /project-env/bin/python] [--json]
demiflow forge dep verify --root /project [--python /project-env/bin/python] [--json]
demiflow forge dep vendor helpers /approved/helpers --root /project [--json]
demiflow forge dep remove helpers --root /project [--json]
demiflow forge dep install --root /project --python /project-env/bin/python [--json]
```

- 可先在 `pyproject.toml` 中声明多个缺失包，再逐个执行 `demiflow forge dep vendor`；缺少 A 类环境包时仍可正常获取 B 类源码包。
- `demiflow forge dep remove <name>` 按包名解除 `pyproject.toml` 中的声明，并仅删除项目内受管副本 `packages/<name>`，绝不触碰外部 `local_ref` 源目录。
- `demiflow forge dep install` 必须由受信项目所有者在终端显式指定项目 Python 执行，受限开发沙箱内保持离线。

## B 类源码包边界与静态检查

`forge dep vendor` 与 `demiflow forge check` 复用同一份单包校验规则：

- 包必须具有合法顶层 Python 标识符名称与 `__init__.py`（所有子包同理），不与项目角色目录、标准库或已安装模块撞名；
- 仅接纳直接的常规 `.py` 源码文件（忽略正常 `__pycache__` 目录），拒绝软链、硬链、权重、数据、压缩归档、Notebook 及反向导入项目角色目录或 `demiflow`；
- 体积与数量上限：单文件 512 KiB，单包 256 文件 / 8 MiB，单项目 16 包 / 512 文件 / 16 MiB；
- 所有消费者在枚举、读取或计算外部源码哈希前，统一核验调用方授予的目录范围。

`demiflow forge check --root /project [--python /project-env/bin/python]` 实时核验 4 类问题并定位到声明项或源码行：B 类包缺失/非法/`packages/` 孤儿目录、明确的 B 类子模块缺失、传 `--python` 时的 A 类版本不符与已安装模块撞名、以及业务源码中可静态确认的缺失导入（`TYPE_CHECKING` 与 `try ... except ImportError` 仅豁免缺失诊断，导入边界继续生效；未传 `--python` 时明确标出跳过的环境核验）。

## 沙箱、评审、运行与 Notebook

- **开发与评审沙箱**：开发 Agent 可直接新建、编辑和保存 `pyproject.toml`，可正常 `import` 已声明包，OS 拒绝其直接写入 `packages/` 和 `.forge/`。受限会话中的 `demiflow forge dep vendor/remove` 经受控宿主通道执行并同回合生效；在已授权读范围内直接修改 `local_ref` 路径，下一条 `python`/`pytest` 即自动加载新版本。独立评审保持只读，使用脱敏且保留行号的 `pyproject.toml` 及包摘要，不展开大包正文，并将原文哈希与包树哈希纳入 `evidence_id`。
- **预览、受管运行与 Notebook**：控制服务留在宿主 Python，业务子进程由 `demiflow.forge.runtime` 按显式项目 Python 与单包精确加载器启动（同级目录不可见）。正式运行首次启动核对当前声明与环境，中断后即使外部包升级，恢复也严格读取原封存字节并校验原 Python 环境。Notebook 新内核读取最新核验版本，已打开内核保持启动时固定的快照版本，只读内核写保护继续生效。
