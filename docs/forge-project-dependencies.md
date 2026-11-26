# Forge 项目依赖

项目用已有的 Python 环境运行。安装的分发包由该环境解析，受控源码包由 Forge 锁定，业务代码沿用普通 Python import。向项目根目录的 pyproject.toml 添加声明时，保留原有的项目包元数据。只使用已安装依赖的项目也可声明空的 packages 表。

```toml
[project]
name = "example-project"
version = "0.1.0"
dependencies = ["numpy>=2"]

[tool.demiflow.packages]
research_model = { source = "local_ref", path = "/approved/research_model" }
helpers = { source = "vendored", path = "packages/helpers" }
```

由项目所有者在可信终端运行。先按需要安装项目的 A 类依赖，再登记依赖锁。显式解释器应具备 DemiFlow 的运行依赖。

```sh
demiflow forge dep install --root /project --python /project-env/bin/python
demiflow forge dep vendor helpers /approved/helpers --root /project --python /project-env/bin/python
demiflow forge dep sync --root /project --python /project-env/bin/python
demiflow forge dep list --root /project
demiflow forge dep verify --root /project
```

源码包应为具有 __init__.py 的正规 Python 包。文件树只接纳直接的 Python 源码。权重、配置与数据使用既有业务资源声明。发布锁的命令会核对源码散列和导入边界，遇到未登记包、冲突名称或超限会失败。多个 vendored 包可以逐个登记。最后执行 `dep sync`，确认完整声明已锁定。`dep remove` 撤销当前锁，源码、安装和历史保留。

Workbench 的项目配置需选择与锁相同的 Python。所有者通过受信终端为该项目批准 lock digest，使用 console 注册表的当前 digest 进行并发校验。所有命令均为 CLI，工作台当前设置继续读取该注册表。

```sh
demiflow workbench console --registry /path/to/registry.json \
    approve-dependencies WORKSPACE_ID --expected REGISTRY_DIGEST --digest LOCK_DIGEST
```

撤销工作台审批时，运行同一命令并将 `--digest LOCK_DIGEST` 换成 `--revoke`。若项目准备恢复为普通项目，也需移除 `[tool.demiflow.packages]` 声明。

启用项目依赖后，预览和运行准备使用已配置的项目解释器。预览目前使用 Linux 的受限执行后端。运行监管继续使用平台解释器。项目解释器信任范围包括其 Python 启动配置，例如环境目录中可执行的 `.pth` 内容。

Workbench 将选定的源码包固定进 run。首次启动检查当前声明和审批，已经开始的 run 恢复时继续读取原封存版本。安装包记录解释器与分发元数据，需维持已批准的环境；跨环境迁移按新 run 准备。受限开发进程使用项目的既有编辑范围，依赖和控制文件保持只读。批准的项目解释器仍由所有者管理。
