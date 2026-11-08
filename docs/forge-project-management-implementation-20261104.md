# 原项目管理与统一检查：A–D 实施记录

本轮只改平台。原 pipeline 目录就是项目；用户自行选择要新建或注册的目录。没有自动接入、搬迁或修正任何实际业务项目，没有改动 `training/`、旧 pipeline 或历史数据。既有控制台登记、令牌、服务器端口 59865 及本机 55971 转发保持原用途。

## A：新建与原地注册

- `console.create(registry, root, name=...)`：目标不存在或为空；初始化必要平台控制后登记。
- `console.add(registry, root, name=None, allow_runs=False)`：原目录必须存在；复用兼容的项目身份，缺少管理信息时添加管理信息；不导入业务代码。
- 初始根目录只新增 `workspace.json`、`.forge/`。没有业务目录模板、自动 README、默认数据位置或写权限。
- 冲突控制文件、重复身份/目录、非规范路径、符号链接及并发覆盖仍拒绝。登记不等于检查通过，也不自动开启运行权限。
- 初始化成功而登记失败时保留可恢复项目；修复登记问题后可重试 add，不删业务文件作为回滚。

## B：统一规范与实际代码边界

统一规范 2.4.0 和平台快照 v5 去掉“目录名称决定 Flow/operator”的旧假设，保留实际调用约束。旧规范 v1–v4 未重写，旧快照仍可读取；新平台不会给历史检查补发认证。

入口可以是 `recipe.py:select`，普通算子可以定义在 `processing.py`，配置可以是 `settings/small.json`。检查解析实际定义、包初始化、导入闭包、签名、字段、原生节点效果和资源约束。注册的普通函数不会因此被认证为 Flow；候选代码不通过 import 执行来发现契约。

平台保留路径边界、资源预算和可信模块保护：禁止路径穿越、符号链接、隐藏控制文件充当业务资源；阻断可信命名空间遮蔽；空目录不能冒充可接纳的本地模块。运行的封存模块也不能覆盖已加载的运行时模块。

`workspace.check_project` → 既有受限 worker → `project_check.check` → 既有 `_inspect_flow`，汇总全部声明入口/配置，保存派生报告。没有第二套规则引擎或执行器。

- 最多 32 组入口/配置；35 秒停止接纳新组，整个 worker 保持既有 60 秒上限；超限明确 incomplete。
- 汇总报告最多 2 MiB，源码复核合计最多 32 MiB；源码或配置在检查期间发生冲突时拒绝发布混合证据。
- 报告记录规范、控制摘要、入口检查摘要、源码闭包、诊断及覆盖范围，通过现有控制锁和 CAS 发布。
- 状态读取复核规范、平台、项目声明和逐入口检查；不会静默生成新通过报告。
- 已知错误显示 invalid；未知/缺声明/检查不足显示 incomplete；依据变化显示 stale。未声明代码不计为通过。
- 本轮实现静态聚合，不新增业务行为测试或人工审查工作流。因此静态通过也不等于整体认证，缺失的行为/审查证据明确保留，CLI 不返回虚假的成功退出码。
- prepare/start/resume 继续使用既有新鲜度、原生契约、范围和资源准入。页面或聚合报告不能绕开执行关口。平台不接管任意直接 Python 执行。

## C：共享入口与操作台

| 模块 | 职责与边界 |
| --- | --- |
| `console.py` | 用户指定目录的初始化/登记，控制台清单 CAS；无业务路径分支 |
| `workspace.py` | 项目控制、入口/配置、源码只读访问、公共检查调用、观察提示 |
| `project_check.py` | 有界检查汇总及新鲜度，不执行或修正业务逻辑 |
| `source.py` / `flow_check.py` / `operator_check.py` | 按实际定义与调用契约检查，不按固定顶层目录分类 |
| `runs.py` / `run_worker.py` | 复用原执行准入与 Dataset 执行；仅适配任意合法项目模块路径 |
| `standards.py` / `platform_snapshot.py` | 统一规则版本与历史绑定兼容 |
| CLI / `workspace_view.py` | 参数、授权与传输；调用同一公共函数，不另写规则 |
| `web/app.js` / `web/view.js` | 项目按钮、目录对话框、原源码查看、规范结果展示与定位 |

数据流：

```text
CLI ───────────────┐
Web / 其他客户端 → HTTP → console / workspace 公共函数
                                  ↓
                        原项目目录与既有管理事实
                                  ↓
                    受限静态检查 → 派生报告 / 诊断
                                  ↓
                     同一状态经 CLI / HTTP 读取
```

没有新服务、客户端专属状态、插件框架或额外业务接入模型。Harness 可以按 capabilities 使用新增接口，不需要平台依赖某个 Harness。

操作台：左侧“新建项目”“注册已有项目”；选项目后顶部“规范检查”。总览可点击原 Python 文件只读查看。已支持的原入口、配置、数据和历史结果继续使用现有接口；缺少受支持声明时显示缺口，不自动猜测脚本入口、不生成另一份数据配置。

观察提示按原声明、近期检查依赖和有界作者清单生成；内容真实性仍在报告读取和执行前复核。观察不扫描全部历史报告，也不能冒充整个任意目录的完整认证。

## CLI 与 HTTP

以下是通用示例，替换为用户自己选择的服务器路径：

```sh
demiflow forge console --registry /absolute/console.json create /absolute/new-project --name 新项目
demiflow forge console --registry /absolute/console.json add /absolute/existing-project
demiflow forge console --registry /absolute/console.json list
demiflow forge workspace check /absolute/existing-project --json
demiflow forge workspace check-status /absolute/existing-project --json
```

接口沿用 HTTP v1 鉴权与错误协议：

| 路径 | 请求 | 结果 |
| --- | --- | --- |
| `POST /api/project-create` | `{root, name}` | 创建并登记；仅控制台服务 |
| `POST /api/project-register` | `{root, name?}` | 原地登记；仅控制台服务 |
| `POST /api/workspaces/{id}/project-check` | `{}` | 统一检查，无规则覆盖参数 |
| `GET /api/workspaces/{id}/project-status` | 无参数 | 报告与新鲜度 |

完整协议见 [HTTP 文档](forge-http-api.md)。单项目查看服务不提供全局项目登记权限；新接口不接受隐式开启运行或跳过规则。

## D：验收与 Review

新增用例位于 `test_forge_project_management.py`、`test_forge_project_checks.py`、`test_forge_project_http.py`、`test_forge_project_browser.py`。

| 验收项 | 证据 |
| --- | --- |
| 新建只产生管理文件 | CLI、公共函数、真实浏览器均断言根目录仅两个管理项 |
| 注册保留原代码 | 故意含 import 即报错的原脚本；注册、查看、检查后字节不变 |
| 不强迫改业务布局 | `recipe.py` / `processing.py` / `settings/small.json` 静态通过并原生执行出预期两条结果 |
| 规则不可由客户端放宽 | HTTP 拒绝 skip_rules 等额外字段；检查、准备运行均拒绝 subprocess 效果 |
| 过期检查不能运行 | 修改源码后报告 stale，已准备运行 start 拒绝 |
| CLI / HTTP 同一事实 | HTTP 登记后 CLI 可见；无 UI 检查得到同样缺口与非零退出码 |
| 不伪造完整合规 | 未声明项目 incomplete；静态通过仍列出缺失行为与审查证据 |
| 历史规模与资源边界 | 150 份索引报告仍可分页和观察；旧记录不重写 |
| 实际页面 | Chromium 实际点击创建、注册、读源码、检查通过与失败，保存截图 |

逐阶段 Review 已修复：重复检查发布缺少 CAS 旧摘要、顶层模块运行时导入名称错误、空目录误当本地 import、默认数据目录残留、观察重新引入 128 份历史报告上限、空清单无法自动发现 CLI 登记，以及把一条规则失败扩大成所有规则失败的误报。示例的显式数据声明恢复相对位置，避免复制测试工作区后误指向原样例目录。没有文件位置的平台诊断也可正常显示。

真实 Chromium 截图（使用隔离项目，没有加入正式登记）：

- [选择目录新建项目](assets/forge-project-management-20261104/create-project.png)
- [原地注册并查看原源码](assets/forge-project-management-20261104/registered-project.png)
- [未找到受支持声明](assets/forge-project-management-20261104/project-check.png)
- [静态检查通过，其他证据仍未验证](assets/forge-project-management-20261104/static-check-passed.png)
- [统一规则定位原文件问题](assets/forge-project-management-20261104/violation-check.png)
- [原端口上的正式控制台](assets/forge-project-management-20261104/formal-console.png)

### 最终验证记录

环境使用既有 Python、pytest、Chromium；未安装或修改共享依赖。

1. 新项目管理初测：5 passed。新增检查、CLI/HTTP 和浏览器闭环初测：18 passed。
2. 扩展回归共 165 项：149 passed，5 failed，11 个初始化错误。5 项失败分别是新增 capability 字段的旧断言，以及采集 fixture 复制后绝对数据路径失效；断言/fixture 已修正。11 个初始化错误均检测到其他并行任务新增 `services/batching.py`、`services/pplx_fast.py`，使测试平台快照过期。保留了这些外部修改，未削弱快照检查。
3. 对最终源码做一致性哈希后复制到隔离平台目录，避免并行编辑干扰。契约、HTTP、空控制台 CLI 自动同步、采集、独立库存 Flow、算子基础、目录角色与登记测试：**94 passed，198.73s**。原生运行（包含前述 11 个初始化错误的用例）：**16 passed，380.46s**。上述所有失败/错误项均完成修正后的复测。
4. 核对隔离测试副本与待上线 `demiflow/forge`，无文件差异。`git diff --check` 通过。逐文件与本任务开始前备份比较；原 dirty 修改保留，没有修改业务仓或 `training/`。
5. 仅替换原查看服务，沿用原命令、原登记文件、原 token 文件和 **127.0.0.1:59865**。原进程 1438，当前进程 24743。登记文件与令牌内容哈希保持一致；原本机 **55971 → 59865** 转发无需调整。
6. 对更新后的正式服务做真实 Chromium 只读验收：仍只有原来 1 个项目；新建/注册按钮可见；新建目录对话框可打开并取消；规范检查及运行记录可打开；没有 JS 异常或 HTTP 错误。没有新建、注册、执行、重新绑定或修正实际业务项目。

这不是“整个仓库所有测试通过”的声明；本轮验证范围为变更相关的项目管理、统一静态检查、已有执行/巡检/接口兼容和浏览器交互。全量仓库仍有其他并行开发。
