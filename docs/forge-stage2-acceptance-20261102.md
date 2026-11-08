# Forge 第二阶段实施与验收

日期：2026-11-02。状态：第二阶段实现及本阶段验收通过。用户已通过设计并要求先实现契约检查闭环、再接入工作区和可视化。本文记录实际实现；业务 Flow 的执行验证、模型节点、Review 写回和生产放行不属于本阶段。

## 实施结构

| 所在层 | 实现与边界 |
| --- | --- |
| 原生 API | `data/flow_contract.py` 提供纯数据接线声明与字段传播，声明贴在真实函数上并返回原函数。`callable_contract.py` 抽出原有参数绑定规则，与原生 field-bound map 共用。 |
| 平台发现 | 动态发现真实公开签名、来源和声明，发布 platform/catalog v2 与 engineering v2。旧 v1 快照保留原版本语义。 |
| 静态检查 | `forge/source.py` 管理有限源码与符号闭包；`operator_check.py` 检查普通函数/actor 的支持范围；`flow_check.py` 解析受约束的平台接线，产出不可执行的派生报告。没有业务算子注册、Dataset 包装或第二套执行器。 |
| 工作区 | `forge/workspace.py` 管理位置、原生 DatasetRef 包装、参数绑定、报告及失效检查；CLI 和 UI 共用该入口。 |
| 原生预览 | `lance/preview.py` 在有限分配的独立 worker 中读取真实固定版本表和本地图片；不导入候选代码。 |
| 可视化 | `forge/workspace_view.py` 与打包的 web 资源提供本地查看服务。图来自同一报告，表/case 来自真实固定版本；离线导出不含样本。 |

原有 dirty 文件保护：启动前记录基线；实际修改限于上述原生契约接入、Forge、打包配置、相关测试/文档及新示例。原先模型、服务、Notebook、业务仓和生产数据没有被本阶段修改或重新部署。

## 验收矩阵与证据位置

以下是设计 §10.4 的 16 个验收组。测试既断言结果，也检查关键诊断、图/字段或没有副作用；具体测试代码是可复查依据。

| 验收组 | 实际覆盖 | 主要证据 |
| --- | --- | --- |
| 合法组合 | A/B、同一接线不同参数、多输入 join、多 map/filter、helper 多次展开、有限配置分支/循环、独立输出 | `test_forge_flow_checks.py` 的 example、config_branches；真实工作区两份 7 节点 / 6 边报告 |
| 来源与绑定 | import/函数/模块别名、转导入真实归属、缺参/重叠、默认值/包初始化、同步/异步/actor、构造参数和 scope | flow_checks 的 wiring_breakages、operator_subset、package_initialization、module_function_alias |
| 字段与端口 | 存在性/null 矩阵、字段式/整行 map、output/outputs、列覆盖、显式返回矛盾、未知类型、空表写入的原生失败语义 | flow_checks；native_semantics 的 field_map、required_and_null_matrix、empty_write |
| 关系语义 | inner/left/semi/anti、重复/null/缺失键、列碰撞、物化前后缺失字段；不假定排序或唯一性 | native_semantics 的真实 native join fixtures，显式受限 local_execution |
| 调用闭包 | 直接/别名/helper/actor 内隐藏 Dataset、外部依赖、动态 import、未知对象和方法、未覆盖 AST | flow_checks 的 helper_alias、operator_subset、actor_helper、initialization |
| 支持范围 | 普通条件、字符串、容器、循环、函数别名、helper、纯计算 actor；未声明第三方能力 incomplete | flow_checks 的 operator_subset；未知语义与 unknown-call 破坏用例 |
| 生命周期/资源 | 初始化资源、非法并发、scope/hook 与同步节点不匹配、共享输入及容器间接修改；纯计算 actor 合法 | flow_checks 的 borrowed_mutation、actor_helper、operator_subset；报告 resource_runtime=not_run |
| 异常策略 | broad catch 正常返回与 finally 抑制阻断；重抛合法；原生 catch 效果可见；真实业务错误语义不冒充已验证 | flow_checks 的 operator_breakages、broad_reraise、actor_helper |
| 动作/提交 | 未消费支路、无终结动作、同目标多写、输入输出交叠、只读目标、独立输出、stream 需物化、checkpoint 未覆盖阻断 | flow_checks；workspace 的 readonly_locations；async checkpoint effects 用例 |
| 原生声明一致 | 相同语义的新名称无需新增 Forge 方法名分支；缺失/新类别 incomplete；声明端口必须存在且角色匹配；纯规则与原生结果一致 | flow_checks 的 same_semantics、unknown_semantics；native_semantics；动态发现回归 |
| 身份 | 参数、helper、表位置/版本与平台身份；工作区移动；检查中并发修改；无关 Flow 不导致失效；旧报告保留 | workspace 的 report_binding、concurrent_parameter_change；platform_snapshot 的 source/dependency drift |
| 不执行候选 | 模块/包/类体/默认值/注解/装饰器/构造器探针，动态调用与原生终结动作均不执行 | flow_checks 探针；CLI inspect 及浏览器重新检查后 planned outputs 不存在 |
| 预算与失败 | 文件/字节/AST/调用/节点/边小预算，报告损坏、磁盘失败、存储配额，worker 超时/取消/进程组清理 | flow_checks budgets；workspace quota/tamper/disk；platform_discovery timeout/sigterm |
| 图/报告一致 | CLI/Python/UI 同一报告摘要，7 节点 6 边、A/B、源码、折叠/导出、部分失败图没有整体通过 | workspace API/CLI 对照；Chromium 实际节点/边计数、检查摘要、离线导出 |
| 表/case | 真实三行图文 fixture、投影分页/详情、明确业务键、重复/缺失匹配、大字段、大图片、路径/符号链接、队列准入 | workspace 真实读接口；Chromium 真实图文/源码/配置对比；服务 Host/Origin/token 与过载拒绝 |
| 兼容/打包 | v1 快照与旧生命周期、直接 Dataset API、wheel/sdist 资源、独立安装且仓库外 CLI、旧入口拒绝新工作区 | 原有 Forge 全套回归；native_semantics；隔离安装的 package evidence |

## 测试执行与范围

相关 pytest 共 186 项通过：主回归 185 项（419.02 秒），输出目标不可隐式重定位另补验 1 项。主回归含旧 Forge 生命周期/快照、新契约/工作区、原生 API 和实际 Chromium 操作。wheel/sdist 与仓库外独立安装验证通过。最终计数、测试文件哈希和实际产物身份记录在 [验收身份 JSON](forge-stage2-demo-20261102.json)。主回归命令如下；浏览器测试使用已有 Chromium，不安装或修改共享环境。

```bash
PYTHONDONTWRITEBYTECODE=1 PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 \
OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 \
python -B -m pytest -q -p no:cacheprovider -p pytest_asyncio.plugin \
  tests/test_forge_*.py tests/test_dataset_api_contract.py tests/test_data_module.py \
  -k 'not test_module_readers_join_and_write_configured_lance_versions'
```

这里排除的单项曾在默认原生内存准入中等待；没有调整容器内其他任务或放松平台准入。该项的原测试断言另在既有 `local_execution` 的显式小预算下执行：1 worker/1 partition、128 MiB pool、2 GiB RSS 上限、有限准入/执行期限。其读取固定版本、join、materialize、create/append/overwrite 和空表断言均按原代码验证。本文不把这一结果说成“默认资源配置下通过”，也不宣称整个仓库的所有测试在本次运行过。

wheel/sdist 在临时源码副本中构建；新建独立安装目录安装 wheel，复用依赖仅为只读路径。仓库外验证实际导入该 wheel、创建工作区、运行 CLI，以及内置标准、网页资源完整。没有覆盖共享 Python 环境。

## 真实用户效果

可复现生成器为 `examples/forge_workspace/make_demo.py`，完整用法见 [工作区指南](forge-workspace.md)。演示和浏览器证据包括：

- 不借助 agent 或 Notebook 查看项目用途、数据位置与固定版本表。
- 看 c01 题目及真实本地三角形图片；按 case_id 查看独立准备的对照内容。
- c02 明确显示两个匹配，c03 显示未匹配，不按行号偷偷取第一条。
- 同一份 Flow 的阈值 0.80/0.90 配置分别检查，查看原生节点、输入输出字段、参数和实际算子源码。
- 导出可独立打开的离线接线报告；图和检查摘要与在线报告一致，不含表样本。

本次实际演示工作区为 `/tmp/forge-stage2-delivery-workspace`。本地查看服务由本任务启动，监听 `127.0.0.1:59865`，会话 token 不写入仓库；可用 `python -m demiflow.cli forge workspace view /tmp/forge-stage2-delivery-workspace` 重新启动。截图和离线 HTML 的绝对路径见验收身份 JSON。

## 保留的运行边界

静态 checked 仅表示本次绑定通过了已实现的结构/来源/策略检查。算子真实返回、业务语义、吞吐、清理、内存峰值、模型预算和实际写入回执仍为未运行。没有执行候选 Flow，没有创建 selected_a / selected_b 输出，没有把模型请求藏进普通算子。

报告摘要是本地内容一致性标识，不是外部签名认证。图片文件内容也不随 Lance 表版本自动固定。预览的分配/投影/批次/响应/图片/目录/并发限制与不支持项均在工作区指南中明确，不将行数限制或 Arrow 字节量冒充 RSS 保证。
