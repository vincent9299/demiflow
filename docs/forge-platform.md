# Forge 平台发现与快照

第一阶段提供当前平台的接口事实、独立工程标准、内容身份和差异检查。接口范围由公开模块确定，模块内导出与类成员动态发现。没有手写 Dataset 方法目录，也没有业务 DAG 定义。

## 使用

```bash
demiflow forge platform list --module demiflow.data --name map --limit 10 --json
demiflow forge platform show demiflow.data:Dataset.map_prompt_async --json
demiflow forge platform snapshot --out platform.snapshot.json --json
demiflow forge platform check platform.snapshot.json --json
```

`list` 默认 50 项、最多 200 项；`--offset` 翻页。`show` 返回一个接口的完整签名、文档、来源、owner 和诊断。`list` 的 `page.total` 是筛选后的数量，`discovered_total` 是整个所选范围的数量。

`snapshot` 默认检查 `demiflow`、`demiflow.data`、`demiflow.operator_llm`、`demiflow.agent`、`demiflow.inference`、`demiflow.services`。可重复 `--module` 缩小新快照范围；不会递归导入所有子模块。默认只盘点必要依赖，`--extra llm` 等显式包含声明的可选依赖。缺少所选依赖会返回 incomplete，工具不会安装依赖。

输出目录必须已经存在。快照只新建，拒绝覆盖已有文件、符号链接和包内输出路径；完整检查后以同目录临时文件和原子链接发布。失败不发布局部快照。旧快照保留，重新绑定使用新文件。

`check` 固定使用文件内原有模块范围、extras 和预算，不接受缩小范围的参数，不修改快照、不执行测试或业务。差异默认显示 50 项，可用 `--offset/--limit` 翻页，`page.total` 和 `change_counts` 始终表示所有差异。

| 状态 | 含义 | 退出码 |
| --- | --- | --- |
| discovered / created | 接口事实已获取 / 完整快照已发布 | 0 |
| current | 声明范围内平台内容及环境元数据一致 | 0 |
| stale | 接口、包文件、规则或环境发生变化 | 1 |
| incomplete | 依赖缺失、扫描失败、超限或无法完成比较 | 1 |
| invalid | 快照或接口 ID 无效 | 1 |
| 参数错误 | 未开放的模块、非法页大小等 | 2 |

命令结果统一为 JSON，`--json` 使用缩进。子进程日志不混入结果。

## 身份与边界

当前新快照格式为 `demiflow_forge_platform_snapshot_v2`，包含原生 API 的版本化纯数据接线语义；未声明的能力保留 unknown。已有 v1 快照仍按原范围、v1 目录和 v1 标准检查，不升级或重写。`digests.standard/catalog/platform/environment/snapshot` 分别绑定工程标准、目录、整个 demiflow 包、依赖盘点与整体。公开方法不变但私有执行文件、检查器或包内资源变化，仍然 stale。源码 checkout 同时绑定 pyproject.toml；安装模式核对包与分发的对应关系。时间和绝对路径不参与平台内容身份。

工程标准保留 `demiflow/forge/standards/engineering-v1.json`，v2 快照使用 `engineering-v2.json`，加入 FL/OP 静态检查覆盖。规则各自声明验证方式与已实现覆盖；运行时业务规则仍标为后续阶段，不能因为静态检查通过就视为已运行验证。

`native_semantics.status=unknown` 表示当前平台没有被目录采集的机器可检查副作用声明。签名与文档存在不授予执行权；文档缺失、签名不可反射分别记录。声明对象无调用签名、属性不执行 getter，都不等于整个扫描失败。

`dependency_assurance=metadata_only`：盘点当前 Python、必要依赖闭包及 METADATA、RECORD 等元数据，不验证每个依赖安装文件的实际内容。direct_url 保存规范化身份的摘要；本地路径不制造差异，远端来源、端口、查询和制品哈希的变化保守地使身份变化，URL 正文及认证参数不输出。身份摘要不是防篡改签名。

导入只在最小环境的独立检查进程中发生，不构造服务、模型或业务对象。公开模块的顶层导入依然执行可信平台代码；这不是操作系统沙箱。进程组在超时、错误和正常退出后清理。用户中断也经过相同 finally 清理；父进程遭 SIGKILL 等不可捕获退出不承诺清理。

扫描预算见 [设计第 8 节](forge-stage1-design-20261101.md#8-资源与检查进程)。源文件分块哈希，目录项、文档、签名、依赖元数据、输出和执行时间均设上限；编码字节预算不是 RSS 硬限制。生成前后再次扫描平台与环境，拒绝检测到的并行修改。活动工作树并非不可变运行制品。

## 与旧 Forge 的关系

旧 `forge new/check/verify/review/freeze/run` 保持 v1 三节点约束和原有证据。新工作区使用独立的 workspace/table/flow 入口，不能借旧入口获得通过。第一阶段的动态契约来源与身份基础继续使用；第二阶段实现的静态检查与独立查看界面见 [工作区指南](forge-workspace.md)，验收及运行边界见 [第二阶段验收](forge-stage2-acceptance-20261102.md)。
