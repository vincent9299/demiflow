# Demi Harness 插件开发交接

面向 DeepSeek 或其他 Harness 插件开发者。目标是让插件与独立 Forge Web **并行作为同一个 Demi 后端的客户端**，不复制业务实现，也不以独立 Web 的内部 JS 为协议。

## 0. 当前状态与双方分工

C1–C4 公共能力已完成分阶段 Review、接口 / 浏览器回归和原入口联调。正式 HTTP 契约为 **demiflow_forge_http_v1**，现有 `59865` 服务已更新。

插件可以开始真实联调：先读取 `/api/capabilities`，再发现项目。当前测试与部署证据见[实施验收记录](demi-public-capabilities-implementation-20261103.md)。具体 Harness 插件尚未实现 / 验收，由接手方负责；不能把公共 HTTP 通过等同于宿主 SDK 已验证。

平台侧负责公共函数、HTTP、独立 Web 与准入 / 版本 / 资源约束；插件侧负责自己的连接设置、宿主导航、展示、草稿和请求生命周期。插件开发不要修改 `demiflow` 的后端、运行数据、项目标准或服务启动配置；接口缺口反馈给平台侧处理，不能在插件侧通过直接读写 journal / 控制文件补齐。

本次不实现开发聊天、调用 Codex CLI、Review/Freeze 发布流程。A1–A4 已增加公共变化核验及独立 watcher；插件只消费这些能力，不自行建巡检器。实施证据见[工作区核验记录](demi-workspace-audit-implementation-20261103.md)。保留旧 pipeline，不修改 `training/`。

权威参考：

1. [HTTP v1 完整接口](forge-http-api.md)：端点、字段、预算、错误和迁移。
2. [通用客户端接入](forge-client-integration.md)：无 UI 流程、多端一致性与信任边界。
3. [中粒度设计](demi-public-capabilities-design-20261103.md)与[源码方案](demi-public-capabilities-source-plan-20261103.md)。

若文档与实际代码出现差异，先提出差异，不在插件里创建兼容猜测或第二套业务规则。

### 变化核验增量接口

接入前检查 capabilities 的 `audit_contract === "demiflow_forge_audit_status_v1"`，不要只根据本文假定已部署。新增 `GET /api/workspaces/{id}/audit` 与 `POST .../audit-check`，完整字段见 [HTTP 文档](forge-http-api.md#workspace-observations)。

- 插件显示报告依据、修改范围和核验时效三个独立维度，不用一个“项目安全”绿灯代替。
- `changes.audit` 更新时读取状态；即使没有新修订，也要按 due_at 显示超期。用服务端 observed_at 加本地 monotonic elapsed 计算，不假定两端系统时钟一致。
- 显式核验只提交 kind、expected_policy，以及报告类型的 digest。不能传新的 writes，也不能借运行权限关闭 scope。
- POST 会更新派生观察缓存，但不会生成新通过报告、移动任务基线或执行模型；仍不自动重发。
- 后台 watcher 与浏览器/插件生命周期分离。插件卸载只停止自己的读取，不结束平台巡检或原生运行。
- scope 的开始、原基线导入、允许范围修订、结束暂由本机 CLI 显式管理。上层 DemiForge 适配不能把当前文件重拍成旧任务基线。

独立 Web 和插件继续平行接入；没有 Codex/DeepSeek 专用字段、会话身份或聊天按钮。

## 1. 连接约定

| 项目 | 约定 |
| --- | --- |
| 服务端监听 | 当前服务器 `http://127.0.0.1:59865` |
| 用户本机入口 | 已有 SSH 转发后的 `http://127.0.0.1:55971` |
| 固定转发 | `ssh -N -L 55971:127.0.0.1:59865 devbox-codex`，不要改成其他端口 |
| 授权 | 每个 API 请求使用 `X-Forge-Token`，从用户私有连接配置获取 |
| 项目定位 | `GET /api/projects` 返回的 workspace_id；客户端键为 `(connection, workspace_id)` |
| 工作区 API | `/api/workspaces/{workspace_id}/{operation}` |
| 协议发现 | `GET /api/capabilities`，要求 `contract === "demiflow_forge_http_v1"` |

根据插件后端实际运行位置选择连接地址：在远程服务器上运行的后端用服务器 loopback；用户本机的后端用既有转发入口。插件前端的 `localhost` 是用户机器，不能假定等同于服务器。

**文档不提供真实 token。** 不将 token 写进仓库、业务工作区、页面链接、日志或聊天历史。客户端安全配置由插件负责。后端若使用固定 token 文件，平台 CLI 支持 `--token-file`；插件不自行重启服务或轮换凭据。

目前 API 不允许任意浏览器跨域访问，Web CSP 也禁止 iframe。因此：

- 宿主能打开浏览器页：直接打开现有独立面板。
- 宿主支持可信后端：插件后端调用 Demi HTTP，前端通过宿主自身通道读取结果。
- 宿主只有浏览器脚本：先提供打开面板入口，不放宽后端 CORS / CSP 来硬嵌。

可信后端仅允许配置的 Demi 地址、已声明的方法和路径，保留大小、超时、并发与错误，不做任意 URL 转发。不要把用户可以直接使用 OS 权限的开发环境误当成多租户隔离系统。

## 2. 面板组织

左侧是已登记工作区 / 项目，来源 `/api/projects`。名称只用于展示，稳定 ID 用于请求；坏项目显示不可用，不影响其他项目。不要把 Harness 的聊天 session ID 当作 workspace_id。

选中项目后的功能区：

| 功能区 | 首批内容 | 主要 API |
| --- | --- | --- |
| 项目总览 | 说明、位置、表 / Flow / 报告概况、可用操作 | context |
| 数据表 / Case | 表引用、元数据、有界分页、单条详情、图片 / 证据 | table、sample、media、table-evidence |
| Flows / 检查 | 已登记入口与配置、检查状态、接线图、节点源码与契约 | context、parameters、inspect、report、source |
| 运行记录 | 准备、启动、状态、attempt、日志、准确结果与别名 | runs、run、run-* |

独立 Web 已有同样的功能范围。插件可以采用宿主自己的组件，无需复制它的 DOM、CSS 或整份 app.js。当前不增加“与 Agent 讨论”或聊天绑定流程。

## 3. 对象与关键响应

以下列出插件需要消费的实际字段，不是完整响应 schema。示例中的 ID / 摘要是占位符，不能硬编码。

**项目列表**：`{projects: [{workspace_id, name, root, allow_runs, status, error?}], digest}`。root 供解释 / 展示，不能把它再作为远程任意路径参数。allow_runs 已包含服务开关的限制。

**上下文**：`{workspace_id, workspace, name, readme, tables, flows, parameter_files, reports, index, runs_enabled, ...}`。

- Flow：`{entry, parameters, description?, line?, diagnostic?}`。entry 例如 `flows/example.py:run`，parameters 是登记的配置路径列表。
- 表：`{alias, reference, uri, digest, binding_digest}`。`reference.ref` 是完整 DatasetRef；其中有版本、逻辑 schema 身份、物理 schema hash、行数和数据位置。不要自行重建 / 精简后用作写入依据。
- `digest` 是表控制编辑摘要，`binding_digest` 是预览身份，二者用途不同。

**表 / Case**：`table` 返回 reference、metadata、binding；`sample` 返回 version、schema_hash、row_count、columns、rows、has_more、binding。保留 `_rowid` 的字符串表示，不能先转 JavaScript Number 再拼查询，避免大整数失真。图片字段位置来自声明的 display，不根据某个业务列名硬编码。

**报告**：检查结果包含 digest、status、entry、nodes、edges、value_edges、action_edges、diagnostics、binding 等。`GET report?digest=...` 返回当前核验状态、变化原因以及原报告；以真实响应为准。

- 用报告里的节点和三类边画图，不执行图，也不从普通源码自行推断另一份 DAG。
- 节点使用自己的 id、api、kind、source、line 和 contract；允许未知但合法的新原生算子，以通用信息展示。
- 当前源码用 `source?path=...&expected_sha256=...`；封存运行源码用 run-source。不要把现在的文件冒充历史运行的源码。
- `checked` 是这份依据的静态检查结果，不是行为 / 业务质量认证。

**运行详情**：`{run_id, digest, attempt, status, state, outputs, report_digest, actions, observations, ...}`。`run?run_id=...&report=1` 额外带封存 report、attempts、current_attempt 和 original_parameters；`attempt=N` 明确查看某次历史尝试。

- 运行状态：prepared、starting、running、stopping、succeeded、failed、cancelled、interrupted。
- output 中包含 name、status、receipt / commit、result、result_digest、retained_from_attempt 等。
- output.result 非空才有经过登记的固定结果引用；result_digest 来自完整固定引用。不要用“运行 succeeded”代替提交证据或业务完成标准。
- 未测量的节点指标保持“未测量”；不能从图结构推导逐行血缘或凭空补零。

## 4. 典型接口调用顺序

### 读取和查看 Case

```text
GET /api/capabilities
GET /api/projects
GET /api/workspaces/{id}/context
GET /api/workspaces/{id}/table?alias={alias}
GET /api/workspaces/{id}/sample?alias={alias}&limit=20&expected_binding={binding.digest}
GET /api/workspaces/{id}/sample?alias={alias}&row_id={rowid}&expected_binding={binding.digest}
GET /api/workspaces/{id}/media?alias={alias}&row_id={rowid}&field_path={JSON路径}&expected_binding={binding.digest}
```

字段值做正确 URL 编码。media 返回二进制，核对 MIME 和 `X-Forge-Binding`，由插件自身传输返回给前端；不把 X-Forge-Token 放入图片 URL。

### 检查、准备、启动

```json
{"entry":"flows/example.py:run","params":"flows/example.json"}
```

上面的 body 分别用于显式 `POST inspect` 和 `POST run-prepare`。prepare 自己执行准入检查，不信任客户端传入“已通过”。初次插件只提供现有登记配置即可；可选 inputs / outputs / limits / new_outputs 按完整 HTTP 文档传递，不能自行发明 JSON 键。

准备成功后启动：

```json
{"run_id":"实际返回的ID","expected":"实际manifest摘要","expected_attempt":1}
```

此 body 用于 `POST run-start`。`1` 必须取响应的实际 attempt，不应恒定写成 1。准备不等于执行；启动由用户明确操作。

取消 / 核对提交：`POST run-cancel` 或 `POST run-reconcile`，body 是 `{run_id, expected_attempt}`。恢复使用 `POST run-resume`，body 是 `{run_id, expected, expected_attempt}`。所有字段取用户看到的那次运行；收到 conflict 后不能自动换成新 attempt 重试。

### 保存准确结果

`POST run-attach` 的 body：

```json
{"run_id":"实际ID","attempt":1,"output":"实际输出名称","alias":"用户选择的新别名","expected_result":"输出的result_digest"}
```

只创建新别名，保留准确固定引用。别名冲突要提示用户，不自动覆盖或生成无穷后缀。历史 attempt 可明确保存；没有历史 attempt 契约的旧记录只能明确传 null。现代运行传 null 会拒绝。

`run:{id}:{output}` 是当前运行结果的查询别名，不等于任意历史 attempt 的永久引用。需要准确复用旧结果时先按上述操作保存固定别名。当前查看 Case 使用 binding 固定身份，不能遇到 409 后自动换版本。

## 5. 多端同步

`GET changes` 返回 `workspace_stamp/sources/tables/reports/runs/run_revision/index` 等观察标记。

1. 每个可见项目视图只保留一条同步链；上一轮结束后约 3 秒查询，隐藏 / 卸载停止。
2. 标记变化时重新读相关列表 / 当前对象，无变化不反复抓图像、Case 或全部日志。
3. 切换项目 / 页面时取消或忽略旧请求，缓存键包含连接与 workspace ID。
4. 新运行可以出现在列表中，但不自动跳转、不覆盖草稿、不替换当前固定表版本。
5. changes 不是 CAS 摘要，不是事件日志，不保证观察到每次瞬间变化。真正写操作使用对象摘要与 attempt。
6. 索引 incomplete / pending 明确展示，不能当作空列表；刷新不自动检查、执行或修复索引。

CLI、独立 Web、插件写入的事实会互相可见。聊天历史、未保存编辑器内容和客户端表单不属于同步范围。主动向空闲对话推送消息由宿主决定，不要求 Demi 知道聊天 session。

## 6. 错误与不确定结果

失败结构为 `{code, error}`。完整映射见 HTTP 文档，插件至少区分：

- 403 unauthorized：授权失效，停止自动重试并提示重新授权；forbidden：此操作权限不足。
- 409 conflict：对象 / attempt / 固定结果变化，重新读取后由用户决定。
- 413 resource_limit：缩小读取范围或按平台预算处理，不无限重试。
- 422 operation_rejected：展示准入原因，不在插件中绕开检查。
- 503 busy：读取可以有界重试；不要自动重放写操作。
- 网络失败 / 空响应 / 非 JSON / 500：保留旧视图并标明未更新，不能渲染为“没有数据”。

**POST 失联不证明未执行。** prepare 每次创建新运行，当前没有持久幂等键。先查询事实，无法唯一对应时保留不确定性。不要让宿主通用 HTTP SDK 自动重试所有方法。

## 7. 插件自身的交付验收

- 能真实加载 / 卸载；选择两个不同工作区，不混入同名表 / Flow 的数据。
- 同一项目中，插件启动运行后独立 Web / CLI 看见同一个 run_id；反向也成立。
- 查看表、单条文本 / 图片 Case、接线图、源码、运行状态和准确结果。
- 从旧页面取消已恢复运行，收到 conflict 且新 attempt 不受影响。
- POST 已生效但响应丢失时，不自动重复操作。
- 页面隐藏 / 插件卸载后没有遗留轮询；慢响应不能覆盖新项目。
- 凭据不进入前端 URL / 日志；可信后端不开放任意 URL / shell 转发。
- 无业务名称特判；不能要求先开启独立 Web 的某个页面才能工作。

插件开发者应实际核对宿主 SDK 的入口 / Tab / 后端通道能力，不把未验证的宿主 API 写成既成事实。无需改变平台内核或复制其他 Harness 的整套代码。

## 8. 平台交付状态

已部署并实测：

- **原连接不变**：本机 55971 → 服务器 59865，原会话凭据保留。用户继续刷新原页面即可，不新增转发。
- 服务通过正式 CLI 启动，复用私有凭据文件 `/yzp/zhaozy/yangzepeng/0905/.forge/console.token`（0600）。这是部署位置，不是可提交的插件配置；文档和源码没有 token 值。
- capabilities 返回 `demiflow_forge_http_v1`；Web 与纯 HTTP 客户端消费同一接口。
- `public-capabilities-acceptance` 是隔离验收项目：纯 CLI 生成两条固定结果，继续加工成一条；Web 另跑一条，第二页面自动看到，CLI / HTTP 输出引用相同。
- 原 demiwtg、两真实 pipeline 的 fixture 及 inventory 仍可查看；保留原项目权限和快照，不运行生产数据。
- 授权失效、409 冲突、503 非 JSON / JSON null、POST 响应丢失不重放、参数慢读取与多端同步均有测试；详细日志位置见实施记录。

接手建议：先完成连接设置 → capabilities → 项目列表 → 数据表 / Flow / 运行只读呈现，再接显式检查与运行操作；最后做第 7 节的真实宿主验收。插件与独立 Web 分别维护呈现，平台接口与公共校验只有一份。
