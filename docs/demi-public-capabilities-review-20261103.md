# Demi 公共能力与多端接入：方案复审与验收

状态：方案已获用户批准并完成 C1–C4 实施；以下保留设计与复审基线，实际功能 / 测试 / 部署结论见 [实施验收记录](demi-public-capabilities-implementation-20261103.md)。

关联：[中粒度方案](demi-public-capabilities-design-20261103.md)、[源码方案](demi-public-capabilities-source-plan-20261103.md)。

## 1. 第一轮：职责与依赖

| 检出问题 | 修订结论 |
| --- | --- |
| 把“公共能力”设计成所有调用都先经过新服务，会使 CLI 依赖常驻进程 | 直接保留 workspace / runs 公共函数，HTTP 是可选入口；不新增 facade |
| 将 Web 称为公共能力，会让别的客户端复制 handler 内业务逻辑 | 将源码、参数、结果登记归回公共模块；media/evidence 提供公开入口，worker 保持执行细节 |
| 新建统一项目数据库会与 workspace / registry 双写 | workspace 持有业务事实；registry 只保存服务访问范围，index 可重建 |
| 把 DemiForge 开发规范当运行前置组件，会与平台边界相反 | Demiurge 继续独立；业务项目遵守自己的登记规范，平台不反向依赖它 |
| 普通 HTTP 消费者验证被误称为已完成插件 | C4 明确是协议与接入资料验收；具体宿主需要额外真实验收 |

## 2. 第二轮：同步与并发

| 检出问题 | 修订结论 |
| --- | --- |
| changes 的 stat 摘要被当成可用于 CAS 的全局版本 | 只作为刷新提示；内容摘要、固定表引用、attempt 才参与相应操作校验 |
| 只在 HTTP 入口核验 attempt，核验后仍可被 resume 越过 | cancel 使用带 attempt 的 RunJournal，在事务内拒绝旧 attempt；reconcile 在执行锁内校验 |
| 结果登记只传 run_id / output，会登记成页面未看过的新 attempt 结果 | 显式 attempt + fixed-ref digest；公共 attach_result 统一核验并 create-only |
| CLI 旧 cancel 命令与 UI 有不同使用意图 | 本地省略 attempt 表示调用时的当前尝试，捕获一次并绑定；Web 必须传已观察值，不自动 latest |
| 写请求断开时，把 retry=true 当成通用恢复办法 | 写操作不自动重放，先查询；prepare 无持久去重，明确不承诺 exactly-once |
| 索引更新不完整时呈现“空列表” | 展示 pending / incomplete；刷新不偷偷重建索引 |
| 原生异常被 worker 字符串包装后丢失冲突分类 | 在原生层定义执行冲突类型，Forge 转 code，经 worker 原样有界传递；不解析英文错误 |
| 默认两端聊天也实时同步 | 只同步已保存事实；Agent 下次读取可见，空闲对话主动推送属于宿主能力 |

## 3. 第三轮：接入、运行与资源

| 检出问题 | 修订结论 |
| --- | --- |
| 默认任意 Harness 都能 iframe 内嵌当前 Web | 当前 CSP 禁止嵌入；第一期打开面板或使用可信后端 HTTP 客户端，不放开通配跨域 |
| 把 allow_runs=false 说成只读 | inspect 会写报告；能力区分读取 / 检查 / 执行，并解释 OS 本地用户不受 HTTP token 约束 |
| 503 空响应使客户端 JSON 解析报错 | 服务拥塞回固定小 JSON；前端还需处理代理非 JSON / 空体 |
| 多个 Tab 各自无限轮询、不断放大 worker 数 | 沿用一页单同步链、隐藏暂停、服务固定并发 / 队列 / worker 预算；不扩成事件系统 |
| 服务每次换地址 / 临时 token，使接入与真实任务割裂 | 固定端口、可选受保护 token 文件；保持既有 SSH 入口，禁止自动换端口 |
| 把页面关闭后运行继续，扩大成主机故障恢复保证 | 只承诺已接受并由 supervisor 接管的运行与页面生命周期解耦；故障核对依赖原生证据 |
| 为 HTTP 正式化建两个等价 handler 和版本化 DTO | 保留既有路由，第一次明确 HTTP 契约；此次写前置条件收紧需同步客户端，缺失拒绝 |
| 把现有浏览器回归当作新方案已通过 | 原证据只说明现有能力；下列新矩阵在实现后逐项执行，不提前勾选 |

## 4. 第四轮：沿实际调用链复核边界条件

| 检出问题 | 修订结论 |
| --- | --- |
| `expected_attempt=None` 同时表示“没传”和“历史没有 attempt”，现代运行可被 null 降级 | 公共函数区分省略与明确 None；前者捕获一次，后者只允许真实历史格式；HTTP 必填字段，严格排除 bool 充当整数 |
| attach_result 核验 ref 后调用 attach_table，后者可能采用 catalog 中的另一套逻辑 schema 身份 | attach_table 增加可选 expected_ref，在发布控制文件前比较完整最终 DatasetRef；不等写完再检查 |
| 将内部 helper 移出 HTTP 后，只在 HTTP 解析了 root | 新增公开读入口先核验 workspace，CLI / HTTP 共享同样的路径与工作区边界 |
| 新的 HTTP capabilities 可能又变成手写算子清单 | 只声明通用传输操作；原生算子能力仍取平台发现，不维护第二份名单 |
| 声称 token 不入日志，却在持久服务启动时打印带 token 地址 | 持久凭据模式只打印无 token 地址；保留现有临时交互启动的私有授权链接，区分二者 |

## 5. 验收矩阵

下列均为待实施后验证。公共契约测试必须同时检查输出和失败时的未授权副作用。

| ID | 场景 | 明确通过条件 |
| --- | --- | --- |
| MC-01 | 服务关闭、无浏览器 / 宿主 API | CLI 完成 context→inspect→prepare→start→show→attach，结果固定且可继续作为输入；无 HTTP 请求 |
| MC-02 | CLI / Python / HTTP 同一用例 | 固定输入的身份、契约拒绝、结果语义一致；随机 run_id / 时间不要求字节相等 |
| MC-03 | HTTP-only 部署 | API-only 无静态页面；同一套受限 API 可读 / 操作；无 Web 代码参与业务判定 |
| MC-04 | 两项目、同名 Flow / 表 | 每请求解析 ID；并发切换不会读写错项目；坏项目不拖垮其余登记 |
| MC-05 | 独立 CLI 操作→Web 更新 | 已保存源码变化使旧检查 stale；新检查 / run / 固定结果在健康连接下下一轮查询及读取完成后可见 |
| MC-06 | Web 操作→第二客户端读取 | 独立标准 HTTP 客户端及 CLI 看到同一 run_id、attempt、版本；无宿主专属全局对象 |
| MC-07 | 旧页面取消 / 核对，另一端已 resume | 返回 conflict；新 attempt 未被取消或写入恢复事件；覆盖检查与变更之间的竞态窗口，以及显式 null / bool 不得绕过现代 attempt 防护 |
| MC-08 | 双端 start / resume / 控制编辑 | 同一合法尝试至多一个 supervisor；重复 start 可读到既有状态；旧摘要 / attempt 拒绝，无重复预算授权 |
| MC-09 | 历史结果登记时新 attempt 已产生 | 只能登记请求指定且核验通过的旧固定结果；错误 digest / 版本失效 / 别名占用 / catalog 逻辑身份不同明确拒绝，且拒绝前不创建别名 |
| MC-10 | 断线发生在 POST 已生效之后 | 页面不自动重放 prepare / inspect / resume / attach；重连查询能展示事实，不伪称没执行；多个相似 prepare 无法唯一对应时明确保留不确定性 |
| MC-11 | 历史选择、草稿、慢请求 / 旧响应 | 刷新不跳最新 run / 表；草稿不丢；旧响应不覆盖新项目 / 新页面；隐藏时停止自动查询 |
| MC-12 | 索引 pending / 缺失 / cursor 过期 | 明确不完整或冲突，刷新不写修复，不把缺失信息当空项目；显式修复后可重新读取 |
| MC-13 | 认证 / Host / Origin / 随意路径 | 错 token、外国 origin、未登记 ID、越界文件、符号链接及未知参数被拒；不读取目标文件或启动 worker |
| MC-14 | 错误贯穿子进程 | attempt / 绑定冲突仍有 conflict code；CLI 保留退出语义；HTTP 409；客户端不解析错误文案 |
| MC-15 | 小预算 / 慢客户端 / 并发拥塞 | 准入前限制请求和任务；503 可理解；受限 worker 超时后清理；不因新增端增加无界队列、全表读取或原始 traceback |
| MC-16 | 凭据与启动 | 有效私有 token 文件重启后可重新认证；非法权限、所有者、符号链接、超长文件拒绝；固定端口冲突不自动改端口 |
| MC-17 | 没有 UI 与关闭 UI | 公共函数不导入 Web / Harness；接管后的运行不因关闭浏览器而中断；不会声称主机宕机也保证继续 |
| MC-18 | 已有功能回归 | 深链接、表 / 图片、图 / 源码、历史 attempt、离线导出和 wheel 静态资产仍有效；历史数据不改写 |
| MC-19 | 两真实 pipeline 与第三种 Flow | 同一契约 / 控件显示 identity、concept_images、inventory；隔离 fixture 的结果引用正确；无 pipeline 名字分支 |
| MC-20 | 保护范围 | 变更清单仅包含本方案代码 / 文档 / 必要测试；旧 pipeline、Notebook 输出、training/、历史数据及他人修改未被覆盖 |

MC-15 的测量不能只看行数：要覆盖请求 / 响应字节、解析副本、同时驻留请求、并发 worker、超时和释放。现有 9 MiB 序列化上限不是 Python 进程 RSS 上限；保留 worker 隔离与原生读取限制，超预算明确失败。

具体宿主插件未来额外验收：真实加载 / 卸载、选择项目、入口与功能区、连接 / 凭据保存、可信代理方法与资源白名单、卸载后无遗留轮询。此项不以 MC-06 的普通 HTTP 客户端测试代替，也不作为当前尚未选择宿主的公共能力实施前置条件。

## 6. Review 结论与待审批边界

经四轮源码对照，方案收敛为：补齐既有公共函数与少量 CLI 入口，收紧身份 / attempt 保护，正式化现有 HTTP，Web 继续读取同一事实。没有新增执行引擎、业务数据库、宿主依赖或命令中间层。

需用户 Review 的具体范围已明确：C1–C4；其中 API-only、可选私有 token 文件、HTTP 写前置条件收紧及接入文档为拟新增内容。具体 Harness 插件代码、开发聊天、跨域嵌入和多租户认证不在本轮实现范围。

这是设计自审结论，不替代代码 Review、资源测试或旧 P4 的最终验收。实施时每阶段对照对应矩阵，有新增证据不符则先修订设计，不用前端补丁掩盖公共规则问题。
