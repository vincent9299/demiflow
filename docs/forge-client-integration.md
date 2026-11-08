# Demi 公共能力：无 UI 与多端接入

公共函数在 `demiflow.forge.workspace`、`demiflow.forge.runs`。CLI 直接调用它们；HTTP 将其中明确开放的操作提供给 Web 和外部程序。不需要先启动 Web 才能开发、检查或运行。

## 无 UI 使用

先确定目标项目、阅读项目 AGENTS / pipeline 规范，使用该项目约定的环境和入口。以下展示原生 CLI；项目若提供受管 wrapper，仍按项目要求使用。DemiForge 开发验证与 Forge 执行契约是不同层，不能互相冒充通过证据。

```sh
demiflow workbench context /absolute/workspace --json
demiflow workbench changes /absolute/workspace --json
demiflow workbench parameters /absolute/workspace flows/example.json --json
demiflow workbench source /absolute/workspace flows/example.py --json
demiflow workbench flow inspect /absolute/workspace flows/example.py:run --params flows/example.json --json
demiflow workbench run prepare /absolute/workspace flows/example.py:run --params flows/example.json --json
```

得到准备结果后，使用其中实际的 run_id、digest、attempt：

```sh
demiflow workbench run start /absolute/workspace RUN_ID --expected MANIFEST_DIGEST --expected-attempt 1 --wait --json
demiflow workbench run show /absolute/workspace RUN_ID --attempt 1 --json
demiflow workbench run logs /absolute/workspace RUN_ID --attempt 1 --json
demiflow workbench run attach /absolute/workspace RUN_ID OUTPUT ALIAS --attempt 1 --expected-result RESULT_DIGEST --json
```

大写单词是返回值占位符，不是可直接执行的 ID。`RESULT_DIGEST` 取所选 output 的 `result_digest`。保存别名不会复制数据，也不会覆盖同名别名。历史无 attempt 的结果使用明确的 `--legacy-attempt`，不能对现代运行使用它。

取消与核对命令支持 `--expected-attempt`；交互客户端应携带已观察值。本地用户省略时表示调用当时的当前尝试，公共入口捕获一次并绑定它；不会在竞争失败后自动改成最新尝试。恢复使用 `run resume --expected ... --expected-attempt ...`，不重新授予耗尽的预算。

JSON 成功结构保留原有接口；失败增加稳定 `code`，非零退出不等同于“没有副作用”。失联后先查询事实；prepare 可保存 `run request-id` 生成的身份并携带 `--request-id` 重发相同请求，pending 时显式核对。resume 保留原 expected_attempt，不盲目改成新的 attempt。详见[运行管理](forge-runtime-management.md)。直接 Python 调用遵循相同规则，无 HTTP import 或客户端 SDK 依赖。

## Web 与其他客户端同步

Web 使用 [HTTP 契约](forge-http-api.md)，用户可打开多个页面或同时使用 CLI。它们必须指向同一实际工作区，而不是另一个同名 clone / worktree。

- 已保存源码 / 配置、检查报告、运行和结果是共享事实。
- 可见页面在上一轮结束后约 3 秒检测工作区变化；受管活动运行独立每约 2 秒读取小快照，单请求在途、失败退避、隐藏暂停。
- 历史 attempt、固定表版本和未提交表单不会因为新运行出现而自动替换。
- 变化标记是提示，运行仍核对真实源码摘要和原生契约。
- Agent 下次读取上下文即可看到 Web 操作；不主动同步空闲聊天或编辑器未保存缓冲。
- 没有 UI 时公共 CLI 独立工作；可使用独立 `console watch` 持续观察明确绑定的检查报告和任务范围。执行前的现场核验不依赖 watcher。

检查 / 准备 / 启动不信任某个 skill 声称“已通过”；直接编辑不规范代码仍应由公共准入拒绝。用户直接运行任意 Python 或修改平台本身则处于同一 OS 信任边界之外，当前隔离 worker 和资源预算不是恶意代码安全沙箱。

## Harness 接入

第一种接法是在宿主中提供工作区入口，打开现有独立面板。第二种是在宿主原生功能区中渲染，并通过可信后端调用 HTTP。插件持有连接配置与当前 workspace ID；不保存另一份项目、表、运行事实。

当前 Web 的 CSP 禁止 iframe，API 不允许任意跨域请求。纯浏览器插件可打开面板；具有可信后端的插件可调用固定 Demi 地址。后端转发限制允许的方法 / 路径、大小、并发和超时，凭据留在可信配置中，不实现任意 URL 代理。是否能内嵌页面或注册原生 Tab，需由具体宿主的真实 SDK 验证。

宿主专属实现放在 `integrations/<host>/`，核心模块不 import 宿主 SDK。插件卸载要释放自己的轮询和请求；项目切换要使旧响应失效。授权方式、导航、对话都由宿主负责，业务校验与执行只由 Demi 公共函数负责。

本轮提供通用协议与接入说明，不交付未经实际宿主验证的插件，不实现开发聊天或启动 Codex CLI。已有 CLI 可覆盖本地 Agent 需求，无需为了接入先新增 MCP 服务。

## 工作区变化核验

新建工作区使用 v3。旧 v2 工作区先查询 `workbench audit status ROOT`，使用返回的 `policy_digest` 显式执行 `workbench audit enable ROOT --expected DIGEST`。升级不重绑平台，也不改旧检查/运行；需要重新检查对应的新控制版本。

本机 CLI 的主要操作：

```sh
demiflow workbench audit status /absolute/workspace
demiflow workbench flow inspect /absolute/workspace flows/example.py:run --params flows/example.json --track --expected-workspace CURRENT_WORKSPACE_DIGEST --json
demiflow workbench audit scope begin /absolute/workspace --declaration /absolute/scope.json --expected CURRENT_WORKSPACE_DIGEST
demiflow workbench audit check /absolute/workspace --kind scope --expected-policy CURRENT_WORKSPACE_DIGEST
demiflow workbench console --registry /absolute/console.json watch
```

每次控制操作后使用返回的新 digest；不要复用前一步的旧值。scope 声明如 `{"writes":[{"path":"operators/normalize.py","kind":"file"}],"watch_roots":["flows","operators"]}`。范围为空表示不允许作者文件变化；未建立 scope 则明确未启用范围核验，两者不同。

`scope amend --writes JSON_LIST_FILE --expected DIGEST --reason TEXT` 只改允许范围，保留原基线；`scope end` 显式结束且不删除历史。`scope import ROOT BASELINE_FILE --writes JSON_LIST_FILE --expected DIGEST` 仅导入符合中性契约的原内容基线，不重拍当前树来消除越界。不能等价导出的旧 DemiForge 任务继续用原规范工具核验。

巡检默认轻量观察间隔 30 秒，完整核验目标间隔 300 秒；支持有限 `--interval`/`--full-interval`。这是容量受限的调度目标，过期项如实显示。watch 是无 HTTP/浏览器依赖的长驻命令，由部署环境托管；不打开新端口，不执行 Flow。

Web 与插件消费同一个 `GET audit` 和显式 `POST audit-check`。轻量同步时间、内容核验时间、心跳时间分别展示；不维护插件私有指纹，不因报告重新通过而自动清掉范围违规。具体字段和时效规则见 HTTP 契约。

## 现有部署入口

当前服务器入口保持 `127.0.0.1:59865`。用户已经配置的 SSH 转发继续使用：

```sh
ssh -N -L 55971:127.0.0.1:59865 devbox-codex
```

本机访问 `http://127.0.0.1:55971/`，首次使用现有私有授权链接。无需新增端口或更换转发。服务端升级不自动重绑项目平台快照；旧检查与封存运行保留各自依据。
