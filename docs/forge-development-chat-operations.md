# DemiFlow 开发对话与运行态组合部署

2026-11-19 试用已上线：`http://localhost:55972/` →“Forge 开发试用”→“开发”，实际运行 `fix/prompt-replay-delivery-20261118` / `_demiflow/worktrees/gpt_prompt_replay_20261118` 的 `20581ae`。真实连续两轮开发及重启后接受、原项目视图和历史保护均已核验；[试用交付记录](forge-development-harness-delivery-20261119.md)包含测试、限制和回退。采用原 token、registry 与分析数据库，当前开发授权配置位于 `_demiflow/shared-review/forge-harness-final-delivery-20261118/trial-development-final.json`。下方 11 月 5 日地址与结果为历史事实，不能替代本轮记录。

2026-11-05：当前原服务已接通本地 DeepSeek CLI，`shared/concepts/identity` 的开发对话可直接使用。刷新原页面，进入「开发」并发送要求即可；界面按 DeepSeek Harness 精简，历史与连接详情按需展开。下面的 `/srv` 路径是移植示例，不是当前安装值。

当前实际部署：配置 `/yzp/zhaozy/yangzepeng/0905/.forge/development.json`，会话状态 `.forge/development-state`，共享运行域 `/var/tmp/demiflow-forge-0`，服务仍为59865/桌面转发55971。仅已配置项目可开发；原业务运行许可保持。单个活动开发单元、2GiB provider RSS采样保护、15分钟应用轮次和10分钟输入等待；当前项目显式配置512KiB上下文，总额度仍512KiB，完整包含祖先AGENTS与PIPELINE_SPEC。默认部署仍为128KiB，超限明确拒绝，不截断规范。

本次新增三轮真实浏览器验收分别完成了临时项目代码修改/4项独立测试、刷新后同对话继续、现有项目完整上下文只读确认；均正常排空。临时验收项目已移出登记与开发授权，最终配置的新会话发送可用。历史验收会话绑定原配置而保留；配置变更后请新建对话，不重写旧授权记录。证据在工作区 `_demiflow/forge_coordination_20261104/direct_use_20261105/final-verification.json`。Codex真实推理仍未验收。

## 开关与归属

开发开关是 `console serve --development-config FILE`，与运行许可独立。`--enable-runs` 和 `--control-only` 的互斥、项目 run_submit/run_control/attach 交集、常规 worker 1、控制 worker 1/有界 waiter、运行快照缓存均保留。开发事件另有 2 个 reader，但仍共用 10 个 HTTP 连接上限；不承诺全部连接被占时停止可接入。

运行态与开发组合启用时，开发配置必须指定与 HTTP/CLI 的 `DEMIFLOW_FORGE_DOMAIN` 相同的 `runtime_domain`。该现有域下显式安装 `policy.json` 与 `host-budget.json`。普通读取不创建或改写这两份部署策略；未配置共享 host 文件的原运行部署沿用原行为。仅开发、且不开放任何运行控制的旧配置仍可单独使用，其 run/other 分区由部署者明确负责，不能称为已通过组合预算验证。

共享 host 文件是固定部署配置，不新增队列、调度器或运行数据库。运行仍走原 `run_domain` 准入；开发仍走原 Store 预留和 state root 服务锁。host 文件把开发预算绑定到一个 state root，不能用第二个 state root 重复消费相同额度。修改域、工作区清单或预算须先停止参与者并核对 unresolved/保留资产，不支持热缩容，也不能更换域绕开旧租约。

## 实际预算口径

以下是一个运行工作区、默认运行域、两个开发单元的具体可验证配置。GiB=1024³，MiB=1024²。

| 项目 | 固定额度 |
| --- | --- |
| run_domain.max_rss_bytes | 8 GiB，已包含域内检查树及运行控制/遥测预留，不再重复加一份 |
| 开发内存 | 2 × (2 GiB RSS + 64 MiB 控制) + 64 MiB = 4.1875 GiB |
| other 内存最低值 | 5 GiB：普通只读检查树按 4 GiB 采样保护，另留 1 GiB 共享控制/响应/缓存余量 |
| 内存合计 | 17.1875 GiB，占声明的 24 GiB 并发预留分区；其他参与者需要再增加 other |
| run 每设备活动额度 | 16 GiB |
| 每工作区历史与请求记录 | 4 GiB + 16 MiB；与 active 槽位数无关 |
| 运行域控制文件 | 4 MiB |
| 一个工作区的 run 磁盘保守合计 | 20 GiB + 20 MiB = 21,495,808,000 bytes |
| 开发磁盘 | 2 × 768 MiB 数据库/回滚 + 512 MiB 私有目录 + 16 MiB 控制 reserve + 4 MiB 控制目录 = 2,168,455,168 bytes |
| other 磁盘示例 | 1 GiB，部署者需根据额外参与者增长增加 |
| 同设备并发预留合计 | 24,738,004,992 bytes，约 23.0391 GiB，占声明的 32 GiB 分区 |

`host-budget.json.workspaces` 是域中所有获准新运行的工作区清单，最多 128 个，不只列当前活动或有开发授权的项目。历史额度按整个清单累加；为了覆盖外置原生对象，在每个声明设备上保守预留全部工作区历史额度。新运行/历史准入拒绝清单之外的工作区；输出或对象设备未列入 `devices` 也拒绝。项目从 Web registry 移除不会自动从历史清单删除。

每次开发发送、运行/历史准入除核对同一策略下的合计，还检查物理设备容量、当前可用空间和 MemAvailable。可用空间检查保守地不抵扣已经分配的参与者额度；因此比运行域自身带 allocation credit 的检查更严格，可能提前拒绝。已有表、历史输出和外部数据会减少真实可用空间，不能被当作免费余量。`devices[].bytes` 是并发预留上限，不是所有历史已提交表的累计占用硬配额；不自动删除或回滚这些内容。

普通只读检查树在配置此 host 文件时沿用 `_capture` 的树 RSS 保护，阈值 4 GiB。受管准备仍使用既有 768 MiB 阈值。这些和 provider RSS/进程/私有目录都是采样保护，存在采样间隙；共享控制的 1 GiB 是配置余量，不是 cgroup 硬上限。运行域 CPU 计数 7 仍按原合同执行；provider 的单进程 CPU 时间和进程数保护不是合计线程/CPU 硬配额。额外 CLI、其他服务及非参与者必须分配额外 other 额度，外部任意进程不受此配置控制。

## 配置文件

以下绝对路径是部署示例，发布者应替换为经过 Review 的实际物理路径。不能直接授权一个未讨论的业务项目。路径不可经符号链接；项目、state root、域控制目录应分离。目录/权限和剩余资源由发布者在启用前核对，保留既有私人认证和历史。

`/srv/demiflow/forge-domain/policy.json`：

```json
{"max_active":1,"max_rss_bytes":8589934592,"cpu_threads":7,"device_bytes":17179869184,"history_bytes":4294967296}
```

同目录 `host-budget.json`（本例全部位于同一个设备）：

```json
{
  "format":"demiflow_forge_host_budget_v1",
  "memory_bytes":25769803776,
  "other_memory_bytes":5368709120,
  "development_memory_bytes":4496293888,
  "development_disk_bytes":2168455168,
  "development_state_root":"/srv/demiflow/development-state",
  "workspaces":["/srv/demiflow/example-project"],
  "devices":[{"path":"/srv/demiflow","bytes":34359738368,"other_bytes":1073741824}]
}
```

开发配置 `development.json` 中 workspace ID 必须使用现有 registry 的实际键，项目来源、phase、授权和写范围必须对应经审任务。下例授权仅为示例说明文件：

```json
{
  "format":"demiflow_development_deployment_v1",
  "mode":"trusted_local",
  "state_root":"/srv/demiflow/development-state",
  "runtime_domain":"/srv/demiflow/forge-domain",
  "projects":{
    "REPLACE_WITH_REGISTERED_WORKSPACE_ID":{
      "root":"/srv/demiflow/example-project",
      "kind":"platform",
      "phase":"design",
      "writes":["design.md"],
      "watch":["design.md"],
      "sources":["AGENTS.md"],
      "non_goals":["生产执行","修改运行控制文件或已有数据"],
      "authorization":"替换为经审任务的明确授权与当前真人要求；本例不授予业务开发许可"
    }
  },
  "providers":{
    "deepseek":{
      "executable":"/yzp/zhaozy/yangzepeng/0905/.dsh/node/bin/dsh",
      "node":"/yzp/zhaozy/yangzepeng/0905/.dsh/node/bin/node",
      "version":"0.2.1-rc.2",
      "model":"deepseek-v4-flash",
      "model_provider":"deepseek-account",
      "native_credentials":"/yzp/zhaozy/yangzepeng/0905/.dsh/home/.credentials.yaml",
      "inherit_proxy":true,
      "effort":"high"
    }
  },
  "host_budget":{
    "memory_bytes":25769803776,
    "run_memory_bytes":8589934592,
    "other_memory_bytes":5368709120,
    "disk_bytes":34359738368,
    "run_disk_bytes":21495808000,
    "other_disk_bytes":1073741824,
    "device_path":"/srv/demiflow"
  }
}
```

平台只检查 native_credentials 的物理路径/权限/大小，由已安装 CLI 原生组件读取；不把认证正文复制进部署文件、数据库或报告。使用同一安装的 raw CLI 是为了避开会固定共享 DSH_HOME 的外层启动器；每个开发会话继续使用私有 HOME/profile。`inherit_proxy` 只显式传递 HTTP(S)_PROXY/NO_PROXY 引用；无需要时设 false，值不写进部署文件或历史。

### 受管开发门禁接入

已登记 DemiForge pipeline 的新开发授权使用 `kind: "demiforge_registered"`、`phase: "implementation"` 和明确的 `pipeline`，省略 `task_id`（等同 `managed: true`）。普通用户继续在开发页输入需求，控制器为项目会话自动创建唯一受管任务；后续消息追加到同一任务，原始 dirty 基线保持。仍须明确授权 writes/watch/sources/non_goals，实际运行登记验证需要 `allow_tests: true`。不得借该设置扩大命令或生产执行权限。

受管任务存于 `state_root/forge-development/<workspace-id>/<session-id>/`，不在项目内复制权威记录。已有明确 `task_id` 的授权继续使用旧本地任务；不能同时指定 `managed: true` 和 `task_id`，也不自动迁移旧历史。修改授权会改变会话绑定的 policy，不能静默升级已有会话。

控制器在 provider 及工具排空后自动检查、运行已授权的固定验证，并使用已配置的同 provider/model 发起独立无工具审阅。确定的检查或语义审阅失败可在同一用户请求内自动续修最多 3 次；独立审阅最多 2 次。原始 goal、dirty/staging 基线和总时限不重建，内部反馈不冒充新的人类授权。未知发送、超时、资源中止、取消及未证明排空均不自动重放。仅语法或测试通过不能显示开发完成。

DeepSeek 审阅阶段固定使用 `low` 推理等级，编码阶段仍使用部署的等级（本次为 `high`）；两者使用同一个已配置 provider/model。审阅单次上限为 16,384 输出 token、180 秒和 64 KiB 结论。预算耗尽且未返回完整结论时明确显示不可用，保留检查/验证事实但不签发接受。真实模型诊断与整条页面验收分别记录，不能把单独的审阅诊断当成原任务已接受。

`restricted` 使用真实 user/PID/network namespace、清零能力、Landlock 与 seccomp，从进程外约束整个 CLI 和它的工具后代。开发只写明确的源文件范围及私有 HOME；固定 verifier 只写 scratch。凭证留在受信模型传输服务，CLI 只有当前任务固定出口，不得到上游凭证、代理或任意 URL。权限和平台源码、控制域重叠、缺失模型通道或边界不可用时拒绝启动，不回退到 trusted_local。它保持调用者的宿主 UID 映射，不能称为独立非 root 宿主账号。RSS、总磁盘增长与进程计数仍是有界采样保护；不是 cgroup 或文件系统硬配额。

验证保持 `live_tree_rechecked`：候选源码原位只读执行，开始和结束重新校验指纹，不声称创建了不可变源码快照。接受还要求真实 developer/verifier 完成回执、同候选独立审阅、当前标准/契约/源码/日志有效，以及控制器证明完整排空。`trusted_local` 保留旧行为和历史，接受标注可信本地、未防篡改。显式 `task_id` 的 legacy 本地任务不能在 restricted 下调用宿主 launcher；新受管任务需单独建立，旧记录不改写。

运行策略 `run_policy` 默认 `observe`。需要严格准入的受管受限项目可在受信部署授权中显式设置 `require`，并配置固定 `runtime_domain`。同一域的受保护 `development-admission.json` 只引用已有配置快照，不是新的运行或接受账本；CLI 无权改它。新准备和首次启动复用当前领域验收及控制器终态，并把接受身份封入原 run manifest。历史 attempt 恢复继续使用原封存对象，停止和历史读取不被新开发证书阻断。不能用省略配置字段悄悄撤销已启用的 require。

## 启用与回退顺序

1. 发布负责人先核对组合源码、wheel 和 Review 摘要；保留既有服务参数、registry/token、dirty 修改及回退材料。由最新Review指定的发布负责人完成合入、启用与服务核验。
2. 停止参与者并确认无未知租约后，安装经审的域和 host 配置；将原 CLI/Web 的 `DEMIFLOW_FORGE_DOMAIN` 指向同一域。不要把仍有历史的工作区从清单移除来取得空间。
3. 用 `Deployment.load` 与 `validate_runtime(admit=True)` 只读校验实际配置和资源。未获最终组合批准前不运行服务，也不发送新模型任务。
4. 原服务参数基础上增加 `--development-config`。保留原 `--enable-runs` 或 `--control-only`，两者不可同时使用；示例命令为：

```sh
DEMIFLOW_FORGE_DOMAIN=/srv/demiflow/forge-domain \
python -m demiflow.cli workbench console --registry /srv/demiflow/registry.json \
  serve --control-only --token-file /srv/demiflow/token \
  --development-config /srv/demiflow/development.json
```

5. 无模型核验项目权限、开发 capability、运行快照、停止路径及静态资产；正常开发请求依用户当次要求执行；验收批次单独记录次数与时限，历史验收次数不是永久禁用开关。
6. 回退先停止开发及确认清理，然后移除开发开关并按原运行服务方案回退。保留 state root、SQLite、unit 和收据；cleanup unknown 不因回退变为 complete。已 claim 但无后代排空证明的开发轮次仍需保留预留；本版没有强制释放入口。

`trusted_local` 使用宿主权限，不是 OS 文件/网络沙箱；范围检查不能阻止同 UID 工具越界。restricted 需通过宿主真实探针和任务启动校验后启用。DeepSeek 的补充输入和 provider transcript replay 未支持，恢复时显示历史缺口；模型文字不授予检查通过、认证或独立 Review。当前固定模型传输只接受文本和本地工具结果；Codex 必须显式配置同模型 Responses 通道，不能借用桌面 App 的认证，也不默默切换 provider。
