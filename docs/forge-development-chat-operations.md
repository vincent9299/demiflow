# DemiFlow 开发对话与运行态组合部署

本文记录当前配置和资源口径。[Forge 开发辅助方案](forge-guided-coding-redesign.md)描述当前开发功能。旧试用的配置与证据另见[历史记录](forge-development-harness-delivery-20261119.md)。

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

### 管道开发

开发 grant 使用 `kind: "pipeline"`。平台源码的明确授权使用 `kind: "platform"`。当前开发存储格式需要独立的新 `state_root`。管道会话在写前提供当前 SDK 与所选项目资料，写后在回合结果中报告两项静态检查。运行授权继续由工作台运行服务校验。

`restricted` 使用真实的 user/PID/network namespace、能力限制、Landlock 与 seccomp。开发只写声明的授权范围及私有 HOME。凭证留在受信模型传输服务。边界不可用时拒绝启动。`trusted_local` 使用宿主权限。RSS 与磁盘和进程计数按有界采样保护。

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

`trusted_local` 使用宿主权限，不是 OS 文件/网络沙箱；范围检查不能阻止同 UID 工具越界。restricted 需通过宿主真实探针和任务启动校验后启用。DeepSeek 的补充输入和 provider transcript replay 未支持，恢复时显示历史缺口；模型文字不授予检查通过、认证或独立 Review。固定模型传输只接受文本和本地工具结果；Codex 必须显式配置同模型 Responses 通道，不能借用桌面 App 的认证，也不默默切换 provider。
