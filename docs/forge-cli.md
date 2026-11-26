# Forge 与工作台

使用独立 Python 环境或受控源码包时，请参阅 [项目依赖](forge-project-dependencies.md)。

- **Forge** 是命令行工具：在一个项目目录里创建标准文件、查询当前 SDK、做静态检查、登记可运行的入口。人和 coding agent 用同一套命令。
- **工作台（Workbench）** 是浏览器界面：打开同一个项目目录，查看 Flow 的图、数据表和运行记录，并在授权后运行。

两者读的是同一份项目声明 `workspace.json`，没有第二份状态。

## 快速开始：从空目录到登记一个 Flow

```bash
demiflow forge init --root ./experiment
demiflow forge add flow counts --root ./experiment
demiflow forge add operator is_even --root ./experiment
```

`init` 写入项目声明和一份 SDK 快照；`add` 创建带占位内容的标准文件。然后填入业务逻辑：

```python
# operators/is_even.py
def is_even(row):
    return row["value"] % 2 == 0


__demiflow_operators__ = ["is_even"]
```

```python
# flows/counts.py
import pyarrow as pa
from demiflow import data

from operators.is_even import is_even


def run(*, output: str, limit: int = 10):
    """Keep even values and write them to a Lance table."""
    rows = data.from_items([{"value": value} for value in range(limit)])
    rows.filter(is_even).write_lance(output, mode="create",
                                     schema=pa.schema([("value", pa.int64())]))
```

Flow 的参数放在配置文件里：

```json
// configs/counts.json
{"output": "/abs/path/experiment/data/evens.lance", "limit": 10}
```

检查，通过后登记：

```bash
demiflow forge check --root ./experiment --entry flows/counts.py:run --params configs/counts.json
demiflow forge add flow --root ./experiment --register --entry flows/counts.py:run --params configs/counts.json
```

登记成功后，这个入口就会出现在工作台的管道列表里。

## 项目目录

| 路径 | 内容 |
|---|---|
| `workspace.json` | 项目声明：身份、已登记的入口和配置、数据位置 |
| `flows/` | Flow：用 Dataset API 把算子接起来的入口函数 |
| `operators/` | 算子：普通函数或类 |
| `prompts/` | 提示词定义 |
| `configs/` | Flow 参数和业务配置 |
| `data_tools/`、`notebooks/` | 数据工具和 Notebook |
| `data/` | 数据表和运行产物 |
| `logs/` | 运行日志 |

目录按需创建。已有代码的目录用 `demiflow forge init --root DIR --existing` 初始化，原有文件保留。

## Forge 命令

| 命令 | 作用 |
|---|---|
| `forge init --root DIR` | 创建项目声明和 SDK 快照 |
| `forge context --root DIR` | 查看编码规则、SDK 签名和这个项目的现状 |
| `forge context --root DIR --api demiflow.data:Dataset.filter` | 查询某个接口的签名和来源 |
| `forge context --root DIR --topic prompt` | 查看某一类主题的规则 |
| `forge add flow NAME --root DIR` | 新建一个 Flow 文件 |
| `forge add operator NAME --root DIR` | 新建一个算子；加 `--actor` 创建有状态的算子 |
| `forge add prompt NAME --root DIR --from FILE` | 新建一个提示词定义；`FILE` 为 `-` 时读标准输入 |
| `forge check --root DIR [--entry F:FN] [--params configs/X.json]` | 静态检查，不执行业务代码 |
| `forge add flow --root DIR --register --entry F:FN [--params ...]` | 登记一个通过检查的入口 |
| `forge review --root DIR --entry F:FN --goal "..."` | 发起一次独立的只读评审 |

`context`、`check`、`review` 支持 `--json`，输出一份 JSON 文档。

退出码：

| 码 | `check`、`add` 等 | `review` |
|---|---|---|
| 0 | 完成，或检查明确不适用 | 评审完成，没有影响交付的问题 |
| 1 | 发现已知违规或内容冲突 | 发现影响交付的问题 |
| 2 | 调用方式错误 | 调用无效 |
| 3 | 依据不足、范围未覆盖或达到预算 | 评审未完成 |

### 检查能说明什么

`forge check` 读源码和声明做静态核对：入口是否存在、参数是否齐全、用到的 SDK 接口是否受支持、目录是否符合约定。它不导入也不执行你的业务代码。

所以检查通过只说明接线合规，不说明业务结果正确。业务行为由项目自己的测试验证；两类结果请分别记录。

- 只检查受影响的部分：`--path 相对路径`，可重复给出。
- 有参数的 Flow 必须给 `--params`，检查和登记要用同一份配置。

### 独立评审

`forge review` 让另一个模型会话只读地评审当前改动：业务选择是否放在合适的位置、交付证据是否充分。

```bash
demiflow forge review --root ./experiment --entry flows/counts.py:run --goal "统计通过筛选的记录数"
```

- 运行前把环境变量 `DEMIFLOW_FORGE_REVIEW_CONFIG` 指向评审模型的配置文件（JSON）。
- 默认评审本项目相对 HEAD 的实际改动；`--base REF` 指定对比基线。
- `--evidence 相对路径` 附上测试或小样本结果，可重复给出。
- 默认时限 180 秒，`--timeout` 可调。

## 在工作台里打开项目

工作台按一份登记文件管理多个项目：

```bash
# 新建，或登记已有项目（都用服务器上的绝对路径）
demiflow workbench console --registry /abs/console.json create /abs/new-project --name my-project
demiflow workbench console --registry /abs/console.json add /abs/experiment
demiflow workbench console --registry /abs/console.json list

# 启动界面
demiflow workbench console --registry /abs/console.json serve --port 59865
```

- 服务只监听本机回环地址。通过 SSH 连接服务器时做端口转发：`ssh -N -L 59865:127.0.0.1:59865 user@server`，然后在本机浏览器打开命令输出的地址。
- 登记只记录目录位置和权限上限，不复制、不改动你的代码和数据。`remove` 只取消登记，目录和历史结果都保留。
- **运行默认是关闭的。** 要在界面里运行，登记时加 `--allow-runs`，启动服务时加 `--enable-runs`，两者都要有。每次运行仍会单独核对。
- 一个登记文件最多 128 个项目。

界面里每个项目有总览、数据表、Flows 与检查、运行记录等视图。Flow 视图显示接线图、每个节点的字段和参数、诊断位置和源码。

## 数据表

把一张固定版本的 Lance 表挂到项目里，之后可以按名字查看和取样：

```bash
demiflow workbench location ./experiment corpus /abs/read-only-root
demiflow workbench table attach ./experiment candidates --location-id corpus \
    --uri candidates.lance --version 3 \
    --dataset-id questions --schema-name questions --schema-version 1
demiflow workbench table list ./experiment --json
demiflow workbench table sample ./experiment candidates --column case_id --column question --limit 20 --json
```

- 表名只是别名；真正的身份是位置、表地址和固定的版本号。之后的读取不会自动跟随新版本。
- 外部位置默认只读；需要写入时在声明位置时加 `--writable`。

## 从命令行运行

登记过的入口也可以不经界面运行，分两步：先准备（固定源码、配置和数据版本），再启动。

```bash
demiflow workbench run prepare ./experiment flows/counts.py:run --params configs/counts.json --json
demiflow workbench run start ./experiment RUN_ID --expected DIGEST --wait
demiflow workbench run show ./experiment RUN_ID
```

`RUN_ID` 和 `DIGEST` 取自 `prepare` 的输出。其他子命令：`list`、`logs`、`cancel`、`resume`。

受管运行对 Flow 有额外要求，不满足时 `prepare` 会拒绝并说明原因。例如写 Lance 表必须用 `mode="create"` 并给出显式 `schema`（上面的例子已经这样写）；项目目录还需要位于部署允许的运行分区内。

## 需要知道的限制

- Forge 的检查和登记不提供沙箱：绕过 Forge 直接用 Python 执行的代码不受它约束。
- 检查报告不是运行许可；运行前平台仍会重新核对源码、配置和数据版本是否与检查时一致。
- 工作台是单机服务，面向本机或经 SSH 转发访问，不是多租户的对外服务。

## 独立受限开发

`demiflow forge develop --root PROJECT --state-dir STATE --python PYTHON --goal TASK` 默认在 Forge 开发沙箱中启动原生作者。STATE 是项目外的私有回合目录，恢复时继续使用同一目录和配置。使用 `--read-root DIR` 显式开放第三方包来源。

有模型推理时，宿主通过 `--model-config FILE` 或 `DEMIFLOW_FORGE_MODEL_CONFIG` 提供固定模型与认证来源。下面是 Codex 的配置示例。

```json
{"providers":{"codex":{"model":"MODEL","base_url":"https://provider.example/v1","credential_env":"PROJECT_MODEL_KEY"}}}
```

DeepSeek 配置还需带上已核验的 `executable`、`node` 和 `version`。作者和独立评审各自使用本回合模型通道，凭据由宿主持有。离线工具可用 `--executable PATH --no-review`。仅在明确授权本地宿主权限时选择 `--mode trusted_local`。
