# Forge 第一阶段实现与验收

日期：2026-11-01。范围：[已通过的第一阶段设计](forge-stage1-design-20261101.md)。第二阶段仅提交 [设计](forge-stage2-design-20261101.md)，未实现。

## 1 交付

已实现独立工程标准、公开接口动态发现、受限检查进程、平台源码与依赖元数据身份、原子创建的快照、范围不变的差异检查，以及 `forge platform list/show/snapshot/check`。

职责分布：`platform.py` 发现接口；`platform_worker.py` 管理检查进程；`platform_environment.py` 盘点依赖；`platform_snapshot.py` 管理身份与差异；`platform_cli.py` 提供命令；`standards.py` 读取独立标准资源。未新增 operator registry、DAG、执行引擎或兼容插件框架。

平台其他代码只为 `agent.py` 增加 `AgentConfig`、`load_agent_config` 的公开导出声明。打包增加工程标准资源，Python 3.10 增加条件依赖 tomli。未改 Dataset、模型客户端、服务、账本或生产数据。

## 2 测试结果

| 测试组 | 结果 | 主要覆盖 |
| --- | --- | --- |
| `test_forge_platform_discovery.py`、`test_forge_platform_snapshot.py` | 45 passed | 动态新增方法、别名、继承、property 与 metaclass、不调用 repr、真实模块导入探针、遮蔽、环境隔离、超时/输出上限、SIGTERM 取消与进程组清理、私有文件变化、配置与环境身份、原子发布、损坏快照、CLI 分页与退出码 |
| 原有七个 Forge 测试文件 | 25 passed | 旧 Check/Verify/Review/Freeze、完整 CLI 生命周期、真实本地 Lance 读写、参考项目 |
| Agent 配置、离线 Prompt、Notebook 执行三组 | 31 passed | 原生配置与导出未损坏、离线请求和假响应、原有非交互执行行为 |
| wheel 与 sdist | 构建成功 | 均包含 `standards/engineering-v1.json` |
| 独立安装副本 | 通过 | 从临时目录启动安装后的 CLI，查询、快照、漂移检测、恢复 |

最后一版代码完整合并执行：**101 passed，97.82 秒，无警告**。本次没有重跑历史 1,865 项全量测试，不把交接文档的全量结果作为本次结果。

较早的分组测试禁用插件自动装载后出现已有 `asyncio_mode` 配置警告；最终显式装载 pytest-asyncio。一次较早合并运行收到 SIGTERM，退出 143，没有完整报告，不计通过；信号来源未确定。新增取消测试另发现 InterruptedError 会被 selectors 当作系统调用中断吞掉，已改为能展开清理路径的异常，确认 SIGTERM 立即终止本任务 worker 并恢复原信号处理器。随后上表 101 项完整合并通过。

复现使用仓库对应 Python 环境，示意：

```bash
PYTHONDONTWRITEBYTECODE=1 PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 \
OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 \
python -B -m pytest -q -p no:cacheprovider -p pytest_asyncio.plugin \
  tests/test_forge_*.py \
  tests/test_agent_config.py tests/test_prompt_offline.py tests/test_execution_notebook.py
```

旧参考项目原绑定已经因 `data/dataset.py` 的先前变更而过期，只有 Dataset.map/write_lance 所在文件摘要不同，公开签名与文档未变化；首次回归正确拒绝旧绑定。参考项目测试现通过 `new_project` 在临时目录创建当前绑定，仅复制原候选实现，再重建 Manifest、重新 Verify/Review/Freeze 和执行。不改原例子、不复用旧认证、不放宽旧门禁。

## 3 实际使用效果

[完整命令结果 JSON](forge-stage1-demo-20261101.json) 来自已安装 wheel 的真实 CLI，不是模拟输出。安装使用独立临时目录，复用现有 Python 环境的依赖且只读，不安装到共享环境。工作目录位于源码仓库之外；快照中的分发模式确认是 `installed`。

| 实际动作 | 结果 |
| --- | --- |
| `list --module demiflow.data --name map --limit 10` | 该命名空间共发现 273 个对象，筛选分页返回匹配接口 |
| `show demiflow.data:Dataset.map_prompt_async` | 返回真实完整签名、文档、定义来源和 unknown 语义状态 |
| 默认完整 snapshot | 851 个对象、917 个包文件、19 个必要依赖分发 |
| 紧接着 check | current，差异 0，退出 0 |
| 仅修改临时安装副本的 `execution/stream_admission.py` | stale，1 个私有包文件变化，退出 1 |
| 为临时副本 Dataset 新增 `acceptance_probe` | show 自动发现；check 报告 4 个导出/继承路径下的新接口及 1 个文件变化 |
| 恢复这两个文件 | current，差异 0，退出 0 |

所有漂移演示只修改临时安装副本，均在 finally 中恢复。原仓库平台文件未用于漂移实验。临时构建与运行证据位于 `/tmp/forge-stage1-acceptance-m3cs3p6r/`，最终安装位置与摘要在 `accepted-installation.json`，完整测试日志在 `tests-accepted.log`；可长期复查的 CLI 结果已经保存至上述仓库 JSON。

用户可以直接使用：

```bash
demiflow forge platform list --module demiflow.data --name map --limit 10 --json
demiflow forge platform show demiflow.data:Dataset.map_prompt_async --json
demiflow forge platform snapshot --out platform.snapshot.json --json
demiflow forge platform check platform.snapshot.json --json
```

## 4 变更与保护范围

实现修改集中在新增 Forge 模块、工程标准资源、Forge 命令组注册、Agent 的公开导出、pyproject 打包声明、两个新测试文件、参考项目测试的临时绑定方式及文档。

开工时记录的 100 个已有 dirty 文件中，最终仅 README、pyproject、第一阶段设计、参考项目测试四项发生本任务预期变化；其余记录均与开工内容摘要一致。已有模型服务、账本、性能实验及其他业务修改保持。Forge 旧生命周期源码与原参考项目证据未改写。

## 5 验收边界

- 当前能力是平台接口事实与身份检查，未把 v1 项目升级成任意原生流程。旧生命周期仍有原三节点限制。
- current 只表示声明范围内平台身份一致，不等于业务验收通过或生产执行许可。
- 依赖保证是 metadata_only，不验证每个依赖安装文件。整个 demiflow 包的保守摘要可能因无关包文件变化而 stale。
- 检查进程执行可信平台模块的顶层导入；不会构造服务或模型，但不是 OS 沙箱。元类签名可能触发描述符时记录 unavailable。
- 未执行真实模型、GPU 服务或生产数据运行；相关模型测试使用本地 fixture / 离线假响应。
- Python 3.11 已执行验收；3.10 的 tomli 条件声明已打包，但没有在独立 3.10 解释器重跑。

下一步只 Review 第二阶段方案：原生入口、需求与实现分离、派生 Manifest、静态接入范围与旧格式边界。
