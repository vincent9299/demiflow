# 固定候选 55acec6：真实 Ray 完整公开 API 与禁用 Ray Data 验收

结论：本轮两个原验收门全部通过。真实同机 Ray Core 覆盖 **98 / 98 API、168 / 168 案例**；失败与未运行均为空。额外原 `test_no_ray_data` 正式门通过。本轮没有修改生产代码、测试断言、预算、FD 阈值或超时。

## 版本与边界

- 集成候选：`55acec616b2c6c7e2297b070d5e6b6001217fa81`。
- 测试提交：`add0a02133614aa132e81e8b0bc3dcd7b2d430e3`，分支 `engine/gpt-v3-residency-20261115`。
- 工作树：`/yzp/zhaozy/yangzepeng/0905/_demiflow/worktrees/gpt_v3_residency_20261115`。合入候选后 `git diff <candidate> -- demiflow tests` 为零；测试前后树干净。
- 两个测试进程各自绑定 **1774** 份 Python / 计划输入及 **2092** 份全部已跟踪源码、测试文件；逐份前后摘要一致。实际 Ray 子进程也独立记录输入摘要并确认零变化。
- 七个冻结文件仍逐字等原基线 `362d8528c27ae60a0d6ae0ccc986a0ff93847de8`。

## 真实结果

| 原门 | 结果 | 原始耗时 | 执行与资源证据 |
|---|---|---|---|
| `test_api_matrix` | 1 passed；98 API / 168 案例全部通过 | 603.16 s | 107 次唯一 `execute_action`，全部 `ray_core`；Ray Data 导入 0；168 个案例 FD 全部 44；子进程正常退出 |
| `test_no_ray_data` | 1 passed | 21.08 s | 真实 JSON → map/filter → Arrow batch 与 sort 两个 action；两个真实 `ray_core` 入口；Ray Data 导入 0 |

矩阵 `execute_action` 采用真实 observation_id 去重，保留原 outer 与 child 观察；不把重复观察算作两次运行。原四个退休入口拒绝断言仍执行。完整逐案例结果和每次调用的 plan_id / action_id / observer_pid 在配套 JSON 与原始 `api_matrix.json`。

矩阵由现有测试子进程拥有独立 Ray 集群，正常退出码 0；子进程外 FD **11 → 11**，原 fixture **12 → 12**，没有放宽原 `baseline + 8` 检查。独立禁用门在另一验证进程中先启动本机私有集群，再建立原 fixture；fixture FD **44 → 47**，仍由原阈值验收，Ray shutdown 后 `ray.is_initialized()` 为 False、原进程子列表为空，验证进程正常退出。没有调用 `ray stop` 或关闭不属于本次运行的文件句柄。

## 命令、环境和原始证据

使用已验证的任务专用 `/tmp/demiflow-ray-validation-env/bin/python`（Python 3.11，Ray 2.60.0）。同 ABI 原 env-ray 依赖优先，仅引用普通环境已有而缺少的包；本轮没有安装、下载或修改共享环境。精确依赖版本 / 路径 / 来源、完整 shell 命令、外部只读观察脚本全文与摘要均在配套 JSON。

- 完整矩阵：`/tmp/demiflow-ray-api-final-55acec6.xml`、同名 `.log`；证据目录 `/tmp/demiflow-ray-api-final-55acec6-evidence/`。
- 禁用门：`/tmp/demiflow-ray-no-data-final-55acec6.xml`、同名 `.log`；证据目录 `/tmp/demiflow-ray-no-data-final-55acec6-evidence/`。
- 观察插件仅在 fixture 清理前复制原案例证据，未替代业务执行或修改判断。仅收集这两个明确门；没有收集装载 Ray doubles 的 H 单元模块。
- 主仓、生产服务、共享集群均未触碰；未 push、未合主仓。

这份证据覆盖同机真实 Ray Core 的完整公开 API 接线和资源收尾。它不代表多机节点故障、物理 GPU、生产付费模型 / PostgreSQL、完整产品所有领域或吞吐余量已验收；这些仍按总控的独立证据记录。
