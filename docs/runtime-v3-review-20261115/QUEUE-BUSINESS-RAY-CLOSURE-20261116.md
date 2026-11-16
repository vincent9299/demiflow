# I09 / V022：Ray 五组业务验收收口

此前 [QUEUE-BUSINESS-20261116.md](QUEUE-BUSINESS-20261116.md) 的 Ray 显式 ACK 阻塞已修复。固定、干净的 `9414e04` 完整运行五组门，**1 passed / 362.30s**；normal、非 final ACK 后恢复、explicit ACK、unknown 不重放、全部边界反例均通过。原报告路径 `/tmp/demiflow-queue-full-ray-r5-business/business.json`，JUnit `/tmp/demiflow-queue-full-ray-r5.xml`。这是实际原生业务组合，没有用单测替代整条链路。

[最终机器证据](QUEUE-BUSINESS-RAY-CLOSURE-20261116.json)绑定 1040 个源文件 SHA256、测试前后 clean HEAD、原始报告字节和所有原 run journal 分配。19 次真实 execute_action，ray.data 导入为零；四条队列任务为三条成功、一条预期 404，每个 HTTP 对象仍只获取一次。历史固定 snapshot 的三个成功对象展开为六条记录，每条原生 agent 两轮并实际调用 read_documents，共十二次模型请求，原请求预算恰好十二；真实 Lance 版本 1 后处理完整得到六条正确结果。

## 规划与资源裁决

总控提交 `a48ce01`（本树 `8733e35`）只让 Ray 使用原本已有的有界 async suffix co-location。原逻辑 kernel、native ACK、source lease、原 journal、resource profile、CPU/内存求和、单机容纳检查及显式隔离都保持。未放开 queue parking，也没有构造另一套 queue 执行器。

最初测试把 `workers=4` 同时当成 Ray host `CPU=4`，但 Local 原 sealed plan 实际声明 source 1000 milliCPU 加 suffix 4000 milliCPU。这个 4 CPU 是 fixture 假设，V022 合同没有要求它。总控明确裁决：I09 私有 Ray host 声明有限 6 CPU，worker 上限仍为 4，业务和每阶段 profile 均不变。测试提交 `9414e04` 将两种上限分别表达，并记录实际 cluster 资源；性能基准的原资源配置完全未改。

真实 Ray explicit 现在为两个 stage，原 grant 为：

| 原所有者 | CPU milli | memory_bytes |
|---|---:|---:|
| 独立 queue source | 1000 | 524288 |
| fetch / pure map / ACK / terminal 尾部 | 4000 | 35651584 |

这些值来自本次原 resource_allocations，逐项等于 Local 实际声明。host 为 6 CPU / 0 GPU，object store 128MiB，worker 上限 4。没有把 worker 数减少解释成 CPU 需求也减少。

修复后另跑真实 **4 CPU 不足反例**：同样得到两 stage、原 1000+4000 grant，却明确 RESOURCE_INFEASIBLE；原 SQLite 为 workers=0、completions=0、tasks pending=1，HTTP 为零。证据 `/tmp/demiflow-queue-ray-explicit-four-cpu/plan.json`、`outcome.json`。它检验的是新规划下仍严格执行 CPU 准入，不是拿旧五 stage 的 worker 数失败充当这个反例。

## 完整性与清理

先运行 explicit/unknown/boundaries 尾组，10 次真实 action、44 个原 Ray allocation 全部释放，再执行上述完整五组门。完整门的所有原 run.sqlite 中共 **112 个** Ray allocation，全部 terminal=1、cleanup=CONFIRMED，每条保留原 `ray-actor-dead` 证据和 boot/placement。私有 cluster 关闭后 children=[]。Ray 没有 LocalCapacityPool 文件，因此 cleanup.json 的本地文件计数为零；不能用这个零冒充 Ray 的分配数量。完整原库存已写入最终 JSON。

driver 与另一个实际 Ray worker 的八项依赖来源/版本一致。Ray 2.60.0、Arrow 25.0.1、Lance 12.0.0 仍取 env-ray；缺失业务依赖来自既有 normal env。私有 `/tmp/demiflow-queue-ray-env-20261116` 是 `venv --without-pip` 配置，自己的 `.pth` 依次引用现有目录，不安装或修改共享环境。原 native search 的子进程仍使用同一 sys.executable。该依赖探针不是对每个业务 actor 的逐 PID 采样；业务 actor 清理证明使用原分配 journal 与原确认死亡事实，另确认整个私有进程树退出。

## 交付版本与范围

- `471b728`：三处 native producer 跨物理 cut、恢复、publication ordinal 修复。
- `ec6036f`：完整原生业务 helper、边界与真实 producer 断点测试，support 只接 `_s_queue_full_business`。
- 总控 `a48ce01`，本树 `8733e35`：Ray async suffix 规划适配。
- `9414e04`：仅 I09 测试 host CPU 与 worker 上限分离；本次完整 Ray 门测试的 clean HEAD。

Local 五组完整门在 `ec6036f` 对应源内容通过，1 passed / 293.08s；Ray 五组在 `9414e04` 通过。两份输入集合只有两处不同：生产 `dataflow_thread_fusion.py` 的 Ray 启用条件，及 queue helper 的私有 Ray host 配置。Local 分支行为未因该生产条件变化而改变；本轮没有重复再跑一次 Local，也不宣称两者是在同一 HEAD 上运行。此前四个 producer 真实断点和七个原 search/helper 检查的版本与证据继续由原报告保留。

本验收证明 Dataset 原生队列业务能从输入、执行、原结果/ACK、恢复、历史读取、模型工具到真实结果持久化串起来。它不单独证明工作台 UI、PostgreSQL、两机完整队列业务、业务 OS SIGKILL，也不替代正式五轮性能比较。产品侧和性能侧由总控的其他验收分别判断，不由这一个业务门推导整体 100%。
