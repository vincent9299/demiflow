# DemiFlow 统一数据流引擎：完整迁移与函数级执行任务 v3

状态：设计交付，`dispatch_ready=true`。本包是设计与派工材料，不是引擎实现，不代表任何运行验收已通过。

基线：`267b9144c6bf7447c6822aa716f6d5c0eda16990`。工作树 `/yzp/zhaozy/yangzepeng/0905/_demiflow/worktrees/gpt_runtime_contracts_v3_20261114`，分支 `docs/runtime-contracts-v3-20261114`。

本包包含 **589张叶任务、68个文件装配任务、19个实现lane、98个公开入口、50个验收门、577个历史测试文件**。完成10轮有输入摘要记录的设计复审；27项工具自身对抗测试通过。所有引擎/产品实现验收均为not_run。

## 为什么重新规划

11 月 7 日已确认的目标包括：DemiFlow 拥有数据流语义；Local / Ray Core 只承载资源与 worker；三种交接、独立算子调度、CPU/GPU 分离、统一 barrier/恢复；全部公开入口接入与旧执行路径退出。上一包 198 卡没有 RayDatasetExecutor 迁移任务，完成该包不能等同本目标完成。

本包保留已实现的资源、原生提交/效果账本、状态、通道与工作台封存关联；补齐通用图编译、源增量、资源调度、多机与公开入口。禁止再增加一条长期并存的“新路径”，然后把默认旧执行器保留下来作为完成。

## 阅读与分发顺序

1. [原始意图、证据和范围](INTENT.md)
2. [目标架构与关键裁决](ARCHITECTURE.md)
3. [冻结共享契约](CONTRACTS.md) 与 [精确类型声明](interfaces.pyi)
4. [公开 API 迁移矩阵](API-MATRIX.md)、[29种算子与来源映射](OPERATOR-MATRIX.md)、[对象与状态所有权](OBJECT-OWNERS.md)、[身份公式](IDENTITY.md)
5. [函数任务总表](TASKS.md)、`tasks/<ID>.md`、`plan.json`
6. [文件所有权与并行分工](LANES.md)
7. [验收场景、历史回归与证据](ACCEPTANCE.md)
8. [迁移及历史保护](MIGRATION.md)
9. [十轮全包设计复审](reviews/INDEX.md)
10. [用户最终看到的产品与API行为](USER-OUTCOME.md)
11. [发给 Qwen 的执行消息](DISPATCH.md)

每个叶任务只有一个生产函数/方法，或一个不可分的数据声明；对应测试可以随任务交付。同一文件只有一个装配 owner。多函数任务不能用“顺手补 helper”越权：新增 helper 必须补任务、调用边和契约。并行指可以根据冻结契约独立编码，不代表提交顺序、运行集成或真实环境验收没有依赖。

## 交付里程碑（不是编码前置阶段）

- **M1 全入口统一**：现有有效 API 保留，所有新 action 经同一计划与 Runtime；禁用 Ray Data 后 Local / Ray 结果等价；工作台读取同一封存计划。
- **M2 数据流完整**：增量来源、多输入、多输出、共享上游、三种 Exchange、排序/分区/关联/聚合都在同一图中；无需用 materialize 弥补调度缺口。
- **M3 模型与恢复完整**：CPU/GPU/远程 API 资源分离，原生模型/队列/状态/缓存适配，跨所有关键边界的 barrier、效果、sink commit、ACK 和恢复闭合。
- **M4 多机与性能交付**：两台不同节点的 Ray Core 承载、工件/数据可达、worker/driver 故障、节点丢失；同预算性能证据及旧路径清理。

全部 lane 可在契约冻结后同时编码，按 M1–M4 集成和验收。M4 没有真实环境证据时必须保持未验，不能靠单机 actor 测试关闭。

## 纪律

不在主仓写代码或跑测试，不 push、不合主仓、不重启服务、不自行分发会话、不调用付费模型。各实现 lane 从本设计最终提交创建自己的隔离树；测试输出仅 `/tmp/demiflow-*`。每个 Python 测试进程断言 `demiflow.__file__` 位于本工作树，并用该树 `PYTHONPATH` 和 `python -m pytest`。保护原主仓 dirty、Notebook、业务数据、历史运行。

现有三份历史/架构文档和 v2 设计原样保留。本包引用历史，不重写过去的身份、摘要或验收结论。设计通过、实现完成、端到端验收是三个独立状态。
