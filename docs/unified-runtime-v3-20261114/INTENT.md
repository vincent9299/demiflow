# 原始意图：引用、解释与完整边界

原会话：[引擎架构讨论](codex://threads/01a1af6f-55a9-7550-a78d-89e732f1d510)。原文文件：`/yzp/zhaozy/yangzepeng/0905/_demiflow/platform_coordination_20261104/takeover_01a1b5db_20261107/direct-human-dialogue-evidence.json`。本包 `evidence/intent-sources.json` 保存所引用消息的原文、message_id、thread_id 和文件摘要，区别直接原话与已确认方案的技术解释。

| ID | 必须交付的意图 | 原始依据及解释 |
|---|---|---|
| I01 | 同一 Dataset 语义、同一数据流引擎 | “还有单机我们怎么设计，感觉最好和分布式统一设计”；同一逻辑图不等于完全相同物理放置 |
| I02 | Ray Core 仅承载，退出 Ray Data 新运行 | “ray core 类似承担的yarn的职责”；11 月 7 日确认方案要求不把 DAG 再交给 Ray Data |
| I03 | 三种交接，自然选择 | 原话列有界内存、持久增量、有限范围成功封存；“不要加太多配置” |
| I04 | 算子独立调度、CPU/GPU 分离 | 明确认可“CPU／GPU 独立调度与有限预取”；模型副本有界，不能固定整链一个 CPU worker |
| I05 | 完整路由、多输入、多输出及共享上游 | 已确认 shuffle、独立调度、barrier 整体方案；共享实例执行一次，两个独立调用不能因函数相同而合并 |
| I06 | 记录及时交接与有限背压 | “消费是拉，但可以等全处理完再拉，或者直接拉”；业务 batch、传输 block、持久 segment、checkpoint 分开 |
| I07 | 统一状态、效果、提交、ACK 与恢复 | 原生 RunJournal/CommitCoordinator 唯一权威；未知付费结果不自动重放；恢复先核代码与来源 |
| I08 | 有限资源、稳定与高性能 | 原已确认方案覆盖内存/磁盘/CPU/GPU/FD/连接/时间/暂态副本；性能必须同预算、同输入比较 |
| I09 | 完整实际业务能力 | 获取→校验→展开→搜索→enqueue→read_queue→结果持久化→ACK→后处理；prompt/embedding/agent/图像及缓存不能成为旁路 |
| I10 | 清晰 A–E 职责和单一权威 | A 资源、B worker、C Exchange、D 提交恢复、E 编译编排；“不要复用Forge，定位不一样” |
| I11 | 多机可用且证据诚实 | 首批 Local 与 Ray Core；数据读取域、工件、节点故障、driver 重启有完整协议；同机不冒充多机 |
| I12 | 产品看懂、配置、运行同一真实计划 | 保留产品 root：统一工作台，分析/比较/评测；节点观测、源码、配置、恢复保证绑定真实 run/attempt |
| I13 | 保护历史与已有成果 | 不重写历史语义身份、已提交结果、未知效果；旧版本可读、恢复兼容须显式判定 |
| I14 | 完整交付而非阶段计数 | “完整架构…完整的改造计划，从现状->预期升级后的架构”；全部公开入口和旧路径清理属于目标 |

## 历史时间线

- 2026-11-07 15:48（+08:00）：`discussion_v1.json` 记录统一图/runtime/channel/recovery 与三种交接、CPU/GPU 调度。
- 2026-11-07 18:02：`resource_management_protocol_v1_acceptance.json` 保存“上面的讨论我确认，可以按这个思路先往下进行”及可执行代码由 DemiFlow 控制的追问。
- 2026-11-07 18:27：`complete_architecture_and_migration_v1.json` 明确全部入口、退出 Ray Data、旧路径清理、多机和性能证据。
- 2026-11-07 18:38：`module_boundary_map_v1.json` 明确 A–E 唯一职责、原生权威和逐模块迁移。
- 2026-11-13/14：v2 函数任务修复已有执行、状态、资源、身份与工作台，198 卡中 RayDatasetExecutor 迁移为 0 项；不能代替 I01–I14。
- 当前：基于 `267b914` 重新覆盖原始意图，不以减少支持能力、删除测试或增加不可见 fallback 达成“完成”。

## 不偷扩范围，也不漏核心

Kubernetes/Yarn provider 完整实现、全 Rust 重写、任意 event-time/watermark/CEP、任意在线 rescale、跨集群混合运行不属于首批必需。冻结契约保留 provider 扩展与 route_epoch，但首批不支持运行中有状态扩缩；恢复沿原逻辑分区，可替换宿主。

Arrow/DataFusion/模型内核可以复用；它们是受控计算 kernel，不能拥有第二份运行/提交恢复账本。普通有界同步任务可物理融合，但使用同一执行合同和资源/恢复解释。把 data.xxx 统一为 Dataset 类方法属于可选 SDK 表面迁移，不得挤掉本引擎交付，也不在本包偷偷破坏旧入口。

本次引擎升级承担产品的计划/观测/配置/控制接线及现有 Notebook/Case 回归，不宣称重做工作台全部领域。产品全 root 的既有未完成项需在验收账本保留，不用引擎交付代替产品百分比。
