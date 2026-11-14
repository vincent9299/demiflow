# 第二轮复审处理：改任务卡和调用链，不用新增说明代替落实

日期：2026-11-14。源码依据仍为 `bbd93ff`，本轮未修改生产源码。输入是本目录 Qwen 的 404 行第二轮报告；原报告保留原样，未代作者改写或纳入本次提交。

## 结论

接受“规范方向基本正确，但未进入任务卡/验收断言”的判断。上一轮只给旧卡加阻塞标记并不足以完成设计交付。这一轮实际新增 **44 项任务：42 个函数/方法、2 个声明装配单元**，总计 **188 项、52 个文件**；修改旧卡中的冲突，并给 8 张 V 卡逐条加入新反例。没有新增实现会话，也没有叫停在途工作。

**仍未达到整包分发条件。** 最主要的两个设计缺口是完整 buffer 持久协议，以及 Arrow TaskScheduler/临时写出的真实 grant 生命周期。不能把“任务数量增加”算成原始意图完成，也不因剩余工作困难就缩小产品/引擎 root。

## 1. 明确纠错和已落实的叶任务

| 主题 | 本次处理 | 可核验位置 |
|---|---|---|
| reserve 入口指错 | 接受纠错。预算上下文在 `_exchange` 的真实 `journal.reserve` 之前，不在 `_fresh_exchange`；M012 继续 verify_preserve | [M007](../tasks/M007.md)、[M012](../tasks/M012.md)、CONTRACTS C07、interfaces |
| native journal 与在途所有者 | 显式借用 RunPromptJournal；native 路径原 `_calls` 仅观测，不能独立限额。新增 contextmanager 与 adapter.drain；aclose 先排空再关 client | [M013](../tasks/M013.md)、[M014](../tasks/M014.md)、M003/M004/M011 |
| 原子性/幂等/线程 | 原事务原子性与相同 charges 幂等已经成立；不重写。复用 BlockingIOPool.copy_context；enqueue 读 ContextVar，writer 读 payload | M007/M013、V007 |
| action 生命周期 | 新增公开 take/take_all/iter_rows/iter_batches/schema 5 叶；delivery 借用外层 action 且不自行 complete；count 保留既有边界 | [A009](../tasks/A009.md)—[A014](../tasks/A014.md)、A004 |
| 漏 terminal 与反向调用 | 新增真实 backend 能力生产者、_apply_plan 和 9 个 executor terminal；补 Engine.python_rows 的第十个调用点 | [A015](../tasks/A015.md)—[A025](../tasks/A025.md)、[L045](../tasks/L045.md) |
| barrier 与 slots 无主 | 新增 barrier 的 __init__/tokens/arrive/fail 四叶、控制循环 verify_preserve、_Stage.__slots__ 独立装配叶 | [R017](../tasks/R017.md)—[R022](../tasks/R022.md) |
| restore 序号 | 新增 restored batch_ordinal 的 type/int/nonnegative 检查；承认 restore 赋值与运行自增是两个合法写点；原满批/字节检查保留 | R006/R008、V008 |
| 工作台 HTTP 不接受二元键 | 新增 GET_FIELDS['graph-source'] 声明任务；复用已有 source_view.get 和真实 resolver，不新造同名函数 | [W022](../tasks/W022.md)—[W024](../tasks/W024.md) |
| 前端旧卡冲突 | W020/W021 改为一个 logical 顶点；详情选择 occurrence。保持 state.node 报告 ID，增加独立 logical_selection；扩现有 chooseNode/loadSource/create | W015/W019/W020/W021、[W025](../tasks/W025.md)—[W027](../tasks/W027.md) |
| driver 不应 pickle 模型 actor | L030 只负责 native_driver_v1 的 code-pin-only 封存；worker 原 in-run pickle 路线保持。补稳定多 slice key 和源侧依赖入口 | L030/L031、[L040](../tasks/L040.md)—[L044](../tasks/L044.md) |
| 秘密与字节兼容冲突 | 新增 HTTP options 验证叶；F002 不再要求泄漏形态也逐字节不变。新版本投影安全字段，旧档案只读不改写 | [M015](../tasks/M015.md)、F002、V020 |
| floor 超 ceiling | 已独立调用真实纯函数：slots=1、floor=2 返回 max_pending=2。仅证明函数越过该上限，未运行图或测 OOM。补 derive_prefetch 和封存前联合求解叶 | [Q007](../tasks/Q007.md)、[Q008](../tasks/Q008.md)、L017/L018、V018 |

独立追查还发现 Dataset.take_batch/aggregate/size_bytes/_write_file 目前仅有 observe_action，没有RunAction；已加 [A026](../tasks/A026.md)—[A029](../tasks/A029.md)，补入V001，避免只给executor派卡又漏公开边界。

生成器 `tools/revise_plan_r2.py` 是本次增补的单一输入；build_plan 已调用它。重建不会把任务卡恢复成旧版。新声明任务与函数任务明确区分，checker 核对其真实基线位置和摘要。

## 2. 六处冲突的处理结果

| 冲突 | 结果 |
|---|---|
| C1 occurrence 顶点 vs logical 顶点 | **已统一**：图层全用 logical ID，二元 Selection 只用于详情；_bridge_node_map 恢复原签名及 verify_preserve |
| C2 所有 slice pickle vs driver code-pin-only | **已统一**：L030/CONTRACTS/CRITICAL-REVISION 同一分派；worker 继续原路径 |
| C3 全字节不变 vs 秘密投影 | **已明确迁移边界**：安全形态稳定；不保留泄漏形态；旧档案不改写，执行遵循版本保护 |
| C4 未冻结 payload vs 类型声明已冻结 | **已撤回旧全键集**：CONTRACTS 删除旧“固定键集”，interfaces 的 StageBufferSnapshot 仅 UNFROZEN 标记；R 继续阻塞，不能自由猜字段 |
| C5 M012 preserve vs 新预算入口 | **已统一**：真实修改入口是 M007，M012 保留；M004/M011 明确跨层借用/排空职责 |
| C6 六种能力 vs 四种 buffer、batch 序号改名 | **拆清语义**：CheckpointStageKind 有六种；BufferKind 只有五种，native_row_local 不是新 buffer 快照。旧 batch_ordinal 不改名。新 buffer payload 尚未冻结 |

另外，命名别名 `RecoveryGuarantee` 已加到 interfaces，生产者/验证者职责加到 A001/L001/L002/L004/L007/L008。它是执行能力描述，不是新公开可选恢复模式；queue proof 中的 `final_commit` 位于另一命名空间，不做同名值互转。运行实现尚未产生该字段，不能据类型声明称已实现。

## 3. 真正进入 V 卡的验收断言

| 卡 | 新增必需断言摘要 |
|---|---|
| V001 | 一次公开 action；内部 delivery 借用；早停/取消排空；封存 backend/profile/program 篡改在读源前拒绝；实际消费者绑定匹配 |
| V007 | 两 fresh 总额=1；两个 node 共享总额；0 上限；事务失败；幂等及组件差异；真实跨线程 payload；ContextVar 清理；inflight 与 close |
| V008 | c=2 pending 聚合；重复/错 slot、错 cut、EOF；native 单 token 全注册表；定时/压力/prepend；快照故障；旧序号格式及非法值 |
| V010 | 带锁模型 actor 的 driver 封存；同 action 多 slice 稳定 key；_apply_plan 十处调用含 Python fallback 消费绑定 |
| V013 | 真 HTTP 双键；单 logical 顶点多 occurrence 的源码/参数/观测；物理摘要；异步旧回包 |
| V018 | floor/ceiling 联合可行性；默认 credit 收紧与显式冲突拒绝；Arrow 实际分配约束，不以参数透传代替 |
| V020 | 合成凭据不入永久载体；凭据轮换不改语义身份；历史读取与当前执行拒绝分开 |
| V021 | source reducer 导入 helper 改码拒绝恢复；原 operations 依赖保护保留；多 input_path 不混段 |

这些断言是任务要求，**未来测试函数尚未实现，也没有跑过这 21 项业务验收**。当前门缺文件会拒绝，不能把任务卡文字当测试通过。共享 fixture 装配仍需收口，不能每个 V 函数各自复制运行框架。

## 4. 验收工具三项反馈

- **G1：已加机器强制。** 7个整文件继续逐字节摘要守卫；三个可局部修改文件增加 scope_guard.py，读取明确的 symbol_scopes。允许指定函数及slots声明变化，拒绝其他代码、imports和声明变化；注释/格式不作为语义改动。无法解析也拒绝。新增helper/顶层import需先进入明确任务与范围表，不能顺手扩scope。守卫有允许修改和越界变红的对照测试。
- **G2：补执行清单。** 本轮工具/结构测试同时保存解释器、命令、cwd、HEAD、dirty 状态及输入文件摘要。证据绑定实际内容，不把带改动工作树的运行伪称仅对应旧 HEAD。上一轮两个裸 log 作为历史保留，不给它们补造从未记录的命令。
- **G3：新增机器聚合。** `tools/aggregate_gates.py` 按 depends_on 求依赖闭包，要求所有门相同源码 revision、同 gates 配置摘要、干净且冻结未改、完整成功重复次数。缺依赖、混 revision、重复证据、旧配置、少轮次均拒绝。结果仅表示测试门齐备，`product_accepted` 固定 false；不是产品/引擎 100% 认证。

用法：在实际 gate result.json 都存在后执行 `python docs/upgrade-contracts-v2-20261113/tools/aggregate_gates.py /tmp/demiflow-*/result.json`。可以用 `--gate RECOVERY` 只验证其完整依赖闭包；最终测试门验收省略 --gate 检查全套。不要混入非本工具的 result.json。旧结果缺 gates 配置摘要须重新取得有效证据，不能手填改绿。

## 5. 不接受的扩大推论与保留项

1. **未出现某目录的任务，不等于必须改那个目录。** ResourceProfile、共享预算、RunJournal 原子性、InputGate/ChannelSequence 的既有正确语义优先复用。resource/exchange 冻结接口不能为满足任务清单覆盖而扩写；真正应改的是其准入生产者/执行消费者。
2. **六种能力不等于六种新快照。** native_row_local 已有注册表与锁保护，不迁入新 buffer 持久载体。这里不能机械按“补六个 enum”合并两种协议。
3. **同名字段不必新造转换。** 描述型 RecoveryGuarantee.final_commit 与 queue 证明字段只是不同命名空间；要防误用，不造一个没有业务含义的互转函数。
4. **已有上界不能报成无限。** reservation 数已有 100000 硬限制；checkpoint 文档有全局节点/深度限制，欠缺的是适合参与者规模的专门准入口径。Arrow 当前有固定 defaults，缺的是消费实际 grant。性能/长任务是否足够仍需证据，但不是“完全没有任何限制”。
5. **旧正确部分不重复造。** source pin/归档已有同一 collector；本轮仅扩 source 根。sealed 与 succeeded 已分开、历史不跑 verify_seal、resolver/picker 已存在，都保留并核验。

## 6. 仍阻塞全量分发的事项

| 优先级 | 未完成项 | 下一步必须交付的具体内容 |
|---|---|---|
| 核心 | R buffer 持久协议 | per-slot payload、group/rows索引、EOF/cut参与者、旧格式兼容及 native 切换不死锁的正反例；在此之前不得打开 R006 新 kind |
| 核心 | Arrow 真实 grant/临时字节 | TaskScheduler/local_tasks 的分配前准入、worker/IPC/写出计量与取消释放；需要精确函数卡，不能仅 constructor 收 execution |
| 核心 | action 全链闭合复核 | 新 A 卡与 checked.action.complete、stream producer 收尾、Python 子阶段传 binding 的真实组合；未做运行验证 |
| 集成 | 符号保护与候选合并 | 符号范围检查已加入；仍需按函数吸收当前候选，不覆盖用户/其他 agent 的改动 |
| 验收 | 共享 fixture/历史断言迁移 | 明确公共测试装配者；旧用例语义变化按 ID 对账；长期历史校验版本策略尚未闭合 |
| 全局目标 | 历史架构未交付部分 | 独立 worker、Local/Ray/CPU/GPU、完整业务矩阵、首条延迟和长任务性能仍按原意图计未完成 |

因此本轮交付的是**有实际任务卡变化的第二轮设计修订**，不是修复了运行代码、也不是完成十遍全量 Review。`dispatch_ready=false` 继续保留；不能拿 188、29、工具测试数量换算成完成百分比。

## 本轮验证记录

验收判定器14项、跨门聚合8项、符号范围守卫8项，共30项工具测试通过；11项结构检查通过。实际输入文件在检查前后摘要一致。命令、解释器、cwd、HEAD、dirty状态和输入摘要见 [执行清单](r2-evidence/verification.json)。[prefetch纯函数探针](r2-evidence/prefetch.json) 独立确认1槽上限返回2个pending的现状。上述都不代表业务验收通过，运行源码没有修改。
