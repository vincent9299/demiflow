# 2026-11-17 合流进展与剩余工作

生产合流到 `d87bdda`，仍在 `engine/gpt-runtime-v3-review-20261115` 隔离分支；**未完成最终验收，不宣称 100%**。本页及 [机器回执](INTEGRATION-PROGRESS-20261117.json) 优先于 CURRENT-STATUS 下方历史快照。

## 对原始意图的实际变化

产品仍须看懂、配置、运行同一真实 pipeline，并分析/比较/评测数据；引擎仍须正确、稳定、高性能、通用、单一权威。原 I01–I14 不变，本轮集中修 I05/I06/I07/I08/I12/I13。

- **同一 actor、正确恢复**：共享实例按原图绑定一个真实生命周期/状态 owner，分支只保留各自调用绑定；默认 Local、进程中断恢复和共享快照在根节点通过。恢复前被动核对当前 recipe 的共享关系；共享变独立或独立变共享均在 worker 启动前拒绝，不读源、不构造业务 actor、不重编历史图。
- **有限窗口中的发布重叠**：只在原 CommitCoordinator 已持久化 prepared 后恢复下一窗口，行和字节额度按 run 分摊；原提交线程仍唯一发布者。新的真实交付前缀不能冒用正在发布的旧 cut。检查点触发从所有物理交接次数求和改成各逻辑边前沿，1024 行/checkpoint64 的原 Lance 反例已过，最多17个cut的断言保留。
- **业务异常与框架失败分开**：局部异常类完整限定名随原 program payload 保存，原 catch 不再因进程序列化失配。批调用接入同一认缺判断。验证/资源/取消/未知效果不进入业务 catch；同名但未声明的异常仍失败。任意业务异常传回 driver 继续遵守 v3 data-only 失败合同，不从远端名称导入或构造 Python 类。
- **真实统计**：公开阶段名称、重复 occurrence 来自同一封存 observation_labels；输入/产出来自 worker 事实，最终提交数单列。真实5/10/15/20/25输入前沿触发回调。计时旧快照只有聚合量时，分位数上界明确标成 observed_maximum_upper_bound；没有伪造精细直方图。批失败诊断有字节/条数上限，不能当作第二份重试账本；该批接线仍等 owner 合流验收。
- **提前停止与资源**：已合 `15d03d8` 等价补丁，超 cancel_grace 的不可杀线程隔离，原 run lease、store、journal 和未确认资源保留；真实退出后才清理。scheduler关闭逐项 claim 原 operation，避免已批量 claim 却没执行的兄弟操作空等 TTL。owner 最终生产44唯一nodeid通过，根节点组合待跑。
- **其他实际闭环**：原 registry/工作台丢UI事件后的原ACK重建、固定版本/错误marker拒绝；Arrow受限单源直线复制carrier；原来源只读上下文补 admitted payload bounds；实际文件同ctime/同size改写检测；JSON reader保留固定库异常词汇。对应先前红回执保留。

## 根节点固定版本证据

| 版本 | 实际结果 | 解释 |
|---|---|---|
| dfc3115 | 3 passed | 工作台 partial结果、错误marker及真实注册重建 |
| 68e9dd0 | 8 passed / 1 failed | Arrow复制/真实GC/恢复/摘要损坏及barrier；红项是Lance cut深度重复计数 |
| 3eb595a | 44 passed / 1 failed | 来源/位置索引/资源及6类控制错误；红项是旧checkpoint异常类型的测试预期 |
| 8174d6f | 15 passed / 3 failed | 共享actor/overlap/Lance原反例已绿；三红都是试图恢复已经正常结束的run |
| df5d253 | 19 passed / 0 skipped | 上述三红改成真实中断后恢复；异常身份、控制错误、真实进度和节点统计全部通过 |

每组回执路径、SHA256、精确nodeid、输入快照数量、失败原文及零存活子进程在机器回执中。**这些跨版本集合不可相加为当前HEAD全量通过。**

## 仍须完成

1. recovery_rows 的原actor输入回灌、共享组排空与中途cut恢复候选；显式 hard_timeout 的已取消认缺与批诊断接线。
2. 单worker、小预算图的合法共置能力；当前历史反例覆盖class/union、deduplicate、K6与微批Lance引用。不能增预算或把不可恢复状态假称可停驻。
3. 剩余历史失败逐项收口：原424个历史selector已全执行，首遍296通过/128失败，owner已逐批修正实际旧观察器并保留红证据；该数是旧固定快照执行记录，不能套成当前失败数。19个join_chain优化相关历史断言与正式性能门仍未闭环。
4. 新候选整体验收：同版本 Local/Ray 和真实工作台；新增接线的真实Ray组合正在交原owner独立复验。既有两宿主/六GPU证据按原版本保留，未受影响不机械重跑。
5. I08性能尚未达标。已有pack同FS五对约8.16%场景改善和Arrow载体正确性不能代替完整同预算性能矩阵。原生Lance目前最终编码/发布，早期证明仅是 durable prefix，不夸大早期native编码。

主仓HEAD `e40a6dd3dd756aba423dca4c7d995ada0a4bff65`、原11条dirty/18个展开文件摘要保持；冻结七件与 `49b666f` 字节相同。未push、未合主仓、未改变共享环境或运行服务。
