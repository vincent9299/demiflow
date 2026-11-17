# 小预算写出闭环与性能候选冻结前验收 I

原始意图保持：产品是看懂、配置、运行真实 pipeline 并分析、比较、评测数据的统一工作台；引擎是同一 Dataset 下正确、稳定、高性能、通用、有限资源和单一提交权威的计算平台。本记录完成一个功能与资源缺口，不宣称正式性能或两大意图 100% 完成。

## 已完成

隔离分支 `engine/gpt-runtime-v3-review-20261115`，代码与证据集成到 `6c829ca`。主仓不动。

原 40 KiB 写出恢复保持 R=8 KiB、B=16 KiB、M=40 KiB、原输入/输出/中断点：新计划明确分配 5 KiB control、14 KiB terminal、21 KiB native publication。原单一 allocator、物理退出才释放、原 ACK 与提交恢复保持；原封存 v1 和原本可行的新计划不重算窗口。

此前独立复审发现的元数据漏账已补齐：外层 manifest、columns、逐块索引、编码时同时存活片段进入同一 terminal frame 检查；同一对象的多处引用只计一次。恢复先检查再读取/解码；外层清单采用有界流式编码，并逐字保持原 JSON 格式。索引合并从 roots 弹出的 children 在合并调用整个期间仍记账，put 完成后释放 raw 再构造新 entry。没有借用 driver 的 native grant。

## 固定版本证据

| 版本 | 结果 | 范围 |
| --- | --- | --- |
| 叶 `51d6066`，与交付 `ed4b38c` 全树相同 | 16 项通过、48 个完整阶段 | 原 40 KiB/80 KiB/64 MiB、旧窗口、原 segment hash/restore、外层清单及列元数据正反例 |
| root `da91549` | 9 项通过、27 个完整阶段，68.37 秒 | 小预算与图分区修复组合；原 40 KiB、native 已提交丢回复恢复、metadata 正反例、memory→durable 图恢复、80 KiB 有限写出 |
| 叶 `1f4eba6` | 12 项通过、36 个完整阶段 | 最后 popped-children 修订、新 merge 正反例、原 40 KiB、原 hash/restore；没有再跑无关宽 join/API |
| root `6c829ca` | 3 项通过、9 个完整阶段 | 最后修订合流后的真实 merge 正/反例及原 40 KiB 写出恢复 |

这四批分别保留回执，不相加冒充一个版本的 40 项全套：

- `/tmp/demiflow-durable-publication-r7-evidence/verification.json`
- `/tmp/demiflow-root-native-waves-combined-r1-evidence/verification.json`
- `/tmp/demiflow-durable-publication-r8-evidence/verification.json`
- `/tmp/demiflow-root-native-merge-final-r1-evidence/verification.json`

每批源码运行期间不变、完整三阶段、结束无遗留子进程；总控独立逐 Git 摘要核验见同名 JSON。详细字段、原红基线和修复边界见 [小预算报告](SMALL-NATIVE-WORKSPACE-CLOSURE-20261117.md)。原 33 项失败及另组 24 项历史核心用例的逐节点状态见 [H](INTEGRATION-CONTINUATION-20261117-H.md)；Local/Ray API、工作台/Notebook/Case、图像故障、Queue 和实际 H100 的既有固定版本证据见 [G](INTEGRATION-CONTINUATION-20261117-G.md)。未受影响者不反复全量跑。

## 接下来实际执行的性能验收

本记录落盘后冻结最终候选 SHA，复用原 I08 harness/plan，按组串行运行，期间不改源码、不并发回归/其他 GPU 测试。原数据、预算、120 秒时限、每侧预热、五对交替、真实取消与 1.2 比值门保持。

本机可执行 9 个 Local 格、1 个真实 GPU 格、4 个同机 Ray 格，共 182 次原试验；两宿主另 4 格/52 次目前被原 SSH 入口公钥认证失败阻塞。缺少两机组不能合成完整 18 格通过报告。B0 历史契约与当前原生恢复保证不同，原比值照算并明确披露；B1/B1-Ray 是能力参考，不能冒充旧 Ray Data 性能基线。

目前尚无正式 I08 结果。已有单次/profile 只帮助定位逐行封装、检查点工件往返和紧预算 worker 启停成本，不能当五轮达标证据。PostgreSQL 无测试 DSN、全仓历史节点未全部最新版本执行等边界继续保留；它们与这次 40 KiB 缺陷已经修复分别报告。

主仓 `e40a6dd`、原 11 项 dirty/18 文件摘要与七冻结件保持；没有 push、合主仓或修改在运服务。
