# 原 33 项失败与 V035 余额核对（2026-11-17）

本文件保留核对时 `799d04a` 的读取快照；后续缓存修复、正式 Ray 队列及最终组合结论见 [最终意图对照](ARCHITECTURE-FUNCTION-REVIEW-20261117.md)。下述“读取时”状态不覆盖该最终报告。

**结论：本次核对没有发现原 33 项或选定 U2/U3 核心 24 项仍有未关闭的已证实架构／功能红项。V035 仍不能标绿，原因是完整候选回归证据与环境覆盖未闭合；不能把这一状态写成“仍有数千个生产缺陷”。** 最终交付应分别保留功能验收结论、已知能力范围和未验证范围。

读取时 root 为 `799d04abe975f693f6f7dece59b79f624b3f2a98`，树 `/yzp/zhaozy/yangzepeng/0905/_demiflow/worktrees/gpt_runtime_v3_review_20261115`。本次只读源码、账本、Git 状态及现有 JSON/XML；未导入执行平台、未 collection／测试、未修改仓库，唯一写出为本报告。按指示不重新调查 D009/D010。

## 已关闭：不应重新列为当前问题

| 集合／旧问题 | 可核验裁决 |
|---|---|
| 原“33 项新失败” | `CONTRACT-SUITES-CLOSURE.json.historical_maps.original33` 的 33 个唯一 nodeid，全部在 `6b2a731` 完整 19 组／564 例 XML 中通过。本次逐个核对 XML testcase，无 failure/error/skip；XML SHA256 为 `bf689a83caecfe7e84d2e19f41ca66ff0c336510458dc5206148d1d3793653cc`。**历史未关闭数 0**，不冒称当前 root 的 33 项同版通过。 |
| U2/U3 Local 18 + Ray 6 | `core-mapping.json.latest` 的 24 个原 nodeid，全数匹配其 9 份回执 SHA，且各自 setup/call/teardown 恰一次并通过，无 xfail/rerun；跨 5 个明确 HEAD。与原 33 个 nodeid 交集为 0。**选定核心未关闭数 0**。原失败／900 秒未完成批次仍保留，后续只补未闭环项。 |
| 另一组原 90 失败 | 独立账本 `demiflow-history-original90-final-map.json` 明确 historical_union_resolved=90、remaining=0；不能与 33／24 相加，也不能当成 V035 全仓通过。本次未重验其全部源摘要。 |
| 原 40 KiB 写出／manifest、columns、索引 children 共驻 | H 中“正在修”已被 I 的 `6c829ca` 集成及原预算回执取代。不是当前待修问题；相关元数据原窗口反例在 reducer 最终叶又取得完整通过。 |
| reducer／seed shuffle | CURRENT-STATUS 与 ARCHITECTURE-FUNCTION 文中的“shuffle 尚在修”已落后于 root 集成事实。`799d04a` 的组合 Local/Ray 两项已完整通过；本次还读到同 HEAD Local 5、Ray 5 功能回执，均含真实关系与排序正式 selector。最终 root 其余在途验收由 root 裁决，不能用旧文本倒退重开这两个缺陷。 |

33 项闭环既有真正生产修复，也有契约夹具迁移，不能统称“全是假失败”。例如 W001 是实际历史可选字段读取缺陷；完整组还修过 SortOp bool 方向、物化句柄暗依赖 ArtifactStore 私有 bounds 等缺口。

U2/U3 中旧 `*-chain`／BlockSink／graph-result 观察、固定“前 4 行 ACK”、旧异常类型和“失败不得留下任何 ACK”不是新运行时必须维持的内部形状。现有迁移保留真实 SIGKILL／actor exit、原输入与预算、业务值／多重性、真实已 ACK 前缀、不重复付费、修改代码前置拒绝及无 final 成功发布；因此这些关闭不能解释为恢复了第二套旧引擎，也不应因旧观察点不同而重开。

## 仍须明确保留的剩余清单

| 项目 | 分类及当前结论 | 交付处理 |
|---|---|---|
| V035 完整候选裁决 | **验证余额，不是已证实运行时缺陷。** 当前 `runtime_v3_regression_scenarios.py:42/203/310` 要求完整 collection、已审 plan、整个输入摘要字典相同、每个 leaf 完整三阶段；历史跨版本并集不能满足该门。 | 保持 V035 未完整验收。不能用部分功能组合宣布它已通过，也无需为了标题重跑已关闭核心范围。本次性能延期／20 机取消须按真实范围留档，不能把对应缺项伪造成 pass。 |
| PostgreSQL | 原余额清单 74 个 selector；其中两个 producer 节点只有环境 skip。未见本轮 task-private DSN 的真实补验凭据。 | **环境未验**；SQLite／本地队列通过不能外推 PostgreSQL。保留明确边界，不报生产已失败。 |
| Stage2 固定业务源 | 原清单 8 项依赖 demiwtg 实际 `concepts.lance` 与固定版本／业务代码；本轮没有获证的受控源导出验收。 | **真实数据场景未验**；不得擅读生产源或造替代输入冒充原测试。不是当前架构反例。 |
| 离线 wheel | 原 `test_forge_development_wheel.py::WheelInstallationTests::test_unpacked_wheel_without_checkout_or_old_repository` 未见最终包的离线 build/install 收据。 | **打包验证缺口**，不等价于统一执行运行时缺陷；按低影响范围披露。 |
| 三个 Ray 转导入节点 | `test_gpt_ray_integration.py` 转导入 U2/U3 同一函数，但有自己的私有 8 CPU cluster fixture；原 core lane 明确未跑这三个 nodeid。 | **低优先级 fixture 变体未验**。其函数语义已有原模块证据，但不能把原模块通过伪称这三个节点已通过。 |
| 其余外围全库余额 | f48 台账曾记 2301 个缺证候选中 210 有历史通过、2 环境 skip、2089 未找到回执。后续已关闭的 core24 **全部属于原 2089**；后来的 API／产品／其他批次也会改变余额。 | 这些旧数字不能继续照抄为当前剩余数。本次未重新扫描全库所有历史回执；“没找到”仅是限定检索的缺证，不是失败结论。按意图影响分级，不把外围无界补全作为核心功能交付前置。 |
| 工作台受管提交仅 Local | 最新架构说明中的**现有能力范围**，不是 33 项或 V035 新暴露的回归。当前没有 UI 选择 Ray 集群入口。 | 若对外说工作台已支持 Ray 受管提交则不准确；应保留 Local 范围，不擅自扩大本次实现。 |

性能与 20 机不列为当前修复阻塞：性能由用户后置；20 机已取消本轮验收。已有双宿主历史证据保留，但不等于当前 SHA 的双机复验。外部付费服务的本地 transport 验收、未部署状态亦应继续按原边界表述。

## 证据入口

- 原 33：`docs/runtime-v3-review-20261115/CONTRACT-SUITES-CLOSURE.{md,json}`；原 XML `/tmp/demiflow-v3-contracts-full-r3.xml`。
- 核心 24：`/tmp/demiflow-v035-core-test-migration-20261117/core-mapping.json`、同目录 `verification.json`／`README.md`；其中 latest 给出每个精确 nodeid、回执路径、完整 SHA 和 HEAD。本次已实核 24 个节点的三阶段及 9 份原回执摘要。
- 90 项独立账本：`/tmp/demiflow-history-original90-final-map.json`。
- V035 原余额和精确环境节点：`/tmp/demiflow-v035-balance-20261117/{remaining.json,observed-ledger.json,CORE-SHORTLIST.md}`；冻结前规则说明 `docs/runtime-v3-review-20261115/V035-FINAL-PREPARATION-20261117.{md,json}`。
- 覆盖旧状态的增量：`INTEGRATION-CONTINUATION-20261117-H.md`（24／33 闭环）、`...-I.md`（40 KiB／元数据闭环）、`ARCHITECTURE-FUNCTION-REVIEW-20261117.md`（性能延期、20 机取消、Local 工作台范围），均在 root 的 `docs/runtime-v3-review-20261115/`。
- 当前 root 已完成回执：`/tmp/demiflow-final-composed-r1-evidence/verification.json`（2 例）；`/tmp/demiflow-final-core-local-r1-evidence/verification.json`（5 例）；`/tmp/demiflow-final-core-ray-r2-evidence/verification.json`（5 例）。读取时均 exit=0、源码前后相同、完整阶段；不包含缺最终 verification 的 Ray r1，也不据在途进度认定通过。

本清单没有将旧红记录删除、改写，亦没有对未检索到的全库节点作“全部无缺陷”的推断。
