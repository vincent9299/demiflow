# 本轮集成与验收：功能组合收齐，有限输入恢复收口中

产品 root：统一工作台，看懂、配置、运行 pipeline，并分析、比较、评测数据。
引擎 root：正确、稳定、高性能、通用、架构干净的统一数据计算平台；同一 Dataset/Runtime，Local/Ray Core 只负责承载，完整支持真实业务。

## 固定候选 6d0530d 的已经验证事实

同一受测提交完整输入摘要2964项，根节点逐字与Git blob独立核对。原17份回执共119个完整setup/call/teardown，118 passed/1 failed。Local/Ray分别168个API场景、组合覆盖原98入口，禁止Ray Data守卫均无尝试。Local模型9窗口、queue完整链路及文档embedding重放通过。工作台14项真实managed/browser/Notebook/关闭组合通过。Ray原frame/region/图路由/terminal恢复/unknown paid effect加12API组合共20项通过。Ray模型与queue两门随后完成同提交补验：三模型九窗口均每窗仅一次HTTP，三种after_send恢复均unknown而不重发；队列normal/recovery/explicit各6输出、12请求、4次原settlement ACK，unknown的第二次执行0请求/0任务发布。实际Ray Core调用分别15与19次，Ray Data尝试0，进程与allocation清理均通过。

唯一失败：原有限join输出恢复测试不能截获真实中途ACK。其默认R/B与新codec共驻声明导致原256MiB下join和terminal合法分阶段；join已经完成时terminal才输出，旧观察只盯join.finish_cursor。但源码也有真实缺陷：非adopt finite reader整member同步读完才ACK/返回，无pause或step进度，不能只改测试观察点。

## 另棵隔离叶树的必要修复，尚不据此宣称完成

树 gpt_finite_drain_20261117，branch engine/gpt-finite-drain-20261117，基线6d0530d。增加私有有状态FiniteInputDrain：原scope和全部member认证后接住引用，按raw frame受限消费，原worker frontier保存edge/seal/producer cursor，原native cut同时提交消费和输出。相同seal重投不重置；原finished_inputs证明完整消费的历史快照仍可连续恢复。GC沿原ArtifactRef链路，无第二存储/结算账本。

原100×10/256MiB/64行/60s测试已在早期候选实际取得非final ACK并恢复1000多重性，不重复启动完成的上游。该证据仅对应daaa241，不作为最终叶树全绿。R1原回执11通过、1内存通道fixture失败、后续用例因停滞诊断主动SIGINT；保留，不伪装18项完成。停滞暴露park状态需按原credit允许有限读取，ad2a576已补；预算复核另发现reader保留缓冲区与下一次编码共驻未合计，正在补原_frame_call统一准入，资源反例已取红。

两个memory专用测试用原小整数行，旧fusion=False没有保证在新合法预算下仍选择memory通道。fixture明确收紧R至64KiB/B至256KiB，总256MiB、256行/64事件上界、workers、真实重复注入和原业务次数断言全部不变。大join默认R/B恢复测试保持，继续覆盖合法分期。

## 尚未交付的部分

最终叶树/集成定向验证、原U3安静窗口、受影响真实GPU、正式I08五轮矩阵、V035完整历史台账。两机旧入口公钥拒绝且信息问题仍待用户回复，不重试猜测凭证，不减少两机范围。PostgreSQL无测试DSN，保持未验。

主仓e40a6dd原11项dirty/18文件摘要和七冻结件均未变。不push、不合主仓、不部署，不接管在运训练/服务。不能将本轮局部测试比例解释为意图完成百分比。

## 原始证据入口

- 根节点独立逐字绑定：本目录 `INTEGRATION-CONTINUATION-20261117-D.json`。
- API/本地业务：`/tmp/demiflow-root-final-local-6d-review.md`。
- 产品：`/tmp/demiflow-root-final-product-6d-preparation/FINAL.md`。
- 真实Ray API/路由/恢复：`/tmp/demiflow-root-final-ray-6d-REPORT.md`。
- 真实Ray模型/队列：`/tmp/demiflow-root-final-ray-business-6d-REPORT.md`。
- 原核心失败：`/tmp/demiflow-root-final-core-6d-evidence/verification.json`；原trace ` /tmp/demiflow-root-finite-trace-6d-evidence/cut-trace.json`。

这是固定6d的完整功能组合记录，后续finite-reader提交仍须独立定向复验；没有把叶树11项通过混入119项。
