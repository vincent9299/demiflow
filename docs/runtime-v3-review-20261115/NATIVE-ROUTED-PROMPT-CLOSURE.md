# 已有公开 Prompt routes 的原生执行修复

固定候选 `651a9bdd83e09b520a53672fc992bd608563e864`，提交顺序 `ba35b99` → `651a9bd`。
工作树 `gpt_v3_regression_20261115`，分支 `engine/gpt-v3-regression-20261115`。
本报告不把 V017 物理 target/broadcast 完整恢复门标成通过。

## 原缺陷与修复

`Dataset.map_prompt_async(routes=..., route=...)` 是已有公开 API（U011），不是新增 DSL。
原 `test_prompt_routes.py` 第三例不依赖私有日志，却在封存时把带锁的
`RoutedPromptActor` 当普通 UDF 序列化，尚未启动 HTTP 即失败。旧测试原样保留并通过。

- `RoutedPromptFactory` 只封存各路已通过原 `prompt_facts` 的被动声明，worker 构造真实子 PromptActor。
- 复用同一 prompt kernel、真实 worker、原 RunPromptJournal 与提交 ACK。没有第二 runner、执行账本或资源分配器。
- RPC 的 node_id 保持原 graph occurrence，原 worker 租约权限检查不变；业务路由名派生稳定的请求及每路预算子域。
- 每次真正发送在原单个 SQLite 事务内同时检查 run、整个路由节点、单路模型三层额度。失败准入不扣费；恢复复用已确认请求，unknown 不当 cache miss。
- 模型只由用户已有 route 列选择。快慢路由能重叠，单路并发守限；401 隔离不换模型，未知路由明确 KeyError 且不调用任何模型。
- 生命周期关闭全部子客户端，即使另一条关闭失败仍继续清理。使用量从各子 coordinator 的实际 worker 计数汇总。
- 可变状态 `when` 不能仅凭 HTTP 有日志就获得恢复保证：真实非 final cut 后按原 attempt-only 保护拒绝恢复，不重发模型请求。
- 未使用 routes 的 Prompt 编译与身份公式未修改；1024/1025/1500 enum 的原 Git golden 正反例通过。

## 固定版本验收

原始命令、进程内模块路径、环境、全部逐项报告和源码摘要在配套 JSON 指向的 verification.json 中。

| 范围 | 结果 | 证据 |
|---|---|---|
| Local、原第3例、预算错误回归、历史身份 | 18 passed / 1 deselected / 76.07s | `/tmp/demiflow-route-final2-local-evidence/verification.json` |
| 同机真实 Ray Core、原非final ACK恢复 | 1 passed / 27.65s | `/tmp/demiflow-route-final2-ray-evidence/verification.json` |

唯一 deselected 为 Ray 参数，已用真实 Ray 环境单独跑绿。两组均锁同一提交的 2133 个
源码/测试/pyproject 文件，before/after 无变化；七件冻结文件逐字匹配 `49b666f`。
Ray 没有使用 Ray Data，测试使用独占私有容量目录和自己创建的私有 cluster。

预算原测试失败独立复现于修改前 `f15016a`：原 RunPromptJournal 已把资源额度错误映射为
`PromptBudgetExceededError`，测试仍期待 `RecoveryContractError`。仅修正内层领域错误断言；
外层 unconfirmed、HTTP 1次、预算1、无提交的全部原断言保留并通过。
修改前与迭代中的红日志完整保留，见 JSON；没有用改预算/skip 删除反例。

## 仍未证明的范围

1. 原两条私有 SQLite/checkpoint 历史测试留给已裁决 delivery-only 兼容方案，不能宣称通过，也不以它建立第二执行账本。
2. 离线 routes 的 driver 已封存子配置读取正在下一步接线；本候选不宣称该组合已验收。
3. Forge/workbench 的静态受管 routes 准入仍有显式边界；本次证明是公开 Dataset 引擎链路。
4. C05 物理 target/broadcast 的完整 worker cut/ACK 恢复尚未由本报告代替；组件 channel 检查与公开业务路由是不同范围。
5. 多机路由恢复未跑，本次 Ray 为本机真实 Ray Core。
