# 原生有限输入和计算响应执行器取消

## 交付与范围

- 隔离树：`/yzp/zhaozy/yangzepeng/0905/_demiflow/worktrees/gpt_finite_cancel_20261117`
- 分支：`engine/gpt-finite-cancel-20261117`
- 基线：`76378d9b5132ec1b381c3f64762d77b286575765`
- 已验生产及测试：`9f75e35e6f237d6d0b36b0e973aa53ece1c0bde0`，生产改动在 `186335a`。
- 证据索引：`FINITE-CANCEL-20261117-verification.json`。本报告只追加文档，不改变已验代码字节。
- 没有写主仓或总控树，没有改共享环境、现有服务或冻结七件，没有推送或合并。

本次修的是取消后纯有限扫描仍继续的问题，不是吞吐优化。原 profile 的 160 秒现象是调查入口，不能仅据 profile 断言其全部耗时由此问题造成。本轮没有运行性能测试，也没有重跑全仓或产品界面。

## 原缺口及修复边界

Local bootstrap 原 `check_stop` 观察 host stop。K3 Cancel 调用执行器 `cancel` 设置当前执行器事件；随后 `runtime.close` 等待业务线程退出，再关闭 host 服务。长有限认证和 borrowed 重读只调用原服务检查，没有看到执行器事件，因而可以在取消后继续扫描。Ray 的通常 K3 `handle(Cancel)` 同样进入原 runtime 控制链；直接 host.cancel 是另一条路径，不能据此排除 Ray 的同类风险。

不能把执行器事件全局塞进原 `RuntimeServices.check_stop`：原生模型已准入响应的持久化仍使用该服务，需要在协作停止时完成原回执和 journal 写入。

修复为一个仅供纯有限工作使用的私有服务视图：

1. `_FiniteServices` 只持有原服务引用和新检查回调；没有复制 constants/programs 树，没有新资源权威或持久状态。
2. 新检查先执行原检查，保留 lease、原始错误和资源保护的优先级，再检查原执行器 `_cancel_event`，抛已有 `CANCELLED` 类型。
3. 原 kernel `_services`、Callable/AsyncInstance、NativeRunView、SourceRunView、schema 服务和 journal/mailbox 都保留原服务。只将有限读取、纯关系计算和排序归并接入视图。
4. 当前业务线程未结束时，原 close 仍须等待它；没有提前退 scratch/lease，也没有提前停止 host/RPC 来强行制造结束。

## 文件和调用链

| 文件 | 具体改动 |
|---|---|
| `worker/dataflow_execution.py` | `_FiniteServices`、`_check_finite_stop`；start 向 kernel/region 接入有限视图 |
| `kernels/dataflow.py` | `make_kernel(..., finite_services=None)` 兼容传递 |
| `kernels/operator.py` | 保留原 `_services`；有限端口、adopt、complete finish 使用有限视图；旧 owned/pending 输入按片检查 |
| `worker/dataflow_graph_runner.py` | 首次完整认证、采用之前及 ACK 之前检查；有限 `_frame_call` 在每次 peer-credit 重试检查同一执行器；默认调用仍使用原检查 |
| `worker/finite_input.py` | 首次认证、恢复后的有界读取、reader/codec 预算等待和消费前检查；默认未提供视图的旧组件调用保持兼容 |
| `kernels/ordering.py` | `_merge_into` 每帧使用已有 `_check_stop`，覆盖中间 spill merge，而不是只在归并组边界检查 |

以上没有改有限封存格式、wire、历史 JSON、cut/receipt 身份、ACK 所有权或资源额度。恢复首次认证与 borrowed/投影二遍都经过同一检查。用户 reducer 的程序来源和原生上下文不换服务。

## 红基线和绿验收

全部测试使用叶树绝对路径导入、`PYTHONPATH=<tree>:<tree>/tests`、四个私有资源域、私有 TMP/D8 路径及唯一执行锁。完整命令与 setup/call/teardown 记录保存在索引指向的回执。

| 阶段 | 结果 | 解释 |
|---|---|---|
| red-r1 / `27110bc` | 6 failed | 初始反例；另外暴露两处测试夹具口径错误，原始失败证据完整保留 |
| red-r2 / `e7284f9` | 6 failed，1 deselected | 更正夹具后，与最终测试同字节；全部 6 个旧接口反例均因原行为继续执行而失败。新增私有 retry 接口例不计红 |
| green-r1 / `9f75e35` | 7 passed，8.46s | 新增取消边界、真实 Dataset、服务隔离和 credit retry 全通过 |
| regression-r1 / `9f75e35` | 95 passed，90.53s | 原模型排空、控制收尾、有限恢复/资源、worker、40KiB native 写出恢复和 nonfinal prefix 全通过；无 skip |

red-r1 的两处夹具更正：worker/driver 共享文件槽计数，worker 关闭应回到原 driver 槽数，而非在 driver 尚存时要求全域为零；公开 RunContext.cancel 的原错误是 `RecoveryContractError("Execution cancellation requested")`。没有更改生产异常或放宽原行为。

新增 7 项分别锁定：

- 真实封存输入认证中，通过原 `executor.cancel` 取消；不得继续读取剩余成员，不 adopt、不 ACK。
- 已完成原采用/ACK 后的 borrowed 重读、join 投影重读，下一行观察取消；原 ACK 保留，不新增 ACK。
- 有限服务共享原 ports/constants/programs 对象；原 NativeRunView/SourceRunView 检查不换，第一次取消原因不被 cleanup 覆盖。
- 真实 Dataset join 的线程 worker 经 RunContext.cancel 和原控制链取消：旧代码继续解码 32 条，修复只解码已进入的 1 条；原资源分配 cleanup 全 CONFIRMED、scope 全 CLOSED、容量池无残余。
- 3 个真实 durable sort run、fan-in 2，强制经过中间 spill merge；取消后不写第二帧，原输入仍在、读写文件槽归还。
- 真实 PeerBuffers 占用引发原 codec 准入等待，在原 condition.wait 处取消；下一轮检查取消，不拖到 codec deadline。释放 peer 后默认调用仍通过原服务，为已准入效果收尾保留通路。

原回归集合：`test_runtime_v3_cooperative_model_stop.py`、`test_runtime_v3_control_drain.py`、`test_runtime_v3_finite_input_progress.py`、`test_runtime_v3_finite_reader_credit.py`、`test_runtime_v3_finite_borrow.py`、`test_runtime_v3_b.py`、`test_runtime_v3_small_native_workspace.py`、`test_runtime_v3_terminal_close.py`。

其中原生/私有模型 × process/thread 四个真实已准入响应排空正例全部通过；原 drain deadline、原 stop 原因、非 final ACK 恢复、旧 prefix 与有限游标恢复断言未削弱。

## 证据有效性与尚未验证

每组回执绑定 2305 个源码/测试/配置输入摘要；前后无变化，HEAD 稳定，七件冻结文件逐字节等于基线；红组各 18 个 phase 报告、新绿组 21 个、回归组 285 个，全部保留。每组结束无存活业务子进程；唯一测试锁已释放。

独立源码复审由 wide_join_perf 完成，提出并已闭合三点：避免 RuntimeServices.replace 复制/重建已冻结嵌套对象；有限 peer-credit 重试也观察当前取消；中间归并逐帧检查。复审没有运行第二份重复业务测试。

本叶未运行真实 Ray Core，不能把共享代码静态核验写成 Ray 通过；总控将在合流后用 env-ray 跑已有真实有限执行/控制取消用例。本次只保证明确接入的纯有限安全点响应取消，不承诺中断任意用户阻塞调用、第三方 native 单次计算或系统 I/O；它们仍受原生命周期/超时机制约束。没有扩大全局 stop 来破坏已准入模型效果的持久化。
