# 统一运行时的真实 Local 线程宿主

此提交仅在 `gpt_v3_residency_20261115` 隔离树完成；主仓未编辑、未测试、未合并、未推送。代码基础为 root `2504b08`，另集成 `f207df3` 的 Record 复制修复、`6d8d6ca` 的真实 worker 观测；尚未合入 root `935bc07` 的 native_limits。机器可读证据见 `THREAD-HOST.json`，其中保留红线发现及修正后的全部回执，不将重叠测试相加成唯一通过数。

## 用户看到的行为

- `local_execution(worker_mode='thread')` 实际使用当前进程中的工作线程。PID/TID 来自原 worker 的 ready 事件，普通进程模式继续使用独立进程。
- 未指定宿主时，原来显式 `.map(execution='thread')` 的共享闭包、锁、Event、Barrier、用户 ContextVar、类生命周期可自然选线程宿主，不要求业务添加新的上下文。显式 `worker_mode='process'` 不会被静默改变。
- 普通 map 的每任务克隆和 callable instance 每 action 复制仍保留。仅使用线程宿主不会自动把可封存程序降成 attempt_only。
- 原 `source → streaming transforms → terminal` 使用独立 source 和共址的纯增量后缀，真实相加后缀驻留/profile 后仍由唯一 allocator 准入。source 不被合进异步/定时后缀，避免阻塞输入饿死异步完成。
- 同规则仅增加一个受控兼容切片：Local、workers=2、iterable 源和显式 streaming map，可在两个真实 worker 内完成 take/count/write_lance；不扩大到任意旧 durable 图。

## 真实执行和资源权威

`ThreadHostEndpoint → DataflowThreadConnector → HostedDataflowWorker → WorkerRuntime → DataflowWorkerExecutor → WorkerGraphRunner` 与 Local process/Ray Core 共用原执行器、K3 生命周期、原 RunJournal/CommitCoordinator、原 artifact authority。线程连接只替换宿主通信，不在 driver 重新实现 Dataset，也不调用 Ray Data。

`ThreadResourceProvider` 复用同一个 `LocalCapacityPool`。记录实际宿主 PID/birth 和唯一 unit；只有控制、业务、peer、async loop、框架 callback/default-pool 线程全部真实退出才 CONFIRMED。挂起、未知归属、未退出线程继续计账。业务 hook 抛错与物理退出分开：错误完整返回，物理线程确已退出才返额，不能因 closed 标志或 hook 返回就释放。

原 allocation envelope 通过 ContextVar 到业务、async loop、同步 callback pool 和 asyncio.to_thread；不修改进程环境或创建第二个 allocator。driver 的 RunAction/RunContext 等权威上下文不会随普通业务 ContextVar 复制给线程。成功/失败/取消/空输入仍执行原生命周期，重复取消不打断正在收尾的 finally。

`astart` 运行在持续到 actor 收尾的 lifetime task 中，业务在 astart 内保存 current_task 后取消它会真正取消该 actor/action，而不是取消一个早已结束的启动 coroutine。单一 cleanup 错误保留原类型，多实例失败用有界 ExceptionGroup 数据传输保留；driver 不再用 resource timeout 覆盖原业务 cleanup 错误。

## 封存和历史身份

新增可选 `RuntimePolicy.worker_mode`：None 表示原进程宿主，wire/identity 均省略；显式 process 归一为 None；thread 只允许 Local。恢复先读原 sealed mode，当前默认配置不能替换历史宿主。

显式 streaming map(thread) 及同条流中无 task_batch_rows 的普通 AsyncMapOp 使用原本的本次进程作用域；单独选择 thread 宿主时，普通可 pickle AsyncMap 仍正常封存，只有无法 pickle 的非 task AsyncMap 使用 live 声明。source、普通 task、native model/effect 程序不被扩大成活对象回退。

活程序的稳定 manifest 为原 `demiflow.dataflow_program.v3`，含原 occurrence、kind、真实 code_identity、business_sources，另有：

```json
{"thread_attempt":{"schema":"demiflow.thread_attempt.v1","capability":"attempt_only","program_key":"sha256(occurrence + real code identity)"}}
```

无假 payload_ref，且 **manifest 不含 run/attempt/fence/token**。原 graph/stage ref 和 plan_id 摘要算法不变。随机 token、对象和原 action owner 仅在有界内存 registry 中，加载用已认证 assignment 的 run/action/attempt/fence + occurrence/program_key 索引。丢失对象、错误 owner、跨 attempt 恢复均拒绝。真实 code-pin/源归档仍保留；修改函数 __code__ 会被拒绝。registry 在原 action 的所有物理 allocation 已确认退出后释放，hook 错误不会凭空抹去退出事实或阻止已确认资源退休。

该声明使相关节点 `state='nonrecoverable'`、action recovery 为 attempt_only；不会把本次活对象当作可恢复镜像。更换 attempt/fence/随机 registry token 后，manifest ref、dag_id、plan_id 恒等，错误 owner lookup 仍拒绝，已有测试锁定。

## 验收范围

- `thread7`: 38 passed，原 actor 生命周期及线程场景组合。
- `thread8`: 142 passed / 3 failed / 1 skipped。3 红分别为新测试的归档导入树断言、缺 DTO 必填字段、单错误被过度包装；均已修，后续回执逐项覆盖。
- `thread9`: 22 passed / 1 failed；唯一为新代码 pin 夹具未接原 artifact authority，后续改为真实 RunContext + DriverArtifactPort。
- `thread12`: 31 passed，含真实 Local/PID、恢复、source→inline→Lance、外部取消、错误清理、原线程 Barrier/ContextVar/上下游 Event 等。
- `thread13`: 5 passed，锁定 callable instance 每 action 复制及原共享函数/类语义。
- `thread14`: 23 passed / 1 failed；唯一为身份测试误用空图默认夹具，未触及生产路径。改成实际三节点 DTO 后，`thread15`: 2 passed，稳定身份和代码漂移拒绝同时通过。
- `thread-ray`: env-ray 私有 Ray Core 集群真实 1 passed，Ray Data 入口禁止，运行原 block manifest 用例，finally 关闭私有集群。
- 承重七件与冻结基线逐字相同；检查结果、完整命令、XML 和最终源码摘要在 JSON 中。

## 仍需总控集成验收的边界

1. root 的 native_limits/RSS/默认窗口修复尚未并入此树。thread 模式 CPU/memory 是原逻辑准入，不是 OS 线程独立 RSS 隔离；RSS 采样是共享进程口径。线程 provider 对 GPU/硬物理限制明确拒绝，不改全进程 CUDA_VISIBLE_DEVICES，不宣称物理 GPU 推理已验证。
2. 原 `filter → thread class(concurrency=3)` 的隐式默认窗口可能过度预留而被拒，root 负责按原总预算缩小默认窗口；不增预算、不修改显式小预算反例。
3. `map(thread) → SaveLance native effect → run_stream` 的额外切缝由 root 单独裁定，不把 native effect 揉进纯后缀或伪造可 park 状态。
4. 非合作的用户线程/调用无法像独立进程一样强杀。未确认框架工作退出时继续保留额度；用户自行创建的线程需其生命周期 hook 负责收尾。不把此模式宣称为进程级故障隔离。
5. 已退休的独立 StreamCheckpoint 接口仍明确拒绝，由测试迁移 lane 对照原 RunContext 恢复重新表达；未把退休门改回第二提交协议。
