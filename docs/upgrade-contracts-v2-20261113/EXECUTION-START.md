# 转入实现：给 Qwen 的派工入口

真人决定（2026-11-14）：不再围绕同一批问题循环互审，先派活、实现，再以代码和真实组合结果验收。

本轮依据为设计提交 `13d19b5`，运行代码共同基线仍为 `bbd93ffcc136c361093c5a4b6d79d0352c4c2557`。第三轮反馈针对的188项版本已修订：L017/L018有修改权限；三种seal和graph cut有归属；无生产者的本地cut_id草案已撤回。无需再次等待Qwen/GPT对这些同一问题互审。

## 可以立即做什么

授权开始实现当前198项任务，按下面12组领取。每项仍保留独立函数/方法、签名、输入输出、测试和回执，不需要创建198个会话。分组不改变函数粒度；组负责人负责文件装配、现有候选吸收与交付。

这份执行决定覆盖旧README/DISPATCH/任务卡中“一律等待整包可派发”的限制。`plan.json.dispatch_ready=false`仍诚实表示**整包接口尚未全部冻结**，不得改成已验收。当前状态另由 `dispatch-20261114/batches.json` 记录为 `implementation_authorized=true, all_contracts_frozen=false, runtime_accepted=false`。原验收门、失败断言、权限与保护边界全部保留。

## 直接交给Qwen执行

1. 读取本文件、`dispatch-20261114/batches.json`和相应组工单。复用正在处理同一范围的会话，核实其状态；已完成的合格函数补丁直接吸收，不重做。旧N1已授权的测试迁移和prompt_facts保护继续；不因为新分组打断它。
2. 给每组指定一个真实活跃负责人和独立工作树。不得让两个在途会话同时拥有同一文件；同文件如需按函数并行，由组负责人分配叶任务并统一装配。L包已拆四组，避免再次全部堆给C。
3. 发送对应 `dispatch-20261114/<组名>.md`。记录实际会话链接、工作树、任务ID与派发结果；没有发送成功就不得标dispatched。不要把回报送给已停止的57bfe8f5。
4. 同一公共接口按当前签名编码。测试可用固定契约样本；生产代码不得增加stub、复制解释器或静默legacy fallback。独立编码不代表所有组合测试立即能跑绿。
5. 交付提交、对应函数ID、准确命令与结果。每函数回执写到 `/tmp/demiflow-upgrade-delivery-20261114/<ID>.json`；组回执写 `<组名>.json`。接到真实需要裁决的问题再集中解决，不重复安排全量文档互审。

本会话当前只有读会话工具，没有创建/发送会话工具，因此**本文件与12份工单已经准备；尚未实际发送给执行会话**。由有派发能力的Qwen执行以上步骤，不通过后台CLI或subagent绕过工作区的会话要求。

## 12组，文件归属互斥

| 组 | 范围 | 函数任务数 | 可立即推进的交付 |
|---|---|---:|---|
| F | 事实与Dataset图编译 | 5 | 真实计划表达、身份稳定、ProgramSlice |
| A | 公开入口与终端接线 | 29 | action只建一次、能力不回退、实际执行消费布局 |
| S | 实例状态与代码身份 | 16 | factory/实例分类、状态登记、恢复保真 |
| R | buffer/barrier/三种progress | 32 | 先在本组闭合buffer载体，再实现各kind与切点恢复 |
| Q | 保真、资源编码与prefetch | 8 | signed-zero/tuple/bytes、预算上限、联合credit求解 |
| M | Prompt/Embedding效果与请求预算 | 15 | 原子额度、borrowed journal、unknown不重发 |
| L-contract | 封存、解释、原生计划入口 | 11 | bindings/ExecutionLayout与实际backend绑定 |
| L-chain | chain与程序/源代码绑定 | 12 | 写出/恢复复用原计划、source依赖保护 |
| L-workers | graph/queue/三个worker | 15 | Local/Ray联合credit接线、异步有限ACK等待 |
| L-native | DataFusion/Arrow与实际资源 | 7 | 既有准入与实际额度、TaskScheduler/临时写出闭环 |
| W | 工作台事实、图、运行控制 | 27 | 按run/attempt/occurrence显示真实源码、状态和结果 |
| V | 真实业务与产品验收 | 21 | 编写/运行真实组合，旧失败与新反例同时覆盖 |

精确任务ID和文件清单由batches.json及每组工单给出。F/A/S/Q/M/W/V及L各组可对固定接口独立编码。R内部未冻结部分由R负责人自己闭合，不要求其余组等待；组外只按C13传递合法状态，不解释新buffer内部字段。

## 两项剩余缺口如何变成实现任务

**R负责buffer协议。** 不把UNFROZEN省略号当已冻结wire。先在自己的分支交付精确payload、合法/非法样本、旧fixed-batch/native row-local兼容规则，以及c=2、EOF、重复到达、压力/定时flush、崩溃恢复的可运行反例。然后在同一负责人范围内实现R001–R008/R010/R012/R015/R017–R022和相关seal。保留fail-closed，逐kind凭证据放开；不改业务并发为1。snapshot、restore、slot登记和budget属于一项完整交付，不能只提交数据类就称完成。

**L-native负责实际资源额度。** L010/L011/L036–L039/L045连成完整链：绑定execution→实际原生options/准入→Arrow任务启动与输入/输出/临时写出→异常取消与释放。必须沿既有资源权威借用额度，不建新allocator。为闭合已知缺口，本组还独占 `demiflow/execution/local_tasks.py` 中 `TaskScheduler.__init__/__enter__/run/__exit__` 、`arrow_batch_sink.py:rechunk/apply_batch` 及所需同文件私有装配（共6个补充函数单元）；先列清实际改动符号、资源持有者、字段/释放点，再实现，并逐函数附回执。新增私有辅助函数必须有真实消费者，不能把“传了execution对象”当成资源接入完成。

这两项可以直接在实现分支完成契约和代码；不再要求先出一轮全局设计稿。若确实必须改变组外公共签名，只提交最小差异及受影响调用方给活跃总控统一处理；其余工作继续。

## 合流、验收与硬边界

先按接口汇合可独立验证的补丁，再跑真实组合；每条链在消费方实际经过新代码才算接通。V可先编写测试、确认原反例，不以缺少其他组实现就改断言。最终由独立集成树跑相关门和核心历史回归；工具测试、样例或单组自测不算产品验收。

产品root保持：统一工作台，看懂/配置/运行pipeline，分析/比较/评测数据。平台root保持：正确、稳定、高性能、通用、职责清楚，真实计划约束执行，唯一资源/提交权威，历史保护与可靠恢复。Notebook/Case/开发历史不因本次引擎改造重写。

禁止改或测试主仓 `/yzp/zhaozy/yangzepeng/0905/demiflow`，禁止push、合主仓、重启服务、付费模型。产物只写 `/tmp/demiflow-*`。承重冻结文件及允许的符号范围见 `review-20261114/frozen-files.json`，不因开工而解除。轻测试最多两路，重测试单路；缺Ray/浏览器证据保留未验。

不承诺今晚100%，也不让所有实现等待文档100%。以真实交付推进，GPT负责验收具体代码与组合结果。
