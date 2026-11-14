# 函数级派工模板（暂停按旧卡分发）

> **2026-11-14：修订中，不可按旧任务卡直接派发。** Qwen Review 发现真实接口与验收缺口；当前 `dispatch_ready=false`。先读 [逐条裁决](review-20261114/RESPONSE.md) 和 [七组关键修订](review-20261114/CRITICAL-REVISION.md)。下文旧目标/草案保留追溯，不代表已经闭合。

设计包分支：`docs/contracts-v2-20261113`。代码基线：`bbd93ffcc136c361093c5a4b6d79d0352c4c2557`。设计工作树：`/yzp/zhaozy/yangzepeng/0905/_demiflow/worktrees/gpt_contracts_20261113`。

用户/活跃协调者只需填任务ID、独立工作树和DELIVERY_ROOT。agent按本包实施，不重新出一套架构。创建/消息派发由用户或明确获授权的协调者执行；本次文档任务没有创建执行会话。

```text
先确认 plan.json.dispatch_ready 为 true 且本任务卡版本与共享契约一致；
当前值为 false，请不要使用这份旧模板直接派发。
你接手 DemiFlow 升级函数任务 <TASK_ID>。

先读 docs/upgrade-contracts-v2-20261113/tasks/<TASK_ID>.md、该卡引用的
CONTRACTS.md条目及interfaces.pyi。原始两大root见README；不另改验收口径。

你的独立工作树：<ABSOLUTE_WORKTREE>
代码基线：bbd93ffcc136c361093c5a4b6d79d0352c4c2557
设计包：docs/contracts-v2-20261113
回执目录：<DELIVERY_ROOT>

只实现/核验任务卡指定代码单元；声明叶仅改指定slots/字段，其他键保持。允许新增 tests/test_upgrade_v2_unit_<TASK_ID>.py。
不要写其他agent的checkout。同文件import、类声明、构造器/slots按
FILE-OWNERSHIP.md由唯一负责人装配；给出明确装配补丁，不静默扩大scope。

按冻结签名、输入输出、错误/副作用和caller/callee实现，不等待别人内部实现。
单元测试可以用本包fixtures或按interfaces建立测试域替身；生产代码不能用stub、
ImportError fallback、复制解释器或第二份状态/提交账本。

verify_preserve任务先检查已存在实现，正确就交证据，不重复实现。
已有候选按任务内容吸收，不覆盖其他人在途工作。不整体回退E，不合B的能力回退，
不改N1已确认的prompt_facts守卫，不用改业务并发/批次或放大预算凑绿。

不修改或测试主仓 /yzp/zhaozy/yangzepeng/0905/demiflow；不push、不合主仓，
不重启现有服务，不使用付费模型。测试产物只能写/tmp/demiflow-*。
运行前确认demiflow.__file__来自你的隔离树。

提交自己的窄补丁，记录准确commit/命令/结果，按INTEGRATION-ACCEPTANCE.md写
<DELIVERY_ROOT>/<TASK_ID>.json。卡住立刻写needs_decision及最小反例，不能只发给
已停止的57bfe8f5等待。tested不等于accepted，最终由独立集成验收决定。
```

## 给文件装配负责人

同文件的函数可以同时编码，但只有你合并该文件。先对比baseline_function_sha256，逐函数合入；相邻函数的patch冲突只处理目标片段，保留他人代码。类字段/slots/import按interfaces.pyi和C11装配，禁止隐藏新增API。

装配后先跑本文件对应门和直接消费者，提交函数→commit映射。跨文件组合交集成负责人，不能让两个包各持一套改过的接口。发现契约确实缺口时，回执列出最小改动和受影响task IDs，由活跃协调者统一更新设计版本后继续。

## 给最终集成验收负责人

读取全部回执和函数补丁，在独立集成树验证，不把任何叶任务的mock绿色算成组合绿色。按29个门与20项root索引核证据；索引不代表全部语义已覆盖；缺浏览器/Ray/性能或真实恢复证据保留未验。最终交付必须同时能讲清产品用户流程与公开引擎API行为，不能只报测试数量。
