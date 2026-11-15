# 原生模型与效果账本接入：继续实施记录

本记录延续 CORE-PROGRESS.md，不代表全部验收。仍只修改隔离分支 `engine/gpt-runtime-v3-review-20261115`；产品“看懂、配置、运行并分析评测真实数据”和平台“正确、稳定、高性能、通用且架构清楚的统一引擎”两大 root 不变。

## 本轮落实

1. 普通 `map_prompt`、`map_prompt_async` 和 `map_embeddings` 封存被验证的声明；worker 内构造实际 actor，在生命周期开始前绑定原生 journal。活锁、客户端、池和可变 request gate 不随声明发送。`prompt_facts` 守卫保留，声明式 request_policy 在 worker 重建。异步 Prompt 的声明不再依赖 Local executor 的专有 factory。
2. 模型编译器和 kernel 对齐真实类型与 `main` 数据端口。模型业务字段映射保留在配置中，不冒充图端口。调用上下文带原生 occurrence，scope 的调用序号随 kernel checkpoint 保存；union 保留源 record_id 的同时，重复业务调用有各自的请求 occurrence，不按输出数推测调用数。
3. 原生 RPC 补齐完整 run limits、不可变 DTO 到 JSON 的双向转换、reserve/retry 的布尔结果、带 references 的 lookup、Embedding 批次 membership 绑定、原生失败事实。worker 不开 SQLite，driver 每个 run 复用一个原生请求 journal。
4. 请求预算与意图走原有 `RunPromptJournal` 的同一事务；未知结果不变成 cache miss。通用 EffectCommand 改为 RunJournal 内 `runtime_effects` 的有界、带 fence 事务，与预算原子提交。删除 `.dataflow-effects` 文件账本实现；发现旧文件事实时拒绝自动重放，要求显式对账。重复同一状态迁移幂等，修改已确认结果或 actor/request 身份被拒。
5. 统一 checked action 不再套用旧的单进程折叠 stream 资源模型；仍继承 run/owner/queue 的 payload 上限，aggregate admission 由真实物理图的资源 profile 执行。旧 stream_delivery 的专用调用保留原预算模型。此修复消除模型与 sort 等合法算子组合在执行前被旧 capability 表拒绝的问题。
6. 新增的 Enum/MappingProxy 声明身份只扩展此前不能描述的类型；现有 native prompt 的 JSON 身份仍直接使用原事实，不重新描述长 enum。普通 model_map 身份使用已有语义参数，剔除随机 node UUID。

## 已运行的组合与限制

- `/tmp/demiflow-native-v3-13.log`：48 passed / 5 failed。其中原生公开调用、恢复与预算、效果事务、M 单元测试以及长 enum / Arrow 历史保护反例通过。五个失败保留在案：`test_gpt_qwen_review_repairs.py` 的 stage/worker factory、运行资源恢复、已完成资源型结果复用、folded predicate 并发准入。涉及测试模块在新 worker 的可见性、旧 stream 中断注入与新图并发语义，尚须逐项处置；不把这组计为全绿。
- `/tmp/demiflow-native-core-current.log`、`.xml`：63 passed，覆盖公开 Dataset 25 项、真实接缝 30 项、原生模型与效果事务 8 项。该次后又补了调用 occurrence 的多重性与恢复游标，不冒充最终 revision 的全量验收。
- `/tmp/demiflow-native-scope-current.log`、`.xml`：9 passed；在 occurrence 修复后的版本复跑全部原生模型/效果事务反例，新增 shared union 后相同行的两次模型调用证明；原生调用游标随部分 checkpoint 恢复的反例再次通过。
- `/tmp/demiflow-ray-native-v3-3.log`：真实 Ray Core 单机集群 1 passed，公开异步 Prompt → sort → take_all → native ACKed commit；两次 HTTP、请求预算为 2；测试禁止 Ray Data 的 from_items/from_arrow/read_datasource。自建集群自行 shutdown。不是多机或 GPU 验收。
- 原生运行中断：8 行输入在部分 checkpoint 恢复后完整返回；总 HTTP 与原生预算均为 8。未知请求：服务端收到请求后断开，原生错误与请求意图保留，恢复前拒绝再次 HTTP。额度耗尽：第二次 dispatch 前拒绝，请求行与预算均只记一次。

## 继续处理的核心范围

Artifact staging 的累计 max_bytes 预留、重复内容结算、失败/崩溃回收还未修；Agent、cache、采集、queue、托管模型服务/媒体路径还未构成完整业务验收；真实 host 资源、pending/续租、跨 run 容量与多机读取域仍待修；actor 并发与状态限制、数据格式/生成器/长流和实际峰值预算、历史测试与工作台完整体验仍在总清单中。

原先 33 个失败的通过证据属于 CORE-PROGRESS 的已记录版本。最终须在最终提交重新绑定证据，不能沿用中间结果宣布 100%。
