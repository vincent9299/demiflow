# 十个维度的分项复核记录（非十遍完整重审）

次数纠正：用户要求的“反复完整Review 10遍”尚未完成。下面只记录本次实际做过的分项复核，不能据此声称全文已重审十遍。

这是本次设计的十个不同维度复核，包含实际源码核对、修订和机械验证；不是十名独立审稿人，也不是十次完整运行测试。代码真相优先于旧文档/Qwen结论。设计校验结果单独写 design-validation.json，所有未来运行验收仍 not_run。

| 轮 | 核对对象与方法 | 发现的问题 | 已纳入的修订与证据 |
|---|---|---|---|
| 1 | 共同基线、候选分支和生产位置；git状态、源文件摘要 | 在途树不是同一个HEAD；主仓有长训；旧总控不消费消息 | 固定bbd93ff代码基线；候选逐函数接收；单独docs工作树；baseline.json；活跃协调+原子回执，不以给旧会话发消息作为交付 |
| 2 | 原始产品15叶、平台root与旧117失败分工逐项映射 | 只清失败不能覆盖产品、后端、历史和性能；7项原后延仍会阻塞 | root-map.json列20项意图；27个门；保留Notebook/Case/开发/媒体/浏览器/Ray/性能，不将它们算作已完成 |
| 3 | AST定位函数/签名/返回值与真实调用点 | 初稿写错_MapWorker.start/get，实际是prepare；若干新参数未体现在最终签名；chain/queue/graph返回类型不准确 | 所有现有Python符号和函数hash核对；最终签名与基线签名分开；修为实际_LocalMaterializedHandle/WriteResult/graph字典；无泛化参数占位 |
| 4 | 能力准入和真实terminal路由，核B候选及已有复现 | reject_streaming_map_options全局前移使合法thread map.take退步；count嵌套动作风险；Arrow选择缺sink schema/真实kernel能力 | A冻结ActionDecision，添加sink_spec/available_backends；take/iter/write分别接线；保留native_delivery_barrier回归门；count内部驱动不放宽RunContext普遍嵌套守卫 |
| 5 | 状态预检→构造→登记→快照，以及identity源码 | class豁免不等于构造后实例无状态；直接无钩子实例拒绝不能证明Counter安全；把ContextVar/config抹去会碰撞 | S统一StatePolicy；有限auto_json或明确hooks；构造后首行前必验；默认保守ownership，不偷偷降并发；class豁免保留；配置身份不抹去；__init__纳入任务 |
| 6 | ProgramRef、verify_loaded_plan和递归PlanInput图 | 仅pin外层不能覆盖join/union子程序；全图重编号不兼容局部verify_loaded_plan索引；code_sources字段初稿不准确 | ProgramSlice与ProgramBinding分清局部算子和全图映射；每子计划单独stage/verify；source identity固定；CodeSourceRef包含source；v1不改变，v2明确版本化 |
| 7 | 实际buffer循环、模型发送位置与恢复生命周期 | 只改_arun漏掉_make_worker；Embedding不是PromptClient；适配器snapshot/restore无实际消费者 | 加真实_make_worker、run_stream接入；Embedding改bind_actor/scope_request；删无人消费快照接口；补astart/aclose/drain；核实barrier已await publication后将两类ACK回调固定async；batch/group计时/ordinal/effect状态明确定义 |
| 8 | 字节/资源链路与既有signed-zero复现；原生session源码 | Python相等键合并+0/-0；只给execute加参数仍可能在Engine/session使用当前默认；业务批次不能为资源测试强改1 | Q保留float bits/类型/原预留守卫；L补Engine.__init__/session/batch_rows和native_resources.dataset_session；ExecutionPlacement派生stage_limits；同预算性能/宽行/慢消费者反例 |
| 9 | 工作台事实→映射→前端选择完整链，实际函数及UI回调 | 初稿漏前端函数；单凭F1不能证明执行成功；logical键会合并多次出现；泛名execute会把任务边错误连向工作台 | 补六个JS任务和_bridge_node_map；来源declared/sealed与运行状态独立；保留旧map并增加occurrence map；新代码call边使用文件限定符；引擎无反向工作台依赖 |
| 10 | 全包连接、样例、任务唯一性、签名、链接、产品API示例和保护文件综合校验 | 构造器/字段装配、历史v1兼容、driver空worker组及文档数字需有可复查依据 | 实际v1解释器接受chain/queue/fanout三样例；原物理编译器接受driver样例；v2解释器明确未实现；check_design.py检查任务/输入/归属/root/连接/链接/源码保护；最终结果见design-validation.json |

## 复核结论的边界

本包可作为按函数独立编码的输入；新增接口已有明确生产者与消费者，公共schema、状态权威、资源和错误约束集中定义。**本次没有实现这些新增能力，不能把设计复核当作代码验收，更不能宣称产品与引擎已经100%。**

主要实施风险仍是：native v2所有实际后端消费布局、buffer切点与模型效果的一致恢复、普通状态适配的并发/身份、真实浏览器与业务验收。发现新缺陷时必须回到具体责任函数和root，不能改验收口径掩盖。宽泛Python对象/不可固定外部源不自动获得durable保证；支持域和未验环境要诚实记录。

设计校验只验证结构及基线兼容。V的21个新验收函数、现有回归、浏览器和性能门必须由集成实现实际执行；到那一步才能更新完成比例。
