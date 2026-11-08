# 四模型统一性能方案与 review

范围：pplx-decider-v1.2、amos-1-large、d3-turbo、AutoTrust-JEV-27B-v1.1（文本）。
现有四种协议适配、services 生命周期和算子执行不另起一套。

## 统一入口和边界

- 新增显式 `deployment.performance: optimized` 预设；省略保持已有配置。
  三个原生 Torch 后端选择有界 CUDA Graph / 独立 stream，JEV 选择 vLLM
  保持引擎配置的请求准备流水线。预设展开后的具体选项进入部署身份；有冲突直接报错。
- `options.cuda_graphs` 保留细调预算入口，适用于验证通过的 Torch 后端。
  JEV 复用 vLLM 的执行器，不叠加第二个图缓存。每模型公布实际能力。
- 不合并不同请求，不修改 padding/精度/校准/原生请求内问题计算顺序。
  batch=false 与 stream 并行分开。既有运行进程不会被切换配置或重启。

## 公共执行组件

共用配置、全局有界图缓存、热度准入、固定 lane 线程、独立 stream、共享只读
权重、工作区准入和进程 allocator 上限。捕获、审计、淘汰必须排空全部 GPU
工作。CPU 编码仍调用原生函数；输入字节、编码 token 和本次工作区均有上限。
不增加行调度、重试、journal 或后台捕获线程。

pplx 保留已经验证的原生 reference_logits 钩子。AMOS 包装原生 decoder
forward，仅缓存 last_hidden_state；FP32 读出和校准仍由原生实现负责。d3 保留
原生 state-once，分别包装 state / questions；保持原生请求内问题批次和排列
平均，不能替换为 pplx 前向或 ROCm 专用 fast path。

AMOS/d3 的 GPU 操作不止发生在包装函数内部。因此 GPU 读租约覆盖整个原生
调用；升级到捕获写租约前先同步并释放读租约，写操作结束后重新获取读租约。
所有其他请求也必须经过该门闩。图返回的张量树复制到普通 allocator，避免
上层继续使用已经被淘汰的私有池地址。各图独立 cuBLAS 工作区和私有池。

## 质量与失败

精确形状包含 tensor 的形状、dtype、stride 和所有非 tensor 参数值，不使用
近似长度桶。首次和周期性重放逐 tensor 精确比较原生结果。差异返回原生结果
并停用该实例图缓存；未知 CUDA 异常让实例失败，不能继续冒险返回结果。
完整回答（概率、标签、usage）还要在原生单条与并发路径间做 GPU 回归。
不能用“首个请求通过”证明所有未来输入绝对一致，验收明确覆盖范围和限制。

## 设计 review 必查

1. 原生计算路径和精度未改，克隆只共享不可变权重，可变模块状态隔离。
2. shared→exclusive 升级无嵌套锁死；等待捕获的请求不再发出 GPU 工作。
3. 图淘汰后原生调用仍可安全读输出；固定线程在图销毁之后退出。
4. 输入/工作区/图缓存/捕获工作量有限；过大输入不静默截断。
5. 并发错误隔离、显存预算、稀有长度回退、不同内容同形状、退化与关闭回归。
6. GPU 仅 0/1；独占锁和进程检查；测试进程独立；不修改环境/权重或重启服务。
7. 逐模型报告吞吐、延迟、显存及实际 graph 命中，未测路径不能称为生效。

此为实现前方案；最终实现 review 和实测结论另列，若某个原生阶段无法安全捕获，
应保留其原生执行并明确记录，不能为宣称全局加速改变模型语义。

## 后续模型如何复用

用户补充要求：高性能是平台长期能力，不能四个模型各写一套优化脚本。

公共 `cuda_graphs.BoundedExecution` 管理所有 Torch 后端的资源与生命周期；
`NativeGraphAdapter` 只依赖一个小的后端接口，不判断模型名：

- `prepare(request)`：原生 CPU 编码和精确 token 预算；
- `clone(runtime, lane)`：共享只读权重，隔离模块状态，接入可捕获的原生 tensor 阶段；
- `execute(model, request, plan)`：仍调用原生协议实现；
- `workspace_estimate(tokens)` / `admission_extra(plan, tokens)`：申报常规和模型特有的驻留工作区；
- `profile`：观测标识；公共执行器不读取特定架构的 hidden/head 配置。

pplx 使用同一公共执行基类，保留已有 readout 图；AMOS/d3 共用 tensor-tree
图执行器。未来同类模型只增加后端薄适配和原生对照用例。生成模型继续复用
vLLM 自己的调度、KV cache 和图机制，不能叠加一套不了解其内部状态的执行器。
模型内核选择、Flash Attention、量化、padding、合批或精度变化均需各自验证，
不能由“高性能”预设越过模型语义。未适配/未验证的后端明确报告不支持该配方。

每个新增模型的准入还应比较冷/热、长尾长度、单条/并发的吞吐、p95、显存和
原生输出；配置、运行时、模型 revision 和硬件一起留证。不能用 GPU 利用率
代替吞吐，不能为了显示 100% 填充无效计算，也不能承诺任意模型均达到峰值。

## 实现中发现并修正

- d3 原生 `prepare` 使用位置参数，包装必须保留该调用方式并在失败时恢复。
- d3 state 缓存包括每层固定 recurrent matrix，以及卷积尾部切片保留的完整
  projection storage；预算只看输入长度或 view.nbytes 会低估，已单独计入。
- d3 questions 内的 CPU 列表转 GPU 索引不能捕获。仅提前构造 present/last
  索引，保留原生 `_layer`、norm、掩码和精度；对照仍直接调用未改动的原生
  questions 方法，不让改造后的函数自己验证自己。
- 图输出返回上层前复制，升级写租约前先同步，避免下一阶段捕获/淘汰释放它。
- 图片/视频暂不声明图加速，走原生独占 GPU 保护；文本普通回退仍可独立并行。

- 最终架构 review 将通用执行器中的 Qwen 工作区公式移到显式选择的后端配方；
  新后端可以提供自己的正整数 byte 估算，无需伪造 Qwen 配置。
- 新增单条性能基准入口，复用已有有界 HTTP 验收工具，报告采用独立 schema。
  单条/并发测试通过不能作为合批证据；同时比较回答和 usage。

- 正式 CLI 复验发现 prompt 的冻结配置不能直接传给 `json.dumps`；启动配方
  改用已有 canonical 序列化，并加入从真实 `parse_prompt_model` 到启动参数的回归。
- JEV 原适配器丢弃了 vLLM 已提供的 usage。单条路径现在累计每个问题的实际
  prompt/completion tokens；缺失时不编造统计，非法 metadata 报 provider 错误。
  候选 batch 只有在 provider 总量与独立 prompt token 数及每 prompt 一次输出
  严格一致时才分配到各行，否则拒绝错误归因。生产 batch 仍未开启。
- 同时记录可选 Triton/FLA/causal-conv1d 等 kernel 包版本及缺失状态，以便后续
  模型性能与质量复验能辨认实际运行环境。
- JEV 跨配置复验发现概率变化，后续也发现旧/新原生实例之间的变化；新的
  容量 64/1 串行配对试验反而完全一致，不能把根因归到容量参数。同一配置内
  串行/流水线一致也不能证明跨启动不变。统一预设不再改变 vLLM 容量，而是设置
  `request_pipelining: true`，仅在后端 HTTP 请求期间持锁，保持至多一个在途
  原生请求，允许 CPU 编码和读出与之重叠。原生容量、精度和插件保持不变。
- 图校验失败会关闭图加速并返回原生结果，但保留已验证的独立原生 lane；
  不以取消真正并行来掩盖图路径的问题。未知 CUDA 错误仍使实例失败。
