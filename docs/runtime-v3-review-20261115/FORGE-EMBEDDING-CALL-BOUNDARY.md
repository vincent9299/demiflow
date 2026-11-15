# 原 EmbeddingModel 接入与真实运行时契约闭环

基线 96c6d4b；本交付在同一隔离分支完成。主仓、外部数据、其他工作树、共享环境未修改；没有 push 或合主仓。

## 实际改变

- 原公开 demiflow.embeddings.EmbeddingModel 加入当前平台只读目录，附纯值声明；用户原 import 与构造形态不变，没有新 model=dict API。已声明平台值类的 builtin 空容器 factory 可静态表达；自定义 factory 保持 opaque，检查器不调用它。
- Dataset.map_embeddings 复用原 embedding_execution_config 与 demiflow.embeddings.v2 身份形状。静态报告绑定 endpoint、credential 名称、有限 max_requests、队列/并发/请求/响应资源；live service、caller-owned gate、私有 SQLite、profile 文件不能被该管理入口隐式授权。request_policy 继续原声明协议。
- 真正修复通用调用缝：compiler 的 native factory 现在保留原 CallableSpec.runtime_contract；原 AsyncInstance 在实际调用前验输入、resolve/await 后验输出。原 BoundMap 已有校验保持；actor 原对象、状态 owner、astart/aclose 权威不替换。RuntimeContractError 用原闭合异常传输词汇准确回传。
- 真实向量契约为量化到 float32 的 list[Python float]，长度等于 dimensions。保留原返回值与历史身份；固定长度校验只作用于明确携带 length 的新字段。显式 Arrow float32 列真实落表验证。

## 曾经的错误推断已撤回

我曾根据“float32 vector”描述推断返回 ndarray，写出了多余的 ndarray 声明。实际 native 回调校验接通后，真实正例立即失败；亲核 embeddings/payload.py::vectors 发现原实现使用 struct.pack/unpack 量化后返回 list。已撤掉 ndarray/Arrow 适配扩展，按源码修正，未改变原模型返回值去迁就设计。早先静态正例通过不足以证明回调契约已被消费，这是本次加入真实错误注入的原因。

## 实跑证据

所有命令均在本工作树，PYTHONPATH=$PWD:$PWD/tests，env/bin/python -B -m pytest，DEMIFLOW_RESOURCE_ROOT 与 --basetemp 全部 /tmp/demiflow-*。

1. /tmp/demiflow-check-call-r2.xml：17 passed / 59.58s。sync/await/batch 各输入/输出错误在真实 Dataset → 原执行器 → sink 前拒绝；输入错业务函数没有执行，输出错不发布；原 actor start/close 各一次；fn_kwargs 与无契约原调用兼容。真实 Embedding HTTP 确认后把向量维度改错，原 factory 携带合同，真实调用边界拒绝且目标未发布。
2. /tmp/demiflow-checked-regression.xml：72 passed / 139.15s。含 19 项新测试、原 Forge group/prepend 10、ordering 17、platform discovery 24、原 managed image 2（生成 → read_object → 同流 Lance / 再 vision 打分）。语义身份对照相等，custom default_factory 未被调用。
3. 最后移除文本协议不需要的 numpy 安装前提，并补精确输出 ID 列表和实际 Arrow float32 类型断言；/tmp/demiflow-browser-final.xml 中 Embedding 11 项全部通过。该文件整组 12 passed / 1 failed（162.74s），不能称全绿：剩余为准备态声明图源码请求走错执行桥接口，见下面未完项。
4. 浏览器另批 /tmp/demiflow-browser-r2.xml：45 passed / 2 failed（607.85s）；真实运行封存图/原身份桥/源码、配置错误修复 → 真正运行 → 固定结果、多 action 选择及相邻 facts 已过。两处旧“结构校验通过”文字期待改为页面真实的“声明投影结构（未执行）”，保留节点/边/共享/源码/响应式全部语义断言。后续暴露源码路由缺口，未隐藏。
5. 原同机 Ray Core/thread 专项 /tmp/demiflow-ray-v3.xml：固定 ccb648a 12 passed / 118.51s，真实原生 HTTP、有限/持久切换、thread/process、资源与代码 pin，不能冒充本新提交的全 Ray 或多机验证。

配套 FORGE-EMBEDDING-CALL-BOUNDARY.verification.json 保存基线、各修改输入摘要与报告摘要。承重七件逐字节匹配 49b666f。

## 尚未完成，独立列出

- prepare-only run 的声明投影点击源码，返回 Execution node is not bridged to a sealed report node。正确修法是按明确图来源读取当时 sealed report 的源码；真实执行图仍须验证原 bridge，不能回退当前代码。
- 总控新派原产品 test_operator_foundation_runs.py 的 flat_batch 预算控制余量，以及 actor / actor_instance 生命周期观测缺口，后续单独修复验收，不包含在本提交完成范围。
- 产品 image/document Embedding 尚未开放；引擎 98 API 矩阵的已有验收不代替这两个产品入口。现 authoring 明确拒绝缺对象读取授权的模态，没有仅根据数据中的 URI 授权文件/网络。

### image/document 下一步的具体原接口

原 workspace.resolve_flow_resources 已支持 kind=objects、parameter、root、relocations、layout，校验 root 等于已注册 location 并封存 location_id；collect.flow_contract.object_resources / normalize_object / analyze_read_object 与 objects.ReadLocalObject 已实现根目录和迁移边界。应复用这些原绑定作为 Embedding 模态读取权限事实，绑定原 action/occurrence，不能新增平行权限或数据库。

目前 embeddings.payload.image_content 直接 open_object(ref.uri)，embeddings.documents.DocumentInputReader._blocks 直接 read_document(ref)，两处都没有接入已封存 workspace 根/迁移能力。补接必须让原 NativeEmbeddingFactory/bind_model_services 获得可核对的已封存对象位置，复用原受限读取器；所有 alternate refs、document_ref、原 SHA256、尺寸/解码/缓存预算仍需验证。来自表里的任意 URI 本身不是授权。静态 authoring 的资源声明和运行时这两个读取点必须一起闭环；单加 effects=objects 不算完成。无需发明另一种 EmbeddingModel 或旁路执行。

- 原 embedding 4 秒首批反例已保留 /tmp/demiflow-timer-cut-trace.jsonl，实际 +1.004s 全 pause、+2.476s 原生输出、下游待源约 4s。总控另修 source-first quiesce；本提交不改计时阈值，也不冒称已在本树复验其修复。
