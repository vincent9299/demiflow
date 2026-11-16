# 控制消息编解码性能与原契约验收

生产候选 `0d06e8a2ec365990407950fde1604bead185db8a`，前置集成 `b11a182`；只修改原 `contracts/dataflow_codec.py` 和 `dataflow_hosts.py::_event_from_dict`，新增专门的负例测试。主仓未改、未跑测试；七件冻结文件与 `362d8528c27ae60a0d6ae0ccc986a0ff93847de8` 完全相等。

## 修改原因与结果

原 driver 收到已经解析为 JSON 对象的 WorkerEvent 后，再 `json.dumps → bytes → decode_wire`；后者在校验闭合字段并调用原 DTO 构造器以后，又完整投影一遍 DTO、遍历预算、校验同一批字段。真实 fastCPU profile 中 53,026 次 decode 累积约18秒，属于平台核心控制开销。

新增 `decode_wire_document(frame, *, type_name, bounds)` 消费完整的 `schema/type/payload` 帧，保留同一字段白名单、原构造器、不可变 DTO、跨字段校验以及预算权威。一次结构遍历同时计算原 payload 的节点数/保守估计，以及完整帧的实际 canonical UTF-8/转义字节数和容器深度；不先编码完整临时帧，不加 trust flag，不缓存业务值。未知字段、NaN/Infinity、bool冒充int、非JSON值、循环/超深/超节点/超字节仍拒绝。

原 bytes 路线仍保留原始长度限制、解析前深度扫描、重复键和非有限常量拒绝；只删除构造后重复投影/重复校验。全部注册类型均单独验证构造后仍通过旧 `validate_dataflow_record`，且重编码为原字节。Record 的既有 typed business codec没有修改。

`encode_wire`、schema类型表、closed字段验证/构造器、identity与历史摘要函数均未改；15个相关函数源码逐字节核对，证据在配套JSON。出生身份与 ACK/提交/资源权威没有改变。

## 性能证据

受控微基准：同一真实 data WorkerEvent，先热身500次；每侧每轮1万次，5轮交替顺序。

| driver路径 | 中位耗时 |
|---|---:|
| 原 JSON 往返＋原解码 | 0.9663秒 |
| 新闭合对象解析 | 0.5630秒 |

该控制路径耗时下降 **41.7%**。这不是整个 Dataset 的性能提升；真实20k→10k、256MiB、4worker的完整性能对比由性能任务另跑。

worker 发送路径也做过候选比较，但精确字节计量的Python成本使该局部路径慢约8%，因此撤回了该变更：`worker/bootstrap.py`与旧发送字节/协议完全不动，未交付无生产消费者的encode helper。前两轮探索结果仍保留在 `/tmp/demiflow-control-codec-benchmark`，最终候选的正式微基准是 `result-final.json`。

## 固定候选验收

- **281 passed /18.32秒**：原 C 类型/身份、H 宿主/出生身份/回执与原数据编码测试，加完整注册类型对照、Unicode/控制字符实际字节、完整帧深度、未知字段、节点/保守估计约束等新负例。H是明确标注的协议测试替身，不冒称真实Ray。
- **45 passed /1 failed，164.67秒**：真实Local标量与tuple/bytes/有符号零、Arrow/Lance/Parquet schema、materialize和原提交恢复、取消/迟到消息、六个真实SIGKILL提交窗口、真实driver重启拒绝旧worker事件、原worker远端journal幂等。无skip，唯一失败如下。

唯一失败 `test_resume_tightens_live_worker_envelope_without_rewriting_sealed_plan` 已在改动前 `b11a182` 的独立 `/tmp` 源码归档上、用进程内导入路径断言复现完全相同的 `DID NOT RAISE Interrupted`（5.27秒）。其 `_three_stage_dataset` 当前只产生2个可同时驻留stage，worker上限2使原计划直接成功并final ACK，不调用测试拦截的`_retire_phase`。本补丁不改拓扑或该测试，未弱化断言；该旧夹具与融合设计的衔接已交总控收口，不能把本组写成全绿。

两组各1,795份输入摘要相同，均与交付提交逐字节一致，运行前后0变。原SIGKILL、过期事件、远端journal证据及日志/JUnit的文件SHA见配套JSON。

## 执行环境纠正与未完成项

首次 `/tmp` runner 的脚本目录令Python把既有 `/tmp/ray` 当 namespace，导致测试收集失败；只修私有runner的sys.path，显式cwd优先。下一轮忘记指定独立资源池，默认capacity已有满额记录，在worker启动前被原准入拒绝；仅中断自己的测试driver，改用 `/tmp/demiflow-control-codec-e2e-r3-capacity` 重跑。未清理共享池、未扩预算，原红记录保留。有效baseline只认 `baseline-pinned.log/xml`，早先从候选cwd运行的baseline命令未作为对照证据。

本包未独立重跑新候选Ray矩阵，也没有把微基准代替完整性能验收。最终根分支仍需集成控制codec、恢复引用及其他改动，再用整体Local/Ray/工作台与性能结果判断两大原始意图；本包不声明100%。
