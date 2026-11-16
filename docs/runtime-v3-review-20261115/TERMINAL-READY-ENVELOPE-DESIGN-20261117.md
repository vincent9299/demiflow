# I08：仅合并已就绪 terminal 元数据的内部传输方案

状态：**待总控裁决，未实现、未验收、无性能收益声明**。源码核对基线
`96f77e7b7d61ea6b7252b98573fcb1c19ed027d3`。本分支从该基线开始，不包含两个
已否决的微优化候选。公共 WorkerEvent/Record DTO、identity、原封存、RunJournal、
CommitCoordinator、每记录 delivery/ACK 以及冻结七件不变。

## 1. 核对结果与优化位置

当前已经有三种合并，不能重复宣称新能力：

1. ControlMailbox.drain 只取现有 outbox 的 ready 事件，最多原 pending 上限。
2. bootstrap.send_messages 已把多个原长度前缀帧放进一次有界 socket 写入。
3. read_peer_many 已在一次连接读取多条原记录；8184 诊断中 10,000 条 terminal
   仅用了 456 个 peer 连接。因此不改 peer 载荷、连接、记录读出或 ACK。

仍然重复的是 bootstrap 对每条 terminal 进行完整 encode_wire→json.loads→外层
document；Local 接收端每条再经过 _event_from_dict→decode_wire_document→冻结
WorkerEvent 构造。它们重复携带相同的 run/attempt/fence/allocation/boot/scope/revision。
Ray 当前直接传原 WorkerEvent tuple；应保持该旧方法兼容，另用内部 opt-in 入口承载
同一种压缩封装，不能把安装 Ray 后实际跑 Local 当成 Ray 验收。

## 2. 建议的具体协议

只新增**不落盘的内部 transport frame**。原 4-byte 长度前缀仍保留，新帧正文为：

```text
b"DFTB1\0" + uint32_be(logical_event_count) + canonical_json(body)
```

固定短前缀让接收端在 JSON 分配前识别版本与逻辑数量。旧 JSON 单事件正文逐字不改。
body 的闭合集合是：

```python
{
    "schema": "demiflow.terminal_ready.v1",
    "header": {
        "run_id": str, "attempt": int, "fence": str,
        "allocation_id": str, "boot_id": str, "plan_revision": int,
        "scope_id": str, "controller_epoch": int,
    },
    "producer_partition": str,
    "entries": [
        {"operation_id": sha256_string,
         "payload": { ... original terminal payload minus producer_partition ... }},
        ...,
    ],
}
```

WorkerEvent.schema、kind="terminal"、payload_ref=None 是该版本的固定值，不从业务
输入推断。payload 中原 delivery_id、record_id、sequence、target_partition、edge_id、
peer/ref 全部保留；不计算新 record_id，不合并两个 ACK，不重写 peer key 或 SHA。
初版只压缩当前原生 terminal payload 的已知形状；其他合法 WorkerEvent 走原单事件。
未知 batch 版本、未知字段、错误类型以及 count≠实际条数拒绝，不静默降级伪造合法性。

只合并同一次 drain 的**连续** terminal 事件，且上述全部公共头、producer_partition
逐项相同。遇见不同分区、boot 或任何 progress/cut/EOI/failure/效果 RPC，先发送已有
组，再保持该原事件的位置。结束本次 drain 即发送；不等凑满、不 sleep、不加计时器、
不新建等待队列。只有一条或放不下两条时，原单帧路径立即发送。

## 3. 函数和文件范围（六个生产文件）

| 文件 | 新增／修改的内部入口 | 契约 |
|---|---|---|
| 新 execution/dataflow_terminal_frames.py | `_TerminalReadyBatch`；`pack_ready_events(events, *, bounds, byte_credit)`；`decode_terminal_frame(raw, *, bounds, event_credit, byte_credit)`；`logical_event_count(item)`；`expand_worker_events(batch)` | 私有 frozen batch 保留一个真实首条 WorkerEvent 作为公共头，以及原不可变 entry payload；不导出到 contracts。pack 为惰性输出，不持有下一次 drain。decode 验证整个封装后才暴露任何 entry。expand 仅供旧 poll 兼容，不在优化后的 driver 热路径调用。 |
| execution/contracts/dataflow_codec.py | 抽出 `_validate_worker_event_header`、`_validate_worker_event_entry`，以及共享的测量／字段校验 helper | 原 `_validate_payload(WorkerEvent)` 使用同一 helper；新 batch 同样逐 entry 验原 operation_id/payload 与 JSON 不变量。公共 encode/decode 输出、旧字节接受集合及错误不变；不得用手写宽松校验替代原 whitelist。 |
| execution/worker/bootstrap.py | `_run_v3_worker` 原 drain→send 位置；`ControlFrameReader` 私有新帧识别及接收 credit | 原单 reader、send_lock、wake、生命周期不变。新帧按 logical count 消耗 pending；仍用原 `_send_encoded_messages`。收包在 decode 前验证版本、长度与 count。 |
| execution/dataflow_hosts.py | `_build_launch` 私有 capability；Local/Ray `_poll_ready`；`_early_event`／`_send_k3` 对 batch 的窄分支；原 `_events` 的逻辑数／字节计数 | public `poll(...) -> tuple[WorkerEvent,...]` 不变，必要时经 expand 适配。driver 私有 `_poll_ready` 可返回原 WorkerEvent 或 private batch。残余只能留在原 `_events`，不能新增 pending queue。K3 等响应期间也正确接收、fence 和缓存。 |
| execution/worker/dataflow_host.py | 新内部 `pull_ready_events(limit)`，原 `pull_events`/`poll` 不变 | Ray 在原 mailbox.drain 上打包；limit 是逻辑事件数。旧入口永远返回原 DTO。没有新 Ray actor、第二 reader 或第二 in-flight pull。 |
| execution/dataflow.py | `_poll_control` 窄分派；`_handle_event` terminal 逻辑提取为 `_handle_terminal_payload(header, operation_id, payload)` | 原 thread/provider 与测试替身没有 `_poll_ready` 时仍调用 public poll。每 entry 仍执行原 fence/allocation/terminal membership/duplicate delivery/terminal queue credit 检查；原 `_ack`、limit/satisfy、delivery map、计数和失败次序不改。 |

公共 DTO 文件、worker graph runner、kernel/operator、placement、PeerBuffers、terminal
scratch 均不修改。这里确实需要 driver 的上述窄接收缝；仅改 socket sender 并在接收端
展开成 N 个完整 WorkerEvent，不能实现“减少 driver DTO 构造”的目标。

private batch 的公共头使用**真实第一条**原 WorkerEvent，不构造虚假 delivery 或零值
operation。其余每条的 operation_id 与 payload 在共享 codec 权威中验证并冻结。
driver 每条用该头执行 `_validate_event`；错误 context 用当前 entry 的 operation_id。
虽然公共身份完全相同，初版仍保留逐条 fence 调用，避免将优化扩大到生命周期检查。
只有反复构造 N 个完整事件及反复测量／解析共享头被移除。原终端 payload 检查提取后
必须由单帧与批帧共同调用，禁止维护两份 terminal 业务规则。

## 4. 准入、分配与残余处理

没有新增资源额度，也不能把一个 batch 按一个事件计费。设 P 为原 max_pending_events，
C 为原 control byte credit（同时受原 socket frame ceiling 限制），N 为本组条数。

- `2 <= N <= min(64, P, 本次剩余逻辑事件 credit)`。已有 mailbox 仍按原 N 个事件
  保留槽位；transport 只是它们本次 drain 的短暂投影。
- writer 在扩展 entries/序列化之前，按原 field walker 计入组公共头、每条变量字段、
  列表/条目开销、原事件仍存活、最终 raw 与 packet 副本。只有该合计在已有 byte credit
  内才压缩；不能先形成超预算大 JSON 再决定拆分。放不下时 flush 当前组或原单帧。
- reader 先读有限固定前缀；count 必须在 P 内。原 raw read-ahead + 正在解析的新帧 +
  已 decoded 但尚未交付的控制文档，合计仍在原 reader/control 额度内。新帧的严格
  pre-parse scanner 计算深度、token/node 数与保守容器/字符串分配量，再调用 JSON
  解码；不信任 sender 宣称的 memory size。沿用原有限原始缓冲，不能另缓存完整批。
- 上述预算是原受管载荷及保守对象分配额度，不能声称是 CPython/第三方进程硬 RSS。
  实现前需给具体计量常数及等价边界测试；若无法从原额度切出安全 workspace，该
  批退回旧单帧，不增加额度、不新增合法业务拒绝。接收端恶意／矛盾的新封装仍拒绝。
- Local poll 若先取出旧 `_events`，再看见一个新帧：只有该帧**全部逻辑条目**及
  raw/decode 工作区都能容纳才解析，否则先返回已取出的事件，未解码 bytes 留在
  原 ControlFrameReader。绝不先展开越界再切片。
- `poll(max_events=1)` 也必须前进：当没有已选出的事件时，可以在原 P/C 内完整验证
  一个 batch，仅返回第一条，其余用原 `_events` 保存一个 batch+offset 视图；释放
  已消费 entry，不能让视图永久保留整个历史批。剩余条数、字节和当前返回的条目一起
  计算，且只能同时驻留一个这种原队列残余，不再预读下一批。
- Ray 原单 pending pull 的 requested limit 仍为逻辑 limit。返回后检验所有 item 的
  逻辑数之和；caller 窗口缩小时也只用上述原 `_events` 残余机制。不可只验证 list
  物理长度，更不能每 item 再各给一份 P/C。

以上 pending/byte 计数只是原 queue 的消费投影，不参与 checkpoint、恢复或资源分配
权威。实际 row bytes 仍在原 PeerBuffers/source/terminal credit 中逐记录核算。

## 5. 协商与失败／恢复边界

`dataflow_v3` 私有 launch config 增加 opt-in `terminal_ready_transport=1`。缺省或不支持
仍发送原单事件。新 Local decoder 同时接受两种正文；Ray 新内部方法只由新 endpoint
调用，旧 public actor method 保持。新 transport 不写入 SealedExecution、Record、
code pin、plan identity 或历史 journal，因此历史单帧／历史结果仍按原规则读取。

批帧整体验证成功后才允许入 driver；其中一条结构非法时不得先 ACK 前面几条。
进入业务处理后则保持原逐事件次序：第 k 条触发重复 delivery、fence 或取消时，不
凭批头给余下条目制造 ACK。partial socket/write、Ray uncertain pull、worker death
仍走原失败／未知状态处理，不能“重发整批”造成双交付。已确认 cut 的恢复与 peer
ownership 的释放仍只由原 journal/ACK 路径决定。

## 6. 必需反例与验收

1. 等价差分：同一原 WorkerEvent 序列，旧单帧解码与 batch 后展开的 schema、身份、
   operation_id、payload、顺序完全一致。tuple/list、bytes refs、Unicode、转义字符、
   边界 int、NaN、bool-as-int、未知键、ref XOR、深度/node/byte 等原检查全部保留。
2. 连续性：terminal(A), progress, terminal(A), cut, terminal(B), failure，绝不跨过
   非 terminal 合并；同 boot 不同 partition、同 partition 不同 boot 不合并。
3. 首行：mailbox 只有 1 条时立即发旧帧，生产者等待该 ACK 才发第二条仍不会死锁。
4. 资源：P=1、小 C、恰好两条临界、单条合法但两条不合法、header 长度伪造、超大
   count、深度攻击、batch 残余+read-ahead+decoded 同时驻留，均证明原上限；不能
   提高测试窗口。max_events=1 多次 poll 有序前进且每时刻残余有限。
5. 协议：stale attempt/fence/boot/revision/allocation/scope、未授权 producer、非终端
   allocation、重复 delivery、篡改 peer SHA/size/token、错误 Record schema 均真实
   拒绝；payload 读取仍走原 SHA 后再暴露给用户。
6. 生命周期：K3 Activate/Drain 等待期间 batch、cut 前后、半帧断开、Ray pending pull
   期间窗口缩小、慢 consumer、early-close/cancel，以及第 k 条业务拒绝，保留原
   已开始 publication 的 ACK、无虚假 final、store/journal/run lease 实际退出后释放。
7. 真实 Local process 与 Ray Core 的 10k 输出、序列 oracle、原 cuts/ACK/final、
   恢复、资源归零及 child 清理。thread 路径保持旧 DTO，做窄兼容验证。
8. 先原 fast_cpu 同 FS／4 worker cap／256MiB／120s，无 profiling 一前一后；同时
   统计逻辑 terminal 数、实际 packed groups、平均/最大组、旧 singleton 比例、
   WorkerEvent 构造次数、总 control bytes、首行时间、CPU 与资源。无明确收益就
   停；有稳定信号才按原 V034 五对比较，不把 timing noise 算提速。

本次仅形成设计，不声称任何上述新增用例已经执行。

## 7. 可以消除多少成本

8184 现成 cProfile 诊断（18.49s，受 profiling 干扰）中，11,099 次接收事件解码累计
2.690s，内含 `_measure_document` 1.563s 与 DTO 构造约 0.604s。这些是嵌套值，不能
相加。10,000 条 terminal 占事件总数约 90%，但变量 payload 校验仍必须保留，因此
2.690s 只是这部分 driver 消除成本的粗上界，绝不是预测可省 2.690s。

新方案能降低完整公共头的传输/测量/构造次数，并减少 worker encode→parse→encode。
平均批次大小取决于真实 ready outbox，不能由最多 64 推断“64 倍提升”。已有
PeerBuffers recv 的 4.28s 含等待，不能算作被这个方案消除的 CPU。

现成 worker cProfile 探针只包业务 runner/K3，未包 bootstrap 主发送线程；另一 lane
也确认没有该线程的现成 profile。本次不将 py-spy 的等待采样充当精确 worker CPU。
若候选有信号，另用私有 probe 包现有 bootstrap 主线程量化变化；不改变工作负载。

因此本方案最多是一个有依据的结构性减负切面，**不能承诺靠它把约 13s 直接变成
1.87s 或关闭 I08**。其余原始记录编码、terminal scratch、恢复/提交与 startup 成本
需要真实测量后另行决定，不叠加此前已否决微优化。

## 8. 1.872162861 秒历史基线：实际能力核对

只读核对 `/tmp/demiflow-perf-preflight-b0-r3/fast_cpu/trial.json` 与其原 run.sqlite。
历史源码 `267b9144c6bf7447c6822aa716f6d5c0eda16990`，该条样本 phase 是 **warmup**。
原 20k→10k 业务和表面预算不变，但执行保证不同：

- 实际 SQLite：runtime_actions=1（iter_rows completed），runtime_sources=3；
  runtime_commits=0、runtime_receipts=0、runtime_artifacts=0、action_events=0。
  是实际 SELECT 证据，不是仅看 benchmark 的 v3 observer `cuts=[]` 猜测。
- 原 bindings 明文 `backend=local_pull`、`placement=driver`、
  **`recovery=attempt_only`**；有 source/code pin/plan 封存，不是“完全没有 RunJournal”。
- 原 `_execute_plan` 冻结源、绑定计划，`execute_native_plan` 分派 `_apply_plan`，
  末尾 action.complete(delivery=completed)。这条 iter_rows 路线没有逐 cut 的
  CommitCoordinator 与确认结果账本。不能把其他 write/materialize 路线有提交推及它。
- 原 local stats 是四个实际 process、79 个融合 Map/Filter/Map batch；当前典型纯线性
  计划 co-location 一个原 worker、每业务记录 terminal 与持久 cut。不得为了追旧时间
  恢复 attempt_only、删恢复或绕过统一图。
- 同一 trial 的 memory_bytes 为 256MiB，但 max_rss_bytes 是 4GiB，原封存 profile
  requests.memory_bytes=2GiB（`max_rss_bytes//2`）。这不是 256MiB 硬 RSS 证明；实际
  sampled process-tree peak 375,824,384B。不得称旧实现与新资源准入完全同义。

结论是**比较业务相同，但恢复与资源准入语义不完全相同**。保留原 >20% 性能门和
所有原始证据；不擅自换基线、不把旧 warmup 当五轮 median，也不把能力差异作为
当前所有回退的解释。对应只读取证摘要与输入 SHA 在配套 JSON。
