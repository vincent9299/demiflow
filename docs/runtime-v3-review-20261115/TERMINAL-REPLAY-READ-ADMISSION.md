# take_all 恢复读取：原读取额度在解码前闭合

结论：c04 的 worker 全量上限、共享上游多重性和已提交前缀计数保持正确；独立审查发现并修复了 **driver 恢复回读先解码整批、再执行收集预算** 的真实缺口。本包没有新增持久账本或修改任何历史结果字节。

## 真实红证据

20 行，每行约 6 KB：真实 `read_json → take_all` 取得原 final ACK 后，业务在 action 之后抛错；同声明合法 `RunContext.resume`，仅用公开 local_execution 收紧到 workers=1、memory=96 KiB。旧 `_OutputSubscription.read` 一次返回 **20 个已解码 Record、121450 encoded bytes**，超过 **98304 resident bytes**，也超过 32768 的 batch window；随后 collect_rows 才报错。

原始证据 `/tmp/demiflow-takeall-replay-r3.xml/.log` 与 `/tmp/demiflow-takeall-review/replay-memory.json`。前期探针曾被程序封存大小、成功 run 不可建新 attempt、每 worker 最小内存三个既有守卫正确拒绝；这些是探针修正过程，不是平台缺陷。

## 实现

- `PublishedRecordReader` 沿原已 ACK manifest、segment 和 ArtifactStore 读取，最多保留一个待处理的原有界 wire frame。它先读 canonical `bp/eb` 控制前缀，通过准入后才调用原 typed Record decoder；不重写格式、来源、身份或收费记录。
- `_OutputSubscription.read` 原签名保留，新增内部 `read_admitted`。恢复前缀必须按原顺序耗尽后才消费 live suffix；本批同时受 row / byte window 限制。关闭时显式关闭历史 generator 和原 SH / 文件租约。
- `RuntimeActionHandle` 的易失读 credit 计入已交给 collector 的 rows/bytes 和尚未交付的预读数据。take_all 的额外记录探针保持 ValueError；它不解码、不收费，也不能把截断结果当成功。exact limit 和空结果仍可等原 EOI 完成。
- 原读缓冲预留 **4 × (max_row_bytes + 1)**：未完成行与下一块可能共占两个窗口，上游 store generator 仍持有该块，另有当前抽出的 wire frame。外层 JSON 显式 decode 后先释放 line，再解析；yield 前释放 document 字符串。检查 eb 不再整帧 decode。旧 tuple 改为逐项弹出的 deque，下一次 read 前释放旧批引用；collector / batch 处理完 Record 后主动解除引用。
- iterate_batches 只把现有 batch bytes 与下一 read credit 接在同一 resident 限制内，并将预读行数限制到本批剩余行数；schema、batch_size、尾批语义保留。`_convert_batch` 不在本包修改范围。
- 提前准入错误先成为原 Handle 的 failure，取消后迟到 ACK 的连接错误不会覆盖它。已确认提交、未知效果和费用原账本均不回滚或重新发起。

这些额度约束原 encoded 数据与 I/O / 序列化窗口，**不等同于任意 Python / Arrow 对象 RSS 的硬上限**。越额记录仍允许读取一个有界 wire frame / 控制头，不能把“未调用业务 decoder”说成“没有分配任何载荷字节”。

## 验证与版本

工作树 `/yzp/zhaozy/yangzepeng/0905/_demiflow/worktrees/gpt_v3_residency_20261115`，基线 `69152b3d53ed22496301b676e371f739b562e2c6`，生产 / tests 基于 c04。七个冻结文件逐字不动。

- 相关完整组合 **41 passed / 104.82 s**：10 新定向用例、14 原终端用例、15 T 句柄契约、2 原正式恢复；这是缓冲预留最后由 3R 收紧到 4R 前的完整组合。
- 最终 4R 生产字节：22 个受影响用例中 **21 passed**，一个新增观察断言错误地要求预算失败前必须返回至少一个窗口。更早在第一批返回前拒绝是正确行为；修正这个观察断言，保留精确解码数量、预算、无 worker 重启、原 ACK 不变和 FD=0，双臂 **2 passed / 7.08 s**。最终覆盖这 22 个用例，生产字节与上一轮完全相同；没有把失败算通过或删反例。
- 真实恢复正臂返回原 20 行、没有 worker 重启、原 execution commits 逐字不变；96 KiB 负臂预留原 I/O workspace 后只解码 **5 行**，第 6 条 metadata 拒绝，绝不返回截断成功。
- 两个真实共享上游 exact/overflow、两个已有 2 行 ACK 前缀续跑 exact/overflow 均通过；真实 typed repartition blocks 的 0 / 1 条 metadata 探针不额外解码，原提交不变，关闭后原 run FD 为 0。
- 64 行、每行 6 KB 的 iter_rows 和 Arrow 两行 iter_batches 可以流式读取总量超过 resident budget 的完整输入，未误用累计收集额度。原 durable after_consume 与 native late-final HTTP 恰一次恢复通过。
- 每个固定源码跑次都保留 XML / 完整失败栈。最后三个跑次均记录全 Python 输入前后摘要且无运行中变化；配套 JSON 保存原命令、观察器全文、输入摘要、逐 case 结果和版本差异。

当前完整包的原始 XML：`/tmp/demiflow-replay-admission-final.xml`；最终受影响复验：`/tmp/demiflow-replay-admission-encoded-final.xml`、`/tmp/demiflow-replay-admission-closure.xml`。普通 iter_rows 显式 early-close 的另一个信用等待问题由 regression 路新复现，已承接为后续独立修复；本包不把它误称已闭环。没有重跑全 API / Ray 矩阵，没有安装、push 或改主仓。
