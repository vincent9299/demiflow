# 字符串长度预检窄优化

基线 `1997f7a5ec174fea0c73a9a3cb4eae6e23cb5d54`，独立工作树 `gpt_v3_string_size_20261117`，分支 `engine/gpt-v3-string-size-leaf-20261117`。生产仅 `dataflow_record_frames.py` 新增 9 行：一个预编译转义字符 regex，加 ASCII 普通字符串的 `len(value)+2` 路径。含转义 ASCII 保留原 native count，非 ASCII 原分支逐字保留。

没有先调用 `json.dumps` 分配编码副本，没有业务对象缓存；原 JSON/JSONL carrier、长度及 resident 预算、准入顺序和提交权威均未变化。密集转义仍用原 C 层 count，避免把每个转义符变成 Python 匹配循环。

## 取证与验证

已有 `fast_cpu` profile 的业务进程 `_string_size` 为 290,000 次、累计 6.193539 秒；driver 为 120,000 次、累计 2.576214 秒。普通 ASCII 原来每次执行 34 次 count 与两个生成器。profile 业务虽完成，整体汇总因一个空的 worker-command profile 失败；46.25 秒是带探针运行耗时，不能当正式基准。这些累积时间也不能直接相加成端到端可节省时间。

固定 `d482b42` 定向集合 **23 passed / 1 failed，8.38 秒**。唯一失败是原非 final 恢复测试的故障注入函数未接已有 `native_queue_state` 参数，抛 TypeError。`725dc86` 只修测试 wrapper 的同签名透传，原 ACK 后中断、同 RunRef 恢复、旧提交前缀逐字保持和 materialized 回读断言不变；该原用例单独重跑 **1 passed，10.11 秒**。两个版本生产文件摘要相同，不把这些结果写成同一版本一次 24 passed。

覆盖包括：全部 ASCII 单字符与控制字符组合、引号/反斜杠、UTF-8 长度边界和混合 Unicode、单独 surrogate 的既有拒绝、1MiB 普通/密集转义输入不产生编码副本（tracemalloc 峰值 <32KiB）、原帧与行历史字节逐字一致、精确行限额、控制字节与 resident 不足时编码前拒绝、真实小预算 Dataset、非 final ACK 恢复、源 pending 恢复、流式 manifest 原字节与提前拒绝。

两份回执均包含固定 HEAD、完整命令/解释器/import、2,302 个源码摘要、冻结七件、setup/call/teardown 各一次、四个私有环境域；无测试期源码变更，无存活子进程。首次新测试的 `json.dumps` guard 恢复过晚，误拦 pytest 证据记录导致 internalerror，原日志保留；修正仅将 monkeypatch 放进局部 context，并给 1MiB 参数添加短测试 ID。

## 纯函数微基准

基线函数从固定 Git blob 的 AST 提取，候选为本树实际 import；同进程预热后七轮交替次序。每轮累积长度 checksum 对照真实 JSON UTF-8 编码，输入、生产源码、runner 和完整轮次均有摘要。仅证明该函数成本变化，**不能代替原 I08 整链性能门**。

| 输入 | 每轮调用 | 原中位数 | 新中位数 | 新/原 |
| --- | ---: | ---: | ---: | ---: |
| 短普通 ASCII | 120,000 | 0.498994 秒 | 0.018748 秒 | 0.03757 |
| 4KiB 普通 ASCII | 1,000 | 0.049208 秒 | 0.019868 秒 | 0.40375 |
| 密集转义 | 2,000 | 0.100081 秒 | 0.101329 秒 | 1.01248 |
| 非 ASCII | 1,000 | 0.204366 秒 | 0.201665 秒 | 0.98679 |

未修改的非 ASCII 分支约 1.3% 差异以及转义分支约 1.2% 差异仅如实记录；不宣称非 ASCII 加速。整链测试由总控独占执行。

## 另一个热点，仅报告

同一 driver profile 中 `raw_workspace` 20,000 次，由 `read_admitted` 直接调用 10,000 次（累计 1.613561 秒）和 `decode_document → admit_document` 再调用 10,000 次（累计 1.444397 秒）组成。基线 `dataflow.py:1778/1781`、`:1823/1826` 是相同 raw 的重复扫描，但两道调用承担不同的 retained/workspace 准入，不可直接删除其中一道。此 lane 没有改该函数、终端读取器或 worker runner，后续由总控统一处理。

交付顺序：`d38a7c7`（生产和新测试）→ `d482b42`（新测试 guard 生命周期）→ `725dc86`（旧恢复注入参数透传）→ 本证据文档提交。主仓、root 集成树、共享环境与运行服务未改，冻结七件保持原字节。
