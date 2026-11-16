# 普通 Dataset worker 不再初始化无关的 Lance 写入栈

2026-11-16。修复生产提交 `6cc29ff`，真实红基线测试提交 `8872887`。隔离树 `gpt_v3_regression_20261115`，未推送、未合主仓。**原20k同预算小规模交替比较中位耗时降低7.92%，采样进程树RSS也下降；这只是有收益的启动优化，不代表完整性能意图已经通过。**

## 第一手归因与最小修复

在普通 `map → filter → map → iter_rows` 的真实20,000输入/10,000输出上，worker K3处理函数内的 profile 显示：

- `VerifyArtifactAndCapabilities`约0.510秒，其中原platform来源摘要约0.415秒；两次 `load_sealed_dataflow`合计只有约0.017秒。**保留所有原封存、代码摘要及身份校验**，不引入跨worker缓存。
- `Activate`约3.713秒，其中 `OperatorKernel._create_instance`无条件导入 `dataflow_stream_lance`约3.595秒，继而加载Lance namespace及366个Pydantic调用校验器。普通算术函数并不使用该能力。以上是受profile扰动的归因，嵌套累计时间不能相加或充当无profile性能对比。

生产只改 `execution/kernels/operator.py`。实际原生 `StreamLanceCall`的canonical模块已经在原封存程序加载时导入；识别时从既有 `sys.modules`读取该canonical类型，与实际instance类型以 `is`比较，然后仍走原 `bind`。普通callable不再为类型判断初始化Lance；不读取用户类/实例属性，也不调用用户重载相等方法。其他三种原生调用类、原bind、原权威和生命周期不变。没有备用执行器、缓存或新协议。

最初全线程profile退出时写出被关闭截断，只有驱动时间线可读，未据空profile下结论。保留 `/tmp/demiflow-current-worker-profile-r1`。有效采集改为原K3函数返回前写出，`r2`真实完整结果通过，observer安装与profile文件均有摘要；此探针只在/tmp，不进入生产代码。

## 功能证据

固定 `8872887` 的新测试在真实独立worker中复现：输出正确，但普通map已经导入Lance，**1 failed**。固定 `6cc29ff` 同一断言通过，仍验证实际不同PID、原ACK和单个action。

修复后固定组合 **19 passed / 2 skipped / 173.77秒**：

- 新普通callable进程测试1项。
- 原 `test_runtime_v3_stream_lance.py`完整16项，含真实写入/固定版本、空输入、拒绝不覆盖、六个实际进程崩溃恢复断点、未知外来版本、记录成员变更、取消和原Arrow双副本拒绝。
- 原producer实际SQLite的native unit、Dataset cut两条恢复缝2项。
- 另外2个PostgreSQL参数因没有任务专用DSN跳过，**没有算通过**。

未弱化已有测试。红、绿两组各2191份源码/测试输入逐项与对应git archive比较，零不符；测试前后源码与HEAD不变，子进程为空，承重七件与原冻结版本相同。完整命令、三私有资源域、实际import路径、原Junit在回执中。

## 同预算交替屏测

同一原harness，基线 `1f90c93`，候选 `6cc29ff`；两者生产字节只差上述一个文件。原输入20,000、输出10,000、worker上限4、256MiB、120秒保持；每臂独立warmup，再按 A/B/B/A 各测两次。总控协调暂停其他测试lane期间执行，未停止用户已有服务。

| 次序 | 基线A（秒） | 候选B（秒） |
| --- | ---: | ---: |
| warmup | 17.071439 | 14.399679 |
| 首次测量 | 16.066573 | 15.120862 |
| 反转次序测量 | 16.371811 | 14.747612 |
| 两次测量中位数 | 16.219192 | 14.934237 |

候选/基线比0.920776，即耗时减少7.92%。六次输出的多重性敏感摘要一致、原40个cut全部ACK、原资源池无在用额、所有观察到的子进程退出、没有Ray Data导入；每次三私有域独立。每次1050生产文件摘要与各自提交逐字一致，harness/计划摘要保持。

采样进程树RSS峰值：两次基线255,524,864 / 256,258,048字节，候选142,856,192 / 143,294,464字节。**这是进程树采样之和，可能重复计共享页，并非硬RSS上限证明。** 没有用扩大预算、增加worker或减少输入换取改善。

原历史约1.872秒差距仍在；本屏测不代替五轮完整性能矩阵、不含Ray性能对称性，也不与root另一个GC候选未经组合测量就相加宣称收益。

## 回执位置

[机器摘要与逐组溯源](NATIVE-IMPORT-STARTUP-VERIFICATION.json)绑定：

- `/tmp/demiflow-startup-red-evidence/verification.json`、`result.xml`。
- `/tmp/demiflow-startup-fixed-evidence/verification.json`、`result.xml`。
- `/tmp/demiflow-current-worker-profile-r2/verification.json`与worker profile文件。
- `/tmp/demiflow-native-import-comparison-r1/report.json`及六份原trial。

此前无收益sendmsg候选已完整撤回；本交付不包含它。需要集成的代码提交仅 `8872887`（红回归）和 `6cc29ff`（生产修复），再取本次文档提交。
