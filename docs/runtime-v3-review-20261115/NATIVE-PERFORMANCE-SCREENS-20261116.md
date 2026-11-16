# I08：原生链路性能取证与两项未采纳候选

日期：2026-11-16。分支 `engine/gpt-v3-residency-20261115`，隔离树
`/yzp/zhaozy/yangzepeng/0905/_demiflow/worktrees/gpt_v3_residency_20261115`。

结论：**这两项候选均未采纳，完整性能门仍未通过。** 多帧发送经过真实协议边界测试，
但尚无有效整链收益证明；Record 浅层复制的微基准收益仅约 0.093 秒，不足以承担特殊构造的复杂度。
该阶段回退提交 `5f2cdf6` 的全部 `demiflow/` 生产字节与候选前 `b91b3a9` 相同。
没有修改主仓、共享环境、冻结七件或资源/提交协议；没有 push。

配套机器证据：`NATIVE-PERFORMANCE-SCREENS-20261116.json`。其中记录原始报告摘要、
固定提交、六次完整命令/私有环境、资源采样、原 ACK 与清理情况。
本文件不能替代原始 JSON/测试回执，也不能作为完整五轮性能验收通过证明。

## 勘误：源码存储位置混杂，旧数值不能证明性能因果

后续检查证实本报告 A/B 的基线源码在 `/tmp` 的本机 xfs/NVMe，候选源码在 `/yzp` 的 dpc
共享文件系统。虽然数据/输出目录、解释器、预算和代码差异受控，但计时包含 worker 启动、
源码导入与代码固定，因此**两臂存储环境并不等价**。原 +1.11% 数值和全部原始记录保留，
不能据此断言 iovec 让代码变慢，也不能据此证明其没有收益。此前文中“已否定收益”的措辞撤回。
未采纳/回退决定仍保留：没有有效收益证明时不引入该复杂路径；本轮不再重复 socket 试验。
后续终端重复编码候选将以两臂均在同一 `/tmp` 文件系统的隔离 Git checkout 重新对照。
只读环境核验见 `/tmp/demiflow-terminal-samefs-alignment.json`，这不替代新的性能结果。

## 1. 持久化与通信的第一手归因

在已经含 Lance 启动、Mapping 与 GC 改善的相同生产基线上，独立观察了 driver 和原权威线程。
报告 `/tmp/demiflow-cut-profile-r1/verification.json`；线程报告目录
`/tmp/demiflow-cut-profile-r1/fast_cpu/authority-profiles/`；原负载仍是 20,000 输入、10,000 输出、
最多 4 workers、256 MiB、120 秒。结果保持原 40 ACK、完整输出与资源释放。

这次加 profiler 的运行仅供归因，不能拿它的耗时与普通运行作性能比较。累计时间有重叠：

- 原业务线程中的 358 次 `store.put` 累计约 3.696 秒；40 次 snapshot 累计约 3.372 秒。
- 原 artifact RPC 共 1,192 次：begin 398 次约 0.759 秒、grow 78 次约 0.263 秒、
  prepare 358 次约 0.597 秒、settle 358 次约 1.392 秒，以上是权威实际 dispatch 累计。
  queue/reply 等待合计约 0.45 秒，证据不支持将主要成本归咎于轮询睡眠。
- driver 读取 10,000 条终端帧，`_read_peer_frame` 累计约 3.677 秒；这些包含等待，
  **不是可以直接删掉的 3.677 秒 CPU 开销**。旧 A/B 有上述存储位置混杂，不能由其判断该候选收益。
- journal 已复用连接；当前不存在可直接复用的批量 artifact 事务 API。
  begin 准入、写入、prepare、发布、settle 的顺序保护了有限资源与未知故障，不因优化省掉其中一项。

原 worker profile：`/tmp/demiflow-current-worker-profile-r3/fast_cpu/worker-profiles/19835-business.prof`。
其 70,000 次 Record 构造、逐行冻结/验证/编码，以及上述持久化路径仍是后续主要调查范围；
这些计数是观察事实，不能直接相加后承诺可消除的总耗时。

## 2. Record 复制：撤回生产，仅保留适用于原实现的守卫

候选 `f246b08` 只尝试内部已构造 Record 的严格标量 provenance 更新；复杂值、未知字段、
子类和可变替换仍走原校验。第一次固定组合测试 259 passed、1 failed；唯一失败是新增测试
误填 `encoded_bytes=256`，原 wire guard 正确拒绝。`ebc60fa` 将夹具改为真实编码长度，22 项通过。

8 次交替函数级微基准：每 20,000 次 clone 的中位约 0.23319 → 0.14056 秒，绝对差约 0.093 秒。
这不是 Dataset 整链加速。按总控裁决没有为该候选启动整链 A/B。
`32b72fa` 撤回生产；原实现上 22 项守卫再次通过。

- 微基准：`/tmp/demiflow-record-copy-micro.json`。
- 原实现回执：`/tmp/demiflow-record-original-guards-evidence/verification.json`。
- 保留测试：`tests/test_runtime_v3_record_copy_cost.py`，含类型/负零、深层可变别名、NumPy、
  非法对象、未知字段及原 codec/depth 校验。
- 不应单独挑取 `f246b08` 的生产优化。测试若需要集成，应仅取当前测试文件。

## 3. 多帧发送：协议正确，但整链无收益，完整撤回

候选提交序列为 `abccf5b`（5 项真实 socket 红基线）→ `5c6cdae`（唯一生产文件）→
`691f32e`（边界测试补充）。候选只改原 `exchange/dataflow_transport.py`：
借用已有准入缓冲，将原 header/payload 作为最多 64 个 iovec 发送，逐帧结束发送租约，
不更改字节、hash、顺序、ACK 或存储权威；短写沿原帧继续，未知错误不重放；限制绝对 deadline。

固定验收：

- 原基线 `abccf5b`：5 failed，精确证明新多帧路径尚不存在。
- `5c6cdae`：30 passed，覆盖真实短写跨 header、首帧独立 ACK、后帧部分错误/零写、关闭、
  原 worker digest/sequence/业务失败、cut/control 边界、FD 准入及终端 ACK 窗口。
- `691f32e`：10 passed，额外验证平台 iovec 上限为 1/3、空帧/重复 key、无 sendmsg 回退、
  部分进展不延长原 deadline、后续 key 缺失只清发送租约、不退原 credit。
- 所有固定回执均绑定测试前后相同提交/生产与测试摘要；无子进程残留。

对应回执前缀：`/tmp/demiflow-peer-iovec-{red,green,final}-evidence/verification.json`。

总控协调两路自然结束测试后，运行 2 次暖机 + 4 次交替测量。两臂仅这一个生产文件有差异，
基线树 `/tmp/demiflow-record-copy-baseline-20261116` 为 `b91b3a9`；候选为 `691f32e`。
原输入、预算、完整输出校验与提交要求没有改变。

| 阶段 | 基线（秒） | 候选（秒） |
| --- | ---: | ---: |
| 暖机 | 12.610604112 | 13.392474022 |
| 测量 1 | 13.441342534 | 13.400296352 |
| 测量 2 | 13.437282681 | 13.777878325 |
| 测量中位 | 13.4393126075 | 13.5890873385 |

候选/基线 = 1.011144523（原观测约慢 1.11%）。除采样噪声外，还存在上述源码文件系统混杂；
**该数值不提供可采纳或可否定的性能因果证据**。六次均为 10,000 条相同完整输出摘要、40/40 原 ACK、
原资源池 active/charged 清零、0 残留子进程、源码不漂移。

报告：`/tmp/demiflow-peer-iovec-comparison-r1/report.json`。
执行脚本：`/tmp/demiflow-peer-iovec-comparison.py`；完整参数/环境见配套 JSON 与每次 `trial.json`。
顶层命令（工作目录为上述隔离树）：

```bash
env PYTHONPATH="$PWD:$PWD/tests" PYTHONDONTWRITEBYTECODE=1 \
  DEMIFLOW_RESOURCE_ROOT=/tmp/demiflow-peer-iovec-comparison-resources \
  DEMIFLOW_RESOURCE_HOME=/tmp/demiflow-peer-iovec-comparison-home \
  DEMIFLOW_FORGE_DOMAIN=/tmp/demiflow-peer-iovec-comparison-forge \
  /yzp/zhaozy/yangzepeng/0905/env/bin/python -P -B \
  /tmp/demiflow-peer-iovec-comparison.py
```

`5f2cdf6` 撤回此候选生产和只适用于候选的测试，保留上述 Git 历史与原始证据；
因此不要把这三个候选提交并入交付分支。其测试删除不是降低已采纳功能的验收要求，
而是撤回尚无有效收益证明的新实现。原 peer/worker/终端测试保持。

## 4. 对原始意图的实际结论

统一 Dataset、真实预算/恢复与工作台实际事实不能以削弱校验或提交来交换速度。
两项试验没有改变原生兼容收口成果，也没有解决当前相对历史基线的性能缺口。
原生兼容交付见独立 `NATIVE-ADAPTER-CLOSURE-20261116.md`；本次没有重复其已完成的模型恢复验收。

本轮回退两项尚不值得采纳的候选，并把 artifact RPC 的等待与实际权威工作分开；
不能把存在环境混杂的对照扩大为“已排除这两条方向的任何整链收益”。
下一步若需要成批 Record/Block 执行或成批提交调度，应先明确原预算、cut、身份和未知故障的等价边界，
不能把更换底层网络调用当成已经解决性能，也不能把本 screen 的 passed 当作原 20% 性能门 passed。
