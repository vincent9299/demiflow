# Ray 性能取证接线：已验证与未通过

本轮仅改性能 harness、专用守卫测试与计划，不改生产。生产被测版本为本隔离树 merge `339f095`（包含 root `1120dcd`）；完整生产摘要、当时 dirty 状态及 harness/计划摘要均在各 trial.json 的 `source_before/source_after`，每次相等。后续基线策略元数据及守卫扩充发生在上述实际试跑之后；不把旧摘要冒称最终 harness 摘要。

同机真实 fast_cpu 原 20000→10000、4 workers、256MiB、120s 通过；receipt `/tmp/demiflow-perf-ray-observer-r2/fast_cpu/trial.json`，cluster `/tmp/demiflow-perf-ray-observer-r2/cluster.json`。49 个原 ACK（最后 final），first 8.4549s、完整 60.7926s、prepare/start 7.8876s。两名业务 actor 各自绑定原 allocation / spec / boot / GCS / PID / birth，1178 次节点采样，原清理 CONFIRMED + GCS DEAD + OS 同 birth 退出，测试监视器退出，private cluster children=[]。注册开销 0.0247s；节点监视线程累计采样 2.131s，预建 1.453s 在业务计时外。driver 与 worker 独立峰值之和 690892800B，不是同时 RSS 或 managed memory 硬上限。

两机 fast_cpu **失败**，不能标性能完成。receipt `/yzp/zhaozy/yangzepeng/0905/_demiflow/physical-host-tests/perf-observer-20261116-r1/fast_cpu/trial.json`，同目录上一级 `cluster.json`。120s 原预算耗尽，含异常收尾 122.3152s，first 22.1824s，prepare/start 20.8057s，仅 16 个非 final 原 ACK。未增预算或减输入。原业务 actor 确在两个物理 hostname 各一名，2403/2415 次真实节点采样；取证本身 errors=[]，各节点累计采样 2.241/2.123s。两名原分配 CONFIRMED、actor DEAD、PID/birth 退出，两监视器退出，本机与远端 owned cluster children=[]。这是有效的失败与资源清理证据。

两次试跑与 root 的 CPU 功能测试同时运行，只验证观测接线和原预算行为，不给出与旧版本的性能比值，不计正式五轮。首次冒烟额外选了不在 Ray 承诺矩阵的 small_data（原 Local 2 thread workers）；它在编译期 RESOURCE_INFEASIBLE，无业务 actor，证据 `/tmp/demiflow-perf-ray-observer-r1/` 保留，未扩大该格承诺。

精确可执行环境入口（自建 private cluster，token 仅内存/管道传递，不记录、不连接默认集群）：

- 同机：`/tmp/demiflow-perf-ray-observer-runner.py`，env `DEMIFLOW_PERF_RAY_OUTPUT=/tmp/demiflow-<owned>`、`DEMIFLOW_PERF_RAY_CASES=fast_cpu`、`DEMIFLOW_RESOURCE_ROOT=$DEMIFLOW_PERF_RAY_OUTPUT/cluster-domain`、`PYTHONPATH=<worktree>:<harness-tree>/tests`、`PYTHONDONTWRITEBYTECODE=1`；工作目录为被测 worktree，解释器 `/yzp/zhaozy/yangzepeng/0905/env-ray/bin/python -B`。head 明确 4 CPU、0 GPU、128MiB object store。
- 两机：`/tmp/demiflow-perf-twohost-observer-runner.py`，output 改为 `/yzp/zhaozy/yangzepeng/0905/_demiflow/physical-host-tests/<owned>`，每节点 2 CPU、0 GPU、128MiB object store，SSH 只启动自有 node，采用共享文件系统，不复制代码。业务 `http_host=10.233.68.27`；remote stdin 管道负责关闭自己 node。其余环境同上。

仍未完成：最终候选固定后的 Local 十格 / Ray 四格 / two_hosts 四格原计划五轮配对、每格 warmup 与取消；旧 Ray Data 性能对照；不可观测第三方复制计数。共享 Ray 服务/object store 的物理内存没有偷算到业务 RSS。计划阈值 1.2、输入、预算、120s 与五轮规则未降低。

## 后续同机原负载与完整性反例

生产同前、harness 固定 `c879676` 的 slow_api 真实通过：`/tmp/demiflow-perf-ray-observer-slow-r1/slow_api/trial.json`，64 次真实请求，first 11.1506s 且早于源 EOI，完整 19.0521s，3 原 ACK 最后 final，3 个原 actor 全部释放及物理退出。cluster children=[]。

同机 finite_sealed p1 **失败**：`/tmp/demiflow-pr-finite1-r2/finite_sealed/trial.json`，first 88.4270s，原 120s 时限耗尽，含失败清理 120.6723s，120 个非 final 原 ACK。原 40000×8192B 双侧、空值/重复、native 256MiB / RSS 2GiB / threads2 / partitions1 全保留，3 个 actor 采样与原清理/物理退出全部闭合。私有 cluster children=[]。首次 cluster 初始化因证据目录过长触发 Ray AF_UNIX socket 107 字节限制，未跑业务、children=[]；保留 `/tmp/demiflow-perf-ray-observer-finite-p1-r1/cluster.json`，独立短临时 socket 目录重启后才是上面实际业务试跑。

另补封闭反例：最终 `RayObservations` 对原 run journal 全部 allocation ID 作有限读取，要求与实际采样集合精确相同，不能仅证明自己登记到的子集。上述四份原业务 receipt（同机 fast、slow、finite 与两机 fast）已做只读补查，完整 ID 集均相等、所有 cleanup=CONFIRMED；报告 `/tmp/demiflow-perf-ray-allocation-completeness-20261116.json` 绑定各 receipt SHA，未回写旧 receipt。新的失败 receipt 另记录 `public_rows_observed`，只计实际从公开 iterator 取到的行，不把部分超时输出冒称完整结果。正式最终报告要求这两个新增字段，旧预热不冒充正式样本。

守卫验收：`/tmp/demiflow-perf-ray-guard-r3.xml` 为 51 passed、1 deliberately deselected slow integration test；原合约、基线身份、空/漏采样、未释放、单机冒两机、额外未观测 allocation 均有拒绝反例。真实业务证据见上，不把 51 个守卫测试数当性能样本数。B1-Ray 隔离参考树 `/yzp/zhaozy/yangzepeng/0905/_demiflow/worktrees/gpt_v3_perf_rayref_20261116` 固定 `2fbe839`，不在主仓运行。
