# Ray 性能取证接线：已验证与未通过

本轮仅改性能 harness、专用守卫测试与计划，不改生产。生产被测版本为本隔离树 merge `339f095`（包含 root `1120dcd`）；完整生产摘要、当时 dirty 状态及 harness/计划摘要均在各 trial.json 的 `source_before/source_after`，每次相等。后续基线策略元数据及守卫扩充发生在上述实际试跑之后；不把旧摘要冒称最终 harness 摘要。

同机真实 fast_cpu 原 20000→10000、4 workers、256MiB、120s 通过；receipt `/tmp/demiflow-perf-ray-observer-r2/fast_cpu/trial.json`，cluster `/tmp/demiflow-perf-ray-observer-r2/cluster.json`。49 个原 ACK（最后 final），first 8.4549s、完整 60.7926s、prepare/start 7.8876s。两名业务 actor 各自绑定原 allocation / spec / boot / GCS / PID / birth，1178 次节点采样，原清理 CONFIRMED + GCS DEAD + OS 同 birth 退出，测试监视器退出，private cluster children=[]。注册开销 0.0247s；节点监视线程累计采样 2.131s，预建 1.453s 在业务计时外。driver 与 worker 独立峰值之和 690892800B，不是同时 RSS 或 managed memory 硬上限。

两机 fast_cpu **失败**，不能标性能完成。receipt `/yzp/zhaozy/yangzepeng/0905/_demiflow/physical-host-tests/perf-observer-20261116-r1/fast_cpu/trial.json`，同目录上一级 `cluster.json`。120s 原预算耗尽，含异常收尾 122.3152s，first 22.1824s，prepare/start 20.8057s，仅 16 个非 final 原 ACK。未增预算或减输入。原业务 actor 确在两个物理 hostname 各一名，2403/2415 次真实节点采样；取证本身 errors=[]，各节点累计采样 2.241/2.123s。两名原分配 CONFIRMED、actor DEAD、PID/birth 退出，两监视器退出，本机与远端 owned cluster children=[]。这是有效的失败与资源清理证据。

两次试跑与 root 的 CPU 功能测试同时运行，只验证观测接线和原预算行为，不给出与旧版本的性能比值，不计正式五轮。首次冒烟额外选了不在 Ray 承诺矩阵的 small_data（原 Local 2 thread workers）；它在编译期 RESOURCE_INFEASIBLE，无业务 actor，证据 `/tmp/demiflow-perf-ray-observer-r1/` 保留，未扩大该格承诺。

精确可执行环境入口（自建 private cluster，token 仅内存/管道传递，不记录、不连接默认集群）：

- 同机：`/tmp/demiflow-perf-ray-observer-runner.py`，env `DEMIFLOW_PERF_RAY_OUTPUT=/tmp/demiflow-<owned>`、`DEMIFLOW_PERF_RAY_CASES=fast_cpu`、`DEMIFLOW_RESOURCE_ROOT=$DEMIFLOW_PERF_RAY_OUTPUT/cluster-domain`、`PYTHONPATH=<worktree>:<harness-tree>/tests`、`PYTHONDONTWRITEBYTECODE=1`；工作目录为被测 worktree，解释器 `/yzp/zhaozy/yangzepeng/0905/env-ray/bin/python -B`。head 明确 4 CPU、0 GPU、128MiB object store。
- 两机：`/tmp/demiflow-perf-twohost-observer-runner.py`，output 改为 `/yzp/zhaozy/yangzepeng/0905/_demiflow/physical-host-tests/<owned>`，每节点 2 CPU、0 GPU、128MiB object store，SSH 只启动自有 node，采用共享文件系统，不复制代码。业务 `http_host=10.233.68.27`；remote stdin 管道负责关闭自己 node。其余环境同上。

仍未完成：最终候选固定后的 Local 十格 / Ray 四格 / two_hosts 四格原计划五轮配对、每格 warmup 与取消；旧 Ray Data 性能对照；不可观测第三方复制计数。共享 Ray 服务/object store 的物理内存没有偷算到业务 RSS。计划阈值 1.2、输入、预算、120s 与五轮规则未降低。
