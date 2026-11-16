# I08 / V034 性能验收执行计划（总控已批准）

依据原 ACCEPTANCE.md §性能与 tasks/V034.md。实现只新增 `tests/runtime_v3_performance_scenarios.py` 与专用性能测试，不改生产、现有路由或原压力断言。四卡测试已释放 GPU；正式 GPU 测量等待 residency 窗口。正式五轮等待最终固定候选，当前可做 harness、历史基线正确性和预热探针。

## 对照版本与分栏

- B0：设计历史基线 `267b9144c6bf7447c6822aa716f6d5c0eda16990`，新建只读独立 worktree，绝不在主仓测试。
- B1：`5ff5da9`，只在 B0 对某能力真实不支持时补充可运行对照；不得把B1说成升级前，也不得填造B0耗时。B0历史RayData承载会明确标记，不混入新引擎禁止RayData的通过证据。
- B1-Ray：总控于 2026-11-16 明确批准固定 `2fbe83901141e33a1848c8330f696c88cdcc3f63`，仅用于 Ray / two_hosts 格：它已有公开 `workers/native_resources` 同预算入口，尚未包含本轮后续控制传输性能修复。这是能力参考，不是升级前基线，不能据此声称快于旧 Ray Data；旧 Ray Data 的性能对照仍 `not_run`。Local 的 slow_api / uneven_model / cpu_gpu 使用 B1，其余 Local 格保持 B0。机器可读 `baseline_policy` 冻结到完整提交、角色、环境，比较器同时核生产目录干净，不能通过改角色、换 HEAD 或改脏基线假绿。
- C：由总控最终冻结的候选；每次都核实际模块导入路径、HEAD、生产/测试/harness摘要，前后变化则该次证据无效。

## 九类真实负载

| 类型 | 固定业务 | 核心规模/预算 |
|---|---|---|
| 快CPU | 两个map夹一个filter，公开iter_rows | 20000窄行→10000；4worker，256MiB |
| 小数据 | 原31行map/flat_map/filter/pandas batch5/limit3链 | 原31行、3结果、2thread worker、batch_rows7，256MiB |
| 慢API | 原生EmbeddingActor，只有外部HTTP transport为自有fixture | 64行，batch1、并发4、64请求额度，30ms响应延迟 |
| CPU→GPU | CPU map→实际CUDA4096 float32运算→CPU map | 32行、真实一张GPU、5worker、总8GiB/模型worker2GiB |
| 100×100展开 | 100父记录，每父flat_map100子记录，再校验 | 完整10000结果、4worker、256MiB |
| 不均匀模型 | 原生EmbeddingActor固定请求序列 | 64行、并发8；每16条一条250ms，其余10ms，64请求额度 |
| 热点key | 90%同key，其余127key，有限count/sum reducer | 16384×4096B，64MiB；不使用增长无界list状态 |
| 大图 | 共享源分16枝，每枝4个不同map后union | 至少65逻辑节点，64输入→1024结果，4worker/256MiB |
| finite-sealed | 原40000宽行双侧left join，null-left与duplicate-right保留，逐行验证payload后投影 | 80002输入/40002输出，每侧常规载荷8192B；原native256MiB、RSS2GiB、threads2、partitions1和2、timeout120s保持 |

finite性能伴随场景用iter_rows测真实用户首条，同时最终候选原 take_all 压力门保留原节点/阈值，不能把新伴随场景代替原测试。详细字段和所有显式限制见同名 JSON。两版本使用同样的业务函数/输入生成器与同样显式预算，实际内部布局可不同并如实记录。

## 测量协议

每个有效版本/环境/用例先独立warmup一轮，再五轮正式样本，按轮交替B/C次序。warmup不进入p50/p95；五轮的p95用明确固定算法，原样保留每个样本。输入按实际流式内容计算摘要，输出以独立oracle的计数、值/重复/顺序合同核验，不能只看耗时。

总耗时包含声明/准入、worker启动、真实结果消费、原ACK和close；另分启动/首条/业务完成/清理。take_all仅在整个结果返回后才有用户可见结果，不能把内部第一行偷偷当成公开首条；可另记录内部边界但字段分开。吞吐按真实语义输出数除完整耗时。

采样每50ms观察本任务driver进程树RSS和FD，记录可见峰值与采样盲区；每500ms有界查看本任务原pool及本任务目录磁盘，限制进程/文件数和监控本身成本，超出观测预算标不完整而非0。不全盘扫描，不把主机共享内存/其它服务算成本任务，不把sum-RSS误称唯一物理页，不把逻辑pool预约误称实际RSS。

Ray 另在每个真实节点预建一个零 CPU / GPU 的测试监视 actor，沿原 allocation → 原 journal spec/boot → 实际 GCS actor/node/PID → `/proc` birth 绑定，仅采样这些业务进程及其子进程。每个 actor 必须有非零 RSS/FD 样本，结束时同时核原 journal `CONFIRMED`、GCS `DEAD`、相同 PID/birth 退出；监视器也证实物理退出。两机要求业务实际覆盖两个 hostname，不能以两个空监视器冒充跨机执行。driver 与各节点峰值相加只是独立采样峰值之和，Ray 共享服务/object store 未归属部分明确排除。监视器预建在业务计时外，注册 RPC 在计时内且单记耗时。每个 trial 用 job runtime_env 绑定被测树与私有 pool，监视器核 worker `/proc/environ` 中这两个非秘密字段，防止历史/候选串树。

复制只在可观测的原encode/serialization/peer传输边界计调用与实际bytes；不猜第三方Arrow/DataFusion/CUDA内部复制，标not_observable。观测spy只包住真实调用，不替换结果、不创建第二个allocator。辅助诊断若有不同观测开销单独标记，不混成正式吞吐数字。

每类取消另测一次：真实开始工作后调用原取消/关闭入口，测请求到本任务物理退出与预算返还；不与完成吞吐混算。慢源首条另保留before-EOI正例，有限全输入操作按其语义不虚称流式首条。

## 环境矩阵和判定

本轮九类Local完整；Ray与two_hosts先做快CPU、慢API、原wide三类代表性复验。GPU须真实设备证据、two_hosts须不同物理host/node。未跑的其它环境格明确not_run，不能写成9×所有后端已完整。

硬门是值/顺序/重复语义、主动上界和清理、真实原执行入口和ACK、环境/源码绑定。任何对照回退超过20%标红并提交瓶颈profile，不扩大预算、不降低120s、不由本agent豁免；由最终Review决定是否允许特定成本。缺基线/环境/依赖明确not_run，baseline不支持与candidate失败分开。

## 独立接口与接线

`performance(fx, fault=None)` 返回五轮 `measurements`（每轮包含具名用例/版本/环境记录）、实际warmup事实、真实 `child_execute_action_calls` 和详细判定。测量由`run_comparison`统筹，`measure_trial`起隔离子进程，`evaluate_report`根据原始记录裁定。新正式test必须锁语义、预算、重复轮数、暖身分离、比较方向以及不完整数据不能假绿；真正引擎用例验证每条测量路径。现有 `runtime_v3_support._s_performance` 由总控接线，本lane不越写。
