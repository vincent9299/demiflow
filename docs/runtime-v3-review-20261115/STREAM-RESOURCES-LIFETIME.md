# 原资源声明与结果独立生命周期

修复两处能力接线：显式 streaming map 的 CPU/GPU/memory/custom resource 声明不再被旧 backend_options 检查整体拒绝，仍由统一 compile_kernel_spec 生成原 profile；尚未映射的 Ray Data compute/worker ranges 精准拒绝，未知字段仍由闭合 parser 拒绝。未新增调度器，未放宽设备能力。

make_action_request 在任何源读取和业务调用之前拒绝已关闭的执行会话。由原 journal/store 发布并持有 owner pin 的 StoredMaterializedHandle 保持独立生命周期，可在原执行 context 退出后读取及继续映射；release 后不可再次读取。没有重新执行上游。

真实 Local 8 passed/53.89s：CPU2 map→join→limit 的 take_all 与 run_stream 均经原 allocator 的 effective_resources 验证；原 journal ACK；缺 GPU 库存时零来源读取/零构造；闭会话与 materialized 生命周期；共享/嵌套图与未知调优预检。Ray Core 私有集群同链 1 passed/30.60s，Ray Data blocker 零调用，finally shutdown。未声称实际 GPU 计算能力已验证。

执行均通过 /tmp/demiflow-regression-runner.py 进程内断言 demiflow 导入本树，PYTHONPATH=$PWD:$PWD/tests，独立 DEMIFLOW_RESOURCE_ROOT；Local 使用 env/bin/python，Ray 使用 env-ray/bin/python。精确 XML 与候选源码摘要见配套 JSON。主仓、冻结件、历史身份未改。
