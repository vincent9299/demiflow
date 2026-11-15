# 固定候选 ab1b334：主控独立验收

生产与测试候选为 `ab1b33442685d09257b1ebc46e55fb40262317de`，隔离分支 `engine/gpt-runtime-v3-review-20261115`。本报告配套 JSON 保存精确命令、实际 HEAD、XML/证据摘要与源码等同校验。

| 验收 | 结果 | 意图对应 |
| --- | --- | --- |
| 原宽数据 5 项 | 5 passed，275.10 秒，0 skip | 大载荷保真、真实 key-only join、原预算与拒绝语义 |
| 核心恢复 55 项 | 55 passed，416.22 秒，0 skip | Dataset 完整链路、晚到最终 cut、原生效果/ACK、崩溃恢复、历史身份、GC、工作台发布后恢复 |
| 原 stream 17 项 | 17 passed，54.36 秒，0 skip | 新提前关闭接线不破坏原类作用域、partial、启动失败、慢消费者、取消、清理和原生恢复 |

最后一组是按本轮停止/关闭改动影响补验的原测试；`test_stream_map.py` 与 `2177a2e` 逐字相同。没有重写测试去适应错误实现，也没有机械重跑旧 106/564 全集合。三组各绑定 1784 项输入，运行前后 HEAD/输入/工作区均未变化。stream 实际测试 HEAD 为只增加文档的 `54f3808`，生产/测试字节仍与 ab1b334 完全一致。

原 40k×8KiB 输入、256MiB pool、120000ms 计划时限和输出断言不变。两个 40k pytest 用例分别 121.703/123.048 秒，包含准备、检查和清理；不能把 pytest 全用例墙钟与引擎计划时限混为一谈。原压力通过但余量仍小，不据此宣布完整性能验收。

读取/转换的最终 101 项见 [LOCAL-TERMINAL-FINAL-AB1B334.md](LOCAL-TERMINAL-FINAL-AB1B334.md)。与本表有原 collection 测试交叉，不相加制造一个虚高总数。Local/Ray 全公开 API、浏览器与停止/恢复由另外两份同源码报告单独证明。

原 33 失败的逐 nodeid 闭环仍见 [CONTRACT-SUITES-CLOSURE.md](CONTRACT-SUITES-CLOSURE.md)，其 564 项属于当时精确版本；本报告不将旧执行记录改标为当前 HEAD。

本轮没有触碰主仓、共享环境、生产数据或在运服务，没有 push 或合主仓。物理多机/GPU、外部 PostgreSQL、完整产品全域与完整吞吐/峰值矩阵不由本表证明。
