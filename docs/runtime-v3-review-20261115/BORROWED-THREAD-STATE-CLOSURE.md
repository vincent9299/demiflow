# 共享线程状态驻留：原慢消费者回归闭环

最终代码候选 `b161161d89afd1c8db11d3783584a32e490c9678`。相对 `59305b7` 唯一生产差异为 `planning/dataflow_place.py::partition_stages`：已由程序封存确认为 `thread_attempt` 的活引用，不再成为另一个独立 resident state owner；保留 kernel profile、输入/输出、函数镜像及 snapshot 原预算。未修改原测试、RunJournal、提交协议、可恢复状态分类或历史错误层级。

原 `test_slow_consumer_bounds_wide_rows_and_inflight_calls` 在 final6 的 110 项中唯一失败，109 项通过，输入字节不变。原因是合法两 worker 已融合成 source＋suffix 后，源另计 16 MiB 独立状态，使 48+192 MiB 超过原 256 MiB 减 32 MiB 控制的 224 MiB。这个源来自已认证当前 attempt 的活 iterator，没有额外独立状态 owner。修复后为 32+192 MiB；同一 32 行宽行、并发3、队列2/1、首消费读取<=8、原256 MiB上限均通过，没有增额。

最终固定字节验收 40 passed / 0 failed，229.63s。包括原慢消费者、新真实 thread/process 双正例、JSON活状态原非final ACK后恢复拒绝且旧commit不变、原 stream_suffix 全组（Lance真实写入及final replay不重建actor、纯函数live原错误、代码漂移、源独立和取消）、function_residency 全组（真实停车、原ACK恢复、显式参数/字段绑定、别名和嵌套状态边界）。它没有把旧矩阵重新标成最终版本复跑。完整命令、解释器、import路径、每nodeid、前后输入摘要及XML在旁边JSON链接的verification中。

最初候选 `98702a5` 曾多改 OperatorKernel 的5行快照逻辑；额外原用例实测证明它改变纯函数 live 恢复错误层级，故全部撤回。最终 operator.py 与59305b7逐字一致。该候选四组跑到119 passed后有4项容量准入失败并停止，不能写成全绿：默认共享容量域累计1026记录，其中1021 RELEASED，旧 allocator 把已释放tombstone计入1024扫描上限，后续拒绝新的worker。原始红证据保留，没有删除任何共享记录。最终40项使用独立 `/tmp/demiflow-regression-borrowed-state-final2-capacity`，该真实长期运行缺陷另交总控修复，专属目录不是对缺陷的掩盖。

平台意图：相同合法 Dataset 不因活共享状态被重复派生计费而拒绝；真实进程独立函数仍保留其状态预算与恢复能力。产品意图：工作台运行沿用同一真实 Dataset/封存计划/提交链。当前修复不声称任意用户Python活对象的进程RSS具有硬上限；有限队列、数据及受管快照预算保持。

主仓未修改或运行测试；七冻结文件与既有基线逐字一致。未push、未合主仓、未安装环境、未停止他人任务。
