# 排序使用原 store 的有效活窗口

基于 `40c7c18`。独立 reviewer 实测：高预算 store 256 与同 root 活 11-slot view 共存时，排序按 256-used 估 fan-in，但真正打开读者时原 store 会按活最小 11 拒绝；同预算两路合并对照成功。

本包新增 `RunArtifactStore.available_file_slots(maximum=...)`：在原同一锁内取自身、caller 与全部活窗口最小值，减实际占用。`require_file_slots`、实际 acquire 与该只读查询共用 `_remaining_files_locked`。查询返回快照，不伪造资源 grant；真正 open 仍必须原子准入。排序只读该有效值，再保留原 put 输出窗口，不再自行解释 store 私有 bounds。join 和 shared_access 未修改；无新 allocator、账本或预算扩大。

固定源码 `/tmp/demiflow-sort-shared.xml`：**17 passed / 15.95s**。包括原 8 个 FD 用例、原 5 个末轮交接用例、两个新增共享窗口例及独立 reviewer 两臂脚本原样复跑。

新增正例：256 store + 活 11 view，两常驻 FD，真实 5 runs 逐行 yield 后立即 put，全部类型与 record provenance 保留；caller 更小窗口也被纳入，低 view 关闭后余量恢复。负例：活 10 view 不够两 reader+put，在读输入之前拒绝。

精确源/XML/独立脚本摘要见同名 JSON。只修改 store 的窗口查询/计算、排序消费者及定向测试；不动主仓、七件冻结、服务、环境或性能接线，不重复全矩阵。
