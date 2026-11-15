# 排序末轮与真实消费者的文件窗口

基于 `4c33cb1`，仅改 `kernels/ordering.py::merge_sort_runs` 准入。总控与独立 reviewer 复现的缺陷成立：10 个 file slots、5 个原生 sort run，旧最终 merge 占用 7 slots，产出第一条后立即原 `store.put` 需要 5，实际业务交接失败；测试域把 fan-in 设 2，同预算全部成功。

冻结的修正规则：最终 merge 的读者跨 yield 存活，因此最终轮与中间 spill 都必须预留原 `store.PUT_FILES` 的单次消费者输出窗口，再用余量计算 reader fan-in。保持原签名、原预算、原类型/顺序/provenance，不先 list 完关闭 reader 再写输出。内存 contract store 没有物理文件/租约开销，维持原分支。

固定源码验收：

- `/tmp/demiflow-sort-handoff.xml`：**9 passed / 12.36s**。新增 1/5/11 个真实 run，逐条 yield 后立即写原 store，且立即读回核 tuple、bytes、负零、嵌套空值、record_id/source_position/sequence；每次写时读者仍然存活。另锁 7/9 slots 不足时尚未读输入就拒绝。包括原真实 sort/FD 例和独立 reviewer 两臂脚本原样复跑。
- `/tmp/demiflow-sort-handoff-k.xml`：**22 passed / 2.11s**。原 K 内存 contract/算子兼容。该组的内存 fixture 不算真实磁盘或 Ray 证明。

同名 JSON 绑定基线、精确源码和 XML 摘要。本包不修改 store、shared_access、性能接线、主仓、服务或环境。未重复全矩阵，不将这项正确性修复当作 40k 压力恢复通过的证据。
