# 工件文件句柄：新增租约的原预算接入

基于 GC 候选 `0d72969`，在原隔离分支修改。此前只读真实反例 `/tmp/demiflow-gc-fd-audit.json`：`max_open_files=2` 时，writer lease 常驻 1 FD，scratch 时 3 FD，scratch+reader 时 5 FD，却未拒绝。本包修复该实际缺口，不提高默认值或业务预算。

## 唯一存储内部的准入

`RunArtifactStore` 内维护当前活文件窗口；同一进程、同一规范化物理 root 的 view 共用弱引用状态。状态随最后一个 view/lease 消失，不登记任务、不保存运行或提交事实，不构成新的资源 allocator/数据库。所有窗口在实际 open 前经同一锁核对；活窗口中最小 max_open_files 继续约束临时 readonly view，不能用更大的局部 bounds 绕过已在使用的小预算。

| 窗口 | 同时槽数/释放点 |
| --- | --- |
| writer lease | 常驻 1；store.close 归还。初始化另外留 header/fsync 临时槽，原子转移到常驻槽。 |
| read/inspect/retain | SH lease+data 共 2；read generator 跨 yield 保留，结束/异常/close 归还。 |
| scratch | data+owner+原 journal/目录 fsync 临时峰值共 3；原 scratch 关闭后归还。 |
| put | 提前要求最多 5 额外槽；实际分阶段计 outer SH、owner/metadata、打开的 data FD。data 关闭后，原 settlement 校验再计自己的 readonly 窗口，不把已关闭 data FD 重复扣账。 |
| 原 native journal owner | 独立 SH 1，经原 store 取得，真正原 journal.close 后租约 close 归还；旧 store 关闭后仍保持计数。 |

计数覆盖本存储拥有的文件/租约窗口；原 resource provider 的 socket、进程控制 FD、SQLite 自身连接不因此转移到存储。它不是整个进程的 OS `RLIMIT_NOFILE` 保证，也不把已有外部 reader 的所有句柄约束冒称完成。

## 算子如何使用

排序按当前常驻占用扣除后计算每 reader 的 2 槽。末轮直接合并和需写中间 run 的 fan-in 分开；后者同时预留 put 的 5 槽。至少两路可合并不足时，在新写入/合并前明确拒绝，不通过中途超配制造进展。

关联在消费输入前要求原 scratch 3 槽、流式输入 2 槽和可能的外置 payload 读取 2 槽，另外计入已存在的 writer/其他活句柄。默认预算不变。内存 contract store 没有实际文件，不虚构 lease 开销。

## 实证

固定源码测试期间未修改 Python 或 HEAD；精确文件摘要与 XML 摘要见同名 JSON。

- `/tmp/demiflow-artifact-fd-r3.xml`：**45 passed / 58.81s**。8 个新增实际 FD 测试，原 29 GC、独立 reviewer 的两项代际反例、原 scratch/join 与真实 typed sort。
- `/tmp/demiflow-artifact-fd-leaf.xml`：**101 passed / 18.65s**。原 H/K 接口契约测试，其测试域 peer/Ray/in-memory doubles 明确不算多机/Ray 实机证据。

8 个新反例与正例：低预算 read/put/scratch 在新数据文件打开前拒绝；足额 scratch+live read；store.close 不提前释放 reader；多个 view 与原 native owner 共用预算；业务迭代器失败和打开 scratch 失败归还；坏 hash/生成器正常结束归还；真实五个 run 的多轮 sort 正反例；真实 join 在输入尚未消费时拒绝并在足额预算下同时流读+hydrate；大预算 readonly view 无法绕过活小预算 reader。

开发首轮 36p/2f 是新 Record 夹具错误写死 encoded_bytes=100，已改真实 typed encoding；第二轮 6p/1f 是新测试试图直接抬高已注册 view 的预算，原低限保护正确拒绝，正例改为关闭后建立新 view。未降低原数据、失败、恢复或资源断言。

GC 原提交的历史根、UNKNOWN/pending、quarantine、重复退款保护均保留，独立 reviewer 的两项“旧代不能退新代同 hash 额度”反例在本包重新通过。未触主仓/环境/服务或七件冻结文件；未重复全 Ray/API/browser 验收。
