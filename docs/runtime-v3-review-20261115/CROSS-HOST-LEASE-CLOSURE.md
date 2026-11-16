# 跨机租约修复与独立恢复验收

修复已集成至 `3f989a0`，原作者固定提交 `dd45d7a`。总控另在 `f9af1eb` 保留等待期间身份变更的原 FENCED 诊断，并将原竞态测试的 inode 篡改按 v1/v2 位置构造；拒绝、时间、FD、原attempt保护断言未削弱。主仓未修改。

## 证据与版本

- 作者固定提交20项通过（含真实双机共享锁读取/互斥/退出后退款、原FD8项），1786输入由总控独立与集成 `3f989a0` 字节比对，0不符；前置同生产GC/accounting46项通过。
- 总控 `3f989a0` 独立跑原 artifact recovery、GC journal contention、history protection：21 passed / 1 failed，64.75秒。唯一失败为测试仍解包旧版双字段 header，尚未进入篡改断言；原进程崩溃/活writer/旧历史保护均通过。
- `f9af1eb` 修正夹具及等价诊断后，原GC等待/旧attempt/有限等待FD/篡改拒绝四项全通过，2.36秒；原红绿原件均保留。后者只改等待身份错误文案与一条测试输入，不冒称22项在此HEAD重复运行。
- 真 Dataset 两机四项由独立 lane 验证，底层租约 probe 不代替完整业务。旧v1跨host仍拒绝，不改写历史或自动升级旧staging物理owner事实。

原始路径：`/tmp/demiflow-cross-lease-final-evidence`、`/tmp/demiflow-lease-independent-3f989a0-evidence`、`/tmp/demiflow-lease-wait-f9af1eb-evidence`。机器记录附独立输入比对及各原验证文件；没有合并重复测试计数。

## 作者原报告（原文）

# 原共享工件锁跨主机身份修复

候选 `dd45d7a5f59b72a625793eba2de1502bef76f116`，隔离分支 `engine/gpt-v3-artifact-recovery-20261115`；主仓、其它执行树、共享环境未改。只有原 object lease、store 初始化、RunJournal 协议存取、GC 协议读取四处生产文件和一份新测试。七个冻结承重件相对 `362d8528` 无差异。

## 实质问题和实现

真实共享文件在两端 inode 一致、f_fsid=3、flock 互通，但本机 st_dev=196、20 主机 st_dev=170。旧 (st_dev,inode) 只能识别本机对象，错误阻断跨机 worker 读取原封存 grant。

新建 store 使用明确 v2 tuple：`[2, filesystem_fsid, root_directory_inode, lock_inode, nonce]`。随机 128-bit nonce 的 32-byte 十六进制内容先写入原锁并 fsync，再发布 header，最后由原 RunJournal 的同一 `runtime_dataflow_gc_protocol` 行新增 nullable `identity_json` 字段封存。旧 device/inode 列保留创建端诊断证据；没有第二个持久权威、allocator、消息格式或 ACK 规则。

每台主机仍用本机 fstat/lstat 比较 dev+inode，并检查普通文件、nlink=1、v2固定32字节、O_NOFOLLOW；实际锁内容必须等于原 nonce。跨机比较原 fsid/root inode/lock inode/nonce。复制目录、替换锁、symlink、错run、header nonce/锁内容篡改、未知版本均拒绝。

保留原 header 文件名，使旧版 reader 在看到 v2 时明确不兼容，不能误走“没有协议的 legacy store”分支。历史 v1 保持空锁、原双整数 header、原三列返回值和严格 host-local dev/inode 语义；不会自动重封。旧数据库尚无新列也仍可只读解析。同机原 v1 读写测试通过；真实跨 host 的 v1 仍拒绝且 header 字节不变。

## 验证

- 固定最终 commit、clean tree：新测试完整12项 + 原 FD8项，20 passed，10.63秒，0 skip。1786份 Python输入摘要、HEAD、dirty状态前后不变，导入来自该隔离树。完整记录 `verification.json`；XML `/tmp/demiflow-cross-lease-final.xml`，日志同名 `.log`。
- 相同生产内容的前置本地46项：新身份反例 + 原 published GC + artifact accounting，46 passed 44.01秒。包括历史/UNKNOWN根、活reader与native线程、真正崩溃窗口、幂等退款等原测试未削弱。XML `/tmp/demiflow-cross-lease-local-r2.xml`。
- 实际两台物理 host：原 ArtifactRef/Bounds 读取并验证原不可变对象；driver EX 真阻挡 remote SH；remote SH 真阻挡 driver GC；remote reader退出后原journal恰好回收一个孤儿并退款一次；旧v1在remote仍明确拒绝。实际host/dev/fsid/inode等见 `two-host-observations.json`。

原FD测试覆盖低预算开读/写/scratch前拒绝、正常与异常归还、多view有效最小预算、真实sort消费者留窗口和join准入。新锁使用原FD，nonce读取用同FD的pread，无额外常驻句柄。

## 边界

本包仅证明原工件锁与原GC协议的跨机闭环；真实Dataset/Ray两主机原四场景由独立residency lane复验，不以本probe冒充。该文件系统已真实证明稳定fsid/inode与跨机flock；其它共享存储若不能提供这些条件仍保守拒绝。旧v1或旧staging/GC物理意图的driver跨主机迁移没有在本包静默升级，不能据此声称所有历史可迁机。

仅在测试新建的 `/tmp/demiflow-cross-lease-*` 与已授权共享 `physical-host-tests/demiflow-shared-lease-*` 范围读写/回收，没有清理既有业务/历史数据，没有安装、复制源码、起停其它服务。
