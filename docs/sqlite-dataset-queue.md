# Dataset 原生任务队列 v2

当前接口与可运行接线统一见 [原始对象获取与原生任务队列](dataset-fetch.md)。
SQLiteChannel、SQLiteQueue、COSQueue 与独立 queue_runner 已退役，不保留第二套
活跃的领取、重试和 ACK 状态模型。

使用 QueueConfig/QueueChannel 声明新库，固定 ProducerSourceSpec；enqueue 逐输入
原生提交，EOS 后 seal；read_queue 只领取未完成任务，经 fetch 和 QueueResultSink
核对对象及结果后原子完成。显式 ack_queue 与自动 sink 互斥。

SQLite v2 只支持本机盘，WAL/FULL；拒绝 symlink 和网络挂载。
多主机控制后端使用 PostgreSQL 的数据库时间、行锁与 owner epoch。
它不启动远程进程，worker 由调用者分别启动；真实多主机共享对象域尚需环境验收。

v1 不原地升级。新建 v2 队列并由固定来源显式生成任务，保留旧封存源码与数据库。
仅对已关闭的一致 v1 SQLite backup，data.read_legacy_queue_records 按文件 SHA256
及有限读取预算查询旧记录，标记 legacy_not_certified，不提供写入、领取或 ACK。
不要复制运行中的 db/WAL 来伪造一致历史，也不要把旧 fingerprint/completion 当 v2 证明。
