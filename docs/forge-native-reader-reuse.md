# 既有原生数据读取进程复用

这是 WorkspaceServer 原 `workers=1` 的只读读取生命周期；不执行 cell、用户代码、业务 import、管道控制、SQL 或 codec，不保存数据、授权或来源验证结果。每个请求仍重新做原 source/version/Case/资源和当前权限检查。CLI 与控制/FD导出保留单次执行；常驻上下文在 finally 恢复，非白名单先回收闲置 reader，再走原路径，失败不自动重放。

| 策略 | 原普通单次 metadata/sample | 原 analysis read | 新常驻只读白名单 |
| --- | --- | --- | --- |
| 树 RSS 采样 | host-budget存在时4 GiB；否则原未指定树阈值 | 768 MiB | 始终≤768 MiB，空闲仍占原readonly预留 |
| native AS | 初始化virtual baseline+384 MiB，最多4 GiB并尊重继承hard | 同左 | 同一初始化上界，进程内不向上重设 |
| Arrow CPU/I/O | 各2 | 各1 | 各1 |
| Rayon/Tokio等环境 | 原2/BLAS1 | 全部1 | 全部1，仅在child固定 |
| wall deadline | 原操作通常60秒 |20秒 | 每消息≤20秒；原更短值优先 |
| 进程寿命 | 一请求即退出 | 同左 | 最多64请求、累计45 CPU秒、idle60秒 |
| 消息 | 请求1 MiB/响应9 MiB/stderr1 MiB | 同左 | 同左，长度前缀、单请求，无队列 |

新常驻模式明确更严。超RSS/AS/时间/消息或原操作失败时拒绝并回收原child；不静默降级启动另一个读取进程，也不重放在途操作。下一次显式请求才可创建新的 reader。控制与pass_fds排除在常驻白名单之外；不会将某次控制请求的deadline作为reader全生命周期deadline。当前模式为受信开发账号，RSS是采样保护，不称为OS硬配额或恶意后代隔离。

可信库导入保留；request/root/参数、native数据集/游标及Python输出在每请求finally释放，cwd/env/sys.path恢复，不缓存用户可写配置。原source/annotation事务、前后验证和CAS保持。idle维护由原HTTP service_actions调用，不新增轮询线程。只关闭本服务创建、原出生身份匹配的child；未知退出或未证实回收不能当成功释放。

临时目录只属于原reader，TMPDIR与XDG缓存定向该目录；每次采样同时检查≤256条目/≤16 MiB逻辑文件总量，不跟随链接；单文件RLIMIT_FSIZE16 MiB。扫描和RSS均为采样观察，非总磁盘/内存硬配额。非白名单没有写出接口；失败后只在原进程及已观察子进程退出后回收该目录。

Linux的PDEATHSIG绑定创建进程的线程（man7 PR_SET_PDEATHSIG说明）。Web常驻reader只能由原serve_forever线程在service_actions中惰性创建；请求线程仅登记当前唯一生命周期启动票据并有界等待，原reader gate保持占用，无新线程或数据排队。server loop身份固定，退出取消未启动票据；未知启动结果保持所有权，不接管/重试。CLI单次和独立组件调用维持原父保护。
