# 合流与新增真实缺口（继续修复中）

原产品 root 仍是统一工作台的看懂、配置、运行和分析、比较、评测；原平台 root 仍是同一 Dataset、正确稳定有界的统一计算、Local/Ray Core 对称和真实业务。当前没有宣布完成，主仓未动。源摘要与回执逐一 Git 核验见同名 JSON。

## 已集成与证据边界

- U3 literal/pure prefix 与私有 router 接线已合。owner 原六 reader/60行/5ms/gap>0.1 三次保持原断言通过；真实 Ray 两路恢复2项回执的2261个源摘要与 c378c5b 全等，未留子进程。
- 同键 join/reduce region 已合：叶输入和原 typed key 复用、中间引用、原逻辑 occurrence/ACK 保持。1f6f0f7 的48 Local＋1实际Ray，四份各2954摘要已核。中间key仍有复制，未声称零拷贝或整链性能通过。**下文状态共驻真缺口使预算结论重新打开，49绿不等于该缺口已修。**
- 原五条缓存恢复/源上下文/Notebook/失败清理用例保留selector，迁到原 RunContext/materialize/ACK；map_cached能力保留。de70b34 的5项完整三阶段与2958摘要已核，root已合。
- 终端批帧候选同负载13.224→14.470秒，未合生产，不用无收益复杂度换交付。

## 两项核心修复仍在进行

1. **行payload与record封装混用上限。** 4KiB内合法typed payload因provenance和JSON转义在终端被误拒；同类限制也存在peer、finite与结果重读。统一分离业务行、document和外层frame的上限，并在原resident/scratch/grant内计同时活副本；旧bytes/media/ACK不变。root新增原完整tuple fold反例，不能以短校验值替代合法宽输出。
2. **reduce状态共驻少计。** 新region只算一份S；原普通reduce也缺显式平台初值/deepcopy/序列化副本核算。真实1MiB任务中376832-byte region grant持有832360-byte三个list容器仍ACK，反例在 `/tmp/demiflow-region-state-audit-20261117/result.json`。正统一修compiler/profile和运行时准入；不能用fallback掩盖普通路径问题，也不提高原256MiB。合流契约增加只读的kernel瞬时驻留观察，让输出编码与尚存reduce状态共用原grant；仅peer ACK可释放的部分允许等待。

## 本轮根节点验收与测试迁移

固定90b0ef7组合：**88 passed、4 failed、0 skipped，522.20秒**，2957输入前后相同、全部与提交一致，子进程清理为空。75项性能harness/比较守卫和13项产品测试通过；不能把这些称作性能矩阵已过。

四红均在同一旧产品测试文件：一处等待已替换的run-bar strong，三处把run旁边的driver-resources目录误数成第二次运行。48e40f9已改为等待真实新run/选择值，并同时核公开历史total和磁盘真实run身份，未删除请求幂等/失败receipt/停止等产品断言。9ff14a8原四条复验全部通过，另配置采纳/并发读图用例通过；同组完整tuple输出16KiB通过、4KiB仍为原封装错误。7项共6绿/1红，2959输入摘要逐一匹配该提交、测试前后不变。回执末端曾见1个新生Python子进程，独立后查该PID已不存在，不能将回执原值改写为零。此前新发现的共享历史证据写入改到tmp_path，性能子进程第4个location-cache域也已加入进程内断言与本机/远端存储比较。

## 尚未完成

上述两项资源修复及合流后受影响API/业务/Local-Ray/工作台；原I08九类、原资源与同文件系统五轮；V035最终完整collection/逐节点状态。历史XML补回1138项观察，不代表最终同源码通过。PG缺真实环境、旧业务Lance源需单独只读边界，不能归为普通私有fixture。

原40 ACK仅为旧screen实测，不是正式plan要求固定40；保持原checkpoint策略并记录实际逐cut。主仓仍e40a6dd，原11项dirty状态/18个文件摘要不变；承重七件不变。未push、未合主仓。

I08执行清单已在 `/tmp/demiflow-i08-execution-preparation-20261117/README.md` 备妥，尚未跑正式压力。原SSH入口本次只读预检公钥认证拒绝，两机复验暂阻；本机Local/Ray/GPU准备可独立继续，不把历史两机通过充作本轮通过。
