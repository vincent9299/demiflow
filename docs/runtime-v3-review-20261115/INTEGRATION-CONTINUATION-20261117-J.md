# 2026-11-17：有限计算协调计时、结果编码热路径

当前隔离集成树 `gpt_runtime_v3_review_20261115`，分支 `engine/gpt-runtime-v3-review-20261115`；代码候选 `76378d9b5132ec1b381c3f64762d77b286575765`。主仓 `e40a6dd` 未改，原11项状态/18文件摘要与冻结七件核对保持。

原意图不变：产品要看懂、配置、运行真实 pipeline 并分析/比较/评测数据；引擎要正确、稳定、高性能、通用、架构干净。此轮修复服务运行可靠性、预算/历史保护和公共数据路径；未把功能通过当高性能通过，未把引擎测试当全产品完成。

## 当前结果

- 原 `1997f7a` Local正式批次留下102/117条完整trial；旧进程71977已结束，group未收尾。第103条finite p1 measure4的child原失败存在但缺外层resource/source_after，不混进正式102条。原report未改。
- 原正式已有红：fast_cpu约18.08s vs B0约1.76s；expand约16.72s vs B0约1.71s；small_data约2.61s vs B0约1.15s；hot_key两个measured coordination超时；finite p1 warmup及measure0..3均120s失败。B0恢复/资源保证较弱的事实保留，不能当同保证提速对照或借此取消1.2门。
- 原profile只取得有效driver/business/authority/publication；worker-command空文件、worker-main和worker退出import证据缺失，汇总EOF。真实fast业务成功，instrumented46.26s，但只认部分诊断，不计正式性能或完整profile通过。
- 三项热点修复：普通ASCII字符串免34次重复扫描；同次terminal Record只编码一次给原durable spool和live peer；decode一次raw_workspace扫描供原codec自身和driver collection两道parse前准入。当前真实source_position/schema元数据、下一行与reader/batch共驻全部保持；无payload cache、新allocator、Ray Data或第二账本。
- 有限reduce协调计时修复：只排除所有非idle参与者均提供当前真实sealed不可快照finite-finish证据的业务等待，原普通沉默60s、cut/publication时限和独立总120s保持，不补满剩余额度、不伪造idle。原错误、ACK、unknown/boot/fence/partition/generation保持。

## 固定版本验收（不相加）

| 范围 | 版本 | 原回执与结果 |
|---|---|---|
| 字符串行为/历史字节/资源/恢复 | `d482b42`、`725dc86`，同生产字节 | string-size-functional2：23过/1旧fault参数失败；只透传现行native_queue_state后recovery单项过。纯函数micro不外推整链。 |
| terminal复用+宽provenance负例+旧metadata小预算 | `3a90a2b`、`c8102de` | r2：38过/3旧routing错误ordinal迁移断言需修；r3：route整组28过。原1997相同6红另留基线。 |
| 单次decode测量、collection/batch/历史reader真实预算 | `ded0c20` | `/tmp/demiflow-terminal-codec-reuse-r4-evidence/verification.json`：36过，137.75s。 |
| 根小预算写出、真实原ACK恢复、frame保真 | `53a0986` | `/tmp/demiflow-root-terminal-codec-combined-r1-evidence/verification.json`：7过，49.08s；原40KiB/80KiB不变。 |
| finite等待和原时限、DTO/codec/控制 | `80c16e0` | finite-checkpoint-r2：151过/1缺Ray跳过；来源2991项逐条Git相符。 |
| 真Local/Ray有限reduce与原Ray ACK批量 | `aae8c40` | finite-checkpoint-ray-r1：22过/0跳过；补齐上一组Ray条目。 |
| 原16384×4KiB热点键与8MiB磁盘拒绝 | `aae8c40` | finite-checkpoint-hot-key-r1：2过，83.49s；正例76.62s功能完成，不称正式median。 |
| 全部修复合流：真Local/Ray、终端、非final与final恢复 | `76378d9` | `/tmp/demiflow-root-performance-fixes-combined-r1-evidence/verification.json`：27过，45.64s。 |

## 本候选原工作负载单次预检（仍未通过完整性能门）

`/tmp/demiflow-performance-fixes-76378d9-r1/verification.json`，未改原harness/plan/预算；每项一个trial，无warmup/五对，因此不计算正式改善百分比。

- fast_cpu：passed，15.547s，首结果2.178s，36个原cut。
- expand_100x100：passed，14.794s，首结果3.731s，2个原cut。
- finite_sealed:p1：failed，原总120.607s，首结果98.337s，18059/40002公开行，113个原cut。原失败保留，不能拿部分结果报成功。

## 当前宽行剖析与下一项

原输入/原256 MiB/原120秒的 instrumented 试验已结束：`/tmp/demiflow-i08-wide-profile-76378d9-r1/verification.json`。外层原harness最终终止driver，未得到业务receipt、driver/authority完整profile及完整资源回执；该次业务失败、诊断缺core，不能据此报性能通过。95个可读profile中保留3个business线程；join线程95.728秒wall/95.143秒CPU显示 outer逐escape Python扫描、内层准入扫描和完整canonical校验为主要前置成本；两个输入worker各40,001行却各960,003次pwrite。累计函数时间嵌套，不相加当业务wall。完整原失败和缺失项均保留。

只读复核同时发现拟议单帧sendmsg与已撤回e8d3f33同机制；原同输入/预算反慢7.69%，停止这个候选，未合入。当前改进限定为原JSONL字节的C解转义快路与有界写合并，均在独立叶实施，尚无通过声明。


优先定位宽行首结果之前的计算/验证/搬运，再处理公共逐行传输成本；原字符串/terminal微改有效范围有限，仍不满足高性能意图。当前763原finite:p1剖析已取得上述部分诊断；接下来按热点修复后重跑未剖析的原工作负载，仍用原120秒。同机GPU、Ray正式矩阵尚未本候选重跑；20机入口权限仍阻塞，PostgreSQL仍无测试DSN，完整I08和全产品最终裁决保持未完成。

独立复核临时证据：`/tmp/demiflow-performance-fixes-independent-audit-20261117.json`、`/tmp/demiflow-codec-final-independent-audit-20261117.json`、`/tmp/demiflow-finite-clock-independent-audit-20261117.json`、`/tmp/demiflow-wide-join-readonly-20261117/`。总控新增独立Git复核见 `INTEGRATION-CONTINUATION-20261117-J.json`：当前root专项、原工作负载预检及部分剖析的输入摘要逐条核对，主仓18文件/11项状态和冻结七件均保持。该回执不覆盖未来叶候选。
