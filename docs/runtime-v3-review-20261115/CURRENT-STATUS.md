# 当前修复与验收状态（2026-11-17，核心架构/功能定向验收完成）

本页替代初审 `REVIEW.md` 的“当前状态”，不替代原始证据，也不宣称全部意图完成。代码仍只在隔离分支 `engine/gpt-runtime-v3-review-20261115`，工作树 `/yzp/zhaozy/yangzepeng/0905/_demiflow/worktrees/gpt_runtime_v3_review_20261115`。

## 最新集成与验收增量（本节优先于下文旧快照）

- **2026-11-17 最终架构/功能收口（本条优先于以下历史快照）**：测试候选 `9e614af`，最终生产代码 `08335c8`。provider 恢复准入、reducer 值类型、seed shuffle、缓存 actor 与函数状态恢复均已修并组合验收。最终 Local/Ray 的 actor/closure/default/partial 共 8 个真实 ACK 后中断→恢复→物化→后续计算场景通过，原缓存业务 6 项通过；三份函数状态叶证据为 38/27/11 项，保留各自版本，不相加冒充全库。Ray 完整队列正式 selector 通过，5 个业务场景、19 次原生执行、112 个原分配全部清理；产品受管/A-B 2 项、图像 8 项、本机真实四卡 GPU 7 项及原 18 个产品场景保留已核源码证据。原 33 项及 U2/U3 核心 24 项已逐项核实关闭。**性能后延、20 机本轮取消；工作台受管提交仅 Local、3 条基线断言差异、PG/Stage2/离线包/完整 V035 未验和未部署继续明确保留**。所有生产与测试只在隔离树；主仓 `e40a6dd`、11 项状态/18 文件摘要及冻结七件保持。后文“shuffle 在修”“函数缓存待验”等都是旧快照。见 [最终意图对照](ARCHITECTURE-FUNCTION-REVIEW-20261117.md)、[证据账本](FUNCTIONAL-CLOSURE-EVIDENCE-20261117.json) 与 [回归余额](REGRESSION-RECONCILIATION-20261117.md)。

- **2026-11-17 最新取舍与功能复核（优先于下方性能阶段）**：真人要求性能保持现状、后续优化；当前集中架构与功能。统一 Dataset/Local/Ray Core/原提交恢复的主干成立；已修未完成任务跨 provider 恢复准入，工作台 18 个选定产品场景取得完整通过证据。reducer list/scalar 兼容已合入 `8e2da6d`，叶最终 135 通过/2 个基线同形失败并经独立复核；显式 seed shuffle 及原宽行兼容仍在修，之后做最终组合验收。未宣称全部功能完成。工作台受管提交目前仅 Local；20 机已取消本轮验收。详见[架构与功能对齐复核](ARCHITECTURE-FUNCTION-REVIEW-20261117.md)。下方“继续性能优化、性能为当前主要阻塞”等均为历史快照，不是当前执行计划。

- **2026-11-17 当前已验组合 `e74fec8`**：原字节的外层 JSON 原生解转义、有界单行写合并、有限工作取消均已合流。root 同版本 88 个不同测试取得通过证据（首组87过/1个Ray临时路径超长；只缩短私有TMP后原失败selector通过），包括真实Local/Ray有限执行、原ACK恢复和已准入模型响应排空。**原宽行性能仍失败**：80,002输入→40,002预期，首结果80.13秒，原120秒内只返回30,125条；fast_cpu 15.81秒、expand 14.94秒，均为单次预检而非正式五轮成绩。当前核心剩余是宽数据/轻量计算性能及最终版本的API/GPU/产品/历史验收收口，不据此另立整轮架构改造。见[组合验收 K 与剩余取舍](INTEGRATION-CONTINUATION-20261117-K.md)。

- **2026-11-17 真人调整验收范围**：用户明确“那机器关掉了，不验证那个机器了”。20机 `gpu32520` 从本轮验收范围移除，停止认证尝试，不再把该机器复验列为交付阻塞。继续本机 Local、Ray Core、GPU 及工作台完整验收；原始多机能力目标与已有双宿主历史证据保留，不能用本机结果冒充最新版本双机复验。

- **2026-11-17 有限计算等待与结果编码修复已合流至 `76378d9`**：root真实Local/Ray、原非final/final恢复和小预算组合27项通过；原热点键也已在叶版本完成。普通ASCII重复扫描、同次terminal重复编码、driver重复decode测量已消除。**性能仍未通过**：原工作负载单次fast 15.55s、expand 14.79s；宽join原120秒只返回18,059/40,002行。当前部分剖析确认输入校验/扫描和大量小写热点，两个窄候选在隔离叶推进；原输入、预算、时限不变。详见[组合验收 J](INTEGRATION-CONTINUATION-20261117-J.md)和[逐Git独立复核](INTEGRATION-CONTINUATION-20261117-J.json)。以下I08“尚未开始”是旧快照：1997的Local正式批次102/117条完整、未收尾且已有真红；不重写原报告或把部分成功当正式通过。

- **2026-11-17 小预算缺口已闭环，组合到 `6c829ca`**：原 40 KiB 写出恢复、外层 manifest/columns、索引合并中已弹出 children 的真实共驻核算已合；旧 v1、原 R/B/M 与原 ACK 不变。叶 16 项、末次窄修 12 项分别通过；root 合流 9 项及最后 3 项通过，源码/完整阶段/进程退出均核验。接下来固定最终 SHA 执行本机 Local/GPU/Ray 原 I08 五轮计划，两机仍认证阻塞，未宣布性能达标。见[组合验收 I](INTEGRATION-CONTINUATION-20261117-I.md)。

- **2026-11-17 图恢复组合已集成到 `20c4ebd`**：单 worker 分区波次、router 边界、每通道存储上限（含自动 durable）与真实连接重置语义均已合。root 11 项组合通过；受影响原历史 Local 8/Ray 5 已在固定生产的两次测试提交上全部取得完整通过证据，保留前批 900 秒中断/旧观察失败。新小预算候选 9 绿后仍查出 manifest/columns 元数据共驻漏账，尚未合入，正在补修。正式 I08 仍未开始。详见[组合验收 H](INTEGRATION-CONTINUATION-20261117-H.md)；以下“图修复待合/历史组仍运行”均是旧快照。

- **2026-11-17 当前权威组合 `8928e5b`，随后修复集成 `a19c60a`**：Local/Ray各98入口168案例通过，真实Ray全部原Core执行且无RayData；队列完整业务和原恢复、31项写出/资源、工作台原14项、实际H100取消/非final恢复均过。原7图像故障窗口在仅更新测试前置、生产逐字8928的叶版本全部过，严格130请求/130行/两cut保持。控制133项130绿/3红已定位真实RST错误语义，叶修复28项过并合入。总控逐Git摘要审计12回执0不符，不混成新HEAD全门绿。见[组合验收 G](INTEGRATION-CONTINUATION-20261117-G.md)。
- **剩余核心修复正在实施**：原单worker图多partition驻留、图router融合和显式storage池在独立叶已走通三exchange中断恢复及1字节前置拒绝，待独立复审/历史原24项组合；旧观察点迁移保留原ACK/精确前缀/真实进程死亡。原40KiB write_lance新增准入红正在对照定位，不提高预算。I08正式性能及V035最终裁决仍未完成，两机认证/PG环境仍未验。下方“图像/宽join未合”等仅为历史快照。

- **2026-11-17 当前集成 `f48bfa3`**：原生大行发布预算已合；actor/class/partial 方法捕获代码固定及原 Ray 恢复观察点已合；Queue 从原 ACKed cut 恢复 bounded claim refs、原租约/已付费获取/原 final ACK 的完整链路已合。原两条 Queue SIGKILL 恢复通过，真实 thread/process 原生 ACK 丢回复恢复及结果类型改变拒绝通过；另一 agent 独立复审未发现新的 P0/P1 阻塞。总控核验 12 份回执输入摘要，均与各自 Git 字节一致；不是同 HEAD 全量通过。见[继续验收 F](INTEGRATION-CONTINUATION-20261117-F.md)。
- **当前两个在途运行时收口**：图像取消的清理期 RPC 错误回复缺失已确证并修复，候选 37 控制项及原两个 130 行/130 请求/严格两 cut 恢复已过，真实 Ray 收尾后待合；原 64/128MiB 宽 join 与 80KiB sort 写出/恢复候选已过原断言，按物理 phase 预留发布空间及小 writer 开销最后验证后待合。旧候选间歇失败完整保留，未放宽 unknown、预算或测试数据。
- **后续仍必须完成**：两候选合流后的真实 Dataset/Local/Ray/工作台组合、原 U3 核心历史节点、受影响 GPU 与正式 I08。V035 先按核心意图核安全余额，未找到回执不算通过；两机当前授权入口与 PostgreSQL DSN 仍缺，不冒充已验。主仓原 HEAD/11 项 dirty/18 文件摘要及七件冻结保持。以下较早“发布预算未合”“图像必然 unknown”等段落仅是当时快照。

- **2026-11-17 最新合流 `7c034a8`**：大 literal 输入改走原有类型化工件；worker/source/index 共用原 grant；严格 checkpoint 恢复要求绑定原封存 physical policy；actor/callback 只读 run 上下文、节点预算错误传输、有限前沿恢复及 cut/timer 顺序均已合。source 26 项、actor 12 项及历史策略保护 1 项各有固定提交证据；总控独立核验全部输入摘要与对应 Git 字节一致，不能把它们相加当最新 HEAD 全量通过。见[继续修复与验收 E](INTEGRATION-CONTINUATION-20261117-E.md)。
- **当前核心未闭环**：大行模型的原生发布共驻预算候选尚未集成；两条原 64/128MiB 宽 join 已通过输入封存、但实际 planner 仍拒绝，不能算能力恢复；图像部分完成后协作取消仍有未封存响应，恢复被 unknown 保护阻止。三个图像最终写出故障窗口通过，损坏对象测试因同一前置 unknown 尚未走到校验点。旧 finite “唯一失败”表述只属于下方 6d 快照，现已由生产修复与真实验证替代。
- **剩余终验**：最新合流的真实 Dataset/Local/Ray/工作台组合、原 U3 安静窗口、受影响 GPU、正式 I08 和 V035 历史总裁决。两机当前 SSH 授权入口仍待回复、PostgreSQL 无测试 DSN，保持未验。主仓原 HEAD/11 项 dirty/18 个文件摘要及七件冻结均复核未变。

- **2026-11-17 最新固定候选 `6d0530d`**：payload/frame、实际tuple codec空间、reduce初值/新旧状态/序列化共驻及历史grant、工作台关闭等待已合。原完整4KiB/16KiB tuple反例、19项join-chain和原40KiB驻留均通过。Local/Ray各168 API场景、覆盖98入口；真实三模型×三恢复窗口、queue完整业务/unknown、工作台14项组合通过。根节点独立核验17份回执的2964输入摘要全部等于同一Git提交，119项中118通过/1失败，详见[最新组合证据与剩余修复](INTEGRATION-CONTINUATION-20261117-D.md)。
- **当前正在修的唯一该组合失败**：默认预算使join/terminal合法分phase，旧测试只看join.finish_cursor；同时实测terminal非adopt有限输入不能中途checkpoint。新隔离叶树已有原100×10中途ACK/恢复1000正例，但仍在收口reader保留空间、park协作、旧frontier连续恢复，并补真实Ray/GC验证。未把测试观察点修改当生产修复，也未宣称叶树最终全绿。主root仍待合此修复。
- **整体仍未完成**：上述finite修复的定向终验、原U3安静窗口、受影响GPU、正式I08五轮矩阵及V035历史总裁决。两机旧SSH入口当前认证拒绝，等待已提出的信息问题回复，不猜凭证；PostgreSQL仍无测试DSN。下方较早“frame/reducer正在修”等均是历史快照，不覆盖本节。

- **2026-11-17 当前继续修复点（优先于以下快照）**：U3候选、同键region与原五条缓存/异步/清理测试迁移已合。独立核owner49项＋真实U3 Ray2项的源码与提交全等；但发现合法payload被record封装误拒，以及reduce初值/新旧状态/编码副本共驻少计，正在统一修复。根节点90b0ef7的4条旧产品locator/目录计数红项已在9ff14a8原selector全部复验通过；另配置并发读图通过。该组6绿/1红，红项为保留原数据的4KiB完整tuple封装反例，仍待核心修复。原I08与最终组合/V035尚未完成，见[本轮事实与缺口](INTEGRATION-CONTINUATION-20261117-C.md)。

- **2026-11-17 当前权威增量**：固定 `8be9e45`，原 GC 与真实 Ray 终端/类型/有限 join 恢复 7 项全部通过；前一固定 `4904f10` 44 项通过、1 项 GC oracle 失败，该 oracle 已修并由本轮原 selector 关闭。原宽行 p2 保持 40k×8KiB/256MiB/120秒，实测 97.4076 秒。已合 source 行数 cut、实际共驻 quantum、确定末行 EOF、观测包装器关闭及有限 join 投影；见 [本轮实现与独立证据](INTEGRATION-CLOSURE-20261117-B.md)。
- **当前剩余优先看这里**：U3 慢分支候选、19 条 join-chain 原生复用、终端元数据优化候选、正式 I08 性能矩阵、最终受影响 API/业务/产品组合与 V035 裁决。以下“U3 六条都未验”“宽行未合”等是旧快照；不把各版本通过数相加，也不据一次宽行通过宣布高性能达标。旧 1.872 秒为 B0 warmup 且恢复保证不同，不再称同保证正式中位数。

- **2026-11-17 最新（优先于下方旧数字）**：最新验收代码 `5e3543c`，recovery_rows、明确回调超时、行/批控制失败、最终普通函数观测及小worker共置已合。根节点组合41通过、union/peer16通过、真实Ray6通过；原74条待合流历史项有68条取得本轮根节点通过证据，仍跨固定版本，不能当全仓全绿。详见 [实际闭环与剩余](INTEGRATION-PROGRESS-20261117.md) 和 [固定版本证据](INTEGRATION-PROGRESS-20261117.json)。
- **仍未完成**：内部U3图6条、19条join-chain优化断言、正式I08性能矩阵和最终整体裁决。两个微优化候选未证明整链收益，未合生产；原宽行两例已在owner最新固定版本通过但时间余量不足，后续优化仍在独立验证。产品全root不能由引擎计数替代，主仓和冻结件保持。

- **2026-11-16 深夜增量（优先于以下旧快照）**：KV 原生提交/恢复/回调、SQLite锁后实际页预算、Lance部分列更新及源激活前CAS、纯算子检查点工件合并已合入。`01413fc`组合36项；`ae41997`原生写入/守卫74项及真实Ray新旧image恢复2项通过。原待跑runtime276条完成（一条过时工厂恢复预期经修正复验）；Forge子测试回执歧义已修守卫并保留旧不完整证据。详见 [本轮收口与历史补验](NATIVE-CLOSURE-AND-REGRESSION-20261116.md)。
- **当前未完成**：Arrow批次直通、公开registry旧checkpoint入口迁移、剩余历史补验/失败修复、最终组合及正式I08性能门。pack同FS五对本场景约8.16%改善，前轮约4.86%，不代表整体性能达标；下方1,735未跑及KV/Lance待修为旧快照，按最新逐批记录更新。

- **2026-11-16 23:15 增量（优先于以下历史快照）**：CSV 原生选项/类型/回调 code-pin 已合入；源回调恢复原 run 的只读身份；直接 `take_all(limit)` 的真实超读、格式参数延迟拒绝、checkpoint 检查器错误分类及 Python 行列顺序退步已修。根节点 `df6125f` 66项、`7b0e72a` Local 33项/真实Ray 6项、`760d85c` 列序3项通过；各版本和原红回执分别保留。[本轮复核与证据](LATE-INTEGRATION-20261116.md)。
- **产品/API 最新证据**：`f0ff14c` Local 98 API/168 case 全过；真实浏览器在指定 Chromium 后通过。Ray 全矩阵三项失败确认为环境缺依赖，同生产源码在私有环境定向复验通过；没有把它称作重新全量跑绿。原40KiB驻留和flush共驻预算均已修并验收，以下“正在修复”属于旧阶段。
- **当前真正未完成项**：SQLiteKV 原生提交恢复/通用 Datasink 回调、Lance partial merge 与宽列复用正在收口；Arrow纯copy仍有逐行Python回退；同cut状态打包性能候选待验收，正式I08矩阵未通过。安全历史批次仍有1,735项未跑。整体验收未完成，不以列举卡数或混合版本通过并集宣称100%。

- **2026-11-16 21:45 合流增量（优先于以下历史快照）**：图完整 target/路由、原三 worker 驻留/恢复、线程工厂的独立状态与外部共享身份均已合入；总控 `a19cc42` Local 19项及真实Ray 9项全过，`6e1aab8` 原 L016 28项全部真实Ray通过。原80项stream_map/22项sync_map在owner完整工厂修复后通过，总控另补审并修正 `__new__` 构造绑定。来源与回执见[合流复核](GRAPH-FACTORY-INTEGRATION-20261116.md)。
- **当前未完成项**：全库8,000余nodeid收集已完成但不计通过，2,529项残余安全批次在跑；原40KiB驻留能力与旧terminal tail读取窗口正在修复；最终同候选全API/工作台及正式性能矩阵尚未完成。终端编码微优化因实际共驻超预算撤回，同文件系统约0.20秒差异也不足以宣称加速；没有合入该微优化。

- **2026-11-16晚间增量**：L016原28个nodeid已迁到真实Ray入口/原sealed分配/原资源表/真实actor退出与恢复；仍在完整复验，不能将迁移完成当28项通过。迁移首先发现一个真实生命周期缺陷：scope-close抛错时原业务错误保留，但Ray worker仍存活。`8fdb865`通过同一ResourceManager的原retirement/apply/reconcile兜底，原关闭错误仍可见，只在原观察确认退出时释放本地句柄。固定原生Ray三项负例3 passed/12.51秒、0子进程，见[本轮关闭修复](RAY-SCOPE-CLOSE-20261116.md)。
- **全回归执行已展开**：owner固定`c22cc5a`已实际执行961节点，951 passed/9 failed/1 skipped，原计划剩2083节点及补充42文件未执行；这些不是最新HEAD全仓通过。发现包括旧选项拒绝期望、旧图AST/永久skip、API无序搜索断言、提前导入后失效的chain观察器，以及线程worker工厂恢复误归attempt-only。根节点已接入完整来源摘要/三阶段执行守卫并修chain观察器；工厂恢复候选待原共享/生命周期组合验收。
- **性能比较方法勘误**：启动优化、小帧发送与终端wire首轮对照曾使用`/tmp`本地xfs基线、`/yzp`共享盘候选，启动/导入/代码校验计入耗时。原测量值及正确性证据保持，但不能归因为代码的7.92%提升、1.11%或5.95%退步；同文件系统对照正在补做，完整I08门仍未通过。终端重复编码候选尚未集成；图恢复的过量停驻/切换修复也在验收。

- **最新同版本兼容接线**：`f38aced` 上 Local 101 passed/173.44秒、真实 Ray 13 passed/16.31秒，零跳过。旧链的错误返回、预算丢失和未关闭 runtime 已修；旧 native/stream 的预算/schema、Ray 原 grant 上下文、双载体读取、零行 take 与真实 buffer 恢复已接回唯一 v3/原 ACK 链。见 [集成复核](COMPAT-INTEGRATION-CLOSURE-20261116.md)。
- **历史回归不以换测试闭环**：原90失败中83项有对应证据，图入口原7项仍在修。曾因迁移断言不等价而重开的低层空聚合返回，已恢复原nodeid并在根节点 `930fdee` 6项真实用例全部通过。此项取代下文38/52及82待复验的旧阶段数字；原33失败已关闭与这批90是不同集合。
- **Local/Ray聚合对称性补修**：独立检查发现Ray旧入口也误用业务名称作program ID；真实红基线两项拒绝合法聚合，`c2686e4`修复后在同一 `fffef12` Local 3项、真实Ray 2项全过，保留空值、自定义名称和原单action/ACK。
- **当前核心剩余**：显式图真实各分支/路由/小预算与恢复、V035真实执行全量分类、最终同版本API/产品组合及高性能。普通CPU原20k→10k目前约14–15秒，历史约1.872秒，性能尚不达标。禁止把新worker预算、跳过或collection当成通过。
- **真实Ray模型恢复与启动优化**：owner固定源码的三模型×三中断窗口已实际通过，未知发送不自动重发，原请求额度不增；四组源码证据均经根节点逐字核验。普通worker无关Lance初始化已移除并集成，原交替筛选数值受文件系统混杂影响，7.92%不能作为代码收益证明，完整性能门仍未过。

- **Ray V017 集成复验已通过**：固定 `c0b6871`，原正式路由恢复 1 passed/173.018 秒、0 skipped；包含空闲代次与恢复阶段时钟修复，2194 份输入与提交逐字相符，原 FD 容许范围保持，本任务子进程全部退出。不是两机路由故障矩阵；[根节点回执](ROUTE-RECOVERY-INTEGRATED-VERIFICATION.json)。
- **GC 候选实测保留、网络实验撤回**：原 20k→10k/4 worker 上限/256MiB/120秒，GC 同基线只差一生产文件，2次热身＋4次交替测量，中位 16.011→15.183 秒（约5.17%改善），结果、40 ACK、资源释放保持；这只是候选筛选，不是完整五轮性能通过。发送聚合实验反慢7.69%，已由owner撤回，未合入。详见 [性能候选裁决](PERFORMANCE-CANDIDATE-SCREEN-20261116.md)。
- **历史回归逐项迁移**：已合入13个历史测试文件的迁移，原业务值/批界/限额/零额度负例保持；取代的是已退休的旧执行路径与“新能力必须拒绝”断言。旧90个失败中38个在各自绑定版本关闭，52个继续处理，未把混合失败报告或collection计为通过。精确原nodeid与版本见 [历史复核](HISTORICAL-MIGRATION-PRIMARY-VERIFICATION.json)。

- **V012 小预算公平运行已在集成版 Local/Ray 全场景通过**：Ray `f67daa9` 正式门 1 passed/139.29 秒，Local `c835ada` 的组合 122 passed/272.02 秒；原 64×4KiB 双分支→128 行、热点键聚合和磁盘不足负例不缩样、不增额。恢复阶段时钟与带代次的空闲通知修复已集成，原分配全部确认释放。下文“Ray V012 仍红”是保留的旧快照，已由此项取代。
- **原生路由恢复与元数据只读**：`c835ada` 的 122 项同时覆盖正式 Local V017、封存/code-pin/环境投影负例，以及 `schema(False)`/`columns(False)` 零业务执行；Ray V017 在 owner `17a93bc` 已通过，集成空闲通知修复后的根节点 Ray 复验继续进行。证据见 [集成验收](INTEGRATED-FAIRNESS-AND-RECOVERY-CLOSURE.md)。
- **当前剩余核心项**：旧原生入口执行预算与 schema 约束、剩余历史兼容失败、最终同版本产品/API 组合和同预算性能对照。GC 小载体预检仅功能通过，尚未测量整链性能收益；不据 V012 通过宣称 I08 性能完成。

- **两台真实宿主正式 V026 已通过**：固定 `f5669fb`，1 passed / 154.083 秒，0 skipped；内部原四个真实业务场景及新增等长不同字节拒绝均通过。恢复保留同一计划与原 ACK，两个节点实际业务执行、代码缓存与读域证明均取证，双方本任务进程全部退出。1866 项输入与提交逐字相符，见 [两机正式验收](TWO-HOSTS-FORMAL-CLOSURE.md)。仅证明 actor 丢失，未宣称整机掉电或网络分区。
- **剩余实施重点**：Ray 小预算共享分支公平运行仍为真实失败；V017 首次封存/路由恢复新候选正在正式 Local/Ray 组合验收；全量历史清单及未覆盖文件正在补验，已发现历史运行集合变更与只读 worker 资源保护需要修复。原 33 个失败关闭，不等于之后完整回归无新失败。性能仍未满足原同预算比较门槛。

- **最终资源测试已集成**：V023 两后端各12个内存场景、27个原allocation均CONFIRMED；等宽cast的两份34,400字节各自可容纳、同时68,800字节超过65,536预算，在分配前被拒且目标Arrow分配峰0；正例仍真实提交。根节点独立核1052份源码与188份原证据文件全部相符，见 [资源复核](RESOURCE-GATES-PRIMARY-VERIFICATION.json)。Ray V012 红单独保留，未冒充全资源门绿。
- **纯链集成复验**：根节点 `3ac20f4` 原 stream-suffix＋新融合＋停驻组合37 passed/58.87秒；1866项输入逐一与固定提交比对相同，见 [组合回执](FUSION-ROOT-COMBINED-VERIFICATION.json)。两宿主正式V026后续结果以上条为准。

- **真实 actor 丢失组合复验**：`9272433` 的正式 V027＋V033＋证据守卫 23 passed，4 个重复组件节点未选（正式门已执行其全部场景）。根节点发现并修正旧故障夹具 `close` 会先取消 HTTP 的竞态，未修改生产实现。实际 SIGKILL、未知模型不重发/不退款、原 ACK 恢复及原工件安全均通过；2184 输入与固定提交相符。见 [根节点复验](ACTOR-OWNER-LOSS-ROOT-VERIFICATION.json)。
- **纯同步线性管道优化**：已集成 `3f560d8`（owner `7159715`）。只在原 worker 上限会迫使合并时，将无状态、纯同步、单输入线性链置于一个原 worker；内存求和、单宿主准入、异步/定时/设备/显式隔离保护保持。owner 62 项功能验收通过，原 20k→10k/256MiB/120 秒单次为 Local 17.496 秒、真实 Ray 25.582 秒；完整五轮性能仍未达标，不称高性能完成。源码与 harness 摘要见 [独立复核](PURE-FUSION-PRIMARY-VERIFICATION.json)。
- **公平调度尚有 Ray 真红**：后文 128 行/35 分配正例仅属 Local。最终 Ray 在只余 1 CPU、原 64×4KiB 双分支/16MiB/120 秒下仍超时（48 worker、terminal 0/128）；正修启动空档消耗 checkpoint 间隔造成的反复退租，不增预算或时限。V023 内存峰值 12 场景 Local/Ray 已由 owner 完整通过，最终测试/证据等待集成。

- **资源、原模型完整文件及真实驻留 GPU**：原 Embedding/inference/SQLite 三个完整文件在 `0712606` 共100 passed；Arrow/terminal/工件/Prompt服务组合在 `ba4fd42` 18 passed。V011现为真实 CPU→CUDA Linear→IO：Local `2f5fa42`、Ray `abeaa7b` 均64行同一模型、不同CPU/GPU/IO进程；慢IO时仅2行GPU/3行CPU准备，原credit与ACK/释放均实测。停驻修复 `3ce2e06` 已合入，原1worker/16MiB/120s公平案例128行通过、35原分配全释放。证据及仍未关闭项见 [RESOURCE-AND-GPU-INTEGRATION-20261116.md](RESOURCE-AND-GPU-INTEGRATION-20261116.md)。下文“原模型完整文件待跑”已由本项取代，完整最终矩阵仍未宣称通过。
- **最后私有模型准入与真实Ray组合**：`492c2b8` 让新Prompt row scope/Embedding batch＋part必须重新准入，原HTTP/cache键不变，同一RPC仍幂等；总控核owner9组源码与40附件零不符。固定`91c8658`真实Ray Core9 passed/0 skipped/121.65秒，含模型缓存、结果重读、回执后强杀、driver restart/FENCED。原模型全文件最终组合尚待补跑，不拼接不同版本冒称全门绿。
- **新压力红保留**：仅余1CPU、64×4KiB共享双分支在120秒内未完成，记录79个ACK/80次worker启动；不是无消费者死锁，仍是不可接受的调度性能问题。没有缩样或扩时间，性能lane正在定位。Arrow与终端索引元数据必须一同做分配前准入，修复在途。

- **Prompt＋真实托管服务完整链路**：`4cf3555` 同版本35 passed/0 skipped，包含原 HTTP owner、私有缓存、原物化结果重读、取消/退出、原生声明守卫与历史身份。真实 H100 服务 PID/NVML/原 device grant、原2 GiB内存预约、CUDA计算结果及最终释放均有证据。未另建服务调度/提交账本；服务完整环境只保留在执行程序，公开事实存摘要。见 [模型与服务总控复验](MODEL-SERVICE-CLOSURE.md) 和 [机器回执](MODEL-SERVICE-PRIMARY-VERIFICATION.json)。Local 数值GPU服务不代表 Ray/任意DeploymentService/路由服务的全量对称。
- **输入、观测与退出增量**：旧 inference＋模型真实观测39 passed；cleanup OSError/原状态机18 passed；大图片 literal 有界身份＋原block21 passed/1 Ray skipped。默认身份及元数据1 MiB限制不改。显式私有 Prompt 普通当前attempt物化已集成；跨库失败不授予恢复。私有缓存 requeue 后新调用计费的最后同类修复仍在 owner 验证。
- **及时交接与内存核心项**：两处 worker 丢唤醒已集成；原控制格式不变，无收益的wire格式实验未合入。实际Arrow反例确认worker/终端/写出存在分配后才检查的路径，正在前移原准入；性能及公平/峰值正式组合仍未通过，不能宣称全部完成。

- **服务退出与原资源释放**：已集成 guardian `c487cde` 与原 RunResourceState 重试 `90f24a4`。返回 UNCONFIRMED 后保留全额占用，只将已完成 I/O 的再次检查间隔降至不超过100ms；未返回 I/O 仍保留原30秒 crash lease。原状态机、真实原pool、Prompt真实池及worker强杀共20个不同case通过（19+后续2，有1个重叠）；强杀测试同时锁定“不早退”和最终原allocation CONFIRMED/pool RELEASED。最初detached红为真实Broken pipe不在夹具故障词表，修订后两种强杀均过，原红保留。见 [资源重试](CLEANUP-RETRY-CLOSURE.md)。guardian cleanup自身错误的持锁强化与本地GPU服务准入仍在执行。
- **旧模型组合**：固定 `56fb2fc` 的完整 Embedding + inference 两文件84 passed/2 failed，Embedding文件全通过。两红之一是Prompt指标缺失（独立候选专项已绿，待最终固定交付），另一个旧父进程观察器已改为实际thread worker及原RemotePromptJournal端口，保留真实池/关闭/零历史审计断言，集成验收通过。显式Prompt缓存与混合两模型磁盘总预算仍在收口，不宣称两文件已同HEAD全绿。

当前生产已集成真实 peer 批读、Ray mailbox、持久展开窗口、受管对象 Embedding、工作台真实 GPU 和原 producer unit 恢复；Ray 异步后缀现与 Local 使用同一资源规则。各项保留实际验收版本。这不是“全部完成”的版本：旧私有模型缓存兼容、性能和部分正式验收门仍在收口，未推送或合入主仓。

- **模型兼容集成**：原生路由 Prompt 已进入同一图、原 journal、run/node/route 原子额度与原 ACK 恢复；owner Local18项、真实Ray1项通过。显式 Embedding 私有缓存保持原 delivery-only，owner48项通过；总控独立核全部源摘要及附件。总控 `d85dca1` 的原 adaptive/有限准备/HTTP预取重叠与路由组合17项通过。原先遗漏的七个 Embedding 失败在旧267b914全部通过，已按真实行为逐项补修，托管服务与合作停止仍在收口。见 [模型兼容与边界](MODEL-COMPATIBILITY-CLOSURE.md)。
- **工作台 GPU 与对象链组合**：owner `fae6a05` 的正式 V030 七个真实产品用例和 74 项合同/历史负例全部通过，已集成为 `fdea56c`；总控核两组各 2131 项源码摘要及 8 份证据附件，零不符。总控再于集成版本 `301b426` 跑 GPU/静态 tensor 负例/受管 image-document Embedding，55 passed / 299.81 秒，零跳过。资源声明、CPU0→CUDA1→CPU0、原 ACK/Lance/历史桥、取消释放与对象篡改零 HTTP 同版本通过。见 [NATIVE-GPU-CLOSURE.md](NATIVE-GPU-CLOSURE.md) 及 [总控复核](PRODUCT-GPU-RAY-ASYNC-PRIMARY-VERIFICATION.json)。只声明已验的封闭 Torch 子集与 Local/process 受管路径。
- **真实队列业务与 Ray 差异**：原 producer unit 跨物理 cut 的恢复修复已集成 `a189328`，Local 五组业务含 19 次原执行、真实模型与队列/ACK/恢复已过。Ray 的 5 逻辑阶段/4 worker 误拒已修为原源与后缀两 owner，CPU/内存仍逐阶段求和；真实 Ray/旧默认合并 20 项过，编译与资源负例另 7 项过。4 CPU 仍在真实反例中明确拒绝。I09 fixture 按真实 6 CPU/4 worker 完整五组业务门通过，19 次原执行、112 次原分配全部 terminal/CONFIRMED；性能基准不增额。总控逐一核 1040 个源码摘要、8 个证据文件以及 112 条原 SQLite 分配的清理、birth/placement，零不符。见 [完整 Ray 业务](QUEUE-BUSINESS-RAY-CLOSURE-20261116.md)、[独立复核](QUEUE-BUSINESS-RAY-PRIMARY-VERIFICATION.json) 与 [RAY-ASYNC-SUFFIX-CLOSURE.md](RAY-ASYNC-SUFFIX-CLOSURE.md)。

- **最新真实 Ray 组合**：固定 `d06e993`，7 passed / 85.01 秒，0 skipped。512 条终端 ACK 的原 RPC 批量、提前 close/最终结果重读/边界、receipt 后 ACK 前真实 SIGKILL、driver 重启与旧 worker FENCED 均通过；1821 项输入前后不变，私有集群退出。原证据 `/tmp/demiflow-ray-combined-peer-r1-evidence/verification.json`。此项关闭下文 mailbox/peer 的待真实 Ray 验收状态，不替代之后新代码的全部矩阵。
- **持久展开窗口**：`763b973` 将已停车的持久交接容量与尚未运行的消费者内存队列分开。原 credit bytes/总 pending/磁盘/ACK 权威不增额；两源各展开两行→队列在 1 worker 下的正常和原 ACK 恢复及原持久断点 7 项通过。原超过全局窗口的 65 路扩增仍被拒绝，12 项 peer/终端预算组合通过。
- **产品与历史正式门**：已集成 [NATIVE-PRODUCT-GATES.md](NATIVE-PRODUCT-GATES.md)。历史原身份/载体门和真实 Notebook/SQL/Case/开发会话产品门分别在其固定版本通过；总控逐组核2126项源码摘要与Git提交全等。工作台停止/恢复及 GPU 完整门已按上条通过，仍不把不同提交的各组结果拼成同 HEAD 全门绿。
- **受管图片/文档 Embedding**：`94dac3e` 真正 prepare/start→对象校验→模型 HTTP→原 ACK/固定 Lance/执行计划桥 4 项通过，含封存后改对象失败且零 HTTP；原绑定、路径和旧默认身份 13 项通过，见 [MANAGED-EMBEDDING-OBJECTS.md](MANAGED-EMBEDDING-OBJECTS.md)。关闭下文“尚未开放”的旧状态，不能代替产品全域完成。
- **性能最新事实**：原 40k×8KiB 每侧宽行 iter_rows 在同一 120 秒预算下完整通过 113.74 秒；窄链26.98秒，仍为旧1.872秒的14.4倍。均单次诊断，完整五轮矩阵未完成。后续严格内置标量 thaw 126项过，但微基准改善不冒称整链同比，见 [FAST-CPU-REMAINING-COST.md](FAST-CPU-REMAINING-COST.md)。

- **Dataset → 原提交 → 恢复**：函数状态修复、borrowed thread 状态预算修复均已集成。原慢消费者不改预算通过；40 项状态/原回归通过，见 [BORROWED-THREAD-STATE-CLOSURE.md](BORROWED-THREAD-STATE-CLOSURE.md)。新增纯辅助函数的不可变默认参数不再把队列生产误标为 attempt-only；可变默认值/闭包仍拒绝假恢复。
- **真实 Ray**：总控在 `fdffa2e` 跑真实 1/4 CPU 函数状态与原 ACK 恢复，以及 Ray 公开 workers/native_resources，**13 passed / 0 skipped / 209.17秒**。此前 `9d3481f` 的98 API/168例矩阵与原GPU绑定合组6 passed；最后无RayData单项因夹具没有集群而红，同HEAD独立私有集群补跑1 passed，红回执保留。
- **多机/真实GPU**：两宿主六GPU normal/failure/cancel 3 passed，远端GPU actor丢失后原ACK恢复1 passed。总控独立核三次1789输入摘要及880归档+17附件，0不符。恢复臂的96次CUDA运算在故障前已完成，恢复的是余下31条已封存缓冲，不能称“故障后又算了31次CUDA”；不是整机掉电/网络分区。见 [PHYSICAL-HOSTS-GPU-CLOSURE.md](PHYSICAL-HOSTS-GPU-CLOSURE.md)。
- **有限存储与启动载体**：有限完整输入按原磁盘预算分配，传输credit不再错误地成为全输入容量上限；64MiB倾斜宽行正确得到128组，8MiB反例由原磁盘权威拒绝。启动assignment/recovery使用原store的有界不可变引用，保留64KiB启动上限、旧inline只读及null/并存/篡改拒绝；原GC仍为唯一引用追踪者。见 [SOURCE-FANOUT-LAUNCH-CLOSURE.md](SOURCE-FANOUT-LAUNCH-CLOSURE.md)，owner50真验含Ray原ACK恢复。
- **大图性能**：原67逻辑节点、64源行、1024输出、4 process worker、256MiB与120秒预算，在 `b4263c7` 完整通过，13.292秒；旧实现同输入12.83秒。纯同步CPU后缀可在原总内存和单宿主grant内合并，源/GPU/异步边界保留。该单次诊断不等于正式五轮性能通过。
- **轻量CPU仍慢**：仅减少重复codec及每cut后代pin写入后，20k→10k实测32.662秒；原实现1.872秒。保留全前缀SHA验证，pin由2514降到50。没有通过减少检查、调大预算或超时获得“达标”。正在实现并测有界控制消息批量；双宿主同负载原120秒仍红，实际actor/原journal/PID退出都已取证。
- **容量池历史积累**：只在原池归档已RELEASED记录，保留原字节、重复release与ID不可重用，bounded scan不足时不准入；50项私有池测试通过。新布局不可与旧flat-only客户端混用，未部署共享池。测试误触共享池及精确恢复单独披露于下方及 [CAPACITY-HISTORY-CLOSURE-20261116.md](CAPACITY-HISTORY-CLOSURE-20261116.md)。
- **本轮新组合失败闭环**：初次35节点为26 passed/7 failed/2 PG skipped；真实producer默认参数误拒已修，结果读取夹具改为观察原read_admitted路径，quota仍锁定原PromptBudgetExceededError及一次计费，进程测试runner加main guard。固定 `fd594c5` 12 passed/2 PG skipped，覆盖7个原失败与5个可变helper反例。原断言的数据/额度/ACK语义保留，未把PG跳过算通过。


- **原提交与物理驻留正式门已接线**：multiworker barrier、actor lifecycle、driver restart 在 owner `a19e78a` Local 4项、真实Ray 3项通过。总控独立逐组核2123条输入摘要及原附件，0不符；见 [NATIVE-RESIDENCY-PRIMARY-VERIFICATION.json](NATIVE-RESIDENCY-PRIMARY-VERIFICATION.json)。完整target/broadcast公开Dataset lowering仍未证明。
- **批量控制通信**：原帧不变，只批已就绪消息，最多64条/1MiB；正常、原真实Ray终端、Local提前关闭/读失败和6个SIGKILL窗口，owner固定 `d8d241e` 27项通过。另512条原ACK只用28次成功Ray RPC。总控独立核两组各1806输入、XML及附件全等，见 [CONTROL-BATCHING.md](CONTROL-BATCHING.md) 和 [独立复核](CONTROL-BATCHING-PRIMARY-VERIFICATION.json)。双宿主20k→10k依然超原120秒，不能称性能闭环。
- **元数据与Ray调度候选**：`1e9fb87` 在原结构遍历先证明编码上界后使用C JSON编码，原字节/身份不变，超保守界继续原增量编码；原历史/身份等115项通过。`6e1020c` 每worker仅一个有界在途Ray mailbox读取，保持原deadline/unknown/fence，86项控制单元通过，尚待合并数据批读后真实Ray复验。两项均不是正式五轮性能证据。
- **完整业务/产品补验暴露真缺口**：search→enqueue跨物理阶段cut会提前结束native producer unit，正在修；工作台静态检查仍拒backend_options，固定thread且环境不传部署GPU配置，不能拿引擎CUDA成功冒充工作台GPU跑通。Notebook真实Jupyter→原SQL→DataFusion→Lance已正向通过；完整产品门仍待收口。

最新原始总控回执：`/tmp/demiflow-ray-launch-fdffa2-evidence/verification.json`、`/tmp/demiflow-native-closure-fd594-evidence/verification.json`、`/tmp/demiflow-largegraph-b426/large_graph/trial.json`。完整同HEAD终验待批量传输候选合并后进行。

## 两条原始意图与目前判断

**产品 root**：统一、真正可用的工作台，让用户看懂、配置、运行 pipeline，分析、比较、评测数据。Demo、开发对话、Notebook Python/SQL、Case/媒体、历史保护仍属于产品，不能被引擎测试数代替。

**平台 root**：正确、稳定、高性能、通用、架构干净的统一数据计算平台，完整支撑实际业务。Local/Ray Core 是承载方式，同一 Dataset、真实图、有限资源、原生提交与恢复必须一起成立。原文、来源会话及 I01–I14 见 [INTENT.md](../unified-runtime-v3-20261114/INTENT.md)。

Dataset→真实 worker→数据交接→终端→原提交/ACK→恢复主链已经接通，真实硬件复验发现的跨机租约缺陷现已闭环，旧回归暴露的函数状态缺陷已修且真验；性能、完整业务和产品接线仍有缺口，不能宣布完成。最新完整矩阵生产候选 **ab1b334** 的 Local/真实 Ray Core 各98 API、168例全通过；工作台实际执行图、取消/恢复、原宽数据5项通过。转换前共驻、恢复回读、提前关闭及旧终端断言迁移均已完成定向验收。随后 **5ff5da9** 仅修私有环境第三方依赖误分类，真实 Ray 转换13项与终端4项通过，未借用旧矩阵冒称新HEAD全量验证。

用户已授权本机4张H100及20机2张H100。单卡、四卡、两宿主六卡及远端actor损失后的原ACK恢复均已实际验证；scope及复核见上方。引擎硬件验收与工作台GPU准入分别取证；最新工作台GPU已按上方55项组合通过。旧workers=1函数状态丢失、双机锁身份误用宿主设备号已修复，不再列为当前缺陷。

## 已验证的范围（每项保留自己的版本）

| 交付/验收 | 实际结果 | 证据与版本边界 |
| --- | --- | --- |
| Local 全公开入口及工作台 | 98 API、168案例全通过，107唯一执行；禁用Ray Data、真实浏览器图及取消/恢复通过 | [LOCAL-PRODUCT-FINAL-AB1B334.md](LOCAL-PRODUCT-FINAL-AB1B334.md)；4项547.35秒，生产/测试字节等于 `ab1b334`；保留原ACK/Lance/GC证据 |
| Ray 全公开入口 | 98 API、168原案例全通过；107唯一真实Ray Core执行、Ray Data导入0 | [RAY-API-AB1B334-VERIFICATION.md](RAY-API-AB1B334-VERIFICATION.md)；599.62秒，禁用门21.38秒；私有集群退出、FD原约束保持 |
| 宽数据、提交恢复与终端 | ab1原压力5项过；核心55项过；原stream受影响17项过；Local终端101项过 | [CORE-FINAL-AB1B334.md](CORE-FINAL-AB1B334.md)、[LOCAL-TERMINAL-FINAL-AB1B334.md](LOCAL-TERMINAL-FINAL-AB1B334.md)；原数据/预算/时限不变，集合不相加，窄余量不称完整性能达标 |
| 返回预算与GC争用 | 真实take_all进统一有界终端、iter_batches在decode前累计准入；原对象租约短暂争用不再误标已成功业务 | [返回预算](../unified-runtime-v3-20261114/review-20261115/TERMINAL-COLLECTION-BOUNDS.md)、[GC与supervisor](GC-SUPERVISOR-CONTENTION.md)；真实负例、计费/历史/FD原约束保持；转换前准入已在ab1最终集合验收，后续新问题另列 |
| 最终提交与无重复恢复 | 普通执行、持久交接完成晚于cut的两种真实竞态已修；完整结果恢复不再启动worker/追加提交/重发模型；合法前缀仍保持非final | [LATE-FINAL-CUT-CLOSURE.md](LATE-FINAL-CUT-CLOSURE.md)、[DURABLE-FINAL-STOP-CLOSURE.md](DURABLE-FINAL-STOP-CLOSURE.md)；普通修复95de52e、持久修复1d4281d |
| 工件GC与FD | 历史/unknown/prepared根、原物理租约、崩溃退款代际；原store准入真实句柄，排序输出和多view最小限均接通 | [PUBLISHED-GC-INDEPENDENT-REVIEW.md](PUBLISHED-GC-INDEPENDENT-REVIEW.md)、[ARTIFACT-FD-INDEPENDENT-REVIEW.md](ARTIFACT-FD-INDEPENDENT-REVIEW.md)；GC独立17项、最终FD独立11项，各自精确版本，不与其它集合相加 |
| 历史与工作台恢复 | 实际旧Git生成原生历史、当前只读与拒绝不兼容、原环境同RunRef恢复、1024/1025/1500 Prompt原身份；工作台唯一原生发布和固定版本 | [HISTORY-PROTECTION-AUDIT.md](HISTORY-PROTECTION-AUDIT.md)、[PRODUCT-INTENT-AUDIT.md](PRODUCT-INTENT-AUDIT.md)；当前v3旧中间状态适配未证明，V029仍NotRun |
| 原 33 个新失败 | 33/33 对应原 nodeid 通过 | [CONTRACT-SUITES-CLOSURE.md](CONTRACT-SUITES-CLOSURE.md)，完整 19 组 564 passed；原147、此前待验35也逐项映射 |
| 原 Local/stream 历史组 | 未改原断言的 106 passed | [ORIGINAL106-ACCEPTANCE.md](ORIGINAL106-ACCEPTANCE.md)，测试 tree 等于 `e6bdc2f`；不与564重复加总 |
| 默认普通管道 | 原31行案例约5.4秒，原先约93秒；真实默认4 worker得到独立source与后缀2 stage | [DEFAULT-SUFFIX-PLANNING.md](DEFAULT-SUFFIX-PLANNING.md)；保持任务重建、设备/显式隔离保护、单宿主容量与原预算 |
| 有限输入/恢复 | 原 seal 完整验证后借用；真实100×10 join中途ACK后恢复为1000行；不再重写整份输入载荷 | [FINITE-INPUT-BORROW.md](FINITE-INPUT-BORROW.md)；旧owned/pending兼容，完整成员/路由/摘要/预算反例 |
| 工作台计划与源码 | 同run/attempt原sealed plan、身份桥、prepare-only历史源码；修改当前代码仍看原快照 | [PRODUCT-LIFECYCLE-SOURCE-CLOSURE.md](PRODUCT-LIFECYCLE-SOURCE-CLOSURE.md)；root集成桥/Save组32项通过，浏览器三项在正确临时目录复验通过 |
| 原生 Save 与恢复 | 授权create/固定版本overwrite，原生发布/回执/ACK及发布后中断恢复 | [SAVE-PRODUCT-CLOSURE.md](SAVE-PRODUCT-CLOSURE.md)；root额外重验实际create和发布后恢复，不放开普通append |
| 失败与清理 | 真实启动失败、取消、业务失败叠加关闭失败，原异常可见，无成功ACK，FD4→4 | `/tmp/demiflow-final-control-probe.json`；独立失败链路，不代替全部资源峰值证明 |
| 数据保真与编码 | 原canonical字节保持，scalar/NumPy、负零、schema、block、引用、非canonical拒绝 | [CODEC-ENCODER-REUSE.md](CODEC-ENCODER-REUSE.md)，93项；没有payload缓存或跳过校验 |
| 后续真实Ray转换及终端 | 13项转换、4项终端通过 | [RAY-TERMINAL-5FF5DA9-CLOSURE.md](RAY-TERMINAL-5FF5DA9-CLOSURE.md)；依赖分类上限64模块/8MiB不变，原失败证据保留 |
| 真实物理GPU | 4 passed / 0 skipped，CPU→CUDA→CPU、final重放、取消/异常清理及中间ACK恢复 | [PHYSICAL-GPU-FINAL-5FF5DA9.md](PHYSICAL-GPU-FINAL-5FF5DA9.md)；1785输入独立字节比对全等，真实H100一张、5次原allocation全部RELEASED；不等于四卡或跨机GPU通过 |
| 真实两宿主 | 真实Dataset原四例4 passed；两host实际执行、远端actor损失后原ACK恢复、共享源union路由 | [PHYSICAL-HOSTS-CPU-CLOSURE.md](PHYSICAL-HOSTS-CPU-CLOSURE.md)；120.207秒，原红证据保留；1786输入及26份原journal归档总控独立核对，0不符 |

Local矩阵的旧汇总214是外层107条观察与案例层107条观察重复拼接，**实际为107次执行**。原始报告未篡改；测试记录器现按观察ID合并、保留两份原观察，独立业务调用即使参数相同也不折叠。本轮矩阵FD54→60通过原+8阈值，不能称零增长。

## 当前仍在处理的核心项

1. **性能及及时交接**：轻量 CPU 和跨机开销仍明显；40k 宽行 iter_rows 在原 120 秒预算内有一次 113.74 秒正例，完整五轮尚未通过。批量传输只合并已就绪有界消息，不等待凑满，不减少 fence/credit/ACK/行字节校验。九类 Local 及 Ray/两机正式矩阵未完成。
2. **路由正式门与既有公开 API**：multiworker barrier、driver restart、actor lifecycle 已真验；route recovery 只覆盖已验证子集。正在核已有 `map_prompt_async(routes=..., route=...)` 与原 RoutedPromptActor 的真实接线，并分别补公开业务路由和 C05 物理合同恢复，不靠增加未要求的 DSL 凑门。其余 NotRun 保持原实际缺口。
3. **完整业务兼容与最终组合**：工作台 object-read 和真实 GPU 已接通并组合验收；旧显式 `sqlite_journal` 的原合法 delivery-only 行为正在补兼容。driver 持原私有 HTTP/cache 账本、原 RunJournal 仍唯一提交及有限准入权威，未知效果不重发，不能把私有 journal 宣称为受管/物化原生恢复。普通 run_stream 与队列/producer 要按原支持边界分清。最终全矩阵仍需匹配最终代码；外部 PostgreSQL 没有测试 DSN，保持未验。
4. **历史与权威**：原RunJournal/CommitCoordinator、code-pin、旧来源/提交/unknown效果边界不变。已验证多机actor损失不代替整机/网络故障，旧布局容量客户端不自动获得新布局兼容保证。

## 对齐意图，而不是用测试数量折算百分比

| 原意图 | 当前可依赖的结论 | 仍需保留的边界 |
| --- | --- | --- |
| I01/I02 同一Dataset、Ray Core承载 | ab1 Local/Ray各168例；无Ray Data调用，真实全矩阵禁止导入 | 函数状态、双机租约与最新Ray原ACK恢复已真验；完整终验仍需匹配最终代码 |
| I03/I05/I06 交接、共享、多输入、及时背压 | 内存/持久增量/有限封存、实际共享图及join、限流与提前结束有真实专项 | 跨宿主完整路由故障矩阵未验 |
| I04 CPU/GPU独立 | 来源保持独立；纯CPU后缀可在真实容量允许时融合，GPU/显式隔离不被吸收 | 本机四卡及两宿主六卡实际CUDA/释放通过；不声称fractional/MIG或硬VRAM上限 |
| I07/I10/I13 单一权威及历史 | 原RunJournal/CommitCoordinator/真实ACK与恢复，未知效果不重发，原封存/代码来源保护 | 不把局部历史兼容推定为任意旧版本兼容；完整GC需保历史根 |
| I08 有限资源和性能 | 原预算反例保持，31行退步、GC/FD、driver累计返回已修且真验 | 原take_all压力通过不代替iter_rows压力；大图通过，轻量CPU/两机及完整性能矩阵未达标 |
| I09 真实业务 | Local模型/队列/fetch/Save及恢复有实际链路；外部模型仅本地transport替身 | 外部PostgreSQL未连库；旧显式模型缓存与托管服务兼容仍在收口 |
| I11 多机 | 真实两宿主Dataset、原生结果/ACK、共享源与actor恢复通过 | 真实两宿主Dataset及远端actor损失恢复通过；整机失效/网络分区未完成 |
| I12 产品 | 工作台看原执行图/来源、真实运行/结果/恢复，Notebook/Case已有专项回归 | 既有产品专项并非最新同HEAD全产品验收；引擎不代表全部产品root |
| I14 完整交付 | 逐公开入口及旧失败映射，显式保留未验项 | [旧17个正式门映射](FORMAL-GATE-EVIDENCE-MAP.md)中disk_gc现已接通；该表是早期快照，后续已接线门以上述带版本证据为准；剩余NotRun不返回常量伪造通过 |

## 历史与工作区纪律

主仓仍为 `e40a6dd`，原11处文档dirty保持；未在主仓运行测试、未push/合主仓、未接管在运服务。承重七件相对 `362d8528` 字节未变。

必须单独披露一次失误：诊断浏览器测试曾使用旧fixture默认目录，覆盖历史 `P1/screenshots/logical-source-unchecked.png` 一张图片。原字节未恢复；较早归档不能证明与原图相同，未冒充原件回填。主仓代码和业务数据未受影响。fixture默认已改为独立`/tmp`，后续截图显式隔离。详情 [BROWSER-EVIDENCE-INCIDENT.json](BROWSER-EVIDENCE-INCIDENT.json)。


2026-11-16另一次测试边界失误：容量回归漏设DEMIFLOW_RESOURCE_ROOT，触发共享默认池1019条已RELEASED历史归档和2条本测试预约。经原锁/测试时间窗/inode/字节验证，1019原名字已精确恢复；总1028现存记录与事故首次取证摘要全等，原5 BOUND未动，2测试预约已原协议释放且保留历史。总控独立再次逐条核对0不符。没有删除历史或退款其他owner；没有先前逐文件摘要，故不宣称整个事故与事故前字节证明已齐。今后入口强制私有pool，不将候选归档布局用于在运共享客户端。
