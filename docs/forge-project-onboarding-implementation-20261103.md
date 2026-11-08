# 真实项目正式接入：实施记录

用户已批准按 P0→P4 连续实施。本文区分已验证的子项和未完成阶段，不作为生产放行依据。

保护与测试证据目录：`/tmp/forge-onboarding-20261103-b5j6rbhw`。历史表保持固定版本只读；本轮不发起真实模型调用。用户明确排除 `training/`，其源码、Notebook 和训练产物不由本任务修改。其他既有 dirty 文件和 Notebook 输出仍受完整基线保护。

最新用户约束：用户将继续通过旧 pipeline 跑数据。迁移采用新增 Flow 并行验证；不删除、替换或自动切换旧 pipeline、旧 Notebook 入口及其业务依赖，不改其数据落点、运行账本或活动服务。新实现使用独立 `flows/`、`operators/`、配置、文档与登记；旧入口何时退出由用户另行决定。此约束覆盖原方案 P3 的“旧实现退出/活动消费者切换”要求。新增 Flow 仍须独立完成同一契约与业务等价性验收，不借旧入口获得认证。

## 阶段状态

| 阶段 | 当前实现与证据 | 尚需完成 |
| --- | --- | --- |
| P0 | candidate.7、project-v2、完整流式源快照和有界 Notebook 结构解析；37 项规范工具测试通过 | P3 将两个目标逐条迁移并绑定新任务；其他登记仍明确保留旧标准 |
| P1 | 非空工作区初始化、稳定项目 ID、登记清单、请求局部路由、明确 Flow 登记、精确历史位置解析；17 项 HTTP/浏览器测试通过；真实 identity、summary 和公共输入已原位读取；展示/数据绑定拆分、列表分页及每入口草稿已通过浏览器回归 | 最终多项目真实入口验收归 P4 |
| P2 | 原生值、CAS、checkpoint/stage、attempt journal、旧提交复用和封存恢复；resume、累计用量、工作区预算、模型 v2 契约；本地 tokenizer、资源节点、Prompt 分块、索引、表展示、历史资源和后代进程收尾已落地；纯计算及历史尝试定位回归通过 | 进入 P3 业务等价性验证；整项目 GP 矩阵仍待 P4，不以平台测试替代真实业务验收 |
| P3 | 两个真实 Flow 及独立 operators/配置/README 已实现；61 项业务隔离测试通过，两个封存 fixture run 成功；真实表原位只读接入 | 最终平台绑定下的 DemiForge context/check/verify/review/handoff 尚需收口；旧 pipeline 及其依赖仍保持原位 |
| P4 | 第三种 inventory Flow 的两次封存运行、wheel 包外运行和资产 HTTP 检查已通过；原 55971 入口已登记五个项目。新增侧栏/顶部功能导航完成页面回归 | 完整 GP-01～28 汇总、最终源码绑定与受管交接仍未完成；页面改版完成不代表生产放行 |

## P2 自审修订

1. Arrow 旧 schema hash 不包含 metadata，不能用作完整 schema 承诺。新增完整序列化身份，旧表身份算法和历史引用不变；写后读字段从实际声明 schema 推导。
2. 有限摘要同时检查 Python 对象大小和 JSON 编码大小。静态仅允许可记录的 JSON 字段；bytes 不能被默认为可写入运行日志。
3. 同表写入只接受显式 CAS 基线；原位置保持稳定。新表实验重定向不适用于覆盖模式。历史版本保持可读。
4. checkpoint 是原生持久资源，Flow 只持有平台返回的对象。实际执行仍是原 Python；动作边和值边用于核验，不新增图调度器。
5. 流式 stage 使用原生固定引用，不伪造覆盖多次提交的 LanceWriteReceipt。失败后的已提交 stage 单独登记；后续 guard 失败不删除其数据。
6. 初始化空表也有提交窗口。新受管 checkpoint format 2 先保存带 operation ID 的创建意图，再提交空表；恢复核对准确事务标记、完整 schema 和空表版本。旧 format 1 保持旧读取入口。
7. 流式 writer 的完整键集合改用可重建 SQLite 索引，缓存和文件页数有界，容量在批次提交前预留；索引故障使本次 writer 失效，重启从已提交表重建。checked 执行的索引归私有 work 目录。
8. 恢复意图的内容校验逐行哈希，不把整个已提交微批次重新收集到 Python 列表。歧义提交查验按键限至两行，不使用无界 `to_table`。
9. 输出文件监测是有界 inotify 增量观察，溢出拒绝继续。它不是内核磁盘配额；极短寿命文件和并发外部写入的计量边界仍需在最终运行预算说明中披露。
10. 新受管 run 显式创建 journal v2。事件、节点观察和 completion 按 attempt 隔离，旧 attempt 可读；状态 CAS 和写入 fencing 拒绝旧 worker。私有 work/log 按 attempt 分开，checkpoint 保持 run 级持久。原日志不自动升级；native CAS 能力不等于 resume 业务准入和累计预算已完成。
11. resume 要求原 manifest 摘要和停止的 attempt 身份。恢复先核验源码/资源绑定、提交证据、当前表头和剩余预算，再以 CAS 新建 attempt；原生 Python 重执行，已提交普通写入按原生事务标记及实际依赖摘要复用。业务 guard 失败不被标成可恢复技术错误。
12. 累计 elapsed_ms/output_bytes 保存在原生 journal；监督进程在启动 worker 前打开计量区间，停止后排空通知再关闭。监督进程丢失或通知溢出时，最终用量未知，不允许借恢复重置额度。固定结果仍可独立核验和查看。这是明确的恢复限制，不是 exactly-once 或内核配额承诺。
13. 增量文件计量改为有界近期缓存，淘汰后再次修改只会保守重计，避免 8192 个终身文件限制阻塞大流。目录突增、通知丢失仍拒绝。计量覆盖观察到的新文件/正向增长，不声称统计所有写盘流量。
14. 预算配置归一个 `run_limits.py`：通用部署上限→工作区显式政策→单次运行收紧。工作区预算改变会使相关检查 stale；原生查询仍有独立准入和每查询 3600 秒上限，不能将更大的 run 总时限误作单次查询无限放行。
15. 模型扩展复用原 PromptActor、SystemOne 协议和 SQLite ledger。多题 answers 是有类型的映射，缺答/errors 保留；图片数量、图片字节、请求字节和字符/token 分开。模型 revision/profile 是绑定及所有者声明，不等于远端部署已获验证。原始调用证据保存在 ledger；业务结果表仍需用稳定 schema 投影/编码，不能默认任意嵌套调用元数据都适合直接写入 Lance。
16. 用户后续明确整个 `training/` 不修改，因此原像素 helper 暂按原路径、原内容保留，保护训练目录中的消费者。正式迁移的数据 Flow 使用原生像素能力；不新建导入转发。此范围修订覆盖原方案“同步全部活动消费者”中涉及 training 的部分。
17. 受管 token 计量要求显式本地 BPE 文件及 SHA256、pattern/special tokens；文件计入资源闭包和源码预算，检查/封存/启动均核对。运行使用公开 `tiktoken.Encoding`，不按名称下载编码文件；普通原生旧名称 profile 保留原行为。
18. `read_object` 只接受登记位置及显式无歧义目录迁移，逐路径目录描述符拒绝符号链接、读取前限文件大小、读取后校验 SHA256。文档、wiki、图像处理分别由原生节点输出结构化值；导语/模板名单等业务策略不进入平台。
19. 图像新增版本化编码 profile，保留 load→EXIF→RGBA→白底 alpha composite→RGB→BICUBIC thumbnail→JPEG 的顺序；原 profile 默认不变。Pillow 子进程继续使用有限内存/CPU/输出/时限，并绑定父进程退出。
20. Prompt 分块复用原 renderer 与 CharacterCounter，只输出有序 start/stop/error 范围；不选业务候选、不构造 chunk ID、不发请求。单条超限保留错误范围；独立计量工作预算耗尽时明确失败，不改变已定义分组以假装成功。
21. 原生节点声明所需第三方 distributions；运行前要求它们存在于同一平台快照。工作区 init/bind-platform 支持显式 extras，未绑定依赖时拒绝运行；不自动修改共享环境。
22. 输出已提交但结果登记曾被 preview 的 4 GiB 地址空间门禁拒绝。登记现在由现有隔离 worker 读取固定 manifest 元数据，不在执行 worker 内调用预览，也不为登记遍历全部 fragment。Case 读取仍执行独立 fragment 与文件检查；不能把元数据登记当作数据访问授权。
23. 一个 SQLite 派生索引承载 run/report 有界分页；原事实发布前写 pending 标记，发布后补索引再清标记。损坏、缺失、待同步明确显示，显式有界重建，不在普通刷新时全扫。索引不作为执行准入依据；执行锁内保存实际 owner，并核对该运行的原始 journal。
24. workspace-v2 运行准备要求入口已登记、文件配置属于该入口；未登记 helper 不能通过直接 HTTP/CLI prepare 执行。旧工作区格式保留原行为。新运行的默认预算可以由显式工作区政策扩大，操作台显示同一有效上限。
25. 表展示采用独立 v2 控制声明，完整控制仍 CAS；数据身份归一到原 v1 固定引用，因此改标题/字段不改变数据绑定或已有 Flow 检查。图片与证据的嵌套字段只声明路径、位置和边界，不提供 handler、任意代码或业务完成公式。
26. 历史图片只从重读的固定 Case 字段取得 URI/SHA256，限制声明根、明确迁移和大小并核对哈希；历史调用记录只读声明位置的原生 PromptRecordRef。证据查看禁用离线图片自动展开，过滤明确的传输凭据键和 URL 参数；不宣称能识别业务正文中任意秘密，也不补造历史源码认证。
27. 取消后的最终计量必须覆盖所有原生子进程。专用 supervisor 作为 Linux subreaper 接收退出 worker 的孤儿后代，停止本次进程组并有界回收；未能回收时用量仍未知，不允许恢复重置额度，也不清理仍可能在用的私有 work 目录。不会接管其他任务的进程。
28. 标签类完整导入需要拒绝源字段静默增加；新增原生 `require_columns`，静态与实际执行均核对完整列名后再投影。它不替代完整 Arrow schema/类型契约，也不加入具体业务字段。
29. 表展示字段先对固定 schema 验证嵌套路径，原生 Case 投影/匹配支持明确 struct 路径。先匹配真实完整列名，再处理分段路径，各段引用转义，不接收 SQL 表达式；列表展示与筛选语义仍分开。
30. 历史 attempt 的节点/复制链接须带准确尝试编号，不能点击后跳回当前尝试。历史节点只展示该尝试的固定结果引用，不能复用当前输出别名误开新版本。
31. 复审发现方案中的纯标准库支持尚未落地，补齐受限 JSON、SHA256、数学、固定正则/HTML 文本处理、排序及有类型局部列表修改。JSON 解析值仅在检查器内部保留未判定结构，必须经 builtin 类型判断后访问；不能用注解直接转换。复制外层容器不授予借用子对象的修改权。拒绝 JSON hook、文件摘要、任意 codec/动态正则或未知调用；仍不声称正则/循环普遍终止或第三方分配可精确预测，运行预算独立生效。

## 当前测试证据

| 日志 | 结果及范围 |
| --- | --- |
| `p1-browser-tests.log` | 17 passed；项目路由与浏览器回归 |
| `p2-summary-tests.log` | 94 passed；有限摘要、标量与实际原生执行 |
| `p2-cas-tests.log` | 85 passed；CAS 和增量产物计量等 |
| `p2-sealed-values-tests.log` | 32 passed，200.49s；封存查询/guard、同表覆盖、工作区依赖绑定、版本化平台快照 |
| `p2-checkpoint-contract-tests.log` | 121 passed，17.51s；原生 checkpoint、固定回读、所有权、空流、初始化/append 提交故障窗口、键索引超限、既有 stream/Flow 检查；随后增加了逐行恢复哈希与受管结果登记，须以新回归为准 |
| `p2-sealed-checkpoint-tests.log` | 17 passed，83.88s；实际封存 worker 的 stage 输出、后续失败和结果表读取 |
| `p2-attempt-journal-tests.log` | 初轮 44 passed；新增 worker 绑定后聚焦 native 回归 30 passed。覆盖并发 CAS、所有旧 worker 写入入口拒绝、旧事实保留和旧格式不迁移 |
| `p2-sealed-attempt-tests.log` | 7 passed，254.94s；新 journal 的封存执行、日志定位和 scratch 清理 |
| `p2-recovery-integration-tests.log` | 41 passed，98.11s；由真实 checkpoint 和封存契约恢复遗漏的 stage 登记 |
| `p2-history-review-tests.log` | 54 passed，13.26s；历史事实、提交复用和依赖变化拒绝等 |
| `p2-resume-native-tests.log` | 14 passed，2.02s；累计用量、attempt fencing、有限文件缓存 |
| `p2-resume-integration-tests.log` | 10 passed / 1 failed；恢复和旧提交复用实际通过，失败为重复请求拒绝文案的断言只接受 attempt 冲突、未接受执行租约冲突。已修正断言，保留原保护规则并复跑 |
| `p2-model-contract-tests.log` | 49 passed，16.43s；SystemOne 多题/图片、缺答与原始证据、请求复用、静态与实际模型事实、预算边界等。随后补充预算漂移与较大 ledger 显式上限测试，待新回归 |
| `p2-resume-model-integration-tests.log` | 8 passed，330.43s；真实封存 resume、值/stage、模型证据与 CLI |
| `p2-local-resource-tests.log` | 57 passed，16.62s；本地文件/编码、模型、原生预算与资源回归 |
| `p2-collect-resource-tests.log` | 29 passed，9.70s；真实原生对象/文档/wiki/图像调用与四种模式/方向的 JPEG 逐字节对比 |
| `p2-native-resource-packing-tests.log` | 43 passed，14.76s；资源与分块合并回归、实际节点参数漂移拒绝 |
| `p2-sealed-resources-tests.log` | 3 passed / 3 failed，231.48s；封存 tokenizer、模型结果及取消通过；三个 collect fixture 在流式节点后直接投影，缺少 materialize，被契约正确拒绝。已修正 fixture 接线，未放宽检查 |
| `p2-sealed-collect-retest.log` | 3 failed，110.73s；定位到已提交表的登记直接调用 preview 与执行进程内存上限冲突，已按第 22 条修正 |
| `p2-resource-registration-retest.log` | 11 passed / 3 failed，409.64s；三个资源封存 fixture 及值/stage 通过，三个旧取消 fixture 被零 drain 值误加余量拒绝；已修正为仅对显式非零 drain 检查 |
| `p2-index-tests.log` | 6 passed，3.30s；超过 128 报告、分页中新写入、pending 补录、索引损坏及重建 |
| `p2-index-workspace-tests.log` | 33 passed / 3 failed，360.98s；旧 fixture 直接制造未索引记录、旧配额断言及 snapshot mock 参数不匹配，已按新明确协议修正 fixture |
| `p2-index-integration-retest.log` | 14 passed / 1 failed，512.99s；取消/故障/恢复/CLI 通过；分页 SQL 断言未接受新增 attempt 条件，已修正断言，保留有界 action 投影检查并复跑 |
| `p2-table-view-process-tests.log` | 14 passed，208.94s；表身份/展示、图片/历史证据、进程组和恢复测试 |
| `p2-foundation-review-retest.log` | 64 passed，25.46s；原生列检查、索引、进程组、模型账本、资源和分块；初轮 normalize_stream_parameters 误放代码已修复并复验 |
| `p2-contract-review-regression.log` | 120 passed，9.23s；契约、stream、模型、本地 tokenizer、checkpoint 和 attempts |
| `p2-onboarding-browser-retest.log` | 18 passed，586.83s；真实浏览器的表展示、每入口草稿、自动同步、运行/结果对照与继续加工 |
| `p2-compute-regression.log` | 133 passed / 1 failed，22.80s；旧 math.floor 拒绝断言与新受限纯计算支持冲突，已更新为支持正例并保留未知 math 调用拒绝负例 |
| `p2-pure-compute-retest.log` | 82 passed，6.29s；纯计算闭包、拒绝类型伪装/隐藏副作用/借用修改，并经实际 checked Dataset 与 Lance sink 验证摘要输出 |
| `p2-attempt-browser-tests.log` | 5 passed，261.84s；补充历史尝试的节点定位/刷新，并复验完整运行页面 |

曾出现的测试错误已保留在工作记录中：一个断言误读 `describe_table` 返回层级；一个直接启动 preview worker 的测试缺少生产入口已有的 BLAS 线程约束。均修正测试本身，并实际复验；没有据此放宽平台准入或修改其他会话的进程。

P2 分项回归及 P3 的代码/隔离验证已完成。完整 GP 矩阵和最终受管交接仍待 P4 收口；fixture 成功不代表真实模型质量、全量性能或生产放行。


## P3/P4 续接进展与呈现调整

- identity 的 audit/import_labels 使用同一 Flow 的静态配置分支；完整原始标签与评估字段保留，重复键和覆盖校验先于模型/提交，更新采用显式 CAS。旧 identity 入口未改。
- concept_images 在完整有序候选上做双侧审核，保留缺答/分歧，按确定性分块复用已提交 checkpoint；分别生成完整明细、accepted、stage 和 summary。旧关系审核入口、Notebook、训练目录及公共历史表未改。
- 第三种 `examples/forge_inventory` 使用多输入归约、关联、guard 和双输出；不含模型，也没有新增平台业务分支。
- 本次用户要求先仅调整呈现：项目侧栏 + 顶部四个功能入口，继续使用原工作区/API/定位/自动同步。没有接入 Codex CLI 或开发对话。原 SSH 转发仍为本机 55971 → 服务器 59865。
- 页面改版使最终分发文件发生变化；此前的 wheel 和业务 verify 保留为对应版本的证据，不能冒充最终源码验证。后续需在源码固定后完成最终绑定与受管交接。

新增验证记录：`p3-business-final-tests.log` 为 61 passed；`p4-inventory-tests-2.log` 为 1 passed（包含两次实际运行）；`p4-wheel-release-acceptance.json` 是页面改版前的包外封存运行及 UI 资产验证。`p4-layout-browser-tests.log` 为 11 passed，`workbench-layout-acceptance.json` 为 12 项实际界面检查、零 page error。

全量平台首轮为 2,396 passed / 18 failed / 20 skipped / 42 deselected，不能写为全绿。失败已分别定位到公开导出断言、预览子进程继承地址空间限制、旧错误文本断言、浏览器安装路径、模型 fixture 缺少显式 llm extras 和连接回收时序；针对性修订/复跑见 `p4-package-fix-tests.log`、`p4-recovery-fixed-tests.log`、`p4-search-browser-tests.log`、`p4-model-browser-tests.log`。不因这些测试修改生产表或放宽执行准入。
