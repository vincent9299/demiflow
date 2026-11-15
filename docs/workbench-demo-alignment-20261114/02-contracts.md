# 冻结契约 UI-D1.9/2

## C0. 形态、所有权和生命周期

沿用当前原生 JS、IIFE/global factory，无新增打包器或框架。**纯计算用函数；持有 DOM、请求和订阅的对象用 factory 返回 controller；持久对象复用已有 Store。** 不为凑“全是类”增加空服务类。后端新 `VersionResolution` 是内部只读 dataclass，不能作为浏览器传入的权限证明。

唯一新增公共命名空间：`ForgeWorkbenchPresentation`（C）、`ForgeAnalysisCharts`（D）。现有 `ForgeWorkbenchGraph` 扩展统一 `mount/update`；`ForgeAnalysisRender.field` 增可选呈现参数；其他 mount 入口保持。A 在 index.html 装配；C/D 不各自注入脚本。所有 API 请求仍走 app.api，不另建 transport。

对象约束：每 controller 只持有自己的 DOM/取消控制器/有限展示状态；`update` 不改传入快照；`dispose` 幂等，停止自己的 observer/request/RAF，释放 blob/worker/media。离开前先 `canLeave/flushDraft`；失败不拆 DOM。异步返回只能更新创建请求时的 project/epoch/binding/selection，不能更新后来选中的对象。

错误统一为已有 `{code,message,operationStarted?}`；只读请求容量不足可以退避/显式重试，写响应不确定查原 request_id 回执。**超时≠未提交**。全局错误摘要不能吞掉字段冲突、权限、来源变化和恢复限制。

## C1. 壳、空间与交互

- 项目侧栏桌面目标宽度208px（长名截断+完整title），收起56px；拖窄窗口不把内容缩成不可读小字。四tab同一项目头部，开发/Readme控制器保持。
- 字号：正文13–14px，说明至少12px；图节点的屏幕实际字号至少12px。参考色使用既有accent/line/muted变量，不使用极浅文本代替层级。
- 图/Case按**内容容器可用宽度**分栏，ResizeObserver只在尺寸变化时布局，不因轮询重排。Case主体>=1040px三栏（约180px / minmax(0,1fr) / 250px）；720–1039px筛选收起、评测可折叠；<720px材料优先、筛选/评测原位面板，不长驻三栏。项目侧栏/数据目录收起必须释放grid列。
- Notebook/Case模式与当前模式主动作在同一行；A创建两个独立actions容器，传给 `ForgeAnalysis.mount({toolbarHost:{notebook,case},...})`。D使用notebook容器，并把case容器传 `ForgeCase.mount({toolbarHost:case,...})`；setMode只切各自容器显隐，dispose只移除自身节点。缺省toolbarHost保持旧本地bar，兼容旧调用。普通状态一行，技术依据放“来源详情”。错误/未保存/部分结果/未知回执不能收起到看不见。
- 图不自动缩到不可读：初始可读布局，超宽横向浏览；“适应全图”由用户显式选择，可切回可读。选择远节点只滚动图容器，不跳动整个页面。
- 所有按钮可键盘访问、可见焦点、明确aria-label。320/390/768/1024/1440/1920px页面无意外横溢出，表和图局部可横滚；缩窄或换tab保留草稿/选择。
- 壳/数据公共 CSS 由 A 迁到 style.css；B 从 workbench.css 同批移除其现有 `.workspace-*`、`.data-*` 公共段。C 图样式迁入新 workbench-graph.css；B删除相同旧规则。不靠后加载覆盖永久保留两套规则。

## C2. 图主视角与事实模型（B消费，C提供）

`chooseGraphView(graph, {actionId:null|string, preference:'auto'|'report'}) -> GraphChoice`。

| binding.basis_kind | auto | 降级/切换 |
|---|---|---|
| source | 源码观察，明确“尚未检查”；只观察到调用时提供检查入口 | 不自动执行用户代码；无read权限/入口则显示原因 |
| report | 报告节点/边，报告状态、freshness不省略 | 不默认另算声明DAG；已有声明投影只在高级诊断 |
| run | 按选中 action 展示 status=sealed 的真实封存DAG | 多action选择器；未封存显示本action原因及可切“当时检查图”，不能静默换成当前图 |

默认action：保留仍存在的选择，否则首个服务端顺序的action；若首个未封存仍显示其缺口，不跳过失败action伪装全成功。`preference='report'` 为明确只读替代视角，标题“当时检查图”，不是“执行计划”。进入新的run/attempt重置action与node，不能沿用旧桥。

`buildGraphModel(graph, {choice, snapshot, selection}) -> GraphModel` 是纯函数。原始图 JSON仍为 `demiflow_workbench_graph_v1`（`graph/model.py::get`）；参数、freshness、completeness、logical.actions 不复制编译。服务端未提供的事实返回null/reason，不发网络请求。

- 报告/源码节点key为JSON序列化 `[basis_digest,report_node_id]`。**按版本区分**：legacy（demiflow.logical_dag.v1）边仅logical ID+port，按逻辑组绘图，组key为`[basis_digest,action_id,logical_node_id,null]`，成员key补occurrence；所有成员保留，不能猜实例边。v3（demiflow.dataflow_graph.v3）按occurrence逐个绘图，key为`[basis_digest,action_id,dag_id,plan_id,plan_revision,occurrence_id]`，边保留原edge_id和两端occurrence/port。必须经C09，缺端点明确unavailable，不能套legacy画法。精确DTO与提供方补充见C8。
- action/dag/plan身份完全来自封存facts；bridge只按原action/dag/report校验结果使用，未知/多个候选不取第一个。source请求复用原`graph-source`，携带graph_binding/node_id及明确logical_node_id/occurrence_id。
- Card：key是绘图节点/组key，边和布局使用它；selection是默认点击目标（报告节点、单成员的明确occurrence，或多成员组），occurrences保留全部成员选择；选中成员时高亮所属组。类型、人可读标题、最多两条事实摘要、状态。平台API类型可静态翻译为“读取/关联/过滤/写出”；自定义函数保留实名，不能自动编造业务用途。摘要取已提供 alias/字段/参数/结果版本；省略时能展开原值。source_call只说“调用入口”。
- Inspector：概况→输入输出→参数→源码入口→高级依据。输入字段仅可显示该边上游已知输出字段并注明来源；缺类型/端口为未知，不能推断契约。不可编辑时显示原因；只有实际可写改动才出现保存动作。
- 观测继续使用既有真实报告节点/occurrence桥；legacy多成员组未选一次出现时只显示“多次出现/状态不同”和逻辑概况，不拿第一条bridge读源码/参数。选定成员后才关联精确report node与观测。fused/unmeasured/unsupported不可用全run CPU复制成逐节点CPU。未知和已完成输出、整体run状态分别显示。
- payload预算沿用服务端1MiB及256节点/512边上界；超限拒绝/明确局部图，不递归全量取图。定位调用既有 node_index/graph node_id，单次64条索引。
- `ForgeWorkbenchGraph.mount(host,{onSelect}) -> GraphController` 新 `update(GraphModel)`；旧 `mountLogical` 只可在本批迁移中短暂保留，B迁移完成后同批删除旧消费者和重复绘图逻辑。status和cpuChart已有消费者继续保留。

接线冻结：`state.setGraphView({preference,actionId,execution=null})`只在原State更新图选择并返回原capture ticket。B的`paintGraph()`统一调chooseGraphView/buildGraphModel/update；fetchGraph和observer.onSnapshot都走它，包括unsupported与状态变化。旧graphView.paint、logicalView.update/select/dispose必须全部迁移；旧报告node定位经chooseNode委托到唯一selectGraphNode。GraphController.update对同结构快照仅更新状态/文案，不能每次轮询清SVG或重置滚动。

深链接：A10将v2 run视图扩展可选`action_id`（非空<=128字符）、`logical_node_id`（SHA256）、`occurrence_id`（SHA256）。logical要求action，occurrence要求logical；这些字段出现时要求明确attempt。原node_id/report_digest成对约束不变。B03写完整定位，B01读取sealed图后核验action/成员和可选报告桥；无桥可只存逻辑引用而不伪造node_id。用户给出的未知/过期定位显示缺口，不能回首个action冒充定位成功。组链接不带occurrence，成员链接带完整出现身份；不向graph HTTP请求加入未声明参数。

## C3. 配置、检查、提交、运行与结果（B）

不改变 `ForgeWorkbenchCommands` 的identity/prepare/start/inspect/saveParameters语义。编辑器与历史context互斥，配置draft key仍project|entry|params。变更参数成功后report视为需复核，未经检查不提交；检查按钮是显式行为，不因打开页面自动运行检查。

“检查问题”显示严重度/问题/影响节点/下一步，不只报规则编号。点击问题选择同一图节点并切对应配置/来源；不允许前端猜修复值或自动修代码。整图与节点参数共用同一份draft和CAS写入口。

“运行”保留prepare→start，按钮在过程中显示“正在准备/提交”。点击时捕获参数sha/report/input/budget；响应未知进入原pending intent核对，不能再造request_id。配置保存未知同理。完成后打开返回的run/attempt；用户已切走则仅在原scope记录结果，不抢走当前视角。

run selector与attempt selector独立、可分页；历史操作只读。cancel/resume目标在点击时固定，并以服务端capability/recovery限制判定，不能因新UI移除`recovery_restriction`提示。日志页标“本次尝试日志”，当前节点只过滤node observations；控制事件不是逐节点执行事件。

结果：显示committed/未提交/已提交待登记、固定版本和行数；每产物直接进入数据页固定binding。部分成功保留真实已提交结果和失败原因；不把失败run的部分产物称全部成功。run_compare保留旧URL/能力，不新增第二比较存储；现行接口按left/right run ID读取，界面明确显示返回的实际attempt，不宣称已支持任意历史attempt对比。

## C4. 数据上下文和版本链（A消费，F提供）

`DataContext` 有两份明确引用：catalog当前选择；Case/Notebook实际使用的source/session sources。共用一行展示，但来源不同必须说明“目录表”与“当前分析固定来源”；不得用目录v8给Notebook中v7输出改标签。

目录项展示别名/标题、固定版本、行数；原生表、识别项目表、已提交运行产物分组而非另开平行工作台。样例与分布使用已选择的固定binding。当前已有原项目 project_table 可指定version，本设计不改其身份规则。

### C4.1 workspace_table 历史版本

当前`table-versions`只提供已登记历史元信息。扩展仍在原Catalog/worker读取链，禁止通过attach_table临时改别名，也不建立新表注册数据库。

1. 新内部 `catalog_versions.resolve_version(root, *, alias, expected_binding, version, expected_version_binding=None) -> VersionResolution`：
   - expected_binding 是**当前目录别名的preview_binding.digest**。先用原table_binding验证别名和权限；version必须正int、已登记、同dataset_id/store_id/relative_uri。run:产物只允许封存版本。
   - 取Catalog对应DatasetRef，校验真实version/schema_hash/row_count；虚拟control仅替换ref，调用现有preview_binding得到**版本binding**。不改变原control文件，不另写身份算法。
   - expected_version_binding若给定必须等于上述digest；不等抛conflict。旧alias漂移也抛conflict，不能凭旧URL猜新别名。校验读取前后同一来源。
   - 返回只读VersionResolution：alias_binding、binding（原preview_binding形状）、reference（虚拟control）、ref（DatasetRef）、uri（仅服务端Path）、metadata（原preview.describe_table形状）。只读约束包括嵌套对象；不传原可变control引用供调用方修改。`to_wire()`返回独立普通JSON dict/list，包含version/alias_binding/binding/reference/metadata，供响应和原_snapshot使用；不能将MappingProxy直接送JSON或原type(dict)校验。
2. `GET table-versions(alias,expected_binding,version?)`：原字段不变；selected在原`{version,metadata}`上增`binding,reference,alias_binding`。无version不打开每个历史表，列表仍128上限，注册列表本身的现有限制不借本批扩容。
3. 原`table / sample / table-profile / media / table-evidence`增可选查询`version`和`version_binding`，**必须成对**；现有expected_binding继续指alias_binding，历史请求必须提供。新分支先resolve_version，再调用同一native reader；响应原`.binding.digest`是所读version_binding；table-profile原字符串`.binding`同理。旧分支缺这两个字段时输出和摘要逐字兼容。此处不接收URI、路径、schema或rows作为权威输入。
4. 分析locator旧形状不变：`{kind:'workspace_table',alias,binding,columns}`。历史扩展为`{kind:'workspace_table',alias,binding:<version_binding>,columns,version,alias_binding}`，version/alias_binding成对且version为正int；binding指实际读取的版本。source_snapshot.binding使用此binding，schema/rows来自实际版本。
5. `sources.locator -> web_sources.resolve/verify -> source_export.fixed_input -> source_query/read/export/media`全链透传。ws.sample_table验证Case membership时必须带alias_binding、version、version_binding；不能只有resolve支持而export仍读当前版本。`media_source_export._selection`现有“当前ref.version必须等于读取version”须迁移为历史分支选择已核验ref；当前公开资源声明和_recheck仍保留。新增字段不改变旧media witness。所有调用方迁移都由F卡覆盖。
6. A数据页选择历史版本只替换自己的固定上下文；D新增内核绑定/E切source显式采用此locator；已运行内核sources和旧标注不改。已有raw图片fallback也传version信息，不能版本A的文字配版本B的图片。

### C4.2 视图链接

v1/v2所有现有URL保留；v2 `table_request/table/case`可选version和alias_binding成对，version正safe-int（浏览器可准确表示范围），alias_binding为SHA256。`table/case.binding_digest`是版本binding；table_request.expected_binding可缺省，但历史version存在时必须等于alias_binding。后端仍验证真实别名与版本；链接不授予权限。run输出保持原run链接。

### C4.3 分布与Notebook互用

只画服务端field.enumeration计数和coverage：null/other/nonfinite/长值分开；样本统计标签必须保留。没有直方图bins不能从min/max虚构直方图。点击类别产生**可见的筛选草稿**，不声称样例已经筛过全表；应用到Case用typed_filter，插入SQL为显式模板且经用户运行。字段/alias标识按双引号转义，字面量正确转义，未知类型不给模板。结果不是完整聚合时图上标“当前结果页”。

## C5. Notebook 操作与输出（D）

已有 `ForgeAnalysis.mount` 返回对象和 Store 格式保留；新内部 `requestCellRun(cellId,{advance=false}) -> Promise<CellRunOutcome>`，由运行按钮/快捷键调用。状态序列：captured→saving→starting（必要时）→submitting→submitted；中间可blocked/pending/cancelled。CellRunOutcome见types。

1. 捕获project/epoch/notebook_id/revision/cell内容/source bindings及generation；校验analysis_write/execute、代码/单元预算；按钮告诉用户是否“保存并运行/启动并运行”。用户点击授权这一串既有操作。
2. 复用`save`→原mutate/outbox/CAS；有冲突、未知回执、用户在await期间改了待执行单元或切换项目，停止后续步骤并保留草稿。不因为保存了其他版本而执行新编辑内容。
3. 没有活内核可按已捕获来源session-start，并用原poll等待可提交状态；不能在前端伪造idle。已有内核sources不等于编辑器绑定时，给“继续原内核/显式停止后重启”选择，默认不破坏活内核。只读页面不触发启动。
4. cell-submit携带实际已保存revision、cell_id、source_sha256、generation；同单元双击只有一个在途链。响应未知保留原request_id等待receipt；会话generation变化后不自动重发。
5. 取消仍走cell-cancel，活执行取消可能停止整代，原说明保留；快捷键Ctrl/Cmd+Enter仅运行当前单元，Shift+Enter受理后移到下一单元，不批量执行隐藏内容。
6. `paintCells`按cell.id保留编辑DOM和选区；poll只更新状态/输出槽，不清空所有textarea/已加载结果。输出按execution/artifact/generation绑定；编辑代码后保留旧输出但标“来自上次代码”。切换输出/离开释放媒体。

输出呈现优先表格/Markdown/文本/图片等实际可用格式，格式切换名称用用户可读名称，完整MIME/摘要在详情。`ForgeAnalysisCharts.mount`只接受已读的有限TableSlice；<=200行/16列/256KiB，只支持有限数值的柱图/折线。null留空，非有限/不安全整数拒绝图表且保留原值；不自动聚合、不读全表、不按行号做跨结果join。柱图从0轴，折线不暗中排序。图表界面明确选择X/Y，只有有意义的数值列才提供选项。

`analysis-output.mountExecution`仍由现有output接口读取，不能把table handle本身当数据。固定table MIME打开目录走onTable现有源注册；内联图表必须通过以下真实读链，不能只写一个没有调用数据源的图表组件。

### C5.1 固定表输出的内联数据读取

`readTableSlice(handle,{project,executionId,outputId,outputIndex,artifactDigest,api,signal,isCurrent})`：捕获原artifact identity，调用既有`analysis-source-from-handle`（body `{handle:{id,generation,digest}}`）；选该source.schema中前16个primitive列（bool/int64/float64/utf8），无可呈现列返回明确不可绘制原因并保留固定表跳转。

用现有协议，不增后端端点：descriptor为`{version:1,source_snapshot:source.digest,case_key:source.case_key,fields:[{id:'col0',field:<column>,label:<column>,role:'context',format:'text'|'json'}...],grouping:[],filters:null,sort:[],layout:'card',questions:[]}`；view为`{id:null,revision:0,descriptor}`；query为`{kind:'cases',columns:<相同列顺序>,grouping:[],filters:null,sort:[],scope:'sample',limit:20}`。调用`analysis-case-page`，body `{digest:source.digest,query,view}`，只取一页、不存view、不写annotation。服务端已有source权限、读预算和native worker保持。

响应核对source_snapshot；`view_digest = await ForgeCase.hash(view)`，`query_digest = await ForgeCase.hash({source:source.digest,view:view_digest,query})`，其中query包含上面所有明确字段、没有offset或review。这与case_pages.prepare的规范化一致，不能直接hash(query)。核验scope_digest/watermark有效性；旧output槽/epoch失效则丢弃。TableSlice.identity.binding=source.binding，同时保留artifactDigest、outputId、executionId、outputIndex与sourceSnapshot；实际行<=20且256KiB，再交图表。coverage以原response为准，始终明确“当前输出的前20行预览”，不能称全表统计。换representation取消在途读、释放图；来源变动不改读最新表。

`mountExecution`将现有api显式传给`analysis-output.mount({...原参数,api})`，后者再交readTableSlice。旧独立mount调用没有api时不启用内联取表，仍可用原onTable；不另建裸fetch。

## C6. Case 与共享字段（E消费，D提供）

现有Case v1/v2 descriptor、Store、mutation/outbox、records保存不变。不新增脱离source_snapshot的前端case ID或“模型账本”。Case标题选descriptor.fields中首个role=context且format=text的非空短字符串，case_key仍作为副标题/身份；重排context可改标题，无需新schema字段。

- 顶栏：视图名称/选已有视图、配置视图、保存；只一处显示dirty/待核对。来源摘要融入A数据栏，完整摘要、历史在详情。
- 左栏：现有query/filter/grouping/review_filter的快捷编辑；复杂组合折叠。服务端不支持的标注筛选明确禁用，不能用当前页伪全源筛选。切页/条件变化取消迟到响应；分页cursor/watermark保留。
- 中栏：共享输入/参考在上，输出方案在下；v2每group.fields按既有display field id取真实字段，最多8组/32材料；v1保持字段模式，不自动写成v2。三种layout=card/comparison/gallery原语义不变。
- 右栏：整个Case/已验证方案/真实字段选择；仅case-group-bindings返回的proof可授权方案级保存；未绑定方案仍可看材料，标注按钮解释不可用。换scope保留独立草稿，不复制答案。
- 配置原位面板分“材料与布局 / 评测题目 / 筛选与排序”，继续同一个descriptor草稿，明确应用预览和保存视图；无格式推断覆盖用户配置。引导首次配置应选择字段并查看真实样例，不自动重写项目文件。
- 保存并下一条只有原saveCurrent/acknowledge确认后才前进；冲突、未知回执、媒体依据变化不前进，显示可核对的旧/当前答案。折叠历史不改变expected head/watermark。

`ForgeAnalysisRender.field`增`density:'normal'|'compact'`和`tools:'inline'|'menu'`（均可缺省，旧行为normal/inline）。返回`{element,ready,dispose}`保持。compact默认直接看材料，格式/复制/原值入“更多”，Markdown使用已有reader的紧凑呈现参数；若没有可用紧凑参数，只在本字段容器隐藏重复装饰，不能改全局reader安全行为。加载失败/超限/来源失效/HTML受限/类型不符仍直显，原值可达。

媒体保持现有resolveMedia/CaseMedia链和用户点击预览准入；不因首屏漂亮自动拉取全部音视频或放宽URL。原位展开按容器适配，缩窗不裁切主内容，关闭/切Case释放资源。源图片没有时展示缺失，不用demo素材顶替。

## C7. 边界样本与预算

冻结样本至少包括：source-only入口、7节点报告、两个sealed action、同logical_id两occurrence、缺桥/缺历史源码、图裁剪、stale参数、未知prepare回执；新旧table locator各一、alias漂移、已删历史版本；dirty notebook/过期revision/内核generation变化/未知submit；Case v1、v2两方案每组图片+文本+评分、重复业务键、未绑定方案、标注冲突、媒体失效。V使用真实fixtures生成，不手写“契约通过”的假事实。

沿用后端上限；前端不能加大读取量来补体验。样例页默认20最多100、投影<=16（分析显式来源<=32）、分布原1000行/8MiB投影上限；API原请求64KiB/URL8192等保持。图表内存256KiB、字段原渲染256KiB、Case原32字段、Notebook64单元/每单元16KiB保持。UI只缓存当前页与已有有界草稿，不预取全部运行/Case/媒体。所有读写不确定性与权限以已有服务返回为准。

## C8. Runtime v3 与平台 W 接线

本契约的不可分部分是[05-runtime-v3-wiring.md](05-runtime-v3-wiring.md)和[runtime-wiring.json](runtime-wiring.json)。它限定新旧plan schema、C09的输入输出、完整ExecutionViewIdentity、v3端口/occurrence图、物理详情、观测作用域、URL四元组以及W→B/C的两文件交接。新字段只用于读取/展示，不能重算身份或创建运行权威。C2的逻辑分组仅适用legacy；v3必须精确实例边。

`buildNodeCard(graph,selection,snapshot,execution=null)`增加本次C09结果作为第四参数，由C03传入；source/report为null。`setGraphView({preference,actionId,execution=null})`只更新原State；execution是已校验ExecutionViewIdentity或null，切换后旧节点/图/源码票据失效，原写入回执归属保持。没有新增全局Store。

提供方W补充是已列明的集成要求，当前状态required_not_verified；类型冻结用于各路独立编码，不能据此声称真实W已交付。W候选源码发生变化时核验字节/字段，不复制其易变内部实现。
