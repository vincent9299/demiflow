# 分析记录候选实现

本模块接续已验收P1-R9，只在独立候选实现。原正式服务、registry和analysis能力保持原值。受信开发模式与原OCI模式共用本记录层；公共真实执行和最终匹配安装仍在验证，OCI的B1执行端及私有持久卷继续需要其独立绑定和校准。本文不是上线或资源放行证明。

当前schema4在既有数据库中增量保存不可变的`analysis_generation_environments`与按原owner/generation/mode验证的`analysis_cleanup_receipts`，不另建执行账本。已知schema1/2/3按原有有界、非覆盖备份流程升级；旧输出、答案、单元和未记录的旧环境不重写、不回填。受信模式停止后仍保留`cleanup_proven=false`，只以对应范围的完整清理收据释放槽位。原OCI布尔清理语义仅在旧OCI记录中保留。

Case描述可带单行、最多256UTF-8字节的可读`name`，沿用原文档版本、CAS和草稿机制。列表只有已命名视图增加name；旧未命名记录形状保持原值。改名不改变方案材料的语义身份，旧答案与其原始题目、媒体依据及来源继续保留；完整视图ID和固定来源在详情中可查。

`analysis.contracts`是Notebook、Python helper和Case界面的唯一描述验证层。原生JSON在调用最终`json.dumps`之前逐项计算准确UTF-8/转义/容器字节，深度16、节点8192、普通请求64KiB；int限有符号64位，row_id另外采用无符号64位十进制字符串。超过编码预算不进入最终encoder。字节上限不等同输入Python对象RSS；预检仅保留最多16层祖先集合，最终编码仍有JSON字符串、UTF-8字节和编码器内部碎片/排序引用等短暂副本，受专用controller的128MiB候选cgroup最终保护。真实RSS与该预算适用性留待B1测试。

Notebook最多32本/项目，每本64个稳定ID单元、单元源16KiB、单元编码48KiB、整本源1MiB。64KiB请求以patch逐次更新；单元页还按56KiB编码预算截取，返回下一offset，不能只按行数超过控制帧。保存和执行分离。nbformat4导入仅取源，不导入输出或内核状态，不覆盖原Notebook。导出SQL单元保留原SQL语言/源，并生成明确依赖受限分析helper的Python调用，不声称外部Jupyter原生理解混合SQL。

`analysis.records`保存不可变文档版本、题目定义、人工事件与head，以及同事务operation receipt。所有写入以当前项目/服务端principal重新授权，事务提交前再检查。新请求的request_id唯一范围为项目+principal；同ID改操作或载荷409，重试返回原回执，不再写或执行。每库最多16384操作回执、8192来源、16384题目定义，达到后明确拒绝并保留事实，不自动淘汰历史。文档历史由DB总量约束。单Case/definition最多100次修订，单批最多16题。

Case持久身份只接受固定版本或已pin来源。服务端可信resolver注册来源且在标注提交前验证精确Case行、字段与已登记方案；浏览器不能自报manifest当授权。读取历史不要求原来源仍存在；写入要求精确原绑定仍有效，不换最新表。题目任何定义变化都有新hash，旧答案保持原解释；未保存视图定义可以随答案保留，保存答案不会保存或更新视图。clear/revert均新增事件，分页以event sequence/watermark读取短事务，不长期持有WAL快照。

v1 Case描述保持无输出组；v2必须显式提供`output_groups:[{id,label,fields:[显示组件ID]}]`，最多8组、32成员引用，成员全局不重复、仅输出/评测角色且每组至少一个输出。原32组件、16真实投影列、16题目与64KiB描述预算保留。未分组输出继续独立显示。输入、参考与上下文是共享材料，不会因为比较输出而重复成第二个Case。

方案身份由服务端规范化`{format:"demiflow_case_group_semantics_v1",source_snapshot,id,fields:排序后的真实源字段集合}`并派生不透明`cg+SHA256`。标签、显示顺序、组件ID和格式不改变身份；组ID、真实成员或固定来源变化产生新身份。原答案键仍是source/Case/variant/field/principal/question：整体Case的variant/field均为空，字段仅field非空，方案仅已登记variant非空。三者沿用同一事件、CAS、head、clear/revert和收据，不增加评测账本或改写旧答案。

已存视图的不可变revision或可信Notebook presentation登记事务，才可在原documents/document_revisions中创建内部`case_output_group` revision1证据；没有任意组登记API。每条证据16KiB、每项目512/全局4096，仍受原DB/WAL保护。后续每次绑定/答案读写及收据重放，都核对规范摘要、固定来源、不可变登记引用和实际组成员。新显式媒体依据只能来自该组真实成员及可信视图/输出中的共享材料；已有basis在clear/revert及省略字段时保留原证明，不按当前展示重写。实际来源/行和媒体当前授权仍在保存事务中重复核对。

只读POST `analysis-case-group-bindings`接受`{digest,case_key,view:{id,revision,descriptor}}`或`{digest,case_key,variant}`二选一。当前视图最多返回8个组，历史variant精确返回一个，响应最多48KiB。未登记组返回registered:false、空variant/target和save_or_register_required，读取不登记、不产生行授权；配置需显式保存后才可评测方案。读者须已从真实case-page取得稳定行键。历史组沿原不可变证据解释，不要求当前视图仍包含旧组。

case-page可带闭合`review:{variant,field,definitions:[当前题目SHA],match,option?}`。match为unanswered/answered_any/answered_all/option；option显式绑定choice/multi题目的定义与选项ID，不从标签猜状态。服务端以当前principal、固定来源、精确目标和watermark查询原事件中每Case/定义的最新状态。只计持久事件，不把本地未保存草稿当已标注。查询最多2,048,000 SQLite VM步、1000个规范uint64键、24KiB内部选择，超限明确拒绝。受限原生reader在字段筛选、分组、排序和分页前应用该集合；全集仍最多1000行/8MiB，较大来源必须显式sample。响应review包含原条件、opaque principal_scope、同一watermark及selection_digest；不返回内部键集。

筛选摘要包含principal与完整条件；cursor固定原cut。异步权限检查每次恢复会重新核验授权，单个metadata请求在Task内只保留一个绑定query digest的watermark，避免等待期间新增标注改变原source RPC。它不是持久快照或第二个账本，也不缓存来源授权。无review的旧请求保持原摘要和分页语义。helper仅在同版实现中广告case_output_groups与case_review_filter，旧helper缺省false；前端v2方案界面与完整产品体验还需同版组合验收。

SQLite只由外层专用controller进程打开和写入，HTTP进程不打开该库，也不改变自身rlimit。`FileSizeGuard.install()`要求显式启动、单线程初始化；硬文件上限128MiB，正常soft上限32MiB。正常SQL写WAL，禁止自动checkpoint。主DB由SQLite max_page_count再限128MiB；序列化checkpoint/启动恢复/显式备份期间才临时把soft升至128MiB，期间不准SQL事务。该机制为每个文件提供OS保护，不以`journal_size_limit`冒充WAL硬限制。预留不足先checkpoint，不能完成则busy/refuse；SQLITE_FULL失败回滚，响应未知查询原receipt。初始化/关闭与checkpoint都由唯一writer flock及进程内锁串行。

候选receiver由同一个单线程事件循环处理有界opaque chunk，避免再加持久进程/线程和常驻副本。一个已预留对象/输入文件的单次写入可进入`guard.object_write()`，soft临时为64MiB（不超过现有hard），结束即恢复32MiB；该scope不能调用SQL、分派事件或执行用户回调，`Database.locked()`会拒绝。它不开放普通SQL的WAL扩额，也不解析Arrow/ZMQ/媒体。这个实现选择及controller128MiB/6pids的实际余量仍需真实B1校准和Review。

`analysis.sessions`与上述记录共用数据库：最多128会话/32项目会话、2048执行/256项目执行；全局1个kernel租约，每代至多2个queued单元。执行只接受已保存Notebook的准确revision/cell/source hash，先提交dispatched再发送代码；丢响应/重启后该执行保持unknown或原事实，不自动重发。停止和取消保留原generation，所有queued单元取消，运行中输出只有已提交且属于本execution的output artifact才可关联为完成。清理未证明时不释放kernel槽；重启必须显式、新generation、不重跑旧单元。单元测试中的清理callback仅证明这些DB栅栏，不证明实际OCI生命周期。

数据库目录仅允许6个固定文件，目录0700、文件0600且归专用controller身份。部署持久盘需另外预留**448MiB数据库区域候选**：主库128、WAL32、旧/新backup各128、元数据/FS余量32；它与2GiB artifacts、暂存/原生scratch等分别计入host预算。每文件rlimit不提供整个卷的总配额或持久性；B1必须落实卷配额/保留并测试fsync、锁、WAL、断电/崩溃、备份恢复和空间满。副本不写用户项目。未知schema先以只读连接拒绝，不打开RW修改header；D8方案定义只复用已有文档表，不新增其迁移。

当前候选已接入来源/有界查询、Notebook输出及媒体登记、权限桥接与独立读取停止生命周期，并有专用SQLite进程、受控原生Lance/PNG及真实浏览器的分层证据。记录层测试不证明外部持久卷、OCI隔离或整个Notebook执行链；运行资源、授权绑定和故障替身均需按各证据范围解释。方案前端与后端的完整组合、真实Python/SQL隔离互用、同源最终部署包、性能和产品验收仍未完成；正式服务与外部B1继续未启用。
