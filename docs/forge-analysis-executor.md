# 受限分析执行端：候选实现与启用条件

本实现延续完整E2意图，不修改已发布P1-R9服务、用户identity业务或当前host-budget v1。当前候选显式支持受信开发与原OCI两种执行模式，复用同一个Controller、数据库、Notebook、broker及Case界面。公共真实组合与最终匹配安装仍在验证；下面的OCI部署、预算和B1说明保留其原适用范围，不能推导受信模式具有同样的隔离或资源保证。

## 受信开发模式与共同接口

`--execution-backend trusted_local`只接受独立的`TrustedBinding`，不构造虚假的OCI Deployment。其私有配置固定解释器、平台完整源码清单、已安装依赖指纹、状态目录、Web回调和服务能力上限；Controller与worker必须使用同一解释器和平台根。注册项目的工作目录由原Web权限回调解析，浏览器不能提交cwd、运行模式、环境或进程身份。默认OCI入口和校准要求保持原值；没有静默回退。

受信模式按当前容器账号启动普通受管进程，不提供项目文件系统/网络隔离、恶意后代收容、RSS/磁盘硬配额或独立强停保证。它采样资源、限制每个任务的时间及临时空间，最多保留32个任务控制项；控制项满时要求显式维护，不能删除Notebook、已提交对象、评审答案或运行历史腾位。停止使用原PID/boot/start、startup FD和服务登记的通道身份；`cleanup_complete`只表示受管进程、已知后代、通道和临时文件清理完成，旧`cleanup_proven`在此模式始终为false。无法证明时保留原槽位。

状态文件暂缺不代表原包装进程死亡：原进程身份与启动单元仍有效时保持运行观察，原进程已消失且缺少退出状态时继续unknown，不释放资源。broker仅可复用先前已核验的同一worker身份，且必须再次验证原包装进程与当前peer存活；每项缓存随有限租约和读取退役释放，不能用旧状态证明清理完成。

新内核代次记录原始代码、解释器、依赖、工作目录、账号、继承文件限制与能力摘要；重启绑定新的代次且不重跑旧单元。当前读取环境与旧输出生产环境分别保存，旧记录不以当前环境回填。执行列表仅携带原环境摘要；明确展开执行依据时，`execution-environment`读取原代次事实，不访问其历史路径。

Notebook 输出、Case 和固定媒体的短读取进程发送完整 END 后，在原 socket 的5秒期限内等待 `demiflow_analysis_read_end_v1` 确认，收到后才退出。控制端仍逐帧核对原进程身份，且只在完整页面/字节范围或合法错误帧已收到时确认；这不是发布或清理成功收据。后续仍核对受管清理、当前权限及原始来源，再向浏览器返回。确认失败不重连或重放读取，worker 与 Controller 须来自同一匹配包。

Controller 启动时只收集原32项以内租约、各1个未清理原生操作/Notebook代次、16个以内媒体捕获及原读取/代次owner。旧代次先记为待清理，旧执行保持未知或取消，不采纳旧内核。已有循环每轮最多清理4项原租约，随后处理固定owner；无进展时按0.2秒至5秒退避，有进展后回到0.1秒。不会反复枚举整份历史或重发执行、SQL、导出、Popen。原范围清理未确认时不允许启动/重启新执行；旧Notebook、评测和环境元数据仍可读。读取和代次owner在unlink后遇到fsync失败，保留本次原身份标记，后续只继续原目录同步；丢失或变化的原租约不能当作清理成功。真实重启及后端退休失败恢复仍须组合验收。

重新打开Notebook后，可在“单元执行记录”中明确选择服务端保存的运行和内核代次，读取原输出；该选择不接管内核或执行旧单元。`notebook-sessions`沿用每项目32个session的原上限，`session-generations`沿用每项目256次执行的原上限并加入当前空代次，单次返回最多56KiB。列表仅在明确展开/刷新时读取，不轮询全部历史。历史选择与当前编辑内核分开，切换时取消旧输出并忽略迟到回应。

已有的metadata、来源、读取和捕获RPC最多16个socket临时加入原selector，以其读/写就绪唤醒已有任务；任务仍按原协议逐次核验，接收/解析、检查次数、deadline、SQL回滚与权限语义不变。不启动额外worker、不忙轮询或缩短空闲100ms定时器；一次等待后移除临时关注，原RPC继续独占响应解析。

受信读取的退休继续使用原backend接口：原进程/临时资源清理完成后，在现有控制目录内持久保存单个pending、原lease/owner/origin及目录身份，完成原身份rename、有界删除、目录同步和terminal收据，才释放read角色和owner。最多32个近期terminal、56KiB journal；`cleanup_complete`不等于control已经退休。忙或IO失败保持原责任，由已有Controller退避继续，不重发读取或执行。新backend可读旧摘要；旧backend不识别新journal，发布或回退都必须保留原state并使用能识别该格式的匹配版本。

目录stat的inode可能占满uint64。私有退休身份保留原有低半区整数；高半区使用19–20位规范十进制文本，比较仍核对完整原始值和目录身份，不截断、不经浮点转换。该表示只用于私有目录身份，公共JSON的有符号整数限制不变。回退版本还须支持这种高位inode表示，否则保留state并拒绝回退。

Python发布表后可显式命名供后续SQL单元使用：

```python
from demiflow.workbench.analysis.client import publish
import pyarrow as pa
numbers = publish(pa.table({'value': [1, 2, 3]})).bind('numbers')
# 下一SQL单元：SELECT SUM(value) FROM numbers
```

`TableHandle.bind(alias, expected=previous)`只更新当前内核代次的SQL名称，原Notebook来源名不能覆盖；更换同名生成表必须给出旧handle作CAS，原来源与生成名称合计最多16个。broker按当前项目、单元、generation和准确对象摘要核验并记录依赖；重启后这些临时名称消失，已保存源码和旧SQL操作/输出依据保持不变。SQL单元的结果变量仍是Python TableHandle，可在后续Python中有界读取。

## 实际模块与调用边界

`analysis.kernel_worker`只在helper创建的kernel namespace/cgroup内启动。其固定`SandboxProvisioner`在每次启动前核对只读root/config/cgroup、父/子namespace区别、非宿主root映射、capabilities全空、no_new_privs、seccomp及本job cgroup控制读回。Jupyter使用本地IPC、固定kernelspec/环境，不发现用户HOME中的配置。裸宿主或缺job manifest直接拒绝，不运行备用子进程。

原始ZMQ/multipart和Jupyter消息解析留在同一1536MiB kernel组；外层128MiB controller仅接有界控制消息和输出引用。这个位置避免用户构造大ZMQ消息挤占数据库/控制器。ZMQ每帧/队列配置是主动收紧，不声称multipart整体RSS靠这些参数绝对受限；内核组与整个analysis组的真实cgroup是最后保护，必须B1实际验收。

SQL单元和Python单元在一本Notebook内。SQL源按准确Python字符串字面量调用同一data broker的SQL helper，可选择普通Python变量名保存返回handle；不是在HTTP里eval，也不把SQL拼成shell。执行源hash、generation、cell/execution ID随命令验证。Notebook导出保留语言/原SQL/结果名metadata，并采用同一显式helper语义。[Jupyter provisioner公开接口](https://jupyter-client.readthedocs.io/en/stable/provisioning.html)用于固定内核生命周期。

当前候选的取消策略是先中断2秒，再结束这个kernel generation；所有变量需要显式新内核恢复，不能显示“已无损中断，继续原变量”。gateway不证明孙进程/挂载清理，外层helper必须验证pidfd结束、cgroup populated=0、scratch和挂载清理后释放槽。输出超限/协议失败/120秒单元期限也使generation停止；idle10分钟、总寿命1小时均有界，不自动重放代码。该具体交互随最终候选供总控Review。

`analysis.client`是Python侧唯一broker helper。只连接固定的每session数据socket，调用携带opaque handle及其generation/digest；不传原Lance URI、宿主路径、pickle或callback。SQL结果新建不可变输出，旧变量/旧输出不被别名更新。上传先在kernel scratch做≤64MiB spool，按完整带长度分帧的数据流hash绑定request_id；不同内容即使schema相同也不得复用同ID成功结果。丢响应查询原receipt，不自动重算Python或SQL。

`analysis.arrow_bridge`每chunk≤1MiB、全局≤100000行/32MiB logical/64MiB encoded/4096chunks。首版保留bool/int64/float64/utf8/binary和null，其他类型明确拒绝。空结果仍传schema。编码前按schema及batch字节估算并拆分；超宽单行拒绝。接收在数组解码前拒绝extension metadata与IPC compression，依据[Arrow Message格式](https://github.com/apache/arrow/blob/main/format/Message.fbs)核对有限元数据，不另造Arrow执行/存储引擎。codec、native decode等第三方峰值仍由所属job cgroup约束，输入大小不是RSS证明。

`analysis.query`复用`DataFusionSession.query(sources={alias:{uri,version}})`。工作进程只接内部固定配置，sources由object ID解析为只读`/sources/<id>/data.lance`的v1；不接URI/version/provider/views/UDF/callback覆盖。使用同一analysis-only native slot目录：1slot/1thread/512MiB RSS、128MiB headroom、256MiB pool、192MiB sampled scratch，外层native leaf1GiB、swap0。原生查询完成后核对结果行数/逻辑字节及文件总量，在session仍有效时传给byte receiver。DDL/DML/statements保护及引擎本体不改，不从pipeline槽借预算。

## 一个对象发布过程

`analysis.artifacts`与Notebook/Case使用同一私有SQLite和receipt，不另建运行账本。新对象先在事务内预留128MiB/128文件系统entries，项目256MiB、全局2GiB/4096entries。来源parent必须是同项目ready且generation/digest相符，已提交对象不因某次kernel停止而删除。

`analysis.object_files`和增量`receivers.ObjectReceiver`不解析Arrow/Jupyter/HTML/codec。controller单线程循环只处理每frame≤1MiB的opaque字节，接收器与SQL互斥的文件rlimit scope见[持久记录说明](forge-analysis-persistence.md)。它只把已授权reservation的最多64个声明文件写到本publish staging；相对路径/目录冲突/总bytes/inodes在写前检查，每chunk先核对剩余额度，校验SHA和大小、fsync、封存。实际物理charge取数据/已分配blocks与文件系统entry余量，不能把逻辑Arrow字节冒充磁盘占用。生产卷仍须落实总量配额、预留及实际证明。

状态是`reserved → receiving → prepared → publishing → ready`。byte receiver seal/rename均明确`database_ready=false`。controller先把准备摘要写入publishing意图，再允许Linux原子RENAME_NOREPLACE，绝不覆盖未知已存在目录。只有文件manifest/rename/fsync、当前权限和原对象身份均验证后才提交DB ready并释放未用预留。取消/崩溃/响应未知保留原ID、staging/目标和charge，进入cleanup_pending；核对所属进程/挂载/文件真的清理后才释放。单元测试的文件缺失证明与mock进程旗标分别标注，不能当真实cgroup证明。

## 唯一共享host预算v2

`forge.host_partition.load`保留v1字节/行为，新增v2的`analysis`字段；原有run/development准入也通过同一个load计入分析，不添加平行host账本。候选结构如下，部署值须管理员在原参与者静默后显式安装，不能把示例当授权：

```json
{
  "format": "demiflow_forge_host_budget_v2",
  "analysis": {
    "memory_bytes": 3221225472,
    "state_root": "<dedicated private persistent volume>",
    "state_disk_bytes": 2952790016,
    "image_roots": [{"path": "<reviewed image and rollback storage>", "bytes": 2147483648}],
    "profile_sha256": "<exact packaged resource-profile-v1.json SHA256>"
  }
}
```

其余v1字段仍必需。所有角色合计3GiB只是候选，不是资源已获批。state最低包含artifacts2GiB、DB区域448MiB、输入256MiB和控制/传输余量64MiB；镜像/回退空间按各自设备另计。state/images与原业务、development及彼此不重叠。每个声明设备累加run history、development、analysis和other，覆盖真实可用内存/磁盘的准入检查；缺设备、预算不足、profile/所有者不符拒绝。v1继续支持既有功能，明确不能准入analysis。本候选没有写入当前正式host-budget文件。

## 固定OCI和镜像

`analysis.oci`只构造固定kernel/native bundle；native已含query和materialize入口，media未完成。命令、rootfs、挂载、环境、UID/GID和资源来自部署者固定配置；public协议不能提交argv/bundle/mount。kernel只见image、scratch/work、job配置和每session的限定broker，native只额外见明确private objects和analysis slots。没有业务根、私有DB、helper socket、个人HOME/SSH/token。仅保留null/zero/urandom等固定虚拟设备，namespace创建/切换/mount/ptrace等syscall不开放给job；clone只准无新namespace标志，clone3返回ENOSYS。具体seccomp及rootless OCI可用性必须实际校准，不凭JSON结构测试宣称隔离成立。

`tools/forge_analysis/build_image.py`已提供有界打包器：amd64 CPython3.11、选择的安装分发及ELF依赖、明确DemiFlow wheel；不在共享env安装/升级，不复制个人startup hooks/.pth/direct_url/目录，不在裸宿主执行内核。image上限2GiB/32768文件，目录/单文件/工具输出亦有限，构建需另外预留rootfs+tar+元数据空间。`P2/image-candidate-01`实际生成了中间wheel、rootfs、tar和60分发精确锁：15503文件/955648309字节；这版早于后续helper生命周期代码，不能充当最终release。`analysis.image_verify`逐文件检查精确内容/模式/所有者，拒绝缺失、额外文件或链接；该实际中间image全树已只读核对，仍没有运行image Python/loader/Jupyter/SQL。最终代码必须另建精确镜像和helper。实际冷启动/峰值/兼容性与隔离仍未验收。

`analysis.deployment`已定义binding v2：管理员site授权与owner release摘要分开；helper UID/GID和Web允许UID必须明确且不同，Web授权回调socket与controller Python摘要不可空。生产还要求绑定该helper/image/profile的十项真实校准收据及Review引用。`inspect_binding`仅报告模板缺项，永远不准入。`runtime`固定create/start分离，先持久租约/原cgroup inode与parent身份，再由独立watchdog确认后启动。`owned_cgroup`只写本nonce叶子，不修改已授权父额度或业务cgroup。`watchdog_service`提供有界Unix监听，按原controller SO_PEERCRED读取固定租约，不能由请求传入PID/cgroup路径或延长deadline；pidfd观察Web/controller死亡，整个原cgroup停止与namespace/file清理证明分开。监督器的watchdog自身死亡/OOM兜底仍为B1部署必要项。

## 剩余实际门槛

### 后续控制器组成（未作实际执行验收）

`analysis.controller`已经组合单线程record store、封闭metadata路由、`Broker`与固定`Runtime`；`analysis.__main__`提供inspect-binding/preflight/controller/watchdog四个入口，不接受shell/任意命令。public socket每次核对允许Web UID和实际boot/start/PID；job socket核对本cgroup及original init，内核代码只见data socket，不能替代gateway/native producer。最多16连接，control/data分别限6/4，其余role1；非阻塞frame和发送队列限制驻留字节，Arrow慢消费者暂停native读取。原生完成先确认对象，再确认进程/namespace/IO清理才返回最终数据结束；unknown不重算。控制存储满时仍尝试停止原cgroup，不因写intent失败跳过kill；原delegation/jobs/leaf inode均须匹配。真实慢磁盘响应、取消独立路径和所有恢复交错仍未完成B1证明。

`analysis.operations`和原记录共库，一次事务提交broker admission、request receipt和artifact/input预留。native总计1个未清理操作，history最多2048/项目256；上传暂存先持有64MiB，完整分帧内容hash匹配才封存。SQL结果、Python publish和pin都走同一可信query/materialize入口；read在native role中导出有界Arrow，outer不解码。pin明确需analysis_write，Case不把ephemeral输出当持久来源。原生writer/reader的父进程RSS并非query子进程512MiB sampler所测范围，二者最后保护都是native1GiB cgroup，待实际校准。

`case_index`在可信native输出发布前扫描实际Lance row_id，最多100000个uint64，排序临时扩张有界；生成≤800056字节的摘要绑定索引。controller仅验证索引SHA/源schema和二分membership，不把0..row_count当真实ID，也不导入Lance。当前私有/外部Case持久身份使用真实row_id；比较可按业务字段分组、每个物理行或输出字段评审。任意额外variant字符串和未证明唯一的字段不作身份授权；需要独立variant/unique-field proof时明确拒绝。

`analysis.authority`回调固定Web authority socket；outer比较其真实peer与原Web的boot/start/PID，发现principal只能来自服务端，权限逐次查当前registry。`web_service`提供有限2线程回调和固定配置，来源读取占用原Web同一metadata worker lane，没有另加native reader池。Web处于内层PID namespace时，外层helper的PID可能显示0；只接受显式映射且与Web不同的专用非root UID，拒绝65534/nobody，不把PID0当有效父进程。outer仍必须看到原Web的真实身份并验证回调是同一个PID。双向UID映射、namespace/proc视图和实际权限撤销/连接失效均待B1验证。候选WorkspaceServer/HTTP/CLI已提供显式`--analysis-config`接线，默认不创建adapter或继承运行权限；已部署正式服务和grant仍未改变。

`sources`只保存Web确认的固定locator或已pin对象；`web_sources`复用P1固定metadata/Case reader，逐项匹配projection/schema/rows/binding，Case存在性只投影_rowid读取。源码、URI、callback或浏览器自报source manifest不能成为授权。完整external表导出/私有materialization准备尚未接线；控制器当前会明确拒绝未准备的external源，不换最新版本。

中间`P2/helper-candidate-02`实际生成纯Python wheel及zipapp，helper SHA为`d0ec933e0628806c1590e6da151bdef9baf0a6ed4ff6dc1e3d880ff458633510`。zipapp从同一wheel逐成员核对生成，profile/seccomp以importlib.resources读取；在普通受限子进程中用`-I -S -B`运行inspect-binding，准确报告旧模板缺项且jobs_started=false。此为可信stdlib元数据入口，不是创建daemon/OCI或运行image Python/Jupyter/native query。worker image01早于这些模块，不是其匹配的最终worker release；最终必须按完成版本重建统一worker/helper、依赖锁、部署/监督器包并Review。

共享`execution` facade改为按需解析原公开对象，以便同一pidfd实现供32MiB watchdog复用时不连带加载pipeline readiness/recovery stack。冷进程普通导入从30220降到23012KiB；公开对象identity和相关process/recovery/开发回归已在精确unit-foundations-05批次覆盖。该数字不是实际cgroup总驻留、文件cache或运行峰值证明。

### 后续控制、HTTP与源单元页面

`authority_rpc.Exchange`使用同一original Web peer/challenge和有界JSON，每轮最多一次非阻塞发送/接收；来源核验/导出可以等待25秒，但该等待不占住controller事件循环。取消关闭原exchange，不重连、不自动再次导出。外部来源经原workspace worker的固定版本/字段投影导出，Web outbox、helper逐frame复制、完整内容承诺和materialize准入接线已在候选中组合；普通自有测试表的真实Arrow导出及原P1数据预览回归属于精确198批次，不能充当新OCI materializer验收。

`deferred_metadata.Task`只适用于不执行进程的封闭metadata路由。缺少远程检查结果时用内部异常展开SQL上下文，回滚并释放mutex；同一请求/CAS依据按精确operation/payload/principal响应序列重新求值，下一项检查发起新RPC。最多32项检查、512KiB响应缓存、60秒；每个RPC仅发送一次。原Records的保存前权限、来源/Case核验、CAS、question定义版本及receipt事务保留；它不重发kernel/native执行。等待期间发生源/Case失效、答案冲突、文档变更或definition变化均须按原身份核对。这项调度变化已获总控继续实施方向确认，组合与最终验收仍未完成。

独立watchdog增加`stop-generation`入口。controller先发布一个固定project/principal/session/generation与父进程owner，lease v3绑定owner摘要；公开请求只能指向该generation，必须重新核对原Web principal及当前execute权限。watchdog先设置内存停止围栏，再停止该owner已有的原cgroup；围栏拒绝后续create/start watch。这个入口明确是立即停止整个内核代次，答复不代表进程、挂载或文件已经清理。普通单元取消仍沿controller的interrupt/两秒后结束代次语义。

为避免控制卷IO阻塞独立停止，watchdog启动恢复之外的状态/父进程/deadline/stop/watch/clean路径不读写持久卷，也不反复读包内profile。可信controller通过仅本角色/peer可访问的private socket传送同一已持久化的封闭lease/owner；不是公开接受PID或cgroup路径。清理watch需同一原delegation/jobs/leaf身份、原lease以及叶子确实不存在；SQL和namespace/file清理证明仍由controller负责。watchdog停止位不另设运行账本；其死亡/重启必须由外层监督器先结束原controller/jobs，未完成这条真实部署证明不能激活。单线程128MiB/6pids和watchdog32MiB/2pids预算没有提高。

精确202局部批次包含真实`Controller.serve`、SQLite和Unix authority的延迟导出测试，使用假runtime/watchdog/cgroup：当export答复不返回时，本地status约0.010秒、取消受理约1.231秒，迟到答复不产生物化/重复导出。该结果不是浏览器、真实cgroup停止或慢卷fsync延迟验收；短生命周期权限RPC、runc和controller文件IO仍可同步等待，CTRL2整体和B1校准尚未关闭。

候选HTTP adapter提供封闭项目metadata接口，两个分析请求槽和一个独立停止槽，不占pipeline停止槽。`--analysis-config`需精确生产helper及明确服务上限；新增分析授权需当前有效helper，配置变更保留其他已配置授权，离线部署只能保留/撤销而不能新增授权。前端源单元页面支持同一Notebook的Python/SQL/Markdown、源hash与保存revision提交、逐单元CAS、本地有界草稿/原请求恢复、源单元导入导出、显式启动/停止及分代次有界执行历史。仅浏览、恢复或导入不会执行代码；此页面的真实输出呈现、配置化Case替换及B1接线仍未交付。现有Case浏览是必要只读能力，不把它标为P3完成；同批完成对应替换时再清除失去消费者的旧路径。

当前仍缺复杂来源/全范围统计与类型化Case查询、注册Notebook输出投影和media、监督器/租约维护与真实组合清理校准、完整Notebook输出/Case持久评审界面、匹配的最终helper/worker镜像与部署包。B1唯一外部binding尚未取得，FFprobe工具与实际媒体验收也未完成。后续在同一候选继续实现与清理，精确Release、真实校准和总控Review后再集成。普通私有目录上的SQLite/atomic rename成功，不代表卷重建持久性、外层资源分配或生产可用。

## 2026-11-06 并行运行路线增量（共同基线中间交付）

本段只描述runtime_deploy路线的增量；共同输入manifest SHA256为`cf7360788eee813b6789ac666931deb2fdfa8df6f13f21960a71aab2b6c7dbfc`，不是最终合并或B1验收。外部binding/部署/校准仍未执行。3GiB候选未增加或获批。

停止顺序改为：核对原delegation/jobs，设置本进程有界stop fence，向原leaf发kill，最后写cleanup_pending。永久阻塞的持久写不再排在kill前面；kill失败仍尝试保留待清理记录，写失败保留原lease与内存围栏，不释放槽。start在放行OCI init前重查watchdog，start/持久确认/后置watch确认失败均停止原属cgroup。JobWatch在create期间leaf暂空时也锁存停止/父死/deadline，迟到进程不能解除停止。恢复仍不重放旧create/start，cleanup_removing_cgroup和原根身份规则保留。

`analysis.supervisor`只管理同版固定controller/watchdog对：在analysis之外的独立预算内启动，持有单实例锁，初始要求专属层级为空，提前捕获固定role membership和analysis cgroup.kill FD，角色入组后才exec。watchdog事件循环通过继承的非阻塞pipe发送心跳，不做持久IO。任一父角色退出或watchdog心跳停止，监督器先终止原专属analysis层级，再有限观察population。它不写运行账本、不宣称挂载/文件清理、不释放数据库预留、不自动重启。外层host manager还必须把监督器和analysis都包含于专属service子树，在监督器自身死亡/OOM时清理整个service；该具体部署和监督器额外预算待管理员提供与B1实际验证。独立角色CLI限制以窄补丁交集成负责人。

media角色增加固定`analysis.media_worker`隔离检查，再调用输出负责人`output_worker`；一项只读`/objects/<opaque id>`、固定job/broker挂载，256MiB/8pids/swap0/5秒租约。缺输出模块时明确不可用，绝不普通宿主执行。controller的已登记witness、output-read通道、当前权限和历史来源接线由原集成负责人统一合入；本路没有复制输出实现或修改其协议。媒体工具依赖采用输出负责人Pillow12.3.0及ffprobe8.1.3，不要求ffmpeg/转码。官方8.1.3完整源码已通过固定发布公钥指纹及离线gpgv验签，精确SHA与私有构建结果见本路证据；隔离行为仍待B1。`build_ffprobe.py`只接受显式已审本地源码和精确hash，`build_image.py --media-lock`只接显式受审工具/许可证，记录ELF闭包，不发现或安装共享PATH工具。

`build_release.py`从同一个wheel构建helper（含supervisor）与worker，并逐包成员比对，记录构建脚本、wheel、package source、helper、worker tree/lock/archive、profile/seccomp摘要。即使生成中间包，也必须从最终合并源码重建再验收，不能拼接旧helper02/image01。`capture_resources.py`是经授权外部环境的只读有界采样工具，记录memory.current/peak/stat/events、pids/threads、RSS/HWM、CPU/swap；它只输出观察，永不自动生成passing校准收据。

本地故障证据使用真实Controller.serve、WatchdogService、SQLite、Unix授权、进程/pidfd/pipe；持久IO阻塞由可控管道注入，cgroup/OCI由明确double模拟。它证明进程间独立停止与身份/迟到围栏，不证明设备卡顿、实际cgroup kill、namespace/mount清理或host预算足够。管理员包逐项要求同版真实执行端完成十类校准、停止多阶段时延、祖先OOM/监督器失败、卷恢复、资源余量和保留历史的回退；缺证据继续disabled/cleanup_pending。
### Parallel runtime R2.1 integration addendum

An independent current media read uses `media-read-owner.json` and a media-only
v4 lease (`owner_kind=media-read`), alongside the existing v3 kernel/native
generation owner. Its current read authority, original Web/controller process
identities, registered artifact and fixed five-second deadline cannot be
borrowed from a historical Notebook generation. Recovery fences the old read;
only full original cleanup and watchdog removal permits clearing its owner.
Read cancellation/revocation does not stop a concurrent kernel/native owner.

Artifact `generation` follows the existing strict integer range1..2^31-1,
distinct from opaque Notebook kernel generations. Boolean, floating-point and
string substitutes are rejected without coercion. The dispatcher accepts the
actual registered witness, including `table_handles`. Cross-lane tests bind
real SQLite registration and the actual output reader to publish/configuration
and Runtime/watch ownership; any frozen test overlay is recorded separately
and is not included in the intermediate runtime release.

Kernel client socket limits apply before connect:8MiB maximum frame,
receive/send HWM4 and bounded send wait. The receive path counts at most16
zero-copy frame views and8MiB total before identity/JSON copies. It closes a
malformed or oversized socket and enters generation-stop handling without
unbounded draining; cancellation and the existing two-second grace remain
responsive. libzmq native buffering and JSON object expansion still require
the real kernel cgroup/RSS calibration. Shared protocol output count/stream
limits and date normalization remain integration-owned.

## Independent Case and pending media reads

The one independent read control accepts exactly `media-read`, `case-read` and
`media-probe`. It never creates a Notebook session or substitutes artifact integer
generation with a kernel generation string. Case reads use the existing native
1024MiB, one-slot,60s job and fixed `case_worker` entry. They mount only the
registered `/objects/<id>` root, validate the complete registered source digest,
and call the existing shared private Case reader. Other native jobs and Case
reads share the same admission. View/cursor/annotation envelopes remain outside
this worker; source/current-read checks apply before admission and return.

Pending media capture uses the existing media256MiB/5s role and requires both
current read and analysis_write. Its closed reservation binds the original
publication ID, seed digest, quota, fixed source and declared content SHA. Only
`.staging/<publish_id>/payload` is mounted readonly at `/pending`; fixed media.bin
must already be frozen0400/single-link/helper-owned and exact-size. The worker
checks the whole SHA in1MiB chunks before Pillow/ffprobe. It returns a descriptor
and at most64 chunk hashes, never ready. Shared Ingress/Artifacts must seal and
atomically publish only after successful probe, complete runtime cleanup and
fresh grants. Existing global/project/entry reservation limits remain in force.

Ready media reads use current read only, one immutable canonical manifest and
fixed payload/media.bin. The worker verifies all manifest/source/descriptor
facts and at most two1MiB chunks for each<=1MiB range. Chunk hashes belong in the
same immutable media manifest; no parallel media store or full64MiB response is
introduced. Source witnesses use the shared registered_media validator, including
resource_binding. Missing shared validators, invalid manifests or unavailable
probe tools refuse explicitly; there is no host decoder fallback. Known worker
failures return an identity-bound error envelope without data or raw host paths.

`Runtime.retire_read(job_id)` is explicit and applies only to proved-clean v4
read controls. Controller order is full cleanup, retirement, exact watchdog
owner clear, then matching durable owner unlink/fsync. Retirement rechecks the
original delegation, absent cgroup leaf, ended startup/init, runtime inventory
and exact watch unregister. It persists one pending identity before staging and
removing only the original control directory, then retains at most32 compact
terminal receipts. Failure or restart retains role charge; unknown/replaced
paths refuse. Generation leases, Notebook/run history and committed artifacts
are never retired by this path, and the32-lease cap is not increased.

Local private fixtures cover65 sequential lifecycles with cleanup/persistence
failure, revoke and restart; real SQLite/Ingress plus small PNG/WAV probes;
private Lance Case reads under both version1 manifest naming layouts; and
synthetic64MiB canonical range IO. Shared test overlays and proposed chunk-schema
patches are hash-declared in the lane evidence. Those tests do not certify the
complete source-export/pending-capture coordinator, actual cgroup/OCI, compressed
AV behavior or resource calibration. Final merged-source integration and B1
remain separate acceptance gates.


## Source33 local startup and one-target legacy WAV maintenance

Local startup uses the existing startup.lock and control pipe. A bounded
startup.json is durable in held before Popen, then released before the sole S
write. The original wrapper installs its subreaper/stop handlers and publishes
wrapper_ready before the controller may release it. Readiness is bounded by
10 seconds and the original owner deadline. It does not hold the final control
lock. Release, stop and controller pipe ownership share 32 fixed lock stripes;
there is no history-sized lock map. Ownership transfer clears the local writer
reference, so stop/exception cannot write or close a reused descriptor. An
unready wrapper receives its original pipe stop/EOF, not default SIGTERM.

Only an unchanged held witness plus the stopped original kernel unit proves
that this new job was never released. Missing/changed/released startup evidence
is not such proof. Worker launch errors additionally require the dedicated
subreaper to verify that it has no children. Existing generations and old
status records are never relabeled as new startup evidence.

The internal tools/forge_analysis/recover_source33_wav.py entry is restricted
to the separately reviewed original Source33 WAV job/read/state and immutable
installation references. Its authorization starts with maintenance_allowed=false
in the prepared draft. A final reviewed authorization SHA is required; this
entry is absent from HTTP and does not stop services or update an environment.

Preparation must run after the old Web/helper have exited, before replacing
the original physical Source33 installed closure. It holds the original
controller.lock and original-inode startup.lock exclusively, verifies the exact
output-media/audio-wav/index0/bytes0-0 closed path and installation-derived
origin, then uses the separately reviewed closed control-domain proof. Known original
controller/Web/wrapper PID/start/boot are read with a32KiB bound; permission or
malformed evidence refuses recovery. Unknown fixed-worker lifetime is covered
only by the reviewed inherited original OFD lock and closed WAV call path.
A same-device/filesystem fork/exec inheritance attestation is required: shared
holders exclude exclusive admission, parent exit does not release a child's
lock, and exclusive succeeds only after all holders exit. The exact mount and
boot are bound; controller/unit FD and path are rechecked under both locks.

This v2 proof does not enumerate global processes/FDs, and never claims such a
scan was complete. Earlier strict global-scan refusals remain forensic history.
Only this reviewed WAV context is admitted, not arbitrary Python or other media.

A single internal legacy-wav-recovery.json (at most16KiB) records the maintenance
proof and independent unchanged failure snapshots. Old code rejects this extra
control name, so an accidental old helper cannot adopt it during handoff. Every
new-helper consumption rechecks original ownership, known lifetimes, source
closure, filesystem attestation and original unit exclusion; there is no global
scan cache. Source33 history is validated from its frozen source/installation
receipt; installing the new package must not rewrite that history. A witness
never creates status.json or startup-held for the old job. New managed_processes
cleanup is a maintenance-time fact, with legacy cleanup_proven=false.

Cleanup, crash-consistent read retirement and the existing OutputReads owner
CAS remain the only release path. The proof is included in the bounded original
retirement intent; helper rebuild after rename/removal and repeated consumption
remain idempotent. The single maintenance witness and independent failure
snapshot remain audit evidence. No worker/cell/SQL/capture or business run is
replayed, no role slots are added, and this protocol does not authorize MP3,
video, native, kernel or arbitrary-code recovery.

The authorization separately pins the initial unknown lease snapshot and the
fresh quiescent lease/optional cleanup hashes. Ordinary shutdown may change the
lease reason; immutable job/context/owner/origin/unit/artifact bindings must
remain equal to the initial snapshot. Preparation snapshots both versions and
any preexisting unknown cleanup before producing new maintenance facts. There
are at most6 snapshot files,each at most64KiB (384KiB total). Final operator
inputs refresh quiescent hashes after shutdown; they never edit the lease or
turn the prepared maintenance_allowed=false draft into an approval by itself.
