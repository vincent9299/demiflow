# 迁移、清理与历史保护

## 保留并复用

ResourceManager及provider契约、WorkerRuntime状态机、原生RunJournal/CommitCoordinator、source/sink/effect身份、原队列acquisition/finalize协议、Arrow/DataFusion受限计算、现有模型与工具实现、工作台manager与身份桥仍是权威。v3只补完整图/调度/来源/宿主接线；不迁一份同名journal，不建立新运行中心，不让Forge import到底层。

七个冻结文件的路径及sha256见 `evidence/frozen-files.json`，任何lane不得改。旧plan/graph适配在新模块完成，不悄悄改dag.py的v1身份。遇到具体反例证明冻结接口无法承载时，交总控一次裁决最小改动，其他lane不用等；这不是默认授权解冻。

## 替换与退出

| 范围 | 新路径 | 旧路径处理 | owner |
|---|---|---|---|
| Dataset有效算子、终端 | 纯声明/ActionRequest→v3 | 删除由后端类型造成的前置能力拒绝；保留真实业务安全拒绝 | U |
| LocalDatasetExecutor | provider+返回值适配 | 明确删除_apply_plan/_execute_plan/_bind_plan/_execution_capabilities/_write_file_body/_require_ray_backend，不保留专属执行循环 | L |
| RayDatasetExecutor | Ray Core宿主、同一worker | 删除_compile/_build_source及Ray datasource/datasink/private lowering，清空ray.data生产调用 | Y |
| Dataset 物化、带路由的图与原生队列写出 | Dataset 的 P1 和 Local/Ray 的 materialize_graph 各自绑定一个 v3 action | P8 已退役独立的 chain、queue_chain 及旧签名的图包装。队列事实与原生确认仍由同一队列协议负责。 | F |
| 原生队列/RawFetch | driver有限服务+worker计算 | 冻结实现不改；不能作为泛化driver UDF fallback | Q |
| streaming/model/kernel | 同一worker协议 | 复用有消费者的计算/生命周期组件；没有入口后清理旧专属路由，不删历史读取 | B/K/M |
| 工作台 | 原生封存v3图/物理布局投影 | 静态预览保留但如实标明；新执行认证不可来自重算descriptor | W |

每张retire卡负责删除一个明确旧符号。文件owner清理相应imports/constants/未使用分支；仍有真实消费者的旧内部符号必须列出保留依据。代码搜索ray.data为零之外，还要在真实公共入口阻止导入的进程运行；字符串扫描不足以证明默认路径切换。

## 兼容差异必须显式呈现

- data.xxx与Dataset都保留，前者是来源入口、后者是数据对象；本批不进行破坏性表面改名。
- 已退役checkpoint/checkpoint_async/checkpoint_lance/checkpoint_lance_async保持明确拒绝，不重新实现旧协议。
- Ray Data内部调参对象不能继续控制执行；有业务含义的资源/并发/分区选项映射到统一契约，未知项明确给替代说明，不吞参数。
- 共享MaterializedDataset.handle被release后，其派生视图按旧契约失效；历史结果和其他独立owner的pin不被删除。
- queue自动结算保留原拓扑限制；公开普通模型图并不因这条限制失去join/异步/排序能力。
- 历史prompt identity和enum>1024原公式保持。private journal写恢复与新原生统一权威冲突时在预检明确拒绝并保留原环境历史读取；不把该兼容限制宣传为所有旧选项无差异。此项须在最终用户迁移说明中单列。

## 历史结果与恢复

v1/v2已完成结果只读不运行UDF。兼容adapter逐字段核旧code/source/operator/effect/sink状态，保持原action与commit；不把旧pickle重新dump后当原artifact。未知格式/缺程序/不可达域/不可恢复资源给INCOMPATIBLE_HISTORY和原run引用，不能复制一个新run冒充恢复。新v3目录、schema、身份域与旧版本隔离，旧内容不回写升级。

所有引用的旧证据只读保留，包括v2任务包、完整记忆、独立全局架构及Qwen报告。本包不重写它们来获得一致。

## 不影响在线环境

实施只在各lane隔离工作树和最后integration树，主仓内容不变。禁止push、合主仓、共享env安装、长训/工作台服务重启、真实付费模型、生产queue/表操作。演示用完整独立workspace与/tmp数据，由真实API驱动；实际上线仍是后续单独动作。
