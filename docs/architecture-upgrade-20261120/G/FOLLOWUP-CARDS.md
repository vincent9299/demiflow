# 可独立评审的后续任务

只有 G1 达到「调用方明确、现行身份与服务前置已完整、收益可验证」的窄切片标准。它仍只是设计候选；G 的授权不包含实施。优先级低于阻碍实际工作台使用的 B/P12 和已获准修复，不阻塞首批发布。其余 17 边、三处混放、六类对象按保留合同验收，不制造额外迁移卡或等待 F1。

## G1：媒体模型配置校验留在被动 model 层

问题与结果：`inspect_prompt_model(media=True)` 为校验 provider 的静态字段调用 `media.validate_provider`，从而加载 `PIL.Image/PIL._imaging`。固定源实测有这一增量；没有发现该路径发网络请求或启动资源 owner。目标是配置校验不加载媒体解码实现，保留当前全部接纳/拒绝和错误文本。不是优化整包导入或降低环的任务。

精确范围：`demiflow/operator_llm/model.py`、`demiflow/operator_llm/media.py`、`demiflow/operator_llm/parser.py`、覆盖该被动入口的有限测试/说明。validator 当前唯一产品消费者是 model.inspect_prompt_model；parser._inspect_prompt、parse_prompt_model 是上游。追到 parser:151 后发现其还直接从 media 导入 IMAGE_RESPONSE_SCHEMA（当前唯一产品消费者），记作邻接 G-ADJ-01，不能只迁 validator 就宣称真实 parser 已隔离。仓内符号/字符串检索没有第二个 validator 消费者或把它写进 plan/pickle 的证据；这不是对外部任意用户代码的保证。

改前→改后：把 media.py:43 的完整校验函数主体放入已有 model.py 的内部 `_validate_media_provider`；inspect_prompt_model 在原位置调用它。函数所需 Mapping/math/re/urllib.parse 都是标准库；model 已有 Mapping/re，需要显式 math。保持所有分支、默认值、顺序、异常类型/消息，不顺手收紧输入。同批把 media.py:19 的 IMAGE_RESPONSE_SCHEMA 原字典常量放到 model.py，parser:151 改读这唯一原值。删除 media.py 的旧函数/常量定义及 model/parser 的旧 import；不新增模块、不 re-export、不留复制实现。media 的图片开解码、BoundedImageBuffer、options/limits、ObjectStore/RemoteImageStore 路径都保持。

输入输出和边界：原 Mapping 输入；modelhub backend 的 model/api/base_url/api_key_env 与参数禁用集、有限 timeout；diffusers 的 model_path/device/seed。检查不读取环境变量、key、model_path 文件或硬件，原 seed 的 bool/整数区别、API名单和 URL校验保持。成功内部函数返回 None；inspect 返回同一 PromptModel 和空 diagnostics，错误仍为原 PromptContractDiagnostic，parser/parse 继续原聚合/ValueError 规则。

不增加状态或资源：没有 service lease、线程、进程、HTTP、对象库、全局缓存；复杂度仍随当前配置，不引入二次规范化/大额copy。现 operator_llm.__init__ 会先导入 client 并创建 dormant executor 对象，这不在 G1 中改变，不能把 G1 宣称为「整个模型包无运行对象」。媒体真正执行时仍可加载 Pillow 并受原资源 owner 控制；G1 不替代 F2 IMAGE 合同。

身份/历史：不移动 PromptModel/PromptPack/任何 dataclass/Enum 或媒体类；类体、方法 AST、字段、__module__/__qualname__ 不变。三个文件普通字节变化会改变全树 platform identity，以及凡以函数值进入身份的同文件 callable 的 source_hash；不能预先豁免所有 hash/id。被搬的是仓内未进入持久载体的内部校验实现，新增内部名不写入计划。类方法 code/source 身份按当前实际算法核；旧 run/manifest/report 不重签，旧版本/环境保留，新环境不同 revision 仍拒恢复。

实施前后固定证据：

1. 从当时最新已发布 main 建独立树，核 G1 三文件相对本设计的差异；重新扫 validator 的直接/别名/字符串消费者，若发现真实外部公开入口或持久对象引用，将该具体项退回评审，不用永久兼容代理解决。
2. 先保存基线接受/拒绝矩阵、完整诊断字段、IMAGE_RESPONSE_SCHEMA 原字典/解析后值及类 AST/导出名单/可达消费者，再实现唯一转移。比较 modelhub 五类 api、diffusers、未知字段、禁止覆盖参数、凭据 URL、环境名、非正/非有限/bool timeout、缺model/device/path、seed bool、原映射复制行为。JSON tuple/list 与错误先后沿原入口，不自行统一。
3. 在全新子进程安装拒绝导入 `PIL` 和 `demiflow.operator_llm.media` 的 finder，再通过真实 `inspect_prompt_model` 和 `parser.parse_prompt_pack` 分别验证合法/非法 image config。陷阱同时拒绝 Thread.start/Popen/socket.connect。基线此项应因原导入失败；候选应通过。另在普通依赖环境验证真实 media 模块与原图片执行相关测试仍导入/工作，不能删媒体能力使测试变绿。
4. parser 输入至少各一组合法 modelhub/diffusers 和多个独立错误；结果/诊断与基线逐字段等值。`inspect_prompt_model(media=False)` 普通/部署配置不回归。配置确认不读声明的 weights/model_path/API key。原 `required_environment` 仅在实际执行需要时调用，不把声明校验误作凭据准入。
5. 固定候选后检查原类 AST/可公开对象路径、字段与错误类型相同；三文件 changed-source callable 全量列出，未被当前 plans 使用的证据与实际代表计划/媒体声明 identity diff 分开。若出现根差异以外的 code/参数/预算或非可解释派生 digest，失败并修正，不更新旧基准/冻结摘要；S1 新版本拒绝旧版本是预期。
6. 跑受影响纯解析/声明/媒体接口测试与冻结/P4/原P1边界。必要 HTTP fixture 为私有本地可控服务，不运行付费模型/GPU/真实业务。保留原失败/skip，实际候选引用的 23 baseline 和本设计 passive probe 不可当实现成绩。
7. 一次候选包含删除旧函数/更新全部消费者/新负例/身份差异说明。总控依正式规范审代码、合入闸和发版；G1 不直接修改 integration/main/在线预览。

失败/回退：在自己任务树修正；若换版需回退，按隔离反向提交及正常 release 流程，保留已创建记录的生产版本。不能复制旧 validator 留永久双轨，也不能 reset 正式目录或通过重签 run 恢复。当前没有待真人选择的新格式或跨版本承诺；仅需该后续任务按常规设计审阅和范围派发。

## 不形成实施卡的项目

- E03/E10 包初始化：已实测服务包会加载管理模块，但没有资源启动；单搬函数不会移除 package initializer 的加载。没有实际冷加载故障/资源成本需求前保留；不能为构造“纯层”扩大到全 services/pipeline API 改造。
- E05/E06 artifact 编码/保存：原协议/旧 graph/chain 消费者和唯一写入路径明确，迁五行不改变 owner；P8 生命周期退役另有范围，不抢改。
- E04 资源归一：原 SDK/Forge/compiler 使用同一规则，复制 pure helper 无收益；不将 D/P2 不变量任务带入 G。
- E07/E08 位置缓存：原中立服务有固定身份与预算，移动位置不减少 IO，不能新增位置数据库或改只读语义。
- 混放三处/P1六类：当前没有换表示的产品需要，历史代价实在；若未来另提目标，再逐对象附兼容选项，不把待定 S2 当所有任务前置。
