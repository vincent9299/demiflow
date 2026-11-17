# E：Case与持久评测体验

原records/outbox/media权威不改；主要实现呈现和配置连续性。数据schema v1/v2兼容，按server proof区分方案和字段。

所有任务mode=implement（移除/样式/装配按action），不设“只能验证不能修改”的消费者。参考基线267b914，签名新增默认值必须兼容旧调用。先按冻结契约编码，不等其他路实现；同路共享文件由一路负责人合入。

## 文件所有权

- `demiflow/workbench/web/analysis-case.css`
- `demiflow/workbench/web/analysis-case.js`

## E01 · `mount`

- 文件：`demiflow/workbench/web/analysis-case.js`；动作：`modify`。
- 签名：`mount(existingOptions + {toolbarHost?}) -> existingCaseHandle`。
- 契约：C0 C6；调用/接线任务：D01, A05。
- 输入：mount(existingOptions + {toolbarHost?}) -> existingCaseHandle。
- 输出：保持返回API、Store和变更回执所有权；常用视图/配置/保存动作可移入A提供toolbarHost，缺省原位。

保持返回API、Store和变更回执所有权；常用视图/配置/保存动作可移入A提供toolbarHost，缺省原位。释放时只删自己的toolbar节点；三栏/窄屏面板构造唯一，保留beforeunload/flush。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V06, V07, V08；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## E02 · `paintStatus`

- 文件：`demiflow/workbench/web/analysis-case.js`；动作：`modify`。
- 签名：`paintStatus() -> void`。
- 契约：C6；调用/接线任务：E01, E07。
- 输入：paintStatus() -> void。
- 输出：只一行展示视图名、dirty/待核对/不可用等真实状态；完整source摘要在详情，publishContext继续向A报真实source。

只一行展示视图名、dirty/待核对/不可用等真实状态；完整source摘要在详情，publishContext继续向A报真实source。未保存答案数量不能被收起，页面title与固定来源变化同步但不改identity。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V06, V07；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## E03 · `caseTitle`

- 文件：`demiflow/workbench/web/analysis-case.js`；动作：`add`。
- 签名：`caseTitle(descriptor,row,caseKey) -> {title,subtitle}`。
- 契约：C6；调用/接线任务：E05, E06。
- 输入：caseTitle(descriptor,row,caseKey) -> {title,subtitle}。
- 输出：按照C6首个context/text非空值生成<=160字符标题，完整key作subtitle；缺标题退为Case key。

按照C6首个context/text非空值生成<=160字符标题，完整key作subtitle；缺标题退为Case key。不把标题当业务键、不按名称去重、不改target。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V06, V07；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## E04 · `paintSettings`

- 文件：`demiflow/workbench/web/analysis-case.js`；动作：`modify`。
- 签名：`paintSettings() -> void`。
- 契约：C6；调用/接线任务：E01。
- 输入：paintSettings() -> void。
- 输出：原位配置面板分材料/评测/筛选三个tab；同一descriptor draft，保存仍saveView。

原位配置面板分材料/评测/筛选三个tab；同一descriptor draft，保存仍saveView。角色/format/分组编辑有真实字段样例，v1旧配置按原语义打开；v2方案只能依据现有output_groups契约，feature不支持明确不可保存。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V06, V07；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## E05 · `paintList`

- 文件：`demiflow/workbench/web/analysis-case.js`；动作：`add`。
- 签名：`paintList() -> void`。
- 契约：C6；调用/接线任务：E11。
- 输入：paintList() -> void。
- 输出：将原当前页选样列表逻辑抽为命名函数（核对原paintRows/paintContent调用后替换）；使用E03标题、固定Case key和server page/cursor，分组摘要紧凑。

将原当前页选样列表逻辑抽为命名函数（从paintPage抽取，由E11接线）；使用E03标题、固定Case key和server page/cursor，分组摘要紧凑。窄屏筛选默认收起，page点击不自动保存、不猜下一页；选样时原loadReview与media释放保持。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V06, V07；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## E06 · `paintContent`

- 文件：`demiflow/workbench/web/analysis-case.js`；动作：`modify`。
- 签名：`paintContent() -> void`。
- 契约：C6；调用/接线任务：E01, E05。
- 输入：paintContent() -> void。
- 输出：保持现有函数真实行数据/字段映射；material子函数统一传D12 compact/menu。

保持现有函数真实行数据/字段映射；material子函数统一传D12 compact/menu。共享输入/参考与v2每group多材料分离；v1三字段仍字段模式。原group proof/field target/read media不动，E03生成标题。窄屏先材料，展开不用全页modal。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V06, V07；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## E07 · `loadReview`

- 文件：`demiflow/workbench/web/analysis-case.js`；动作：`modify`。
- 签名：`loadReview() -> Promise<void>`。
- 契约：C6；调用/接线任务：E01, E06。
- 输入：loadReview() -> Promise<void>。
- 输出：保持现有heads/watermark/definitions/verification，压缩逐题重复历史按钮到统一详情入口，原答案来源仍可定位。

保持现有heads/watermark/definitions/verification，压缩逐题重复历史按钮到统一详情入口，原答案来源仍可定位。目标scope显著、选项来自真实case/group/field授权，切scope保留草稿；失效/冲突不能显示保存成功。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V07, V08；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## E08 · `saveCurrent`

- 文件：`demiflow/workbench/web/analysis-case.js`；动作：`modify`。
- 签名：`saveCurrent(next) -> Promise<void>`。
- 契约：C6；调用/接线任务：E07。
- 输入：saveCurrent(next) -> Promise<void>。
- 输出：复用原saveEntries/mutation/acknowledge，只改成功后导航与可见反馈；next在全部目标答案确认前不得触发。

复用原saveEntries/mutation/acknowledge，只改成功后导航与可见反馈；next在全部目标答案确认前不得触发。未知响应、媒体变化、409冲突留在原case，保留编辑；不使用onfinally无条件下一条。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V07, V08；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## E09 · `paintFilterControls`

- 文件：`demiflow/workbench/web/analysis-case.js`；动作：`add`。
- 签名：`paintFilterControls() -> void`。
- 契约：C6；调用/接线任务：E11。
- 输入：paintFilterControls() -> void。
- 输出：新增从现有query及review_filter编辑器提取快捷条件/高级条件面板；明确未应用draft，不在客户端做全表结果统计。

新增从现有query及review_filter编辑器提取快捷条件/高级条件面板；明确未应用draft，不在客户端做全表结果统计。数值/enum/null等沿用typed_filter，应用调用原load(reset)，版本/source变化清cursor；窄屏默认收起长编辑器。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V06, V07；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## E10 · `case-style`

- 文件：`demiflow/workbench/web/analysis-case.css`；动作：`style`。
- 签名：`声明式CSS：三栏/工具栏/材料/评测/窄屏原位面板`。
- 契约：C1 C6；调用/接线任务：E01, E04, E06, E07。
- 输入：声明式CSS：三栏/工具栏/材料/评测/窄屏原位面板。
- 输出：按内容容器断点和C1布局；材料优先、按钮与错误可达，评测保存条可sticky但不遮内容；素材contain自适应，重复格式说明收进更多。

按内容容器断点和C1布局；材料优先、按钮与错误可达，评测保存条可sticky但不遮内容；素材contain自适应，重复格式说明收进更多。无权改analysis-render全局行为，用D12契约。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V01, V06, V07；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## E11 · `paintPage`

- 文件：`demiflow/workbench/web/analysis-case.js`；动作：`modify`。
- 签名：`paintPage() -> void`。
- 契约：C6；调用/接线任务：E01, E06。
- 输入：paintPage() -> void。
- 输出：把现有rail/列表/filters主渲染接到E05/E09；保留pageIsCurrent、queryBasis、cursor和不可用来源历史入口。

把现有rail/列表/filters主渲染接到E05/E09；保留pageIsCurrent、queryBasis、cursor和不可用来源历史入口。不留下旧长筛选与新快捷筛选两套独立值。只移动DOM，原load/reset仍一个入口。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V06, V07；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。
