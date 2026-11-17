# B：管道完整用户链路

保留Commands/Observer/State权威；主视图不再让用户手动理解E2。B owns workbench.js整个文件，函数卡不授权修改C文件。

所有任务mode=implement（移除/样式/装配按action），不设“只能验证不能修改”的消费者。参考基线267b914，签名新增默认值必须兼容旧调用。先按冻结契约编码，不等其他路实现；同路共享文件由一路负责人合入。

## 文件所有权

- `demiflow/workbench/web/workbench-state.js`
- `demiflow/workbench/web/workbench.css`
- `demiflow/workbench/web/workbench.js`

接线修订：另读 [Runtime v3 接线](../05-runtime-v3-wiring.md)（C8）；平台在途源码不由本路覆写，合流按明确交接执行。

## B01 · `mount`

- 文件：`demiflow/workbench/web/workbench.js`；动作：`modify`。
- 签名：`mount(options) -> Promise<existingWorkbenchHandle>`。
- 契约：C0 C2 C3 C8；调用/接线任务：A02。
- 输入：mount(options) -> Promise<existingWorkbenchHandle>。
- 输出：沿用options和原返回API；重排toolbar、主图、inspector、source与run/results槽位。

沿用options和原返回API；重排toolbar、主图、inspector、source与run/results槽位。只构造一个GraphController，加入action选择器和高级诊断入口；按钮调用下列同一控制器方法。完整dispose保留observer/源码阅读器清理。 消费C2的run深链接字段，读取封存图后恢复指定action/成员；未知引用明确错误，不回首个action冒充定位成功。 合流时承接W018–W020的v3语义，按C8恢复旧/新URL并核对真实计划，B/C两处文件交接见05；只维护一张主图。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V03, V04, V08；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## B02 · `fetchGraph`

- 文件：`demiflow/workbench/web/workbench.js`；动作：`modify`。
- 签名：`fetchGraph(ticket,{preserveSelection=false,refresh=false}={}) -> Promise<void>`。
- 契约：C2 C8；调用/接线任务：B01, B07。
- 输入：fetchGraph(ticket,{preserveSelection=false,refresh=false}={}) -> Promise<void>。
- 输出：接受state.acceptGraph后调用C01/C03和graph.update。

接受state.acceptGraph后调用C01/C03和graph.update。封存主视角按action保留；partial/空/source-only明确且提供检查入口。保持existing node邻域/分页，不自动拉全图、不调用编译器。状态/桥变化触发派生模型更新，普通metrics轮询不重排。 C调用通过B14统一封装；不保留第二个builder入口。 v3与legacy交C09唯一适配；graph binding相同但action/plan/revision变化也须更新选择与读取票据。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V03, V04；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## B03 · `selectGraphNode`

- 文件：`demiflow/workbench/web/workbench.js`；动作：`add`。
- 签名：`selectGraphNode(selection:NodeSelection) -> Promise<void>`。
- 契约：C2 C8；调用/接线任务：B01, B04。
- 输入：selectGraphNode(selection:NodeSelection) -> Promise<void>。
- 输出：新统一入口替代两个相互改选择的处理链。

新统一入口替代两个相互改选择的处理链。校验当前GraphModel内key；有reportNodeId才调用state.chooseNode/loadSource，封存occurrence准确透传；不在当前图时定位邻域后按原identity重选。无桥只显示真实封存节点详情，不借别人的源码。 selection可为组或其显式成员；组内多occurrence先展示成员picker，未选成员前不调用graph-source、不用第一条bridge。 rememberView写C2完整action/logical/occurrence引用；读回按封存模型重新核验，不仅保存报告node_id。 NodeSelection新增execution；v3点选是精确occurrence，legacy组规则不变。rememberView按C8写完整dag/plan/revision/format；未知或过期定位不回首节点。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V03, V04；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## B04 · `paintIssues`

- 文件：`demiflow/workbench/web/workbench.js`；动作：`modify`。
- 签名：`paintIssues() -> void`。
- 契约：C3；调用/接线任务：B01, B02。
- 输入：paintIssues() -> void。
- 输出：问题列表保留诊断cause/rule/path且主文优先detail；按真实related_nodes调用B03，缺关联走文件/整图。

问题列表保留诊断cause/rule/path且主文优先detail；按真实related_nodes调用B03，缺关联走文件/整图。checks模式保留图和可编辑inspector，不以hidden切断定位；未保存/过期/未覆盖明确。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V03；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## B05 · `paintConfiguration`

- 文件：`demiflow/workbench/web/workbench.js`；动作：`modify`。
- 签名：`paintConfiguration(node:RawReportNode|null) -> void`。
- 契约：C2 C3 C8；调用/接线任务：B03, B04。
- 输入：paintConfiguration(node:RawReportNode|null) -> void。
- 输出：用C04展示输入输出/参数来由；可写字段仍affects/parse/CAS原规则。

用C04展示输入输出/参数来由；可写字段仍affects/parse/CAS原规则。无修改隐藏保存，readonly显示原因。run配置仅展示sealed值；不读当前params覆盖历史；原共享参数影响数保留。 通过C04在只读运行详情展示真实resource role/profile、exchange原因和恢复限制，计划配额不得当作观测，不能放进可编辑参数列表。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V03, V04；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## B06 · `loadSource`

- 文件：`demiflow/workbench/web/workbench.js`；动作：`modify`。
- 签名：`loadSource() -> Promise<void>`。
- 契约：C2 C8；调用/接线任务：B03, B05。
- 输入：loadSource() -> Promise<void>。
- 输出：保持graph-source查询与严格graph_binding/node_id验证；新选择带明确action所对应report node及logical/occurrence。

保持graph-source查询与严格graph_binding/node_id验证；新选择带明确action所对应report node及logical/occurrence。异步旧source丢弃，未知/缺封存不得取当前实现。Prompt仅有真实材料时可用，无关节点不把空Prompt页作为默认选项。 请求HTTP字段不扩；捕获execution与detail票据，返回时必须仍属同action/dag/plan/revision/occurrence；选中action的bridge才授权源码。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V03, V04, V08；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## B07 · `submit`

- 文件：`demiflow/workbench/web/workbench.js`；动作：`modify`。
- 签名：`submit() -> Promise<void>`。
- 契约：C3；调用/接线任务：B01。
- 输入：submit() -> Promise<void>。
- 输出：原commands.identity/prepare/start/inspect串接不变；可见阶段提示合并一个主按钮。

原commands.identity/prepare/start/inspect串接不变；可见阶段提示合并一个主按钮。保存/报告过期先引导检查；点击捕获原身份，未知回执保留pending，用户已切走不抢当前页面。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V03, V08；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## B08 · `paintRunBar`

- 文件：`demiflow/workbench/web/workbench.js`；动作：`modify`。
- 签名：`paintRunBar() -> void`。
- 契约：C3 C8；调用/接线任务：B01, B02。
- 输入：paintRunBar() -> void。
- 输出：运行/attempt/动作、当前与历史清楚分离；stop/resume捕获exact目标并保留权限及恢复限制；返回当前配置恢复原草稿，默认action变化不改run。

运行/attempt/动作、当前与历史清楚分离；stop/resume捕获exact目标并保留权限及恢复限制；返回当前配置恢复原草稿，默认action变化不改run。高级dag/plan id放详情。 v3 physical.recovery仅是计划声明，实际resume仍由manager能力判定；INCOMPATIBLE_HISTORY/UNKNOWN_EFFECT/CAPACITY_PENDING如实说明，不自动重新提交。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V04, V08；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## B09 · `paintResults`

- 文件：`demiflow/workbench/web/workbench.js`；动作：`modify`。
- 签名：`paintResults() -> void`。
- 契约：C3 C4 C8；调用/接线任务：B01, B08。
- 输入：paintResults() -> void。
- 输出：按真实output状态显示固定版本/行数与打开数据、比较入口；未登记/部分提交/失败不合并为成功。

按真实output状态显示固定版本/行数与打开数据、比较入口；未登记/部分提交/失败不合并为成功。原openResult/run_compare契约保留，打开结果到A05且有具体binding。 平台内部ArtifactRef/ResultHandle不直接转浏览器URL；只消费原run-view已确认且有权限的固定结果登记，交A/D/E原分析链。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V04, V05；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## B10 · `loadRunDetail`

- 文件：`demiflow/workbench/web/workbench.js`；动作：`modify`。
- 签名：`loadRunDetail(force=false,resetRetry=false) -> Promise<void>`。
- 契约：C3 C8；调用/接线任务：B01, B08。
- 输入：loadRunDetail(force=false,resetRetry=false) -> Promise<void>。
- 输出：统一详情tab文案与所选run/attempt；run日志不假node过滤。

统一详情tab文案与所选run/attempt；run日志不假node过滤。保留log offset/events cursor/observation分页；折叠不主动读全文，切run清旧响应；缺测单独说明，预算及4096等原上限不扩。 v3节点指标必须具有C8完整作用域凭据；无plan身份的旧页不提升为当前v3指标，run日志仍按原run/attempt分页。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V04, V08；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## B11 · `create`

- 文件：`demiflow/workbench/web/workbench-state.js`；动作：`modify`。
- 签名：`create(project) -> existingStateController`。
- 契约：C0 C2 C8；调用/接线任务：B01, B03。
- 输入：create(project) -> existingStateController。
- 输出：仅在现有state内增加graphPreference=auto|report、actionId和完整NodeSelection；transition重置相应选择；capture/valid/acceptGraph语义保持。

仅在现有state内增加graphPreference=auto|report、actionId和完整NodeSelection；transition重置相应选择；capture/valid/acceptGraph语义保持。禁止另外持久化GraphModel或新全局Store；旧draft格式不变。 新增setGraphView({preference,actionId,execution=null})方法，只在原state更新选择并返回capture ticket；不增加持久存储。 在现有state增加ExecutionViewIdentity与setGraphView签名C8；只使图/源码/节点观测读取票据随action/dag/plan/revision失效。运行级stop/日志及原写入pending scope保留，不因换节点丢提交回执。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V03, V04, V08；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## B12 · `pipeline-layout`

- 文件：`demiflow/workbench/web/workbench.css`；动作：`style`。
- 签名：`声明式CSS：管道容器/检查列表/inspector/代码/运行结果`。
- 契约：C1；调用/接线任务：B01。
- 输入：声明式CSS：管道容器/检查列表/inspector/代码/运行结果。
- 输出：按C1图中心结构；删除迁至A/C的公共/图样式，保留既有媒体查询的能力但不叠加永久旧类。

按C1图中心结构；删除迁至A/C的公共/图样式，保留既有媒体查询的能力但不叠加永久旧类。errors/partial/pending可见，配置区域不会因source展开推失图。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V01, V03, V04；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## B13 · `chooseNode`

- 文件：`demiflow/workbench/web/workbench.js`；动作：`modify`。
- 签名：`chooseNode(id,selection=null) -> Promise<void>`。
- 契约：C2；调用/接线任务：B02, B04。
- 输入：chooseNode(id,selection=null) -> Promise<void>。
- 输出：保留旧定位/历史URL入口的薄委托：在当前GraphModel找报告node对应完整NodeSelection，已给occurrence须严格匹配；多个候选需显式选择，不能取第一项。

保留旧定位/历史URL入口的薄委托：在当前GraphModel找报告node对应完整NodeSelection，已给occurrence须严格匹配；多个候选需显式选择，不能取第一项。最终只调用B03，不保留旧graphView.select(reportId)/logicalView.select双写。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V03, V04；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## B14 · `paintGraph`

- 文件：`demiflow/workbench/web/workbench.js`；动作：`add`。
- 签名：`paintGraph() -> GraphModel|null`。
- 契约：C2 C8；调用/接线任务：B01, B02, B15。
- 输入：paintGraph() -> GraphModel|null。
- 输出：统一纯呈现调用入口：从当前state.graph/action/preference/snapshot调C01/C03后graph.update并保留选中key；无graph返回null。

统一纯呈现调用入口：从当前state.graph/action/preference/snapshot调C01/C03后graph.update并保留选中key；无graph返回null。B02成功读取及B01 observer.onSnapshot都调用这里，包括unsupported/unchanged分支；不再调用已删除的graphView.paint。模型结构没变不重排。 C03内唯一调用C09；结构版本/plan变更重建精确映射，metrics变化不重排。将W018/W019的真实语义迁入此链，禁止另开v3画布。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V03, V04, V08；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## B15 · `toggleGraphKind`

- 文件：`demiflow/workbench/web/workbench.js`；动作：`modify`。
- 签名：`toggleGraphKind() -> void`。
- 契约：C2；调用/接线任务：B01。
- 输入：toggleGraphKind() -> void。
- 输出：原E2开关改为明确当前主视角/当时检查图切换，更新B11同一个state中的preference并调用B14。

原E2开关改为明确当前主视角/当时检查图切换，更新B11同一个state中的preference并调用B14。无run时不显示执行计划开关；action选择仍B01显式处理，不重建第二画布。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V03, V04；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## B16 · `renderLogical`

- 文件：`demiflow/workbench/web/workbench.js`；动作：`remove`。
- 签名：`移除renderLogical(graph)，由paintGraph单一路线替代`。
- 契约：C2 C8；调用/接线任务：B14。
- 输入：移除renderLogical(graph)，由paintGraph单一路线替代。
- 输出：删除旧logicalView.update及仅被它使用的logicalName/logicalPanel等私有helper；仍有高级事实消费者的logicalEntry/桥helper先迁到C或保持单份再删除。

删除旧logicalView.update及仅被它使用的logicalName/logicalPanel等私有helper；仍有高级事实消费者的logicalEntry/桥helper先迁到C或保持单份再删除。全文件rg校验无悬空logicalView，不能保留隐藏第二执行UI。 迁入W019的v3物理详情/恢复限制后删除renderLogical；替代消费者C03/C04/B14。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V09；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## B17 · `chooseLogicalNode`

- 文件：`demiflow/workbench/web/workbench.js`；动作：`remove`。
- 签名：`移除chooseLogicalNode(id,declared)，统一NodeSelection`。
- 契约：C2 C8；调用/接线任务：B03, B13。
- 输入：移除chooseLogicalNode(id,declared)，统一NodeSelection。
- 输出：源码/参数/观测选择迁B03、C04；删除旧occurrence按钮写logicalSelected逻辑，新的occurrence直接NodeSelection key。

源码/参数/观测选择迁B03、C04；删除旧occurrence按钮写logicalSelected逻辑，新的occurrence直接NodeSelection key。原chooseNode兼容委托仅B13，去除旧observer/handle.dispose对logicalView调用。 迁入W020的精确实例及历史定位后删除chooseLogicalNode；唯一入口B03/B13。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V04, V09；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## B18 · `logicalEntry`

- 文件：`demiflow/workbench/web/workbench.js`；动作：`remove`。
- 签名：`移除旧logicalEntry选择入口；选择统一由C01/C09/B14处理`。
- 契约：C2 C8；调用/接线任务：B01, B14, B16, B17。
- 输入：W018交付行为及C8唯一版本适配/选择合同。
- 输出：旧helper及全部消费者删除，W的v3能力保留在新主图。

W018先完成其隔离交付；B合流时把按action选择及v3字段处理迁到C01/C09/B14，再移除logicalEntry。旧URL/显式错误保持；禁止两套选择状态同时存活。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V04, V09；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。
