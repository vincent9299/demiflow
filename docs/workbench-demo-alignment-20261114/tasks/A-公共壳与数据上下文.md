# A：公共壳与数据上下文

A负责公共装配，不得替B/D/E改模块内部。app.js内表/Case旧只读入口需兼容，新version由F提供，consumer可先依冻结返回编写。

所有任务mode=implement（移除/样式/装配按action），不设“只能验证不能修改”的消费者。参考基线267b914，签名新增默认值必须兼容旧调用。先按冻结契约编码，不等其他路实现；同路共享文件由一路负责人合入。

## 文件所有权

- `demiflow/workbench/web/app.js`
- `demiflow/workbench/web/index.html`
- `demiflow/workbench/web/style.css`
- `demiflow/workbench/web/view.js`

接线修订：另读 [Runtime v3 接线](../05-runtime-v3-wiring.md)（C8）；平台在途源码不由本路覆写，合流按明确交接执行。

## A01 · `updateNavigation`

- 文件：`demiflow/workbench/web/app.js`；动作：`modify`。
- 签名：`updateNavigation() -> void`。
- 契约：C1；调用/接线任务：A02。
- 输入：updateNavigation() -> void。
- 输出：读取当前sidebar展开状态与容器尺寸，改壳class/aria-expanded；不重建子页面。

读取当前sidebar展开状态与容器尺寸，改壳class/aria-expanded；不重建子页面。窄屏关闭侧栏回焦触发按钮，收起后主列真实变宽。保留项目选择和原updateNavigation调用位置。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V01；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## A02 · `openView`

- 文件：`demiflow/workbench/web/app.js`；动作：`modify`。
- 签名：`openView(value, {historyMode="push"}={}) -> Promise<void>`。
- 契约：C0 C3 C4；调用/接线任务：A03。
- 输入：openView(value, {historyMode="push"}={}) -> Promise<void>。
- 输出：沿用ForgeView校验和先canLeave后dispose顺序；登记B/D/E新控制器与读取消；四tab保留，run/attempt与版本URL准确恢复。

沿用ForgeView校验和先canLeave后dispose顺序；登记B/D/E新控制器与读取消；四tab保留，run/attempt与版本URL准确恢复。加载失败从busy退出，当前项目与重试入口仍可操作。历史/浏览器返回不丢Notebook或Case草稿。 生命周期仅新增currentViewHandles有界Set收集A07/D11等DOM资源，在成功canLeave后统一dispose再清DOM；这是清理登记，不保存业务状态。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V01, V08；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## A03 · `switchProject`

- 文件：`demiflow/workbench/web/app.js`；动作：`modify`。
- 签名：`switchProject(id, requested=null, historyMode="push") -> Promise<void>`。
- 契约：C0 C1；调用/接线任务：A01。
- 输入：switchProject(id, requested=null, historyMode="push") -> Promise<void>。
- 输出：切换前询问原控制器canLeave，成功才增加epoch并读取新项目。

切换前询问原控制器canLeave，成功才增加epoch并读取新项目。项目读取失败保留身份、可重试按钮和可切其他项目；不同项目迟到响应不污染。调用A01/A02/A04，不修改项目文件。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V01, V08；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## A04 · `renderLoadFailure`

- 文件：`demiflow/workbench/web/app.js`；动作：`add`。
- 签名：`renderLoadFailure(parent, error, {retryRead,scope,isCurrent}) -> void`。
- 契约：C0；调用/接线任务：A02, A03, A05。
- 输入：renderLoadFailure(parent, error, {retryRead,scope,isCurrent}) -> void。
- 输出：新增局部失败呈现。

新增局部失败呈现。scope为冻结{project,operation,label}，retryRead为只读回调；将会话失效/容量不足/来源变化转成可理解动作，详情保留原code。operationStarted未知写操作只能回到现有回执核对，不提供盲重发。关闭/换页后重试结果不再渲染。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V08；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## A05 · `tables`

- 文件：`demiflow/workbench/web/app.js`；动作：`modify`。
- 签名：`tables(request,onBinding=()=>{}) -> Promise<void>`。
- 契约：C4；调用/接线任务：A02。
- 输入：tables(request,onBinding=()=>{}) -> Promise<void>。
- 输出：保留现有原生/原项目/运行产物读取与ForgeAnalysis.mount装配，合并数据toolbar和mode主动作槽。

保留现有原生/原项目/运行产物读取与ForgeAnalysis.mount装配，合并数据toolbar和mode主动作槽。目录项改紧凑行，字段列表可直接选分布；通过A06选版本，A07渲染分布，A08插入模板。传给D的setSource始终是原locator；不能以Case新来源重标旧sample。读取样例按binding校验，初次未接入分析仍可看固定表。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V02, V05, V06；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## A06 · `selectTableVersion`

- 文件：`demiflow/workbench/web/app.js`；动作：`add`。
- 签名：`selectTableVersion({alias,aliasBinding,version}, ticket) -> Promise<DataSelection|null>`。
- 契约：C4；调用/接线任务：A05。
- 输入：selectTableVersion({alias,aliasBinding,version}, ticket) -> Promise<DataSelection|null>。
- 输出：新内部函数：先读table-versions的selected，再携version/version_binding及原expected_binding读table。

新内部函数：先读table-versions的selected，再携version/version_binding及原expected_binding读table。ticket捕获viewEpoch/project；校验两响应版本/binding/别名一致后一次替换目录上下文、样例读取参数、view链接，返回DataSelection。失败保留旧选择和草稿；不得调用attach。Case/内核是否采用由原显式setSource流程决定。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V02；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## A07 · `catalogProfile`

- 文件：`demiflow/workbench/web/app.js`；动作：`modify`。
- 签名：`catalogProfile(parent,endpoint,parameters) -> FieldHandle`。
- 契约：C4；调用/接线任务：A05。
- 输入：catalogProfile(parent,endpoint,parameters) -> FieldHandle。
- 输出：保留table-profile/project-table-profile边界；改为选中字段的可读条形计数、null/other和coverage。

保留table-profile/project-table-profile边界；改为选中字段的可读条形计数、null/other和coverage。parameters给固定locator查询，response.binding须匹配实际版本。profile不是完整直方图；只有真实enumeration计数可画。dispose取消本组件读取。 返回handle登记到A02当前view清理集合；分布使用独立计数列表DOM，D11图表若使用须适配合法TableSlice，不能把原profile JSON冒充TableSlice。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V02, V05；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## A08 · `insertAnalysisTemplate`

- 文件：`demiflow/workbench/web/app.js`；动作：`add`。
- 签名：`insertAnalysisTemplate({language,sourceAlias,field,filter}, analysisHandle) -> boolean`。
- 契约：C4 C5；调用/接线任务：A05, A07。
- 输入：insertAnalysisTemplate({language,sourceAlias,field,filter}, analysisHandle) -> boolean。
- 输出：新增显式插入模板：language=sql|python，sourceAlias为已登记内核绑定名，field/filter来自有界schema和typed_filter。

新增显式插入模板：language=sql|python，sourceAlias为已登记内核绑定名，field/filter来自有界schema和typed_filter。经D.insertTemplate返回接受/拒绝；SQL标识与字面量转义，拒绝未知操作；不执行、不修改既有单元、不自动启动内核。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V05；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## A09 · `renderCase`

- 文件：`demiflow/workbench/web/app.js`；动作：`modify`。
- 签名：`renderCase(parent,reference,row,display={}) -> void`。
- 契约：C4 C6；调用/接线任务：A05。
- 输入：renderCase(parent,reference,row,display={}) -> void。
- 输出：保留离线/未接入分析的只读Case和业务键比较；不新建第二标注器。

保留离线/未接入分析的只读Case和业务键比较；不新建第二标注器。新增版本引用透传到media/sample等路径：当前row与图片必须同version/binding；新配置Case仍交ForgeCase，legacy入口与旧URL可达。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V02, V08；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## A10 · `validateView`

- 文件：`demiflow/workbench/web/view.js`；动作：`modify`。
- 签名：`validateView(value) -> validatedClone`。
- 契约：C4 C8；调用/接线任务：A02, A06。
- 输入：validateView(value) -> validatedClone。
- 输出：扩C4.2三种v2视图的version/alias_binding成对字段及对应schema；旧v1/v2编码字节不因缺省字段变化。

扩C4.2三种v2视图的version/alias_binding成对字段及对应schema；旧v1/v2编码字节不因缺省字段变化。严格正safe-int/SHA校验，未知字段仍拒绝，encode/decode沿用本验证器。 同时按C2扩v2 run action_id/logical_node_id/occurrence_id依赖约束，旧node_id/report_digest成对规则保留，历史链接不重写。 C8进一步定义run URL的dag_id/plan_id/plan_revision/plan_format四元组全部给或全不写；要求action与attempt，v3新生成实例链接有occurrence。旧URL字节保持，不发未声明HTTP参数。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V02, V04, V08；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## A11 · `common-layout`

- 文件：`demiflow/workbench/web/style.css`；动作：`style`。
- 签名：`声明式CSS：公共壳/数据头/目录/分布，按C1 token和容器宽度`。
- 契约：C1；调用/接线任务：A01, A05。
- 输入：声明式CSS：公共壳/数据头/目录/分布，按C1 token和容器宽度。
- 输出：接管原workbench.css公共workspace/data规则，统一正文/说明字号和间距。

接管原workbench.css公共workspace/data规则，统一正文/说明字号和间距。项目列208/56，data目录收起grid列归零；table/svg才允许局部横滚。所有新规则限制在shell/data作用域，不影响reading/development内部布局。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V01, V06；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## A12 · `asset-assembly`

- 文件：`demiflow/workbench/web/index.html`；动作：`assembly`。
- 签名：`按依赖顺序登记script/link，保留defer及CSP`。
- 契约：C0；调用/接线任务：A02。
- 输入：按依赖顺序登记script/link，保留defer及CSP。
- 输出：加载workbench-presentation.js在workbench.js之前、analysis-charts.js在analysis-output/app之前、workbench-graph.css在通用style后。

加载workbench-presentation.js在workbench.js之前、analysis-charts.js在analysis-output/app之前、workbench-graph.css在通用style后。不给内联脚本/unsafe-eval授权；新文件须被原打包规则覆盖，测试直接检查构建资源可达。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V01, V09；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## A13 · `showCase`

- 文件：`demiflow/workbench/web/app.js`；动作：`modify`。
- 签名：`tables内部 showCase(rowId,shownColumns,route=false) -> Promise<void>`。
- 契约：C4；调用/接线任务：A05, A09。
- 输入：tables内部 showCase(rowId,shownColumns,route=false) -> Promise<void>。
- 输出：sample/detail/比较/返回表URL都携原version与alias_binding；Case row_id作为字符串保留，版本变化重置选择。

sample/detail/比较/返回表URL都携原version与alias_binding；Case row_id作为字符串保留，版本变化重置选择。旧未接入分析只读入口沿用，不以当前表binding读取历史字段。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V02, V08；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## A14 · `load`

- 文件：`demiflow/workbench/web/app.js`；动作：`modify`。
- 签名：`tables内部 load() -> Promise<void>`。
- 契约：C4；调用/接线任务：A05, A06。
- 输入：tables内部 load() -> Promise<void>。
- 输出：样例读取仅当前固定version/query scope，offset/page size沿用原界限；期望binding是所选version_binding，alias校验独立透传；失败保留旧页并标来源，迟到旧页不覆盖。

样例读取仅当前固定version/query scope，offset/page size沿用原界限；期望binding是所选version_binding，alias校验独立透传；失败保留旧页并标来源，迟到旧页不覆盖。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V02, V08；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。
