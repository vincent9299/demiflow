# D：Notebook与通用输出

保存/启动/执行整合在已有控制器，不另建执行服务。保持Case共用field接口；不能为了紧凑移除失败/权限/来源信息。

所有任务mode=implement（移除/样式/装配按action），不设“只能验证不能修改”的消费者。参考基线267b914，签名新增默认值必须兼容旧调用。先按冻结契约编码，不等其他路实现；同路共享文件由一路负责人合入。

## 文件所有权

- `demiflow/workbench/web/analysis-charts.js`
- `demiflow/workbench/web/analysis-output.css`
- `demiflow/workbench/web/analysis-output.js`
- `demiflow/workbench/web/analysis-render.js`
- `demiflow/workbench/web/analysis.css`
- `demiflow/workbench/web/analysis.js`

## D01 · `mount`

- 文件：`demiflow/workbench/web/analysis.js`；动作：`modify`。
- 签名：`mount(existingOptions) -> existingAnalysisHandle + insertTemplate`。
- 契约：C0 C5；调用/接线任务：A05。
- 输入：mount(existingOptions) -> existingAnalysisHandle + insertTemplate。
- 输出：保持已有setSource/setMode/canLeave/dispose等返回；加入insertTemplate供A，模式主动作移至A传入可选toolbarHost（缺省使用原bar）。

保持已有setSource/setMode/canLeave/dispose等返回；加入insertTemplate供A，模式主动作移至A传入可选toolbarHost（缺省使用原bar）。挂载与dispose同一Notebook Store，不因Case切换新建内核；既有Case.mount调用保留。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V05, V06, V08；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## D02 · `requestCellRun`

- 文件：`demiflow/workbench/web/analysis.js`；动作：`add`。
- 签名：`requestCellRun(cellId,{advance=false}={}) -> Promise<CellRunOutcome>`。
- 契约：C5；调用/接线任务：D05。
- 输入：requestCellRun(cellId,{advance=false}={}) -> Promise<CellRunOutcome>。
- 输出：严格C5保存→必要启动→提交；捕获代码与绑定，全部网络复用mutate/acknowledge和原poll。

严格C5保存→必要启动→提交；捕获代码与绑定，全部网络复用mutate/acknowledge和原poll。readonly/conflict/变更generation/未知回执分别blocked/pending，绝不盲重发。同一cell仅一链，先取消/清理再释放锁。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V05, V08；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## D03 · `save`

- 文件：`demiflow/workbench/web/analysis.js`；动作：`modify`。
- 签名：`save(onlyCell=null) -> Promise<SaveOutcome>`。
- 契约：C5；调用/接线任务：D02, D01。
- 输入：save(onlyCell=null) -> Promise<SaveOutcome>。
- 输出：在原实现上返回{kind:saved|pending|conflict,notebookId,revision}供D02；原按钮可忽略返回值。

在原实现上返回{kind:saved|pending|conflict,notebookId,revision}供D02；原按钮可忽略返回值。revision来自acknowledge，未确认不能自增；草稿变更期间保留后续编辑，不能覆盖执行捕获内容。旧outbox格式不改。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V05, V08；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## D04 · `ensureSessionForRun`

- 文件：`demiflow/workbench/web/analysis.js`；动作：`add`。
- 签名：`ensureSessionForRun(capture) -> Promise<SessionOutcome>`。
- 契约：C5；调用/接线任务：D02。
- 输入：ensureSessionForRun(capture) -> Promise<SessionOutcome>。
- 输出：capture含project/epoch/notebookId/revision/sources/cellSha；已有generation与sources匹配则复用。

capture含project/epoch/notebookId/revision/sources/cellSha；已有generation与sources匹配则复用。无活内核用原session-start，限时读取原session状态；超时pending保留request_id。sources不匹配blocked给显式选择，不自动终止活内核。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V05, V08；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## D05 · `paintCells`

- 文件：`demiflow/workbench/web/analysis.js`；动作：`modify`。
- 签名：`paintCells() -> void`。
- 契约：C5；调用/接线任务：D01, D06。
- 输入：paintCells() -> void。
- 输出：按cell.id复用编辑article/textarea与selection；按钮与Ctrl/Cmd+Enter、Shift+Enter调D02；poll只更新执行槽，删除cell才dispose该输出。

按cell.id复用编辑article/textarea与selection；按钮与Ctrl/Cmd+Enter、Shift+Enter调D02；poll只更新执行槽，删除cell才dispose该输出。历史输出来自不同source_sha时标旧代码而不丢失。保存/运行主操作简化，更多含删除/详细身份。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V05, V08；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## D06 · `paintKernel`

- 文件：`demiflow/workbench/web/analysis.js`；动作：`modify`。
- 签名：`paintKernel() -> void`。
- 契约：C5；调用/接线任务：D01, D04。
- 输入：paintKernel() -> void。
- 输出：一行展示可运行/准备/执行/停止与明确原因，环境摘要进详情；停止、取消、重启仍显示作用范围。

一行展示可运行/准备/执行/停止与明确原因，环境摘要进详情；停止、取消、重启仍显示作用范围。原session/generation权限不变，按钮由D04和原control路由使用，不构造自己的kernel状态。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V05, V08；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## D07 · `insertTemplate`

- 文件：`demiflow/workbench/web/analysis.js`；动作：`add`。
- 签名：`insertTemplate({language,source,title}) -> boolean`。
- 契约：C5；调用/接线任务：A08。
- 输入：insertTemplate({language,source,title}) -> boolean。
- 输出：新增返回handle方法；language仅python/sql，source<=16384且插入后cells<=64。

新增返回handle方法；language仅python/sql，source<=16384且插入后cells<=64。生成新cell.id添加草稿并flush，返回成功；无空间/不能安全暂存返回false并保留原稿；不运行、不覆盖当前单元。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V05；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## D08 · `mount`

- 文件：`demiflow/workbench/web/analysis-output.js`；动作：`modify`。
- 签名：`mount(existingOptions + {api?}) -> FieldHandle`。
- 契约：C5 C6；调用/接线任务：D09。
- 输入：mount(existingOptions + {api?}) -> FieldHandle。
- 输出：默认展示最佳实际可用representation，人可读格式名称；原provenance摘要一行，完整固定依据收起。

默认展示最佳实际可用representation，人可读格式名称；原provenance摘要一行，完整固定依据收起。仅有界含rows的table表示可调D10/D11，table handle仍onTable。onCase必须携原definition_witness/origin和artifact，不能弱化caseEvidence。 table handle若用户选择表格/图表，调用D15真实读取；原handle跳转保留，禁止仅写一个永远没有rows输入的图表组件。 D09向本mount显式传api；旧独立调用缺api时不启用内联取表，只保留原onTable。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V05, V06；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## D09 · `mountExecution`

- 文件：`demiflow/workbench/web/analysis-output.js`；动作：`modify`。
- 签名：`mountExecution(existingOptions) -> FieldHandle`。
- 契约：C5；调用/接线任务：D05。
- 输入：mountExecution(existingOptions) -> FieldHandle。
- 输出：维持同execution/output/generation/binding校验与分页<=8；explicit运行输出内联展开，恢复旧输出默认按需。

维持同execution/output/generation/binding校验与分页<=8；explicit运行输出内联展开，恢复旧输出默认按需。开关/换页释放D08旧handle，过期fetch结果不更新；来源/输出不匹配显示错误不换成最新。 调用D08时显式传现有api，不另建fetch。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V05, V08；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## D10 · `validateChart`

- 文件：`demiflow/workbench/web/analysis-charts.js`；动作：`add`。
- 签名：`validateChart(slice:TableSlice,spec:ChartSpec) -> {allowed,reason}`。
- 契约：C5；调用/接线任务：D11。
- 输入：validateChart(slice:TableSlice,spec:ChartSpec) -> {allowed,reason}。
- 输出：校验200行/16列/256KiB、X/Y存在、Y有限安全数值；非有限/unsafe int明确不绘制，null保留缺口。

校验200行/16列/256KiB、X/Y存在、Y有限安全数值；非有限/unsafe int明确不绘制，null保留缺口。complete只来自传入coverage。返回原因不用异常替换原值；不fetch。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V05；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## D11 · `mount`

- 文件：`demiflow/workbench/web/analysis-charts.js`；动作：`add`。
- 签名：`mount({parent,slice,spec,isCurrent}) -> FieldHandle`。
- 契约：C5；调用/接线任务：D08, A07。
- 输入：mount({parent,slice,spec,isCurrent}) -> FieldHandle。
- 输出：纯呈现表/JSON/bar/line切换；D10不允许时禁图但表和原值可用；bar0轴、line按原行序，明确当前页/采样。

纯呈现表/JSON/bar/line切换；D10不允许时禁图但表和原值可用；bar0轴、line按原行序，明确当前页/采样。用SVG/DOM，选X/Y不改变源数据，dispose停止resize。不接受代码/SQL/自由URL。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V05；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## D12 · `field`

- 文件：`demiflow/workbench/web/analysis-render.js`；动作：`modify`。
- 签名：`field(existingOptions + {density="normal",tools="inline"}) -> FieldHandle`。
- 契约：C6；调用/接线任务：D08, E06。
- 输入：field(existingOptions + {density="normal",tools="inline"}) -> FieldHandle。
- 输出：compact/menu仅变装饰与操作位置，正文优先；原typed格式、HTML/markdown安全、media proof、budget和原值保留。

compact/menu仅变装饰与操作位置，正文优先；原typed格式、HTML/markdown安全、media proof、budget和原值保留。media需点击，错误/来源失效不折叠。元数据可达且兼容原调用方，isCurrent和dispose语义保持。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V06, V07, V08；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## D13 · `notebook-style`

- 文件：`demiflow/workbench/web/analysis.css`；动作：`style`。
- 签名：`声明式CSS：Notebook正文/工具/内核状态/编辑与输出`。
- 契约：C1；调用/接线任务：D01, D05, D12。
- 输入：声明式CSS：Notebook正文/工具/内核状态/编辑与输出。
- 输出：合并工具栏，textarea字号/高度适应内容且有上限；保留focus、局部滚动和旧输出标识，错误不被折叠。

合并工具栏，textarea字号/高度适应内容且有上限；保留focus、局部滚动和旧输出标识，错误不被折叠。shared typed field compact规则限定该容器class，不污染Readme。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V01, V05, V06；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## D14 · `output-style`

- 文件：`demiflow/workbench/web/analysis-output.css`；动作：`style`。
- 签名：`声明式CSS：输出摘要/格式切换/有界图表`。
- 契约：C1；调用/接线任务：D08, D11。
- 输入：声明式CSS：输出摘要/格式切换/有界图表。
- 输出：来源摘要单行并可展开，图表与素材max-width:100%，卡片不引入全页横滚；表维持局部滚动。

来源摘要单行并可展开，图表与素材max-width:100%，卡片不引入全页横滚；表维持局部滚动。以D08/D11固定类名接线，不覆盖Case/media授权状态。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V01, V05；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## D15 · `readTableSlice`

- 文件：`demiflow/workbench/web/analysis-output.js`；动作：`add`。
- 签名：`readTableSlice(handle,{project,executionId,outputId,outputIndex,artifactDigest,api,signal,isCurrent}) -> Promise<TableSlice>`。
- 契约：C5；调用/接线任务：D08。
- 输入：readTableSlice(handle,{project,executionId,outputId,outputIndex,artifactDigest,api,signal,isCurrent}) -> Promise<TableSlice>。
- 输出：先沿现有analysis-source-from-handle验证id/generation/digest并取source；只投影前16个primitive字段，按C5.1构造只读v1 descriptor+query，analysis-case-page取当前20行，不存view/annotations。

先沿现有analysis-source-from-handle验证id/generation/digest并取source；只投影前16个primitive字段，按C5.1构造只读v1 descriptor+query，analysis-case-page取当前20行，不存view/annotations。核验source/view/query/binding，转换为TableSlice供D11；无primitive/预算拒绝保留原固定表跳转。相同output槽最多一项请求，切页释放。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V05, V08；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。
