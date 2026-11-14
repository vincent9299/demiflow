# C：可读图与纯呈现

只做布局和事实投影；不编译计划、不执行源码、不写封存文件。新的presentation模块由A装配、B调用，不能只写孤立helper。

所有任务mode=implement（移除/样式/装配按action），不设“只能验证不能修改”的消费者。参考基线267b914，签名新增默认值必须兼容旧调用。先按冻结契约编码，不等其他路实现；同路共享文件由一路负责人合入。

## 文件所有权

- `demiflow/workbench/web/workbench-graph.css`
- `demiflow/workbench/web/workbench-graph.js`
- `demiflow/workbench/web/workbench-presentation.js`

## C01 · `chooseGraphView`

- 文件：`demiflow/workbench/web/workbench-presentation.js`；动作：`add`。
- 签名：`chooseGraphView(graph, {actionId,preference}) -> GraphChoice`。
- 契约：C2；调用/接线任务：B14。
- 输入：chooseGraphView(graph, {actionId,preference}) -> GraphChoice。
- 输出：严格按C2表选择basis；未知action先报告再回首项，不能选择某个成功action掩盖失败。

严格按C2表选择basis；未知action先报告再回首项，不能选择某个成功action掩盖失败。sealed不可用返回reason，report替代须用户显式选，源码图不宣称可执行。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V03, V04；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## C02 · `buildNodeCard`

- 文件：`demiflow/workbench/web/workbench-presentation.js`；动作：`add`。
- 签名：`buildNodeCard(graph,selection,snapshot) -> NodeCard`。
- 契约：C2；调用/接线任务：C03。
- 输入：buildNodeCard(graph,selection,snapshot) -> NodeCard。
- 输出：按selection唯一找报告节点/封存occurrence；标题优先真实alias/operator.name，类型字典仅描述平台动作。

按selection唯一找报告节点/封存occurrence；标题优先真实alias/operator.name，类型字典仅描述平台动作。summary最多两条真实字段参数摘要，字符串每项<=160字符；状态复用真实observations，不用run成功填所有节点。 多occurrence组的selection.occurrenceId=null、reportNodeId=null，返回完整occurrences供选择；state说“多次出现/状态不同”而不是借用首成员。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V03, V04；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## C03 · `buildGraphModel`

- 文件：`demiflow/workbench/web/workbench-presentation.js`；动作：`add`。
- 签名：`buildGraphModel(graph,{choice,snapshot,selection}) -> GraphModel`。
- 契约：C2；调用/接线任务：B14。
- 输入：buildGraphModel(graph,{choice,snapshot,selection}) -> GraphModel。
- 输出：逐节点用C02生成key，边保持原端口/关系类型；多occurrence分别绘制，不改DAG身份。

逐报告节点或封存逻辑组用C02生成卡；当前dag边只有logical_node_id+port，故同action同logical ID绘一组，occurrences完整列出，不猜instance连线。组key含basis/action/logical/null，成员selection含occurrence；边按真实logical端点连组，无跨action合并/笛卡尔扩边。明确组内状态混合，不用第一成员充当整组。预算与局部图保持，纯函数不fetch/写Store。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V03, V04；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## C04 · `buildNodeDetails`

- 文件：`demiflow/workbench/web/workbench-presentation.js`；动作：`add`。
- 签名：`buildNodeDetails(graph,selection) -> NodeDetails`。
- 契约：C2；调用/接线任务：B05。
- 输入：buildNodeDetails(graph,selection) -> NodeDetails。
- 输出：只关联可证明的report/bridge字段；前驱字段标明上游输出，不冒充输入声明；未知值null+reason。

只关联可证明的report/bridge字段；前驱字段标明上游输出，不冒充输入声明；未知值null+reason。恢复限制始终保留；纯函数不读取当前业务文件。 选中多occurrence组时仅给逻辑级概况，sourceAvailable=false/reason=select_occurrence；成员选择后才关联具体report/observation。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V03, V04；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## C05 · `layoutGraph`

- 文件：`demiflow/workbench/web/workbench-graph.js`；动作：`add`。
- 签名：`layoutGraph(model,{width,nodeWidth=200,nodeHeight=116,gapX=36,gapY=20}) -> GraphLayout`。
- 契约：C1 C2；调用/接线任务：C06。
- 输入：layoutGraph(model,{width,nodeWidth=200,nodeHeight=116,gapX=36,gapY=20}) -> GraphLayout。
- 输出：新增有界DAG排层与分叉布局，按既有节点顺序稳定；循环/引用边不无限拓扑递归，保留原边并单独路由。

新增有界DAG排层与分叉布局，按既有节点顺序稳定；循环/引用边不无限拓扑递归，保留原边并单独路由。复杂度O(V+E)或明确有界排序，不按metrics重排；输出像素坐标不修改model。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V03, V04；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## C06 · `mount`

- 文件：`demiflow/workbench/web/workbench-graph.js`；动作：`modify`。
- 签名：`mount(host,{onSelect}) -> GraphController`。
- 契约：C0 C1 C2；调用/接线任务：B01。
- 输入：mount(host,{onSelect}) -> GraphController。
- 输出：复用SVG/ResizeObserver但统一update(model)路线；C05布局、卡片文本多行与summary，固定screen字号优先、键盘焦点/aria-label完整；选择回调NodeSelection。

复用SVG/ResizeObserver但统一update(model)路线；C05布局、卡片文本多行与summary，固定screen字号优先、键盘焦点/aria-label完整；选择回调NodeSelection。resize保持manual视角，fit由显式操作触发。dispose断开全部监听。 update结构不变时仅更新状态/文案，不能每个snapshot清空SVG或恢复滚动位置。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V03, V04, V08；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## C07 · `mountLogical`

- 文件：`demiflow/workbench/web/workbench-graph.js`；动作：`remove`。
- 签名：`移除重复逻辑绘图入口及其私有helper，仅保留单一mount`。
- 契约：C0 C2；调用/接线任务：B01, B02, B16, B17。
- 输入：移除重复逻辑绘图入口及其私有helper，仅保留单一mount。
- 输出：B01/B02迁移到统一model后删除mountLogical export和重复SVG；status/cpuChart仍有消费者须保留。

B01/B02迁移到统一model后删除mountLogical export和重复SVG；status/cpuChart仍有消费者须保留。迁移commit可短期兼容，最终验收不能存在两份普通/逻辑状态和两张隐藏画布。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V09；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## C08 · `graph-style`

- 文件：`demiflow/workbench/web/workbench-graph.css`；动作：`style`。
- 签名：`声明式CSS：.wb-graph内卡片/边/焦点/状态`。
- 契约：C1；调用/接线任务：C06。
- 输入：声明式CSS：.wb-graph内卡片/边/焦点/状态。
- 输出：从B移出SVG专属样式，卡片标题/摘要至少12px实际字号；status不靠颜色单独表达。

从B移出SVG专属样式，卡片标题/摘要至少12px实际字号；status不靠颜色单独表达。容器滚动不改变页面宽；只声明wb-graph局部规则。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V01, V03；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。
