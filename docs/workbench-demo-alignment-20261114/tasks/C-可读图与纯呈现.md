# C：可读图与纯呈现

只做布局和事实投影；不编译计划、不执行源码、不写封存文件。新的presentation模块由A装配、B调用，不能只写孤立helper。

所有任务mode=implement（移除/样式/装配按action），不设“只能验证不能修改”的消费者。参考基线267b914，签名新增默认值必须兼容旧调用。先按冻结契约编码，不等其他路实现；同路共享文件由一路负责人合入。

## 文件所有权

- `demiflow/workbench/web/workbench-graph.css`
- `demiflow/workbench/web/workbench-graph.js`
- `demiflow/workbench/web/workbench-presentation.js`

接线修订：另读 [Runtime v3 接线](../05-runtime-v3-wiring.md)（C8）；平台在途源码不由本路覆写，合流按明确交接执行。

## C01 · `chooseGraphView`

- 文件：`demiflow/workbench/web/workbench-presentation.js`；动作：`add`。
- 签名：`chooseGraphView(graph, {actionId,preference}) -> GraphChoice`。
- 契约：C2 C8；调用/接线任务：B14。
- 输入：chooseGraphView(graph, {actionId,preference}) -> GraphChoice。
- 输出：按C2/C8选择source/report/legacy/v3；未知明确action不自动回首项。。

按C2选择主视角；sealed版本由C09校验，不能凭exchanges等字段猜测。深链接的明确action不存在时返回unavailable；只有没有明确选择才按服务端顺序默认。缺封存不静默切当前图；report替代须用户显式选。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V03, V04；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## C02 · `buildNodeCard`

- 文件：`demiflow/workbench/web/workbench-presentation.js`；动作：`add`。
- 签名：`buildNodeCard(graph,selection,snapshot,execution=null) -> NodeCard`。
- 契约：C2 C8；调用/接线任务：C03。
- 输入：原graph、NodeSelection、原snapshot；execution为C03本次构建的ExecutionProjection或null（source/report），不是持久状态。。
- 输出：按精确实例生成NodeCard；legacy才保留逻辑组和成员picker。。

source/report沿原规则。sealed消费C03传入的ExecutionProjection，不重新建身份。legacy组保留所有成员，不能借首成员状态；v3一occurrence一卡，相同semantic也不合并。title保留真实实名，summary<=2且每项<=160字符。按C8完整作用域核对观测，缺scope/缺页/融合均不猜成功。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V03, V04；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## C03 · `buildGraphModel`

- 文件：`demiflow/workbench/web/workbench-presentation.js`；动作：`add`。
- 签名：`buildGraphModel(graph,{choice,snapshot,selection}) -> GraphModel`。
- 契约：C2 C8；调用/接线任务：B14。
- 输入：buildGraphModel(graph,{choice,snapshot,selection}) -> GraphModel。
- 输出：版本分支明确的GraphModel；execution携带本次真实运行身份。。

source/report沿旧投影；sealed先调用C09一次，再把结果传C02。legacy按logical ID分组、成员选择；v3按occurrence单独绘图，用原edge_id和端口连接，禁止同semantic合并或笛卡尔扩边。缺实例端点/不匹配返回明确unavailable及可选当时检查图；不伪画完整计划。保持预算，纯函数不fetch/写Store。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V03, V04；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## C04 · `buildNodeDetails`

- 文件：`demiflow/workbench/web/workbench-presentation.js`；动作：`add`。
- 签名：`buildNodeDetails(graph,selection) -> NodeDetails`。
- 契约：C2 C8；调用/接线任务：B05。
- 输入：buildNodeDetails(graph,selection) -> NodeDetails。
- 输出：NodeDetails增加execution、placement、exchanges与recovery事实，未知为null/reason。。

调用C09验证选中action；legacy多成员未选择时只给组概况，v3精确occurrence关联。按C8物理索引读取stage/profile/role/分区/replica目标、与本节点相邻的exchange.mode/reason、recovery/limitations。计划配额不冒充实时用量。源码参数只经真实bridge，缺桥禁用；不读取当前业务源码。

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
- 契约：C0 C1 C2 C8；调用/接线任务：B01。
- 输入：mount(host,{onSelect}) -> GraphController。
- 输出：复用SVG/ResizeObserver但统一update(model)路线；C05布局、卡片文本多行与summary，固定screen字号优先、键盘焦点/aria-label完整；选择回调NodeSelection。

复用SVG/ResizeObserver但统一update(model)路线；C05布局、卡片文本多行与summary，固定screen字号优先、键盘焦点/aria-label完整；选择回调NodeSelection。resize保持manual视角，fit由显式操作触发。dispose断开全部监听。 update结构不变时仅更新状态/文案，不能每个snapshot清空SVG或恢复滚动位置。 统一mount必须承接runtime-v3:W021的真实v3详情/节点能力，禁止按旧logical ID把v3节点去重；只消费GraphModel，不自行做格式分派。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V03, V04, V08；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## C07 · `mountLogical`

- 文件：`demiflow/workbench/web/workbench-graph.js`；动作：`remove`。
- 签名：`移除重复逻辑绘图入口及其私有helper，仅保留单一mount`。
- 契约：C0 C2 C8；调用/接线任务：B01, B02, B16, B17。
- 输入：移除重复逻辑绘图入口及其私有helper，仅保留单一mount。
- 输出：B01/B02迁移到统一model后删除mountLogical export和重复SVG；status/cpuChart仍有消费者须保留。

B01/B02迁移到统一model后删除mountLogical export和重复SVG；status/cpuChart仍有消费者须保留。迁移commit可短期兼容，最终验收不能存在两份普通/逻辑状态和两张隐藏画布。 按05-runtime-v3-wiring的W021→C06迁移表，在W交付后移除；保留真实端口/资源/恢复展示语义，不整文件覆盖W的提交。

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

## C09 · `normalizeExecutionPlan`

- 文件：`demiflow/workbench/web/workbench-presentation.js`；动作：`add`。
- 签名：`normalizeExecutionPlan(entry,binding) -> ExecutionProjection`。
- 契约：C2 C8；调用/接线任务：C03, C04。
- 输入：原graph.logical.actions[]的一个action＋原GraphBinding；不读文件/不fetch。
- 输出：available联合类型：legacy逻辑组或v3实例图、完整身份、物理摘要；失败为稳定reason。

按05及contracts.d.ts精确版本识别，校验action/dag/plan/revision、节点/端口/边/物理关联、现有预算。legacy保留原字节和组边；v3保留occurrence/edge_id/端口，缺端点返回v3_endpoint_identity_missing。未知schema不能降级旧图；不重算hash，不把投影持久化。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V04, V09；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。
