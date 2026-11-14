# V：独立集成验收

可先写真实夹具和测试，全部实现集成后执行。每项拆一个test函数；必要参数化场景在该函数范围，不能用skip计完成。

所有任务mode=implement（移除/样式/装配按action），不设“只能验证不能修改”的消费者。参考基线267b914，签名新增默认值必须兼容旧调用。先按冻结契约编码，不等其他路实现；同路共享文件由一路负责人合入。

## 文件所有权

- `tests/d19_fixtures.py`
- `tests/test_workbench_d19_browser.py`
- `tests/test_workbench_d19_versions.py`

## V01 · `test_ui_d19_shell_and_responsive`

- 文件：`tests/test_workbench_d19_browser.py`；动作：`test`。
- 签名：`test_ui_d19_shell_and_responsive(owned_fixture, browser) -> None`。
- 契约：C0 C1 C2 C3 C4 C5 C6；调用/接线任务：A01, A11, B12, C08, D13, E10。
- 输入：本树实际实现、/tmp私有服务/数据、已声明环境与权限；只在故障/布局单测使用明确替身。
- 输出：绑定源码摘要的真实断言结果/截图/请求事实；无环境或未执行明确not_run。

四tab/项目切换/目录收起及320–1920视口；读取内容宽度、可见焦点、图文字实际字号；截图人工对照，开发/Readme保留。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V01；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## V02 · `test_ui_d19_fixed_version_chain`

- 文件：`tests/test_workbench_d19_versions.py`；动作：`test`。
- 签名：`test_ui_d19_fixed_version_chain(owned_fixture, browser) -> None`。
- 契约：C0 C1 C2 C3 C4 C5 C6；调用/接线任务：A06, A09, A10, F02, F03, F04, F05, F06, F07, F08, F09, F10, F11, F12, F13, F14, F15, F16, A13, A14。
- 输入：本树实际实现、/tmp私有服务/数据、已声明环境与权限；只在故障/布局单测使用明确替身。
- 输出：绑定源码摘要的真实断言结果/截图/请求事实；无环境或未执行明确not_run。

真实Lance两已登记版本不同schema/行/图片；浏览器切版本→sample/profile→Notebook source export→Case membership/media均同版本；alias漂移/被删/未登记/旧locator摘要/URL兼容反例。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V02；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## V03 · `test_ui_d19_graph_config_check_run`

- 文件：`tests/test_workbench_d19_browser.py`；动作：`test`。
- 签名：`test_ui_d19_graph_config_check_run(owned_fixture, browser) -> None`。
- 契约：C0 C1 C2 C3 C4 C5 C6；调用/接线任务：B01, B02, B03, B04, B05, B07, C01, C02, C03, C04, C05, C06。
- 输入：本树实际实现、/tmp私有服务/数据、已声明环境与权限；只在故障/布局单测使用明确替身。
- 输出：绑定源码摘要的真实断言结果/截图/请求事实；无环境或未执行明确not_run。

从source-only入口→显式检查→真实7节点图→节点配置保存/冲突→再次检查→运行→固定输出；验证API命中、节点摘要事实与返回绑定，不靠label通过。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V03；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## V04 · `test_ui_d19_sealed_graph_attempts`

- 文件：`tests/test_workbench_d19_browser.py`；动作：`test`。
- 签名：`test_ui_d19_sealed_graph_attempts(owned_fixture, browser) -> None`。
- 契约：C0 C1 C2 C3 C4 C5 C6；调用/接线任务：B02, B06, B08, B09, B10, B11, C01, C03。
- 输入：本树实际实现、/tmp私有服务/数据、已声明环境与权限；只在故障/布局单测使用明确替身。
- 输出：绑定源码摘要的真实断言结果/截图/请求事实；无环境或未执行明确not_run。

真实两个action、重复logical occurrence、局部图、缺桥/源码与历史attempt；默认封存图、精确selection/observations、日志范围、恢复限制与切换不串。无需重跑引擎全套，复用已存在业务验收夹具生成真实事实。 额外断言logical边不被展开成猜测的occurrence笛卡尔连线，多成员组选定一次出现后源码/观测才精确绑定。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V04；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## V05 · `test_ui_d19_notebook_to_case`

- 文件：`tests/test_workbench_d19_browser.py`；动作：`test`。
- 签名：`test_ui_d19_notebook_to_case(owned_fixture, browser) -> None`。
- 契约：C0 C1 C2 C3 C4 C5 C6；调用/接线任务：A08, D01, D02, D03, D04, D05, D06, D07, D08, D09, D10, D11, F11, D15。
- 输入：本树实际实现、/tmp私有服务/数据、已声明环境与权限；只在故障/布局单测使用明确替身。
- 输出：绑定源码摘要的真实断言结果/截图/请求事实；无环境或未执行明确not_run。

实际analysis runtime明确pin本树：SQL/Python交互、单击保存启动执行、取消/旧generation、保存重开；固定输出表/图/Case，来源一致。无runtime时标not_run/blocked，不能拿metadata double替代通过。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V05；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## V06 · `test_ui_d19_case_multimaterial`

- 文件：`tests/test_workbench_d19_browser.py`；动作：`test`。
- 签名：`test_ui_d19_case_multimaterial(owned_fixture, browser) -> None`。
- 契约：C0 C1 C2 C3 C4 C5 C6；调用/接线任务：E01, E02, E03, E04, E05, E06, E09, D12, E11。
- 输入：本树实际实现、/tmp私有服务/数据、已声明环境与权限；只在故障/布局单测使用明确替身。
- 输出：绑定源码摘要的真实断言结果/截图/请求事实；无环境或未执行明确not_run。

真实v2两方案每组image+md+evaluation，共享input/reference；三种layout和配置改动、标题、字段格式、原位预览；v1兼容，不以v1三个字段冒充多方案通过。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V06；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## V07 · `test_ui_d19_review_scope_and_conflict`

- 文件：`tests/test_workbench_d19_browser.py`；动作：`test`。
- 签名：`test_ui_d19_review_scope_and_conflict(owned_fixture, browser) -> None`。
- 契约：C0 C1 C2 C3 C4 C5 C6；调用/接线任务：E04, E07, E08, F10, D12。
- 输入：本树实际实现、/tmp私有服务/数据、已声明环境与权限；只在故障/布局单测使用明确替身。
- 输出：绑定源码摘要的真实断言结果/截图/请求事实；无环境或未执行明确not_run。

整个Case/有proof方案/字段各写不同答案；未绑定拒绝、不同source不串、未知回执/409/媒体变更不前进，成功保存下一条；关闭重开草稿/历史/冲突并存无丢失。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V07；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## V08 · `test_ui_d19_late_response_and_cleanup`

- 文件：`tests/test_workbench_d19_browser.py`；动作：`test`。
- 签名：`test_ui_d19_late_response_and_cleanup(owned_fixture, browser) -> None`。
- 契约：C0 C1 C2 C3 C4 C5 C6；调用/接线任务：A02, A03, A04, B06, B07, D02, D09, E01, E08。
- 输入：本树实际实现、/tmp私有服务/数据、已声明环境与权限；只在故障/布局单测使用明确替身。
- 输出：绑定源码摘要的真实断言结果/截图/请求事实；无环境或未执行明确not_run。

延迟graph/source/output/Case及项目读取，切对象后旧响应不得涂新页；重复点击写只有一intent，容量不足GET有界退避。20次切tab/case后worker/blob/observer/requests回基线，失败不清除草稿。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V08；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## V09 · `test_ui_d19_asset_and_single_authority`

- 文件：`tests/test_workbench_d19_browser.py`；动作：`test`。
- 签名：`test_ui_d19_asset_and_single_authority(owned_fixture, browser) -> None`。
- 契约：C0 C1 C2 C3 C4 C5 C6；调用/接线任务：A12, C07。
- 输入：本树实际实现、/tmp私有服务/数据、已声明环境与权限；只在故障/布局单测使用明确替身。
- 输出：绑定源码摘要的真实断言结果/截图/请求事实；无环境或未执行明确not_run。

index实际加载新模块、globals可用、所有新函数真实调用；无mountLogical消费者/双画布/第二Store；打包assets可达，冻结七件和旧主仓不变，实际import路径匹配。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V09；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## V10 · `owned_fixture`

- 文件：`tests/d19_fixtures.py`；动作：`add`。
- 签名：`@pytest.fixture owned_fixture(tmp_path,monkeypatch,request) -> Iterator[OwnedFixture]`。
- 契约：C0 C7；调用/接线任务：V01, V02, V03, V04, V05, V06, V07, V08, V09。
- 输入：@pytest.fixture owned_fixture(tmp_path,monkeypatch,request) -> Iterator[OwnedFixture]。
- 输出：建立本树import断言、独占/tmp域、port=0私有workspace service，复用已存在合成pipeline和Case v2夹具；参数need_runtime=true时只接受WORKBENCH_D19_ANALYSIS_CONFIG的已批准私有候选环境，否则明确门未完成。

建立本树import断言、独占/tmp域、port=0私有workspace service，复用已存在合成pipeline和Case v2夹具；参数need_runtime=true时只接受WORKBENCH_D19_ANALYSIS_CONFIG的已批准私有候选环境，否则明确门未完成。提供固定表两注册版本和图片/标注真实数据，不伪造service权限；finally关闭自己启动的服务/进程。测试不得链接主仓服务。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V09；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。

## V11 · `OwnedFixture`

- 文件：`tests/d19_fixtures.py`；动作：`add`。
- 签名：`@dataclass OwnedFixture(root,project_id,app,evidence_dir,source_head,source_digests,analysis_profile)`。
- 契约：C0 C7；调用/接线任务：V10。
- 输入：@dataclass OwnedFixture(root,project_id,app,evidence_dir,source_head,source_digests,analysis_profile)。
- 输出：测试域对象：root/evidence_dir为/tmp Path，project_id来自真实workspace，app是私有WorkspaceServer，source_head/digests绑定候选树，analysis_profile为真实环境事实或None。

测试域对象：root/evidence_dir为/tmp Path，project_id来自真实workspace，app是私有WorkspaceServer，source_head/digests绑定候选树，analysis_profile为真实环境事实或None。只有V10持有生命周期，不给产品代码使用；无付费模型、生产数据、共享运行服务。

**失败与状态**：遵守上述契约的作用域、预算、权限和未知回执规则；不能用空值/成功提示吞错，不能清除旧草稿和已提交结果。UI异步结果须绑定创建请求时的身份。

**验收**：V09；实现方提交具体断言与结果，消费者调用覆盖必需。局部替身测试不替代最终真实集成。
