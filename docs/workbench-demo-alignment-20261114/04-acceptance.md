# 独立编码、合流与最终验收

## 1. 交付要求

每路只交其所有权文件的commit、精确测试命令、源码路径/HEAD、结果和未验项。新函数必须有生产调用者，不能只在自己的mock单测使用。提供者/消费者按C0–C7同时编码；集成冲突由总控一次处理，不改冻结字段去迁就局部测试。完成整批后由GPT/Qwen做源码与真实用户旅程review，再统一修改；期间不要求逐函数人工审批。

公用资源不可并发覆写：app.js/index.html归A，workbench.js归B，graph归C，analysis-render归D，Case归E。同文件函数分给小agent时各用独立分支，由该路负责人合入；不把对方未完成视为不能编码。

## 2. 端到端验收场景与精确入口

| 门 | 唯一新测试入口 | 成功标准 | 必需反例 |
|---|---|---|---|
| V01 | `tests/test_workbench_d19_browser.py::test_ui_d19_shell_and_responsive` | 四tab、目录列释放、内容主导、真实屏幕字号、320–1920px截图人工对照 | 极长项目名/窄窗/侧栏收起/键盘焦点/未保存离开 |
| V02 | `tests/test_workbench_d19_versions.py::test_ui_d19_fixed_version_chain` | 表v1/v2不同内容/字段，sample/profile/Notebook export/Case/图片同版本 | 未登记/删除/alias漂移/只给版本对一半/run产物改版/旧digest逐字兼容 |
| V03 | `tests/test_workbench_d19_browser.py::test_ui_d19_graph_config_check_run` | source-only→检查→真实图→参数修复→检查→运行→固定结果 | 无效配置/保存冲突/过期报告/无权限/无节点不宣称通过 |
| V04 | `tests/test_workbench_d19_browser.py::test_ui_d19_sealed_graph_attempts` | 封存action默认可见，多action/occurrence/观测精确匹配 | 缺桥/缺源码/历史attempt/未知CPU不可造数/不支持恢复 |
| V05 | `tests/test_workbench_d19_browser.py::test_ui_d19_notebook_to_case` | 真Python+SQL同内核、单击运行、固定表页图表、转Case、保存重开 | CAS冲突/运行期间改代码/跨generation/旧输出/取消/重复点击/来源不匹配 |
| V06 | `tests/test_workbench_d19_browser.py::test_ui_d19_case_multimaterial` | v2两方案各图+文+评分、共享材料、配置三布局、移动窗口仍能读材料 | v1兼容/null材料/大字段/格式不符/媒体未加载/方案无proof |
| V07 | `tests/test_workbench_d19_browser.py::test_ui_d19_review_scope_and_conflict` | Case/方案/字段答案不同，重开可读；确认后保存并下一条 | 响应丢失/409/换source/媒体变化不前进、不吞稿、不覆盖别scope |
| V08 | `tests/test_workbench_d19_browser.py::test_ui_d19_late_response_and_cleanup` | 迟到响应隔离，失败可继续，20次切换后资源回稳 | graph/source/Case/output各延迟；低容量；旧写回执不得自动再发 |
| V09 | `tests/test_workbench_d19_browser.py::test_ui_d19_asset_and_single_authority` | 脚本/样式真实加载，旧重复画布/样式清理，调用接线和冻结件保持 | 移除新script应失败；对新builder加探针，真实页面未调用应失败 |

每个场景必须断言内容/版本/目标及关键API事实，不能仅断言页面没报错或按钮文本存在。可用假transport注入超时/迟到，但基准业务链必须真实API；未接分析runtime的metadata替身不得计V05通过。

特别要求：

- 图卡和inspector显示的schema/版本/状态与服务端返回逐项核对；实际调用C的新模型，不能旧图恰好也画对便通过。
- V02必须证明历史版本schema/rows/媒体不同于当前值，否则透传错误可假绿；原tables控制文件hash不变。
- V05内联图表实际调用D15读固定handle→source→page；不能只测手写rows。低于2列/没有数值时有诚实降级。
- V06/V07用现有`test_forge_analysis_case_groups_ui_browser.py`的v2、proof、真实Records/Lance模式做基础；本次comparison的v1文字截图仅作布局证据，不满足该门。
- 运行/Case列表页不预取所有内容；日志折叠不增读取；media只点击激活。验证私有句柄，不用无关系统进程数量当泄漏判据。
- UI相似性不用截图像素百分比替代人工判断：内容与后端事实不同；审“图读得清、材料优先、工具不遮主体、操作链连续、错误可恢复”。

## 3. 实施后的命令（本次未执行，测试尚待V实现）

所有命令在候选隔离树，**不得在主仓跑**。`UI_TREE`为真正待验树，环境只复用、不修改。真内核门须指定已批准、绑定当前候选代码的私有分析配置；没有环境则报告V05 not_run，不把skip变passed。测试服务port=0，产物/private domain均在新/tmp目录。

```bash
UI_TREE=/absolute/path/to/candidate-worktree
UI_EVIDENCE=$(mktemp -d /tmp/demiflow-d19-acceptance.XXXXXX)
export PYTHONPATH="$UI_TREE"
export PYTHONDONTWRITEBYTECODE=1
export DEMIFLOW_FORGE_DOMAIN="$UI_EVIDENCE/domain"
export FORGE_WORKBENCH_EVIDENCE="$UI_EVIDENCE/browser"
export FORGE_CHROMIUM=/yzp/zhaozy/yangzepeng/0905/.cache/ms-playwright/chromium-1243/chrome-linux64/chrome
export WORKBENCH_D19_ANALYSIS_CONFIG=/absolute/path/to/approved-private-candidate-analysis-config
cd "$UI_TREE"
/yzp/zhaozy/yangzepeng/0905/env/bin/python -c 'import os, pathlib, demiflow; assert pathlib.Path(demiflow.__file__).resolve().is_relative_to(pathlib.Path(os.environ["PYTHONPATH"]).resolve())'
/yzp/zhaozy/yangzepeng/0905/env/bin/python -m pytest \
  tests/test_workbench_d19_browser.py::test_ui_d19_shell_and_responsive \
  tests/test_workbench_d19_versions.py::test_ui_d19_fixed_version_chain \
  tests/test_workbench_d19_browser.py::test_ui_d19_graph_config_check_run \
  tests/test_workbench_d19_browser.py::test_ui_d19_sealed_graph_attempts \
  tests/test_workbench_d19_browser.py::test_ui_d19_notebook_to_case \
  tests/test_workbench_d19_browser.py::test_ui_d19_case_multimaterial \
  tests/test_workbench_d19_browser.py::test_ui_d19_review_scope_and_conflict \
  tests/test_workbench_d19_browser.py::test_ui_d19_late_response_and_cleanup \
  tests/test_workbench_d19_browser.py::test_ui_d19_asset_and_single_authority \
  --basetemp "$UI_EVIDENCE/pytest" -o cache_dir="$UI_EVIDENCE/cache" \
  --junitxml="$UI_EVIDENCE/new-acceptance.xml" -q
```

需要重测试锁的执行者仍按工作区既定锁非阻塞获取，冲突记未开跑，不起争抢循环。不要改既有环境/服务配置去使门绿。

## 4. 受影响的旧回归

以下文件均已在当前树核实存在，至少完整执行这些文件；旧断言若依赖被设计明确替换的DOM/文案，可改选择器，但必须保留原业务/身份/异常断言，并在变更表说明。尤其不能删除现有E2真实性测试，只能改为主图新入口。

```bash
/yzp/zhaozy/yangzepeng/0905/env/bin/python -m pytest \
  tests/test_forge_workbench_frontend.py \
  tests/test_forge_workbench_browser.py \
  tests/test_workbench_execution_binding_browser.py \
  tests/test_forge_public_api.py \
  tests/test_forge_workspace.py \
  tests/test_forge_analysis_case_demo_browser.py \
  tests/test_forge_analysis_case_groups_ui_browser.py \
  tests/test_forge_analysis_case_compact_browser.py \
  tests/test_forge_analysis_notebook_history.py \
  tests/test_forge_analysis_web_sources.py \
  tests/test_forge_analysis_source_export.py \
  tests/test_forge_analysis_media_source_export.py \
  tests/test_forge_analysis_http.py \
  tests/test_forge_markdown_reading.py \
  --basetemp "$UI_EVIDENCE/regression" -o cache_dir="$UI_EVIDENCE/regression-cache" \
  --junitxml="$UI_EVIDENCE/regression.xml" -q
```

V拥有**这些旧测试中必要的选择器/夹具兼容修改**，不得改生产实现；尤其旧metadata helper需补`execution_profile=None`，不得伪造实际runtime准入。资源/身份相关实现修改扩大时，额外纳入直接受影响套件而非无限全仓重跑。

## 5. 回执与判定

最小回执字段：source_tree、HEAD、dirty_diff_sha256、Python/Chromium路径、进程内import断言、精确命令、每nodeid结果、skip/xfail/未跑列表、screenshots、请求/版本/绑定证据、修改文件摘要。提交后验证被测字节等于交付commit；不要只写测试发生时的旧HEAD而不绑定dirty。

新9门漏收集/skip/xfail/重复nodeid只挑最后通过均不能通过验收。旧用例失败须隔离复现后分新回归/原有失败；不能静默deselect。真实runtime不可用是环境阻塞，不是产品通过。现有主仓/用户数据/保护七件hash核验独立记录。

## 6. 本次设计审查发现并已补入的接线点

1. E的新紧凑列表/筛选必须有`paintPage`迁移卡，补E11，避免新helper存在而旧主渲染仍使用老控件。
2. 历史版本不仅table/sample，补`table_media/table_evidence`及`media_source_export._selection`；后者有当前ref.version守卫，单改fixed_input不足。
3. Notebook真实table MIME是handle，不能只做消费虚构rows的图表；补D15固定handle→source→page读取与V05真实命中断言。
4. 主图选择不能只key logical_id；核对实际DAG边不含occurrence端点，采用“逻辑组＋明确成员选择”，不伪造每次出现的连线。多action与occurrence、缺桥、历史report替代均写入冻结模型及V04。
5. 所有新增提供方有可修改的生产消费者；所有新验收函数都有精确命令。检查器只验证设计结构，真实可用性仍靠上面旅程。
