# 最终 Local 98 API 矩阵

固定隔离树 `/yzp/zhaozy/yangzepeng/0905/_demiflow/worktrees/gpt_v3_regression_20261115`；分支 `engine/gpt-v3-regression-20261115`。测试 HEAD `e412fb6164c4c2501db37fc9feead396d8b2b909`，与根交付 `e304640c8ae91688c18747d18a94c5b44addad34` 的 `demiflow/`、`tests/` 逐字节一致。合并前保留 `backup/regression-pre-final-api-b5e0591`。

原 `test_runtime_v3_acceptance.py::test_api_matrix`、`::test_no_ray_data` 两项 **2 passed / 362.18s**，无 skip/xfail。API 矩阵 **98/98 tested、168/168 case passed、4 个正式退役入口按原契约拒绝、0 failed、0 not_run**；所有 Ray Data import attempts 均为空。未修改任何验收断言、预算或矩阵选择器。

## 执行次数纠正

逐案例 recorder 记录 **107 次真实 `execute_action`**。原 fixture 外层 recorder 同时观察到这107次，再把每case的107条观察拼到尾部，最终字段变为214；本轮已逐项证实前107与后107完全相等。**214是重复观察数，不能称214次独立执行。** 原no-Ray-Data smoke另有2次真实执行。

- `107-actual-executions.json`：按原 case index + case-call index 列举，保持每次真实调用，未以相同plan_id合并独立调用。
- `168-cases.json`：完整168案例的名称、API映射、正负性质、variants、执行调用数、原native commit数、结果与Ray Data尝试。
- 原始 `test_api_matrix/api-matrix.json` 与 `api_matrix.json` 保留原值，未篡改旧汇总来掩盖双计；后续修正 recorder 只需统一观察口径，不涉及生产执行能力或本次通过结果。

## 来源与资源证据

`verification.json` 包含1753份生产/测试/计划输入的 source_before/source_after 摘要，前后全同；测试前后HEAD相同、Git干净。进程内核验 demiflow及318个已加载模块均为本隔离树真实磁盘模块，0 test doubles。每项setup重核HEAD及干净状态，最终再次核源码，整个运行中未编辑源码、提交或合并。

FD如实记录：矩阵fixture baseline53 → close前59 → close后59（+6，原有+8阈值通过）；后续smoke59 → 59。两个fixture均closed且自有scope已删除。进程级测试前4、全部pytest结束51含初始化后的全局库/pytest资源；本轮没有逐FD归因，因此不宣称“零增长”或擅自把+6归类为业务泄漏/无泄漏。未放宽原资源清理断言。

证据在fixture删除自有目录前由外部pytest只读观察插件复制至本 `/tmp/demiflow-regression-final-api-evidence/`；插件未替换compiler/worker/journal/sink，未改原fixture关闭逻辑。XML、完整日志、外部runner及原证据文件摘要见 `SUMMARY.json`。

## 完整命令

```sh
PYTHONPATH="$PWD:$PWD/tests" DEMIFLOW_RESOURCE_ROOT=/tmp/demiflow-regression-capacity-final-api DEMIFLOW_V3_PROVIDER=local DEMIFLOW_V3_ENV=local DEMIFLOW_EXPECTED_HEAD=e412fb6164c4c2501db37fc9feead396d8b2b909 DEMIFLOW_TARGET_HEAD=e304640c8ae91688c18747d18a94c5b44addad34 /yzp/zhaozy/yangzepeng/0905/env/bin/python -B /tmp/demiflow-regression-final-api-runner.py -q tests/test_runtime_v3_acceptance.py::test_api_matrix tests/test_runtime_v3_acceptance.py::test_no_ray_data --basetemp=/tmp/demiflow-regression-final-api --junitxml=/tmp/demiflow-regression-final-api.xml > /tmp/demiflow-regression-final-api.log 2>&1
```

## 实际边界

这是固定e304640源字节的Local公开API矩阵和禁止Ray Data的真实smoke，不代替Ray全矩阵、多主机、真实GPU、外部业务数据库、40k压力或其他NotRun正式门。不重复先前106/564验收，不把既有专项结果合并为本次覆盖。主仓、其他隔离树和共享环境未改；本批无新源码提交，证据留 `/tmp` 供根协调者集成。
