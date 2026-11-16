# 固定 ab1b334 的真实 Ray 完整验收与新增边界失败

## 总裁决

**原完整 API 矩阵通过；新增边界仍有两个明确待处理点，不能宣称本轮全部通过。**

候选 `ab1b33442685d09257b1ebc46e55fb40262317de`，本隔离树实际测试 HEAD
`a315742c0f3b503840f7ec49544d47618b41f3f9`。合流后的 `demiflow/`、`tests/`
与候选逐字相等，测试期间无源码/测试编辑。

本轮在 `/tmp/demiflow-final-v3-pressure-r4-evidence/verification.json`
确认 head/head_after 同候选、exit_code=0、changed_inputs=[]、status_after 为空后才启动。
没有与原 120 秒宽数据压力并行，没有调整任何预算、FD 阈值、原案例或断言。

| 真实组 | 结果 | pytest 总耗时 |
|---|---|---|
| 原 API 完整矩阵 | 98/98 API、168/168 case；pytest 1 passed | 599.62s |
| 独立 no-Ray-Data 门 | 1 passed | 21.38s |
| 新转换公共入口 13 项 | 7 passed、6 failed | 60.18s |
| 新仓内 Ray terminal seams 4 项 | 3 passed、1 failed | 28.15s |

## 完整矩阵的真实证据

- 168 case 全部 passed，failed/not_run 均为空；107 个唯一 observation_id 对应
  107 次真实 `execute_action`，provider 全为 `ray_core`，没有把 outer/child 观察重复相加。
- Ray Data 导入 0。使用实际 Ray Core **2.60.0**、ObjectRef/ActorHandle/host/provider
  模块身份核对，无 H 组 fake Ray；cluster 固定 `address='local'`、私有短 `/tmp`。
- 每个 case 的进程内 FD 均为 **44**；父 fixture 边界 FD **11→11**。
  私有 child 正常退出，结束时 Ray 已 shutdown。独立 no-Ray-Data、转换与 seams
  各进程也记录 shutdown=false-initialized、剩余子进程列表为空。
- **1784** 项 Python/plan 输入与 **2102** 项全部已跟踪源码/测试输入前后无变化。
  七承重文件对 `362d8528` 字节相同。没有安装、修改共享环境、外部 cluster、主仓或生产服务。
- 私有解释器 `/tmp/demiflow-ray-validation-env/bin/python` 复用已有同 ABI 依赖，
  全部来源与版本记录于各组 start/verification，而不把环境缺口假装成业务通过。

## 六条转换失败：确认是第三方归属分类接缝

失败范围：`test_public_conversion_preflight_and_success` 的
`take_batch/iter_batches × pyarrow/pandas/numpy`。全部在 source code-pin 阶段报：

```
INVALID_CONTRACT: Program code-pin refused:
Business dependency manifest exceeds budget; code-pin refuses (fail-closed)
```

尚未进入第三方转换构造。没有把这个错误改成期待的“转换准入”错误，正向臂也不能算已执行。
其余 typed-null 与六种原生 Arrow layout 的实际公共入口均通过。

独立纯探针 `/tmp/demiflow-ray-pin-ab1b334/probe.py` 在相同源码分别用私有 Ray
环境和原普通环境运行，无 Ray 集群、无 constructor monkeypatch、未修改依赖上限：

- 私有环境 numpy/pandas/pyarrow/pytest 的词法路径在该环境 site-packages，实际
  symlink resolve 到原 `env-ray/.../site-packages`。`chain._classify_dependency_module`
  只将 resolved path 与当前 sysconfig purelib/platlib 比较，错误地归入业务模块。
- 仅 import numpy 的三行 source、仅 import pandas 的 source，以及原 `_wide_rows`
  不做任何 monkeypatch，全部在第 65 个模块碰到原 64 模块上限。
- stdlib math 对照仅一个业务模块、成功。原普通环境同三种 source 全成功，
  numpy/pandas/pyarrow/pytest 分类全部为 None，业务模块均为 1。

因此不是 16 KiB state 预算过小或 forbid_constructor 污染，而是合法符号链接依赖
与实际第三方来源分类不一致。修复归总控裁决，不能直接扩大 `_DEP_MAX_MODULES`，
也不能略过真实业务来源/历史代码保护。private.json/normal.json 保留完整路径和分类链。

## 一条新 seam 失败：新增测试误查分支私有字段

`test_final_ack_replay_without_worker_or_new_commit` 已真实跑完首次 20×6 KiB 输入，
原 final ACK 后恢复得到相同全结果，恢复禁止 `_launch_phase` 且原 commits 完全相等。
最后测试却断言 `_verified_completed_image is not None`，因此失败。

这个字段仅由 `completed_live_nodes` 的已完成 live 对象特化分支赋值；普通可恢复
source 在 `start` 中经原 coordinator.recover 保存到 `_restore_image`。
将 live 私有分支当作统一恢复证据是**本路新增测试错误**，不是结果恢复失败。
原红证据保留，已建议总控将该行改为核验原 `_restore_image.cut.final` 及对应
原 journal ACK，而不动数据一致性、零新 worker、零新 commit 断言。当前未擅自改或重跑。

其余三条已真实 Ray 通过：`iter_rows` first-next 后 close 保原 20 秒/真实未耗尽
source cursor/非 final prefix/无剩余 delivery 和 worker；`take_all` limit=0/3
分别区分 exact 成功与 overflow ValueError，没有静默截断。

## 原始材料与范围

本报告 JSON 保存所有输入摘要、原始材料 SHA256、逐 case 结果、精确命令与环境来源。
四组前缀分别为 `/tmp/demiflow-ray-{api,no-data,conversion,seams}-final-ab1b334`，
均有 `.py/.log/.xml` 和 `-evidence/`。门控脚本/压力证据摘要保存于
`/tmp/demiflow-ray-final-ab1b334-launch.json`；因 conversion 非零自动停止，
随后按总控授权独立执行原仓内 seams，未把它伪装成门控全绿。

这是同机真实 Ray Core 证据，不替代多机/GPU/生产数据库；矩阵通过也不抹去新增
边界失败。本次只有证据报告提交，没有为过测试改实现、主仓或旧验收口径。
