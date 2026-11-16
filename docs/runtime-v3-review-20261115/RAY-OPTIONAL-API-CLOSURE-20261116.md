# Ray 搜索和 Wiki 三项失败收口

## 裁决

原全量矩阵 `f0ff14c` 上的三个失败均由 `env-ray` 缺少原来已经在
`pyproject.toml` 声明的可选依赖造成。本次没有修改生产实现，`demiflow/`
与 `f0ff14c` 字节一致。真实 Ray Core 下三个原案例已经定向通过，未调用
Ray Data，未减弱输出、资源边界或原生提交断言。该结果不是重新跑完一次
98 API 全矩阵，也不代表性能或多机验收。

隔离树：`/yzp/zhaozy/yangzepeng/0905/_demiflow/worktrees/gpt_v3_ray_api_20261116`。
分支：`engine/gpt-v3-ray-api-20261116`。
生产基线：`f0ff14c91e70fd27e7d36537c1db06122c6bc89b`。
测试与报告守卫提交：`25ebfa0`、`3a71c58`。本文属于后续文档提交。

## 原失败和对照

| 原 case | 裸 env-ray 的真实错误 | 同生产代码、补齐私有依赖后的结果 |
|---|---|---|
| native-search-http | `SearchConfig.validate_language` 导入 `babel` 失败；尚未执行 Dataset | 真实 1 HTTP、1 查询、1 execute_action、4 个 ACK |
| wiki-native | Ray worker 导入 `mwparserfromhell` 失败 | 正确解析 7 个节点，1 execute_action、3 个 ACK |
| wiki-character-bound | 同上，先被依赖缺失挡住 | 原 `ValueError: Wikitext exceeds character budget`，没有放宽 `max_chars` |

没有把缺依赖改成 skip 或虚构成功。三条测试直接复用
`runtime_v3_api_scenarios._cases` 与 `_run_case` 的原始案例和检查；只有
整个 Ray 库不存在的环境明确跳过此专用 Ray 测试，跳过不算 Ray 验收。

四份运行证据均独立保留：

- `25ebfa0`、裸环境：3 failed / 20.85 秒。
  `/tmp/demiflow-ray-api-red-evidence/verification.json`。
- `25ebfa0`、私有依赖装配：3 passed / 30.47 秒。
  `/tmp/demiflow-ray-api-green-evidence/verification.json`。
- `3a71c58`：3 条真实 Ray 案例及 1 条失败证据守卫，4 passed / 29.48 秒。
  `/tmp/demiflow-ray-api-final-evidence/verification.json`。
- `3a71c58`、进一步对齐搜索的 `python-dateutil==2.9.0.post0` 后，只复验受影响的
  native-search-http，1 passed / 16.25 秒。
  `/tmp/demiflow-ray-api-search-pinned-evidence/verification.json`。

以上每次都绑定运行前后源码摘要、实际 import 路径、三阶段测试记录和私有资源域；
无输入漂移、无残留子进程。最终代码有 2,215 份输入摘要，冻结七件逐字不变。
原红基线和其 worker traceback 没有被覆盖。机器可读汇总及原 case 明细在
[RAY-OPTIONAL-API-VERIFICATION-20261116.json](RAY-OPTIONAL-API-VERIFICATION-20261116.json)。

## 环境边界

私有解释器：`/tmp/demiflow-ray-api-optional-env/bin/python`。
由现有 Python 3.11 创建不带 pip 的 venv，私有 `.pth` 只读引用现有
`env-ray/lib/python3.11/site-packages` 与 `env/lib/python3.11/site-packages`。
所有测试和子进程使用该解释器；`PYTHONPATH` 显式绑定本隔离树。
没有往共享环境安装软件，也没有改其文件。

第一次组合装配沿用了 `env-ray` 的 `python-dateutil=4.0.0`；发现它与已有
search extra 的 `2.9.0.post0` 不一致后，仅在私有 venv 中建立指向 `env` 中
原版本包及其 dist-info 的只读链接，并重新验证搜索。两个环境清单及摘要都保留，
不声称最初组合环境已完整满足依赖声明。主要实际版本：Ray 2.60.0、
mwparserfromhell 0.7.2、babel 2.18.0、flask-babel 4.0.0、lxml 6.1.3、
pyarrow 25.0.1、datafusion 54.0.0。

每次只启动本任务 2 CPU / 0 GPU 的私有 Ray 集群，结束由原 executor.close 与
ray.shutdown 收尾。资源容量、资源 home、Forge domain 和测试目录均是独立
`/tmp/demiflow-*` 路径。没有修改主仓、共享环境、运行中的服务或其他任务。

## 修复的验收缺陷

原 `_s_api_matrix` 在聚合断言失败前未通过 `_save_evidence` 导出详细报告，随后
fixture 清理自己的目录，导致总控只留下失败 case 名称。本次仅在失败分支调用
已有证据出口，再照常抛原断言；成功路径和结果判定不变。
`test_runtime_v3_api_failure_evidence.py` 锁定失败报告保留原错误及 traceback，
该测试只验证报告守卫，不当成真实 Dataset 执行次数。

后续全量 Ray 验收应明确具备所选 API 对应的可选依赖。裸 `env-ray` 仍保持原样，
直接再次使用它运行 Wiki 和搜索，仍会得到相同缺依赖错误；这不是本次交付被撤回。
