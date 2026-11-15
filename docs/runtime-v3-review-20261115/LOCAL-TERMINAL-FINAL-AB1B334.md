# 固定候选 ab1b334：Local 终端 / 恢复 / 转换组合验收

结论：**101 passed / 268.77s，0 failed，0 skipped，0 duplicate**。本次只做总控指定的最终组合，没有修改任何实现或测试断言。

## 版本与启动条件

- 根候选：`ab1b33442685d09257b1ebc46e55fb40262317de`。
- 隔离树：`/yzp/zhaozy/yangzepeng/0905/_demiflow/worktrees/gpt_v3_artifact_recovery_20261115`。
- 本树实际运行 merge HEAD：`48640e65350f0192084bba0f10ac838af084ab5a`；运行前后相同。
- 合流后、执行前和执行后，`git diff ab1b334 -- demiflow tests` 均为空；工作树干净。两者源码等同，不把隔离 merge HEAD 冒称根 commit。
- 严格等待 `/tmp/demiflow-final-v3-pressure-r4-evidence/verification.json`：同候选 HEAD 前后相同、exit 0、输入未变、工作树干净，且压力 XML 摘要相符后，才启动本组合。
- 进程内断言 `demiflow.__file__` 位于本隔离树；1,784 项输入摘要（production / tests Python、plan.json）逐项等压力测试候选，运行结束仍未变化。

## 实际范围

| 范围 | 实际测试项 |
|---|---:|
| 完整 `test_runtime_v3_batch_conversion_admission.py` | 56 |
| 完整 `test_runtime_v3_terminal_replay_admission.py` | 10 |
| 完整 `test_runtime_v3_terminal_read_workspace.py` | 7 |
| 完整 `test_runtime_v3_terminal_stop.py` | 5 |
| 完整 `test_runtime_v3_terminal_close.py` | 4 |
| 原 Arrow / Lance / Parquet schema 与投影 / 重命名 | 4 |
| 原空 typed schema / materialized 历史恢复 | 10 |
| 原 Dataset Arrow / NumPy / Pandas 批次 API | 2 |
| 原 collection 小预算拒绝、显式 batch size / drop_last | 3 |
| **总计** | **101** |

后四行是独立复审清单 `/tmp/demiflow-batch-conversion-targeted-review.json` 的全部 8 个 selector 展开的 19 例。完整五文件共 82 例。收集时记录 nodeid 去重，实际没有重复项或被移除测试。

验收包括真实 Dataset 输入 → 统一执行器 → 结果、原 ACK 历史恢复、提前结束 / 关闭与 FD 释放；正负预算、schema 变化时仍活预读窗口、转换前拒绝、原类型 / 值 / metadata、空输入和显式分批语义均沿原测试断言。没有为过门降低原预算或删减断言。

## 完整证据

- 原运行记录：`/tmp/demiflow-final-local-terminal-evidence/verification.json`。
- 收集清单：同目录 `collection.json`；完整参数、环境、实际导入路径及源摘要在 `start.json` / `verification.json`。
- 完整 XML：`/tmp/demiflow-final-local-terminal.xml`。
- 日志：`/tmp/demiflow-final-local-terminal.log`。
- XML SHA-256：`6839571f96d8326c19ea61ee3fa23e08e8bad725bf20691d8917740da26464e8`。
- verification SHA-256：`dcf18b3dfb2998038617879d74acbb709247f086a66a3dc8f32971d7d6e3cb07`。
- 本报告相邻 `LOCAL-TERMINAL-FINAL-AB1B334.json` 是原 verification 的逐字节副本。

执行使用 `env/bin/python -B`，显式 `PYTHONPATH=<本树>:<本树>/tests`、`PYTHONDONTWRITEBYTECODE=1`，资源、Forge domain、测试和证据目录均为本次独占 `/tmp/demiflow-final-local-terminal*`。没有触碰主仓、外部服务、共享环境或生产数据。

## 意图对应与限制

本组合支持平台 root 的统一真实执行、有限托管载荷、正确恢复及历史保护；也保护产品读取真实结果和历史时的类型与批次语义。这里的预算保证是已识别的托管载荷 / 工作区准入，不是普遍进程 RSS 硬隔离。

这是一份固定版本 **Local** 组合证据。Ray 的最终验收由另一条执行道报告；本轮没有重跑 Ray 全矩阵、另外 106 / 564 项、浏览器、真实多机或 GPU，也不以本报告宣称整个产品 100% 完成。总控指定的本轮范围内没有发现失败。
