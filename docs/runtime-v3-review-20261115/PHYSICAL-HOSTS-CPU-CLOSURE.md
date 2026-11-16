# 两台真实宿主 CPU Dataset 闭环

固定执行提交 `d56d9c7763cec69798c786cf3dd9710e2acf09ef`：原生产 `5ff5da9` 加原协议 owner 的租约修复 `dd45d7a`。原 `177a623` 四条验收未改断言、预算或数据。**4 passed，120.207 秒**。

| 原验收 | 真实结果 |
|---|---|
| `/tmp` 不共享的读取域 | 原准入拒绝；没有 worker phase、没有提交 |
| `range(12) → map → write_lance(create)` | 两个真实 hostname/node ID均有实际 worker；12 行准确，原 final ACK |
| 真正的远程 actor 在已 ACK 后被杀，再 resume | 只杀本测试 actor；同 plan 两个 attempt；原 ACK 对应 4→8→12→final 12 行，结果每个 ID 恰好一次 |
| 同一 source 的两分支 union → repartition(3) | 两宿主实际执行，最终 16 条 `(id,branch)` 多重性完全一致；原 final ACK |

本机 `job-09b54ebad3bd-20260629073159-5584d65864-wcm7n`（10.233.68.27）与远端 `job-bb1b7400ff7f-20260828124445-68db7f5c68-hfxcn`（10.233.122.251）。每台私有 Ray 节点仅 2 CPU、0 GPU；无 Ray Data；真实 host/node/actor/PID/boot/plan/cut/receipt/rows 逐例在配套 JSON。共享源码通过两节点实际导入摘要比对，1786 个 Python 输入前后 `changed_inputs=[]`。

完整执行脚本 `/tmp/demiflow-physical-hosts-20261116/run_retest.py`。环境为原共享 `env-ray/bin/python -B`，显式 `PYTHONPATH=<隔离树>:<隔离树>/tests`，`DEMIFLOW_PHYSICAL_CANDIDATE=d56d9c7763cec69798c786cf3dd9710e2acf09ef`，独立证据根 `/tmp/demiflow-physical-hosts-20261116/r2`。原始日志 `/tmp/demiflow-physical-hosts-20261116/run-r2.log`，XML `r2/result.xml`。

两端私有进程已确认退出：remote exit=0、remote children=[]，local Ray shutdown、children=[]。没有全局 stop、安装、修改共享环境或主仓。初轮 1 绿 3 红的报告和文件摘要保留，没有覆盖。

仅本测试两棵带 owner 清单的共享目录已清理：`cpu-6d81795387bc3bdb`、`cpu-2816d1264820a95e`。原 journal 以 SQLite backup 归档、原 v1/v2 lock/header 和 commit 字节证据留在 `/tmp/demiflow-physical-hosts-20261116/cpu-journal-archive`，配套 JSON 逐文件 SHA/大小与删除后不存在事实齐全。

此证明覆盖真实跨机 Dataset、路由、共享源分支、原提交和 owned actor 故障后的恢复。它没有覆盖两个 GPU 宿主的 GPU 执行、整机失效、网络分区或本批 driver 进程崩溃；GPU 绑定方案另行实现，不把 CPU 成功冒充 GPU 完成。
