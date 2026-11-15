# 真实分组与前缀拼接接入（2026-11-16）

本提交把已有 Dataset.group_batches / prepend 接入原 Forge 静态声明与 CheckedExecution；沿用同一统一编译器、RunJournal 与原提交回执。有限分组仍输出 group_index/group_last，计时分组仅输出键与 items，不把微批误标为完整分组。prepend 第二输入递归绑定真实 Dataset；运行时改变前缀节点会被拒绝。

真实执行暴露并修复计时分组的三处缺陷：空闲时 poll 不刷新、每条输入重置计时器、未按原 chunk/buffer/grant 核算驻留。现按原预算限制分组行/字节，稳定生成输出 ID；快照保留剩余毫秒而非进程绝对单调时钟，恢复重新核验原额度。旧快照含待处理分组但没有可移植计时证据时拒绝恢复，不猜测历史。

## 验证

固定改动快照，命令 PYTHONPATH=$PWD:$PWD/tests、env/bin/python -B -m pytest；原资源目录与测试输出全部在 /tmp/demiflow-group-final*。组合为 test_runtime_v3_group_timing.py、test_runtime_v3_forge_grouping.py、test_runtime_v3_forge_ordering.py、test_runtime_v3_batch_timing.py。

结果：37 passed / 1 failed，196.10s。/tmp/demiflow-group-final.xml 与 .log。

新增分组/前缀 13 项全部通过：真实计时首组早于源 EOI、tuple/bytes/-0.0 保真、chunk/buffer 压力、真实 os._exit 后原 cut 恢复、三种 checked → write_lance → 原 ACK、六种非法声明、前缀漂移拒绝。原排序 17 项通过；批处理计时/恢复 8 项中 7 项通过，唯一未闭环为既有 embedding 首批到达超过源等待 4 秒，未放宽断言、未算通过。

另运行原 test_stream_grouping_lookup.py 为 2 passed / 4 failed；包含旧外部 seen 列表和跨进程 asyncio.Event 假设，已交总控及回归路继续裁决，未删断言或标为通过。本提交不声称全部产品或计时路径已完成。

承重七件与 49b666f 字节一致。主仓、生产数据、共享环境与其他工作树未修改。
