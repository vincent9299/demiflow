# 固定版本组合验收与剩余核心修复 G

原始意图不变：产品是看懂、配置、运行真实 pipeline，以及分析、比较、评测数据的统一工作台；引擎是正确、稳定、高性能、通用、资源有界、架构干净的统一数据计算平台。当前仍未全部完成，不以测试计数代替意图交付。

## 固定组合版本与实际验收

固定版本 `8928e5bef612f00d751b0ec02729cfeccfbb97cf` 已包含 Queue 原 ACKed cut 恢复、宽 join/有限写出空间复用、图像协作取消及原恢复计数。验收期间源码未改。

| 范围 | 实际结果 | 证据 |
| --- | --- | --- |
| Local Dataset 输入→统一执行→结果→原提交/恢复 | 98 入口/168 案例矩阵通过；队列完整业务、12 原生队列恢复/预算反例及2原SIGKILL；该组17项通过 | `/tmp/demiflow-final-8928-local-api-queue-r1-evidence/verification.json` |
| 真实 Ray Core | 同一98入口/168案例全过，107次实际 execute_action 全为 ray_core，Ray Data 尝试为0；额外宽join原ACK、literal原ACK/GC/resume两项通过 | `/tmp/demiflow-root-final-ray-8928-summary/summary.json` |
| 写出、宽行、共驻资源 | 31项通过；64/128MiB宽join、80KiB有限写出恢复、完整模型输出、Blob、等宽cast同时持有输入/输出的预算反例 | `/tmp/demiflow-final-8928-publication-r1-evidence/verification.json` |
| 原生模型/私有journal/取消/控制/历史 | 133项中130通过、3失败；三红均为真实SIGKILL在socket RST路径泄漏ConnectionResetError，未把它们计绿 | `/tmp/demiflow-final-8928-controls-r1-evidence/verification.json` |
| 工作台原14产品用例 | 受管执行2、真实Chromium4、Notebook1、关闭7，完整三阶段通过；封存图/历史attempt/源码参数/Case查询/Notebook真实执行 | `/tmp/demiflow-root-product-8928-evidence/REPORT.md` |
| 原7图像故障窗口 | 在仅2文件测试前置更新的 `f0f7470` 通过；生产逐字等于8928。两个130请求/130结果且严格128→130两cut；unknown、损坏对象、write_lance prepared/ACK及ACK后损坏保护 | 同一产品报告；旧故障注入未命中3红/中断原样保留 |
| 真实本机GPU | 原取消释放与非final ACK恢复2项通过；实际H100/原CUDA grant/原cut，非模拟GPU | `/tmp/demiflow-final-8928-gpu-controls-r1/receipt.json` |

总控独立逐 Git blob 复核12份回执的每条输入摘要，0不符、0运行期源码漂移、0遗留子进程。详见同名JSON，包含失败回执，不拼成“全部通过”。产品7项的测试HEAD、连接修复的叶HEAD均与8928分开记账。

## 新发现及处理

1. **连接终止语义**：真实RST在poll及K3读取入口遗漏了UNKNOWN_EFFECT映射。修复只捕获ConnectionResetError/ConnectionAbortedError，保持协议损坏/超时各自错误，不新增发送、重试或ACK。叶 `37544e8` 原3个SIGKILL代码固定双臂、真实RST及控制负例共28项通过，已经集成到 `a19c60a`。生产只有dataflow_hosts.py 12增2删。
2. **原图单worker能力**：旧stage把2/3个reader partition强制同时驻留，原workers=1在业务前被拒。独立叶新增分区波次，保留真实分区/参与者/原ACK，v2驻留信息只进入原cut载体，v1历史不重写；修复stage有别的partition镜像时错误给未启动partition恢复。新三种交换模式的中断恢复已过，仍待独立代码审查与原历史24项组合。
3. **图路由融合/显式存储预算**：普通后缀融合误吞显式router边界；graph_spec.storage_bytes遗漏封存/准入。候选已保留边界、把显式存储池分给真实边，1字节不足在任何worker/业务之前明确BUDGET_EXCEEDED。原反例待组合验收，不靠提高预算。
4. **历史观察迁移**：原U2/U3部分业务已正确执行/恢复，但仍找旧-chain/-graph-result账本、假定旧进程数或固定物理cut数。迁移到原v3 source frontier、逐条已发布业务行、实际PID/birth/SIGKILL和原ACK连续性，不减少原数据/预算/恢复断言。增量执行失败前已ACK的正确前缀保留；禁止错误地要求删除它们，也禁止失败后行或final成功发布。
5. **原40KiB写出恢复节点**：扩大到旧durable族时发现write_lance准入仍拒，正在固定8928对照及定位。保持原40KiB/R8KiB/B16KiB，未通过修改测试预算处理。

## 仍未验收的整体要求

图修复与旧观察迁移最终组合；40KiB写出；正式I08同环境五轮性能矩阵及原比值门；V035核心历史最终裁决。正式性能尚未启动，功能验收/旧宽行单次耗时均不冒充性能达标。

两机旧已授权SSH入口公钥认证仍失败，待当前有效入口；PostgreSQL无测试DSN。原Stage2固定业务数据/外部环境未擅用。未找到历史回执的外围节点不视为通过，也不把与意图无关的补全无限扩大为交付前置。

主仓仍为 `e40a6dd3dd756aba423dca4c7d995ada0a4bff65`；原11项dirty/18文件摘要与七件冻结全等。没有push、合主仓或更改在运服务。
