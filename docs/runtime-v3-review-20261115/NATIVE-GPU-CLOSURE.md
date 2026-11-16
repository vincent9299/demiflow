# 工作台原生 GPU 与正式 V030 收口

固定实现提交：`fae6a05f420770eeee438ccd234336178925e603`。工作树：`/yzp/zhaozy/yangzepeng/0905/_demiflow/worktrees/gpt_v3_regression_20261115`；分支 `engine/gpt-v3-regression-20261115`。对应证据摘要与真实运行标识在 [NATIVE-GPU-CLOSURE.json](NATIVE-GPU-CLOSURE.json)。本记录接续前一阶段 [NATIVE-PRODUCT-GATES.md](NATIVE-PRODUCT-GATES.md)，不重绑旧测试结果。

## 结果

- 正式 `test_workbench_real_plan`：**1 passed / 637.51s**；内部 **7 passed / 620.84s**，没有 skipped/xfail/rerun。覆盖真实 union+嵌套 materialize、原报告桥、E2与 sealed_at、封存源码/参数、多 run/attempt 与过期响应隔离、删除当前代码后停止/恢复/历史结果、CPU资源/交换/恢复限制，以及真实 GPU 展示。
- 合同与历史反例：**74 passed / 139.91s**。唯一 deselected 是真实 GPU 用例，已在上面的正式门作为必需项执行通过。25 张量协议/资源反例、33 原纯计算、12 新资源/部署/环境合同、2 真执行证明替换反例、2 原 prompt identity 黄金与篡改反例。
- 两组同一 HEAD，**2,131 项 demiflow/tests/pyproject.toml 摘要前后全等**；七个冻结文件逐字等于 `49b666f`。测试入口进程内核验导入本工作树，没有裸 pytest 测到 editable 主仓的问题。

## 实际产品链路

真实 manager.prepare → 原静态及 CheckedExecution 参数核对 → 原平台 snapshot/gpu extra → 原 process host/DevicePool → H100 CUDA → 原 ACK/Lance → report identity bridge → Chromium E2。三条实际结果均为 CPU-before=0、GPU-visible=1、CUDA算术结果=16773120、CPU-after=0。原封存 profile 显示 `accelerators=[{"count":1,"type":"gpu"}]`、`memory_bytes=2147483648`，有真实节点观测。没有另造工作台计划、设备分配器或运行账本。

第二次独立运行实际取得同一部署域内的新设备 grant 后取消，要求原 manager 确认进程退出、`cleanup=complete`、`execution_stopped=true`、无结果提交且原设备 active/charged 清空。NVML 前后数值完整保存：free 直接取查询值，未用 total-used；允许驱动保留量，结束时没有本卡 compute app。2 GiB 是 host RAM 请求，GPU 无独立 memory 请求时按原规则独占设备，不能冒称 2 GiB 显存限制。

实现只增加新 manifest 的可选封存部署字段；旧无资源事实、旧错误层级、CPU/thread guard 和历史身份公式保持。GPU 虚拟地址窗口由受信部署显式给出有限值，原整树 RSS、磁盘、超时、取消监督仍在。Torch 依赖通过既有 extras/environment_inventory/verify_seal 绑定，没有第二份环境清单，没有共享环境安装操作。

原 OperatorChecker 检查完整函数体；允许的是有限纯张量值协议，未知 API、动态 shape、循环/随机状态/IO/模型加载/设备切换、缺GPU声明仍拒绝。构造、临时张量、tolist叶子/各层容器和指针数组共享同一累计窗口，helper 不刷新预算。能力范围及部署方式见 [workbench-native-gpu.md](../workbench-native-gpu.md)。

## 证据与范围

正式证据：`/tmp/demiflow-product-gpu-final-workbench_real_plan-evidence/verification.json`、`scenarios/workbench_real_plan-child.json`；合同证据：`/tmp/demiflow-product-gpu-final-contracts-evidence/verification.json`。真实截图及原 VM 观察路径均在 JSON 中逐项绑定 SHA-256。

子进程执行 `-I` 封存平台，测试进程自身 execute_action_calls=0 没有改成虚假计数；7 份原生执行证明在父进程再次重读原 journal/action/attempt、graph/physical、ACK、native receipt、固定结果版本、report_digest/桥以及候选源码和 pyproject 摘要后通过。空证明、错 run/attempt/report/plan/receipt/source/project_digest 与真实封存文件篡改全部拒绝。owned 临时工作区按原 fixture 在重读后清理，嵌入证据保留原事实。Ray Data 探针只覆盖测试进程，封存子进程没有冒称已放探针；独立禁 Ray Data 门由总控保留。

本组证明的是 Local/process 受管真实 GPU 产品链路，不增加多机/Ray GPU 声明，不声称任意 Torch 模型/训练已支持。环境验证仍为原 metadata-only 范围。历史/product_regression 的前一轮固定提交结果继续分别列在原记录，没有把它们重新标成在 fae6a05 执行。

## 保留的红证据

`/tmp/demiflow-native-gpu-positive-r2.log`：私有部署原 8 GiB 域不足以同时容纳明确运行额度与监督开销，正确拒绝；最终测试显式给私有域 20 GiB、运行 RSS 16 GiB/host内存6 GiB，未修改生产默认预算。

`/tmp/demiflow-native-gpu-positive-r3.log` 与 `-r5.log`：真实 CUDA/提交已成功，测试用 hash mutation 打开历史视图时仍停 README；app 普通分享 view 在 startup 消费。`-r6.log`：合法 view reload 后又点“读图与配置”，按原产品合同切回当前源码图。测试修正为原 kind=run 分享链接 reload 后留在运行监控查看 E2；生产导航未改。只读复放原 R6 成功运行的浏览器已绿，随后上述正式7案完整通过。

原 stop/resume 的日志导航修正 `333fd18` 已在这轮完整门通过，原全部停止/恢复/固定输出/历史断言保留。主仓、共享安装和共享服务不作为写入或测试目标；未 push、未合主仓。
