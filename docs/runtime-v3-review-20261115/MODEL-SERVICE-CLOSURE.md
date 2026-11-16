# 模型服务、输入与资源释放：总控组合复验

继续在 `engine/gpt-runtime-v3-review-20261115` 隔离分支实施，没有推送或合入主仓。本页是有版本范围的增量，不表示两大 root 或性能全部完成。

## 已串通的用户链路

普通 `Dataset.map_prompt_async(service=ManagedHTTPService(...))` 现在通过同一编译器封存被动声明，由实际 worker 构造服务，沿已有 guardian、进程 unit 和端口锁关闭。没有另建模型执行器或提交账本。正常调用可物化为原 ResultRef；重复读取已提交结果不再调用模型。显式 SQLite 缓存命中时不启动服务；运行额度为零时不产生新 HTTP。取消必须离开真实服务进程，并且原 action 没有成功结果。

GPU 服务使用已有 DevicePool 和 worker grant。固定 `4cf3555` 的真实 H100 测试记录实际服务 PID、CUDA tensor `[3,4,0]`、NVML UUID、原 active device allocation 与 2 GiB 原内存预约，Prompt 结果来自该 tensor。最后原设备占用释放。物理 GPU 不由旧 flock 再分配；带服务的 Prompt 与 Embedding 共用现有资源与 guardian 协议。

服务声明只接受未启动、未修改的原生 ManagedHTTPService；启动后的 owner、被替换的方法、声明后变更的环境或删除服务均拒绝。普通 Prompt 的原语义事实、HTTP/cache 身份保持；完整环境仅在执行程序里，公开计划存服务摘要，不复制环境值和任意命令参数。原 request_policy 守卫未移除。

## 总控已运行的验收

完整来源、JUnit 摘要与真实 GPU 证明见 [机器回执](MODEL-SERVICE-PRIMARY-VERIFICATION.json)。不同版本、重叠用例不相加冒充一次全量通过。

| 固定版本 | 实跑结果 | 结论范围 |
| --- | --- | --- |
| `0522631` | 39 passed | 完整旧 inference 文件与新模型观测；Prompt/Routed 真实指标、原线程池与关闭 |
| `b10997b` | 18 passed | 返回的 cleanup OSError 仍为 UNCONFIRMED，保持占用并用原状态机重试 |
| `5707e1b` | 21 passed、1 Ray skipped | 大图片字面输入有界哈希、旧身份字节与原 block；未把 Ray 跳过算通过 |
| `5d66d51` | 26 passed、1 failed | guardian/原资源释放通过；唯一旧 Prompt＋服务＋缓存入口失败，后续由本修复关闭 |
| `428b67b` | 20 passed | 真实 Prompt 服务、私有缓存/原结果、取消、历史身份与普通 HTTP owner |
| `4cf3555` | **35 passed、0 skipped，80.89 秒** | 上述服务、H100、历史身份与原生声明守卫的同版本组合 |
| `91c8658` | **真实 Ray Core 9 passed、0 skipped，121.65 秒** | 私有模型 scope/part、原结果重读、bounded ACK RPC、receipt 后 ACK 前强杀、driver restart 与旧 worker FENCED |

新服务第一轮有四个生产接线失败：冻结配置的容器未经过现有 strict-JSON 投影；已改用原 `_json_config`，没有放宽 DTO。下一轮只剩新取消测试把异常捕获放在 RunContext 内部，原 context 退出仍正确报告取消；测试已包住完整生命周期，并断言原 action result 为 None。所有红回执保留，未将其写成通过。

旧 `test_http_service` 原来用父进程的全部 Popen 次数作为模型服务次数；统一进程 worker 后该观测位置不能区分 source/worker/服务。现在从实际服务脚本记录 PID/birth，仍严格要求 **只启动一次**、两次八行结果、服务真实退出，同时保留父侧所有子进程退出断言。

大输入修复仅让 literal payload 用有界流式摘要参与源身份；原 describe/document/identity 不改，1 MiB 元数据上限不改。1.6 MB 图片、1024/1025 边界、负零、UTF-8、旧身份、实际物化均有正反例。

## 保留的边界与在途工作

- 普通私有 Prompt 可以提交当前 attempt 的结果，但私有 HTTP 库与 RunJournal 不是跨库原子事务；失败后禁止伪装成原生恢复。Embedding 的原 delivery-only 边界未放开。
- 本服务接线不宣称所有 VLLM/DeploymentService、自定义 live 服务、routed service 多设备放置已完成。旧数值 GPU 下标的服务目前只支持已显式声明 inventory/PCI 顺序的 Local process；Ray 上的普通原生 GPU 算子有此前独立证据，不能混称全部服务对称。
- GPU 整卡独占、进程树 RSS 抽样上限不等于分数 GPU/MIG/OS 硬内存沙箱。guardian 与丢弃 unit FD 的后代同时遭 SIGKILL 仍在已披露的合作进程单元边界之外。
- 两处 worker 丢唤醒已集成为 `258a173`/`3656c73`，只修改同一 condition 的通知代际。owner 21 项及迁移后的四项有固定版本证据，旧三个失败在原 runner 上同样复现；总控的真实 Ray 组合见上表。
- 性能未达标。新 wire 批格式整链无实质收益，未合入；唤醒修复的 25.880 秒只是原 20k→10k 单次诊断，不能替代同预算五轮矩阵。
- 私有模型缓存显式 requeue 后的新调用额度已修复并集成 `492c2b8`；总控独立核 owner 九组源码与40附件零不符，真实 Ray 组合也通过。Arrow 分配前准入、公平与内存正式门仍在收口；仅余1CPU的64行共享分支暴露80次worker启动/120秒超时，保留为实际核心性能失败。产品既有完整演示/Notebook/Case 的证据按原版本保留，不用本组替代最终产品验收。

主仓 HEAD/原 dirty 文件和七冻结件保持；此前已披露的共享目录边界事故仍按原记录保留，不因本轮检查通过而撤销。
