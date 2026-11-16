# 双物理宿主真实 GPU 绑定与恢复验收

本批补齐统一 Dataset 引擎在 Ray Core 多机 GPU 路线的实际设备绑定。生产候选是 `883cc5c0cb6b3ad12701464a21377818679f41f7`，工作树 `gpt_v3_residency_20261115`。所有业务、提交、恢复与资源记账沿既有平台权威；不调用 Ray Data，不新建 allocator 或提交账本。

## 实现及边界

- Ray Core 先授予真实物理 GPU 并建立尚未执行用户算子的 worker 外壳；立即保存原 actor handle。外壳报告实际 node、Ray GPU ID、CUDA 可见 UUID及 NVML 总显存，经核验后，原 DevicePool 预约完全相同的设备，最后才激活业务。
- Ray 默认数字 GPU ID 不假设等于 NVML 序号。已授予的外壳使用 CUDA driver 读取可见 UUID；旧 v1 数字 inventory 保留原 pool ID，由同一原记录关联实测 UUID。CPU worker 显式清空 CUDA 可见设备。
- 部署 v2 inventory 明确每台 hostname 的设备 UUID；拓扑只读，不存可用余额。单卡/单宿主可行性在原准入处核对，不能拿集群总显存冒充一张卡容量。
- identity 超时不能丢弃已创建 actor handle；未知退出保留资源占用。跨 provider 重建按原 journal 中的 cluster/namespace/actor/spec/boot 身份精确恢复清理权，不把查不到名字当作从未创建。
- 使用整数整卡 Ray grant。逻辑显存预算、Torch tensor 峰值与 CUDA driver/context 内存有区别；没有宣称硬 VRAM 隔离、fractional GPU、MIG或训练性能。

## 实测

双机集群只属于本次测试：本机 6 CPU/4 H100，远端 6 CPU/2 H100；每台 Ray object store 128 MiB；每个 GPU worker 请求 1 GPU/2 GiB，run 内存准入 16 GiB。现有 env-ray 与 env 的库仅通过私有 /tmp 环境链接使用，没有安装或修改共享环境。远端通过共享文件系统运行同一隔离源码，不做 SSH copy。

| 场景 | 结果 | 关键事实 |
|---|---|---|
| 六卡正常 | 通过 | 单个真实 Dataset CPU源→六个CUDA分支→union→CPU结果，六张不同UUID在两台机器同时进入CUDA回调；六份原allocation与原DevicePool逐一相等，最终原ACK |
| 六卡业务异常 | 通过 | 所有六卡同时实际执行后，指定分支抛业务错误；六份原reservation全部经退出确认释放 |
| 六卡用户取消 | 通过 | 所有六卡同时实际执行后调用公开RunContext.cancel；六份原reservation全部释放 |
| 远端GPU actor丢失后恢复 | 通过（146.95秒） | 同一六分支图；ACK65行后只杀本任务远端GPUactor；两attempt同一plan恢复到96行，原ACK前缀不重算，原3条ACK不改，12个GPU分配全部释放 |

前三项 `3 passed, 1 deselected`，168.92秒。1条deselected为独立默认数字inventory本机测试，不计双机通过项；它另有真实默认可见性正向结果 `1 passed` /21.98秒。原资源协议/真实Ray身份与清理11项兼容测试另有 `11 passed` /25.86秒。

原三项的1,789条Python输入摘要已逐一与883cc5c提交内容核对，0不匹配；执行前后输入0改变；实际两机worker的源码摘要与隔离树路径一致。完整原始日志、JUnit、集群进程/设备清理记录、原GPU分配回执和原run数据库归档路径见配套JSON。

恢复最终测试提交 `52d94576343026e27609339dddcdd3e62dd90ed8`，1,789条输入与该提交逐字节相等，执行前后0改变。实际杀actor前96次CUDA回调已完成，原非最终ACK只发布65行；恢复重新创建6份GPU宿主分配并恢复原缓冲，后31行不重做CUDA。不能据此宣称未计算CUDA后缀也已验收。

恢复测试首轮有一个测试自身错误：把Record.inline写成Record.payload，在读原45行前缀时抛AttributeError。该红记录保留，没有算生产缺陷或删掉反例；修正仅改读字段，生产仍883cc5c。

## 未覆盖

- 物理整机掉电、网络分区、driver进程被SIGKILL与不同Ray集群重建，均不能用本次单actor死亡替代。
- 多机GPU吞吐/性能比较另归性能矩阵；本批是实际设备/资源/恢复语义验收。
- 不是单个map自动复制到六张卡；本批通过显式六个GPU业务分支迫使双机设备并发，并核其真实UUID。
- 根隔离分支尚须集成本生产候选，并结合函数状态/整体Local-Ray-工作台补丁统一验收；本报告不宣称全产品100%。

原三项和恢复结束后，本机/远端private集群全部退出（remote exit0、两侧children=[]），六卡显存均0MiB。三个owner路径内的原run数据库、封存/提交/缓冲文件按有限规模完整归档到 `/tmp/demiflow-physical-hosts-20261116/gpu-shared-archive` 后，仅删除这三个本任务共享测试目录；没有触及其他路径。七件冻结文件与既定基线逐字节一致；主仓未修改、未测试。
