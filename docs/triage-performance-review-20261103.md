# 真实 triage 性能实现与验收

证据目录：`/yzp/zhaozy/yangzepeng/0905/_demiflow/triage-performance-20261103`。
设计见 [triage-performance-design-20261103.md](triage-performance-design-20261103.md)。

## 最终实现与边界

- `services`：独立请求使用已有有界 GPU lanes。周期原生校验不再独占所有 lanes；
  捕获、淘汰、释放仍独占。发现差异立即禁用图，排空读者后释放，返回原生结果。
- `services/capture_kernels.py` 与 `pointwise_cuda.py`：Qwen3.5 图捕获候选内核。
  仅融合逐元素运算，保留原生归约、激活和 BF16 舍入；原生参考函数在捕获外恢复。
  带 bias/不兼容 dtype 或布局的卷积保留原生；外部已安装卷积内核不替换。
  没有八种长度白名单，没有跨请求 batch，没有修改模型参数/精度/prompt。
- `execution/location_index.py`：大 artifact 重定位 manifest 使用有界可重建点查
  索引，避免每个 journal 进程完整解析 785 MB JSON。业务层和 journal 语义不变。
- `prepare.py`：新增内核纳入 adapter 源码身份，旧 prepared manifest 不会静默
  复用。运行环境、权重、请求逻辑身份分别核验。

没有增加业务调度器、结果缓存或后台无效 GPU 工作。`streams` 支持 1–4，2 是
当前配置的默认值。已对 1/2/3/4 做同输入精确对照；更多 streams 未形成足够稳定
的端到端优势，本次保留原配置及 journal 身份，未把在途数当成真实 forward 数。

## 同机真实请求对照

gpu32520，单卡 H100，原共享模型环境只读。200 个不同的真实 journal 请求，完整
保留 state 字符串、criteria 顺序与 usage。下表使用相同的 8 在途请求、2 GPU lanes；
HTTP 包含实际 forward，Dataset 包含 journal 与 checkpoint 的初始化和提交。

| 指标 | pplx 旧版 | pplx 新版 | AMOS 旧版 | AMOS 新版 |
|---|---:|---:|---:|---:|
| HTTP 吞吐，条/秒 | 20.17 | 23.98 | 21.61 | 23.98 |
| HTTP p95，毫秒 | 618 | 373 | 461 | 385 |
| Dataset 200 行，秒 | 18.99 | 9.00 | 19.56 | 8.95 |
| Dataset p95，毫秒 | 501 | 443 | 707 | 440 |

短阶段包含少量冷形状，不能把两个 200 行阶段的差值外推成固定加速比。另做每模型
2,000 次持续请求（200 种真实输入循环，每次都实际 forward）：pplx 23.79 条/秒，
AMOS 23.88 条/秒，完整 model/answers/usage 全部精确一致。

该持续窗口排除首尾各 1 秒，1 秒采样一次：AMOS **82/82 个样本为 100%**；pplx
平均 **98.87%**、51/82 个样本为 100%、最低 82%。pplx 窗口仍有 2 次新图捕获，
保留此低谷，不把平均值四舍五入宣传为持续 100%。证据分别为
`amos-e2e-candidate/`、`pplx-e2e-final/` 和 `steady-summary.json`。

GPU busy 不代表理论算力利用率，也不代表任意输入永远 100%；本次吞吐和输出
一起验收。两模型持续窗口的图回放比例约 99.9%，0 质量差异、0 淘汰，图缓存约
2.1 GiB，Torch allocator 峰值分别约 50.8/53.9 GiB，仍受 4/64 GiB 上限约束。

## 质量与资源 review

1. 官方 causal-conv1d 融合 SiLU 改变中间舍入，pplx 147/200 概率变化、1 标签
   变化；AMOS 116/200 概率变化、2 标签变化。**拒绝上线该路径**，未安装到共享环境。
2. 新内核保留原生参考。先捕获精确比较，继续每 64 次原生校验，未降低频率和
   精度门槛。两 lane 同时校验、差异后等待其他读者退出再释放均有并发回归。
3. 长度 1–8192、不同布局/批维/通道/卷积宽度：468 组卷积检查（234 组无 bias
   候选、234 组有 bias 原生回退），160 组归一化检查逐位一致；GPU 峰值 22 MiB。
   最初带 bias 测试失败的证据保留，最终修复见 `pointwise-gpu-exact-v2.json`。
4. 两模型每个 4/8 在途阶段各 200 行 Dataset 精确验证，按行 journal 均 200 请求、
   200 响应、0 uncertain；只读重放全部 reused。不存在用重放结果计 GPU 吞吐。
5. CPU 广泛回归 **214 passed**；最终针对性回归 **60 passed**，包括捕获作用域
   恢复、并发校验、索引语义/预算/失效/损坏/文件句柄。日志分别为
   `final-cpu-tests.log` 和 `final-targeted-tests.log`。
6. 索引真实源验证 229 个分散记录一致，源文件身份不变。一次构建 14.08 秒、
   845 MiB；同机只读重放从 13.08 秒降至 0.579 秒。缓存属于可删除的派生物，
   不是对生产 JSON 的迁移。预算与超限行为见设计文档。
7. D3 共用图执行路径：200 个不同请求先跑独立原生参考，再执行 3 × 600 次并发
   请求，1,028 次内部精确校验、33 次图淘汰，零差异/错误。图缓存 3.81 GiB，
   allocator 峰值 53.35 GiB，见 `d3-graph-regression.json`。这是质量、并发和生命周期
   回归，校验频率特意设为每 3 次，不把该压力测试当成 D3 最佳吞吐基准。

最终代码 review 核对了：独立原生基线、gate 的独占/共享边界、部分安装异常恢复、
不兼容内核回退、缓存与磁盘上限、部署身份、原 journal 请求身份和未提交改动。
无独立 reviewer 参与；本节为实现者复审与可复现测试证据。

## 生产恢复记录

用户授权暂停后，按精确 PID 身份停止原两个生产者及其服务；原 checkpoint partial
与完成响应保留。仅通过平台恢复 API 重排 7 条 pplx、6 条 AMOS 未完成预约，保留
恢复事件且不返还请求额度，见 `production-pause.json`、`production-requeue.json`。
两个任务已使用保存的原 argv 恢复，PID 为 pplx 53446、AMOS 53447，见
`production-resume.json`。原 manifest 已备份后重建；运行环境、权重签名和
model_contract 与原配置一致，最终 adapter 为
`cc78b77a66f32c7729a739841034180fd2ff06a9dbdc05cce5012b6c617c8496`。
先重放旧 journal，不重复已完成的模型请求。实际恢复后只读核对 249 条 pplx、
250 条 AMOS 原完成记录，request/response/error 逐字未变；7/6 条未完成预约
已重新完成。新 checkpoint 中的历史记录标为 reused，证据为
`production-replay-proof.json` 和 `production_steady_clean.json`。

恢复后的真实跑批持续观测（不注入测试请求）：

| 65 秒窗口 | pplx / GPU 0 | AMOS / GPU 1 |
|---|---:|---:|
| 平均 GPU 利用率 | 98.65% | 99.62% |
| 最低 / 最高 | 97% / 100% | 97% / 100% |
| 100% 的 1 秒采样点 | 12/65 | 49/65 |
| 实际模型请求吞吐 | 23.91 条/秒 | 23.73 条/秒 |
| 新捕获 / 淘汰 | 0 / 0 | 0 / 0 |
| 窗口内原生精确校验 | 22 | 22 |

此窗口将 499 条历史记录点查核对移到计时之外，期间只取模型计数、journal 顶部
和单行预算状态。吞吐按 `Δhits + Δnative - Δchecks` 计算，不重复计算校验。
结束后的累计内部校验分别为 218/200，差异和失败均为 0；journal 持续前进。
原始 CSV、每阶段计数和核对结果保存在 `production_steady_clean.*`。

前两个观察窗口也保留：第一个仍有 1/2 次新捕获，AMOS 最低曾到 24%；第二个
无捕获，两者平均 98.61%/99.61%。这两个窗口含历史记录点查 I/O，不能用于声称
平台的性能上限。**AMOS 的独立真实请求对照已证明持续 100% 能达到；整个生产
跑批、两个模型每一秒均为 100% 尚未达到。** 不关闭校验、填充无效计算或忽略
低谷来满足指标；也不把本轮结果宣称为所有输入、所有未来模型的固定性能保证。

230 个既有脏文件中，本任务修改了 5 个原有文件，其余 225 个内容哈希未变，
包括 `systemone.py` 图片支持。新增文件、artifact resolver 的改动及本轮补丁分别
记录在 `implementation-sha256.json`、`implementation.diff` 和
`dirty-preservation.json`；未 commit/push，未修改共享环境或业务源码。

## 全量跑批完成

两个恢复后的任务均已结束，所属模型服务已退出并释放 GPU。直接打开最终 Lance
产物复核：pplx、AMOS 各 297,506 行，error 非空数和 label 为空数均为 0，最终
checkpoint 均存在。证据为 `production-completed.json` 和
`production-pplx-resumed.log`、`production-amos-resumed.log`。

输出在业务运行目录 `demiwtg/shared/concepts/identity/runs/identity_gap_triage_20261101/`
下的 `pplx_full.lance`、`amos_full.lance`。恢复后的执行耗时包含历史 journal 重放，
不能用 297,506 除以该耗时冒充模型吞吐。性能收益以同条件对照及上述实际新请求
窗口为准；200 行端到端约 2.1 倍的提升包含启动成本，不外推为长跑批整体翻倍。
