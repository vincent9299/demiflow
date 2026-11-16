# 工作台图片与文档 Embedding 的原链路接入

针对 I09/I12：工作台的图片和文档 Embedding 现在显式声明对象读取资源，并复用原生 Embedding、原模型请求日志、Dataset 提交和固定 Lance 结果。未增加模型循环、权限账本或恢复权威。

`options.object_read={root, relocations, layout}` 复用原 `collect.flow_contract` 的根目录声明、`workspace.resolve_flow_resources` 的 location 校验，以及 `objects.local_object_uri/read_local_object` 的路径/SHA/分配前字节限制。静态检查与实际模型读取同时使用该声明。图片保持原解码/像素限制；文档保持原文档 SHA、span、text_sha256 和有界缓存。没有此参数时原 native API 及默认 options 字节不变；受管图片/文档仍明确拒绝未授权读取。

prepare 需要绑定原 `embeddings` extra，图像还声明实际 `pillow` 依赖，沿原平台环境快照验证。没有绕过平台依赖检查。准入估算计入 prepare 并发、图像解码或文档解析与缓存；它不冒充 RSS 硬上限，原进程资源保护保留。

## 实测及版本

- `3697e04`：13 passed / 35.84 秒。实际注册位置→CheckedExecution→原生 HTTP→RunJournal confirmed/requests_consumed→原 ACK→Lance 与身份桥；含 image/document 正例、未声明/未登记拒绝、外部路径/符号链接/网络/SHA/大小拒绝、明确 relocation 和旧 options SHA 黄金值。
- `94dac3e`：4 passed / 127.94 秒。两模态均实际经过 manager.prepare/start、封存受管进程、原执行图/物理计划/桥、原 ACK、固定 DatasetRef 和 Lance 结果；另两例在 prepare 后改对象，均在 HTTP 和输出发布前失败。外部模型 HTTP 是确定性 transport fixture；静态检查、运行、读对象、提交、结果与工作台事实读取没有替身。
- `41dcdf0` 加强腐败对象反例：从 prepare 的实际 report actions 核查无成功 ACK，避免引用较早静态报告的 action ID。两模态反例与旧默认 options 黄金值共3项通过；同组另外2项旧独立SQLite缓存API失败，总结果3 passed / 2 failed，69.13秒。两旧例在原native journal互斥守卫处拒绝，另列修复，未删除选项或改变原断言。此组不能称全绿。

原回执：`/tmp/demiflow-managed-embedding-objects-r2-evidence/verification.json`、`/tmp/demiflow-managed-embedding-objects-r5-evidence/verification.json`；均保留实际代码/测试摘要、HEAD、环境、原 XML 摘要，前后源码未变。

总控逐项与实际Git提交核对、源前后/HEAD一致及XML摘要复核见 [OBJECTS-RAY-PRIMARY-VERIFICATION.json](OBJECTS-RAY-PRIMARY-VERIFICATION.json)，包含上述部分失败组，未用owner结论替代取证。

探索红回执全部保留：r1 的测试从错误 report 层读取 resources；r3 的新 workspace 测试夹具未建立 tables catalog；r4 未绑定 embeddings extra，被原环境准入正确拒绝。这些不是隐藏通过，也没有为消红移除运行守卫。r1 中其余原 Embedding 回归实际 22 passed；与后续集合重叠，不相加。

本项证明受管运行与原事实接通，不声称另做了 Embedding 专属浏览器交互、远端真实模型权重验收或任意 URI 支持。产品整体 GPU/最新完整组合和引擎性能仍单列。
