# 集成后旧分路门迁移与源 pending 修复

当前独立树 `engine/gpt-v3-regression-20261115` 在 `49b666f` 跑完整 19 lane：412 passed /147 failed，90.08 秒。已接入父提交 `2c5b391`，本批最终 F/G/P/Q/S/U/W/Y 共 **216 passed，0 failed，57.97 秒**。进程内断言导入路径属于本树，capacity 和全部测试临时数据均在 `/tmp/demiflow-*`。完整命令、逐文件摘要和日志在 `REGRESSION-LANE-1.json`。本批不是宣告全部 19 lane 已通过。

## 真实缺陷及修复

`QueueReader.next_record` 覆写基类方法时忽略恢复的 `_replay_pending`，可能把已封存、未确认记录丢掉并重新请求 claim。现先重放封存的完整记录，恢复其 delivery provenance，并核对 cursor 的 lease reference；不发起新 poll。`ProducerReader` 对这条重放保留已经封存的 record_id、sequence、producer offset/cursor，不再重读或推进 producer descriptor。

S044 新增恢复后记录与 cursor 一致、poll 总次数仍为 1 的断言；新 producer 反例在删除原 descriptor 后从 pending artifact 重放，验证身份、cursor、delivery ref 不漂移。这里恢复的是数据与出处，不授予新的 native claim；跨 attempt 的 claim 有效性仍由 driver 准入负责。

## 测试契约迁移依据

- F/Y：分路时以 sys.modules 挂入不存在模块的假实现现已失效。改为 test-scoped monkeypatch 实际 facade 调用点，保留同一公开 terminal 向统一引擎委托、禁止 Ray import/Ray Data 的断言；不把单元假宿主计为真实 Ray 通过。
- G/P：使用实际闭合 DTO、合法 SHA 标识和完整 InputPort；类工厂的状态归属为 auto_json。take(0) 验证空源声明且去掉原 transform，并以真实 Dataset/from_iter 验证源函数完全不被调用。
- Q：实际 RuntimePolicy/WorkerAssignment、稳定 SHA 操作 ID（重试不二次 hash），仍跑真实本地 HTTP 与 SQLite acquisition/finalize；保持错误 reply、取消、unknown 等反例。
- S：Record 的 row 为 mapping，tuple/bytes/-0.0 放列值内，保留类型和负零；seek pending 现在是含完整 provenance 的文档，不是旧裸 payload ref，新增真实 reader 恢复断言。JSON reader 与 FileRecords reader 的形状分别保持。
- U：按主控裁决，不同 executor 对象的输入是声明，终端选 provider。保留两输入 source/plan 身份，退出 facade recording double 后实际 Local 执行 union 得到两侧结果；非法非 Dataset 输入继续拒绝。
- W：真实 SHA、实际 `.dataflow-store/<domain>/objects` 路径；只在明确读取单元边界 patch sealed loader；JavaScript harness 改为 v3 按 occurrence 选择节点、v1 按 logical id 选择，维持真实计划/历史格式诚实显示断言。

未改主仓、承重七件、提交协议或生产校验，未 skip/delete 原有测试、未放宽资源/身份断言。B/E/H 旧夹具和产品较大历史回归继续处理，不能将本批 216 项作为全产品交付完成度。
