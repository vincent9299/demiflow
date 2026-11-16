# Lance 核心闭环（2026-11-16）

只在 `gpt_v3_artifact_recovery_20261115` / `engine/gpt-v3-artifact-recovery-20261115` 修改、测试。主仓、运行服务和冻结七件未动。机器证据见 `LANCE-CLOSURE-R10.json`，它引用完整原始三阶段收据与 SHA256，明确实际源码摘要，未把 collection 当通过。

## 已闭环

1. **业务列序与 typed Schema**：提交 e59f3b4 保留 typed map 的 entry 插入序，旧排序编码仍原样读取/再编码；semantic/record 身份公式与历史工件未改。两种顺序各自稳定往返，tuple/bytes/±0.0 保真，Local 与真实 Ray Core 写 Lance 保留列序及 field/schema metadata。原 codec 及 Local 原 schema/partial-update 用例 176 passed；真实 Ray 1 passed，无 `ray.data`。
2. **部分列更新的性能与数据语义**：无变换、无 filter/projection 的原 LanceScan 使用已封存 staged Arrow 的精确行数传入原 `_prepare_merge`。不在 driver 重扫源，也不启动旧 DataFusion 独立执行；原源封存、原 grant、原 sink、原 CommitCoordinator/ACK 和版本 CAS 保持。有限范围（limit）、稀疏和完整范围 patch 恢复 `rewrite_columns`，旧宽 payload 文件/fragment 保留；filtered、unknown、insert/ignore、全行 patch 仍按原规则 `auto`。消费行数与封存精确值不符时提交前拒绝。
3. **真实 prepare/commit 与资源负例**：竞态注入到实际 `_prepare_merge` 完成后，再追加竞争版本；原 `_commit_merge` 严格拒绝，竞争版本完整保留。资源测试使用真实 v3 native RSS 策略，明确 `BUDGET_EXCEEDED`、无 commit/ACK、目标版本和值不变；它证明采样 RSS 准入，未声称这是 OS 硬隔离。
4. **键过滤历史诊断迁移**：原顺序/范围/重复/null/固定版本/批次边界断言全留；真实 worker probe 证明 key scan 仅键与 row_id，join/sort 不混宽 payload/irrelevant，hydrate 来自原固定版本、准确行数且 batch<=2。提前关闭检查原资源表全 `CONFIRMED`；真实 join 失败不运行下游 payload、无 hydrate。缺失列仍在源封存前由精确的 `InvalidLanceRequest` 拒绝。
5. **未声明字段**：原显式 Schema 禁止静默丢字段，测试匹配当前准确错误且断言未产生版本。

## 验收边界

- merge/partial 全套原规模：33 passed / 1 failed，334.73 秒。原 100,010 行真实溢写、最后一行重复、复合 binary key、原版本、nested null、CAS 与丢回执均通过。唯一失败是迁移测试吞掉预期异常后触发 RunContext 正确收尾保护；随后只调整测试捕获范围。
- 定向收口：10 passed，72.06 秒，包含上述 RSS 反例、原 key-filter 全套与 nested-schema 全套。没有重跑已绿大集合。
- 不同源码快照的通过不能冒充“当前 HEAD 全套通过”；证据 JSON 保留每份输入摘要和完整命令，所有实际通过均需 setup/call/teardown 三阶段通过、无 xfail/rerun/deselect。
- 首次 r9 命令误写不存在的测试文件，0 项执行；未计为通过，原错误回执仍保留。

## 仍未完成

**Arrow 纯复制仍逐行 Python 化，是实际性能/能力缺口**。两条旧 copy 测试不能靠删除 `_apply_plan` spy 算通过。`ARROW-CARRIER-REUSE-REVIEW.md` 给出复用 Record.payload_ref/schema_ref/source_position 的候选、当前 ref 字节计量硬约束和需要统一裁决的最小版本化 MIME 扩展；尚未实施。

本 lane 已复核但未执行的 safe selectors 仍有 **1735**；root 所有的 public API、SourceRunView、CSV 与 KV 修复还需合并版本按因果验收。本报告不宣称全部历史测试、工作台或引擎已经 100%。
