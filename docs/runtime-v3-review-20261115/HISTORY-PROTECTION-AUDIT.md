# 历史原值与恢复兼容报告复核

本批修复的是历史保护报告的诚实性，不是宣告当前 v3 已能继续执行所有旧 checkpoint。隔离分支 `engine/gpt-v3-regression-20261115`，测试基线 `73304ccecf90ca7a23ff36999e52e312d73c8aff` 加本批 diff。主仓未修改、未测试、未 push。

## 已确认并修复

`dataflow_history.validate_resume_compatibility` 的原生历史分支只依据旧物理计划中 recovery 字段判断能力，没有检查 `restore_image`，也没有证明旧平台与当前 runtime 相容。其原接口即使收到可读取的程序/计划与一个描述性 image，也没有可验证的运行时适配依据。独立单元保留这些文件真实存在的正前提，原实现会错误地返回支持；修复后分别报告缺少原 journal 确认的镜像、缺少经过验证的当前 runtime adapter。原引用与旧代码身份不变。

当前原生 chain/graph 的 K6 manifest 在 commit 之后才写入 `StateArtifacts`，引用仅留在 `self.checkpoints`，没有进入该提交的 `prepared.state`。当前历史 loader 只接受原 ACK 可达的 K6 manifest，所以实际旧运行的 `restore_image=None` 是正确结果。不能扫描文件并把找到的描述冒充已提交状态。

另一个必须区分的格式事实：旧 K1 `business.entrypoints[].definition_ref.sha256` 是业务 code-pin 的语义摘要，`location` 是归档源码位置；它不是该 `.py` 字节的散列。当前只读绑定保留此原值，严格内容 store 无法将其验证为内容寻址的程序。修复没有重算或改写旧 definition_ref，也没有放宽 store 摘要检查。报错改为“缺失或无法作为内容寻址程序验证”，不把归档文件存在冒充恢复证明。

## 实际旧版本对照

固定旧提交：`b4dd41722130be3898a8a98057541d843777906e`。测试从本地 Git 提取该提交的原始 demiflow 包到测试私有 `/tmp`，1247 个文件、33,536,134 字节；不下载、不安装、不向仓库提交旧包。提取前后均有文件数/字节上限，业务 runner 与全部原生运行目录也在私有临时目录。

实际载体是 **`demiflow.sealed_plan_facts.v1` 与 `aligned-barrier-v2` 协议**。这是一个固定旧 engine revision 的两个格式/协议名字，不能称为“两个旧引擎版本都已验”。

- 原旧环境实际执行两行 Dataset → map → write_lance；完成结果版本 1、两行原值。
- 另一路在原 CommitCoordinator `after_acked` 故障点中断：真实原 ACK 已存在、action 未完成。
- 当前实现只读原 journal、sealed plan、原 native receipt 和固定 Lance v1，保持 dag_id 与所有历史文件摘要不变。
- 当前 `RunContext.resume` 对这份旧历史明确拒绝，理由是需要原 engine revision；拒绝不创建新 attempt、不替换 pickle。
- 同一个原环境、同一个 RunRef 恢复中断运行；原 commit、原 seal、原请求身份和全部旧归档字节保持，Lance 仍只有版本 1，结果仍为两行。

可复现测试：`tests/test_runtime_v3_history_protection.py::test_original_native_history_read_refusal_and_original_runtime_resume`。原 package 哈希、完整子进程命令、实际 import 路径、前后文件摘要与原始原生事实都在测试的 `history/history-evidence.json`。

## Prompt 原值黄金

`tests/fixtures/runtime_v3_history/prompt-identities.json` 中 1024 / 1025 / 1500 三组值来自上述旧提交的真实 `parse_prompt_pack → operation_facts → plan_identity → plan_code_pin`，不是用当前公式构造期望。保留原 prompt 文本与 SHA-256、生成来源和四个语义域的原摘要（native、operation、action、code operations）。当前实际解析逐项比对；篡改一个黄金摘要的反例必须失败。

原 `test_prompt_long_enum_preserves_historical_native_bytes[1/1024/1025/1500]` 四项也未经修改复跑通过。这里不要求两个不同平台实现的完整 platform revision/code pin 相等，只冻结原业务语义身份。

## 验收与剩余能力

- `/tmp/demiflow-regression-history-r1.xml`：10 passed，11.96 秒（本批五项、W002、原四个 enum 反例）。
- `/tmp/demiflow-regression-golden-r1.xml`：7 passed，85.57 秒（最终本批五项、W002、真实工作台停止/恢复）。1766 个源码输入前后摘要相同，固定 HEAD；完整记录在 `/tmp/demiflow-regression-golden-r1-evidence/verification.json`。

**V029 仍未整体完成。** 已验证原历史只读、原环境续算、当前不兼容拒绝和真实 Prompt 原身份；当前 v3 的旧原生中间状态执行 adapter 未实现/未证明。其它未列出的旧载体族不因上述测试而被推定支持。正式 route 不回填常量、不缩小承诺，继续保留 NotRun；“有专项证据”与“全门完成”分别陈述。

C12 明确允许必须使用原环境恢复的历史，因此“不跨 revision 续算”本身不是违反原意图，也不意味着必须新增任意旧状态转译。当前列出的 `checkpoint_manifest.v1` translator 只有描述转换能力；已查到的原 chain/graph producer 没有把这种 manifest 放进该 loader 所要求的原 ACK 可达引用。本批不声称存在已验证的当前 v3 原生历史续算适配域。正式门保留状态，是为了先明确并接入实际支持域，不为取得绿色扩写不可靠 adapter。
