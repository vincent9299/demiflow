# Artifact staging 崩溃恢复交付

工作树 `gpt_v3_artifact_recovery_20261115`，分支 `engine/gpt-v3-artifact-recovery-20261115`；基线 `49b666f`，已接父提交 `2c5b391`（本树 cherry-pick 为 `4185701`）。H 两处夹具协议适配单独提交 `1eebd37`，该文件已交回总控。主仓、共享环境、在运服务未操作。

## 与两大意图的关系

引擎：同一个 RunJournal 仍是额度、对象与恢复的唯一账本；新 driver 不能把自己的 fence 或 runtime.lock 当作旧 worker 已退出的证明。产品：已提交和历史引用的对象不删不改；恢复只补原事实的结算，绝不从目录制造执行成功或可恢复事实。Local 与 Ray Core 使用同一工件协议。

## 实际变更

- `RunArtifactStore.put`：先以 `O_EXCL|O_NOFOLLOW` 创建物理 owner lease 并持有 `flock`，再登记原 journal 的 owner inode 与初始额度。数据 FD 在释放 lease 之前关闭；固定 `.pending-<reservation>-data` 路径不需要扫描目录。
- `ArtifactCommand.begin_staging`：`operation_id` 是 reservation，`owner` 是 store_id，`ref=None`，`size_bytes` 为初始准入。authority 验证锁已被持有，原 journal 在同一 fenced transaction 内登记 ownership+quota；重试不能改 inode/store/初始额度，也不能给 legacy reservation 补造 ownership。
- `ArtifactCommand.prepare_staging`：`owner` 是 reservation，`ref` 是即将发布的真实 digest/size/store/media_type；先持久登记不可变 publication intent，再允许 hardlink。未扩 DTO 字段，codec/allowlist 同步。
- `recover_staging`：由 `DriverArtifactPort` 构造接线。仅枚举原 journal 未结算 owner，每页最多128项、默认每轮1024项（硬上限4096），核验字节总量不超过 `Bounds.max_spill_bytes`。原 journal 保存轮询游标，避免活 owner 永远挡住后面的孤儿；不在每次 put 扫目录。
- 必须取得**原登记 inode** 的独占锁才回收。锁仍占用、丢失、被替换、路径 symlink、旧记录无 owner 证据都不退款。未发布 pending 先删除并 fsync，再退款；已发布对象逐字节验证 digest/size 后补唯一结算，不删除对象。损坏对象明确拒绝并保留 charge。
- native settlement 保留原幂等校验；有 pending 文件时不得结算退款，有已发布对象时不得 release。已结算后变更 settlement 仍抛原 `RecoveryContractError`。明确的初始准入事务拒绝清理本次空 lease；未知回执保留物理证据。
- `_file_digest` 使用 regular-file / O_NOFOLLOW 验证且至多读取 ref 声明大小+1，避免恢复哈希遇到增长文件无限读取。

## 验收

完整命令都在本树执行，显式 `PYTHONPATH=$PWD:$PWD/tests`，资源根和 pytest basetemp 只在 `/tmp/demiflow-*`。进程崩溃子测试另断言实际 demiflow import 属于当前工作树。

1. `env/bin/python -B -m pytest -q tests/test_runtime_v3_artifact_accounting.py tests/test_runtime_v3_artifact_recovery.py tests/test_runtime_v3_real_seams.py tests/test_runtime_v3_public_execution.py tests/test_runtime_v3_native_execution.py --basetemp=/tmp/demiflow-artifact-stage4 --junitxml=/tmp/demiflow-artifact-stage4.xml`：81 passed、2 failed。两项已交总控：旧 real_seams 要求单 stage 加载全图程序；旧 native test 把原生 `UncertainPromptCall` 写成了 `DataflowError`。失败没有新增 HTTP 或退款，未改这些测试断言掩盖。
2. 在最后补充“初始 quota 拒绝不得泄露空 lease”后，`env-ray/bin/python -B -m pytest -q tests/test_runtime_v3_ray_native.py::test_ray_prompt_and_relational_chain tests/test_runtime_v3_artifact_accounting.py tests/test_runtime_v3_artifact_recovery.py --basetemp=/tmp/demiflow-artifact-ray --junitxml=/tmp/demiflow-artifact-ray.xml`：**20 passed in 73.44s**。
3. 其中13项新增恢复测试：真实 `os._exit` 五个断点×四次 crash/resume；活旧 worker 持锁/打开数据FD 跨 driver fence；锁替换/丢失；历史 pin 保留和对象去重；旧无 owner 记录与损坏发布对象拒绝退款；begin/prepare 幂等；有界公平扫描；禁止 pending/已发布对象提前退款；初始准入拒绝不泄露空文件。既有6项 artifact accounting 原断言保留（创建失败注入仅从 tempfile.mkstemp 改到固定路径 `_create_staging`）。
4. 真实 Ray Core prompt→sort→take、两次原生模型 HTTP、原生提交至 acked 通过；测试显式封堵 Ray Data 入口。只证明同机 Ray Core，未冒充多机文件系统故障验收。
5. 七个冻结文件逐字节等于 `49b666f`。输入与 JUnit SHA256 见 `ARTIFACT-RECOVERY-EVIDENCE.json`。

## 明确边界

- shared store 必须提供一致的 POSIX flock/hardlink/fsync。丢锁文件或不同挂载导致 inode 证据不一致时保守保留，不用 PID/mtime/fence 猜测物理退出。
- 旧版本没有 owner/publication 记录的 reservation 不自动退款；也不会绕过平台历史 code pin 做迁移。
- 已发布但尚未被后续 manifest 引用的对象仍保留且按唯一实际字节收费。重复相同发布不重复计费；真正增加的不同对象仍受磁盘额度限制。当前没有未引用 published-object 删除器，避免破坏历史、finite_seals、manifest_ref 等根。
- 已完成结算后、删除空 lease 文件前的极窄崩溃窗口可能保留一个空元数据文件；它不保留 staging bytes，也不重复计费。此处不通过全目录扫描删除无归属文件。
