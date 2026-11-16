# Ray 转换、恢复与提前关闭：5ff5da9 定向闭环

**本次受影响的全部 17 条真实 Ray 回归通过，原先两个失败原因已闭合。**
完整 API 矩阵仍引用 `ab1b334` 的 98/98 API、168/168 case 证据；没有把它写成
在本次新 HEAD 再跑了一遍。

候选 `5ff5da9e9cad3d595d84212b44c1cbd787e08dc4`，本树测试 HEAD
`d58f293f84bd2c3d454aa7efd5cdff8d0f39f17d`，`demiflow/`、`tests/` 与候选
逐字一致。相对原完整矩阵候选，生产仅 `chain._classify_dependency_module`
14 行变化；新增 160 行分类测试，修正新 Ray seam 的 6 行私有字段断言。
其余引擎、终端、传输源码逐字未变，因此按总控决定只复验受影响边界。

| 固定源码验证 | 结果 |
|---|---|
| 原 13 条公共转换回归，真实 Ray Core | **13 passed / 115.72s** |
| 仓内 4 条 Ray terminal seams | **4 passed / 30.20s** |
| 无 monkeypatch 的 source code-pin：私有环境 | NumPy、Pandas、math、原 `_wide_rows` 全成功 |
| 同一纯 probe：原普通环境 | 同四条全成功；原正常 manifest 逐项不变 |

## 失败的真正闭环

前版六条转换失败在业务 source 的 code-pin 阶段误把合法安装 symlink 的第三方库
当作业务依赖，未到转换就耗尽 64 模块上限。本次不扩大上限、不降低保护，只在
真实 sourcefilename 和 `__file__` 两个词法路径均位于当前明确 sysconfig 安装根时
判为已安装第三方，保留原 resolved-path 检查。私有环境下 numpy/pandas/pyarrow/pytest
分类均为 None；每个纯 probe 仍有且仅有一个真实业务模块。
两环境四条 probe 的依赖 manifest 与前版普通环境逐项相同，未略过业务模块。

真实 Ray 六条负/正转换链路现在均完整执行，原 `max_state_bytes=16 KiB`、
`max_batch_bytes=32 KiB` 负例和正例预算均未改变；负例在 payload constructor
之前由原转换准入拒绝，正例保留全值和三种返回格式。typed null 与六类原生 layout
也再次通过，schema/metadata/Pandas 等原业务断言保持。

前版一个 seam 失败来自本路新增测试误查 live 对象专用 `_verified_completed_image`。
总控已改为校验统一恢复路径的 `_restore_image.cut.final`、对应原 journal ACK。
20×6 KiB 全结果一致、恢复禁止新 worker、整个 commit 列表不变等原断言没有删除。
本轮真实 Ray 恢复通过；其余三个 case 再次验证普通迭代器提前关闭保持原 20 秒、
非 final 的真实 source prefix、无残留 credit/worker，以及 take_all limit=0/3
完整 exact 与 overflow 语义，没有静默截断。

## 证据与版本边界

- 实际私有 Ray Core 2.60.0；每组 owned cluster 使用原 `address='local'`，RayData
  blocker 完整保留；两个进程结束时 Ray 均已 shutdown，子进程列表为空。
- **1785** 项 Python/plan、**2103** 项全部已跟踪源码/测试输入前后无变化，工作树
  在运行前后均干净，七承重字节保持原值。没有主仓或共享环境写入。
- 旧 `ab1b334` 的全部 43 份原始材料摘要复核无变化，原六红/一红均保留，不覆盖证据。
- 原始日志/XML/runner：`/tmp/demiflow-ray-{conversion,seams}-final-5ff5da9.*`；
  每组 `-evidence/verification.json` 保存导入、命令、环境和前后摘要。
  纯探针在 `/tmp/demiflow-ray-pin-5ff5da9/`；顺序执行记录为
  `/tmp/demiflow-ray-targeted-5ff5da9-launch.json`。
- 本报告 JSON 绑定全部精确节点、输入、材料 SHA256 与版本差异。

本次没有重新跑 98/168 大矩阵，也没有跨机器/GPU/生产数据库验收。最终结论是：
`ab1b334` 的完整同机 Ray API 证明，加上 `5ff5da9` 唯一生产分类变化及相关终端
边界的独立定向全绿；不是不同版本证据未经说明的混加。
