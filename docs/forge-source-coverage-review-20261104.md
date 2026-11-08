# F-01/F-02 交付 Review（2026-11-04）

当前结论：**返修终审通过（2026-11-04 21:01，Asia/Singapore）**。两个已复现问题由原负责人完成修复、验证、合入与制品更新；主会话审阅实际差异并独立复跑关键场景，确认满足原验收条件。未改变历史标准、任务基线或业务授权。下面保留首次审查和裁决，末节记录返修验收。

审核对象为 `/tmp/forge-source-coverage-implementation-20261104-sl8h0a3t/final-verified-source` 与正式源码。独立审查记录的 12 项文件 SHA 一致；主会话另核对 9 个合入目标、9 份冻结标准、最终 wheel/sdist 实际摘要和 8 个业务控制文件。120 项回归通过是有效证据，但未覆盖下述问题。

## 首次审查发现（现已关闭）

1. **P1：已知本地导入不能因路径受保护或超限而转成外部依赖。**
   `coverage.py` 的 `local_evidence()` 第 335–340 行，在检查已登记的本地根之前，对受保护段或超过 64 段直接返回 `False`。`import orders.venv.missing`、`from orders.__pycache__ import missing`、`orders` 下 65 段导入均在 `start → 读取全部 context → check` 中得到 `passed=True`、无 violations、coverage complete。主会话重新运行独立 fixture 确认。
   应先确定已有本地根/前缀证据，再将受限或超限本地路径明确诊断为非通过；保持裸外部 `venv` 等未声明外部库的原边界。不得为修复遍历受保护目录或扩大预算。

2. **P2：readonly 快照需要保留属于其范围的明确来源。**
   `project.py` 的 `readonly()` 第 502–508 行固定使用 `exact=set()`。当 `sources.settings.path="platform/runs/settings.json"`、`readonly=["platform"]` 且目录只含该配置时，tree 有完整配置摘要，readonly 却只有 `platform/` 标记，并以 `platform/runs` 未覆盖阻断检查。主会话复现与设计第 88 行不符。
   将已声明且属于 readonly 范围的合法精确来源传入同次快照，保持只读保护、身份一致性与预算；不展开未声明兄弟历史/数据目录。验证配置变化可被 readonly 门禁发现，保留 watch/readonly 重叠首次身份检查。

复现脚本：`/tmp/forge-source-independent-review-20261104/probe.py`；正常路径对照：同目录 `controls.py`。脚本中动态导出用例不属于本轮要求放宽的行为。

## 对独立审查另一项建议的裁决

不采纳“存在真实子模块就应在动态导出拒绝之前放行”的建议。包级 `__getattr__` 可以在 `from package import clean` 中返回其他对象，即使 `clean.py` 存在；子模块存在不足以确定实际名称来源。主会话自有 Python fixture 验证：`__getattr__('clean')` 返回 `42`，同目录的 `clean.py` 并未导入。

维持已批准的保守边界：无法静态确定的动态导出/星号绑定保持非通过，不为此次返修引入候选执行或完整动态解释器。若日后能静态证明具体绑定来源，可另按事实完善，不能简单交换分支顺序。

## 返修验收

将两个复现纳入真正的生命周期测试，并核对普通外部导入、正常本地符号/子模块、旧标准分派、只读来源和兄弟目录不遍历。修复后复验受影响用例、development 完整环境/stdlib、独立 wheel 及打包摘要；原标准、业务登记、baseline 和历史记录不变。更新实施报告与最终证据索引，旧失败/旧产物保留为过程记录。

总体继续采用按实际标准分派、单次 SourceFacts/Observation、统一快照与覆盖门禁、refresh 保留 baseline、verify 重新取证和版本 binding。无需另建配置协议、执行器或检查体系。

## 返修终审结果

主会话审阅 `coverage.py`、`project.py` 和覆盖测试相对返修前的实际差异。本地身份先由已登记根或有界安全前缀确定，受保护/超深路径在探测前明确拒绝；readonly 与 tree 共享同次取证的精确来源，按真实根投影范围内的标签。投影数量、元数据和时间有界，未放宽兄弟历史目录或动态导出边界。

独立复核证据位于 `/tmp/forge-source-final-review-20261104-vg8i5u9i/`：

- 从首次独立 `probe.py` 复制，仅替换源码和临时目录路径，针对当前正式代码重跑。三类本地受限/超深导入的完整 check 均非通过；readonly 配置具有完整摘要；动态导出继续非通过。
- 独立重跑新增 8 项针对性测试，全部通过（2.725 秒），包括完整生命周期、普通外部 venv、禁止保护目录读取、readonly 修改后拒绝验证且保留原基线、兄弟历史不遍历、不同根标签与首次身份冲突。
- 实际核对 4 个交付目标、1,000 个正式包文件与构建快照；9 份原标准、427 个历史文件和 8 个业务控制文件摘要均一致。wheel 的 25 个 development 文件/资源与正式源码一致，wheel/sdist 的实际大小、SHA 及冻结标准一致。
- 审阅负责人最终日志：完整环境 **128/128 通过**，stdlib **124 通过/4 条件跳过**，独立 wheel **107 项通过**（包含于完整环境验证），原生 catalogue **24 项通过**。上述计数分别说明验证范围，不相加为去重总数。

当前制品与负责人证据见[返修交付报告](forge-source-coverage-implementation-20261104.md#终审返修交付2026-11-04当前版本)，根目录 `/tmp/forge-source-coverage-review-fix-20261104-1a9eob1a/`。本次验收关闭 F-01/F-02 的实现与交付问题；旧任务仍按原始事实读取，不重基线、不追认真实业务认证或生产执行。
