# 函数状态跨停车与恢复：修复及验收

本次修复真实 P1：同一个 Dataset 在 `workers=1` 触发持久增量停车时，函数每次重新加载后丢失闭包计数；`workers=4` 常驻时却正确。原 8 行结果分别为 `[1,1,1,1,1,1,1,1]` 与 `[1,2,3,4,5,6,7,8]`。这不是逻辑 task 边界：原图仅一个 source split，两个 AsyncMapOp，无 task_batch_rows。原 F7c 因计数永远不到 5 而没有触发真实崩溃。

修复同时覆盖同根的 `functools.partial`、显式 `fn_args`/`fn_kwargs` 及字段绑定。显式绑定的独立红基线为 3 行、1 worker `[1,1,1]` 对比 4 workers `[1,2,3]`。这些旧红证据均保留。

## 实现与契约

- `compile_kernel_spec` 是函数状态的唯一分类入口：只读 module/type/Path/不可变参数不增加状态预算；实际拥有的可变 JSON cell、default、kwdefault、attribute、partial 参数和显式调用绑定进入 adapter。普通 task 不再有另一段覆盖状态分类的旧逻辑。
- 原 OperatorKernel 持有每个 occurrence/slot 的独立函数状态。克隆代码和只读依赖，复制经准入的 owned 数据，保持空 cell，修正 self 引用；不修改 LoadedProgram 蓝图或 driver 原声明，不因 cloudpickle 按名称返回共享函数而污染两个节点。
- `function_slots_v1` 存入原 kernel image、原 cut、原 RunJournal。没有新增状态账本、提交者或重新计算的计划。旧合法 `task_function` 镜像及读取分支保留；新分类不重写历史封存图。
- 普通 task 继续按原 `batch_rows`/frontier 重建；停车发生在一个 task 中间时恢复该 task 的已有状态。显式 stage/worker 语义不会变成 task 语义。
- 递归检查嵌套 code 对外层 cell/global 的修改；未知对象、锁、共享可变 alias/cycle、未适配的外部 global 不伪称可恢复。不能停车时在业务前明确拒绝，足够资源常驻的原可运行形态仍可执行。原 image 中非可恢复标记不会被旧 task JSON 格式掩盖。
- 数据快照按原 max_state_bytes 准入，保留原严格 JSON 和结构边界；依赖分类与复制不执行自定义容器钩子。只复制函数显式调用参数，未把 class constructor 配置重复当 actor state。thread_attempt 的共享活对象规则未变。

## 历史测试迁移依据

F9 保留真实工作台 prepare/start/show、固定 Lance 输入、3 条原中文业务结果、源码字节不变。末尾将已退役 chain `runtime_sources *-code-pin-*` 查询换为实际 v3 `*-dataflow-sealed`、原 code_pin_ref/program code_identity 对应、原 ACK 和 action 完成事实；不拿 mock 或单独 pickle 回环代替工作台运行。

F7c 保留原 8 行、checkpoint_rows=2/interval=60000、counter 到第 5 次 SIGKILL 条件。仅将宿主设为 process，防止新真实 thread provider 把 pytest driver 本身杀掉。两臂实测均先有真实原 ACK（输出前缀 4 行）再 SIGKILL；只修改 default/kwdefault 引用的 helper，原 RunRef 恢复被拒；原 seal 与 ACK 不变。精确既有错误是首跑 `UNKNOWN_EFFECT: worker connection closed during execution`，恢复 `INVALID_CONTRACT: Loaded program code differs from the sealed pin`；本次没有为测试更改错误词汇。

原 task 恢复 4→2 worker 负臂预算、30 秒上限、零新增业务调用、原 ACK 不变及回到 4 workers 的成功恢复全部保留。只更新错误文本定位：固定旧基线 5ff 的 `DataflowExecution._configure_phases` 已先调用 `pack_finite_phases`，`dataflow_residency.py:88`（原 0d12223）会在不可容纳 cohort 时先抛 `ready cohort exceeds admitted host or resident capacity`，早于旧测试所匹配的 active-worker 分支。不是放宽资源拒绝。

## 验收与版本边界

下列轮次均在本隔离树；完整命令、进程内 import 路径、运行 HEAD、每个 nodeid、XML 和前后全部输入摘要见旁边 JSON 指向的原始证据。所有轮次输入摘要无变化。

- `/tmp/demiflow-regression-function-final-evidence/verification.json`：72 passed / 0 failed，352.01s；290 条真实 allocation，状态 {'RELEASED': 290}。
- `/tmp/demiflow-regression-function-final2-evidence/verification.json`：56 passed / 1 failed，337.01s；282 条真实 allocation，状态 {'RELEASED': 282}。
- `/tmp/demiflow-regression-function-final3-evidence/verification.json`：13 passed / 0 failed，62.87s；50 条真实 allocation，状态 {'RELEASED': 50}。

第一轮 72 全绿涵盖原 code-pin 全文件、dependency classification 14 项、新函数状态、原 task 全组与受影响 stream 生命周期。第二轮加入显式绑定和独立分类后，56 个真实/组件场景通过，唯一失败为新增 unit fixture 将普通 FakeServices 错当 dataclass；已修测试端口构造并在第三轮验证。该组件测试明确使用 fake store/journal 端口，只证明原 kernel/DTO/codec 的不可恢复标记；原生 driver/worker/RunJournal 证据由其他真实场景提供。

第三轮为最终源字节：13 全绿，包括原两条 F7c SIGKILL/代码漂移拒绝、单 worker 实际停车、实际字段绑定、同一 importable 函数双 occurrence 隔离、只读依赖不加 state 预算、alias/空 cell/嵌套状态/非可恢复 image 和无用户容器钩子执行。第二轮另有 cell 与显式 bound 参数两种真实原 ACK 后中断→同 RunRef 恢复正证据，已提交前缀不重执；所有原 stream 共享线程、partial、class、启动失败、兄弟调用排空、外部取消、cleanup 错误正例保持通过。

不能把前轮 72 项或更早 98 API 矩阵重新标为最终字节全量复跑。本轮未重跑全 Local/Ray 矩阵、真实多机/GPU；由总控按改动影响和已有证据安排。自动状态适配只承诺上述有界 JSON 槽位，不承诺任意 Python 对象或构造器外部副作用 exactly-once。

## 对两大意图的作用

平台 I01/I07：改变 worker 数量或临时停车不再改变函数可观察状态；已 ACK 前缀和函数状态由同一个原提交恢复。I08/I13：状态预算、未知状态拒绝及历史 code-pin 守卫保留。产品 I12：真实工作台首跑仍执行原封存程序，实际业务结果与原 seal/ACK 可核查。本批没有用流程计数或移除测试替代能力证据。

主仓未修改或运行测试；七个冻结承重文件与 49b666f 逐字一致；没有 push、主仓合并、共享环境安装或生产服务操作。
