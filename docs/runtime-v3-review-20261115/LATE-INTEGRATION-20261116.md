# 原生输入、结果语义与产品接线的本轮收口

2026-11-16 23:15（Asia/Singapore）。本阶段集成代码 `760d85c`，隔离树
`/yzp/zhaozy/yangzepeng/0905/_demiflow/worktrees/gpt_runtime_v3_review_20261115`，
分支 `engine/gpt-runtime-v3-review-20261115`。下表证据分别绑定其实际运行版本，
不合计为同一 HEAD 的全库通过，也不宣称 I08 性能通过。

## 对照两大原始意图

产品仍要求看懂、配置、运行真实 pipeline，并分析、比较、评测数据；引擎仍要求
同一 Dataset、统一 Local/Ray Core、有限资源、唯一提交恢复权威和完整业务能力。
本轮补齐输入声明、worker 输入回调、真实封存写出及用户可见的列顺序；没有用
删除旧业务断言、关闭 code-pin 或放宽资源限额消掉失败。

| 变化 | 用户实际得到的行为 | 验证 |
|---|---|---|
| CSV 原生选项与回调 | 原 Arrow Read/Parse/ConvertOptions、gzip/TSV、类型、空串/null 与重复关联生效；局部/partial 回调的业务依赖同时固定与归档 | owner `f28e0ac` Local 54 项、真实 Ray 1 项；root `7b0e72a` 组合验证 |
| 输入回调的原 run 身份 | source 的打开、拉取、关闭可读取原 `current_run().ref`，失败仍清理并保留原错误；没有授予 driver journal、提交或新建 action 权限 | `3f348e0` 原宽行关联与 process/thread 生命周期 8 项；后续真实 Ray 正反例 |
| 有界收集 | 直接输入调用 `take_all(limit=n)` 最多读取第 n+1 个溢出探针就失败；过滤前的输入不会被错误截断 | `df6125f` 66 项，包含 0/2/65 上限、跨检查点、原测试、过滤正例及清理 |
| 参数与检查器 | `iter_batches` 的错误格式立即拒绝，不等首次迭代；已退休 checkpoint facade 在检查器中准确报 invalid | 同上；单独早拒绝 3 项在 `c4f7104` 通过 |
| 列顺序与类型 | 无显式 schema 的 Python 行保留原字段顺序；tuple/bytes/-0.0 保真；原排序 map 字节仍读取并原样重编码 | owner `e59f3b4` Local 176、真实 Ray 1；root `760d85c` 3 项 |
| 工作台真实接线 | 浏览器读到封存图及真实身份桥，停止/恢复沿原运行管理；不重算假执行图 | `f0ff14c` 真实浏览器 1 项、实际管理链路随 Local 组合通过 |

字段顺序修复只修改 typed map 的 entry 数组编码顺序。没有修改记录 ID 或语义
身份公式，没有重写旧工件、历史 journal 或旧 payload。CSV 描述仅新增此前
无法完整表达的原生选项；普通旧 options 摘要和历史 prompt 大 enum 身份不变。

CSV 的输入 block 在 `open_csv` 前准入，实际保留的 Arrow buffer 在 Python
转换前检查，旧批次在读下一批前释放。Arrow parser/解压的内部临时分配没有
被平台硬 allocator 接管，不能把这些检查叫作进程 RSS 的硬保证。

## 根节点亲验与保留的失败

- `f0ff14c`：Local 组合 12 passed/1 skipped，API 矩阵实际覆盖 98 API、168 case，
  没有 failed/not_run；另包含原取消恢复、终端驻留/flush 限额。浏览器首次因缺
  可执行路径跳过，随后显式 Chromium 1 passed/94.77 秒，跳过没有冒充通过。
- 同版本 Ray 全矩阵 1 failed/1 passed。三项原 case 的根因是 `env-ray` 缺少
  `babel`/`mwparserfromhell`。owner 用同生产源码、私有依赖环境定向通过三项；
  生产没有为环境缺包更改行为。原失败完整保留。
- `3f348e0`：原输入上下文组合 8 passed/18.19 秒；worker/chain 组合
  24 passed/1 failed，发现异常清理测试将真实 reader 转交另一 executor 时缺
  上下文。修复已进入 `4992077`，原失败在 `df6125f` 的 66 项中复验通过。
- `df6125f`：66 passed/70.22 秒，无跳过，包含完整 Dataset API 合约、原直接
  超读反例、终端边界、失败关闭以及退休检查器。
- `7b0e72a`：Local CSV、写出封存/代码漂移与上下文组合
  **33 passed/173.59 秒**。真实 Ray CSV 恢复、三个原可选 API 场景、输入身份/
  超读正反例及一个报告守卫 **6 passed/45.96 秒**；其中报告守卫不是业务运行。
- `760d85c`：新旧 map 字节、类型保真、真实无 schema/有 schema Lance 写出
  **3 passed/7.20 秒**。它补充 owner 原 codec 全集及真实 Ray 证据。

根节点 Ray 组合第一轮 5 passed/1 failed：私有 venv 仅用 `.pth` 指向共享
依赖，pytest assertion-rewrite 被原严格 code-pin 视为安装目录外的业务依赖，
超出原 64 模块限额。没有放宽守卫。仅在该私有 venv 建立只读包链接，使导入
路径归属于该 venv 的安装目录，再以相同生产/测试提交复跑上述 6 项。
共享 `env`/`env-ray` 均未修改；原失败及 619 条只读链接清单保留在证据中。

## 证据及隔离

[本轮证据](LATE-INTEGRATION-20261116.json) 保存根节点 11 份原回执的摘要、
命令/资源域、逐 setup/call/teardown 结果及按提交去重的完整输入摘要。
所有根节点运行均前后 HEAD/源码一致，import 明确指向隔离树，零遗留子进程。

独立逐字核验了 Ray 可选 API 2,215 输入、性能比较守卫 2,198 输入、CSV 两份
各 2,202 输入、列序两份各 2,904 输入与历史迁移 2,903 输入，均与各自交付
commit 一致。历史迁移那份原回执仍是 12 passed/1 failed；其中直接超读红例
由根节点实际修复后通过，不能改写原回执为绿。

主仓仍为 `e40a6dd`，原 dirty 状态与展开的 18 份文件摘要一致；七个冻结件
逐字与原基线一致。本轮无主仓写入/测试、push、合并或共享环境安装。

## 尚未闭环

1. SQLiteKV 原写入适配缺 prepare/commit/inspect/abort，及通用 Datasink 回调
   接线：独立候选正在做真实中断恢复和容量反例，尚未根节点验收。
2. Lance partial merge 的 `rewrite_columns` 选择、宽列文件复用与真实 CAS/
   资源负例：独立候选正在完成原十万行用例及回归。
3. 原纯 Lance copy 的 Arrow batch 直通：当前仍逐行转 Python，是真实性能
   回退。不能只删除旧内部 spy 就算完成；需在统一载体、credit、游标、GC 和
   原 ACK 内修复，不增加 driver 旁路。
4. 高性能 I08：普通轻 CPU 链仍有逐行包装/验证及大量小状态持久化成本。
   同 cut 无状态 image 打包候选正在验证，不改变原 40 ACK/预算。原九类、
   五轮正式矩阵没有通过；同文件系统比较守卫的通过不等于引擎性能达标。
5. 历史安全回归仍有 1,735 个未跑 nodeid；已有跨版本通过并集不等于最新版本
   全套通过。Notebook/Case 的整体产品验收也不能由一个浏览器图测试替代。
