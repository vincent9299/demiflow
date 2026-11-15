# 并行编码与文件所有权

推荐 **19 个实现会话，加 Qwen 总控和最终 GPT/Qwen Review**。这是 589 个函数/方法/类型叶任务的文件装配分组，不是开 589 个会话。每个叶可以独立编码；同文件只有一个装配 owner。

所有 lane 同时从设计最终提交建隔离分支，按冻结接口写代码，不能等待供应方内部实现后才设计消费者。单元测试的 spec fake 仅在测试域；完整集成才要求所有实现到位。

| lane | 职责 | 叶数 | 文件数 | 完成交付 |
|---|---|---:|---:|---|
| C | 共享类型、codec、身份、错误 | 88 | 3 | 全部指定符号、反例、完整回执 |
| G | 真实图捕获与算子注册 | 13 | 4 | 全部指定符号、反例、完整回执 |
| P | 物理计划与资源可行性 | 9 | 2 | 全部指定符号、反例、完整回执 |
| S | 增量来源与游标 | 50 | 2 | 全部指定符号、反例、完整回执 |
| K | 算子与关系/排序/聚合内核 | 22 | 5 | 全部指定符号、反例、完整回执 |
| X | 通道、路由、credit、GC | 19 | 3 | 全部指定符号、反例、完整回执 |
| B | 同一worker运行时 | 20 | 3 | 全部指定符号、反例、完整回执 |
| E | 驱动编排和公平调度 | 19 | 3 | 全部指定符号、反例、完整回执 |
| D | 程序封存、cut和唯一提交 | 17 | 3 | 全部指定符号、反例、完整回执 |
| T | 终端、迭代器和物化句柄 | 15 | 2 | 全部指定符号、反例、完整回执 |
| M | 模型、状态、缓存和原生journal适配 | 27 | 4 | 全部指定符号、反例、完整回执 |
| H | 多机宿主、控制通道、工件和driver权限 | 79 | 8 | 全部指定符号、反例、完整回执 |
| Q | 原生获取/队列结算服务 | 9 | 1 | 全部指定符号、反例、完整回执 |
| U | Dataset公开入口迁移 | 55 | 1 | 全部指定符号、反例、完整回执 |
| L | Local执行器变薄 | 32 | 1 | 全部指定符号、反例、完整回执 |
| Y | Ray Core入口及Ray Data退出 | 37 | 1 | 全部指定符号、反例、完整回执 |
| F | Pipeline/data与旧调用兼容接线 | 21 | 10 | 全部指定符号、反例、完整回执 |
| W | 工作台真实图与历史 | 21 | 9 | 全部指定符号、反例、完整回执 |
| V | 集成验收与测试域基础 | 36 | 3 | 全部指定符号、反例、完整回执 |

## 每个文件一个装配任务

下面文件任务包含其全部函数叶卡。同一文件的 imports/类声明骨架/注册表由该文件 owner 装配，不分给第二lane；不能用新增未登记helper绕过权限。测试文件由对应lane单独持有，V只拥有集成测试和root fixture。

| 文件 | owner | 叶任务 |
|---|---|---|
| `demiflow/data/api.py` | F | [F007](tasks/F007.md), [F008](tasks/F008.md), [F009](tasks/F009.md), [F010](tasks/F010.md), [F011](tasks/F011.md) |
| `demiflow/data/dataflow_operation.py` | G | [G013](tasks/G013.md) |
| `demiflow/data/dataset.py` | U | [U001](tasks/U001.md), [U002](tasks/U002.md), [U003](tasks/U003.md), [U004](tasks/U004.md), [U005](tasks/U005.md), [U006](tasks/U006.md), [U007](tasks/U007.md), [U008](tasks/U008.md), [U009](tasks/U009.md), [U010](tasks/U010.md), [U011](tasks/U011.md), [U012](tasks/U012.md), [U013](tasks/U013.md), [U014](tasks/U014.md), [U015](tasks/U015.md), [U016](tasks/U016.md), [U017](tasks/U017.md), [U018](tasks/U018.md), [U019](tasks/U019.md), [U020](tasks/U020.md), [U021](tasks/U021.md), [U022](tasks/U022.md), [U023](tasks/U023.md), [U024](tasks/U024.md), [U025](tasks/U025.md), [U026](tasks/U026.md), [U027](tasks/U027.md), [U028](tasks/U028.md), [U029](tasks/U029.md), [U030](tasks/U030.md), [U031](tasks/U031.md), [U032](tasks/U032.md), [U033](tasks/U033.md), [U034](tasks/U034.md), [U035](tasks/U035.md), [U036](tasks/U036.md), [U037](tasks/U037.md), [U038](tasks/U038.md), [U039](tasks/U039.md), [U040](tasks/U040.md), [U041](tasks/U041.md), [U042](tasks/U042.md), [U043](tasks/U043.md), [U044](tasks/U044.md), [U045](tasks/U045.md), [U046](tasks/U046.md), [U047](tasks/U047.md), [U048](tasks/U048.md), [U049](tasks/U049.md), [U050](tasks/U050.md), [U051](tasks/U051.md), [U052](tasks/U052.md), [U053](tasks/U053.md), [U054](tasks/U054.md), [U055](tasks/U055.md) |
| `demiflow/execution/chain.py` | F | [F013](tasks/F013.md), [F014](tasks/F014.md) |
| `demiflow/execution/checked.py` | W | [W005](tasks/W005.md), [W006](tasks/W006.md) |
| `demiflow/execution/contracts/dataflow.py` | C | [C007](tasks/C007.md), [C008](tasks/C008.md), [C009](tasks/C009.md), [C010](tasks/C010.md), [C011](tasks/C011.md), [C012](tasks/C012.md), [C013](tasks/C013.md), [C014](tasks/C014.md), [C015](tasks/C015.md), [C016](tasks/C016.md), [C017](tasks/C017.md), [C018](tasks/C018.md), [C019](tasks/C019.md), [C020](tasks/C020.md), [C021](tasks/C021.md), [C022](tasks/C022.md), [C023](tasks/C023.md), [C024](tasks/C024.md), [C025](tasks/C025.md), [C026](tasks/C026.md), [C027](tasks/C027.md), [C028](tasks/C028.md), [C029](tasks/C029.md), [C030](tasks/C030.md), [C031](tasks/C031.md), [C032](tasks/C032.md), [C033](tasks/C033.md), [C034](tasks/C034.md), [C035](tasks/C035.md), [C036](tasks/C036.md), [C037](tasks/C037.md), [C038](tasks/C038.md), [C039](tasks/C039.md), [C040](tasks/C040.md), [C041](tasks/C041.md), [C042](tasks/C042.md), [C043](tasks/C043.md), [C044](tasks/C044.md), [C045](tasks/C045.md), [C046](tasks/C046.md), [C047](tasks/C047.md), [C048](tasks/C048.md), [C049](tasks/C049.md), [C050](tasks/C050.md), [C051](tasks/C051.md), [C052](tasks/C052.md), [C053](tasks/C053.md), [C054](tasks/C054.md), [C055](tasks/C055.md), [C056](tasks/C056.md), [C057](tasks/C057.md), [C058](tasks/C058.md), [C059](tasks/C059.md), [C060](tasks/C060.md), [C061](tasks/C061.md), [C062](tasks/C062.md), [C063](tasks/C063.md), [C064](tasks/C064.md), [C065](tasks/C065.md), [C066](tasks/C066.md), [C067](tasks/C067.md), [C068](tasks/C068.md), [C069](tasks/C069.md), [C070](tasks/C070.md), [C071](tasks/C071.md), [C072](tasks/C072.md), [C073](tasks/C073.md), [C074](tasks/C074.md), [C076](tasks/C076.md), [C077](tasks/C077.md), [C078](tasks/C078.md), [C079](tasks/C079.md), [C080](tasks/C080.md), [C081](tasks/C081.md), [C082](tasks/C082.md), [C083](tasks/C083.md), [C084](tasks/C084.md), [C085](tasks/C085.md), [C086](tasks/C086.md), [C087](tasks/C087.md), [C088](tasks/C088.md) |
| `demiflow/execution/contracts/dataflow_codec.py` | C | [C001](tasks/C001.md), [C002](tasks/C002.md), [C003](tasks/C003.md), [C004](tasks/C004.md), [C005](tasks/C005.md) |
| `demiflow/execution/contracts/dataflow_errors.py` | C | [C006](tasks/C006.md), [C075](tasks/C075.md) |
| `demiflow/execution/dataflow.py` | E | [E001](tasks/E001.md), [E002](tasks/E002.md), [E003](tasks/E003.md), [E004](tasks/E004.md), [E005](tasks/E005.md), [E006](tasks/E006.md), [E007](tasks/E007.md), [E008](tasks/E008.md) |
| `demiflow/execution/dataflow_api.py` | E | [E015](tasks/E015.md), [E016](tasks/E016.md), [E017](tasks/E017.md), [E018](tasks/E018.md), [E019](tasks/E019.md) |
| `demiflow/execution/dataflow_artifacts.py` | H | [H011](tasks/H011.md), [H012](tasks/H012.md), [H013](tasks/H013.md), [H014](tasks/H014.md) |
| `demiflow/execution/dataflow_authority.py` | H | [H001](tasks/H001.md), [H002](tasks/H002.md), [H003](tasks/H003.md), [H004](tasks/H004.md), [H005](tasks/H005.md), [H006](tasks/H006.md), [H007](tasks/H007.md), [H008](tasks/H008.md), [H009](tasks/H009.md), [H010](tasks/H010.md), [H036](tasks/H036.md), [H037](tasks/H037.md), [H056](tasks/H056.md), [H057](tasks/H057.md), [H066](tasks/H066.md), [H067](tasks/H067.md), [H068](tasks/H068.md), [H069](tasks/H069.md), [H070](tasks/H070.md), [H071](tasks/H071.md), [H072](tasks/H072.md), [H073](tasks/H073.md), [H074](tasks/H074.md), [H075](tasks/H075.md) |
| `demiflow/execution/dataflow_checkpoint.py` | D | [D001](tasks/D001.md), [D002](tasks/D002.md), [D003](tasks/D003.md), [D004](tasks/D004.md), [D005](tasks/D005.md), [D006](tasks/D006.md), [D007](tasks/D007.md) |
| `demiflow/execution/dataflow_control.py` | H | [H045](tasks/H045.md), [H046](tasks/H046.md), [H047](tasks/H047.md), [H048](tasks/H048.md) |
| `demiflow/execution/dataflow_handles.py` | T | [T008](tasks/T008.md), [T009](tasks/T009.md), [T010](tasks/T010.md), [T011](tasks/T011.md), [T012](tasks/T012.md), [T013](tasks/T013.md), [T014](tasks/T014.md), [T015](tasks/T015.md) |
| `demiflow/execution/dataflow_history.py` | W | [W001](tasks/W001.md), [W002](tasks/W002.md), [W003](tasks/W003.md), [W004](tasks/W004.md) |
| `demiflow/execution/dataflow_hosts.py` | H | [H015](tasks/H015.md), [H016](tasks/H016.md), [H017](tasks/H017.md), [H018](tasks/H018.md), [H019](tasks/H019.md), [H020](tasks/H020.md), [H021](tasks/H021.md), [H022](tasks/H022.md), [H023](tasks/H023.md), [H024](tasks/H024.md), [H025](tasks/H025.md), [H028](tasks/H028.md), [H029](tasks/H029.md), [H054](tasks/H054.md), [H055](tasks/H055.md), [H058](tasks/H058.md), [H059](tasks/H059.md), [H060](tasks/H060.md), [H061](tasks/H061.md), [H062](tasks/H062.md), [H063](tasks/H063.md), [H064](tasks/H064.md), [H065](tasks/H065.md) |
| `demiflow/execution/dataflow_inputs.py` | F | [F001](tasks/F001.md), [F002](tasks/F002.md), [F003](tasks/F003.md), [F004](tasks/F004.md), [F005](tasks/F005.md) |
| `demiflow/execution/dataflow_model_journal.py` | M | [M008](tasks/M008.md), [M009](tasks/M009.md), [M010](tasks/M010.md), [M011](tasks/M011.md), [M012](tasks/M012.md), [M013](tasks/M013.md), [M014](tasks/M014.md), [M015](tasks/M015.md), [M016](tasks/M016.md), [M017](tasks/M017.md), [M018](tasks/M018.md), [M019](tasks/M019.md), [M020](tasks/M020.md), [M021](tasks/M021.md), [M022](tasks/M022.md), [M023](tasks/M023.md), [M024](tasks/M024.md) |
| `demiflow/execution/dataflow_models.py` | M | [M001](tasks/M001.md), [M002](tasks/M002.md), [M003](tasks/M003.md), [M004](tasks/M004.md), [M005](tasks/M005.md), [M006](tasks/M006.md), [M007](tasks/M007.md) |
| `demiflow/execution/dataflow_queue.py` | Q | [Q001](tasks/Q001.md), [Q002](tasks/Q002.md), [Q003](tasks/Q003.md), [Q004](tasks/Q004.md), [Q005](tasks/Q005.md), [Q006](tasks/Q006.md), [Q007](tasks/Q007.md), [Q008](tasks/Q008.md), [Q009](tasks/Q009.md) |
| `demiflow/execution/dataflow_scheduler.py` | E | [E009](tasks/E009.md), [E010](tasks/E010.md), [E011](tasks/E011.md), [E012](tasks/E012.md), [E013](tasks/E013.md), [E014](tasks/E014.md) |
| `demiflow/execution/dataflow_seal.py` | D | [D012](tasks/D012.md), [D013](tasks/D013.md), [D014](tasks/D014.md), [D015](tasks/D015.md), [D016](tasks/D016.md), [D017](tasks/D017.md) |
| `demiflow/execution/dataflow_services.py` | H | [H026](tasks/H026.md), [H027](tasks/H027.md) |
| `demiflow/execution/dataflow_sinks.py` | D | [D008](tasks/D008.md), [D009](tasks/D009.md), [D010](tasks/D010.md), [D011](tasks/D011.md) |
| `demiflow/execution/dataflow_sources.py` | S | [S001](tasks/S001.md), [S002](tasks/S002.md), [S003](tasks/S003.md), [S004](tasks/S004.md), [S005](tasks/S005.md) |
| `demiflow/execution/dataflow_store.py` | H | [H030](tasks/H030.md), [H031](tasks/H031.md), [H032](tasks/H032.md), [H033](tasks/H033.md), [H034](tasks/H034.md), [H035](tasks/H035.md) |
| `demiflow/execution/dataflow_terminals.py` | T | [T001](tasks/T001.md), [T002](tasks/T002.md), [T003](tasks/T003.md), [T004](tasks/T004.md), [T005](tasks/T005.md), [T006](tasks/T006.md), [T007](tasks/T007.md) |
| `demiflow/execution/dataset_native.py` | F | [F018](tasks/F018.md), [F019](tasks/F019.md) |
| `demiflow/execution/driver.py` | F | [F012](tasks/F012.md) |
| `demiflow/execution/exchange/dataflow_bridge.py` | X | [X012](tasks/X012.md), [X013](tasks/X013.md), [X014](tasks/X014.md), [X015](tasks/X015.md) |
| `demiflow/execution/exchange/dataflow_channels.py` | X | [X001](tasks/X001.md), [X002](tasks/X002.md), [X003](tasks/X003.md), [X004](tasks/X004.md), [X005](tasks/X005.md), [X006](tasks/X006.md), [X007](tasks/X007.md), [X008](tasks/X008.md), [X009](tasks/X009.md), [X010](tasks/X010.md), [X011](tasks/X011.md) |
| `demiflow/execution/exchange/dataflow_state.py` | X | [X016](tasks/X016.md), [X017](tasks/X017.md), [X018](tasks/X018.md), [X019](tasks/X019.md) |
| `demiflow/execution/executors/local.py` | L | [L001](tasks/L001.md), [L002](tasks/L002.md), [L003](tasks/L003.md), [L004](tasks/L004.md), [L005](tasks/L005.md), [L006](tasks/L006.md), [L007](tasks/L007.md), [L008](tasks/L008.md), [L009](tasks/L009.md), [L010](tasks/L010.md), [L011](tasks/L011.md), [L012](tasks/L012.md), [L013](tasks/L013.md), [L014](tasks/L014.md), [L015](tasks/L015.md), [L016](tasks/L016.md), [L017](tasks/L017.md), [L018](tasks/L018.md), [L019](tasks/L019.md), [L020](tasks/L020.md), [L021](tasks/L021.md), [L022](tasks/L022.md), [L023](tasks/L023.md), [L024](tasks/L024.md), [L025](tasks/L025.md), [L026](tasks/L026.md), [L027](tasks/L027.md), [L028](tasks/L028.md), [L029](tasks/L029.md), [L030](tasks/L030.md), [L031](tasks/L031.md), [L032](tasks/L032.md) |
| `demiflow/execution/executors/ray.py` | Y | [Y001](tasks/Y001.md), [Y002](tasks/Y002.md), [Y003](tasks/Y003.md), [Y004](tasks/Y004.md), [Y005](tasks/Y005.md), [Y006](tasks/Y006.md), [Y007](tasks/Y007.md), [Y008](tasks/Y008.md), [Y009](tasks/Y009.md), [Y010](tasks/Y010.md), [Y011](tasks/Y011.md), [Y012](tasks/Y012.md), [Y013](tasks/Y013.md), [Y014](tasks/Y014.md), [Y015](tasks/Y015.md), [Y016](tasks/Y016.md), [Y017](tasks/Y017.md), [Y018](tasks/Y018.md), [Y019](tasks/Y019.md), [Y020](tasks/Y020.md), [Y021](tasks/Y021.md), [Y022](tasks/Y022.md), [Y023](tasks/Y023.md), [Y024](tasks/Y024.md), [Y025](tasks/Y025.md), [Y026](tasks/Y026.md), [Y027](tasks/Y027.md), [Y028](tasks/Y028.md), [Y029](tasks/Y029.md), [Y030](tasks/Y030.md), [Y031](tasks/Y031.md), [Y032](tasks/Y032.md), [Y033](tasks/Y033.md), [Y034](tasks/Y034.md), [Y035](tasks/Y035.md), [Y036](tasks/Y036.md), [Y037](tasks/Y037.md) |
| `demiflow/execution/graph.py` | F | [F015](tasks/F015.md), [F016](tasks/F016.md) |
| `demiflow/execution/kernels/aggregate.py` | K | [K013](tasks/K013.md), [K014](tasks/K014.md), [K015](tasks/K015.md) |
| `demiflow/execution/kernels/dataflow.py` | K | [K001](tasks/K001.md), [K002](tasks/K002.md), [K003](tasks/K003.md), [K004](tasks/K004.md) |
| `demiflow/execution/kernels/operator.py` | K | [K016](tasks/K016.md), [K017](tasks/K017.md), [K018](tasks/K018.md), [K019](tasks/K019.md), [K020](tasks/K020.md), [K021](tasks/K021.md), [K022](tasks/K022.md) |
| `demiflow/execution/kernels/ordering.py` | K | [K009](tasks/K009.md), [K010](tasks/K010.md), [K011](tasks/K011.md), [K012](tasks/K012.md) |
| `demiflow/execution/kernels/relational.py` | K | [K005](tasks/K005.md), [K006](tasks/K006.md), [K007](tasks/K007.md), [K008](tasks/K008.md) |
| `demiflow/execution/local_kernel.py` | F | [F006](tasks/F006.md) |
| `demiflow/execution/plan_facts.py` | G | [G011](tasks/G011.md), [G012](tasks/G012.md) |
| `demiflow/execution/queue_chain.py` | F | [F017](tasks/F017.md) |
| `demiflow/execution/run_context.py` | W | [W013](tasks/W013.md) |
| `demiflow/execution/run_journal.py` | H | [H076](tasks/H076.md), [H077](tasks/H077.md), [H078](tasks/H078.md), [H079](tasks/H079.md) |
| `demiflow/execution/sealed_plans.py` | W | [W007](tasks/W007.md), [W008](tasks/W008.md) |
| `demiflow/execution/source_readers.py` | S | [S006](tasks/S006.md), [S007](tasks/S007.md), [S008](tasks/S008.md), [S009](tasks/S009.md), [S010](tasks/S010.md), [S011](tasks/S011.md), [S012](tasks/S012.md), [S013](tasks/S013.md), [S014](tasks/S014.md), [S015](tasks/S015.md), [S016](tasks/S016.md), [S017](tasks/S017.md), [S018](tasks/S018.md), [S019](tasks/S019.md), [S020](tasks/S020.md), [S021](tasks/S021.md), [S022](tasks/S022.md), [S023](tasks/S023.md), [S024](tasks/S024.md), [S025](tasks/S025.md), [S026](tasks/S026.md), [S027](tasks/S027.md), [S028](tasks/S028.md), [S029](tasks/S029.md), [S030](tasks/S030.md), [S031](tasks/S031.md), [S032](tasks/S032.md), [S033](tasks/S033.md), [S034](tasks/S034.md), [S035](tasks/S035.md), [S036](tasks/S036.md), [S037](tasks/S037.md), [S038](tasks/S038.md), [S039](tasks/S039.md), [S040](tasks/S040.md), [S041](tasks/S041.md), [S042](tasks/S042.md), [S043](tasks/S043.md), [S044](tasks/S044.md), [S045](tasks/S045.md), [S046](tasks/S046.md), [S047](tasks/S047.md), [S048](tasks/S048.md), [S049](tasks/S049.md), [S050](tasks/S050.md) |
| `demiflow/execution/source_snapshot.py` | F | [F021](tasks/F021.md) |
| `demiflow/execution/stream.py` | F | [F020](tasks/F020.md) |
| `demiflow/execution/worker/bootstrap.py` | B | [B019](tasks/B019.md), [B020](tasks/B020.md) |
| `demiflow/execution/worker/dataflow_execution.py` | B | [B001](tasks/B001.md), [B002](tasks/B002.md), [B003](tasks/B003.md), [B004](tasks/B004.md), [B005](tasks/B005.md), [B006](tasks/B006.md), [B007](tasks/B007.md), [B008](tasks/B008.md), [B009](tasks/B009.md), [B010](tasks/B010.md) |
| `demiflow/execution/worker/dataflow_host.py` | H | [H038](tasks/H038.md), [H039](tasks/H039.md), [H040](tasks/H040.md), [H041](tasks/H041.md), [H042](tasks/H042.md), [H043](tasks/H043.md), [H044](tasks/H044.md), [H049](tasks/H049.md), [H050](tasks/H050.md), [H051](tasks/H051.md), [H052](tasks/H052.md), [H053](tasks/H053.md) |
| `demiflow/execution/worker/runtime.py` | B | [B011](tasks/B011.md), [B012](tasks/B012.md), [B013](tasks/B013.md), [B014](tasks/B014.md), [B015](tasks/B015.md), [B016](tasks/B016.md), [B017](tasks/B017.md), [B018](tasks/B018.md) |
| `demiflow/operator_llm/client.py` | M | [M025](tasks/M025.md), [M026](tasks/M026.md) |
| `demiflow/operator_llm/runtime.py` | M | [M027](tasks/M027.md) |
| `demiflow/planning/dataflow_compile.py` | G | [G001](tasks/G001.md), [G002](tasks/G002.md), [G003](tasks/G003.md), [G004](tasks/G004.md), [G005](tasks/G005.md), [G006](tasks/G006.md), [G010](tasks/G010.md) |
| `demiflow/planning/dataflow_place.py` | P | [P001](tasks/P001.md), [P002](tasks/P002.md), [P003](tasks/P003.md), [P004](tasks/P004.md), [P005](tasks/P005.md), [P006](tasks/P006.md), [P007](tasks/P007.md) |
| `demiflow/planning/dataflow_policy.py` | P | [P008](tasks/P008.md), [P009](tasks/P009.md) |
| `demiflow/planning/dataflow_registry.py` | G | [G007](tasks/G007.md), [G008](tasks/G008.md), [G009](tasks/G009.md) |
| `demiflow/workbench/graph/model.py` | W | [W015](tasks/W015.md), [W016](tasks/W016.md), [W017](tasks/W017.md) |
| `demiflow/workbench/runs/facts.py` | W | [W009](tasks/W009.md), [W010](tasks/W010.md), [W011](tasks/W011.md), [W012](tasks/W012.md) |
| `demiflow/workbench/runs/manager.py` | W | [W014](tasks/W014.md) |
| `demiflow/workbench/web/workbench-graph.js` | W | [W021](tasks/W021.md) |
| `demiflow/workbench/web/workbench.js` | W | [W018](tasks/W018.md), [W019](tasks/W019.md), [W020](tasks/W020.md) |
| `tests/conftest.py` | V | [V036](tasks/V036.md) |
| `tests/runtime_v3_support.py` | V | [V001](tasks/V001.md), [V002](tasks/V002.md), [V003](tasks/V003.md), [V004](tasks/V004.md), [V005](tasks/V005.md) |
| `tests/test_runtime_v3_acceptance.py` | V | [V006](tasks/V006.md), [V007](tasks/V007.md), [V008](tasks/V008.md), [V009](tasks/V009.md), [V010](tasks/V010.md), [V011](tasks/V011.md), [V012](tasks/V012.md), [V013](tasks/V013.md), [V014](tasks/V014.md), [V015](tasks/V015.md), [V016](tasks/V016.md), [V017](tasks/V017.md), [V018](tasks/V018.md), [V019](tasks/V019.md), [V020](tasks/V020.md), [V021](tasks/V021.md), [V022](tasks/V022.md), [V023](tasks/V023.md), [V024](tasks/V024.md), [V025](tasks/V025.md), [V026](tasks/V026.md), [V027](tasks/V027.md), [V028](tasks/V028.md), [V029](tasks/V029.md), [V030](tasks/V030.md), [V031](tasks/V031.md), [V032](tasks/V032.md), [V033](tasks/V033.md), [V034](tasks/V034.md), [V035](tasks/V035.md) |

## 编码、集成与回报

编码不设阶段Review、不逐函数打断。缺口只在具体反例证明冻结接口不成立时上报，附producer/consumer/数据流和最小修订；Qwen集中裁决，不让双方私自扩字段。不相关lane继续。

集成顺序 C→G/P/S/K/X/M/H/Q→B/D/E/T→U/L/Y/F/W→V 是合并/启动顺序，不是开工顺序。每个lane交付完整commit与回执后，Qwen在新的integration树合并，GPT与Qwen各自按源码终审，GPT统一修复并完整验收。禁止合主仓。

每lane新分支建议 `engine/runtime-v3-<lane>-20261114`，工作树位于 `_demiflow/worktrees/runtime_v3_<lane>_20261114`。基于设计最终commit，不从主仓未提交状态或其他lane HEAD起步。

回报只交当前活的Qwen总控；回执中保存该总控会话ID与回报接收状态。不得复用已停止的57bfe8f5。会话状态与磁盘commit分别核验。

推荐资源受限时保留19个逻辑owner、降低同时运行测试的数量；不要把所有执行接入又塞回单一C会话。函数编写可并行，重Ray/GPU/两host测试按独立测试资源锁排队。
