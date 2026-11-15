# 状态对象、所有权与销毁

函数卡不是孤立函数堆。纯编译/校验/投影用函数；有生命周期和状态的部分用下面的类；跨实现边界以Protocol约束；冻结数据以不可变dataclass表示。消费者不得为状态对象复制第二份持久账本。

| 类 | 持有的状态 | 构造/调用者 | 生命周期与线程 |
|---|---|---|---|
| DataflowExecution | request/services、sealed、worker endpoints、cut coordinator、scheduler、terminal订阅、native acquisition service、状态/失败 | execute_action | driver唯一owner；prepare→start→pump→finish/cancel→close |
| DataflowScheduler | ready有界队列、DRR deficit、stage/partition lease、residency预算 | DataflowExecution | 不分配第二份物理资源；通过ResourceManager；close等退出证明 |
| DataflowCheckpointCoordinator | cut sequence、固定participants、contribution摘要、prepared/unknown/confirmed映像 | DataflowExecution | driver唯一全局提交编排；原CommitCoordinator持久化 |
| DataflowWorkerExecutor | assignment、kernels/readers/channels、局部cut/buffer、有限inflight、services/mailbox | WorkerRuntime/HostedDataflowWorker | 控制与业务线程分离；不持driver HostEndpoint或RunJournal；snapshot不提交 |
| OperatorKernel | spec、运行实例、输入端口状态、bounded batch/spool/timer、输出cursor、state adapter | make_kernel | worker业务上下文；start/push/finish/snapshot/restore/close；每owner清理一次 |
| Reader九类 | snapshot、split、下一读取cursor、已读未发refs、有限解码器/FD | open_source_reader | worker；QueueReader只通过原生服务端口；close不ACK |
| ChannelSetState / RoutingCounters | credit owner、序列、路由ordinal、reader前沿、buffers、EOI/seal/tombstone | open_channels | worker串行路由上下文；cut封存；已确认后释放 |
| DriverJournalService | action、store、有限服务队列/inflight、原journal、绑定的native服务 | make_driver_services | driver；submit非阻塞，后台执行原生事务，poll_replies有界；close保unknown |
| DriverArtifactPort | action/bounds，只提供artifact/reserve | make_driver_services | 先于store构造；不依赖服务反向初始化；同原journal |
| WorkerJournalClient | transport、grant/lease、有限请求与deadline | make_runtime_services | worker端口，不打开数据库；超时保留未知语义 |
| Local/RayHostEndpoint | lease、当前boot连接/actor、有限事件/回执表 | make_host_endpoint | driver；activate/poll/reply/cut/cancel/closed；旧boot不能接新回执 |
| HostedDataflowWorker | spec_hash/boot、WorkerRuntime、ControlMailbox、执行上下文 | provider远端启动 | 三参数构造匹配既有Ray provider；控制方法不运行长UDF |
| ControlMailbox | bounded outbox/pending/condition、取消与期限 | HostedDataflowWorker | request只在业务线程等待，控制线程drain/deliver，close唤醒全部 |
| RunArtifactStore | authenticated root/store_id、ArtifactAuthorityPort、bounds | driver/worker services | immutable数据文件；pin属原RunJournal；不自行新建ledger |
| NativeAcquisitionService | 原QueueDatasource/RawFetch、lease/claim表、IO任务、原生namespace | DataflowExecution.prepare | driver受管IO；configure→start→poll/acquire/finalize→finish/close |
| RemotePromptJournal | worker port、原journal约束只读值、action/node | M binder | 不拥有数据库；close不关闭driver；方法固定白名单 |
| RemotePromptRecovery / RemoteEmbeddingRecovery | 原actor、NativeActionView、RemotePromptJournal、semantic_owners/response_bound/inflight | M各自binder | 原有业务恢复方法复用，构造不能开SQLite；Embedding无Prompt coordinator |
| RuntimeActionHandle | 同一execution、输出订阅、完成/异常、close状态 | open_terminal | 迭代器全生命周期持有，早关取消必要生产者 |
| StoredMaterializedHandle / InputMaterializedHandle | immutable manifest或输入block、schema/rows、pin/released | materialized_handle/build_input_handle | release仅当前handle；共享其派生视图失效，其他owner不删 |

可变状态仅在对应owner的线程/锁/事件循环内访问；跨线程的CALL_RESERVATION明确复制进原生命令。状态类若使用__slots__，必须包含表内所有字段和卡片需要的字段；不得等实现时发现snapshot字段写不进去。不可变DTO和进程内服务视图区别见interfaces.pyi；深冻结只作用可冻结声明，不强行冻结活运行对象。
