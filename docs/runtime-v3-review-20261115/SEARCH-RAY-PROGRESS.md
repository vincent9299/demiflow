# 搜索接入与 Local/Ray 业务链复验

继续对齐产品 root（看懂、配置、运行真实 pipeline，分析/比较/评测）与平台 root（统一、正确、稳定、有限资源的通用数据计算）。本批补齐 I01/I02/I05/I07/I09；仍不是整个升级完成声明。

## 修改

- 普通 SearchWeb、与 RawFetch 组合的 SearchWeb、生产端 SearchWeb 都通过既有 driver 原生 I/O 服务端口执行。公开 Dataset 不携带 WebSession 连接到 worker，不新增 journal/执行引擎。
- 原生搜索服务按封存 occurrence 核对真实 worker 权限；每个 occurrence 有独立 actor 绑定、调用身份与原生响应回执。共享 WebSession 由同一异步任务负责启动/关闭。
- 在首个 Dataset cut 前中断，恢复按原 RunJournal 搜索响应事实判断：固定响应复用，未知响应拒绝重发。磁盘缓存删除也不改变这个判断。
- 暂时没有 I/O 空位时按原上限进行有期限、可取消的等待；不扩大并发或把临时拥塞错误地标成可重试的预算耗尽。
- `union` 不再要求输入 Dataset 持有同一个 Python executor 对象；终结动作选择整个图的执行宿主，上游声明、共享/独立调用与固定来源身份保留。
- 普通搜索的资源指标进入原 `execution_metadata`/StreamStats；数据来自真实 WebSession 观测。历史公开搜索测试使用实际返回结果，不再依赖 worker 修改 driver 的闭包列表；所有结果 schema、计数、清理断言保留。

## 实测证据与边界

| 证据 | 结果 |
| --- | --- |
| `/tmp/demiflow-original33-r5.xml` | 原 33 项新失败按原 nodeid 加 M 组 27 项，60 passed，未删除或跳过原 33 项。 |
| `/tmp/demiflow-search-r4.xml` | 普通搜索、固定/未知响应的首 cut 前恢复、共享 session 的独立 union 分支，4 passed。 |
| `/tmp/demiflow-search-local5.xml` | 公开 Dataset、原历史公开搜索、类型保真、共享上游、状态/结果/Lance 恢复及生命周期组合，30 passed。 |
| `/tmp/demiflow-ray-business1.xml` | 本机真实 Ray Core：Prompt→sort→结果；enqueue→read_queue→fetch→原生结果提交/ACK→历史结果校验，2 passed。Ray Data 入口被禁止。 |
| `/tmp/demiflow-search-combined6.xml` | 搜索/生产者恢复与 DTO/codec 组合，97 passed、1 failed、2 deselected；失败为新增组合测试把默认 `result.raw_ref` 误写成 `raw_ref`。 |
| `/tmp/demiflow-search-final7.xml` | 正确读取公开字段并加强为 ObjectRef 实际字节核对后，fetch→search 1 passed；两个 HTTP 服务的实际请求各一次。 |

Local 测试使用隔离树的 `env/bin/python -B -m pytest`；Ray 使用 `env-ray/bin/python`，显式 `PYTHONPATH=$PWD:$PWD/tests`，仅本任务私有 Ray 集群和本地 HTTP fixture，数据均在 `/tmp/demiflow-*`。集群在测试 finally 内关闭，没有触碰生产服务。

上述证明同机 Ray Core 的实际组合，不能代替两台物理机、物理 GPU、完整性能和全部产品验收。剩余 lane 的 fixture/真实契约接线、原生 Agent/文档组合、分期驻留、孤儿 staging 与最终产品回归仍继续处理。
