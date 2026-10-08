# 提案：异步 client 的 journal 落盘搬离事件循环 + store compaction 阈值调参

日期：2026-09-27 ｜ 作者：GLM（scene_pool_add_v5 全量视觉标注护航） ｜ 状态：已在生产 run 上验证，待正式入库 review

## 背景

`scene_pool_add_v5_l4_keep_hold_tp2_v1`（172,297 张图、每图一次 27B 视觉调用、
128 并发）暴露出两个执行机制问题，新请求吞吐被锁死在 ~46 行/分钟，GPU 大部分
时间处于"饿着的 100%"（Running 4-65 锯齿、Waiting=0、KV cache 峰值 24%）：

1. `AsyncOperatorLLMClient.execute` 内 `journal.reserve/lookup/response/failed`
   是同步调用，直接跑在事件循环上。每次 reserve/response 含独占 flock、
   key 扫描和一次 lance 数据集提交（PVC 上实测 put p50=25.5ms、p90=38.9ms），
   执行期间事件循环冻结，全部并发 HTTP 协程停摆。
2. `LanceRecordStore.put` 每 32 个单行块触发一次 compaction + BTREE 重建
   （实测尖峰 ≈2s，且成本随表行数增长）。高频 journal（每图 2 笔）下，
   这成为周期性秒级全局停顿，且随表增大持续恶化。

修复后（两处同时生效）：新请求段 Running 持续 115-128，生成吞吐 2489→3468 tok/s，
吞吐 46→150+ 行/分钟（见文末实测）。

## 变更 1：journal I/O 专用执行器（operator_llm/client.py）

```diff
+import concurrent.futures
+
+# journal 落盘专用执行器。与 asyncio.to_thread 的默认池隔离——默认池被调用方
+# 业务长任务（如视觉读图/编码）占满时，响应完成风暴的 journal 写会在池里排队
+# 数秒，表现为服务端并发周期性塌落。小尺寸即可：put 内部由 LanceRecordStore
+# 的独占 flock 串行化，多线程只提供扫描/提交阶段的重叠。
+_JOURNAL_EXECUTOR = concurrent.futures.ThreadPoolExecutor(
+    max_workers=4, thread_name_prefix='demiflow-llm-journal')
+
+async def _journal_io(fn, *args):
+    return await asyncio.get_running_loop().run_in_executor(_JOURNAL_EXECUTOR, fn, *args)

 async def execute(self,request):
     ...
-    if self.journal and not self.journal.reserve(source):
-        return self.decode(request,self.journal.lookup(source),reused=True)
+    if self.journal:
+        reserved = await _journal_io(self.journal.reserve, source)
+        if not reserved:
+            saved = await _journal_io(self.journal.lookup, source)
+            return self.decode(request,saved,reused=True)
     ...
-        if self.journal:self.journal.response(source,record)
+        if self.journal:await _journal_io(self.journal.response,source,record)
     except BaseException as exc:
-        if self.journal:self.journal.failed(source,exc,time.monotonic()-started)
+        if self.journal:await _journal_io(self.journal.failed,source,exc,time.monotonic()-started)
```

### 语义分析（为什么这不是平台语义变更）

- **逐请求时序不变**：reserve 完成仍严格先于 POST 发出（await 完成才继续）；
  response/failed 仍在收到响应/异常后、decode 返回前落盘。durability 语义原样。
- **并发面变化**：此前同进程内 journal 写被事件循环天然串行（隐式互斥）；
  现在可能多线程并发进入 put。安全性依据：`LanceRecordStore.put` 的
  检查+写入在独占 flock 内原子完成，该锁本就为跨进程并发写设计，
  不变量比"单线程串行"更强，不依赖本次变更引入的任何新假设。
- **线程池隔离的理由**：首版实现直接用 `asyncio.to_thread`（默认池
  max_workers=32），与调用方业务级并行读图（to_thread 长任务）共享池，
  响应完成风暴时 journal 写排队 → 并发周期性塌落。专用 4 线程小池消除该
  争用；4 足够（flock 串行化 put，线程只重叠扫描/提交的锁外阶段）。

### 影响面与风险

- 影响所有走异步 client 且带 lance_journal 的 prompt 调用（收益：通用）。
- 新增 1 个常驻 4 线程 executor（daemon 线程，进程退出不阻塞）。
- 回归风险集中在"LanceRecordStore 线程安全"这一既有事实上；
  若未来 put 改为无锁实现，需同步回收本变更。

## 变更 2：compaction 触发阈值 32→512（lance/records.py）

```diff
     singletons = sum(f.metadata.physical_rows == 1 for f in ds.get_fragments())
-    if singletons >= 32:
+    if singletons >= 512:
```

- 纯物理维护启发式参数：单笔 put 仍一笔一提交（durability 不变），
  仅碎片合并频率变化。原注释的意图（避免 every-put 维护）在 512 下同样成立。
- 32 阈值下高频 journal 每 ~16 图一次秒级 compaction，成本随表增长，
  172k×2 笔的 run 内不可持续；512 将其摊薄约 16 倍。
- 副作用：新表首次 compaction 前最多积累 512 个单行块，无索引期间
  `get` 为线性扫描（512 行量级毫秒级，可忽略）。

## 配套的业务层变更（非本仓库）

`benchmark/edit/source_images/source_image_pipeline.py`：视觉输入准备
`prepare_image` 由同步 `.map()`（流式路径降级为单 worker 且阻塞事件循环）
改为 `map_async` + `asyncio.to_thread` 并行级（新增 config 参数
`image_prepare_concurrency`，本 run 配 32）。请求身份不变，journal 复用不受影响。

## 实测记录（H100×2, TP=2, qwen3.8-27b, 128 并发）

| 阶段 | 新请求吞吐 | 服务端 Running | 生成吞吐 |
| --- | --- | --- | --- |
| 修复前（同步 map 单 worker） | 41 行/分钟 | 4-65 锯齿 | ≤2489 tok/s |
| +业务层并行读图 | 46 行/分钟 | 仍锯齿 | ~2489 tok/s |
| +本两处变更 | 150+ 行/分钟 | 115-128 持续 | 3468 tok/s |

journal put 实测（同 PVC）：p50=25.5ms p90=38.9ms max≈2s（32 阈值 compaction 尖峰）。

## 回滚

两处均可独立回滚（无数据迁移、无格式变化）；回滚后行为回到修复前
（高频 journal 场景吞吐回落）。
