# demiflow 活性硬化与批语义升级计划(2026-09-14 起草;**同日 U1/U2/U4 已实现并全测通过 70/70**)

> 实现补充发现(已修并入):原引擎 drain 的 sentinel-put 满队列死角(工人
> 被取消后主协程永久阻塞);事件循环内 import 死锁与 format_stack/
> linecache 对挂起帧挂死两个生态暗雷(转储改纯属性拼行规避)。
> 未做:U3(消费方收敛,待 flow_images_batch 改指平台原语)、U5(fleet 运维)。

> 背景:2026-09-12~14 概念库④配图线 30+ 小时实战(25 机 fleet,五波 882 万图,
> 峰值 100 张/秒,零数据丢失)。暴露的短板与本计划遵循本库一贯哲学:
> **机制归引擎、策略归消费方;实战模式沉淀为平台原语**。
> 证据存档:night_watch.log(09-12 05:0x 任务级 dump)、/tmp/hang2.py 复现脚本。

## 一、事实与定性

1. **冻结是类缺陷,不是孤案**。net.get_client 文档已记载 2026-08-21 三次夜跑
   同签名卡死(半读连接复用,当时以检索池 keepalive=0 收敛);09-12 流式引擎
   第四起:worker 停在 stream.py:139 `await _call(fn,row)` 内的**无定时器
   Future** 上(任务级 dump:9 worker 全挂无超时 Future,feed 阻塞满队列,
   管家狗正常空转——现有 watchdog 只认异常,不认停摆)。四起共性:
   **任何一层缺"必醒"保证,整链静默凝固**。
2. **批查询语义缺席**。MediaWiki 50 题/次的批量查询使 API 配额利用率×50,
   是本轮提速 28 倍的主因。当前引擎契约(行→None|dict|list)可表达
   (list 展开),但"攒 N 行→一次调用"需消费方在源头手工做批(分片粒度
   被迫变粗)。该模式通用(任何支持批量端点的源都适用),应沉为机制。
3. **应急旁路重复发明了平台**。flow_images_batch.py(demiwtg-data,实战版)
   自带了令牌桶/重试/HTTP 客户端/账本追加——分别对应本库 net/附带的
   429 退避/fetch_tiers 的硬超时字节封顶/AppendManifestStore。旁路原因是
   上述 1(引擎带雷)且当时 net 层在嫌疑圈内;雷排除后应收敛回平台原语。

## 二、升级项(按风险收益排序)

### U1 活性硬化(消除冻结类,P0,约 1 天)

三层防御,全部是机制,零策略:

- **U1a 算子级硬超时**:`StreamStage` 增加可选 `hard_timeout`(缺省继承
  全局,如 300s)。worker 的 `await _call(fn,row)` 包 `asyncio.wait_for`;
  超时按 catch 白名单计 miss(白名单外抛 TimeoutError 走管家狗终止)。
  语义:任何"必醒"缺口被此层兜底,单卡不再全停。
- **U1b 管家狗升级——零吞吐检测**:run_stream 维护全局 emitted 计数,
  非 EOF 状态连续 `stall_timeout`(缺省 120s)零产出 → 抛 `StallError`,
  附带全部挂起任务栈转储(asyncio.all_tasks + get_stack——即 09-12
  手工做的事,沉淀为机制)。
- **U1c 闸门活性登记**:net 的 Semaphore/RateLimiter 登记 acquire 时刻,
  reaper 协程发现超龄持有(>2×hard_timeout)→ 记错+栈转储+强制释放。
  (net 已有四起卡死史,此层让第五起在 30 秒内显形而非凌晨三点。)

回归:hang2.py 场景入库为 test_stream 用例(复现→U1a 认缺→管线续跑);
net 闸门超龄用例。

### U2 BatchMapOp——攒批进引擎(P1,1~2 天)

```
ds.from_iter(逐条任务)                     # 分片保持任务粒度(优于源头做批)
  .batch_map(meta_fn, max_batch=50,        # 引擎攒批:条数或 flush_interval 触发
             flush_interval=0.5)
  .map_async(download_fn)                  # list 输出经既有 fan-out 展开
  .map_async(sink)
```

- 机制:feed 与 stage 之间加攒批适配器(条数/时间双触发,尾部 flush);
  fn(list)→list|None;批内整批失败→dead-letter 重试队列(幂等键由
  消费方 key_of 提供),不再弃批。
- 策略留给消费方:max_batch 数值、批量端点细节(MediaWiki 的 50 题拼装、
  `|` 裸传、continue 续页等留在 demiwtg-data/operators)。
- 兼容:纯新增算子;现有 map_async 管线零影响。
- 首个用户:demiwtg-data/operators/commons.py 的批量版(已就绪,
  _meta_batch + list 行 __call__,09-12 所写)。

### U3 旁路收敛(P1,半天)

flow_images_batch.py 三处改指平台原语(引擎无关,不依赖 U1/U2):
- Bucket/自写重试 → `net.register_limits` + `net.request/net.stream`
  (429 长退避已在 net,口径单源);
- 下载守门/封顶 → `fetch.fetch_tiers`(硬超时+字节封顶+verify 钩子,
  本库已有;P18 的原图>10MB 降缩略图作为档位轮转的最小实例);
- 自写账本追加 → `store.AppendManifestStore`;
- done-set → `resume.scan_counts`(已存在,勿再发明)。
收敛后与 U2 版 flow_images.py 合流为同一实现,batch_fetch 退役
(git 史保留其实战履历)。

### U4 断点续跑口径统一(P2,半天)

from_iter 侧提供 `checkpoint_skip(manifest, key_of)` 组合子(内用
resume.scan_counts),使"账本幂等+机器级重跑零重复"成为声明式能力
——三天五波全靠此模式手工实现。

### U5(可选)fleet 运维沉淀(P2)

本轮 launch/heal(stallheal:清单增量判定+幂等重启)与旧七机时代的
supervise/patrol 同构——可沉淀为 demiwtg-data 的 ops 工具统一入口,
不进 demiflow(运维策略归消费方)。

## 三、验收

1. test_stream 新增:冻结复现用例(U1 三层各一);
2. BatchMapOp:条数触发/时间触发/尾部 flush/批失败重试 四用例;
3. 收敛后 flow_images 全量小样本跑通,账本行与 batch_fetch 版逐字段一致;
4. 挑一条实跑验证:恢复单机流式版跑 1 小时零冻结(U1 生效的最终判据)。

## 四、排期建议

D1: U1(含回归)→ D2: U3 收敛 → D3~4: U2 + flow_images 回迁合流 → D5: U4(+U5 视人力)。

## 五、复核修正(2026-09-14 复核后追加,源码行为已验证)

1. **U1a 接线补全(五处,缺一静默失效)**:hard_timeout 除 StreamStage/
   AsyncMapOp/stream.py 外,必须加进 dataset.map_async 的 policy_attrs
   鸭子检测元组与 actor 解析分支(L249)——检测只认在场属性,漏加则
   actor 声明被静默忽略。AsyncMapOp 为 frozen dataclass 且 map_async
   按位置构造,新字段须置于末位并带默认值。
2. **U3 档位语义更正**:fetch_tiers 的字节封顶是"认缺不轮转"(fetch.py
   文档明文),">10MB 降缩略图"不能由轮转表达。正确做法:档位选择留在
   算子层(meta 先行选好 orig/thumb),fetch_tiers 收单候选,字节封顶
   作兜底。operators/commons.py 相应有一处小改(此前"无需改"作废)。
3. **BatchMapOp 生命周期**:攒批适配器是有状态件,必须实现 aclose 尾刷
   钩子(缓冲批在退出期落盘),对齐平台"收尾钩子必须落盘"契约;
   其 actor 实例经 _stades/aclose 机制统一收尾,与既有模型同构。
