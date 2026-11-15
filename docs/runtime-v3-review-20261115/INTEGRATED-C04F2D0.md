# 合流候选 c04f2d0：压力、原生恢复与真实终端迁移

本轮固定生产候选为 `c04f2d0e2b2e13836ce763f17117d8754e0d01ea`。后续 `2f70ff6` 只修改三处旧测试的终端身份断言，生产源码未变。原红结果保留，不将一次失败记录改写为成功。

## 运行结果与版本

| 集合 | 实测 | 版本与说明 |
| --- | --- | --- |
| 原宽数据压力 | 5 passed / 276.56 秒 | c04f2d0；40k 原输入、256 MiB 与 120 秒执行限额保持，未用缩小数据或扩额过门 |
| journal 通知、GC 锁、收集预算、最终提交、join/fetch/模型/Save 恢复、历史组合 | 54 passed / 1 failed / 385.15 秒 | c04f2d0；唯一失败为旧 `sinks['iter_rows']` 断言读取新的真实 `take_all` 收据 |
| 两种 fetch 中断恢复、Local 共享嵌套图、全部收集预算反例 | 17 passed / 70.59 秒 | 2f70ff6；真正执行恢复到完成，原请求次数、计费、结果、零行非 final cut 与 ACK 断言保留 |
| U 公开接线单元组 | 55 passed / 21.19 秒 | 2f70ff6；单独进程运行 recording facade，明确不当作真实执行证据 |
| 真实 Ray 共享嵌套图 | 1 passed / 32.49 秒 | 2f70ff6；真实 Ray 2.60.0，禁止 Ray Data；共享上游各执行一次，take_all/run_stream 两个原生 action 均 ACK |

每次都绑定 1,776 份生产/测试 Python 与计划输入，前后零变化、HEAD 不变、工作树干净。精确命令、原 XML 摘要、失败原文及输入摘要见 [JSON](INTEGRATED-C04F2D0.json)。集合存在交集，不能将这些数字相加作为独立测试覆盖率。完整两后端矩阵另见 [Local 与工作台](LOCAL-PRODUCT-FINAL-C04F2D0.md)、[真实 Ray](RAY-API-FINAL-C04F2D0.md)。

## 为什么迁移三处测试

`take_all` 原先在公开层调用 `iter_rows` 自行 append，绕开统一终端的累计返回预算。修复后实际执行、封存与结果收集都使用 `take_all`，没有写回任何旧运行历史。

- fetch 原生中断恢复仍证明空的非 final Dataset cut、原生 receipt 恢复、恰好两次 HTTP 与原请求/字节计费；另外断言 sink 集合只能是 `take_all`。只是读取真实新终端，未放松恢复语义。
- Local/Ray 共享嵌套图仍证明输出内容、重复执行次数、两个 action 的 ACK 与实际 provider；终端断言从 `iter_rows` 改为 `take_all`。
- U 单元组只证明公开委托接口。记录器本身不运行 collection，不能让它伪造“返回第三行后公开层拒绝”。现在验证 `take_all(limit=None/2)` 原参数传递及执行错误传播；真实超限/恰好限额/零行/类型保真由实际 worker 的 14 项收集反例验证。

## 结论与未完成边界

真实 Dataset 的输入、共享图、返回、原生效果、最终提交与恢复可以串起来；原数据/预算/时限未削弱。实际工作台因 GC 独占对象租约而被误标失败的问题在本候选真实浏览器和取消/恢复黄金路径复验通过。

这些证据**先于下一项转换前准入补丁**。后续只对 Arrow/Pandas/NumPy 转换改动运行对应真实定向验收，不能把完整矩阵冒称运行在尚未产生的提交。已查实的 rows/Arrow/Pandas 共驻预分配缺口仍需修；同机 Ray 不等于多机/GPU验收，原宽数据通过也不等于完整性能基准达标。
