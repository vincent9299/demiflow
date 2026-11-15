# 原分组/查表历史回归

原 test_stream_grouping_lookup.py 六项完整通过；连同原生group timer3项与workers4/prepend布局，共 **10 passed / 60.95s**。精确源码和XML摘要见JSON。

旧parent seen列表迁到真实worker文件，保留原version1/cache行修改隔离/重复查询/缺失key/嵌套list+struct/含引号key/超match额度拒绝。分组压力仍原8×600字符、chunk/buffer2048/maxgroups2，完整原数据不丢；超大10000字符仍拒绝。

原timer例依赖的是第三行必须等下游首次group输出，旧同loop asyncio.Event不适用于独立worker。新tmp文件gate只承担测试同步，原30ms flush、2s等待、3个id精确一次保持。首轮真实4pass2fail证明两个分组缺口未被观察迁移掩盖；合96c6d4b后oversize修好，剩默认4worker/5stage内存组准入失败。

本批在实际stage数超Localworker预算且含AsyncMap时启用原线性增量suffix共址，全部原kernel/state/port保持，源独立，CPU/memory profile相加，不自动增加worker/credit/预算。原timer链因此可以执行并通过。

未声称跨worker共享原asyncio.Event，也未退回driver执行。已另记录EOI同步drain可能阻碍同worker下游timer的竞态风险，下一轮用更慢但仍低于原2s的timer独立验证；本报告不把未测边界写成完成。主仓及冻结件未改。
