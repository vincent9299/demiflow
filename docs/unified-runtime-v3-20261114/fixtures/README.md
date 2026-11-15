# 契约样例

index.json列出7个正向、5个反向wire样例。它们是字段与边界样本，工件引用是测试fixture标识，**不是已经封存的真实运行结果**。C的codec测试读取这些样例；D/E/V必须用真正store生成工件、真正compiler生成图和layout，再检验跨模块一致。

source-cursor的offset=7，pending_refs保存已经读取的序号6；恢复应先发该pending，再从7读取，不能把6丢掉或读取两遍。record与ref/inline恰一，pending与EOI明确区分。reserve直接小控制payload避免“先申请写artifact额度才能发送申请”的循环。finite scope明确两个参与者，不允许第一个完成就封存全scope。

不得以这些静态样例通过代替真实业务测试。字段/参数变化时producer、consumer、fixture、task与gate同一次更新。
