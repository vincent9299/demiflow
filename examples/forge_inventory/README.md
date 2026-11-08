# 独立第三种 Flow：库存交付核对

这个非模型例子验证接入的通用性：两个不同字段的固定输入，经 native reduce/join/guard 交付详情与状态计数。同一入口有严格／允许差一件两份配置。缺发货仍保留订单，未知订单或重复订单拒绝，空状态不伪造。没有 identity、图片或模型的特殊分支。

`create.py <新目录>` 只用公开 workspace API 创建隔离数据、接线和登记，不执行运行、不覆盖既有目录。使用 Forge 的 `run prepare/start` 或操作台显式执行；输出为每次运行独立固定表，配置/源码变化生成新检查。该文件是示例初始化工具，正式业务处理只有 `flows/inventory.py:run`。

预期：strict 的 A matched、B/C short；tolerant 三个订单 matched。操作台查看两次运行的配置、接线、详情与各自状态计数。这是工程验收夹具，不是生产库存数据。
