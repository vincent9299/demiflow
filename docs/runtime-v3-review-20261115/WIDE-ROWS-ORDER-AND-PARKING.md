# 宽行、顺序与阶段切换复验

本次以真实 Dataset 为入口补修四个相连问题：数据路由不再把完整业务行复制进随后丢弃的 1 MiB K4 控制文档；typed data codec 继续严格校验行、父身份与实际字节。原显式 K4 桥及承重文件不改。join 的真实资源请求计入两侧行、编码与输出，并由原 allocator 准入。

分组折叠的来源排序使用有类型的数值坐标，避免 JSON 文本把位置 104 排在 26 前。`seed=None` 从原封存的每节点 constants 中读取，不能另生成第二个随机种子。

小资源下的有限输入 join → 持久增量消费者复现了真实卡住：新输入入队与上一轮 idle 通知存在竞态。worker 在消费新命令时重新撤销自身旧 idle 标记，确保新输出之后再次通知 driver。未伪造 EOI、ACK 或 cut；源、join、消费者仍走原提交权威。

## 证据

- `/tmp/demiflow-wide-order-seed-r1.xml`：49 passed / 1 failed。失败是 9.75 MB 原始二进制行的 typed 编码与 Arrow 转换同时驻留超过默认批预算，该保护保留。
- `/tmp/demiflow-wide-order-seed-r2.log`：1 passed 后，增加明确批预算暴露上述阶段切换死等。215.97 秒后主动中断自己的测试；不记作通过。独立小数据复现 `/tmp/demiflow-debug-finite-durable.log` 同样耗尽 20 秒执行预算。
- `/tmp/demiflow-wide-order-seed-r3.xml`：6 passed，47.19 秒。包括默认预算下超过控制文档上限的宽行；显式 64 MiB 批预算下 9.75 MB 行 join 扩增后写入真实 Lance；120 个位置的非交换折叠；未指定随机种子的实际 shuffle；超行预算拒绝且无提交；有限 join 向不共驻消费者交接及原生 ACK。

本次没有放宽默认转换共驻预算，默认配置并不保证所有合法单行在任意转换中都能驻留。批预算是可用能力的实际约束，测试显式说明所需预算。上述通过数量不是产品/平台整体完成率。跨 attempt 随机种子历史保护沿原封存规则，未由本新增测试单独证明。
