# 全局 AI 设置：2026-11-19

真人要求：右上角可直接保存代理、模型和连接；一次配置，所有工作台登记项目自动共用，无逐项目启用步骤。左下角仍为 Forge 开发限制等级，不改为连接设置。

实现边界：沿用 conversations.Service、Store、worker、受限执行和模型通道；不另建服务、会话账本或代理循环。全局设置为宿主私有配置，项目目录不保存密钥。网页只提供已安装的代理，不允许填写任意执行命令。已有原生登录可复用，API key 只写入私有凭据文件，不返回浏览器。

接口：GET /api/development-settings 返回 revision、已安装代理、全局模型连接及凭据是否存在；POST 同路径接受 expected revision、provider、model、effort、authentication、base_url、api_key（留空保持已有凭据）。保存走原服务锁与原子文件替换，过期 revision 冲突；活动开发未排空时拒绝更换，草稿保留。不重启工作台来保存设置。

项目绑定：HTTP 仍只允许工作台登记根。继承模式按原目录、已有 Forge 声明自动生成固定写范围；源代码与 Prompt 可写，数据、运行结果、Notebook 输出和控制文件保护。既有项目明确的较窄范围继续适用。绑定采用项目自己的不可变执行配置，新增其他项目不会使本项目会话失效；更改全局模型后新会话使用新配置，旧会话和证据保留，不替旧历史改授权。父目录 Forge 声明保持原标准与验证入口，不能把已有登记项目降级为无验收开发。

验收：两项目和新登记项目继承；真实 HTTP 保存及刷新/重启持久化；无密钥回显、日志或项目落盘；CAS/无效配置/忙碌保存；旧会话与独立代码范围；真实浏览器可编辑保存；左下角限制等级和会话草稿保持。模型外部传输可用协议夹具，不能冒充真实付费模型验收。隔离树与 /tmp 私有数据；原预览只有空闲后才更新，不碰主仓、生产业务数据或运行任务。

已有服务端连接也作为全局默认值；无需再点一次启用。`Service.show` 从原封存策略投影历史模型/effort，不把当前模型标到旧会话上。`development-settings` 的读取不绑定项目页面 epoch，首次加载和切换项目不会取消全局对话框。原有 32 份策略快照数量上限保留；忙碌判断同时包含 provider 与 Forge 检查/修复阶段。

实现文件：`conversations/global_settings.py` 管理全局配置与自动项目绑定；`service.py` 沿用同一 Store/锁/worker 并封存每项目策略；`policy.py`、`development.py`、`context.py`、`worker.py`、`inspection.py` 支持业务子目录继承父目录 Forge 声明；`api.py`/`http.py` 接线；`web/app.js` 为右上角表单，`development.js` 刷新能力且保留草稿，`development-composer.js` 保留独立权限入口和历史模型显示。没有新建第二套代理循环。

验证记录位于 `/tmp/demiflow-global-ai-settings-20261119/`。首次扩大回归发现原策略历史数量上限被放宽，现已恢复并通过原用例；该批进程在 82% 收到 SIGTERM，不能计为完整通过。修复后的 final.log：48 passed，覆盖全局设置 8 项、真实浏览器表单、原 Service/HTTP 与输入组件。外部模型使用协议夹具，未把它当作真实付费模型验证。

最终相关验证：55 个不同用例分批通过（final.log 48；history.log 6；last.log 新增重新登记不扩大旧权限 1，另 9 项复跑）。真实 RefGen 配置只读预检：已继承 DeepSeek、restricted、Forge gate 保留；没有发起模型调用。真实上下文准备的额外预检被既有声明阻塞：父目录 Project.observe 覆盖 `evaluation/t2i/model_evaluation/reviews/condition_v21_expanded_20261114/review.html`，超过其 128 MiB 单文件上限。任务仅在 `/tmp/demiflow-global-ai-settings-20261119/refgen-context` 准备，业务/历史未写；未绕过 Forge、抬限或改成未登记项目。此项不等于连接失败，也不能据配置可用声称 RefGen 已通过端到端开发验收。
