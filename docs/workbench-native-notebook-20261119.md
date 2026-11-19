# 原生 Jupyter 数据工作区

真人确认：左侧数据字典，右侧原生 Jupyter，尽量直接复用，少自定义按钮；旧 Case 分析“**一起移除界面，保留历史数据**”。这覆盖旧 Notebook/Case 界面的交付前提，不能后续再以旧 UI 要求恢复第二套编辑器。

## 所有权

- 原生 Notebook 7.5.7 / JupyterLab 4.5.11 前端及 Jupyter Server 拥有编辑、执行、中断、重启、输出、保存、文件菜单和检查点。工作台没有第二套单元格或执行状态。
- 工作台负责项目、左侧表/字段/版本字典和同源代理。字典使用原有固定版本读取器；点击“插入读取代码”只放入代码，不自动运行。普通/历史定位保留绑定，外部重新绑定同名表时明确报冲突。
- `.ipynb` 位于项目 `notebooks/`，Python 内核的工作目录是项目根。输入可以使用其他可访问目录，无新增输入配置或强制数据登记；原管道输出策略不变。
- 项目 `analysis_write` / `analysis_execute` 分开控制。采用已有受信开发环境语义，不宣称 OS 沙箱或用户 Python 代码的硬内存/CPU限制。浏览器代理每请求检查权限、Origin、项目 Cookie；原生文件接口和下载接口共同校验路径/软链接。
- 不改开发对话、算子、管道、运行和 Readme 的业务行为；合流保留最新运行导航提交。

## 生命周期与边界

原生服务按需启动。一个工作台最多 4 个活动项目服务，每项目最多 2 个内核，启动计数串行；全局最多 4 条内核 WebSocket。切换功能区或项目保留原生 iframe、未保存内容和内核。第五工作区只回收本页中已保存且空闲的旧 pane；服务端另确认没有内核连接、进行中 HTTP 请求或运行中的内核，才退役实例。旧根目录绑定保留，不能把旧文档自动保存到新根。

撤销执行权限会关闭对应服务和内核，包括“先只读打开、后授权执行”的情况。正常退出关闭自有进程；子服务监视原父进程消失后触发原生 shutdown。退役时清理私有临时目录；失败只留一份最多 64 KiB 的脱敏启动诊断。项目文档和检查点不随私有目录清理删除。

HTTP 文档/文件 16 MiB、响应 32 MiB、目录 512 项；原生输出限流复用 Jupyter。它们约束集成传输，不等同于限制任意 Python 用户代码的所有资源。

## 历史与清理

移除旧自建 Notebook、Case 比较/标注/保存视图的界面、重复工具栏和其 JS/CSS；对应被替代的 UI 测试退役。混合测试中仍有关的图、HTTP、版本与持久化断言保留或迁移。

原 SQLite 记录、输出、标注和保存视图及后台读取接口保留，未删除历史。每次打开最多将 32 份旧已保存 Notebook 的原 ID/revision 源码复制为原生文件；每份最多 64 单元，已存在原生文件不覆盖。浏览器旧草稿保留原 localStorage，并按内容 hash 一次性导出为 `恢复草稿-*.ipynb`。SQL 历史单元保留原 SQL 文本为 raw，避免伪装成可直接运行的 Python；旧 Python 中依赖原 broker 的 helper 导入需改用左侧字典或普通 Python 读取。旧输出不伪造为新内核的执行结果。

## 启用与验证

宿主显式配置 `workbench view ... --notebook-python /path/to/python` 或 `DEMIFLOW_NOTEBOOK_PYTHON`；该环境安装项目 `notebook` extra。本次使用 0905 下独立虚拟环境，共享原环境未更换。7.6.5 组合遇到原生快捷键设置异常，改用上述已实测兼容版本，没有修改第三方 Notebook 源码。

真实 Chromium + 原生内核验证：编辑、运行、表格输出、保存刷新、重命名刷新、中断、重启、外部输入文件、跨功能区/项目草稿保留、旧浏览器草稿恢复、桌面/390px。真实 HTTP 验证未授权/外源/跨项目、编码路径、下载软链接与大小、权限先授予后撤销、并发内核上限、第五项目空闲回收和临时目录清理；固定绑定冲突另有浏览器测试。独立只读 Review 发现的问题已关闭。具体批次结果及发布保护证据在工作区 `_demiflow/shared-review/workbench-native-notebook-20261119/`，不声称全仓测试通过。

原生接入依据：[Jupyter Server 同源嵌入/URL 前缀配置](https://jupyter-server.readthedocs.io/en/stable/operators/public-server.html)、[Notebook 原生插件管理](https://jupyter-notebook.readthedocs.io/en/v7.0.5/configuring/plugins.html)。
