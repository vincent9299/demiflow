# Ray scope关闭失败：物理退出与原资源权威

根节点隔离分支`engine/gpt-runtime-v3-review-20261115`，生产修复`8fdb865`。
产品root仍是看懂/配置/运行真实pipeline并分析评测；引擎root仍是统一、正确、稳定、有限资源和高性能。
本项保障失败退出与历史真实性，不代替完整产品、图能力或性能验收。

## 原缺陷与修法

L016从退役v2假driver迁到真实Ray时，控制连接故障叠加`ResourceManager.close`异常，
原业务异常仍可见，但真实worker继续存活。固定`f03b22e`复现，测试在原owner fence内
恢复原方法并调用原资源协议清理，预先捕获的失败观察不因测试清理变绿。

`DataflowScheduler.close`现在保留原scope关闭异常，借同一ResourceManager的原
ResourcePlan/ReleaseSpec/apply/reconcile流程退休本scheduler仍持有的请求。
没有直接kill任意进程、另建账本、假造CONFIRMED或绕过原计费。
关闭回执若已经提交却丢失，继续处理原scope的关闭意图。
兜底失败保留附加错误与原未确认收费；只有原观察确认所有已知worker物理退出，
DataflowExecution才释放其本地store句柄，scope关闭错误仍被返回。

## 已验与待验

固定`8fdb865`，原生Ray私有2 CPU/0 GPU，三项通过：RUNNING之前准入失败、
RUNNING之后连接失败、scope关闭失败叠加连接失败。结果3 passed/12.51秒，
测试前后源码/提交未变，三阶段完整、冻结七件保持、0子进程。
回执：`/tmp/demiflow-root-l016-fault-r2-evidence/verification.json`。
Ray在fixture内初始化并关闭；整进程FD为3→14，残留项包含Ray日志及eventfd/socket，尚未逐项证明全部匿名句柄的归属；
不宣称FD零增长，也不以本组代替已有独立FD预算门。

L016全部28个原nodeid保持：布局/程序/资源/通道从真实封存和assignment读取；
原9个fake故障现于真实provider/endpoint精确注入；5个原Ray案例保持六行、
两CPU、checkpoint_rows=2、checkpoint_interval_ms=60000、真实SIGKILL和恢复。
完整28项仍待组合复验；没有以删除失败或降低预算断言达成全绿。

先前r1有两个红，须区分：一项真实worker残留；另一项新故障夹具直接在provider入口抛错，
没有形成可验证创建/死亡身份，原协议正确保留UNKNOWN收费。现夹具改为真实provider
创建但尚未激活业务的shell后返回失败观察，再验证原身份退出；没有伪造never-created。
该旧负例仅留下本测试私有/tmp中的未确认逻辑记录，没有主仓/共享资源的修改，
未将其改写为已确认历史。

前两次真实Ray迁移回执另保留：`9d5f304`因新fixture的Unix socket路径过长在setup失败；
`80a1fdd`的五项业务执行在新断言处失败，因为原资源表会压缩已完成intent。
修订断言改查永久allocation的真实死亡证据，未完成的退出仍必须有原intent。
这两个测试适配错误不包装为生产缺陷或已通过业务验收。
