# 深目录资源退出证据修复

基线 `c29978a`。产品真实暴露：原 thread.json 绝对路径约 247 字符，加
`:confirmed-thread-exit` 后超过冻结 DTO 的 256 字符限制。线程实际上已退出、
资源池甚至已释放，manager 却拒绝回执，使运行失败。Local process 的
process.json/cleanup.json 绝对路径存在同类风险。

## 实现与协议

- 新引用形式：`df-resource-evidence:v1:<sha256>:<record.json>`，约 100 字节。
  摘要绑定原 provider 根目录、scope、allocation、原记录名和原文件字节；
  cleanup.json 还绑定原 process.json 的 birth/unit 身份。不截断路径，不放宽 DTO。
- `provider.resolve_evidence(scope, allocation, reference)` 从原目录读取并校验。
  没有新增证据文件、表或第二账本。每份原记录最多读 8192 字节。
  引用只认证原证据身份/内容；资源释放仍依赖原物理退出检查，不能拿引用代替退出事实。
- thread 确认所有所属线程退出后才写原 thread.json 的 CONFIRMED 终态，之后不重写。
  process 每次仍实际检查 birth、unit lock、descendants；已有 confirmed cleanup.json
  不再覆盖时间戳。后续 unknown 返回不确认，不改写已有历史终态证据。
- 重复 cleanup 保持同引用。原容量池已 RELEASED 的历史记录不以新格式重新 release，
  保留旧 `release_evidence` 原字节，兼容此前已经用超长绝对路径完成的真实释放。
- 历史绝对路径记录仍保留；新 resolver 的版本化接口用于新引用，不改写历史路径。

## 验收

两条真实 Dataset 使用 **run 目录自身即超过 256 字符** 的深目录，thread/process
都复现基线失败；修复后完整执行、确认退出、归还原资源。重复 cleanup 原证明不变。

最终 **22 passed / 0 failed，9.49 秒**，包含：

- thread/process 深目录真实 Dataset 和解析原退出证据；
- 跨 scope/allocation/provider 根域、改原记录、换 process 身份均拒绝；
- 缺失记录随后出现时原 absence 引用失效，超大证据读取拒绝；
- 真实 process 已用旧长路径释放后，再次 cleanup 保留旧资源账与原终态文件；
- 真实未排空 thread 的额度仍保留，unknown process cleanup 不允许替代者绕过额度；
- 原 Local managed process 的真实 spawn、单位锁、birth、退出、容量及 manager 回归。

命令、日志/XML、完整源码摘要、冻结七件逐字检查见同名 JSON。
原始红日志 `/tmp/demiflow-evidence-red.log`；最终日志 `/tmp/demiflow-evidence-final.log`。
未改主仓、未 push、未操作生产服务。该批为原 provider 证据传输修复，不扩展宿主能力。
