# Forge B1 外部执行边界：实现与验收事实

日期：2026-11-18/19，Asia/Singapore。工作位置：`_demiflow/worktrees/gpt_prompt_replay_20261118`；基线 `dab45bb3018dc922a85f4753f207fd89d61e6407`。依据 DH.4、DH.9 及本轮继续交付授权。本文件记录本 worktree 的新实现，不改写 v2 方案、D0 探针或主仓的历史报告。

## 实际后端与设计变化

本机 root filesystem 和 `/tmp` 没有 shared mount propagation。临时探针确认：`unshare -Urm --propagation unchanged` 可以创建 namespace，但真正的 tmpfs mount 和 bind mount仍失败；`unshare -Urpf --kill-child=SIGKILL` 和 `unshare -Urnpf --kill-child=SIGKILL` 成功，内部进程看到 PID 1。

因此实现选用 **user + PID + network namespace、固定 PID 1 监护、全部 capability 清零、锁定 securebits、no_new_privs、Landlock ABI 1 与 seccomp allowlist**。不依赖 mount/bwrap，不创建系统用户，不改变业务 ACL、owner 或 mode。

这里的 UID 映射明确保留调用者的宿主身份。当前宿主调用者为 UID 0，内部 UID 也是 0，但 untrusted CLI 的 permitted/effective/inheritable/bounding/ambient capability 集全部为 0。**这不是专属非 root 宿主账户。** 进程、文件、网络边界由上述组合提供；这个替代路线已经与主协调明确，不以措辞替换身份事实。

父控制进程丢失后，外层固定 launcher 的 parent-death signal 会终止它；util-linux `unshare --kill-child=SIGKILL` 随之终止 namespace PID 1；Linux 内核终止该 PID namespace 的全部后代。PID 1 也在 CLI 主进程退出、deadline 或资源观察超限时退出。`setsid`、双重 fork 和主动关闭合作租约 FD 不会脱离这个内核进程域。

## 固定代码与接口

- `conversations/sandbox.py`：可信 scope 校验、密封 memfd 启动合同、固定 Popen、readiness 与实现身份。
- `sandbox_entry.py`：宿主父进程死亡绑定，固定 util-linux namespace 命令。
- `sandbox_child.py`：可信 PID 1、完整限制后 exec、deadline/资源观察、域终止。
- `sandbox_native.py`：capabilities/securebits/NNP、Landlock 和 x86-64 seccomp profile。
- `sandbox_relay.py`：两个固定任务通道的有界运输，不决定模型、认证或工具语义。
- `execution.py`：Codex/DeepSeek 共用 provider 启动口；restricted 不回退 trusted_local。

```python
SandboxSpec(
    read_paths=(), write_paths=(), runtime_paths=(), protected_paths=(),
    private_home="...", denied_paths=(), role="developer", seconds=120,
    rss_bytes=2 * 1024**3, processes=32,
    file_bytes=64 * 1024**2, private_bytes=256 * 1024**2,
)

launch_sandbox(argv, *, spec, env, cwd, lease_fd=None,
               control_socket=None, model_socket=None)  # -> Popen

launch_provider(*, mode, adapter, config, home, root, lease_fd,
                sandbox_spec=None, control_socket=None, model_socket=None)

execution_identity()  # 有界只读，不运行探针
```

`protected_paths` 禁写，但可以由显式 `read_paths`/`runtime_paths` 只读授权。`denied_paths` 不得与任何读取、执行或写入授权相交。可写父目录覆盖受保护对象时直接拒绝，不能以子对象的“只读”声明抵消父目录写权限。scope 对象按 device/inode 绑定，在实际安装 Landlock 规则时复核。

`runtime_paths` 与任何写路径（含 HOME）双向相交均拒绝；实际加载的 `demiflow` 包、当前 Python executable/prefix/base_prefix、固定 `/usr`（含 unshare）及 `/lib`、`/lib64`（loader/native libraries）自动加入不可写保护，不能由调用方遗漏。这些固定系统位置仅用于拒绝写入，不额外授予 CLI 读取权限。开发平台自身时，控制器必须运行于另一份不可写安装，不能同时编辑当前正在执行的控制模块。实现 hash 是证据身份，不是允许执行被污染代码的理由。

`readonly` 和 `verifier` 均禁止 source `write_paths`，仅 `private_home` 可写。verifier 不允许 control/model 通道。原生 verifier argv、输出预算与日志合同仍归 Forge；它通过可信 launch callback 复用此启动器。

仅固定 verifier 可以提供 `PYTHONPATH`，每一项必须是非空绝对物理路径，且属于已经授权的 read/runtime 范围；开发 CLI 不获得这个例外。Python 直接脚本和 shebang 入口所需的 FIOCLEX/FIONCLEX 仅改变自己已有 FD 的继承位，已做真实入口验证。

provider HOME 是不可信持久内容：trusted adapter 写配置之前检查现有 HOME 的符号链接、硬链接和规模，避免上一轮 CLI 用链接诱导控制器越权写入。restricted 新会话使用 `session_home/provider`；控制记录、turn scratch、租约 FD 和 broker HOME 留在其外。不能将整个 `state_root` 或 `session_home` 放入 allowlist。

所有非约定 FD 在 untrusted exec 之前关闭。`.unit` 仅由可信侧保留合作协调用途，不进入 CLI。密封 spec FD、控制状态 FD、继承的 host channel 和 listener FD 同样不交给 CLI。只有 stdio 和成功 exec 前用于传递失败原因的 CLOEXEC 管道存活至最后一步。

## 文件、进程与网络限制

Landlock 负责读/写/执行/创建/删除的实际路径范围，所有后代继承。ABI 1 没有的修改接口由 seccomp 拒绝：路径 truncate/ftruncate、chmod/chown、utime/xattr、未经限定的 ioctl、mount/chroot、namespace 创建、ptrace/process_vm、pidfd 提取、io_uring、BPF 等不在 allowlist。`open/openat` 仅允许不带截断，或明确 WRONLY/RDWR 的调用；不会允许 `O_RDONLY | O_TRUNC`。`openat2` 和 `clone3` 返回不支持，使已有运行库使用可检查的接口；未知 syscall 不默认放行。

兼容 x32/i386 调用不能借原生 x86-64 syscall number 绕过 profile。host `/proc`、`/sys` 和 `/run` 不可读；进程注入接口被拒绝；宿主 PID 不处于子域的 signal 命名空间。固定允许 `/dev/null`，不授权 `/dev` 整棵树或其他设备。

原生 DSH 使用同目录 hardlink 原子持久化会话，因此允许 `link/linkat` 通过 Landlock 检查；跨目录将只读文件或控制文件链接到可写区的对抗用例仍被拒绝。ABI 1 的跨目录 rename/link 限制保持，不因 CLI 兼容而放宽。

没有 host 网络路由。CLI 只能创建隔离 netns 内的 IPv4 TCP socket，不能创建 pathname AF_UNIX、raw/IP packet socket，也不能创建新 namespace 或提升网络能力。正常匿名 `socketpair` 可用于自己工具的本地通信。

可信 namespace relay 开启本域 loopback，将 `DEMIFLOW_CONTROL_ENDPOINT` 和 `DEMIFLOW_MODEL_ENDPOINT` 指向本域随机端口。每个通道只有启动时由控制器绑定的一个 host Unix socket 目标；不可信进程不能提供新 host path/地址、CONNECT 或新的转发目标。跨域依赖预先创建的匿名 socketpair，实际 host socket 在可信父侧连接。转发按请求序号隔离关闭残帧，限制连接、帧、字节和总时间。

模型认证与模型请求限制仍由固定 `model_broker.py` 处理。上游凭证、真实 provider/model、配额和 native account 实现留在可信侧；子 CLI 仅持有该任务的窄 token。模型通道传递模型请求/响应，不执行模型提出的工具。Codex argv 中唯一固定占位符 `__DEMIFLOW_MODEL_ENDPOINT__` 可被替换为实际本域 endpoint；没有 model channel 时拒绝该占位符，不做通用字符串或 shell 展开。

## 资源与证据边界

默认单任务 deadline 最多 3600 秒；有固定 NOFILE、单文件大小、CPU 和 core 限制。PID 1 每 50ms 观察自己的后代进程与 RSS；每 200ms 统计私有 HOME 全量大小，加上所有 source write 范围相对准入基线的正增长，并按 device/inode 去重计入本域进程仍持有 FD 的无名 regular file（已 unlink 文件、memfd/O_TMPFILE），合计超过 `private_bytes` 即终止进程域。新增文件/目录同样有条目预算；source 初始 inventory 最多 4096 项，每次扫描最多 8192 项，全域观察 FD 最多 8192 个。目录扫描使用 FD 与 NOFOLLOW，不追随链接，扫描竞态或不支持的 inode 也失败关闭。

`/proc` 读取、scope inventory、队列、配置、channel frame 和实现 hash 都有明确上限。**RSS、进程数和总写盘是采样保护，不是 cgroup/文件系统配额硬上限；RLIMIT_FSIZE 是单文件硬限。** 源码增长不会因位于 HOME 之外而完全逃过计量；采样间隔内的瞬时超额仍可能发生。

该后端不隐藏一切文件名/存在性等 metadata，也不保护免受其他受信宿主 root 进程主动干预。保证对象是 CLI 与其后代对未授权内容的读取和对受保护对象的修改、控制器进程/凭证与宿主服务通道。并发外部编辑和业务语义仍由当前内容指纹、门禁和 Review 处理。

`execution_facts('restricted')` 在本进程中按当前实现 hash 缓存一次真实 scratch 正反例；不可由配置 bool 置为成功。此结果是宿主 backend readiness，不能代替 provider 协议或任务验收。

每次实际 launch 都完整安装限制。只有可信 PID 1 经 CLOEXEC 错误通道确认 confined exec 成功，且启动前后的五模块 hash 相同，返回的 Popen 才带：

```json
{
  "execution_assurance": "enforced",
  "execution_evidence": {
    "backend": "linux-user-pid-net-landlock-seccomp-v1",
    "implementation": "实际实现内容 SHA-256",
    "execution_id": "本次启动独有随机身份"
  }
}
```

它不是 CLI 输出。控制器只在执行排空、候选身份相符后签 domain receipt；Forge 复核当前 backend implementation，旧版本 receipt 不自动获得当前 enforced 身份。`Popen.wait()` 等待外层 unshare 回收 PID 1，其退出路径已执行内核 PID namespace 清理。

## 已执行验证

```text
/yzp/zhaozy/yangzepeng/0905/env/bin/python -m pytest -q \
  tests/test_workbench_development_sandbox.py \
  tests/test_workbench_development_execution.py
28 passed in 6.69s
```

实际测试覆盖：授权读写；控制/只读文件 read/write/truncate/只读 open 截断/metadata；跨目录 hardlink；继承后的越权尝试；宿主 `/proc` 的 environ/fd/root；namespace、ptrace、process_vm、io_uring、openat2 与兼容 ABI；AF_UNIX/raw socket/host loopback；capability 集与 NNP；控制 FD 不继承；launcher SIGKILL、总 deadline、主 CLI 退出时 detached 后代一起终止；固定通道连续请求；HOME 链接投毒；readonly/verifier scope；私有磁盘超额；缺 channel 拒绝；唯一执行 identity 与实现绑定。测试不使用真实模型。

独立复审后的新增用例覆盖：runtime 与写路径双向相交、实际控制包及固定启动依赖默认保护、Python 直接脚本/shebang、verifier PYTHONPATH 的已授权路径正例与空段/别名/越界反例、source 多文件增长触发共享预算、unlink 后继续持有 FD 与 memfd 写入触发共享预算；以上写盘反例均保留外部历史 sentinel。

另用一次性临时 HOME 验证了真实 Node 24.10.0 的 subprocess、DSH 0.2.1-rc.2 `--version` 与 ACP initialize。随后使用原配置的 `deepseek-account` 模型 route，通过可信 native broker 与受限 proxy 完成 initialize 和 session/new：4 次窄 channel 连接，**0 次模型 stream 请求**。未打印凭证内容、未复制认证文件给子 CLI。Node 需要显式读取 `/etc/ssl/openssl.cnf`；未授权 `/etc/ssl/private`。

上述不是整体 DH 完成证明。真实模型生成、独立审阅、当前候选门禁和完整工作台试用由组合验收记录。最终是否给用户显示“受限执行通过”还需有效的开发、verifier 和 Review 证据，不能仅看本文件或 `mode=restricted`。
