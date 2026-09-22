"""demiflow 队列执行器：认领 → 消费方批算子 → complete/release 的常驻循环。

第三代 queue_worker 机制沉淀（2026-09-20，机制归引擎）：
- 队列语义全部委托 cosqueue（成功才 complete / 失败释放+退避）；
- **身份注入**：出口身份（代理/UA 等）由部署侧 KEY=VALUE env 文件提供，
  引擎不理解身份语义、原样透传给算子（值可含括号等任意字符——实战中
  UA 含括号曾被 shell source 炸掉，此处纯 split 不经 shell）；
- **进程外守护**：本循环不自愈（COS 硬故障上抛退出），存活性交由
  systemd-run / 保姆等部署层（fleet.py 一系）——与第三代运维口径一致；
- 队列排空即退出（返回 "drained"）；如需常驻轮询传 idle_poll。

消费方只写批算子（策略全在此）::

    def my_op(rows, ctx):
        for row in rows:
            data = fetch(row["url"])            # 传输用 exec_curl/net
            ctx.io.put_bytes(row["blob_key"], data)

    run(my_op, queue=q, worker="w8", identity_file="/tmp/qw_env_8")
"""
from __future__ import annotations

import os
import time
import traceback
from dataclasses import dataclass, field
from typing import Callable, Optional

from .cosio import COSIO
from .cosqueue import COSQueue, _NotClaimOwner

__all__ = ["RunContext", "load_identity", "run"]

Executor = Callable[[list, "RunContext"], None]


@dataclass
class RunContext:
    """批算子可见的机制面：身份表 + COS 原语 + 工作目录 + 本批句柄。"""

    worker: str
    workdir: str
    io: COSIO
    identity: dict = field(default_factory=dict)
    handle: object = None


def load_identity(path: str) -> dict:
    """KEY=VALUE 行式身份文件；空行/注释跳过，值取首个 = 后整段。"""
    out = {}
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, _, v = line.partition("=")
            out[k.strip()] = v
    return out


def run(executor: Executor, *, queue: COSQueue, worker: str,
        workdir: str = ".", identity_file: Optional[str] = None,
        on_failure_sleep: float = 60.0,
        idle_poll: Optional[float] = None,
        log: Callable[[str], None] = print,
        sleep: Callable[[float], None] = time.sleep) -> str:
    """常驻认领循环。返回 "drained"（队列排空）。

    失败语义：算子抛异常 → 释放认领 + 退避后重试（批回 todo）；
    COS 硬故障（重试耗尽）→ 上抛退出，交部署层重启。
    """
    identity = load_identity(identity_file) if identity_file else {}
    ctx = RunContext(worker=worker, workdir=workdir, io=queue.io,
                     identity=identity)
    os.makedirs(workdir, exist_ok=True)
    while True:
        handle = queue.claim(worker)
        if handle is None:
            if idle_poll is None:
                return "drained"
            sleep(idle_poll)
            # 等待期间到达的新批：保存第二次认领的句柄，直接进入执行分支
            # （丢弃句柄会让已认领的批从 todo 消失且永不执行）。
            handle = queue.claim(worker)
            if handle is None:
                return "drained"
        ctx.handle = handle
        try:
            rows = queue.fetch_batch(handle)
            executor(rows, ctx)
        except Exception:
            log(f"[qr] {worker} 批 {handle.bid} 失败，释放认领退避重试：\n"
                f"{traceback.format_exc(limit=8)}")
            try:
                queue.release(handle)
            except _NotClaimOwner:
                log(f"[qr] {worker} 批 {handle.bid} 认领已易主，跳过释放")
            sleep(on_failure_sleep)
            continue
        try:
            queue.complete(handle)
            log(f"[qr] {worker} 完成 {handle.bid}")
        except _NotClaimOwner:
            # 执行期间认领被回收并易主：本 worker 的成功结果不再记账，
            # 批由当前 owner 继续负责（幂等算子重放可接受）。
            log(f"[qr] {worker} 批 {handle.bid} 执行成功但认领已易主，"
                "不写 done；由当前 owner 负责")
