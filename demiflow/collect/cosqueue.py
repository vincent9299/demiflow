"""demiflow COS 队列：分批生产 / 认领 / 完成 / 释放 / 巡检回收。

第三代采集队列机制沉淀（2026-09-20 实战定型，机制归引擎）：
- **控制面沉进对象存储**：worker 自组织认领，指挥机宕机不影响在跑；
- **排他认领（R2 修复）**：认领对象是每批唯一的 ``claims/<bid>``，用
  ``If-None-Match: *`` 条件 PUT 原子创建，天然只有一个 worker 成功；
  认领体携带 owner token，complete/release 先校验归属，租约过期被新
  owner 接管的批，旧 owner 无法完成或释放。旧版 ``claims/<bid>.<worker>``
  多对象认领不具备排他性，仅作过渡期审计残留被巡检兼容读取；
- **成功才 complete**：执行失败绝不写 done——实战中一次依赖缺失让
  374 批 × 2000 行被空标 done（一行未下、静默丢失），此语义以三处
  断言固化在测试里；
- 失败由调用方 release + 退避；崩溃残留的认领由 requeue_stale 巡检回收。

布局（队列前缀下三目录）::

    batches/b000000.jsonl.gz   任务批（jsonl+gzip）
    claims/<bid>               认领标记（body: {"worker","token","ts"}）
    done/<bid>                 完成标记（body: {"worker","token","ts","rc"}）

契约（全部现算，不落盘状态）：
- ``produce(rows, rows_per_batch)``：流式切批上传，返回批 id 列表；
- ``claim(worker)``：条件创建认领一个待办批，空返回 None；
- ``complete(handle, rc=0)`` / ``release(handle)``：归属校验后生效；
- ``fetch_batch(handle)``：下载+解压+逐行解析（坏行容忍跳过）；
- ``snapshot()``：{batches, claimed, done} 计数与集合；
- ``requeue_stale(max_age_s)``：回收超龄未完成认领，返回回收批 id。
"""
from __future__ import annotations

import gzip
import io
import json
import time
import uuid
from dataclasses import dataclass
from typing import Callable, Iterable, Optional

from .cosio import COSIO

__all__ = ["COSQueue", "BatchHandle", "QueueSnapshot"]


@dataclass(frozen=True)
class BatchHandle:
    """一次成功认领的批。complete/release 只对认领者语义成立。"""

    bid: str
    key: str
    claim_key: str
    worker: str
    token: str = ""


@dataclass(frozen=True)
class QueueSnapshot:
    batches: frozenset
    claimed: frozenset
    done: frozenset

    @property
    def todo(self) -> frozenset:
        return self.batches - self.claimed - self.done


def _bid_of(key: str) -> str:
    return key.rsplit("/", 1)[-1].split(".")[0]


class _NotClaimOwner(RuntimeError):
    """complete/release 时的归属校验失败：认领已易主或丢失。"""


class COSQueue:
    """以 COS 为唯一协调面的任务队列（无租约续期：批粒度小、崩则回收）。"""

    def __init__(self, io: COSIO, prefix: str,
                 clock: Callable[[], float] = time.time):
        self.io, self.prefix, self._clock = io, prefix.rstrip("/"), clock

    def _k(self, rel: str) -> str:
        return f"{self.prefix}/{rel}"

    # ---- 生产 ----

    def produce(self, rows: Iterable[dict], rows_per_batch: int = 2000,
                start_index: int = 0, skip_existing: bool = False,
                upload_workers: int = 8) -> list:
        """流式切批 + **并发上传**（跨域单 PUT 延迟 ~1.7s，串行是大瓶颈）。

        幂等性由 skip_existing 的 HEAD 咨询与重跑的同 bid 同内容覆盖共同保证。
        返回按批号有序的批 id 列表。"""
        import concurrent.futures as cf
        out, buf, n = [], [], start_index
        futures = []
        with cf.ThreadPoolExecutor(upload_workers) as ex:
            def emit(batch_rows, idx):
                return ex.submit(self._upload_batch, idx, batch_rows,
                                 skip_existing)
            for row in rows:
                buf.append(row)
                if len(buf) >= rows_per_batch:
                    futures.append(emit(buf, n))
                    buf, n = [], n + 1
            if buf:
                futures.append(emit(buf, n))
            for f in futures:
                out.append(f.result())
        return out

    def _upload_batch(self, index: int, rows: list, skip_existing: bool) -> str:
        bid = f"b{index:06d}"
        key = self._k(f"batches/{bid}.jsonl.gz")
        if skip_existing and self.io.head(key) is not None:
            return bid
        bio = io.BytesIO()
        with gzip.GzipFile(fileobj=bio, mode="wb", mtime=0) as gz:
            for row in rows:
                gz.write((json.dumps(row, ensure_ascii=False) + "\n").encode())
        self.io.put_bytes(key, bio.getvalue())
        return bid

    # ---- 状态 ----

    def snapshot(self) -> QueueSnapshot:
        b = frozenset(_bid_of(k) for k in self.io.list_prefix(self._k("batches/")))
        c = frozenset(_bid_of(k) for k in self.io.list_prefix(self._k("claims/")))
        d = frozenset(_bid_of(k) for k in self.io.list_prefix(self._k("done/")))
        return QueueSnapshot(b, c, d)

    # ---- 消费 ----

    def claim(self, worker: str) -> Optional[BatchHandle]:
        """排他认领 todo 首批。

        对每批唯一的 ``claims/<bid>`` 做 ``If-None-Match: *`` 条件 PUT：
        只有对象不存在时才创建成功（412 = 已有 owner），多 worker 并发
        竞争同一批时恰有一个成功。认领体带一次性 owner token。
        """
        snap = self.snapshot()
        token = uuid.uuid4().hex
        for bid in sorted(snap.todo):
            claim_key = self._k(f"claims/{bid}")
            body = json.dumps({"worker": worker, "token": token,
                               "ts": self._clock()}).encode()
            st, _, _ = self.io.call("PUT", claim_key, data=body, timeout=60.0,
                                    headers={"If-None-Match": "*"})
            if st in (200, 204):
                return BatchHandle(bid, self._k(f"batches/{bid}.jsonl.gz"),
                                   claim_key, worker, token=token)
            continue                              # 已被他人认领或本次未成
        return None

    def _current_claim(self, handle: BatchHandle) -> Optional[dict]:
        st, _, got = self.io.call("GET", handle.claim_key, timeout=60.0)
        if st != 200:
            return None
        try:
            claim = json.loads(got)
        except Exception:
            return None
        return claim if isinstance(claim, dict) else None

    def _require_owner(self, handle: BatchHandle) -> None:
        claim = self._current_claim(handle)
        if claim is None:
            raise _NotClaimOwner(
                f"claim for {handle.bid} is gone; it cannot be completed "
                "or released by a stale owner",
            )
        if claim.get("token") != handle.token or claim.get("worker") != handle.worker:
            raise _NotClaimOwner(
                f"{handle.worker} no longer owns {handle.bid} "
                f"(current owner: {claim.get('worker')!r})",
            )

    def complete(self, handle: BatchHandle, rc: int = 0) -> None:
        """只在批执行成功后调用（失败路径走 release）。

        归属校验失败（租约过期被回收/新 owner 接管）抛 _NotClaimOwner，
        不写 done——防止旧 worker 把新 owner 正在执行的批标记完成。
        """
        self._require_owner(handle)
        body = json.dumps({"worker": handle.worker, "token": handle.token,
                           "ts": self._clock(), "rc": rc}).encode()
        self.io.call("PUT", self._k(f"done/{handle.bid}"),
                     data=body, timeout=60.0)

    def release(self, handle: BatchHandle) -> None:
        """释放认领（执行失败 / 主动放弃）。

        认领已不存在视为幂等成功；存在但 token 不匹配说明批已易主，
        拒绝释放（抛 _NotClaimOwner），不得动新 owner 的认领。
        """
        claim = self._current_claim(handle)
        if claim is None:
            return                                # 已释放/已被回收：幂等
        if claim.get("token") != handle.token or claim.get("worker") != handle.worker:
            raise _NotClaimOwner(
                f"{handle.worker} cannot release {handle.bid}: "
                f"currently owned by {claim.get('worker')!r}",
            )
        self.io.delete(handle.claim_key)

    def fetch_batch(self, handle: BatchHandle) -> list:
        """下载批 → jsonl 解析；坏行容忍跳过（与追加清单读端口径一致）。"""
        raw = self.io.get_bytes(handle.key)
        if raw is None:
            raise RuntimeError(f"批对象不存在：{handle.key}")
        rows = []
        with gzip.GzipFile(fileobj=io.BytesIO(raw)) as gz:
            for line in io.TextIOWrapper(gz, encoding="utf-8"):
                line = line.strip()
                if not line:
                    continue
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
        return rows

    # ---- 巡检 ----

    def requeue_stale(self, max_age_s: float,
                      now: Optional[float] = None) -> list:
        """回收「已认领、未完成、超龄」的批认领（worker 崩溃残留）。

        返回回收的批 id 列表。done 已存在的批不回收（其认领标记留作审计；
        如需清理另有消费方口径）。"""
        now = now if now is not None else self._clock()
        snap = self.snapshot()
        reclaimed = []
        for key in self.io.list_prefix(self._k("claims/")):
            bid = _bid_of(key)
            if bid in snap.done:
                continue
            _, _, got = self.io.call("GET", key, timeout=60.0)
            try:
                ts = json.loads(got).get("ts", 0)
            except Exception:
                ts = 0
            if now - float(ts) > max_age_s:
                self.io.delete(key)
                reclaimed.append(bid)
        return reclaimed
