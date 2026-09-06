"""demiflow 采集落盘机制：内容寻址 blob 原子写 + jsonl 追加清单 + 跨进程幂等去重。

自 collect_v2.op_sink 机制沉淀（2026-09-04，机制归引擎）：
- blob 原子写：临时文件与目标同目录（同文件系统）+ os.replace；
  内容寻址（同内容只落一份）由调用方的布局函数保证；
- 清单追加写：崩溃最多留一行坏行，读端容忍 JSONDecodeError；
- 跨进程幂等：fcntl 排他锁内「先吸收增量尾部再判重」（吸收式推进——
  只查特定键且 miss 也推进偏移会把区间内其他进程新键永久漏检）；
- 无锁 contains 快查：咨询语义（索引只含本进程与已吸收行，跨进程新行
  可能漏查——漏查时照常走写路径，锁内权威判定兜底，永不双写）。

业务注入：key_of（清单行 → 去重键集）、blob 路径布局、清单行构造。
"""

from __future__ import annotations

import asyncio
import fcntl
import json
import os
import time
from typing import Callable, Optional

KeyOf = Callable[[dict], list]


def atomic_write_bytes(path: str, data: bytes) -> None:
    """原子写字节：pid 唯一临时文件 + os.replace（同目录同文件系统）。

    内容寻址 blob 的并发同内容写安全：不同进程临时名互不碰撞，
    replace 后发者胜且内容相同；不同内容同路径只可能来自哈希碰撞
    （工程上忽略）。2026-09-04·D1 新增：行引用化后下载算子即时落
    blob 用（跨节点并发写的原子性由本函数保证）。
    """
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = f"{path}.{os.getpid()}.tmp"
    with open(tmp, "wb") as f:
        f.write(data)
    try:
        os.replace(tmp, path)
    except OSError:
        # cosfs 等对象存储 FUSE 的 rename 语义缺陷（tmp 刚写完即改名可
        # ENOENT）。回退直写终径：内容寻址下同内容并发写良性（同字节流
        # 自偏移 0 顺序写，任意交错终态一致；异内容同径=哈希碰撞，忽略）
        with open(path, "wb") as f:
            f.write(data)
        try:
            os.unlink(tmp)
        except OSError:
            pass


class AppendManifestStore:
    """追加式清单存储：幂等去重键为任意可哈希值（如 (内容哈希, 归属键)）。"""

    def __init__(self, *, manifest: str, lock_path: str) -> None:
        self.manifest = manifest
        self._lock_path = lock_path
        self._alock = asyncio.Lock()
        self._known: Optional[set] = None   # 去重键索引（本进程）
        self._scan_end = 0                  # 索引已覆盖到的清单字节偏移
        self._key_of: Optional[KeyOf] = None
        os.makedirs(os.path.dirname(manifest) or ".", exist_ok=True)

    # ------------------------------------------------------------------
    # 去重索引
    # ------------------------------------------------------------------

    def load_index(self, key_of: KeyOf) -> int:
        """全量扫清单建本进程去重索引（续跑依据），返回索引条数。

        坏行跳过（追加写崩溃残留的半行本就容忍）；索引各进程一份不同步，
        跨进程新行靠锁内吸收增量。
        """
        self._key_of = key_of
        known: set = set()
        if os.path.exists(self.manifest):
            with open(self.manifest, encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rec = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    known.update(key_of(rec))
            self._scan_end = os.path.getsize(self.manifest)
        self._known = known
        return len(known)

    def contains(self, key) -> bool:
        """无锁快查：键是否已在本进程索引（咨询语义，漏查由锁内兜底）。"""
        if self._known is None:
            raise RuntimeError("须先 load_index(key_of)")
        return key in self._known

    # ------------------------------------------------------------------
    # 写入
    # ------------------------------------------------------------------

    async def write(self, *, data: bytes, blob_path: str, key,
                    record: dict) -> bool:
        """写一条：blob 原子落盘（已存在不重写）+ 清单行追加。

        返回 True=已追加；False=同 key 已存在（幂等跳过，blob 与清单都不动）。
        blob_path 为绝对路径（布局由调用方决定）；record 由调用方构造完整
        （含其 path/溯源字段），本方法只负责机制。
        """
        if self._known is None:
            raise RuntimeError("须先 load_index(key_of)")
        async with self._alock:
            if key in self._known:           # 快路径：本进程索引命中
                return False
            done = await asyncio.to_thread(
                self._write_disk, data, blob_path, key, record)
            if done is False:                # 权威判定：锁内吸收后命中
                return False
        self._known.add(key)
        return True

    def _write_disk(self, data: bytes, blob_path: str, key, record: dict) -> bool:
        """blob 原子写 + 清单行追加（全程持 fcntl 跨进程锁）。

        返回 False = 权威判定撞车（吸收增量后同 key 已存在）。
        """
        with open(self._lock_path, "a") as lf:
            fcntl.flock(lf, fcntl.LOCK_EX)
            try:
                self._absorb_tail()
                if key in self._known:
                    return False
                os.makedirs(os.path.dirname(blob_path) or ".", exist_ok=True)
                if not os.path.exists(blob_path):
                    atomic_write_bytes(blob_path, data)
                with open(self.manifest, "a", encoding="utf-8") as f:
                    f.write(json.dumps(record, ensure_ascii=False) + "\n")
                self._scan_end = os.path.getsize(self.manifest)   # 推进含本行
                return True
            finally:
                fcntl.flock(lf, fcntl.LOCK_UN)

    def _absorb_tail(self) -> None:
        """锁内吸收增量尾部：索引偏移之后其他进程新追加的行，全部键进索引。

        吸收式推进是竞态正解：锁内文件状态已定格，推进偏移安全；
        增量区间通常只有几行几 KB。容忍坏行（尾部首行可能是残行）。
        """
        assert self._key_of is not None
        try:
            size = os.path.getsize(self.manifest)
        except FileNotFoundError:
            return
        if size <= self._scan_end:
            return
        with open(self.manifest, "rb") as f:
            f.seek(self._scan_end)
            tail = f.read().decode("utf-8", errors="replace")
        for line in tail.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            self._known.update(self._key_of(rec))
        self._scan_end = size
