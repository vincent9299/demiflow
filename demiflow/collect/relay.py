"""demiflow 中继平台件：COS 前缀队列中继的机制骨架（2026-09-21 自冷备链沉淀）。

机制归引擎、策略归消费方（:mod:`demiflow.collect` 哲学）。本模块只认
钩子/参数，不出现任何业务名（子树闭集、毒闸门日期、镜像布局、阈值
数字全部由消费方注入）。

沉淀自 123pan 冷备链首夜实战（2026-09-20/21，三机常驻、跨境劣化期
零数据丢失），关键机制按实战教训固化：

- **账本双库**：``PushLedger``（key→etag/state/unit）与 ``ConsumeLedger``
  （unit→md5/pan_id/state），schema 与首夜部署版逐列一致（断链可续的
  前提：新代码直接接管现役 ledger.db）；
- **背压三闸**（``Gates``）：盘位预算 / 下游积压（读下游心跳 JSON）/
  停机信号，过水位停产不停排；
- **流式封卷**（``TarPacker``）：散对象从不单独落盘，边算 md5 边顺序
  写 tar，卷满或 linger 超时封卷 + manifest；半卷 ``.tmp`` 永不上传；
- **心跳**（``Heartbeat``）：``_status/<node>.json`` 跨节点协调（上游
  读它决定是否停推）；
- **看门狗**（``Watchdog``）：requests/urllib 发送阻塞盲区的兜底——
  单元处理超过 deadline 即 os._exit 交由 supervisor 重拉（幂等恢复）；
- **死信**（``DeadLetter``）：jsonl 追加；注意死信 tar 只在节点重启时
  经 ``recover_pairs`` 重推（生产节点需外部 supervisor 巡检重启）。
"""
from __future__ import annotations

import hashlib
import io
import json
import os
import re
import shutil
import sqlite3
import tarfile
import threading
import time
from dataclasses import dataclass, field
from typing import Callable, Iterable, Optional

__all__ = ["PushLedger", "ConsumeLedger", "DeadLetter", "Gates", "TarPacker",
           "Heartbeat", "Watchdog", "recover_pairs", "md5_stream", "dir_size"]


def md5_stream(path: str) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def dir_size(path: str) -> int:
    total = 0
    for root, _, files in os.walk(path):
        for f in files:
            try:
                total += os.path.getsize(os.path.join(root, f))
            except OSError:
                pass
    return total


# ---------- 账本（schema 与 2026-09-20 部署版逐列一致） ----------

class PushLedger:
    """生产端账本：key → etag/size/state/unit。已推+已见都记；防重推/防重扫。"""

    def __init__(self, spool: str, filename: str = "ledger.db"):
        os.makedirs(spool, exist_ok=True)
        self.con = sqlite3.connect(os.path.join(spool, filename), timeout=60,
                                   check_same_thread=False)
        self.con.execute("PRAGMA journal_mode=WAL")
        self.con.execute("PRAGMA synchronous=NORMAL")
        self.con.execute("""CREATE TABLE IF NOT EXISTS objects(
            key TEXT PRIMARY KEY, etag TEXT, size INTEGER, state TEXT, unit TEXT, ts REAL)""")
        self.con.execute("CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v TEXT)")
        self.con.commit()
        self.lock = threading.Lock()

    def get(self, key: str):
        with self.lock:
            return self.con.execute(
                "SELECT etag,size,state FROM objects WHERE key=?", (key,)).fetchone()

    def seq(self) -> int:
        with self.lock:
            row = self.con.execute("SELECT v FROM meta WHERE k='seq'").fetchone()
            n = int(row[0]) + 1 if row else 1
            self.con.execute("INSERT OR REPLACE INTO meta VALUES('seq',?)", (str(n),))
            self.con.commit()
            return n

    def record_unit(self, rows: Iterable[tuple]) -> None:
        now = time.time()
        with self.lock:
            self.con.executemany(
                "INSERT OR REPLACE INTO objects VALUES(?,?,?,?,?,?)",
                [(k, e, s, st, u, now) for k, e, s, st, u in rows])
            self.con.commit()

    def snapshot_to(self, path: str) -> None:
        tgt = sqlite3.connect(path)
        with self.lock:
            self.con.backup(tgt)
        tgt.close()


class ConsumeLedger:
    """消费端账本：unit → size/md5/pan_file_id/state（done/dead* 终态可跳过）。"""

    def __init__(self, spool: str, filename: str = "ledger.db"):
        os.makedirs(spool, exist_ok=True)
        self.con = sqlite3.connect(os.path.join(spool, filename), timeout=60,
                                   check_same_thread=False)
        self.con.execute("PRAGMA journal_mode=WAL")
        self.con.execute("PRAGMA synchronous=NORMAL")
        self.con.execute("""CREATE TABLE IF NOT EXISTS units(
            rel TEXT PRIMARY KEY, size INTEGER, md5 TEXT, pan_file_id INTEGER,
            state TEXT, ts REAL)""")
        self.con.commit()
        self.lock = threading.Lock()

    def get(self, rel: str):
        """返回 (state, md5)——同路径新版本判定依据(路径 done 但 md5 变了=要重消费)。"""
        with self.lock:
            return self.con.execute(
                "SELECT state, md5 FROM units WHERE rel=?", (rel,)).fetchone()

    def set(self, rel: str, size: int, md5: Optional[str],
            pan_file_id: Optional[int], state: str) -> None:
        with self.lock:
            self.con.execute(
                "INSERT OR REPLACE INTO units VALUES(?,?,?,?,?,?)",
                (rel, size, md5, pan_file_id, state, time.time()))
            self.con.commit()

    def counts(self) -> dict:
        with self.lock:
            return dict(self.con.execute(
                "SELECT state, COUNT(*) FROM units GROUP BY state").fetchall())

    def snapshot_to(self, path: str) -> None:
        tgt = sqlite3.connect(path)
        with self.lock:
            self.con.backup(tgt)
        tgt.close()


# ---------- 死信 ----------

class DeadLetter:
    """jsonl 追加式死信档（仅记录；tar/单元本体留在 spool 等重启恢复）。"""

    def __init__(self, spool: str, filename: str = "dead_units.jsonl"):
        os.makedirs(spool, exist_ok=True)
        self.path = os.path.join(spool, filename)

    def add(self, kind: str, info: dict) -> None:
        with open(self.path, "a") as f:
            f.write(json.dumps({"kind": kind, "info": info,
                                "ts": time.time()}) + "\n")


# ---------- 背压三闸 ----------

class Gates:
    """盘位预算 + 下游心跳积压闸 + 停机信号。

    ``status_reader``：() -> dict|None，读下游心跳 JSON（backlog_bytes/ts）；
    返回 None（读不到）默认放行——自身网络抖动不该停上游。
    ``backlog_max``/``stale_max``：下游积压/心跳超龄停推线。
    """

    def __init__(self, spool: str, budget_bytes: int,
                 status_reader: Optional[Callable[[], Optional[dict]]] = None,
                 backlog_max: int = 50 * 1024 ** 3,
                 stale_max: float = 24 * 3600.0,
                 disk_floor: int = 10 * 1024 ** 3,
                 strict_status: bool = False):
        self.spool, self.budget = spool, budget_bytes
        self.status_reader = status_reader
        self.backlog_max, self.stale_max = backlog_max, stale_max
        self.disk_floor, self.strict = disk_floor, strict_status
        self._stop = threading.Event()
        self._status_ts, self._status = 0.0, None

    def stop(self) -> None:
        self._stop.set()

    @property
    def stopped(self) -> bool:
        return self._stop.is_set()

    def disk_ok(self) -> bool:
        try:
            if shutil.disk_usage(self.spool).free < self.disk_floor:
                return False
        except OSError:
            return True
        return dir_size(self.spool) < self.budget

    def downstream_ok(self) -> bool:
        if not self.status_reader:
            return True
        now = time.time()
        if now - self._status_ts >= 60:
            try:
                self._status = self.status_reader()
            except Exception:
                self._status = None          # 读不到: 不拦
            self._status_ts = now
        s = self._status
        if s is None:
            return True
        if s == "missing":
            return not self.strict
        try:
            if float(s.get("backlog_bytes", 0)) >= self.backlog_max:
                return False
            if now - float(s.get("ts", 0)) > self.stale_max:
                return False
        except (TypeError, ValueError):
            return True
        return True

    def wait_open(self, poll: float = 30.0) -> None:
        """过水位则阻塞等待（停产不停排：上传侧仍在排水）。"""
        while not self._stop.is_set() and not (self.disk_ok() and self.downstream_ok()):
            time.sleep(poll)


# ---------- 流式封卷 ----------

@dataclass
class SealSpec:
    """封卷策略：卷上限 / linger 秒 / 卷内成员名映射（业务注入）。"""
    cap_bytes: int = 4 * 1024 ** 3
    linger_s: float = 600.0
    member_name: Callable[[str], str] = lambda key: key   # COS key → tar 内路径


class TarPacker:
    """散对象流式封卷器（单线程独占）：add() 缓冲在内存的只有当前对象。

    封卷回调 ``on_seal(tar_path, manifest_path, unit_rel, manifest_lines)``
    由节点注入（上传+记账+清理）；卷路径 ``<spool>/blobs/<date>/
    <host_tag>-part-<seq:06d>.tar``，半卷 ``.tar.tmp`` 永不上传、重启清理。
    """

    def __init__(self, spool: str, ledger: PushLedger, host_tag: str,
                 unit_prefix: str, on_seal: Callable,
                 spec: SealSpec | None = None):
        self.spool, self.ledger, self.host_tag = spool, ledger, host_tag
        self.unit_prefix, self.on_seal = unit_prefix, on_seal
        # field(default_factory=...) 只在 dataclass 字段上生效；普通 __init__
        # 参数必须显式构造，否则 self.spec 是 Field 对象而非 SealSpec。
        self.spec = spec if spec is not None else SealSpec()
        self._reset()

    def _reset(self):
        self.tar = self.base = None
        self.manifest: list[str] = []
        self.size = 0
        self.last_add = 0.0

    def _open(self):
        date = time.strftime("%Y%m%d")
        seq = self.ledger.seq()
        base = os.path.join(self.spool, "blobs", date,
                            f"{self.host_tag}-part-{seq:06d}")
        os.makedirs(os.path.dirname(base), exist_ok=True)
        self.base = base
        self.path = base + ".tar.tmp"
        self.tar = tarfile.open(self.path, "w")

    def add(self, key: str, etag: str, buf: bytes):
        if self.tar is None:
            self._open()
        md5 = hashlib.md5(buf).hexdigest()
        ti = tarfile.TarInfo(self.spec.member_name(key))
        ti.size = len(buf)
        self.tar.addfile(ti, io.BytesIO(buf))
        self.manifest.append(json.dumps(
            {"key": key, "md5": md5, "size": len(buf), "etag": etag}))
        self.size += 1024 + len(buf)
        self.last_add = time.time()
        if self.size >= self.spec.cap_bytes:
            self.seal()

    def maybe_linger_seal(self):
        if (self.tar is not None and self.manifest
                and time.time() - self.last_add > self.spec.linger_s):
            self.seal()

    def seal(self):
        if self.tar is None:
            return
        self.tar.close()
        tar_path = self.base + ".tar"
        os.rename(self.path, tar_path)
        man_path = self.base + ".manifest.jsonl"
        with open(man_path, "w") as f:
            f.write("\n".join(self.manifest) + "\n")
        date_dir = os.path.basename(os.path.dirname(tar_path))
        unit_rel = f"{self.unit_prefix}/{date_dir}/{os.path.basename(tar_path)}"
        manifest_lines = self.manifest
        self._reset()
        self.on_seal(tar_path, man_path, unit_rel, manifest_lines)


# ---------- 心跳 ----------

class Heartbeat:
    """_status/<name>.json 读写：{ts, backlog_bytes, host, **extra}。"""

    def __init__(self, put_json: Callable[[str, dict], None], key: str,
                 host: Optional[str] = None):
        self._put, self.key = put_json, key
        self.host = host or os.uname().nodename

    def emit(self, backlog_bytes: float, **extra) -> None:
        self._put(self.key, {"ts": time.time(), "backlog_bytes": backlog_bytes,
                             "host": self.host, **extra})

    @staticmethod
    def reader(get_json: Callable[[str], Optional[dict]], key: str,
               max_age: float = 120.0) -> Optional[dict]:
        """带时效的读（供 Gates.status_reader；异常返回 None=不拦）。"""
        try:
            d = get_json(key)
        except Exception:
            return None
        if not isinstance(d, dict):
            return None
        if time.time() - float(d.get("ts", 0)) > max_age:
            return None               # 心跳超龄视同读不到, 不拦(不误停)
        return d


# ---------- 看门狗（requests/urllib 发送阻塞盲区兜底） ----------

class Watchdog:
    """单元级 deadline：超时 os._exit(1) 交外层 supervisor 重拉。

    盲区机理：对端半死连接+零窗口时，PUT 的 send()/recv() 可无限挂起
    （timeout 只覆盖连接建立与读间隙，不覆盖发送阻塞）。2026-09-20 冷备
    链首夜 sg1/cn1 各两次实证；进程级自戕+supervisor 幂等恢复是最简解。
    """

    def __init__(self, deadline_s: float = 1800.0):
        self.deadline = deadline_s
        self._armed_at = 0.0
        self._t = threading.Timer(self.deadline, self._fire)
        self._t.daemon = True

    def _fire(self):
        os._exit(3)                    # 3 = 看门狗击穿(账本幂等, 重启即续)

    def __enter__(self):
        self._armed_at = time.time()
        self._t = threading.Timer(self.deadline, self._fire)
        self._t.daemon = True
        self._t.start()
        return self

    def feed(self, min_gap_s: float = 60.0) -> None:
        """进度喂狗：有真实进展时续期死线，稳态慢传不再被一刀切。

        距上次布防不足 ``min_gap_s`` 的续期直接忽略（切片高频免抖动）；
        零进展（发送阻塞盲区）无人喂狗，到期照杀——兜底语义不变。
        2026-09-22 边缘速度段实证：4.3G tar 稳态 ~2.3MB/s 总耗时恰越
        1800s 死线，近完成传输被反复击穿报废，故由上层按片速率喂狗。
        """
        now = time.time()
        if now - self._armed_at < min_gap_s:
            return
        self._t.cancel()
        self._armed_at = now
        self._t = threading.Timer(self.deadline, self._fire)
        self._t.daemon = True
        self._t.start()

    def __exit__(self, *exc):
        self._t.cancel()
        return False


# ---------- 崩溃恢复 ----------

def recover_pairs(blobs_dir: str) -> list[tuple]:
    """成对收集 (manifest, tar)——孤本不删不推（首夜教训：逐文件迭代曾把
    已封 tar+manifest 当孤儿误删）。半卷 .tmp 直接清理。"""
    pairs, mans, tars = [], {}, {}
    for root, _, files in os.walk(blobs_dir):
        for f in files:
            p = os.path.join(root, f)
            if f.endswith(".tar.tmp"):
                os.remove(p)
            elif f.endswith(".manifest.jsonl"):
                mans[p[:-len(".manifest.jsonl")]] = p
            elif f.endswith(".tar"):
                tars[p[:-len(".tar")]] = p
    for base in sorted(set(mans) & set(tars)):
        pairs.append((tars[base], mans[base]))
    return pairs
