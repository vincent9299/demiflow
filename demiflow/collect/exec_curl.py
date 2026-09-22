"""demiflow curl 执行模式（2026-09-18，fleet 实战沉淀上移为引擎原语）。

背景：asyncio/httpx 长驻进程在腾讯 VM 上会静默失去全部连接（事件循环
空转、产出归零）；短命 curl 子进程同机始终可靠。本模块把该传输模式
与 AIMD 节拍固化为框架原语，供 fleet 采集器消费。

契约：
- curl_fetch：单 URL 一次完整取（短命子进程、-D 落响应头、字节数封顶、
  分类重试：429/5xx 退避 + 尊重 Retry-After，404/403 等确定性失败不重试）
- RateGovernor：AIMD 动态节拍（成功稳步提速、限速立刻减半、窗口占比
  超阈熔断冷却），全部变速写 rate.log 可观测
- EgressIdentity：出口唯一身份分配（proxy:<ip>/host:<rN> → UA 表），
  查不到告警回退，绝不静默换身份

与 net.py 的关系：net.py 是 httpx 双池底座（检索/低量场景仍首选）；
本模块是 curl 子进程底座（大批量图片下载、VM 长跑场景）。二者共用
SOURCE_LIMITS 注册与身份 UA 由消费方自带的约定。
"""
from __future__ import annotations

import collections
import json
import os
import re
import subprocess
import time
import zlib

RETRY_DELAYS_DEFAULT = (0, 5, 15, 30)


class CurlResult:
    """一次 fetch 的终态：ok / throttled（429/5xx 耗尽）/ hard（确定性失败）。"""

    __slots__ = ("ok", "data", "status", "reason", "throttled")

    def __init__(self, ok=False, data=b"", status=0, reason="", throttled=False):
        self.ok, self.data = ok, data
        self.status, self.reason, self.throttled = status, reason, throttled


def parse_retry_after(hdr_path: str) -> float:
    """从 curl -D 落盘的响应头里取 Retry-After 秒数；无则 0。"""
    try:
        with open(hdr_path, "r", encoding="latin-1") as f:
            for line in f:
                k, _, v = line.partition(":")
                if k.strip().lower() == "retry-after":
                    return max(0.0, float(v.strip() or 0))
    except (OSError, ValueError):
        pass
    return 0.0


def curl_fetch(url: str, *, ua: str, referer: str = "", proxy: str = "",
               cap_bytes: int = 64 << 20, timeout: int = 180,
               retries: tuple = RETRY_DELAYS_DEFAULT,
               tmp_path: str = "") -> CurlResult:
    """短命 curl 取一个 URL。成功时 data 为响应体（≤cap_bytes）。

    referer 为空则不携带；UA 必填（身份 UA 由消费方从 EgressIdentity 取）。
    """
    tmp = tmp_path or f"/tmp/demiflow_curl.{os.getpid()}"
    hdr = tmp + ".hdr"
    for delay in retries:
        if delay:
            time.sleep(delay)
        cmd = ["curl", "-sSLk", "--max-time", str(timeout),
               "--max-filesize", str(cap_bytes), "-D", hdr,
               "-A", ua, "-w", "%{http_code}", "-o", tmp, url]
        if referer:
            cmd[1:1] = ["-e", referer]
        if proxy:
            cmd[1:1] = ["-x", proxy]
        try:
            r = subprocess.run(cmd, timeout=timeout + 15,
                               capture_output=True, text=True)
        except subprocess.TimeoutExpired:
            continue
        if r.returncode != 0:
            continue
        status = (r.stdout or "").strip()
        if status == "429" or status.startswith("5"):
            ra = parse_retry_after(hdr)
            if ra > 0:
                time.sleep(ra)
            continue
        if status and status != "200":
            for p in (tmp, hdr):
                if os.path.exists(p):
                    os.unlink(p)
            return CurlResult(False, b"", int(status or 0), f"http:{status}")
        if not os.path.exists(tmp):
            continue
        data = open(tmp, "rb").read()
        os.unlink(tmp)
        if os.path.exists(hdr):
            os.unlink(hdr)
        if len(data) > cap_bytes:
            return CurlResult(False, b"", 200, "over_cap")
        return CurlResult(True, data, 200, "")
    for p in (tmp, hdr):
        if os.path.exists(p):
            os.unlink(p)
    return CurlResult(False, b"", 0, "retries_exhausted", throttled=True)


class RateGovernor:
    """AIMD 动态节拍：25 连胜 +step 提速；任一限速减半；窗口过半且占比
    超阈则熔断冷却（基础值×2^n，上限 1h）。全部事件记 out_dir/meta/rate.log。"""

    def __init__(self, out_dir, start, lo, hi, window=60, trip=0.05,
                 cooldown=600.0, ai_every=25, ai_step=0.02):
        self.log = os.path.join(out_dir, "meta", "rate.log")
        os.makedirs(os.path.dirname(self.log), exist_ok=True)
        self.rps = start
        self.lo, self.hi = lo, hi
        self.window, self.trip, self.cooldown = window, trip, cooldown
        self.ai_every, self.ai_step = ai_every, ai_step
        self.hist = collections.deque(maxlen=window)
        self.ok_streak = 0
        self.trips = 0
        self._emit("start")

    def _emit(self, event, extra=""):
        with open(self.log, "a", encoding="utf-8") as f:
            f.write(json.dumps({"ts": round(time.time(), 1), "event": event,
                                "rps": round(self.rps, 3), "extra": extra}) + "\n")

    def on_ok(self):
        self.hist.append(False)
        self.ok_streak += 1
        if self.ok_streak >= self.ai_every and self.rps < self.hi:
            self.rps = min(self.hi, self.rps + self.ai_step)
            self.ok_streak = 0
            self._emit("ai")

    def on_throttle(self, sleep=None):
        """限速反馈；sleep 注入测试用（生产走 time.sleep）。"""
        self.hist.append(True)
        self.ok_streak = 0
        self.rps = max(self.lo, self.rps / 2.0)
        self._emit("md")
        if (len(self.hist) >= self.window // 2
                and sum(self.hist) / len(self.hist) > self.trip):
            self.trips += 1
            wait = min(self.cooldown * (2 ** (self.trips - 1)), 3600.0)
            self._emit("trip", f"wait={wait:.0f}s trips={self.trips}")
            (sleep or time.sleep)(wait)
            self.hist.clear()
            self.rps = self.lo

    def pace(self):
        time.sleep(1.0 / self.rps)


class EgressIdentity:
    """出口唯一身份：assign_tsv 形如 `proxy:<ip>\\tUA` / `host:<rN>\\tUA`。

    取法：proxy → proxy:<ip>；直连 → env_file（`UA=...`，部署器按 ssh 别名
    写入，规避远程真实主机名≠别名问题）→ host:<主机名> → crc 回退（告警）。
    """

    def __init__(self, assign_tsv: str, pool_file: str = "", env_file: str = ""):
        self.assign = {}
        try:
            with open(assign_tsv, encoding="utf-8") as f:
                for line in f:
                    k, _, v = line.rstrip("\n").partition("\t")
                    if k and v:
                        self.assign[k] = v
        except OSError:
            pass
        self.pool = []
        if pool_file:
            try:
                self.pool = [l.strip() for l in open(pool_file, encoding="utf-8")
                             if l.strip()]
            except OSError:
                pass
        self.env_file = env_file

    def pick(self, proxy: str = "", fallback_key: str = "") -> str:
        import socket
        if proxy:
            m = re.search(r"@(\d+\.\d+\.\d+\.\d+):", proxy)
            key = f"proxy:{m.group(1)}" if m else ""
        else:
            if self.env_file:
                try:
                    for line in open(self.env_file, encoding="utf-8"):
                        if line.startswith("UA=") and line[3:].strip():
                            return line[3:].strip()
                except OSError:
                    pass
            host = fallback_key or socket.gethostname().split(".")[0]
            key = f"host:{host}"
        ua = self.assign.get(key)
        if ua:
            return ua
        if self.assign:
            print(f"[exec_curl] 警告：分配表无 {key or '(未知出口)'}，crc 回退", flush=True)
        if self.pool:
            return self.pool[zlib.crc32((proxy or key).encode()) % len(self.pool)]
        return ""
