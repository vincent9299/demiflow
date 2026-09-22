"""demiflow fleet 运行器（2026-09-18，systemd-run 托管发射 + 巡检）。

背景教训（全部实战沉淀）：
- ssh 会话结束后某些环境的会话/cgroup 清杀器会收走 setsid 进程
  （逃得出会话逃不出 cgroup）→ 本模块远端常驻一律 sudo systemd-run 托管；
- bash 启动器三连坑：while-read 被 ssh 吃 stdin、括号/引号多层转义断裂、
  pgrep 自匹配 → 本模块 argv 直传 ssh + 守卫用 systemctl is-active；
- 幂等是硬要求：guard 命中即跳过，重复发射靠内容寻址存储去重兜底。

对外原语：
- worker_plan()         N worker → (unit, host, proxy) 分配计划
- deploy_worker()       单 worker systemd-run 发射（幂等）
- deploy_fleet()        全量错峰发射
- patrol_fleet()        一次巡检：存活数 / manifest 行数 / rate.log 熔断数
- stop_fleet()          按 unit 前缀全停
消费方（项目侧）提供：payload 构造函数（每 worker 的完整远端命令）。
"""
from __future__ import annotations

import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field


def sh(host: str, script: str, timeout: int = 40) -> subprocess.CompletedProcess:
    """argv 直传 ssh（无本地 shell、无引号地狱）。"""
    return subprocess.run(["timeout", str(timeout), "ssh", "-o",
                           "ConnectTimeout=15", host, script],
                          capture_output=True, text=True)


@dataclass
class FleetConfig:
    hosts: list = field(default_factory=list)          # ["r1", ..., "r20"]
    proxy_specs: list = field(default_factory=list)    # ["ip:port:user:pass", ...]
    workdir: str = "~/wk_backfill"
    unit_prefix: str = "dfw"                           # systemd unit = dfw{w}


def worker_plan(n: int, cfg: FleetConfig) -> list[dict]:
    """前 len(proxy_specs) 个 worker 绑代理（轮转主机宿主），其余直连
    （每机一个）。返回 [{w, host, proxy, unit}]。"""
    plan = []
    nd = max(n - len(cfg.proxy_specs), 0)
    for w in range(n):
        if w < len(cfg.proxy_specs):
            ip, port, user, pwd = cfg.proxy_specs[w].split(":")
            proxy = f"http://{user}:{pwd}@{ip}:{port}"
            host = cfg.hosts[w % len(cfg.hosts)]
        else:
            proxy = ""
            host = cfg.hosts[(w - len(cfg.proxy_specs)) % len(cfg.hosts)]
        plan.append({"w": w, "host": host, "proxy": proxy,
                     "unit": f"{cfg.unit_prefix}{w}"})
    return plan


def deploy_worker(item: dict, payload: str, cfg: FleetConfig,
                  log_name: str = "") -> str:
    """单 worker 幂等发射。payload 为远端完整命令（不含重定向），
    引号由本函数负责：payload 不得包含单引号。返回 receipt 字符串。"""
    host, unit = item["host"], item["unit"]
    if guard_running(host, unit):
        return f"{unit}@{host} already"
    log = log_name or f"{unit}.log"
    script = (f"sudo -n systemd-run --unit={unit} --uid=1000 --gid=1000 "
              f"--property=Restart=always --property=RestartSec=15 "
              f"bash -c 'cd {cfg.workdir} && {payload} > {log} 2>&1'")
    r = sh(host, script)
    out = (r.stdout or "").strip()
    if "Running as unit" in out or r.returncode == 0:
        return f"{unit}@{host} OK"
    return f"{unit}@{host} FAIL {(r.stderr or out)[-60:]}"


def deploy_fleet(plan: list[dict], payload_fn, cfg: FleetConfig,
                 stagger: float = 3.0) -> list[str]:
    """全量错峰发射；payload_fn(item) → 该 worker 的远端命令。"""
    receipts = []
    for item in plan:
        receipts.append(deploy_worker(item, payload_fn(item), cfg))
        time.sleep(stagger)
    return receipts


def guard_running(host: str, unit: str) -> bool:
    r = sh(host, f"systemctl is-active --quiet {unit} && echo RUN", 25)
    return "RUN" in r.stdout


def stop_fleet(cfg: FleetConfig, hosts: list | None = None) -> None:
    for h in (hosts or cfg.hosts):
        sh(h, f"sudo -n systemctl stop '{cfg.unit_prefix}[0-9]*' 2>/dev/null; true", 30)


def patrol_fleet(cfg: FleetConfig, manifest_glob: str = "",
                 max_workers: int = 8) -> dict:
    """一次巡检：每机 systemd unit 存活数、manifest 总行数、rate.log
    最近 12 分钟 trip 事件数。SSH 慢是常态，并行 + 各自带超时。"""
    def one(h):
        alive = sh(h, f"systemctl list-units '{cfg.unit_prefix}[0-9]*' --no-legend 2>/dev/null | grep -c active; true", 30)
        rows = sh(h, f"cat {cfg.workdir}/{manifest_glob} 2>/dev/null | wc -l", 30) if manifest_glob else None
        trips = sh(h, f"find {cfg.workdir} -name rate.log -mmin -12 2>/dev/null | xargs grep -h trip 2>/dev/null | wc -l", 30)
        return (h,
                int((alive.stdout or "0").strip() or 0),
                int((rows.stdout or "0").strip() or 0) if rows else -1,
                int((trips.stdout or "0").strip() or 0))
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        res = list(ex.map(one, cfg.hosts))
    return {"alive_units": sum(r[1] for r in res),
            "manifest_rows": sum(r[2] for r in res if r[2] >= 0),
            "trips_12min": sum(r[3] for r in res),
            "per_host": {r[0]: {"units": r[1], "rows": r[2], "trips": r[3]} for r in res}}
