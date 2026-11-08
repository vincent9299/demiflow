"""Worker-side refusal checks; only the external helper creates sandboxes.

These checks are defense in depth, not permission to self-create a sandbox or
proof of host allocation. No worker falls back to a plain host subprocess.
"""
from __future__ import annotations

import os
from pathlib import Path
import stat

from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis.resource_profile import profile

JOB_PATH = Path("/job/job.json")
_NAMESPACES = ("user", "mnt", "pid", "net", "ipc", "uts", "cgroup")
_PROFILE, _PROFILE_SHA = profile()
_ROLES = {role: (_PROFILE["memory"][role], _PROFILE["pids"][role], _PROFILE["cpu_max"][role])
          for role in ("kernel", "native", "media")}


def _read(path, maximum=4096):
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor, "rb") as source:
        raw = source.read(maximum + 1)
    if len(raw) > maximum:
        c.reject("Sandbox control metadata exceeds limit")
    return raw


def _readonly(path):
    if not os.statvfs(path).f_flag & os.ST_RDONLY:
        c.reject("Analysis worker requires a read-only root/config/cgroup mount")


def require_job(role):
    c.one_of(role, _ROLES, "sandbox role")
    try:
        info = JOB_PATH.lstat()
        if not stat.S_ISREG(info.st_mode) or info.st_size > 65536:
            c.reject("Missing fixed analysis job manifest")
        _readonly("/")
        _readonly(JOB_PATH)
        _readonly("/sys/fs/cgroup")
        manifest = c.decode(_read(JOB_PATH, 65536))
        c.fields(manifest, {"format", "job_id", "role", "host_uid", "host_gid", "parent_namespaces", "profile_sha256", "config"}, label="owned sandbox job")
        if manifest["format"] != "demiflow_analysis_job_v1" or manifest["role"] != role:
            c.reject("Analysis job role differs from its entrypoint")
        c.identity(manifest["job_id"], "owned job ID")
        c.sha(manifest["profile_sha256"], "resource profile digest")
        if manifest["profile_sha256"] != _PROFILE_SHA:
            c.reject("Analysis job resource profile differs from its worker image", code="conflict")
        c.integer(manifest["host_uid"], "dedicated host UID", 2**32-2, 1)
        c.integer(manifest["host_gid"], "dedicated host GID", 2**32-2, 1)
        c.fields(manifest["parent_namespaces"], set(_NAMESPACES), label="parent namespaces")
        for name in _NAMESPACES:
            parent = c.integer(manifest["parent_namespaces"][name], "parent namespace inode", 2**63-1, 1)
            if os.stat("/proc/self/ns/" + name).st_ino == parent:
                c.reject("Analysis worker shares a required host namespace")
        uid_map = _read("/proc/self/uid_map").decode("ascii").split()
        gid_map = _read("/proc/self/gid_map").decode("ascii").split()
        if uid_map != ["0", str(manifest["host_uid"]), "1"] or gid_map != ["0", str(manifest["host_gid"]), "1"]:
            c.reject("Analysis worker UID/GID mapping differs from the approved single-user map")
        status = {}
        for line in _read("/proc/self/status", 16384).decode("ascii").splitlines():
            if ":" in line:
                key, value = line.split(":", 1); status[key] = value.strip()
        if status.get("NoNewPrivs") != "1" or status.get("Seccomp") != "2" or any(int(status.get(key, "1"), 16) for key in ("CapInh", "CapPrm", "CapEff", "CapBnd", "CapAmb")):
            c.reject("Analysis worker must have no capabilities and no_new_privs")
        if _read("/proc/self/cgroup").decode("ascii").strip() != "0::/":
            c.reject("Analysis worker must see only its own cgroup leaf")
        memory, pids, cpu = _ROLES[role]
        for file, expected in {"memory.max": str(memory), "memory.swap.max": "0", "memory.oom.group": "1", "pids.max": str(pids), "cpu.max": cpu}.items():
            if _read("/sys/fs/cgroup/" + file).decode("ascii").strip() != expected:
                c.reject("Analysis worker cgroup control readback differs: " + file)
        interfaces = _read("/proc/net/dev", 16384).decode("ascii").splitlines()[2:]
        if any(line.split(":", 1)[0].strip() != "lo" for line in interfaces if ":" in line):
            c.reject("Analysis worker must have a private network namespace")
        # Configuration is small JSON; role-specific code applies its closed
        # schema before opening any source or launching the fixed kernel.
        if type(manifest["config"]) is not dict:
            c.reject("Invalid fixed analysis job configuration")
        return manifest
    except (OSError, UnicodeError) as exc:
        c.reject("Restricted analysis sandbox is unavailable: " + type(exc).__name__ + "; no host execution fallback", code="operation_rejected")
