"""Fixed OCI bundle construction for a separately approved external helper.

Public requests contain job/object IDs, never these host paths or OCI options.
This module creates no namespaces/cgroups and starts no runtime. Its outputs
are candidates until host allocation, image review and real preflight succeed.
"""
from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import re

from demiflow.forge.io import no_symlinks
from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis.resource_profile import profile
from demiflow.workbench.analysis.resource_profile import seccomp

_JOB = re.compile(r"j[0-9a-f]{32}\Z")
_OBJECT = re.compile(r"o[0-9a-f]{32}\Z")
_NAMESPACES = ("user", "mount", "pid", "network", "ipc", "uts", "cgroup")
_PROC_NAMESPACES = ("user", "mnt", "pid", "net", "ipc", "uts", "cgroup")
_ENTRIES = {"kernel": "demiflow.workbench.analysis.kernel_worker", "native": "demiflow.workbench.analysis.native_worker",
            "media": "demiflow.workbench.analysis.media_worker"}


def job_id(value):
    if type(value) is not str or not _JOB.fullmatch(value):
        c.reject("Invalid owned analysis job nonce")
    return value


def object_id(value):
    if type(value) is not str or not _OBJECT.fullmatch(value):
        c.reject("Invalid private analysis object ID")
    return value


def _path(value):
    if type(value) is not str and not isinstance(value, Path):
        c.reject("Invalid fixed deployment path")
    path = Path(value)
    if not path.is_absolute() or ".." in path.parts or str(path) != str(value):
        c.reject("Deployment paths must be normalized absolute paths")
    return no_symlinks(path)


@dataclass(frozen=True)
class FixedPaths:
    rootfs: Path
    state: Path
    delegated_cgroup: Path
    helper_uid: int
    helper_gid: int

    def __post_init__(self):
        roots = [_path(value) for value in (self.rootfs, self.state, self.delegated_cgroup)]
        c.integer(self.helper_uid, "dedicated non-root helper UID", 2**32-2, 1)
        c.integer(self.helper_gid, "dedicated non-root helper GID", 2**32-2, 1)
        if any(a == b or a.is_relative_to(b) or b.is_relative_to(a) for index, a in enumerate(roots) for b in roots[index + 1:]):
            c.reject("Worker image, private state and cgroup roots must be disjoint")


def _bind(source, destination, *, writable=False):
    return dict(destination=destination, type="bind", source=str(source),
                options=["rbind", "rw" if writable else "ro", "nosuid", "nodev", "noexec", "rprivate"])


def _tmpfs(destination, size, inodes):
    return dict(destination=destination, type="tmpfs", source="tmpfs",
                options=["nosuid", "nodev", "noexec", f"size={size}", f"nr_inodes={inodes}", "mode=0700"])


def manifest(paths, *, identifier, role, config):
    job_id(identifier)
    c.one_of(role, _ENTRIES, "implemented worker role")
    c.encoded(config)
    return dict(format="demiflow_analysis_job_v1", job_id=identifier, role=role,
        host_uid=paths.helper_uid, host_gid=paths.helper_gid,
        parent_namespaces={name: os.stat("/proc/self/ns/" + name).st_ino for name in _PROC_NAMESPACES},
        profile_sha256=profile()[1], config=config)


def bundle(paths, *, identifier, role, source_ids=(), input_id=None, output_id=None, read_kind=None, probe_publish_id=None):
    job_id(identifier)
    c.one_of(role, _ENTRIES, "implemented worker role")
    if type(source_ids) not in {tuple, list} or len(source_ids) > 16:
        c.reject("Owned query supports at most 16 private source objects")
    for value in source_ids:
        object_id(value)
    if len(set(source_ids)) != len(source_ids) or role != "native" and source_ids:
        c.reject("Only native jobs have Lance source mounts; duplicate query mount rejected")
    if read_kind not in {None,'case-read'} or read_kind=='case-read' and (role!='native' or source_ids or input_id is not None):
        c.reject('Only the fixed native Case reader may use this purpose')
    if probe_publish_id is not None:
        if (role!='media' or read_kind is not None or source_ids or input_id is not None or output_id is not None
                or type(probe_publish_id) is not str or not re.fullmatch('p[0-9a-f]{32}',probe_publish_id)):
            c.reject('Pending media probe requires only its fixed owned publication mount')
    elif role == "media" or read_kind=='case-read':
        object_id(output_id)
        if input_id is not None:
            c.reject("Media job cannot mount native input")
    elif output_id is not None:
        c.reject("Only media jobs can mount a committed output object")
    if input_id is not None:
        if role != "native" or source_ids or type(input_id) is not str or not re.fullmatch(r"i[0-9a-f]{32}", input_id):
            c.reject("Materializer input must be one fixed owned ingress, without query sources")
    resource, profile_sha = profile()
    policy, seccomp_sha = seccomp()
    quota, period = map(int, resource["cpu_max"][role].split())
    temporary = resource["role_tmpfs"][role]
    common = resource["shared_role_tmpfs"]
    state, rootfs, delegated = map(Path, (paths.state, paths.rootfs, paths.delegated_cgroup))
    owned = state/"runtime"/identifier
    mounts = [dict(destination="/proc", type="proc", source="proc", options=["nosuid", "nodev", "noexec"]),
        dict(destination="/sys/fs/cgroup", type="cgroup", source="cgroup", options=["nosuid", "nodev", "noexec", "ro"]),
        _tmpfs("/scratch", temporary["scratch_bytes"], temporary["scratch_inodes"]),
        _tmpfs("/work", common["work_bytes"], common["work_inodes"]),
        _tmpfs("/dev", common["dev_bytes"], common["dev_inodes"]),
        _bind(owned/"job", "/job"), _bind(owned/"broker", "/broker")]
    if role == "native":
        mounts.append(_bind(state/"native-slots", "/var/lib/demiflow-analysis/native-slots", writable=True))
        mounts.extend(_bind(state/"objects"/identifier, "/sources/" + identifier) for identifier in source_ids)
        if input_id is not None:
            mounts.append(_bind(state/"transfer"/"ingress"/input_id, "/input"))
    if probe_publish_id is not None:
        mounts.append(_bind(state/'.staging'/probe_publish_id/'payload','/pending'))
    elif role == "media" or read_kind=='case-read':
        mounts.append(_bind(state/"objects"/output_id, "/objects/" + output_id))
    environment = dict(PATH="/usr/local/bin:/usr/bin:/bin", HOME="/work", TMPDIR="/scratch",
        LD_LIBRARY_PATH="/usr/local/lib",
        TOKIO_WORKER_THREADS="1", RAYON_NUM_THREADS="1", OMP_NUM_THREADS="1", OPENBLAS_NUM_THREADS="1",
        MKL_NUM_THREADS="1", NUMEXPR_NUM_THREADS="1", LANCE_CPU_THREADS="1")
    if role == "native":
        environment.update(LANCE_MEM_POOL_SIZE=str(resource["native_options"]["memory_bytes"]//4),
                           LANCE_MAX_TEMP_DIRECTORY_SIZE=str(resource["native_options"]["max_scratch_bytes"]))
    devices = [dict(path="/dev/null", type="c", major=1, minor=3, fileMode=0o666, uid=0, gid=0),
               dict(path="/dev/zero", type="c", major=1, minor=5, fileMode=0o666, uid=0, gid=0),
               dict(path="/dev/urandom", type="c", major=1, minor=9, fileMode=0o666, uid=0, gid=0)]
    value = dict(ociVersion="1.0.2", root=dict(path=str(rootfs), readonly=True), hostname="demiflow-analysis",
        process=dict(terminal=False, user=dict(uid=0, gid=0),
            args=["/usr/local/bin/python3.11", "-I", "-B", "-m", 'demiflow.workbench.analysis.case_worker' if read_kind=='case-read' else _ENTRIES[role]],
            env=[key + "=" + value for key, value in sorted(environment.items())], cwd="/work",
            noNewPrivileges=True, capabilities={key: [] for key in ("bounding", "effective", "inheritable", "permitted", "ambient")},
            rlimits=[dict(type="RLIMIT_CORE", soft=0, hard=0),
                     dict(type="RLIMIT_FSIZE", soft=64*1024*1024, hard=64*1024*1024),
                     dict(type="RLIMIT_NOFILE", soft=128, hard=128)]), mounts=mounts,
        linux=dict(namespaces=[dict(type=kind) for kind in _NAMESPACES],
            uidMappings=[dict(containerID=0, hostID=paths.helper_uid, size=1)],
            gidMappings=[dict(containerID=0, hostID=paths.helper_gid, size=1)],
            cgroupsPath=str(delegated/"jobs"/identifier), devices=devices,
            resources=dict(memory=dict(limit=resource["memory"][role]), cpu=dict(quota=quota, period=period),
                pids=dict(limit=resource["pids"][role]), unified={"memory.swap.max": "0", "memory.oom.group": "1"}),
            seccomp=policy, maskedPaths=["/proc/kcore", "/proc/keys", "/proc/timer_list", "/proc/acpi", "/proc/scsi"],
            readonlyPaths=["/proc/sys", "/proc/sysrq-trigger", "/proc/irq", "/proc/bus"]),
        annotations={"demiflow.job": identifier, "demiflow.role": role,
                     "demiflow.profile.sha256": profile_sha, "demiflow.seccomp.sha256": seccomp_sha})
    c.encoded(value)
    return value
