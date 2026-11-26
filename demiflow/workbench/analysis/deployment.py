"""Explicit administrator binding for the external analysis controller/helper.

Loading this file never mounts filesystems, changes cgroups or starts jobs.
Template inspection is separate from authorization. Production requires the
exact release and separately reviewed real calibration evidence.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import os
from pathlib import Path
import stat

from demiflow.forge.io import no_symlinks
from demiflow.forge import host_partition
from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis.oci import FixedPaths
from demiflow.workbench.analysis.resource_profile import profile
from demiflow.workbench.analysis.resource_profile import seccomp

FORMAT = "demiflow_analysis_executor_binding_v2"
SITE_FIELDS = frozenset({"host_identity", "helper_uid", "helper_gid", "allowed_web_uids", "delegated_analysis_cgroup_root",
    "state_volume_root", "host_transfer_root", "web_transfer_mount", "helper_socket_path", "web_helper_socket_mount",
    "priority_socket_path", "web_priority_socket_mount",
    "web_auth_socket_path", "runtime_binary", "runtime_sha256", "existing_pipeline_native_resource_directory",
    "host_budget_v2_path", "resource_authorization_ref", "supervisor_policy_ref"})
RELEASE_FIELDS = frozenset({"helper_binary_path", "helper_sha256", "controller_python_path", "controller_python_sha256",
    "worker_rootfs_path", "worker_image_digest", "dependency_lock_sha256", "seccomp_profile_path",
    "seccomp_profile_sha256", "release_review_ref"})
_PATH_FIELDS = {"delegated_analysis_cgroup_root", "state_volume_root", "host_transfer_root", "web_transfer_mount",
    "helper_socket_path", "web_helper_socket_mount", "priority_socket_path", "web_priority_socket_mount", "web_auth_socket_path", "runtime_binary",
    "existing_pipeline_native_resource_directory", "host_budget_v2_path", "helper_binary_path", "controller_python_path",
    "worker_rootfs_path", "seccomp_profile_path"}
_CALIBRATION = frozenset({"cgroup_limits", "namespaces_uid_files_network", "jupyter_protocol", "native_query",
    "arrow_roundtrip", "cancellation_descendants", "parent_controller_watchdog_death", "oom_limits",
    "volume_lock_fsync_wal_backup_recovery", "artifact_quota_publication_recovery"})


def _read(path, maximum=65536):
    path = no_symlinks(Path(path))
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, "rb") as source:
        before = os.fstat(source.fileno())
        if not stat.S_ISREG(before.st_mode) or before.st_size > maximum:
            c.reject("Analysis deployment metadata is not a bounded regular file")
        data = source.read(maximum + 1)
        after = os.fstat(source.fileno())
    if len(data) > maximum or (before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns):
        c.reject("Analysis deployment metadata changed", code="conflict")
    return data


def _path(value):
    c.text(value, "fixed deployment path", 2048)
    path = Path(value)
    if not path.is_absolute() or str(path) != value or ".." in path.parts:
        c.reject("Analysis deployment requires normalized absolute paths")
    return no_symlinks(path)


def _hash_file(path, expected, *, maximum=128*1024*1024, executable=False):
    c.sha(expected, "release file digest")
    path = _path(str(path))
    info = path.stat()
    if (not stat.S_ISREG(info.st_mode) or info.st_size > maximum or info.st_mode & 0o022
            or executable and not info.st_mode & 0o111):
        c.reject("Release input is mutable, too large or not executable")
    hashed = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024*1024), b""):
            hashed.update(chunk)
    after = path.stat()
    if (info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns) != (after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns) or hashed.hexdigest() != expected:
        c.reject("Release input digest differs", code="conflict")


def inspect_binding(path):
    """Read template omissions without claiming site or execution readiness."""
    value = c.decode(_read(path))
    if type(value) is not dict:
        c.reject("Invalid executor binding")
    site, release = value.get("site", {}), value.get("owner_release", {})
    if type(site) is not dict or type(release) is not dict:
        c.reject("Invalid executor binding sections")
    missing_site = sorted(name for name in SITE_FIELDS if site.get(name) is None)
    missing_release = sorted(name for name in RELEASE_FIELDS if release.get(name) is None)
    return dict(format=value.get("format"), supported_active_format=FORMAT,
                missing_site_fields=missing_site, missing_owner_release_fields=missing_release,
                activation_allowed=False, jobs_started=False, site_controls_changed=False,
                reason="Template inspection only; this does not verify resource authorization, release or calibration")


@dataclass(frozen=True)
class Deployment:
    value: dict
    paths: FixedPaths
    mode: str

    @classmethod
    def load(cls, path, *, mode="production", role="controller"):
        c.one_of(mode, {"production", "calibration"}, "helper mode")
        c.one_of(role, {"controller", "watchdog"}, "outer helper role")
        raw = _read(path)
        value = c.decode(raw)
        c.fields(value, {"format", "binding_status", "activation", "profile_sha256", "site", "owner_release"}, label="executor binding")
        if value["format"] != FORMAT or value["profile_sha256"] != profile()[1]:
            c.reject("Executor binding requires the exact v2 profile/release")
        c.fields(value["site"], SITE_FIELDS, label="administrator site binding")
        c.fields(value["owner_release"], RELEASE_FIELDS, label="owner release binding")
        site, release = value["site"], value["owner_release"]
        if any(item is None for item in [*site.values(), *release.values()]):
            c.reject("Executor binding is incomplete; no jobs may start")
        c.fields(value["activation"], {"enabled", "calibration_allowed", "resource_candidate_is_authorization", "calibration_receipt"}, label="executor activation")
        activation = value["activation"]
        for name in ("enabled", "calibration_allowed", "resource_candidate_is_authorization"):
            if type(activation[name]) is not bool:
                c.reject("Executor activation flags must be explicit booleans")
        if activation["resource_candidate_is_authorization"] is not False:
            c.reject("Candidate numeric budgets are not resource authorization")
        c.one_of(value["binding_status"], {"GRANTED_FOR_CALIBRATION", "REVIEWED_ACTIVE"}, "administrator binding status")
        c.integer(site["helper_uid"], "dedicated helper UID", 2**32-2, 1)
        c.integer(site["helper_gid"], "dedicated helper GID", 2**32-2, 1)
        if 65534 in {site["helper_uid"],site["helper_gid"]}:
            c.reject("Shared nobody/unmapped identities cannot be a dedicated analysis helper")
        c.sequence(site["allowed_web_uids"], "trusted Web caller UIDs", 4, minimum=1)
        for uid in site["allowed_web_uids"]:
            c.integer(uid, "Web caller UID", 2**32-2)
        if site["helper_uid"] in site["allowed_web_uids"] or len(set(site["allowed_web_uids"])) != len(site["allowed_web_uids"]):
            c.reject("Web and helper identities must be distinct dedicated principals")
        for key in ("host_identity", "resource_authorization_ref", "supervisor_policy_ref"):
            c.text(site[key], key, 1024)
        c.text(release["release_review_ref"], "exact owner release Review", 1024)
        for section in (site, release):
            for name in _PATH_FIELDS & set(section):
                _path(section[name])
        for name in ("helper_socket_path", "web_helper_socket_mount", "priority_socket_path", "web_priority_socket_mount", "web_auth_socket_path"):
            if len(os.fsencode(site[name])) > 107:
                c.reject("Fixed analysis Unix socket path is too long")
        if (site["priority_socket_path"] in {site["helper_socket_path"], site["web_auth_socket_path"]}
                or site["web_priority_socket_mount"] in {site["web_helper_socket_mount"], site["web_auth_socket_path"]}):
            c.reject("Independent priority socket must have its own admitted mount/path")
        binding_info = Path(path).stat()
        if stat.S_IMODE(binding_info.st_mode) != 0o600 or binding_info.st_uid not in {0, site["helper_uid"]}:
            c.reject("Administrator binding must be private and owned by root/helper")
        if os.geteuid() != site["helper_uid"] or os.getegid() != site["helper_gid"]:
            c.reject("Run helper/preflight as the exact dedicated non-root UID/GID; root access is not proof")
        if mode == "calibration":
            if not activation["calibration_allowed"]:
                c.reject("Administrator did not authorize owned calibration")
        elif not activation["enabled"] or value["binding_status"] != "REVIEWED_ACTIVE":
            c.reject("Production analysis remains disabled until reviewed calibration")
        fixed = FixedPaths(Path(release["worker_rootfs_path"]), Path(site["state_volume_root"]),
                           Path(site["delegated_analysis_cgroup_root"]), site["helper_uid"], site["helper_gid"])
        deployment = cls(value=value, paths=fixed, mode=mode)
        deployment.check_release(check_image=role == "controller")
        deployment.check_site()
        if mode == "production":
            deployment.check_calibration()
        return deployment

    def check_release(self, *, check_image=True):
        release, site = self.value["owner_release"], self.value["site"]
        _hash_file(site["runtime_binary"], site["runtime_sha256"], executable=True)
        _hash_file(release["helper_binary_path"], release["helper_sha256"], maximum=64*1024*1024)
        _hash_file(release["controller_python_path"], release["controller_python_sha256"], executable=True)
        _hash_file(release["seccomp_profile_path"], release["seccomp_profile_sha256"], maximum=65536)
        for name, section in (("runtime_binary", site), ("helper_binary_path", release), ("controller_python_path", release), ("seccomp_profile_path", release)):
            if Path(section[name]).stat().st_uid not in {0, site["helper_uid"]}:
                c.reject("Executable release inputs must be owned by root/dedicated helper")
        if release["seccomp_profile_sha256"] != seccomp()[1]:
            c.reject("Installed seccomp differs from the worker release", code="conflict")
        if not check_image:
            # The independent32 MiB watchdog never starts workers. It verifies
            # its own fixed inputs, not the large image manifest owned by the
            # controller's mandatory preflight.
            return
        lock_path = self.paths.rootfs.parent/"image-lock.json"
        raw = _read(lock_path, 16*1024*1024)
        if hashlib.sha256(raw).hexdigest() != release["dependency_lock_sha256"]:
            c.reject("Installed worker dependency lock differs", code="conflict")
        lock = c.decode(raw, maximum=16*1024*1024, nodes=512000, depth=16)
        if lock.get("image_tree_sha256") != release["worker_image_digest"] or not self.paths.rootfs.is_dir():
            c.reject("Installed worker rootfs identity differs", code="conflict")
        from demiflow.workbench.analysis.image_verify import verify_rootfs
        verify_rootfs(self.paths.rootfs, lock, allowed_owners={0, site["helper_uid"]})

    def check_site(self):
        site = self.value["site"]
        cg = self.paths.delegated_cgroup
        if not cg.is_relative_to(Path("/sys/fs/cgroup")) or cg == Path("/sys/fs/cgroup"):
            c.reject("Analysis requires a dedicated delegated cgroup-v2 subtree")
        expected = profile()[0]
        for path, name in ((cg, "analysis"), (cg/"jobs", "jobs"), (cg/"controller", "controller"), (cg/"watchdog", "watchdog")):
            controls = {"memory.max": str(expected["memory"][name]), "memory.swap.max": "0", "pids.max": str(expected["pids"][name])}
            if name in expected["cpu_max"]:
                controls["cpu.max"] = expected["cpu_max"][name]
            for key, value in controls.items():
                with (path/key).open("rb") as source:
                    actual = source.read(65).decode("ascii").strip()
                if actual != value:
                    c.reject("Site cgroup bound differs: " + name + "/" + key)
        state = self.paths.state.stat()
        if state.st_uid != site["helper_uid"] or stat.S_IMODE(state.st_mode) != 0o700:
            c.reject("Analysis state must be private and owned by its dedicated helper")
        if Path(site["host_transfer_root"])!=self.paths.state/"transfer/web-outbox":
            c.reject("Web source outbox must be inside the same admitted analysis state volume")
        outbox=Path(site["host_transfer_root"]).stat()
        if (outbox.st_uid not in site["allowed_web_uids"] or outbox.st_gid!=site["helper_gid"] or stat.S_IMODE(outbox.st_mode)!=0o2770):
            c.reject("Source outbox requires its exact Web owner/helper group and shared2770 mode")
        if len(os.fsencode(self.paths.state/"runtime"/("j"+"0"*32)/"broker/kernel.sock"))>107:
            c.reject("Fixed analysis state root is too long for per-job Unix IPC")
        native = self.paths.state/"native-slots"
        existing = Path(site["existing_pipeline_native_resource_directory"])
        if native == existing or native.is_relative_to(existing) or existing.is_relative_to(native):
            c.reject("Analysis cannot reuse pipeline native resource capacity")
        budget = Path(site["host_budget_v2_path"])
        if budget.name != "host-budget.json":
            c.reject("Analysis must use the existing domain's single host-budget.json")
        capacity = host_partition.policy(budget.parent)
        host_partition.load_analysis(budget.parent, capacity, state_root=self.paths.state,
                                     image_root=self.paths.rootfs.parent)

    def check_calibration(self):
        receipt = self.value["activation"]["calibration_receipt"]
        c.fields(receipt, {"path", "sha256"}, label="reviewed real calibration receipt")
        raw = _read(_path(receipt["path"]))
        if hashlib.sha256(raw).hexdigest() != c.sha(receipt["sha256"]):
            c.reject("Real calibration receipt changed", code="conflict")
        proof = c.decode(raw)
        c.fields(proof, {"format", "passed", "profile_sha256", "helper_sha256", "image_tree_sha256", "checks", "review_ref"}, label="real calibration proof")
        release = self.value["owner_release"]
        if (proof["format"] != "demiflow_analysis_calibration_v1" or proof["passed"] is not True
                or proof["profile_sha256"] != self.value["profile_sha256"] or proof["helper_sha256"] != release["helper_sha256"]
                or proof["image_tree_sha256"] != release["worker_image_digest"]):
            c.reject("Real calibration is absent or belongs to another release", code="conflict")
        c.text(proof["review_ref"], "independent calibration Review", 1024)
        if type(proof["checks"]) is not dict or set(proof["checks"]) != _CALIBRATION or any(value is not True for value in proof["checks"].values()):
            c.reject("Required real calibration/cleanup checks did not all pass")
