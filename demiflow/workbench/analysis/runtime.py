"""Owned OCI lifecycle for the external, single-thread analysis controller.

No public request supplies a command, environment, host path or mount. The
controller reserves its analysis execution before calling here. This layer
only owns sandbox leases, never pipeline run scheduling or its run ledger.
"""
from __future__ import annotations

import fcntl
import os
from pathlib import Path
import resource
import select
import signal
import subprocess
import tempfile
import threading
import time
import uuid

from demiflow.execution.process_observation import _pidfd
from demiflow.execution.process_observation import _signal_fd
from demiflow.workbench.process import process_identity
from demiflow.execution.process_observation import require_pidfds
from demiflow.execution.process_observation import unit_stopped
from demiflow.forge.io import atomic_write_json
from demiflow.forge.io import atomic_write_text
from demiflow.forge.io import no_symlinks
from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis.database import private_directory
from demiflow.workbench.analysis.deployment import Deployment
from demiflow.workbench.analysis.deployment import _hash_file
from demiflow.workbench.analysis.oci import bundle
from demiflow.workbench.analysis.oci import manifest
from demiflow.workbench.analysis.oci import job_id
from demiflow.workbench.analysis.owned_cgroup import Cgroups
from demiflow.workbench.analysis.owned_cgroup import identity
from demiflow.workbench.analysis.owned_cgroup import read_control
from demiflow.workbench.analysis.leases import FORMAT as LEASE_FORMAT
from demiflow.workbench.analysis.leases import MEDIA_FORMAT
from demiflow.workbench.analysis.leases import load as load_lease
from demiflow.workbench.analysis.read_control import validate as validate_read_owner
from demiflow.workbench.analysis.read_control import seconds as read_seconds
from demiflow.workbench.analysis import read_retirement


def runtime_arguments(binary, root, log, operation, identifier, *, bundle_path=None, pid_file=None):
    """Closed argv construction; this is not an arbitrary runc RPC."""
    job_id(identifier)
    c.one_of(operation, {"create", "start", "state", "delete", "list"}, "owned runtime operation")
    arguments = [str(binary), "--root", str(root), "--rootless", "true", "--log", str(log), "--log-format", "json", operation]
    if operation == "create":
        if bundle_path is None or pid_file is None:
            c.reject("Owned create requires the fixed bundle and pid-file")
        arguments.extend(["--bundle", str(bundle_path), "--pid-file", str(pid_file)])
    elif bundle_path is not None or pid_file is not None:
        c.reject("Unexpected runtime path override")
    if operation == "list":
        arguments.extend(["--format", "json"])
        return arguments
    if operation == "delete":
        arguments.append("--force")
    arguments.append(identifier)
    return arguments


def _child_limits():
    # This controller deliberately stays single-threaded. The short trusted
    # runtime CLI gets a64 KiB log soft limit. Its hard64 MiB allows runc to set
    # the OCI child's declared64 MiB limit without host CAP_SYS_RESOURCE.
    resource.setrlimit(resource.RLIMIT_FSIZE, (65536, 64*1024*1024))
    resource.setrlimit(resource.RLIMIT_NOFILE, (128, 128))
    resource.setrlimit(resource.RLIMIT_CORE,(0,0))


class Runtime:
    def __init__(self, deployment, *, register_watch):
        if not isinstance(deployment, Deployment) or not callable(register_watch):
            c.reject("Runtime requires the admitted external deployment/watchdog")
        if threading.active_count() != 1:
            c.reject("Analysis controller runtime must use a single event-loop thread")
        self.deployment = deployment
        self.register_watch = register_watch
        require_pidfds()
        site = deployment.value["site"]
        if os.geteuid() != site["helper_uid"] or os.getegid() != site["helper_gid"]:
            c.reject("Runtime must run as the exact dedicated non-root helper")
        # This is the dedicated controller, not the existing Web process.
        os.umask(0o077)
        self.closed = False
        self.groups = Cgroups(deployment.paths.delegated_cgroup)
        if str(os.getpid()) not in read_control(deployment.paths.delegated_cgroup/"controller", "cgroup.procs").split():
            c.reject("Analysis controller is outside its fixed resource cgroup")
        self.root = private_directory(deployment.paths.state/"runtime")
        self.runc = private_directory(self.root/"runc")
        self.home = private_directory(self.root/"home")
        self.xdg = private_directory(self.root/"xdg")
        self.lock_fd = os.open(no_symlinks(self.root/"controller.lock"), os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        try:
            fcntl.flock(self.lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BaseException:
            os.close(self.lock_fd)
            raise
        self.leases = {}
        self.stop_fences = set()
        self.retiring_reads = set()
        self.pidfds = {}
        try:
            self._recover_index()
        except BaseException:
            os.close(self.lock_fd)
            raise

    def _recover_index(self):
        # Completed small lease directories are retained until explicit bounded
        # maintenance. No history is silently deleted to admit another job.
        with os.scandir(self.root) as entries:
            for count, entry in enumerate(entries):
                if count >= 64:
                    c.reject("Analysis runtime control history is full", code="resource_limit")
                if entry.name in {"runc", "home", "xdg", "controller.lock", "supervisor.lock", "watchdog.sock", "priority-owner.json", "media-read-owner.json", read_retirement.NAME, read_retirement.STAGE}:
                    continue
                job_id(entry.name)
                value = load_lease(self.root, entry.name)
                # No adoption/restart. The controller later reconciles each
                # original PID/cgroup/mount/IO identity before any role is free.
                self.leases[entry.name] = value
        read_retirement.recover(self)

    def _save(self, value):
        atomic_write_json(self.root/value["job_id"]/"lease.json", value)
        # State is visible in memory only after its durable write succeeds.
        # Nested ownership identities are immutable in this lifecycle.
        self.leases[value["job_id"]] = dict(value)

    def _command(self, value, operation):
        if threading.active_count() != 1:
            c.reject("Runtime CLI cannot use preexec limits from a threaded controller")
        identifier = job_id(value["job_id"])
        owned = self.root/identifier
        binary = self.deployment.value["site"]["runtime_binary"]
        _hash_file(binary, self.deployment.value["site"]["runtime_sha256"], executable=True)
        args = runtime_arguments(binary, self.runc, owned/"runtime.log", operation, identifier,
            **(dict(bundle_path=owned/"bundle", pid_file=owned/"init.pid") if operation == "create" else {}))
        environment = dict(PATH="/usr/sbin:/usr/bin:/bin", HOME=str(self.home), XDG_RUNTIME_DIR=str(self.xdg), GOMAXPROCS="1")
        # Startup unit is inherited by the short trusted CLI, not preserved into
        # the OCI job. It fences the creation window independently of PID reuse.
        unit = os.open(no_symlinks(owned/"startup.lock"), os.O_RDWR | os.O_NOFOLLOW)
        fcntl.flock(unit, fcntl.LOCK_SH)
        try:
            with tempfile.TemporaryFile(dir=owned) as output:
                process = subprocess.Popen(args, cwd=owned, env=environment, stdin=subprocess.DEVNULL,
                    stdout=output if operation in {"state", "list"} else subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                    pass_fds=(unit,), preexec_fn=_child_limits, close_fds=True)
                original = process_identity(process.pid)
                descriptor = None
                if original is not None:
                    try:
                        descriptor = _pidfd(process.pid)
                        if process_identity(process.pid) != original:
                            os.close(descriptor); descriptor = None
                    except OSError:
                        descriptor = None
                try:
                    try:
                        result = process.wait(timeout=8)
                    except subprocess.TimeoutExpired:
                        if descriptor is not None:
                            _signal_fd(descriptor, signal.SIGKILL)
                        try:
                            process.wait(timeout=2)
                        except subprocess.TimeoutExpired:
                            pass
                        c.reject("Owned runtime command timed out; inspect original lease, no automatic replay", code="operation_failed")
                    if result != 0:
                        c.reject("Owned runtime command failed; inspect its bounded log/original lease", code="operation_failed")
                    if operation in {"state", "list"}:
                        if output.tell() > 65536:
                            c.reject("Runtime state exceeds byte limit", code="resource_limit")
                        output.seek(0)
                        return c.decode(output.read(65537))
                    return None
                finally:
                    if descriptor is not None:
                        os.close(descriptor)
        finally:
            os.close(unit)

    @staticmethod
    def _config(role, config):
        if role == "kernel":
            c.fields(config, {"session_id", "generation"}, label="kernel configuration")
            c.identity(config["session_id"]); c.identity(config["generation"])
        elif role == "native":
            operation = config.get("operation") if type(config) is dict else None
            c.one_of(operation, {"query", "materialize", "read", "case-page"}, "native operation")
            if operation == "query":
                from demiflow.workbench.analysis.query import query_config
                query_config(config)
            elif operation == "materialize":
                from demiflow.workbench.analysis.materialize import materialize_config
                materialize_config(config)
            elif operation=='case-page':
                from demiflow.workbench.analysis.case_worker import configuration
                configuration(config)
            else:
                from demiflow.workbench.analysis.read_worker import read_config
                read_config(config)
        elif role == "media":
            from demiflow.workbench.analysis.media_worker import configuration
            configuration(config)
        else:
            c.reject("This worker role is not implemented in this release")

    def create(self, *, project, execution_id, role, config, web_parent, owner_digest, authorize, read_owner=None):
        from demiflow.workbench.projects.registry import identity as project_identity
        project_identity(project); c.identity(execution_id, "owning analysis execution")
        c.sha(owner_digest, "original generation control owner digest")
        self._config(role, config)
        purpose=('media-probe' if config.get('operation')=='probe-media' else 'media-read') if role=='media' else 'case-read' if role=='native' and config.get('operation')=='case-page' else None
        if purpose is not None:
            validate_read_owner(read_owner)
            if (read_owner['kind']!=purpose or read_owner["project"] != project
                    or read_owner["controller"] != process_identity(os.getpid()) or read_owner["web_parent"] != web_parent
                    or read_owner["read_request_id"] != execution_id or c.digest(read_owner) != owner_digest
                    or not time.monotonic() < read_owner["deadline"] <= time.monotonic()+read_seconds(read_owner)):
                c.reject("Read owner differs or expired; no kernel owner substitution", code="conflict")
            if purpose=='media-read':
                if config.get('operation')=='artifact-media':
                    from demiflow.workbench.analysis.media_artifact_worker import owner_artifact
                    fixed_project=config['artifact']['project'];fixed_artifact=owner_artifact(config)
                else:
                    fixed_project=config['witness']['provenance']['project'];fixed_artifact=config['witness']['artifact']
                if fixed_project!=project or read_owner['artifact']!=fixed_artifact:
                    c.reject('Media artifact/project differs from its read owner',code='conflict')
            elif purpose=='media-probe':
                from demiflow.workbench.analysis.media_artifact_worker import probe_binding
                if config['reservation']['seed']['project']!=project or read_owner['probe']!=probe_binding(config):
                    c.reject('Pending probe differs from its admitted reservation',code='conflict')
            elif (config['witness']['project']!=project or read_owner['case']!=dict(object=config['witness']['artifact'],
                    source_digest=config['witness']['source']['digest'],config_digest=c.digest(config))):
                c.reject('Case configuration differs from its read owner',code='conflict')
        elif read_owner is not None:
            c.reject("Kernel/native roles cannot borrow a media read owner", code="conflict")
        c.encoded(config)
        c.fields(web_parent, {"pid", "start", "boot"}, label="authorized Web parent identity")
        c.integer(web_parent["pid"], "Web parent PID", 2**31-1, 1)
        c.text(web_parent["start"], "Web parent start", 32); c.text(web_parent["boot"], "Web host boot", 64)
        if not callable(authorize) or authorize() is not True:
            c.reject("Current job permission is absent")
        if read_retirement.charged(self,role) or any(item["role"] == role and
                (item["state"] != "cleanup_complete" or item["format"] == MEDIA_FORMAT) for item in self.leases.values()):
            c.reject("This analysis role is busy or cleanup is unresolved", code="busy")
        if len(self.leases) >= 32:
            c.reject("Analysis sandbox lease history requires explicit maintenance", code="resource_limit")
        # Failed directory/lease creation can leave an owned control entry.
        # Refuse new work until it is explicitly reconciled; do not let repeated
        # disk failures create an unbounded set of unindexed directories.
        self._recover_index()
        self.deployment.check_site()
        controller = process_identity(os.getpid())
        if controller is None or process_identity(web_parent.get("pid")) != web_parent:
            c.reject("Original Web/controller process ownership is unavailable", code="conflict")
        identifier = "j" + uuid.uuid4().hex
        owned = self.root/identifier; owned.mkdir(mode=0o700)
        (owned/"job").mkdir(mode=0o700); (owned/"broker").mkdir(mode=0o700); (owned/"bundle").mkdir(mode=0o700)
        unit = os.open(owned/"startup.lock", os.O_CREAT | os.O_EXCL | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        info = os.fstat(unit); os.close(unit)
        value = dict(format=MEDIA_FORMAT if purpose is not None else LEASE_FORMAT, job_id=identifier, role=role, state="creating",
            project=project, execution_id=execution_id, controller=controller, web_parent=web_parent, owner_digest=owner_digest,
            delegation_identity=self.groups.root_identity, jobs_identity=self.groups.jobs_identity,
            cgroup=None, unit_identity=dict(device=info.st_dev,inode=info.st_ino),init_identity=None,reason=None,
            deadline=read_owner["deadline"] if purpose is not None else time.monotonic() + {"kernel":3600, "native":60}[role])
        if purpose is not None:
            value["owner_kind"] = purpose
        self._save(value)
        try:
            value["cgroup"] = self.groups.create(identifier, role)
            self._save(value)
            if self.register_watch(value) is not True:
                c.reject("Independent watchdog did not accept the owned lease")
            source_ids = sorted({item["object_id"] for item in config.get("sources", [])}) if role == "native" else []
            if role == "native" and config["operation"] == "read":
                source_ids = [config["object_id"]]
            output_id=None
            if purpose in {'media-read','case-read'}:
                output_id=config['artifact']['id'] if config.get('operation')=='artifact-media' else config['witness']['artifact']['id']
            spec = bundle(self.deployment.paths, identifier=identifier, role=role, source_ids=source_ids,
                          input_id=config.get("input_id") if role == "native" else None,
                          output_id=output_id, read_kind='case-read' if purpose=='case-read' else None,
                          probe_publish_id=config['reservation']['publish_id'] if purpose=='media-probe' else None)
            atomic_write_text(owned/"bundle/config.json", c.encoded(spec).decode("utf-8"))
            job = manifest(self.deployment.paths, identifier=identifier, role=role, config=config)
            atomic_write_text(owned/"job/job.json", c.encoded(job).decode("utf-8"))
            if role == "media":
                atomic_write_text(owned/"job/output-request.json", c.encoded(config).decode("utf-8"))
            if authorize() is not True:
                c.reject("Analysis permission was revoked before OCI create")
            self._command(value, "create")
            with no_symlinks(owned/"init.pid").open("rb") as source:
                raw = source.read(33)
            if len(raw) > 32 or not raw.strip().isdigit():
                c.reject("Owned runtime init PID is unavailable", code="operation_failed")
            pid = int(raw)
            value["init_identity"] = process_identity(pid)
            if value["init_identity"] is None:
                c.reject("Owned init identity could not be captured before start", code="operation_failed")
            self.pidfds[identifier] = _pidfd(pid)
            if process_identity(pid) != value["init_identity"]:
                c.reject("Owned init PID changed before start", code="conflict")
            self.groups.verify(identifier, value["cgroup"]["cgroup_identity"], role)
            value["state"] = "created"; self._save(value)
            if self.register_watch(value) is not True:
                c.reject("Independent watchdog did not confirm the created sandbox")
            return dict(job_id=identifier, phase="created", execution_started=False, cleanup_proven=False)
        except BaseException as exc:
            value["state"], value["reason"] = "cleanup_pending", type(exc).__name__
            # Stop BEFORE persistent IO: even a permanently blocked fsync must
            # not delay the owned kill. Keep the original lease on uncertainty.
            try:
                if value["cgroup"] is not None:
                    self.group(value)
                    self.groups.kill(identifier, value["cgroup"]["cgroup_identity"])
            finally:
                self._save(value)
            raise

    def start(self, identifier, *, authorize):
        original = self.leases.get(job_id(identifier))
        value = None if original is None else dict(original)
        if identifier in getattr(self, "stop_fences", set()):
            c.reject("Original sandbox has an in-memory stop fence", code="operation_rejected")
        if value is None or value["state"] != "created":
            c.reject("Original created sandbox is unavailable; no create/start replay", code="conflict")
        if not callable(authorize) or authorize() is not True:
            self.stop(identifier, reason="permission_revoked_before_start")
            c.reject("Analysis permission was revoked before sandbox start")
        self.group(value)
        self.groups.verify(identifier, value["cgroup"]["cgroup_identity"], value["role"])
        try:
            # Recheck the generation fence before releasing the created init.
            # A concurrent stop remains latched by the independent job watch.
            if self.register_watch(value) is not True:
                c.reject("Watchdog did not admit this original generation", code="operation_failed")
            self._command(value, "start")
            value["state"] = "started"; self._save(value)
            if self.register_watch(value) is not True:
                c.reject("Sandbox start is unconfirmed by its watchdog", code="operation_failed")
        except BaseException:
            self.stop(identifier, reason="start_or_watchdog_confirmation_failed")
            raise
        return dict(job_id=identifier, phase="started", cleanup_proven=False)

    def stop(self, identifier, *, reason="stop_requested"):
        original = self.leases.get(job_id(identifier))
        value = None if original is None else dict(original)
        if value is None:
            c.reject("Owned sandbox lease is not found", code="not_found")
        c.text(reason, "sandbox stop reason", 256)
        if value["state"] == "cleanup_complete":
            return dict(job_id=identifier, accepted=True, cleanup_proven=True)
        if value["state"] == "cleanup_removing_cgroup":
            self.delegation(value)
            if not self.groups.absent(identifier):
                self.group(value)
                self.groups.kill(identifier, value["cgroup"]["cgroup_identity"])
            return dict(job_id=identifier, accepted=True, cleanup_proven=False)
        if value["cgroup"] is not None:
            self.group(value)
        # This bounded process-local fence survives failed persistence. Recovery
        # never starts original leases, so a crash cannot turn it into replay.
        if not hasattr(self, "stop_fences"):
            self.stop_fences = set()
        self.stop_fences.add(identifier)
        value["state"], value["reason"] = "cleanup_pending", reason
        try:
            if value["cgroup"] is not None:
                self.groups.kill(identifier, value["cgroup"]["cgroup_identity"])
        finally:
            self._save(value)
        return dict(job_id=identifier, accepted=True, cleanup_proven=False)

    def cleanup(self, identifier, *, close_bridges, clean_private_io):
        original = self.leases.get(job_id(identifier))
        value = None if original is None else dict(original)
        if value is None:
            c.reject("Owned sandbox lease is not found", code="not_found")
        owned = self.root/identifier
        if value["state"] == "cleanup_complete":
            return dict(job_id=identifier, cleanup_proven=True)
        group = value["cgroup"]
        self.delegation(value)
        if group is not None:
            self.group(value)
        present = self.leaf(value) if group is not None else not self.groups.absent(identifier)
        if group is None and present:
            c.reject("Cgroup creation identity is unresolved; no adoption or cleanup proof", code="conflict")
        if group is not None and present and self.groups.populated(identifier, group["cgroup_identity"]):
            c.reject("Owned sandbox still has processes", code="busy")
        if not unit_stopped(owned/"startup.lock", value["unit_identity"]):
            c.reject("Owned runtime startup/CLI is still active", code="busy")
        if not callable(close_bridges) or close_bridges(identifier) is not True:
            c.reject("Owned broker connections/queued descriptors are not closed", code="conflict")
        # No shell and no global container deletion. Only the original nonce is
        # passed to this deployment's private runc root.
        if self._listed(value):
            self._command(value, "delete")
        if self._listed(value):
            c.reject("Original runtime container is still registered", code="busy")
        if group is not None and present and self.groups.populated(identifier, group["cgroup_identity"]):
            c.reject("Cleanup command has not fully exited", code="busy")
        if not callable(clean_private_io) or clean_private_io(identifier) is not True:
            c.reject("Owned private IO/scratch cleanup is unproved", code="conflict")
        if not unit_stopped(owned/"startup.lock", value["unit_identity"]):
            c.reject("Owned runtime cleanup/CLI is still active", code="busy")
        descriptor = self.pidfds.pop(identifier, None)
        if descriptor is not None:
            if not select.select([descriptor], [], [], 0)[0]:
                self.pidfds[identifier] = descriptor
                c.reject("Original sandbox init pidfd did not end", code="conflict")
            os.close(descriptor)
        elif value["init_identity"] is not None and process_identity(value["init_identity"]["pid"]) == value["init_identity"]:
            c.reject("Original init process still exists", code="conflict")
        # Persist the completed process/runtime/IO witness BEFORE unlinking the
        # cgroup. If the final save loses power/space, recovery accepts absence
        # only from this precise durable stage and rechecks the other witnesses.
        value["state"] = "cleanup_removing_cgroup"; self._save(value)
        if group is not None and present:
            self.groups.remove_empty(identifier, group["cgroup_identity"])
        value["state"] = "cleanup_complete"; self._save(value)
        return dict(job_id=identifier, cleanup_proven=True)

    def retire_read(self,identifier):
        return read_retirement.retire(self,identifier)

    def _listed(self, value):
        # Exact read-only inventory of this controller's private runtime root.
        # A failed create may have no container to delete. Failed list/unknown
        # identities remain unresolved; errors are never interpreted as absent.
        items = self._command(value, "list")
        if items is None:
            items = []  # runc JSON represents an empty inventory as null
        c.sequence(items, "private runtime inventory", 32)
        found = False
        for item in items:
            if type(item) is not dict or "id" not in item:
                c.reject("Private runtime inventory is malformed", code="conflict")
            identifier = job_id(item["id"])
            if identifier not in self.leases:
                c.reject("Private runtime contains an unowned container", code="conflict")
            found = found or identifier == value["job_id"]
        return found

    def group(self, value):
        """Require the ORIGINAL delegation and jobs root as well as leaf inode."""
        self.delegation(value)
        group = value["cgroup"]
        if group is None or group["delegation_identity"] != self.groups.root_identity or group["jobs_identity"] != self.groups.jobs_identity:
            c.reject("Owned lease belongs to another cgroup delegation", code="conflict")
        if group["job_id"] != value["job_id"] or group["role"] != value["role"]:
            c.reject("Owned cgroup role/nonce differs", code="conflict")
        return group

    def delegation(self, value):
        self.groups._root()
        if value["delegation_identity"] != self.groups.root_identity or value["jobs_identity"] != self.groups.jobs_identity:
            c.reject("Owned lease belongs to another cgroup delegation", code="conflict")

    def leaf(self, value):
        self.group(value)
        if self.groups.absent(value["job_id"]):
            if value["state"] != "cleanup_removing_cgroup":
                c.reject("Original cgroup disappeared without its durable cleanup witness", code="conflict")
            return False
        return True

    def close(self):
        if self.closed:
            return
        for descriptor in self.pidfds.values():
            os.close(descriptor)
        self.pidfds.clear()
        os.close(self.lock_fd)
        self.closed = True
        # Stopping this object is not job cleanup. Independent watchdog and host
        # supervisor retain responsibility for any unresolved sandbox lease.
