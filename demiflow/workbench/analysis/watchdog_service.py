"""Independent bounded watchdog service for the admitted helper deployment.

The supervisor starts this process in the fixed watchdog cgroup. It is not
started by Notebook code or a Web request. The original controller registers
the same closed, already-persisted lease and original kernel-owned cgroup
identities. Runtime control after startup performs no persistent-volume IO;
Web callers cannot provide a lease, PID, path, deadline or kill target.
"""
from __future__ import annotations

from contextlib import closing
import os
from pathlib import Path
import selectors
import socket
import time

from demiflow.forge.schema import ForgeError
from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis.deployment import Deployment
from demiflow.workbench.analysis.deployment import _read
from demiflow.workbench.analysis.database import private_directory
from demiflow.workbench.analysis.framing import Frames
from demiflow.workbench.analysis.ipc import Channel
from demiflow.workbench.analysis.ipc import Listener
from demiflow.workbench.analysis.ipc import Peer
from demiflow.workbench.analysis.oci import job_id
from demiflow.workbench.analysis.owned_cgroup import Cgroups
from demiflow.workbench.analysis.owned_cgroup import read_control
from demiflow.workbench.analysis.resource_profile import profile
from demiflow.workbench.analysis.watchdog import JobWatch
from demiflow.workbench.analysis.leases import load as load_lease
from demiflow.workbench.analysis.leases import validate as validate_lease
from demiflow.workbench.analysis.priority_control import NAME as OWNER_NAME
from demiflow.workbench.analysis.priority_control import StopRequest
from demiflow.workbench.analysis.priority_control import load as load_owner
from demiflow.workbench.analysis.priority_control import validate as validate_owner
from demiflow.workbench.analysis import read_control as media_control
from demiflow.workbench.analysis import read_retirement


class WatchClient:
    def __init__(self, deployment):
        self.deployment = deployment
        self.path = deployment.paths.state/"runtime/watchdog.sock"

    def _peer(self, connection):
        peer = Peer.capture(connection, allowed_uids={os.geteuid()})
        if (not peer.live() or str(peer.pid) not in read_control(self.deployment.paths.delegated_cgroup/"watchdog", "cgroup.procs").split()):
            c.reject("Independent watchdog is outside its original resource role", code="operation_failed")
        return peer

    def status(self):
        connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM); connection.settimeout(2)
        with closing(connection):
            connection.connect(str(self.path)); peer = self._peer(connection)
            frames = Frames(connection); frames.send("json", dict(operation="status"))
            return frames.json() == dict(format="demiflow_analysis_watchdog_status_v1", identity=peer.identity, profile_sha256=profile()[1], active=True)

    def register(self, lease):
        identifier = job_id(lease["job_id"])
        connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        connection.settimeout(2)
        with closing(connection):
            connection.connect(str(self.path))
            self._peer(connection)
            frames = Frames(connection)
            frames.send("json", dict(operation="clean" if lease["state"]=="cleanup_complete" else "watch", lease=lease))
            return frames.json() == dict(accepted=True, job_id=identifier, cleanup_proven=False)

    def owner(self, operation, digest, *, record=None):
        c.one_of(operation, {"bind-owner", "clear-owner", "owner-status"}, "priority owner control")
        c.sha(digest, "priority owner digest")
        connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM); connection.settimeout(2)
        with closing(connection):
            connection.connect(str(self.path)); self._peer(connection)
            frames = Frames(connection); frames.send("json", dict(operation=operation, owner_digest=digest,
                **({"record":record} if operation=="bind-owner" else {})))
            value = frames.json()
            c.fields(value, {"owner_digest", "bound", "stop_requested"}, label="independent stop owner")
            if value["owner_digest"] != digest or type(value["bound"]) is not bool or type(value["stop_requested"]) is not bool:
                c.reject("Independent stop owner response differs", code="conflict")
            return value


    def read_owner(self, operation, digest, *, record=None):
        c.one_of(operation, {"bind-read-owner", "stop-read-owner", "clear-read-owner", "read-owner-status"}, "media read owner control")
        c.sha(digest, "media read owner digest")
        connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM); connection.settimeout(2)
        with closing(connection):
            connection.connect(str(self.path)); self._peer(connection)
            frames = Frames(connection)
            frames.send("json", dict(operation=operation, owner_digest=digest,
                **({"record": record} if operation == "bind-read-owner" else {})))
            value = frames.json()
            c.fields(value, {"owner_digest", "bound", "stop_requested"}, label="independent read owner")
            if value["owner_digest"] != digest or type(value["bound"]) is not bool or type(value["stop_requested"]) is not bool:
                c.reject("Independent read owner response differs", code="conflict")
            return value


class WatchdogService:
    def __init__(self, deployment, *, heartbeat=None):
        if not isinstance(deployment, Deployment):
            c.reject("Watchdog requires an admitted deployment")
        self.deployment = deployment
        self.heartbeat = heartbeat
        self.groups = Cgroups(deployment.paths.delegated_cgroup)
        if str(os.getpid()) not in read_control(deployment.paths.delegated_cgroup/"watchdog", "cgroup.procs").split():
            c.reject("Independent watchdog is outside its fixed resource cgroup")
        self.root = private_directory(deployment.paths.state/"runtime")
        self.listener = Listener(self.root/"watchdog.sock")
        self.priority_listener = Listener(deployment.value["site"]["priority_socket_path"], public=True)
        self.poll = selectors.DefaultSelector()
        self.poll.register(self.listener.socket, selectors.EVENT_READ, ("listener", "private"))
        self.poll.register(self.priority_listener.socket, selectors.EVENT_READ, ("listener", "priority"))
        self.connections = {}
        self.watches = {}
        self.owner = None
        self.stop_requested = False
        self.last_cleared = None
        self.read_owner = None
        self.read_stop_requested = False
        self.last_read_cleared = None
        self.read_monitor = None
        self.read_next_check = 0
        self.profile_sha256 = deployment.value["profile_sha256"]
        self.running = True
        try:
            self._recover()
        except BaseException:
            self.close()
            raise

    def _recover(self):
        if (self.root/OWNER_NAME).exists():
            self.owner = load_owner(self.root)
        if (self.root/media_control.NAME).exists():
            self.read_owner = media_control.load(self.root)
        with os.scandir(self.root) as entries:
            for count, item in enumerate(entries):
                if count >= 64:
                    c.reject("Watchdog control directory exceeds its entry limit", code="resource_limit")
                if item.name in {"runc", "home", "xdg", "controller.lock", "supervisor.lock", "watchdog.sock", OWNER_NAME, media_control.NAME, read_retirement.NAME, read_retirement.STAGE}:
                    continue
                lease = load_lease(self.root, item.name)
                if lease["state"] != "cleanup_complete" and lease["cgroup"] is not None:
                    self._watch(lease)
        if self.read_owner is not None:
            # Recovery preserves the owner/slot but never resumes an old read.
            self.read_stop_requested = True

    def _scope(self, lease):
        if lease.get("owner_kind") in read_retirement.KINDS:
            value = getattr(self, "read_owner", None)
            if (value is None or lease.get("owner_kind")!=value['kind']
                    or lease["execution_id"] != value["read_request_id"] or lease["deadline"] != value["deadline"]):
                c.reject("Media lease does not belong to its independent read owner", code="conflict")
            return value, self.read_stop_requested
        return self.owner, self.stop_requested

    def _watch(self, lease):
        validate_lease(lease)
        identifier = lease["job_id"]
        owner, stopped = self._scope(lease)
        if (owner is None or lease["owner_digest"] != c.digest(owner)
                or any(lease[name] != owner[name] for name in ("controller", "web_parent", "project"))):
            c.reject("Sandbox does not belong to its original control owner", code="conflict")
        if stopped:
            if lease["cgroup"] is not None:
                self.groups.kill(identifier, lease["cgroup"]["cgroup_identity"])
            c.reject("Original generation has an independent stop fence", code="operation_rejected")
        old = self.watches.get(identifier)
        if old is not None:
            original = old["lease"]
            immutable = ("format","owner_kind","cgroup", "controller", "web_parent", "deadline", "role", "execution_id", "project", "owner_digest")
            if any(lease.get(name) != original.get(name) for name in immutable):
                c.reject("Watchdog registration cannot replace or extend an owned lease", code="conflict")
            old["lease"] = dict(lease)
            return
        if lease["cgroup"] is None or lease["state"] == "cleanup_complete":
            c.reject("Cannot watch a missing or cleaned cgroup", code="conflict")
        if len(self.watches) >= 3 or any(item["lease"]["role"] == lease["role"] for item in self.watches.values()):
            c.reject("Watchdog role slots are full or cleanup is unresolved", code="busy")
        watch = JobWatch(self.groups, lease["cgroup"], controller=lease["controller"],
                         web_parent=lease["web_parent"], deadline=lease["deadline"])
        self.watches[identifier] = dict(lease=lease, watch=watch)

    def _close_connection(self, descriptor):
        item = self.connections.pop(descriptor)
        channel = item["channel"]
        if item["stop"] is not None:
            item["stop"].close()
        self.poll.unregister(channel.connection); channel.close()

    def _owner_control(self, peer, operation, digest, *, record=None):
        from demiflow.workbench.process import process_identity
        if (not peer.live() or str(peer.pid) not in read_control(self.deployment.paths.delegated_cgroup/"controller", "cgroup.procs").split()):
            c.reject("Priority owner control requires its live controller role", code="operation_rejected")
        value = validate_owner(record) if operation=="bind-owner" else self.owner
        if value is None and operation in {"clear-owner","owner-status"} and self.last_cleared==(digest,peer.identity):
            return dict(owner_digest=digest,bound=False,stop_requested=False)
        if value is None or c.digest(value) != digest or self.owner is not None and c.digest(self.owner) != digest:
            c.reject("Priority owner identity differs", code="conflict")
        if operation == "bind-owner":
            if peer.identity != value["controller"]:
                c.reject("Only the original controller can bind its generation", code="operation_rejected")
            if self.owner is None:
                self.owner, self.stop_requested = value, False
        elif operation == "clear-owner":
            if peer.identity != value["controller"] and process_identity(value["controller"]["pid"]) == value["controller"]:
                c.reject("Original generation controller is still live", code="conflict")
            self._tick_watches()
            if any(item["lease"]["owner_digest"] == digest for item in self.watches.values()):
                c.reject("Original generation still has unresolved job watches", code="busy")
            self.owner, self.stop_requested = None, False
            self.last_cleared = (digest,peer.identity)
        elif operation != "owner-status" or self.owner is None or peer.identity != self.owner["controller"]:
            c.reject("Original priority owner is unavailable", code="conflict")
        return dict(owner_digest=digest, bound=self.owner is not None, stop_requested=self.stop_requested)

    def _stop_read(self):
        self.read_stop_requested = True
        monitor = getattr(self, "read_monitor", None)
        if monitor is not None:
            monitor.close(); self.read_monitor = None
        owner = getattr(self, "read_owner", None)
        if owner is not None:
            for owned in self.watches.values():
                if owned["lease"].get('owner_kind')==owner['kind'] and owned["lease"]["owner_digest"] == c.digest(owner):
                    owned["watch"].tick(stop_requested=True)

    def _tick_read_authority(self):
        owner = getattr(self, "read_owner", None)
        if owner is None or self.read_stop_requested:
            return
        if time.monotonic() >= owner["deadline"]:
            self._stop_read(); return
        try:
            if self.read_monitor is None and time.monotonic() >= self.read_next_check:
                self.read_monitor = media_control.Authorization(self.deployment, dict(owner))
            if self.read_monitor is not None and self.read_monitor.advance():
                self.read_monitor.close(); self.read_monitor = None
                self.read_next_check = time.monotonic()+0.2
        except (OSError, ForgeError, EOFError):
            self._stop_read()

    def _read_owner_control(self, peer, operation, digest, *, record=None):
        from demiflow.workbench.process import process_identity
        if (not peer.live() or str(peer.pid) not in read_control(self.deployment.paths.delegated_cgroup/"controller", "cgroup.procs").split()):
            c.reject("Read owner control requires its live controller role", code="operation_rejected")
        current = self.read_owner
        value = media_control.validate(record) if operation == "bind-read-owner" else current
        if value is None and operation in {"clear-read-owner", "read-owner-status"} and self.last_read_cleared == (digest, peer.identity):
            return dict(owner_digest=digest, bound=False, stop_requested=False)
        if value is None or c.digest(value) != digest or current is not None and c.digest(current) != digest:
            c.reject("Read owner identity differs", code="conflict")
        if operation == "bind-read-owner":
            if peer.identity != value["controller"]:
                c.reject("Only the original controller may bind a read", code="operation_rejected")
            if current is None:
                if not time.monotonic() < value["deadline"] <= time.monotonic()+media_control.seconds(value):
                    c.reject("Read deadline is expired or exceeds its fixed purpose limit", code="conflict")
                self.read_owner, self.read_stop_requested = dict(value), False
                self.read_monitor, self.read_next_check = None, 0
        elif peer.identity != value["controller"] and process_identity(value["controller"]["pid"]) == value["controller"]:
            c.reject("Original read controller is still live", code="conflict")
        elif operation == "stop-read-owner":
            self._stop_read()
        elif operation == "clear-read-owner":
            self._tick_watches()
            if any(item["lease"].get('owner_kind')==value['kind'] and item["lease"]["owner_digest"] == digest for item in self.watches.values()):
                c.reject("Original read still has unresolved job watches", code="busy")
            if self.read_monitor is not None:
                self.read_monitor.close(); self.read_monitor = None
            self.read_owner, self.read_stop_requested = None, False
            self.last_read_cleared = (digest, peer.identity)
        elif operation != "read-owner-status":
            c.reject("Unsupported read owner operation")
        return dict(owner_digest=digest, bound=self.read_owner is not None, stop_requested=self.read_stop_requested)

    def _priority_request(self, item, request):
        c.fields(request, {"version", "project", "operation", "payload"}, label="independent stop request")
        if type(request["version"]) is not int or request["version"] != 1 or item["stop"] is not None:
            c.reject("Invalid independent stop request", code="conflict")
        read = request["operation"] == "stop-read"
        owner = self.read_owner if read else self.owner
        if request["operation"] not in {"stop-generation", "stop-read"} or owner is None or request["project"] != owner["project"]:
            c.reject("Independent stop requires its original owner", code="conflict")
        factory = media_control.StopRequest if read else StopRequest
        item["stop"] = factory(self.deployment, item["peer"], dict(owner), request["payload"])

    def _advance_stops(self):
        for descriptor, item in list(self.connections.items()):
            task = item["stop"]
            if task is None or item["responded"]:
                continue
            try:
                if not task.advance():
                    continue
                read = isinstance(task, media_control.StopRequest)
                owner = self.read_owner if read else self.owner
                if owner != task.owner:
                    c.reject("Priority owner changed during authorization", code="conflict")
                # Set the fence before touching cgroups. No later watch/start
                # can revive this generation, even if a kill write fails.
                if read:
                    self._stop_read()
                else:
                    self.stop_requested = True
                    for owned in self.watches.values():
                        if owned["lease"].get('owner_kind') not in read_retirement.KINDS and owned["lease"]["owner_digest"] == c.digest(owner):
                            owned["watch"].tick(stop_requested=True)
                value = dict(format="demiflow_analysis_priority_read_stop_v1" if read else "demiflow_analysis_priority_stop_v1", **task.payload, accepted=True,
                    cleanup_proven=False, force=True, helper_sha256=self.deployment.value["owner_release"]["helper_sha256"],
                    profile_sha256=self.profile_sha256)
                item["channel"].queue("json", value)
                item["responded"] = True
                self.poll.modify(item["channel"].connection, selectors.EVENT_WRITE, ("connection", descriptor))
            except (OSError, ForgeError, EOFError) as exc:
                self._failure(descriptor, item, exc)

    def _failure(self, descriptor, item, exc):
        try:
            item["channel"].queue("json", dict(error=str(exc)[:1024], code=getattr(exc, "code", "operation_rejected")))
            item["responded"] = True
            self.poll.modify(item["channel"].connection, selectors.EVENT_WRITE, ("connection", descriptor))
        except (OSError, ForgeError):
            self._close_connection(descriptor)

    def _tick_watches(self):
        self._tick_read_authority()
        for identifier, item in list(self.watches.items()):
            # No persistent-volume reads on this independent control loop.
            # The controller sends the same already-persisted closed lease;
            # cleanup requires a separate exact original lease + absent leaf.
            lease = item["lease"]
            try:
                owner, stopped = self._scope(lease)
                item["watch"].tick(stop_requested=lease["state"] in {"cleanup_pending", "cleanup_removing_cgroup"}
                    or stopped and owner is not None and lease["owner_digest"] == c.digest(owner))
            except FileNotFoundError:
                pass  # unresolved disappearance, retain the original watch

    def _clean_watch(self, peer, lease):
        from demiflow.workbench.process import process_identity
        if (not peer.live() or str(peer.pid) not in read_control(self.deployment.paths.delegated_cgroup/"controller","cgroup.procs").split()
                or peer.identity!=lease["controller"] and process_identity(lease["controller"]["pid"])==lease["controller"]):
            c.reject("Only the original or recovery controller can finish an owned watch",code="operation_rejected")
        if (lease["state"]!="cleanup_complete" or lease["delegation_identity"]!=self.groups.root_identity
                or lease["jobs_identity"]!=self.groups.jobs_identity or not self.groups.absent(lease["job_id"])):
            c.reject("Owned watch cleanup lacks its exact original delegation/absent leaf",code="conflict")
        item=self.watches.get(lease["job_id"])
        if item is not None:
            original=item["lease"]
            if any(lease.get(key)!=original.get(key) for key in ("format","owner_kind","owner_digest","cgroup","controller","web_parent","deadline","role","execution_id","project")):
                c.reject("Cleanup lease cannot replace the original watch",code="conflict")
            item["watch"].close();del self.watches[lease["job_id"]]

    def serve(self):
        while self.running:
            self._tick_watches()
            self._advance_stops()
            if self.heartbeat is not None:
                # An inherited nonblocking pipe to the outer supervisor. No
                # volume IO, public request, thread or independent timer.
                self.heartbeat()
            for key, events in self.poll.select(0.2):
                event_kind, event_value = key.data
                if event_kind == "listener":
                    listener = self.listener if event_value == "private" else self.priority_listener
                    try:
                        connection, _ = listener.socket.accept()
                    except BlockingIOError:
                        continue
                    limit = 4 if event_value == "private" else 2
                    if len(self.connections) >= 6 or sum(item["kind"] == event_value for item in self.connections.values()) >= limit:
                        connection.close(); continue
                    try:
                        allowed = {os.geteuid()} if event_value == "private" else set(self.deployment.value["site"]["allowed_web_uids"])
                        peer = Peer.capture(connection, allowed_uids=allowed)
                        channel = Channel(connection)
                    except (OSError, ForgeError):
                        connection.close(); continue
                    descriptor = connection.fileno()
                    self.connections[descriptor] = dict(channel=channel, peer=peer, kind=event_value, stop=None, responded=False)
                    self.poll.register(connection, selectors.EVENT_READ, ("connection", descriptor))
                    continue
                descriptor = event_value
                item = self.connections.get(descriptor)
                if item is None:
                    continue
                channel, peer = item["channel"], item["peer"]
                try:
                    if events & selectors.EVENT_READ:
                        frame = channel.receive()
                        if frame is not None:
                            kind, request = frame
                            if kind != "json" or type(request) is not dict:
                                c.reject("Unsupported watchdog operation")
                            if item["kind"] == "priority":
                                self._priority_request(item, request)
                                continue
                            if request.get("operation") == "status":
                                c.fields(request, {"operation"}, label="watchdog status")
                                from demiflow.workbench.process import process_identity
                                if str(os.getpid()) not in read_control(self.deployment.paths.delegated_cgroup/"watchdog", "cgroup.procs").split():
                                    c.reject("Watchdog resource role changed", code="conflict")
                                channel.queue("json", dict(format="demiflow_analysis_watchdog_status_v1", identity=process_identity(os.getpid()), profile_sha256=self.profile_sha256, active=True))
                                self.poll.modify(channel.connection, selectors.EVENT_WRITE, ("connection", descriptor))
                                continue
                            if request.get("operation") in {"bind-read-owner", "stop-read-owner", "clear-read-owner", "read-owner-status"}:
                                operation = request["operation"]
                                c.fields(request, {"operation", "owner_digest"}, {"record"} if operation == "bind-read-owner" else set(), label="read owner control")
                                c.sha(request["owner_digest"])
                                channel.queue("json", self._read_owner_control(peer, operation, request["owner_digest"], record=request.get("record")))
                                self.poll.modify(channel.connection, selectors.EVENT_WRITE, ("connection", descriptor))
                                continue
                            if request.get("operation") in {"bind-owner", "clear-owner", "owner-status"}:
                                c.fields(request, {"operation", "owner_digest"}, {"record"} if request["operation"]=="bind-owner" else set(), label="priority owner control")
                                c.sha(request["owner_digest"])
                                channel.queue("json", self._owner_control(peer, request["operation"], request["owner_digest"],record=request.get("record")))
                                self.poll.modify(channel.connection, selectors.EVENT_WRITE, ("connection", descriptor))
                                continue
                            c.fields(request, {"operation", "lease"}, label="watchdog control")
                            if request["operation"] not in {"watch","clean"}:
                                c.reject("Unsupported watchdog operation")
                            lease = validate_lease(request["lease"])
                            if request["operation"]=="clean":
                                self._clean_watch(peer,lease)
                            elif (peer.identity != lease["controller"] or not peer.live()
                                    or str(peer.pid) not in read_control(self.deployment.paths.delegated_cgroup/"controller","cgroup.procs").split()):
                                c.reject("Only the original live controller can register its lease", code="operation_rejected")
                            else:
                                self._watch(lease)
                            channel.queue("json", dict(accepted=True, job_id=lease["job_id"], cleanup_proven=False))
                            self.poll.modify(channel.connection, selectors.EVENT_WRITE, ("connection", descriptor))
                    if events & selectors.EVENT_WRITE:
                        channel.flush()
                        if not channel.outgoing:
                            self._close_connection(descriptor)
                except (OSError, EOFError):
                    self._close_connection(descriptor)
                except ForgeError as exc:
                    self._failure(descriptor, item, exc)
            for descriptor, item in list(self.connections.items()):
                channel = item["channel"]
                wall = 6 if item["kind"] == "priority" else 3
                if channel.expired(wall_seconds=wall, idle_seconds=wall):
                    self._close_connection(descriptor)

    def close(self):
        self.running = False
        for descriptor in list(self.connections):
            self._close_connection(descriptor)
        for item in self.watches.values():
            try:
                item["watch"].tick(stop_requested=True)
            except (OSError, ForgeError):
                pass  # external supervisor still owns the failed observation
            item["watch"].close()
        self.watches.clear()
        if getattr(self, "read_monitor", None) is not None:
            self.read_monitor.close(); self.read_monitor = None
        self.poll.close(); self.listener.close(); self.priority_listener.close()
        # The host supervisor must stop owned jobs when this watcher exits.
