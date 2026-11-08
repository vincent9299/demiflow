"""Incremental publication receivers, driven by the controller event loop.

They accept one pre-reserved object and opaque chunks; no Arrow/Jupyter/HTML
decoding occurs in the controller. Current access is checked again at each
commit boundary. A lost connection retains its reservation and partial files
for the original owned-job cleanup/recovery path.
"""
from __future__ import annotations

from demiflow.workbench.analysis import contracts as c
from demiflow.workbench.analysis.object_files import Ingress
from demiflow.workbench.analysis.object_files import verify_object


class ObjectReceiver:
    def __init__(self, artifacts, access, state_root, reservation, *, execution_id, native, on_ready):
        if not callable(on_ready):
            c.reject("Publication requires its owning execution completion adapter")
        self.artifacts, self.access, self.state_root = artifacts, access, state_root
        self.reservation = reservation
        self.execution_id = c.identity(execution_id, "owning analysis execution")
        self.native, self.on_ready = native, on_ready
        self.ingress = None
        self.phase = "header"
        self.current_name = None
        self.expected_names = None
        self.count = 0
        self.bytes_digest = None

    def consume(self, kind, value):
        self.access.check("analysis_execute")
        if self.phase == "header":
            if kind != "json":
                c.reject("Publication requires a control header")
            if self.native:
                c.fields(value, {"version", "operation", "candidate"}, label="native result header")
                if value["version"] != 1 or type(value["version"]) is not int or value["operation"] != "native-result":
                    c.reject("Unsupported native result protocol")
                candidate = value["candidate"]
                c.fields(candidate, {"publish_id", "description", "files"}, label="native publication candidate")
                if candidate["publish_id"] != self.reservation["publish_id"]:
                    c.reject("Native result belongs to another reservation", code="conflict")
                description, files = candidate["description"], candidate["files"]
                if type(description) is not dict or description.get("format") != "lance":
                    c.reject("Native result requires its fixed Lance payload")
            else:
                c.fields(value, {"operation", "execution_id", "bytes", "sha256"}, label="Notebook output header")
                if value["operation"] != "notebook-output" or value["execution_id"] != self.execution_id:
                    c.reject("Notebook output belongs to another execution", code="conflict")
                c.integer(value["bytes"], "Notebook output bytes", 16*1024*1024, 1); c.sha(value["sha256"])
                description = dict(format="document", mime="application/vnd.demiflow.output+json",
                                   bytes=value["bytes"], sha256=value["sha256"], complete=True)
                files = {"document.json": dict(bytes=value["bytes"], sha256=value["sha256"])}
                self.bytes_digest = value["sha256"]
            admission = self.artifacts.receiving(self.access, self.reservation["id"])
            if admission["seed"]["execution_id"] != self.execution_id:
                c.reject("Publication reservation belongs to a different execution", code="conflict")
            self.ingress = Ingress(self.state_root, publish_id=admission["publish_id"], seed=admission["seed"], description=description, files=files)
            self.expected_names = list(sorted(files))
            self.phase = "file_header" if self.native else "file_data"
            if not self.native:
                self.ingress.start_file("document.json")
            return dict(accepted=True)
        if self.phase == "file_header":
            if kind == "end":
                if self.count != len(self.expected_names):
                    c.reject("Native output is missing files", code="conflict")
                return self._commit()
            if kind != "json" or self.count >= len(self.expected_names):
                c.reject("Unexpected native file header")
            c.fields(value, {"file", "bytes"}, label="native file header")
            name = self.expected_names[self.count]
            if value != dict(file=name, bytes=self.ingress.manifest["files"][name]["bytes"]):
                c.reject("Native file differs from its declared manifest/order", code="conflict")
            self.ingress.start_file(name); self.current_name = name; self.phase = "file_data"
            return None
        if self.phase == "file_data":
            if kind == "blob":
                with self.artifacts.database.guard.object_write():
                    self.ingress.write(value)
                return None
            if kind != "end":
                c.reject("Native file requires bounded byte chunks followed by end")
            self.ingress.finish_file(); self.count += 1
            if self.native:
                self.phase = "file_header"
                return None
            return self._commit()
        c.reject("Publication already finished; query its original receipt", code="conflict")

    def _commit(self):
        receipt = self.ingress.seal()
        identifier = self.reservation["id"]
        digest = self.ingress.digest
        self.artifacts.prepared(self.access, identifier, manifest=self.ingress.manifest, receipt=receipt)
        self.artifacts.begin_publish(self.access, identifier, expected_digest=digest)

        def authorize(publish_id, expected):
            self.access.check("analysis_execute")
            return publish_id == self.reservation["publish_id"] and expected == digest

        self.ingress.rename(authorize=authorize)
        ready = self.artifacts.commit_ready(self.access, identifier, expected_digest=digest,
            verify_renamed=lambda name, sha: verify_object(self.state_root, name, sha))
        self.on_ready(ready)
        self.phase = "committed"
        return dict(committed=True, manifest_digest=digest) if self.native else dict(committed=True, output_id=identifier, sha256=self.bytes_digest)

    def close(self):
        if self.ingress is not None:
            self.ingress.close()
        # No erase/free claim here: the owned sandbox/input cleanup witness is
        # required before an incomplete reservation can be released.
