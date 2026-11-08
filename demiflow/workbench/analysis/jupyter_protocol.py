"""Bounded Jupyter output state, executed inside the kernel job's sandbox.

This parser is not the outer controller's wire protocol. Multipart/ZMQ parsing
and rich data remain in the kernel resource group; only bounded opaque output
references cross the restricted controller bridge. It starts no processes.
"""
from __future__ import annotations

from demiflow.workbench.analysis import contracts as c


class ExecutionOutput:
    def __init__(self, *, generation, msg_id, output_bytes=16*1024*1024):
        self.generation = c.identity(generation, "kernel generation")
        self.msg_id = c.identity(msg_id, "Jupyter parent message ID")
        self.maximum = c.integer(output_bytes, "execution output limit", 16*1024*1024, 1024)
        self.outputs = []
        self.sizes = []
        self.bytes = 2
        self.wire_bytes = 0
        self.stream_bytes = 0
        self.messages = 0
        self.displays = {}
        self.clear_wait = False
        self.reply = None
        self.idle = False
        self.failed = False
        self.limit_exceeded = False
        self.ignored = 0

    @property
    def complete(self):
        return self.reply is not None and self.idle

    @property
    def status(self):
        if self.limit_exceeded:
            return "output_limit"
        if not self.complete:
            return "running"
        return "error" if self.failed or self.reply != "ok" else "succeeded"

    def _clear(self):
        self.outputs.clear()
        self.sizes.clear()
        self.displays.clear()
        self.bytes = 2
        self.clear_wait = False

    def _limit(self):
        self.limit_exceeded = True
        c.reject("Notebook output limit exceeded; interrupt/cleanup required", code="resource_limit")

    def _put(self, output, *, display_id=None, update=False):
        try:
            size = c.encoded_size(output, maximum=self.maximum, nodes=32768, depth=16)
        except ValueError as error:
            if getattr(error, "code", None) == "resource_limit":
                self._limit()
            raise
        if self.clear_wait:
            self._clear()
        if update:
            indices = self.displays.get(display_id, [])
            proposed = self.bytes
            replacements = []
            for index in indices:
                # execute_result retains its execution_count and type.
                candidate = {**self.outputs[index], "data": output["data"], "metadata": output["metadata"]}
                candidate_size = c.encoded_size(candidate, maximum=self.maximum, nodes=32768, depth=16)
                proposed += candidate_size - self.sizes[index]
                if proposed > self.maximum:
                    self._limit()
                replacements.append((index, candidate, candidate_size))
            for index, candidate, candidate_size in replacements:
                self.outputs[index] = candidate
                self.sizes[index] = candidate_size
            self.bytes = proposed
            if not indices:
                self.ignored += 1
            return
        if (len(self.outputs) >= 32 or self.bytes + size + bool(self.outputs) > self.maximum
                or display_id is not None and len(self.displays) >= 32 and display_id not in self.displays):
            self._limit()
        index = len(self.outputs)
        self.outputs.append(output)
        self.sizes.append(size)
        self.bytes += size + bool(index)
        if display_id is not None:
            self.displays.setdefault(display_id, []).append(index)

    def consume(self, channel, message, *, generation):
        """Return False for other parents/generations and all post-completion data."""
        if generation != self.generation or self.complete or self.limit_exceeded:
            self.ignored += 1
            return False
        if type(message) is not dict or type(message.get("parent_header")) is not dict or message["parent_header"].get("msg_id") != self.msg_id:
            self.ignored += 1
            return False
        from demiflow.workbench.analysis.output_messages import jupyter_envelope
        message = jupyter_envelope(message)
        try:
            self.wire_bytes += c.encoded_size(message, maximum=8*1024*1024, nodes=32768, depth=16)
        except ValueError as error:
            if getattr(error, "code", None) == "resource_limit":
                self._limit()
            raise
        self.messages += 1
        if self.messages > 4096 or self.wire_bytes > 64*1024*1024:
            self._limit()
        content = message.get("content")
        if type(content) is not dict:
            c.reject("Jupyter message content must be an object")
        header = message.get("header", {})
        if type(header) is not dict:
            c.reject("Invalid Jupyter header")
        kind = c.text(header.get("msg_type"), "Jupyter message type", 128)
        if channel == "shell" and kind == "execute_reply":
            self.reply = c.one_of(content.get("status"), {"ok", "error", "abort", "aborted"}, "execute_reply status")
            self.failed |= self.reply != "ok"
            return True
        if channel != "iopub":
            if channel == "stdin":
                c.reject("Notebook stdin interaction is unsupported", code="operation_rejected")
            return False
        if kind == "status":
            self.idle |= content.get("execution_state") == "idle"
        elif kind == "clear_output":
            if type(content.get("wait")) is not bool:
                c.reject("Invalid clear_output wait flag")
            if content["wait"]:
                self.clear_wait = True
            else:
                self._clear()
        elif kind == "stream":
            name = c.one_of(content.get("name"), {"stdout", "stderr"}, "stream name")
            value = c.text(content.get("text"), "stream text", 64*1024, empty=True)
            self.stream_bytes+=len(value.encode("utf-8"))
            if self.stream_bytes>1024*1024:self._limit()
            self._put(dict(output_type="stream", name=name, text=value))
        elif kind in {"display_data", "execute_result", "update_display_data"}:
            data, metadata = content.get("data"), content.get("metadata", {})
            if type(data) is not dict or len(data) > 16 or type(metadata) is not dict:
                c.reject("Invalid MIME bundle")
            for mime in data:
                c.text(mime, "output MIME", 128)
            c.encoded_size(metadata, maximum=65536)
            transient = content.get("transient", {})
            if type(transient) is not dict:
                c.reject("Invalid transient display metadata")
            display_id = transient.get("display_id")
            if display_id is not None:
                c.text(display_id, "display ID", 128)
            if kind == "update_display_data" and display_id is None:
                c.reject("update_display_data requires a display ID")
            output = dict(output_type="execute_result" if kind == "execute_result" else "display_data", data=data, metadata=metadata)
            if kind == "execute_result":
                output["execution_count"] = c.integer(content.get("execution_count"), "execution count", 2**31-1)
            self._put(output, display_id=display_id, update=kind == "update_display_data")
        elif kind == "error":
            traceback = c.sequence(content.get("traceback"), "traceback", 128)
            for line in traceback:
                c.text(line, "traceback line", 4096, empty=True)
            output = dict(output_type="error", ename=c.text(content.get("ename"), "error name", 256),
                          evalue=c.text(content.get("evalue"), "error value", 4096, empty=True), traceback=traceback)
            self.failed = True
            self._put(output)
        else:
            # comm/widget and unknown messages cannot execute UI code.
            self.ignored += 1
        return True

    def snapshot(self):
        return dict(generation=self.generation, parent_msg_id=self.msg_id, status=self.status,
                    complete=self.complete, outputs=self.outputs, output_bytes=self.bytes,
                    messages=self.messages, ignored_messages=self.ignored,
                    stdin_supported=False, active_widgets_supported=False)
