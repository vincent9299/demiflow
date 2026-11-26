"""Workbench Store adapter for Forge's Notebook assistant command bridge.

Forge owns the `data` CLI and command contract (`demiflow.forge.bridge.notebook_commands`);
this host adapter only binds Workbench's session/turn Store and pending cell-request queue.
"""
import json
import secrets
import time

from demiflow.forge.bridge.notebook_commands import NOTEBOOK_METHOD as METHOD
from demiflow.forge.bridge.notebook_commands import NotebookCommandBridge
from demiflow.workbench.conversations.common import reject


class CellCommands(NotebookCommandBridge):
    def __init__(self, store, turn, deadline):
        self.store = store
        self.tid = turn['id'] if turn is not None else None
        self.epoch = turn['epoch'] if turn is not None else None
        self.deadline = deadline if turn is not None else 0.0
        super().__init__(active_turn=self if turn is not None else None)

    def set_turn(self, turn, deadline):
        with self.lock:
            self.tid = turn['id']
            self.epoch = turn['epoch']
            self.deadline = deadline
            self.input_access = self
            self.bound_identity = (self.tid, self.epoch)
            self.count = 0
            self._write_binding(self.tid, self.epoch)

    def park_idle(self):
        with self.lock:
            self.tid = None
            self.epoch = None
            self.deadline = 0.0
            self.input_access = None
            self.bound_identity = None
            self.count = 0
            self._write_binding(None, None)

    def require_active(self):
        if not self.tid or not self.epoch:
            reject('Notebook turn is no longer active', 'conflict')
        record = self.store.turn(self.tid)
        if record['epoch'] != self.epoch or record['stop_at'] or record['cleanup'] != 'pending' or time.time() >= self.deadline:
            reject('Notebook turn is no longer active', 'conflict')

    def poll_result(self, request_id: str) -> dict:
        with self.store.connect() as db:
            row = db.execute(
                'SELECT request,answer,expires FROM pending WHERE id=? AND turn=? AND epoch=?',
                (request_id, self.tid, self.epoch),
            ).fetchone()
        if not row or json.loads(row['request'])['method'] != METHOD:
            reject('Unknown notebook command')
        if row['answer'] is not None:
            return {'done': True, **json.loads(row['answer'])}
        if time.time() >= row['expires']:
            reject('Notebook did not return a result before this command expired', 'conflict')
        return {'done': False}

    def submit_command(self, method: str, params: dict) -> str:
        return self.store.request(
            self.tid,
            dict(rpc_id=secrets.token_hex(12), method=METHOD, command=method, arguments=params),
        )
