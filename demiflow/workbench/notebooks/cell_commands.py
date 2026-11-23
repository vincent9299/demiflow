"""Authenticated command line channel for the notebook tab owning a cell turn."""
import json
import os
from pathlib import Path
import secrets
import shlex
import sys
import time

from demiflow.workbench.conversations.context import BridgeServer
from demiflow.workbench.conversations.common import integer, reject, text

METHOD = 'notebook/command'


class CellCommands(BridgeServer):
    def __init__(self, store, turn, deadline):
        self.store = store
        self.tid = turn['id'] if turn is not None else None
        self.epoch = turn['epoch'] if turn is not None else None
        self.deadline = deadline if turn is not None else 0.0
        super().__init__(self if turn is not None else None)
        self.bin = self.directory / 'bin'
        self.bin.mkdir(mode=0o700)
        self._write_binding()
        program = Path(__file__).with_name('data_cli.py')
        launcher = self.bin / 'data'
        launcher.write_text(f'#!/bin/sh\nexec {shlex.quote(sys.executable)} -IB {shlex.quote(str(program))} '
                            f'{shlex.quote(str(self.bin / "binding.json"))} "$@"\n')
        launcher.chmod(0o700)

    def _write_binding(self):
        payload = [self.token, self.tid, self.epoch] if (self.tid and self.epoch) else ['', '', '']
        tmp = self.bin / ('.binding-' + secrets.token_hex(6) + '.json')
        tmp.write_text(json.dumps(payload))
        os.replace(tmp, self.bin / 'binding.json')

    def set_turn(self, turn, deadline):
        with self.lock:
            self.tid = turn['id']
            self.epoch = turn['epoch']
            self.deadline = deadline
            self.input_access = self
            self.bound_identity = (self.tid, self.epoch)
            self.count = 0
            self._write_binding()

    def park_idle(self):
        with self.lock:
            self.tid = None
            self.epoch = None
            self.deadline = 0.0
            self.input_access = None
            self.bound_identity = None
            self.count = 0
            self._write_binding()

    def close(self):
        for entry in list(self.bin.iterdir()):
            entry.unlink(missing_ok=True)
        self.bin.rmdir()
        super().close()

    def require_active(self):
        if not self.tid or not self.epoch:
            reject('Notebook turn is no longer active', 'conflict')
        record = self.store.turn(self.tid)
        if record['epoch'] != self.epoch or record['stop_at'] or record['cleanup'] != 'pending' or time.time() >= self.deadline:
            reject('Notebook turn is no longer active', 'conflict')

    def dispatch(self, method, params, active):
        if method == 'result' and type(params) is dict and set(params) == {'id'}:
            with self.store.connect() as db:
                row = db.execute('SELECT request,answer,expires FROM pending WHERE id=? AND turn=? AND epoch=?',
                                 (text(params['id'], 128), self.tid, self.epoch)).fetchone()
            if not row or json.loads(row['request'])['method'] != METHOD:
                reject('Unknown notebook command')
            if row['answer'] is not None:
                return {'done': True, **json.loads(row['answer'])}
            if time.time() >= row['expires']:
                reject('Notebook did not return a result before this command expired', 'conflict')
            return {'done': False}
        fields = {'context.show': set(), 'catalog.list': {'group', 'offset'}, 'catalog.show': {'group', 'id', 'offset'},
                  'cell.show': set(), 'cell.edit': {'source'}, 'cell.run': set()}
        if method == 'catalog.list' and type(params) is dict and 'query' in params:
            text(params['query'], 256)
            fields[method].add('query')
        if method == 'catalog.show' and type(params) is dict and 'version' in params:
            integer(params['version'], 1, 2**63-1)
            fields[method].add('version')
        if method not in fields or type(params) is not dict or set(params) != fields[method]:
            reject('Unsupported notebook command')
        if method.startswith('catalog.') and params['group'] not in ('dictionary', 'tools'):
            reject('Unknown catalog')
        if 'offset' in params:
            integer(params['offset'], 0, 4096)
        if 'id' in params:
            text(params['id'], 4096)
        if 'source' in params:
            text(params['source'], 12000)
        key = self.store.request(self.tid, dict(rpc_id=secrets.token_hex(12), method=METHOD,
                                               command=method, arguments=params))
        return {'id': key}
