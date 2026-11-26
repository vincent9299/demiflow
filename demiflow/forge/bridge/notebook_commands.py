"""Forge-owned Notebook assistant command schema, validation, and authenticated bridge."""
from __future__ import annotations

import json
import os
from pathlib import Path
import secrets
import shlex
import sys

from .common import integer, reject, text
from .server import BridgeServer

NOTEBOOK_METHOD = 'notebook/command'
NOTEBOOK_CONTEXT_TOPICS = frozenset({'cell', 'before', 'output', 'env', 'catalog'})
NOTEBOOK_CATALOG_GROUPS = frozenset({'dictionary', 'tools'})


def validate_notebook_command(method: str, params):
    """Validate Notebook assistant command name and bounded arguments."""
    fields = {
        'context.show': set(),
        'catalog.list': {'group', 'offset'},
        'catalog.show': {'group', 'id', 'offset'},
        'cell.show': set(),
        'cell.edit': {'source'},
        'cell.run': set(),
        'tool.write': {'path', 'source'},
    }
    if method == 'context.show' and type(params) is dict and 'topic' in params:
        if params['topic'] not in NOTEBOOK_CONTEXT_TOPICS:
            reject('Unknown context topic')
        fields[method].add('topic')
    if method == 'catalog.list' and type(params) is dict and 'query' in params:
        text(params['query'], 256)
        fields[method].add('query')
    if method == 'catalog.show' and type(params) is dict and 'version' in params:
        integer(params['version'], 1, 2**63 - 1)
        fields[method].add('version')
    if method not in fields or type(params) is not dict or set(params) != fields[method]:
        reject('Unsupported notebook command')
    if method.startswith('catalog.') and params['group'] not in NOTEBOOK_CATALOG_GROUPS:
        reject('Unknown catalog')
    if 'offset' in params:
        integer(params['offset'], 0, 4096)
    if 'id' in params:
        text(params['id'], 4096)
    if 'path' in params:
        text(params['path'], 128)
    if 'source' in params:
        text(params['source'], 65536 if method == 'tool.write' else 12000)
    return method, params


class NotebookCommandBridge(BridgeServer):
    """Authenticated command bridge for a Notebook assistant turn.

    Forge owns the `data` CLI launcher, turn-bound token file, and command contract;
    the host (e.g. Workbench) injects turn liveness, command submission, and result polling.
    """

    def __init__(self, *, active_turn=None):
        super().__init__(active_turn)
        self.bin = self.directory / 'bin'
        self.bin.mkdir(mode=0o700)
        self._write_binding(
            active_turn.tid if active_turn is not None else None,
            active_turn.epoch if active_turn is not None else None,
        )
        program = Path(__file__).with_name('notebook_cli.py')
        launcher = self.bin / 'data'
        launcher.write_text(
            f'#!/bin/sh\nexec {shlex.quote(sys.executable)} -IB {shlex.quote(str(program))} '
            f'{shlex.quote(str(self.bin / "binding.json"))} "$@"\n',
            encoding='utf-8',
        )
        launcher.chmod(0o700)

    def _write_binding(self, tid, epoch):
        payload = [self.token, tid, epoch] if (tid and epoch) else ['', '', '']
        tmp = self.bin / ('.binding-' + secrets.token_hex(6) + '.json')
        tmp.write_text(json.dumps(payload), encoding='utf-8')
        os.replace(tmp, self.bin / 'binding.json')

    def poll_result(self, request_id: str) -> dict:
        raise NotImplementedError

    def submit_command(self, method: str, params: dict) -> str:
        raise NotImplementedError

    def dispatch(self, method, params, active):
        if method == 'result' and type(params) is dict and set(params) == {'id'}:
            return self.poll_result(text(params['id'], 128))
        validate_notebook_command(method, params)
        return {'id': self.submit_command(method, params)}

    def close(self):
        for entry in list(self.bin.iterdir()):
            entry.unlink(missing_ok=True)
        self.bin.rmdir()
        super().close()
