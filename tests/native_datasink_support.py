"""Importable native Datasink fixture; driver and workers execute identical code."""
import json
from pathlib import Path
from demiflow.data.datasink import WriteResult
from demiflow.execution.run_context import current_run
from test_execution_recovery_entrypoints import FormalSink


class CallbackSink(FormalSink):
    def __init__(self, events, behavior='normal'):
        self.events, self.behavior = Path(events), behavior
        self.receipt = None

    def recovery_identity(self):
        return {**super().recovery_identity(), 'events':str(self.events), 'behavior':self.behavior}

    def record(self, event):
        with self.events.open('a') as stream:
            stream.write(json.dumps(event)+'\n')

    def on_write_start(self, schema=None):
        self.record(['start'])

    def prepare(self, blocks, context):
        self.record(['prepare'])
        if self.behavior == 'prepare_failure':
            raise ValueError('original native preparation failure')
        return super().prepare(blocks, context)

    def commit(self, prepared, context):
        self.record(['commit'])
        return super().commit(prepared, context)

    def on_write_complete(self, result):
        assert isinstance(result, WriteResult)
        assert all(c['status']=='acked' for c in current_run().journal.execution_commits('action-1'))
        self.receipt = result
        self.record(['complete', result.written_rows])
        marker = self.events.with_suffix('.completed-once')
        if self.behavior == 'complete_failure' and not marker.exists():
            marker.write_text('already called after publication')
            raise RuntimeError('completion callback failure')

    def on_write_failed(self, error):
        self.record(['failed', str(error)])
        if self.behavior == 'prepare_failure':
            raise LookupError('secondary failure callback failure')

