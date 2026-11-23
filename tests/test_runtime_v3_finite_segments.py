"""Finite producer checkpoints publish each payload tail once."""
import json
from dataclasses import replace
from types import SimpleNamespace

from demiflow import data
from demiflow.execution.run_context import RunContext
from demiflow.execution.storage.segments import block_chunks, block_totals, FINITE_RECORDS_MEDIA
from demiflow.execution.worker.terminal_blocks import TerminalBlocks
from test_runtime_v3_artifact_accounting import store_fixture
from test_runtime_v3_c import bounds, make


def test_old_flat_finite_prefix_can_append_into_same_incremental_forest(tmp_path):
    with store_fixture(tmp_path) as (_, store, port):
        limits = bounds(max_state_bytes=1024*1024)
        services = SimpleNamespace(store=store,bounds=limits)
        old = b'{"target":"p","n":1}\n'
        ref = store.put(iter([old]),max_bytes=len(old),media_type=FINITE_RECORDS_MEDIA)
        state = TerminalBlocks(services,'p',partitioned=False)
        state.restore({'rows':1,'bytes':len(old),'partitions':[{'data_ref':vars(ref),'rows':1,'bytes':len(old)}]})
        new = b'{"target":"p","n":2}\n'
        state.append(make('Record'),new)
        from demiflow.execution.contracts.dataflow import ArtifactRef
        combined = ArtifactRef(**state.manifest_partitions()[0]['data_ref'])
        assert block_totals(store,combined,limits)==(2,len(old)+len(new))
        assert b''.join(block_chunks(store,combined,limits))==old+new
        assert store.inspect(ref)
        state.close()


def test_real_finite_cuts_do_not_rewrite_committed_payload_prefix(tmp_path):
    count = 24
    # Bound admission as well as the trigger: a fast source must not race
    # all 24 rows past the first checkpoint request on an unloaded machine.
    with RunContext.start(tmp_path/'run',limits={'checkpoint_rows':4,
            'checkpoint_unconfirmed_rows':4, 'checkpoint_interval_ms':60000,'max_row_bytes':8192,'max_batch_bytes':16384}) as run:
        result = data.from_items([{'n':i,'value':'x'*128} for i in reversed(range(count))]).sort('n').take_all()
        assert result == [{'n':i,'value':'x'*128} for i in range(count)]
        assert len(run.journal.execution_commits('action-1')) >= 4
        finite_rows = 0
        for path in (run.directory/'.dataflow-store').glob('*/objects/*'):
            raw = path.read_bytes()
            if not raw.startswith(b'{"document":'):
                continue
            documents = [json.loads(line) for line in raw.splitlines()]
            if all('target' in item and 'record_id' in item for item in documents):
                finite_rows += len(documents)
        # Only source -> sort is finite; each of its input records is stored once
        # across all checkpoint artifacts, not once for every later prefix.
        assert finite_rows == count
