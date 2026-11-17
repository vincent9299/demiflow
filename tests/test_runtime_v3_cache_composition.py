"""Cached state resumes into a materialized result on the actual provider."""
from functools import partial
import json
import sqlite3
import time

import pytest

from demiflow import data
from demiflow.execution.dataflow import DataflowExecution
from demiflow.execution.run_context import RunContext
from demiflow.execution.sealed_plans import load_sealed_plan
from test_runtime_v3_reducer_values import provider, value_executor


class Counter:
    def __init__(self):
        self.count = 0

    def __call__(self, value):
        self.count += 1
        return self.count


class Interrupted(Exception):
    pass


def _counter(kind):
    if kind == 'actor':
        return Counter()
    if kind == 'closure':
        count = 0

        def count_closure(value):
            nonlocal count
            count += 1
            return count

        return count_closure
    if kind == 'default':
        def count_default(value, counts=[0]):
            counts[0] += 1
            return counts[0]

        return count_default
    assert kind == 'partial'

    def count_partial(value, *, counts):
        counts[0] += 1
        return counts[0]

    return partial(count_partial, counts=[0])


@pytest.mark.parametrize('callback_kind', ['actor', 'closure', 'default', 'partial'])
def test_cached_actor_resume_materialize_shuffle_and_read(provider, callback_kind, tmp_path, monkeypatch,
                                                        record_property):
    def flow():
        return data.from_items([{'value': n} for n in range(6)]).map_cached(
            _counter(callback_kind), cache_dir=tmp_path / 'cache', version='v1',
            inputs=['value'], output='count')

    original = DataflowExecution.pump
    reached = []

    def interrupt(execution, **kwargs):
        result = original(execution, **kwargs)
        if execution._restore_image is not None and not execution._restore_image.cut.final:
            from demiflow.execution.kernels import state_codec
            from demiflow.execution.dataflow_kernel_pack import PACK_MEDIA
            nodes = {node.occurrence_id for node in execution._graph.nodes
                     if node.kernel.kind == 'AsyncMapOp'}
            images = []
            for contribution in execution._restore_image.contributions:
                for occurrence, reference in contribution.operator_states.items():
                    if occurrence not in nodes:
                        continue
                    # Stateful cache images use the original individual object.
                    assert reference.media_type != PACK_MEDIA
                    raw = b''.join(execution._store.read(reference,
                        max_bytes=execution._bounds.max_state_bytes))
                    images.append(state_codec.loads(raw, max_bytes=execution._bounds.max_state_bytes))
            if not images or not images[0]['call_ordinal']:
                return result
            assert len(images) == 1
            assert images[0]['state']['value']['cache_frontier']['generation'] >= 1
            accepted_calls = images[0]['call_ordinal']
            # Require the actual failure window: shared cache is ahead of the
            # authenticated state image, on both Local and Ray workers. The
            # number allowed to prefetch belongs to the runtime's finite credit.
            deadline = time.monotonic() + 15
            while True:
                with sqlite3.connect(tmp_path / 'cache' / 'results.sqlite') as database:
                    cached = database.execute('SELECT COUNT(*) FROM kv').fetchone()[0]
                if cached > accepted_calls:
                    break
                assert time.monotonic() < deadline, 'cache did not advance beyond the original ACK'
                time.sleep(.01)
            reached.append({'commit': execution._restore_image.commit_id,
                            'accepted_calls': accepted_calls, 'cached_results': cached})
            raise Interrupted('after original nonfinal ACK')
        return result

    with pytest.raises(Interrupted):
        with RunContext.start(tmp_path / 'run', limits={'checkpoint_rows': 2}) as run:
            reference = run.ref
            with monkeypatch.context() as patch:
                patch.setattr(DataflowExecution, 'pump', interrupt)
                flow().materialize()
    assert reached
    record_property('cache_frontier_window', json.dumps(reached))

    with RunContext.resume(reference) as run:
        result = flow().materialize()
        try:
            expected = [{'value': n, 'count': n + 1} for n in range(6)]
            assert result.take_all() == expected
            one = result.random_shuffle(seed=7).take_all()
            two = result.random_shuffle(seed=7).take_all()
            assert one == two
            assert sorted(one, key=lambda row: row['value']) == expected
            sealed = load_sealed_plan(run.directory, 'action-1')
            assert sealed['physical'].policy.provider == provider
            commits = run.journal.execution_commits('action-1')
            assert commits and all(c['status'] == 'acked' for c in commits)
        finally:
            result.release()
