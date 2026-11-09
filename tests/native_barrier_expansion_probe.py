"""Local 100 x 100 model/image/read substitutes; never calls a service."""
import asyncio
import hashlib
import json
from pathlib import Path
import sys
import time

from demiflow import data
from demiflow.data.plan import LogicalPlan
from demiflow.execution.run_context import RunContext, current_action
from demiflow.execution.recovery_identity import plan_identity
from demiflow.execution.stream_materialize import StreamMaterialization
from demiflow.objects import ObjectRef

root = Path(sys.argv[1])
started = time.monotonic()
mode = sys.argv[2] if len(sys.argv) > 2 else "materialize"


def expand(row):
    for child in range(100):
        yield {'id': row['id'], 'child': child}


async def prompt(row):
    await asyncio.sleep(0)
    return {**row, 'prompt': str(row['id'])+':'+str(row['child'])}


async def image(row):
    # Repeated image bytes are deliberate; occurrence multiplicity stays 10000.
    store = next(iter(current_action().object_sinks.values()))
    raw = ('fixture-pixels-' + str(row['child'])).encode()
    reference = await asyncio.to_thread(store.stage, raw)
    return {**row, 'image': reference}


async def read_and_score(row):
    raw = await asyncio.to_thread(ObjectRef(**row['image']).read, max_bytes=128)
    return {**row, 'score': len(raw)}


with RunContext.start(root, limits={'timeout_s': 600, 'max_row_bytes': 4096,
        'max_batch_bytes': 65536, 'checkpoint_rows': 16, 'checkpoint_interval_ms': 60000}) as run:
    graph = data.from_items([{'id': i} for i in range(100)]).flat_map(expand).map_async(
        prompt, concurrency=4).map_async(image, concurrency=4).map_async(read_and_score, concurrency=4)
    if mode == 'direct':
        import demiflow.execution.object_sink as object_module
        original_specs = object_module.image_object_specs
        original_bind = StreamMaterialization.bind_stages
        object_module.image_object_specs = lambda _: {str(root/'assets'): 128}
        def bind(self, stages):
            original_bind(self, stages)
            self.object_cut = 2
        StreamMaterialization.bind_stages = bind
        try:
            graph.write_lance(str(root/'result.lance'), mode='overwrite')
            import lance
            result = lance.dataset(str(root/'result.lance')).to_table(columns=['id', 'child'])
            keys = set(zip(result['id'].to_pylist(), result['child'].to_pylist()))
            facts = run.journal.execution_commits('action-1-stream')
            assert result.num_rows == len(keys) == 10000
            from types import SimpleNamespace
            stats = SimpleNamespace(metrics={'terminal': 'write_lance', 'version': 1})
        finally:
            object_module.image_object_specs = original_specs
            StreamMaterialization.bind_stages = original_bind
    else:
        with run.action(plan_identity('materialize', graph._source, graph._plan)) as action:
            terminal = StreamMaterialization(action, lambda: graph._executor.iter_rows(graph._source, LogicalPlan()), object_specs={str(root/'assets'): 128})
            # This local fixture shares one store; declaring the owner interval is
            # the same cut native PromptActor ownership discovers automatically.
            original = terminal.bind_stages
            def bind(stages):
                original(stages)
                terminal.object_cut = 2
            terminal.bind_stages = bind
            stats = graph._run_stream(consume=terminal.append, progress=terminal, source_iter=terminal.source_rows())
            terminal.finish()
            facts = run.journal.execution_commits(action.action_id)
            keys = set()
            for fact in facts:
                for row in terminal.row_sink.iter_rows(fact['receipts']['output']['data']['rows']):
                    keys.add((row['id'], row['child']))
            assert len(keys) == 10000 and terminal.rows == 10000
print(json.dumps({'terminal': mode, 'inputs': 100, 'children_per_input': 100, 'rows': 10000,
    'unique_occurrences': len(keys), 'commits': len(facts), 'seconds': time.monotonic()-started,
    'metrics': stats.metrics}, ensure_ascii=False))
