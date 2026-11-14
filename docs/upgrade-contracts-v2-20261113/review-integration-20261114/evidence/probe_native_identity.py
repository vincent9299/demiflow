"""Run with the checked-out source on PYTHONPATH; no HTTP requests."""
from pathlib import Path
import json
import demiflow
from demiflow import data
from demiflow.embeddings import EmbeddingModel
from demiflow.execution.recovery_identity import plan_identity
from demiflow.execution.chain import plan_code_pin
from demiflow.execution.plan_facts import operation_facts
assert Path(demiflow.__file__).resolve().is_relative_to(Path.cwd())
model = EmbeddingModel('fixture', 'r1', 3, 'http://127.0.0.1:1/v1')
def build(**extra):
    return data.from_items([{'text': 'x'}]).map_embeddings(
        model=model, inputs={'text': 'text'}, concurrency=1, **extra)
def semantic(ds):
    return plan_identity('materialize', ds._source, ds._plan)
a = build(batch_size=2)
result = {}
for name, b in [('batch_size', build(batch_size=3)),
                ('call_output', build(batch_size=2, call_output='audit')),
                ('error_output', build(batch_size=2, error_output='error')),
                ('request_budget', build(batch_size=2, max_requests=1))]:
    result[name] = dict(same_action_identity=semantic(a) == semantic(b),
                        same_code_pin=plan_code_pin(a._plan) == plan_code_pin(b._plan),
                        same_full_facts=operation_facts(a._plan.operations[-1]) == operation_facts(b._plan.operations[-1]))
before = operation_facts(a._plan.operations[-1])
a._stages[-1].output = 'changed'
result['mutated_actor_output'] = dict(facts_unchanged=before == operation_facts(a._plan.operations[-1]))
print(json.dumps(result, indent=2))
