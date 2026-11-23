"""IR-02: real native declarations must bind semantics and actual actors."""
import pytest
from demiflow import data
from demiflow.execution.recovery_identity import plan_identity
from demiflow.execution.seal.business_sources import plan_code_pin
from demiflow.execution.seal.plan_facts import operation_facts
from demiflow.data.runtime_contract import RuntimeContractError


def eligible(row):
    return True


def excluded(row):
    return False


def dedup(tmp_path, **options):
    return data.from_items([{'id': '1', 'group': 'a'}]).deduplicate('group',
        path=str(tmp_path/'dedup.sqlite'), key='id', keep_duplicates=True, **options)


@pytest.mark.parametrize('options', [{'when': eligible}, {'when': excluded},
                                    {'max_entries': 2}, {'max_key_bytes': 128},
                                    {'max_disk_bytes': 65536}])
def test_dedup_semantic_change_cannot_collide(tmp_path, options):
    a, b = dedup(tmp_path), dedup(tmp_path, **options)
    assert plan_identity('materialize', a._source, a._plan) != plan_identity('materialize', b._source, b._plan)
    assert plan_code_pin(a._plan) != plan_code_pin(b._plan)


@pytest.mark.parametrize('field,value', [('output', 'changed'), ('max_entries', 2), ('when', eligible)])
def test_dedup_actor_drift_rejected(tmp_path, field, value):
    ds = dedup(tmp_path)
    actor = ds._plan.operations[0].callable.target.__self__
    setattr(actor, field, value)
    with pytest.raises(RuntimeContractError, match='differs'):
        operation_facts(ds._plan.operations[0])


def vector(tmp_path):
    return data.from_items([{'q': [1., 2.]}]).search_vectors(query='q', output='hits',
        uri=str(tmp_path/'unopened.lance'), version=1, vector_column='v', columns=['id'])


@pytest.mark.parametrize('field', ['output', 'query', 'spec', 'options', 'filter_column'])
def test_vector_actual_actor_configuration_is_bound(tmp_path, field):
    ds = vector(tmp_path)
    op = ds._plan.operations[0]
    actor = op.callable.target.__self__
    if field == 'spec':
        actor.spec['top_k'] += 1
    elif field == 'options':
        actor.options['batch_size'] += 1
    else:
        setattr(actor, field, 'changed')
    for read in (lambda: operation_facts(op), lambda: plan_code_pin(ds._plan),
                 lambda: plan_identity('materialize', ds._source, ds._plan)):
        with pytest.raises(RuntimeContractError, match='differs'):
            read()
