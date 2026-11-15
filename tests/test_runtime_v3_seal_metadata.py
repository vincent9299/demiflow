"""Native journal metadata: epoch-ms seal time, stable replay and legacy absence.

The journal is real; the content-addressed store is the D lane's bounded unit
fixture. Real workbench/browser coverage lives in execution_binding_browser.
"""
from test_runtime_v3_d import ACTION, env, make_graph, make_physical, _stage_graph
from demiflow.execution import dataflow_seal as seal


def test_first_seal_time_is_epoch_ms_and_reseal_preserves_every_identity(env, monkeypatch):
    graph = make_graph()
    physical = make_physical(ACTION, graph, env.store)
    staged = _stage_graph(env.action, graph, env.store)
    instant_ns = 1_794_527_206_071_123_456
    monkeypatch.setattr(seal, 'time_ns', lambda: instant_ns, raising=False)
    first = seal.seal_dataflow(env.action, staged, physical, store=env.store)
    original = env.run.journal.source_binding(ACTION + '-dataflow-sealed')
    assert type(original['sealed_at']) is int
    assert original['sealed_at'] == 1_794_527_206_071
    assert original['dag_id'] == staged.dag_id
    assert original['plan_id'] == physical.plan_id
    monkeypatch.setattr(seal, 'time_ns', lambda: instant_ns + 99_000_000_000)
    assert seal.seal_dataflow(env.action, staged, physical, store=env.store) == first
    assert env.run.journal.source_binding(ACTION + '-dataflow-sealed') == original
    loaded_graph, loaded_physical, loaded = seal.load_dataflow(
        env.run_dir, ACTION, store=env.store, bounds=physical.bounds)
    assert (loaded_graph, loaded_physical, loaded) == (staged, physical, first)


def test_legacy_seal_without_time_is_not_backfilled_or_reidentified(env, monkeypatch):
    graph = make_graph()
    physical = make_physical(ACTION, graph, env.store)
    staged = _stage_graph(env.action, graph, env.store)
    bind = env.run.journal.bind_source

    def legacy_bind(fence, key, value):
        # Produce an explicit pre-metadata history fixture without rewriting
        # an existing journal fact after it has been accepted.
        if key == ACTION + '-dataflow-sealed':
            value = {k: v for k, v in value.items() if k != 'sealed_at'}
        return bind(fence, key, value)

    with monkeypatch.context() as old:
        old.setattr(env.run.journal, 'bind_source', legacy_bind)
        first = seal.seal_dataflow(env.action, staged, physical, store=env.store)
    original = env.run.journal.source_binding(ACTION + '-dataflow-sealed')
    assert 'sealed_at' not in original
    from pytest import fail
    monkeypatch.setattr(seal, 'time_ns', lambda: fail('history must not invent a sealing time'), raising=False)
    assert seal.seal_dataflow(env.action, staged, physical, store=env.store) == first
    assert env.run.journal.source_binding(ACTION + '-dataflow-sealed') == original
    assert seal.load_dataflow(env.run_dir, ACTION, store=env.store, bounds=physical.bounds) == (staged, physical, first)
