"""Checked prepend rebinding uses the original view's replacement contract."""
from dataclasses import replace
from types import SimpleNamespace

from demiflow import data
from demiflow.data.dataset import Dataset
from demiflow.data.plan import LogicalPlan
from demiflow.execution.pipeline import checked


def test_checked_prefix_rebinding_keeps_view_owner_without_constructing_dataset(monkeypatch):
    main = data.from_items([])
    prefix = data.from_items([{'id': 1}])
    operation = main.prepend(prefix, max_rows=1)._plan.operations[0]
    rebound_source = object()
    replaced = []
    replacement = object()

    class View:
        _source, _plan = prefix._source, prefix._plan
        def replace_plan(self, source, plan):
            replaced.append((source, plan))
            return replacement

    operation = replace(operation, source_dataset=View())
    owner = checked.CheckedExecution.__new__(checked.CheckedExecution)
    owner.nodes = {'prepend': {}, 'prefix': {}, 'main': {}}
    owner.parents = {'prepend': ['main', 'prefix']}
    owner._operation = lambda op, node: op
    owner._source = lambda source, identity, depth: (
        rebound_source if identity == 'prefix' else source, LogicalPlan())
    monkeypatch.setattr(checked, 'operation_facts', lambda op: {})
    monkeypatch.setattr(checked, 'check_composition', lambda facts: {'supported': True, 'driver': 'batch'})
    def forbidden(*args, **kwargs):
        raise AssertionError('checking a prefix must not construct an SDK Dataset')
    monkeypatch.setattr(Dataset, '__init__', forbidden)
    source, plan = owner.plan(main._source, LogicalPlan((operation,)), 'prepend')
    assert source is main._source
    assert replaced == [(rebound_source, LogicalPlan())]
    assert plan.operations[0].source_dataset is replacement
