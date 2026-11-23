"""The input port preserves admission order and one executor context."""
from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from demiflow.data.execution_contract import InputSpec, _current_executor, _use_executor
from demiflow.execution.executors.local import LocalDatasetExecutor
from demiflow.execution.executors.base import prepare_public_items


def test_managed_input_rejection_precedes_host_selection(monkeypatch):
    import demiflow.execution.pipeline.checked as checked
    monkeypatch.setattr(checked, 'current', lambda: object())
    def select():
        pytest.fail('invalid managed input selected a host')
    with pytest.raises(ValueError, match='1..128'):
        prepare_public_items(InputSpec('public_items', []), select_executor=select)
    with pytest.raises(Exception, match='budget|limit|bytes'):
        prepare_public_items(InputSpec('public_items', [{'x': 'x' * (256 * 1024)}]), select_executor=select)


def test_public_registration_occurs_inside_action_after_host_selection(monkeypatch):
    import demiflow.execution.pipeline.checked as checked
    events = []
    monkeypatch.setattr(checked, 'current', lambda: None)
    @contextmanager
    def action(kind, source, plan, *, arguments):
        assert kind == 'from_items' and source is None and plan is None
        events.append(('enter', arguments))
        yield SimpleNamespace(materialized=lambda handle: events.append(('materialized', list(handle.rows()))))
        events.append(('exit',))
    monkeypatch.setattr(checked, 'action', action)
    executor = LocalDatasetExecutor()
    def select():
        events.append(('select',))
        return executor
    handle, selected = prepare_public_items(InputSpec('public_items', [{'x': 1}]), select_executor=select)
    assert selected is executor and handle.row_count == 1
    assert events == [('enter', {'items': [{'x': 1}]}), ('select',), ('materialized', [{'x': 1}]), ('exit',)]


def test_raw_input_keeps_copy_and_does_not_enter_checked(monkeypatch):
    import demiflow.execution.pipeline.checked as checked
    monkeypatch.setattr(checked, 'action', lambda *a, **k: pytest.fail('raw input opened an action'))
    rows = [{'x': 1}]
    executor = LocalDatasetExecutor()
    handle = executor.prepare_input(InputSpec('items', rows))
    rows[0]['x'] = 2
    assert list(handle.rows()) == [{'x': 1}]
    assert executor.prepare_input(InputSpec('items', [])).row_count == 0
    with pytest.raises(TypeError, match='materialized list'):
        executor.prepare_input(InputSpec('items', iter(rows)))


def test_context_alias_and_nested_reset_share_one_slot():
    from demiflow.data.api import _current_executor as original_import
    assert original_import is _current_executor
    outer, inner = object(), object()
    initial = _current_executor.get()
    with _use_executor(outer):
        assert original_import.get() is outer
        with _use_executor(inner):
            assert _current_executor.get() is inner
        assert _current_executor.get() is outer
    assert _current_executor.get() is initial
