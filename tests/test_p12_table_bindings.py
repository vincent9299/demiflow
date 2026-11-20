"""Explicit mixed-selector capability preserves owned fixed references."""
from pathlib import Path
import inspect

import pytest

from demiflow.forge.schema import ForgeError
from demiflow.workbench.table_bindings import make_resolver
from demiflow.workbench.projects import workspace as ws, checks


def test_resolver_passes_current_control_and_actual_owner_objects(tmp_path):
    from demiflow.lance.refs import DatasetRef
    ref = DatasetRef('fixed', 'fixed.lance', 1, 'fixed', '1', 'a' * 64, 3, store_id='local')
    result = ({'ref': ref.to_dict()}, ref, tmp_path / 'fixed.lance')
    observed = []
    def registered(root, control, selector):
        observed.append((root, control, selector))
        return result
    def run_result(root, run_id, output, *, attempt):
        observed.append((root, run_id, output, attempt))
        return result
    resolver = make_resolver(registered, run_result)
    first, second = {'locations': {'first': {}}}, {'locations': {'second': {}}}
    assert resolver(tmp_path, first, 'fixed') is result
    assert resolver(tmp_path, second, 'fixed') is result
    assert observed[0][1] is first and observed[1][1] is second
    for selector, attempt in [('run:r123:out', None), ('run:r123:out:1', 1), ('run:r123:out:32', 32)]:
        assert resolver(tmp_path, second, selector) is result
        assert observed[-1] == (tmp_path, 'r123', 'out', attempt)
    assert result[1] is ref and isinstance(result[2], Path)


@pytest.mark.parametrize('selector', ['run:r:out:0', 'run:r:out:33', 'run:r:out:01',
    'run:r:out:True', 'run:r', 'run:r:out:1:extra'])
def test_invalid_attempt_cannot_reach_run_owner(selector):
    def forbidden(*a, **k): pytest.fail('invalid selector reached an owner')
    with pytest.raises(ForgeError):
        make_resolver(forbidden, forbidden)(Path('/fixture'), {}, selector)


@pytest.mark.parametrize('target', [ws.resolve_arguments, ws._inspect_flow, ws._report_status,
    ws._audit_check, checks.check, checks.status])
def test_core_requires_explicit_resolver_and_has_no_domain_fallback(target):
    parameter = inspect.signature(target).parameters['table_resolver']
    assert parameter.kind is inspect.Parameter.KEYWORD_ONLY
    assert parameter.default is inspect.Parameter.empty
    signature = inspect.signature(target)
    supplied = {name: None for name, item in signature.parameters.items()
                if name != 'table_resolver' and item.default is inspect.Parameter.empty}
    with pytest.raises(TypeError, match='table_resolver'):
        signature.bind(**supplied)
