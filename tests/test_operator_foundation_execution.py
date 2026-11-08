"""Real native execution and mutation checks for the generic contract path."""
import importlib.util
from pathlib import Path
import sys

import lance
import pyarrow as pa
import pytest

from demiflow import data
from demiflow.data.flow_contract import field, scalar
from demiflow.data.runtime_contract import RuntimeContractError
from demiflow.execution.checked import CheckedExecution
from demiflow.forge.flow_check import FlowChecker
from demiflow.forge.platform import inspect_modules, LIMITS
from demiflow.workbench.runs.manager import DEFAULT_LIMITS as RUN_LIMITS
from test_forge_native_semantics import native


def _load(monkeypatch, name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, name, module)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("mode", ['execute', 'mutate', 'observation_failure'])
@pytest.mark.parametrize("union", [False, True])
def test_real_native_alias_executes_and_parameter_drift_fails(tmp_path, monkeypatch, mode, union):
    # A real additional public name, backed by the original native callable.
    # No Forge checker, runner or UI branch knows the alias spelling.
    monkeypatch.setattr(data.Dataset, "transform", data.Dataset.map, raising=False)
    platform_root = Path(data.__file__).resolve().parents[1]
    catalog = inspect_modules(["demiflow.data"], platform_root, LIMITS)
    assert catalog["inspection_complete"], catalog["diagnostics"]
    source, target = str(tmp_path / "source.lance"), str(tmp_path / "result.lance")
    lance.write_dataset(pa.Table.from_pylist([{"x": 1}, {"x": 2}]), source)
    (tmp_path / "flows").mkdir()
    (tmp_path / "operators").mkdir()
    operator_path = tmp_path / "operators" / "increment.py"
    operator_path.write_text("def increment(x: int | None, *, amount: int = 1) -> int:\n    return int(x) + amount\n")
    flow_path = tmp_path / "flows" / "example.py"
    flow_path.write_text(
        "from demiflow import data\nfrom operators.increment import increment\n"
        "def run(*, source, target):\n"
        "    rows = data.read_lance(source, version=1).transform(increment, inputs=['x'], output='x', fn_kwargs={'amount': 3})\n"
        "    return rows.write_lance(target, mode='create', return_receipt=True)\n"
    )
    if union:
        flow_path.write_text(flow_path.read_text().replace('.transform(', '.union().transform('))
    tables = {"source": {"uri": source, "version": 1, "fields": {"x": field(scalar("int", nullable=True))}}}
    report = FlowChecker(tmp_path, catalog, tables, [target]).inspect("flows/example.py:run", {"source": source, "target": target})
    assert report["status"] == "checked", report["diagnostics"]
    assert any(n["api"].endswith(".transform") for n in report["nodes"])
    _load(monkeypatch, "operators.increment", operator_path)
    flow = _load(monkeypatch, "flows.example", flow_path)
    events = []
    context = CheckedExecution(
        nodes=report["nodes"], edges=report["edges"], actions=report["actions"],
        operators=report["operators"], source_root=tmp_path,
        limits={**RUN_LIMITS, "batch_rows": 16},
        record=lambda *event: events.append(event), check_stop=lambda: None,
    )
    if mode == 'observation_failure':
        def failed_final_snapshot(updates):
            if any(update['document']['state'] == 'succeeded' for update in updates):
                raise OSError('fixture observation disk failure')
        context.observe = failed_final_snapshot
    if mode == 'mutate':
        original = data.Dataset.map

        def changed(self, fn, **kwargs):
            kwargs["fn_kwargs"] = {"amount": 99}
            return original(self, fn, **kwargs)

        monkeypatch.setattr(data.Dataset, "transform", changed)
    with native(tmp_path):
        if mode == 'mutate':
            with pytest.raises(RuntimeContractError, match="fn_kwargs"):
                with context.bind_context():
                    flow.run(source=source, target=target)
            assert not Path(target).exists()
            assert not events
        elif mode == 'observation_failure':
            with pytest.raises(RuntimeContractError, match='observations could not be persisted'):
                with context.bind_context():
                    flow.run(source=source, target=target)
            # A committed native table survives an observation failure. The
            # overall action failed; its durable receipt must not be erased.
            assert sorted(lance.dataset(target).to_table().column('x').to_pylist()) == [4, 5]
            assert [event[1] for event in events] == ['intent', 'receipt', 'failed']
        else:
            with context.bind_context():
                receipt = flow.run(source=source, target=target)
            assert receipt.status == "committed"
            assert sorted(lance.dataset(target).to_table().to_pylist(), key=lambda r: r['x']) == [{"x": 4}, {"x": 5}]
            assert [event[1] for event in events] == ["intent", "receipt"]
