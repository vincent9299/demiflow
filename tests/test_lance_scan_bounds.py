import lance
import pyarrow as pa
import pytest

from demiflow.data.api import DataAPI
from demiflow.errors import InvalidLanceRequest
from demiflow.lance.model import LanceScanSpec
from demiflow.lance import read


def test_explicit_scan_bounds_reach_lance_and_preserve_rows(tmp_path, monkeypatch):
    uri = str(tmp_path / 'rows.lance')
    lance.write_dataset(pa.Table.from_pylist([{'i': i} for i in range(7)]), uri)
    real = read.open_lance_dataset
    import json
    import os
    probe = tmp_path / 'scan-calls.jsonl'
    injected = tmp_path / 'worker-probe'; injected.mkdir()
    # Process-local monkeypatches do not cross the real managed worker boundary.
    # This probe delegates every call unchanged and records only scalar bounds.
    (injected / 'sitecustomize.py').write_text(
        "import json, os, lance\n"
        "_scan = lance.LanceDataset.scanner\n"
        "def _tracked(self, **kwargs):\n"
        "    if 'batch_size' in kwargs:\n"
        "        with open(" + repr(str(probe)) + ", 'a') as stream:\n"
        "            stream.write(json.dumps({'pid': os.getpid(), **{k: kwargs.get(k) for k in "
        "('batch_size', 'batch_readahead', 'fragment_readahead')}}) + '\\n')\n"
        "    return _scan(self, **kwargs)\n"
        "lance.LanceDataset.scanner = _tracked\n")
    monkeypatch.setenv('PYTHONPATH', str(injected) + os.pathsep + os.environ['PYTHONPATH'])
    class Tracked:
        def __init__(self, ds): self.ds, self.schema = ds, ds.schema
        def __getattr__(self, name): return getattr(self.ds, name)
        def scanner(self, **kwargs): return self.ds.scanner(**kwargs)
    monkeypatch.setattr(read, 'open_lance_dataset', lambda *a: Tracked(real(*a)))
    rows = DataAPI().read_lance(uri, version=1, batch_size=1, batch_readahead=1,
                               fragment_readahead=1).take_all()
    assert rows == [{'i': i} for i in range(7)]
    calls = [json.loads(line) for line in probe.read_text().splitlines()]
    assert len(calls) == 1
    assert calls[0]["pid"] != os.getpid()
    assert {k: calls[0][k] for k in ('batch_size', 'batch_readahead', 'fragment_readahead')} == {
        'batch_size': 1, 'batch_readahead': 1, 'fragment_readahead': 1}


@pytest.mark.parametrize('name', ['batch_size', 'batch_readahead', 'fragment_readahead'])
@pytest.mark.parametrize('value', [0, -1, True, 1.5])
def test_invalid_scan_bounds_fail_before_io(name, value):
    with pytest.raises(InvalidLanceRequest):
        LanceScanSpec('/tmp/unused.lance', **{name: value})
