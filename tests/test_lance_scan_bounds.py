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
    calls = []
    class Tracked:
        def __init__(self, ds): self.ds, self.schema = ds, ds.schema
        def scanner(self, **kwargs):
            calls.append(kwargs)
            return self.ds.scanner(**kwargs)
    monkeypatch.setattr(read, 'open_lance_dataset', lambda *a: Tracked(real(*a)))
    rows = DataAPI().read_lance(uri, version=1, batch_size=1, batch_readahead=1,
                               fragment_readahead=1).take_all()
    assert rows == [{'i': i} for i in range(7)]
    assert len(calls) == 1
    assert {k: calls[0][k] for k in ('batch_size', 'batch_readahead', 'fragment_readahead')} == {
        'batch_size': 1, 'batch_readahead': 1, 'fragment_readahead': 1}


@pytest.mark.parametrize('name', ['batch_size', 'batch_readahead', 'fragment_readahead'])
@pytest.mark.parametrize('value', [0, -1, True, 1.5])
def test_invalid_scan_bounds_fail_before_io(name, value):
    with pytest.raises(InvalidLanceRequest):
        LanceScanSpec('/tmp/unused.lance', **{name: value})
