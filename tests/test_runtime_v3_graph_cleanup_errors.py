"""Graph final metadata cannot hide a real worker failure."""
import pytest

from test_u3_graph_local import _execute, _graph_spec, _stream_map


@pytest.mark.parametrize('business_failure', [False, True])
def test_graph_keeps_primary_failure_when_final_metadata_fails(tmp_path, monkeypatch, business_failure):
    from demiflow.execution import dataflow_api
    original = dataflow_api.record_execution_metadata
    observed = []

    def metadata(request, execution):
        original(request, execution)
        observed.append(True)
        raise RuntimeError('final-metadata-failed')

    def business(row):
        if business_failure:
            raise ValueError('business-operation-failed')
        return {**row, 'x': row['x'] * 2}

    spec, readers, count = _graph_spec(n=2)
    spec['ancestor']['plan'] = _stream_map(business)
    monkeypatch.setattr(dataflow_api, 'record_execution_metadata', metadata)
    # An unrelated caller exception must not be mistaken for this action's
    # primary failure, including when the graph itself completed successfully.
    try:
        raise LookupError('unrelated caller context')
    except LookupError:
        expected = ValueError if business_failure else RuntimeError
        message = 'business-operation-failed' if business_failure else 'final-metadata-failed'
        with pytest.raises(expected, match=message) as caught:
            _execute(tmp_path, spec, readers, count)
    assert observed == [True]
    if business_failure:
        assert any('final-metadata-failed' in note for note in caught.value.__notes__)
