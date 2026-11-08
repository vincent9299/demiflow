"""Narrow component resource regression, separate from real public proofs."""
import os
import pytest
from demiflow.workbench.analysis import output_reader


def test_embedded_fd_missing_python_api_closes_owned_scratch_on_error(tmp_path,monkeypatch):
    monkeypatch.delattr(os,'memfd_create',raising=False)
    # Unit-only path map; real public tests use the admitted JobContext.
    def owned(path):
        assert path=='/scratch'
        return str(tmp_path)
    monkeypatch.setattr(output_reader,'job_path',owned)
    descriptor=None
    with pytest.raises(RuntimeError,match='owned parser rejection'):
        with output_reader._embedded_fd() as descriptor:
            os.write(descriptor,b'fixed bytes')
            assert os.pread(descriptor,11,0)==b'fixed bytes'
            raise RuntimeError('owned parser rejection')
    with pytest.raises(OSError):os.fstat(descriptor)
    assert list(tmp_path.iterdir())==[]
