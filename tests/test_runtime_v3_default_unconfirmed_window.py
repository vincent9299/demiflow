"""The default unconfirmed source window is a target; a declared window stays a hard bound.

Source owners share checkpoint_unconfirmed_bytes and reserve max_row_bytes before
reading each record. With the default window every share carries that reservation,
so an owner reads until its actual bytes pass its share: small rows are not read one
per checkpoint, and any row within max_row_bytes is read even when it is larger than
the share. A window the caller declared is still never exceeded.
"""
import pytest

from demiflow import data
from demiflow.execution.action_driver.dataflow import DataflowExecution
from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution.journal.run_journal import RunJournal
from demiflow.execution.pipeline.run_context import RunContext

MB = 1024 ** 2
WINDOW = 64 * MB  # the default checkpoint_unconfirmed_bytes, shared by the source readers
LIMITS = {'memory_bytes': 2048 * MB, 'max_rss_bytes': 8192 * MB, 'max_row_bytes': 24 * MB,
          'max_batch_bytes': 24 * MB, 'timeout_s': 120}


def _files(tmp_path, lines=50, long_line=None):
    paths = []
    for f in range(8):
        path = tmp_path / ('part-%d.txt' % f)
        text = ''.join('line %d of %d\n' % (i, f) for i in range(lines))
        if long_line and f == 3:
            # First record of its reader, read while all eight readers share the window.
            text = 'x' * long_line + '\n' + text
        path.write_text(text)
        paths.append(str(path))
    return paths


def _readers(physical):
    sources = {snapshot.source_id for snapshot in physical.sources}
    return sum(len(s.partitions) for s in physical.stages if sources & set(s.occurrences))


def _read(tmp_path, paths, limits, monkeypatch=None):
    plans = []
    if monkeypatch is not None:
        prepare = DataflowExecution._prepare_launch
        def capture(self, *args, **kwargs):
            plans.append(self._physical)
            return prepare(self, *args, **kwargs)
        monkeypatch.setattr(DataflowExecution, '_prepare_launch', capture)
    with RunContext.start(tmp_path / 'run', limits=limits) as run, data.local_execution(
            workers=8, worker_mode='thread', memory_bytes=limits['memory_bytes']):
        rows = data.read_text(paths).take_all()
    return rows, RunJournal(run.ref.directory).execution_commits('action-1'), plans


def test_a_legal_row_larger_than_its_share_of_the_default_window_is_read(tmp_path, monkeypatch):
    rows, _, plans = _read(tmp_path, _files(tmp_path, long_line=20 * MB), LIMITS, monkeypatch)
    # Precondition: each reader's share is smaller than this row, which is within max_row_bytes.
    assert WINDOW // _readers(plans[0]) < 20 * MB <= LIMITS['max_row_bytes']
    assert sorted(len(r['text']) for r in rows)[-1] == 20 * MB
    assert len(rows) == 8 * 50 + 1


def test_small_rows_are_not_read_one_per_checkpoint(tmp_path):
    rows, commits, _ = _read(tmp_path, _files(tmp_path), LIMITS)
    assert sorted(r['text'] for r in rows) == sorted('line %d of %d' % (i, f) for f in range(8) for i in range(50))
    # 400 rows; an owner held to one row per checkpoint needed about 50.
    assert len(commits) <= 4


def test_a_declared_window_still_refuses_a_row_it_cannot_hold(tmp_path):
    limits = {**LIMITS, 'checkpoint_unconfirmed_bytes': 8 * MB}
    with pytest.raises(DataflowError, match='source row exceeds checkpoint_unconfirmed_bytes'):
        _read(tmp_path, _files(tmp_path, long_line=2 * MB), limits)
