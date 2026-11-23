import os

import pytest

from demiflow.execution.contracts.dataflow_errors import DataflowError
from demiflow.execution import datafusion as host


# The calling relation owns table and column semantics. This exercises the
# actual supervised native process with a full region and stable occurrence order.
SQL = """
    SELECT a.off AS lo, b.off AS mid, c.off AS ro FROM akeys a
    INNER JOIN bkeys b ON a.k = b.k LEFT JOIN ckeys c ON b.k = c.k
    ORDER BY a.ord, b.ord, c.ord LIMIT 8
"""
TABLES = {'akeys': [(b'x', 9), (b'x', 7), (b'y', 11), (None, 1)],
          'bkeys': [(b'x', 102), (b'y', 101), (b'x', 105), (None, 4)],
          'ckeys': [(b'x', 56), (None, 98)]}
RSS = 256 * 1024**2


def query(*, sql=SQL, tables=TABLES, maximum=7, rss=RSS, check_stop=lambda: None):
    return host.query_region_offsets(sql, tables=tables, max_pairs=maximum,
                                     rss_bytes=rss, check_stop=check_stop)


def test_region_child_uses_one_grant_and_preserves_typed_nulls_and_duplicates(monkeypatch):
    original = host._start_sql_worker
    launched = []
    def start(*args):
        process = original(*args)
        launched.append(process)
        return process
    def no_admission(*args):
        pytest.fail('A worker already owns this allocation')
    monkeypatch.setattr(host, '_start_sql_worker', start)
    monkeypatch.setattr(host.DataFusionSession, '_admit', no_admission)
    assert query() == [(9, 102, 56), (9, 105, 56), (7, 102, 56), (7, 105, 56), (11, 101, None)]
    assert len(launched) == 1 and launched[0].returncode == 0
    assert query(maximum=4) is None
    assert query(tables={**TABLES, 'akeys': []}) == []


def test_region_refuses_oversized_input_without_launch(monkeypatch):
    monkeypatch.setattr(host, '_start_sql_worker', lambda *args: pytest.fail('worker launched'))
    with pytest.raises(DataflowError) as error:
        query(tables={'akeys': [(b'x' * 8192, 0)]}, rss=64*1024)
    assert error.value.code == 'BUDGET_EXCEEDED'


def test_region_cancel_fences_work_and_reaps_the_owned_process(monkeypatch):
    original = host._start_sql_worker
    processes = []
    def start(*args):
        process = original(*args)
        processes.append(process)
        return process
    monkeypatch.setattr(host, '_start_sql_worker', start)
    def cancel_when_launched():
        if processes:
            raise DataflowError('CANCELLED', 'cancelled by worker')
    with pytest.raises(DataflowError) as error:
        query(check_stop=cancel_when_launched)
    assert error.value.code == 'CANCELLED'
    assert processes[0].returncode is not None
    with pytest.raises(ProcessLookupError):
        os.kill(processes[0].pid, 0)


def test_region_native_rss_and_query_errors_are_not_empty_matches():
    with pytest.raises(DataflowError) as error:
        query(rss=16*1024**2)
    assert error.value.code == 'BUDGET_EXCEEDED'
    with pytest.raises(DataflowError) as error:
        query(sql='SELECT * FROM unbound_table')
    assert error.value.code == 'INTERNAL_PROTOCOL'


def test_region_reports_native_pool_exhaustion_within_the_granted_rss():
    table = [(b'x', offset) for offset in range(900)]
    sql = ('SELECT count(*) FROM (SELECT a.off, b.off FROM akeys a CROSS JOIN bkeys b '
           'GROUP BY a.off, b.off)')
    with pytest.raises(DataflowError) as error:
        query(sql=sql, tables={'akeys': table, 'bkeys': table}, maximum=1024)
    assert error.value.code == 'BUDGET_EXCEEDED'
