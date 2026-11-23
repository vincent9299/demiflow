import csv
import gzip
from pathlib import Path

import lance
import pyarrow as pa
import pyarrow.csv as pacsv
import pytest
from demiflow import data
from tests.test_local_relational import _oracle_join, assert_same_rows
from demiflow.execution.run_context import RunContext


@pytest.mark.slow
@pytest.mark.parametrize('how', ['inner', 'left', 'semi', 'anti'])
def test_gzip_csv_join_keeps_original_strings_and_multiplicity(tmp_path, how):
    left = [{'id': 'b', 'n': 1}, {'id': 'a', 'n': 2}, {'id': 'a', 'n': 3}, {'id': None, 'n': 4},
            {'id': '\n', 'n': 5}, {'id': '', 'n': 6}, {'id': 'missing', 'n': 7}]
    right = [{'id': 'a', 'description': 'quoted, text\nnext line'}, {'id': 'a', 'description': ''},
             {'id': 'b', 'description': '中文'}, {'id': '\n', 'description': 'backslash\\quote"'},
             {'id': '', 'description': 'empty key'}]
    path = tmp_path / 'records.csv.gz'
    with gzip.open(path, 'wt', encoding='utf-8-sig', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=['id', 'description'])
        writer.writeheader(); writer.writerows(right)
    lp = tmp_path / 'left.lance'
    lance.write_dataset(pa.Table.from_pylist(left), str(lp))
    with RunContext.start(tmp_path / 'run') as run, \
            data.local_execution(workers=1, partitions=2, temp_directory=str(tmp_path)):
        csv_rows = data.read_csv(str(path),
            parse_options=pacsv.ParseOptions(newlines_in_values=True),
            convert_options=pacsv.ConvertOptions(column_types={'id': pa.string(), 'description': pa.string()}))
        assert csv_rows.take_all() == right
        result = data.read_lance(str(lp), version=1).join(csv_rows, on='id', how=how)
        assert_same_rows(result.take_all(), _oracle_join(left, right, ['id'], ['id'], how))
        # The unified engine reads native CSV in its sealed source worker;
        # DataFusion no longer owns a second file-scan plan or query directory.
        from demiflow.execution.seal.plan import load_dataflow
        from demiflow.execution.storage.objects import RunArtifactStore
        from demiflow.planning.dataflow_policy import resolve_runtime_policy
        metadata = result.execution_metadata().diagnostics
        assert metadata['engine'] == 'dataflow_v3'
        assert not any(s['name'] == 'python_boundary' for s in metadata['stages'])
        action_id = metadata['action_id']
        facts = run.journal.source_binding(action_id + '-dataflow-sealed')
        _, bounds = resolve_runtime_policy({}, run.limits, provider='local')
        store = RunArtifactStore(root=run.directory / '.dataflow-store', journal=None,
            store_id=facts['logical_ref']['store_id'], bounds=bounds, readonly=True)
        try:
            graph, physical, _ = load_dataflow(run.directory, action_id, store=store, bounds=bounds)
        finally:
            store.close()
        assert graph.dag_id == metadata['dag_id'] == physical.dag_id
        assert physical.plan_id == metadata['plan_id']
        assert any(n.kind == 'source' and n.kernel.kind == 'file'
                   and n.kernel.configuration['format'] == 'csv' for n in graph.nodes)
        assert any(n.kind == 'relational' and n.kernel.kind == 'join' for n in graph.nodes)
        commits = run.journal.execution_commits(action_id)
        assert commits and all(c['status'] == 'acked' for c in commits)


def test_tsv_strict_width_and_literal_quotes(tmp_path):
    path = tmp_path / 'records.tsv.gz'
    with gzip.open(path, 'wt') as stream:
        stream.write('id\tname\na\t"quoted"\nb\t\n')
    lp = tmp_path / 'keys.lance'
    lance.write_dataset(pa.table({'id': ['a', 'b']}), str(lp))
    with data.local_execution(workers=1, partitions=2, temp_directory=str(tmp_path)):
        right = data.read_csv(str(path), parse_options=pacsv.ParseOptions(delimiter='\t', quote_char=False),
            convert_options=pacsv.ConvertOptions(column_types={'id': pa.string(), 'name': pa.string()}))
        rows = data.read_lance(str(lp), version=1).join(right, on='id').take_all()
        assert_same_rows(rows, [{'id': 'a', 'name': '"quoted"'}, {'id': 'b', 'name': ''}])
    with gzip.open(path, 'wt') as stream:
        stream.write('id\tname\na\textra\tbroken\n')
    # A failed action must also leave its run failed. Assert outside the scope
    # so the context does not mistake a swallowed parser error for success.
    with pytest.raises(Exception, match='Expected 2 columns'), \
            RunContext.start(tmp_path / 'invalid-run') as failed_run, \
            data.local_execution(workers=1, partitions=2, temp_directory=str(tmp_path)):
        right = data.read_csv(str(path), parse_options=pacsv.ParseOptions(delimiter='\t', quote_char=False),
            convert_options=pacsv.ConvertOptions(column_types={'id': pa.string(), 'name': pa.string()}))
        data.read_lance(str(lp), version=1).join(right, on='id').take_all()
    assert failed_run.journal.execution_action('action-1')['result'] is None
    from demiflow.execution.storage.state_artifacts import StateArtifacts
    assert not any(StateArtifacts(failed_run).read(c['prepared']['state']['dataflow-cut-v3'])['cut']['final']
                   for c in failed_run.journal.execution_commits('action-1'))
