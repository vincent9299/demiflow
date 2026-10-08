import csv
import gzip
from pathlib import Path

import lance
import pyarrow as pa
import pyarrow.csv as pacsv
import pytest
from demiflow import data
from tests.test_local_relational import _oracle_join, assert_same_rows


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
    with data.local_execution(workers=1, partitions=2, temp_directory=str(tmp_path)):
        csv_rows = data.read_csv(str(path),
            parse_options=pacsv.ParseOptions(newlines_in_values=True),
            convert_options=pacsv.ConvertOptions(column_types={'id': pa.string(), 'description': pa.string()}))
        assert csv_rows.take_all() == right
        result = data.read_lance(str(lp), version=1).join(csv_rows, on='id', how=how)
        assert_same_rows(result.take_all(), _oracle_join(left, right, ['id'], ['id'], how))
        stages = result.execution_metadata().diagnostics['stages']
        assert not any(s['name'] == 'python_boundary' for s in stages)
        plans = [(Path(s['query']) / 'plan.txt').read_text() for s in stages if s['name'] == 'datafusion']
        assert any('file_type=csv' in p for p in plans)


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
        with pytest.raises(Exception):
            data.read_lance(str(lp), version=1).join(right, on='id').take_all()
