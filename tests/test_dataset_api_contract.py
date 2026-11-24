"""Public export and Dataset action contracts."""

import pyarrow as pa
import pytest

import demiflow
from demiflow import data, pipeline
from demiflow.data import dataset as dataset_module


ROOT_EXPORTS = {
    'Pipeline', 'PipelineProgram', 'ResourceAPI', 'StreamCheckpoint',
    'AdmissionQuotaReader', 'data', 'Dataset', 'MaterializedDataset',
    'Datasource', 'Datasink', 'ReadTask', 'BlockMetadata', 'WriteResult',
    'AggregateFnV2', 'AbsMax', 'Count', 'Max', 'Mean', 'Min', 'Std', 'Sum',
    'PipelineBackend', 'PipelineBundleRef', 'PipelineExecutionEnvironment',
    'PipelineRunObservation', 'PipelineRunReadiness', 'PipelineRunRequest',
    'PipelineRunResult', 'PipelineExecutionTarget', 'PlatformRuntimeIdentity',
}
DATA_EXPORTS = {
    'read_queue', 'read_queue_records', 'read_legacy_queue_records', 'add_lance_columns',
    'ensure_lance_vector_index', 'local_execution', 'from_items', 'from_iter',
    'read_records', 'range', 'from_arrow', 'from_numpy', 'from_pandas',
    'read_parquet', 'read_json', 'read_csv', 'read_text', 'read_binary_files',
    'read_images', 'read_sql', 'read_datasource', 'read_lance',
    'read_document_receipts', 'vector_search_lance', 'Dataset',
    'MaterializedDataset', 'BlockMetadata', 'Datasource', 'ReadTask', 'Datasink',
    'WriteResult', 'AbsMax', 'AggregateFnV2', 'Count', 'Max', 'Mean', 'Min',
    'Std', 'Sum', 'require', 'schema', 'open_checkpoint', 'read_checkpoint',
    'FitRef', 'ModelSelection', 'TrainingRecipe', 'PredictorSpec',
}
PIPELINE_EXPORTS = {
    'PIPELINE_BACKENDS', 'PIPELINE_DEFAULT_DATASET_WORKERS',
    'PIPELINE_DEFAULT_TIMEOUT_SECONDS', 'PIPELINE_EXECUTION_FORMAT',
    'PIPELINE_PACKAGE_PATH', 'Pipeline', 'PipelineContractDiagnostic',
    'PipelineContractError', 'PipelineDefinition', 'PipelineInspection',
    'ProgramDeclaration', 'PipelineExecution', 'PipelineProgram', 'ResourceAPI',
    'discover_pipeline_definition', 'inspect_pipeline_definition', 'load_program',
    'parse_pipeline_execution', 'validate_pipeline_execution',
}


@pytest.mark.parametrize('module, expected', [
    (demiflow, ROOT_EXPORTS),
    (data, DATA_EXPORTS),
    (pipeline, PIPELINE_EXPORTS),
])
def test_public_export_surface_is_explicit(module, expected):
    assert len(module.__all__) == len(set(module.__all__))
    assert set(module.__all__) == expected


def test_public_reexports_share_identity_and_internal_helpers_stay_private():
    assert demiflow.data is data
    assert demiflow.Dataset is data.Dataset
    assert demiflow.MaterializedDataset is data.MaterializedDataset
    assert demiflow.Pipeline is pipeline.Pipeline
    assert demiflow.PipelineProgram is pipeline.PipelineProgram
    assert demiflow.ResourceAPI is pipeline.ResourceAPI
    for module in (demiflow, data):
        assert not hasattr(module, 'DataAPI')
        assert not hasattr(module, 'normalize_bound_inputs')
        assert not hasattr(module, 'normalize_outputs')
    assert callable(dataset_module.normalize_bound_inputs)
    assert callable(dataset_module.normalize_outputs)


def counted_source(rows, seen):
    def factory():
        for row in rows:
            seen.append(row)
            yield row
    return data.from_iter(factory)


@pytest.mark.parametrize('limit', [-1, -10])
def test_take_rejects_negative_limit_before_reading(limit):
    seen = []
    ds = counted_source([{'x': 1}], seen)
    with pytest.raises(ValueError, match='take limit must be non-negative'):
        ds.take(limit)
    assert seen == []


def test_take_and_take_all_preserve_bounded_collection_semantics():
    rows = [{'x': i} for i in range(3)]
    assert data.from_items(rows).take(0) == []
    assert data.from_items(rows).take(2) == rows[:2]
    assert data.from_items(rows).take_all(limit=3) == rows
    assert data.from_items([]).take_all(limit=0) == []
    with pytest.raises(ValueError, match='more than the take_all limit'):
        data.from_items(rows).take_all(limit=2)
    with pytest.raises(ValueError, match='take_all limit must be non-negative'):
        data.from_items(rows).take_all(limit=-1)


def test_take_all_limit_reads_at_most_one_extra_row(tmp_path):
    from _worker_event_fixtures import emit, events
    seen = tmp_path / 'source-reads.jsonl'
    def rows():
        for i in range(5):
            emit(seen, 'read', x=i)
            yield {'x': i}
    ds = data.from_iter(rows)
    with pytest.raises(ValueError, match='more than the take_all limit'):
        ds.take_all(limit=2)
    assert [event['x'] for event in events(seen)] == [0, 1, 2]


@pytest.mark.parametrize('options', [
    {'batch_size': 0}, {'batch_size': -1}, {'batch_format': 'not-a-format'},
])
def test_iter_batches_rejects_invalid_options_before_reading(options):
    seen = []
    ds = counted_source([{'x': 1}], seen)
    with pytest.raises(ValueError):
        ds.iter_batches(**options)
    assert seen == []


def test_iter_batches_rejects_unknown_keyword_before_reading():
    seen = []
    ds = counted_source([{'x': 1}], seen)
    with pytest.raises(TypeError, match='batch_szie'):
        ds.iter_batches(batch_szie=1)
    assert seen == []


def test_iter_batches_explicit_arrow_and_numpy_formats_and_drop_last():
    rows = [{'x': i} for i in range(5)]
    arrow = list(data.from_items(rows).iter_batches(batch_size=2, batch_format='pyarrow'))
    assert [batch.num_rows for batch in arrow] == [2, 2, 1]
    assert all(isinstance(batch, pa.Table) for batch in arrow)
    dropped = list(data.from_items(rows).iter_batches(
        batch_size=2, batch_format='pyarrow', drop_last=True,
    ))
    assert [batch.num_rows for batch in dropped] == [2, 2]
    numpy = list(data.from_items(rows).iter_batches(batch_size=2, batch_format='numpy'))
    assert [len(batch['x']) for batch in numpy] == [2, 2, 1]
    assert [int(value) for batch in numpy for value in batch['x']] == list(range(5))


def test_iter_batches_explicit_pandas_format():
    pd = pytest.importorskip('pandas')
    batches = list(data.from_items([{'x': i} for i in range(3)]).iter_batches(
        batch_size=2, batch_format='pandas',
    ))
    assert all(isinstance(batch, pd.DataFrame) for batch in batches)
    assert [len(batch) for batch in batches] == [2, 1]
    assert batches[0].to_dict('records') == [{'x': 0}, {'x': 1}]
