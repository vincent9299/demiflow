"""Presentation cannot alter data identity or grant arbitrary resource reads."""
import hashlib
import json
from pathlib import Path
import sqlite3

import pyarrow as pa
import pytest

from demiflow.workbench import catalog as table_view
from demiflow.workbench.projects import workspace as ws
from demiflow.workbench import project_operations as project_ops
from demiflow.forge.platform import content_digest
from demiflow.workbench.worker import operation
from test_forge_workspace import demo, isolated, attach


def test_presentation_changes_keep_fixed_binding_and_inspection(isolated):
    root = isolated
    before = project_ops.describe_table(root, 'candidates')
    report = project_ops.inspect_flow(root, 'flows/questions.py:select', params='flows/select_a.json')
    result = project_ops.set_table_view(root, 'candidates', expected=before['digest'],
        display={'title': 'Questions', 'fields': [{'path': ['question'], 'label': '正文', 'kind': 'text'}]},
        provenance={'kind': 'historical', 'note': 'Existing fixed input, no new run evidence'})
    assert result['digest'] != before['digest']
    after = project_ops.describe_table(root, 'candidates', expected_binding=before['binding']['digest'])
    assert after['binding'] == before['binding']
    assert project_ops.report_status(root, report['digest'])['status'] == 'checked'
    assert project_ops.sample_table(root, 'candidates')['columns'] == ['question']
    with pytest.raises(ValueError, match='changed'):
        project_ops.set_table_view(root, 'candidates', expected=before['digest'], display={}, provenance={})
    with pytest.raises(ValueError, match='not certify'):
        project_ops.set_table_view(root, 'candidates', expected=result['digest'], display={}, provenance={'kind': 'managed'})


def test_nested_images_use_declared_location_and_fixed_hash(isolated, tmp_path):
    from PIL import Image
    root = isolated
    objects = tmp_path / 'objects'
    objects.mkdir()
    image = objects / 'encoded'
    Image.new('RGB', (8, 4), 'red').save(image, format='PNG')
    sha = hashlib.sha256(image.read_bytes()).hexdigest()
    ws.add_location(root, 'objects', objects)
    schema = pa.schema([('items', pa.list_(pa.struct([('uri', pa.string()), ('sha', pa.string())])))])
    attached = attach(root, 'nested', [{'items': [{'uri': image.as_uri(), 'sha': sha}]}], schema)
    field = dict(path=['items', '*', 'uri'], sha256_path=['items', '*', 'sha'], kind='image',
                 location_id='objects', max_bytes=1024)
    project_ops.set_table_view(root, 'nested', expected=attached['digest'], display={'fields': [field]}, provenance={})
    row = project_ops.sample_table(root, 'nested')['rows'][0]
    args = dict(alias='nested', row_id=int(row['_rowid']), field_path=['items', 0, 'uri'])
    shown = operation('media', root, **args)
    assert shown['media_identity'] == 'sha256:' + sha and shown['width'] == 8
    with pytest.raises(ValueError, match='not uniquely declared'):
        operation('media', root, **{**args, 'field_path': ['items', 0, 'sha']})
    image.write_bytes(b'changed')
    with pytest.raises(ValueError, match='SHA256'):
        operation('media', root, **args)
    image.unlink()
    image.symlink_to(root / 'README.md')
    with pytest.raises(ValueError, match='symbolic|Symlink|Too many levels'):
        operation('media', root, **args)


def test_historical_evidence_reads_only_stored_json_and_scrubs_credentials(isolated, tmp_path):
    root = isolated
    evidence = tmp_path / 'evidence'
    evidence.mkdir()
    ledger = evidence / 'calls.sqlite'
    payload = {'headers': {'Authorization': 'fixture-secret', 'Cookie': 'fixture-cookie'},
               'url': 'https://fixture:password@example.invalid/api?api_key=fixture-key&model=fixture',
               'image': {'_demiflow_offline_input': {'path': '/must-not-read', 'sha256': '0' * 64, 'header': 'data:image/png;base64'}},
               'answer': 'kept'}
    with sqlite3.connect(ledger) as db:
        db.execute('CREATE TABLE calls(request_key TEXT PRIMARY KEY, request_json TEXT, response_json TEXT, error_json TEXT)')
        db.execute('INSERT INTO calls VALUES(?,?,NULL,NULL)', ('fixed-request', json.dumps(payload)))
    ws.add_location(root, 'evidence', evidence)
    reference = dict(journal_path=str(ledger), request_id='fixed-request', kind='request')
    schema = pa.schema([('calls', pa.list_(pa.struct([(key, pa.string()) for key in reference])))])
    attached = attach(root, 'history', [{'calls': [reference]}], schema)
    field = dict(path=['calls', '*'], kind='evidence', location_id='evidence', max_bytes=1024**2)
    project_ops.set_table_view(root, 'history', expected=attached['digest'], display={'fields': [field]},
                      provenance={'kind': 'historical', 'source': 'Original ledger'})
    row = project_ops.sample_table(root, 'history')['rows'][0]
    args = dict(alias='history', row_id=int(row['_rowid']), field_path=['calls', 0])
    value = operation('table_evidence', root, **args)
    assert value['document']['answer'] == 'kept'
    assert value['document']['headers']['Authorization'] == '[redacted]'
    assert 'fixture-key' not in value['document']['url'] and 'password' not in value['document']['url']
    assert value['document']['image']['_demiflow_offline_input']['path'] == '/must-not-read'
    with pytest.raises(ValueError, match='not uniquely declared'):
        operation('table_evidence', root, **{**args, 'field_path': ['calls', 0, 'journal_path']})


def test_resource_declarations_reject_ambiguous_or_unknown_access():
    field = dict(path=['items', '*', 'uri'], sha256_path=['items', '*', 'sha'], kind='image',
                 location_id='objects', max_bytes=1024)
    for changes in [{'location_id': 'unknown'}, {'sha256_path': ['sha']}, {'max_bytes': 0},
                    {'relocations': ['/old', '/old/nested']}, {'handler': 'arbitrary.code'}]:
        with pytest.raises(ValueError):
            table_view.validate({'fields': [{**field, **changes}]}, {}, {'objects': {}})
    with pytest.raises(ValueError):
        table_view.field_path(['items', True])
    with pytest.raises(ValueError, match='Plain display'):
        table_view.validate({'fields': [dict(path=['x'], kind='text', location_id='objects')]}, {}, {'objects': {}})


def test_nested_projection_and_matching_are_literal_schema_paths(isolated):
    schema = pa.schema([('detail', pa.struct([('answer', pa.string()), ('payload', pa.string())]))])
    attach(isolated, 'nested_projection', [{'detail': {'answer': 'yes', 'payload': 'unused'}},
                                          {'detail': {'answer': 'no', 'payload': 'other'}}], schema)
    result = project_ops.sample_table(isolated, 'nested_projection', columns=['detail.answer'], match={'field': 'detail.answer', 'value': 'yes'})
    assert len(result['rows']) == 1 and result['rows'][0]['detail.answer'] == 'yes'
    assert 'detail.payload' not in result['rows'][0]
    for column in ['detail.missing', 'detail.answer + 1', '1; DROP TABLE detail']:
        with pytest.raises(ValueError, match='column'):
            project_ops.sample_table(isolated, 'nested_projection', columns=[column])
    before = project_ops.describe_table(isolated, 'nested_projection')
    with pytest.raises(ValueError, match='schema'):
        project_ops.set_table_view(isolated, 'nested_projection', expected=before['digest'],
            display={'fields': [{'path': ['absent'], 'kind': 'text'}]}, provenance={})


def test_table_versions_list_registered_facts_and_preview_without_moving_binding(isolated):
    import lance
    from demiflow.lance.refs import DatasetRef
    from demiflow.lance.registry import Catalog
    root = isolated
    schema = pa.schema([('question', pa.string())])
    uri = str(root / 'data' / 'vt.lance')
    lance.write_dataset(pa.Table.from_pylist([{'question': 'a'}], schema=schema), uri)
    lance.write_dataset(pa.Table.from_pylist([{'question': 'a'}, {'question': 'b'}], schema=schema), uri, mode='overwrite')
    catalog = Catalog(root / 'data')
    for version in (1, 2):
        meta = operation('describe', uri, version=version)
        catalog.register(DatasetRef('vt', 'vt.lance', version, 'vt', '1', meta['schema_hash'], meta['row_count'], store_id='local'))
    project_ops.attach_table(root, 'vt', relative_uri='vt.lance', version=2, dataset_id='vt', schema_name='vt', schema_version='1')
    binding = project_ops.describe_table(root, 'vt')['binding']['digest']
    listed = project_ops.table_versions(root, 'vt', expected_binding=binding)
    assert listed['status'] == 'versions' and listed['truncated'] is False and listed['total_registered'] == 2
    assert [item['lance_version'] for item in listed['versions']] == [1, 2]
    assert listed['versions'][0]['row_count'] == 1 and listed['versions'][1]['row_count'] == 2
    assert listed['binding']['digest'] == binding
    selected = project_ops.table_versions(root, 'vt', expected_binding=binding, version=1)
    assert selected['selected']['version'] == 1 and selected['selected']['metadata']['row_count'] == 1
    assert 'question' in selected['selected']['metadata']['fields']
    # Previewing another version never moves the fixed binding or the bound sample.
    assert project_ops.describe_table(root, 'vt', expected_binding=binding)['binding']['digest'] == binding
    assert project_ops.describe_table(root, 'vt')['metadata']['version'] == 2
    with pytest.raises(ValueError, match='not a registered version'):
        project_ops.table_versions(root, 'vt', version=3)
    with pytest.raises(ValueError, match='binding changed'):
        project_ops.table_versions(root, 'vt', expected_binding='0' * 64)
    # An attached-but-unregistered table is honestly empty, never a guessed history.
    unregistered = project_ops.table_versions(root, 'candidates')
    assert unregistered['versions'] == [] and unregistered['total_registered'] == 0
    assert unregistered['reason'] == 'no_registered_versions'


def test_table_versions_run_product_is_a_sealed_single_version(isolated, tmp_path, monkeypatch):
    import time
    from demiflow.workbench.runs import manager as runs
    monkeypatch.setenv('DEMIFLOW_FORGE_DOMAIN', str(tmp_path / 'g1-runtime-domain'))
    root = isolated
    prepared = runs.prepare(root, 'flows/questions.py:select', params='flows/select_a.json')
    runs.start(root, prepared['run_id'], expected=prepared['digest'])
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        run = runs.show(root, prepared['run_id'])
        if run['status'] in runs.TERMINAL:
            break
        time.sleep(0.1)
    assert run['status'] == 'succeeded', run
    listed = project_ops.table_versions(root, 'run:' + run['run_id'] + ':target')
    assert listed['versions'] == [] and listed['total_registered'] == 0 and listed['truncated'] is False
    assert listed['reason'] == 'run_output_sealed_version'
    assert listed['binding']['reference']['ref']['lance_version'] >= 1
