"""Registered objects → native embedding HTTP → original Lance/ACK/plan bridge."""
import base64
import hashlib
import importlib.util
import io
import json
from pathlib import Path

import lance
import pyarrow as pa
import pytest
from PIL import Image

from demiflow import data
from demiflow.data.flow_contract import arrow_fields
from demiflow.embeddings.config import embedding_options
from demiflow.embeddings.object_input import object_read_binding, read_bound_object
from demiflow.execution.checked import CheckedExecution
from demiflow.execution.run_context import RunContext
from demiflow.forge.flow_check import FlowChecker
from demiflow.forge.platform import inspect_modules, LIMITS
from demiflow.objects import LocalObjectStore
from demiflow.workbench.projects import workspace as ws
from demiflow.workbench.runs.manager import DEFAULT_LIMITS
from runtime_v3_recovery_scenarios import _native_calls
from test_document_embeddings import fixture_input
from test_embeddings import server


@pytest.fixture(scope='module')
def catalog():
    return inspect_modules(['demiflow.data', 'demiflow.embeddings'],
                           Path(data.__file__).resolve().parents[1], LIMITS)


def test_default_options_keep_the_original_identity_bytes():
    encoded = json.dumps(embedding_options(), sort_keys=True, separators=(',', ':'), allow_nan=False)
    assert hashlib.sha256(encoded.encode()).hexdigest() == 'be997e0da4b9cdd1904c640a203d620862372f70770722273921dc56f37c86c4'
    assert 'object_read' not in embedding_options()


@pytest.mark.parametrize('fault', ['outside', 'symlink', 'sha', 'oversize', 'network'])
def test_bound_reads_reject_before_embedding_transport(tmp_path, fault):
    root = tmp_path / 'objects'
    root.mkdir()
    target = root / 'inside'
    target.write_bytes(b'bounded')
    ref = {'uri': target.as_uri(), 'sha256': hashlib.sha256(b'bounded').hexdigest()}
    binding = object_read_binding({'root': str(root)})
    assert read_bound_object(ref, binding=binding, max_bytes=7) == b'bounded'
    limit = 7
    if fault == 'outside':
        outside = tmp_path / 'outside'; outside.write_bytes(b'bounded')
        ref['uri'] = outside.as_uri()
    elif fault == 'symlink':
        link = root / 'link'; link.symlink_to(target)
        ref['uri'] = link.as_uri()
    elif fault == 'sha':
        ref['sha256'] = '0' * 64
    elif fault == 'network':
        ref['uri'] = 'https://example.invalid/object'
    else:
        limit = 6
    with pytest.raises((ValueError, OSError)):
        read_bound_object(ref, binding=binding, max_bytes=limit)


def test_explicit_relocation_uses_only_the_bound_destination(tmp_path):
    root = tmp_path / 'new'; root.mkdir()
    (root / 'item').write_bytes(b'data')
    old = tmp_path / 'old'
    ref = {'uri': (old / 'item').as_uri(), 'sha256': hashlib.sha256(b'data').hexdigest()}
    binding = object_read_binding({'root': str(root), 'relocations': [str(old)]})
    assert read_bound_object(ref, binding=binding, max_bytes=4) == b'data'
    with pytest.raises(ValueError):
        object_read_binding({'root': str(root), 'relocations': [str(root / 'ambiguous')]})
    with pytest.raises(ValueError):
        embedding_options({'object_read': {'root': str(root)}, 'max_image_bytes': 129 * 1024**2})


def _nonnullable(typ):
    if pa.types.is_struct(typ):
        return pa.struct([pa.field(f.name, _nonnullable(f.type), nullable=False) for f in typ])
    if pa.types.is_list(typ):
        return pa.list_(pa.field('item', _nonnullable(typ.value_type), nullable=False))
    return typ


def _fixture(root, catalog, endpoint, modality, *, registration=True, binding=True):
    ws.init_workspace(root, modules=('demiflow.data', 'demiflow.embeddings'))
    objects = root / 'objects'; objects.mkdir()
    if registration:
        ws.add_location(root, 'objects', objects)
    if modality == 'document':
        value, expected = fixture_input(root)
    else:
        raw = io.BytesIO()
        Image.new('RGB', (3, 2), (42, 0, 0)).save(raw, format='PNG')
        value = LocalObjectStore(objects).put(raw.getvalue()).to_dict()
        expected = (42, 0, 0)
    table = pa.Table.from_pylist([{'id': 1, 'input': value}])
    table = table.cast(pa.schema([pa.field(f.name, _nonnullable(f.type), nullable=False) for f in table.schema]))
    source, target = root / 'input.lance', root / 'output.lance'
    lance.write_dataset(table, str(source))
    options = dict(max_image_bytes=65536, max_decode_pixels=1024,
        max_document_bytes=65536, max_document_text_bytes=4096, document_cache_bytes=65536,
        max_request_bytes=65536, max_response_bytes=65536, timeout_s=5)
    if binding:
        options['object_read'] = {'root': str(objects)}
    path = root / 'flows' / 'object_embedding.py'
    path.parent.mkdir(exist_ok=True)
    path.write_text('from demiflow import data\nfrom demiflow.embeddings import EmbeddingModel\n'
        'def run(*, source, target, endpoint):\n'
        f'    model = EmbeddingModel("fixture", "fixed-revision", 3, endpoint, input_format={"chat" if modality == "image" else "text"!r})\n'
        f'    rows = data.read_lance(source, version=1).map_embeddings(model=model, inputs={ {modality: "input"}!r}, '
        f'max_requests=1, batch_size=1, options={options!r})\n'
        '    return rows.write_lance(target, mode="create", return_receipt=True)\n')
    args = dict(source=str(source), target=str(target), endpoint=endpoint)
    report = FlowChecker(root, catalog,
        {'input': {'uri': str(source), 'version': 1, 'fields': arrow_fields(table.schema)}},
        [str(target)], resource_resolver=lambda tree, refs, bindings:
        ws.resolve_flow_resources(root, tree, refs, bindings)).inspect('flows/object_embedding.py:run', args)
    return path, args, report, expected


@pytest.mark.parametrize('modality', ['image', 'document'])
def test_registered_objects_reach_checked_native_model_and_original_ack(tmp_path, catalog, server, modality):
    path, args, report, expected = _fixture(tmp_path, catalog, server['model'].base_url, modality)
    assert report['status'] == 'checked', report['diagnostics']
    node = next(n for n in report['nodes'] if n['kind'] == 'embeddings')
    object_fact, = [r for r in node['analysis']['resources'] if r['kind'] == 'objects']
    assert object_fact['location_id'] == 'objects' and object_fact['root'] == str(tmp_path / 'objects')
    assert object_fact in report['resources'].values()
    spec = importlib.util.spec_from_file_location('flows.object_embedding', path)
    module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
    observations = []
    checked = CheckedExecution(**{k: report[k] for k in ('nodes', 'edges', 'actions', 'operators')},
        source_root=tmp_path, limits=DEFAULT_LIMITS, report_digest=report['digest'],
        record=lambda *a: None, observe=lambda *a, **kw: observations.append((a, kw)), check_stop=lambda: None)
    with RunContext.start(tmp_path / 'run') as run, data.local_execution(workers=4), checked.bind_context():
        receipt = module.run(**args)
        commits = run.journal.execution_commits(report['actions'][0])
        assert commits and commits[-1]['status'] == 'acked'
        calls = _native_calls(run.journal)
        assert calls['confirmed'] == calls['requests_consumed'] == 1 and not calls['errors']
        bridge_ref = run.journal.source_binding(report['actions'][0] + '-identity-bridge')
        assert bridge_ref
        bridge_path = run.directory / bridge_ref['path']
    rows = lance.dataset(receipt.uri).to_table().to_pylist()
    assert len(rows) == 1 and rows[0]['id'] == 1
    assert rows[0]['embedding'] == pytest.approx([.6, .8, 0.])
    assert len(server['bodies']) == 1 and observations
    if modality == 'image':
        uri = server['bodies'][0]['messages'][0][0]['content'][0]['image_url']['url']
        with Image.open(io.BytesIO(base64.b64decode(uri.split(',')[1]))) as image:
            assert image.getpixel((0, 0)) == expected
    else:
        assert server['bodies'][0]['input'] == [expected]
    # The original sealed bridge must connect every observed report occurrence.
    # Check the native artifact itself: no reconstructed display-only identity.
    raw = bridge_path.read_bytes()
    assert hashlib.sha256(raw).hexdigest() == bridge_ref['sha256']
    bridge = json.loads(raw)
    assert bridge['report_digest'] == report['digest'] and bridge['entries']
    report_ids = {node['id'] for node in report['nodes']}
    assert all(entry['report_node_id'] in report_ids for entry in bridge['entries'])


@pytest.mark.parametrize('modality', ['image', 'document'])
@pytest.mark.parametrize('fault', ['unregistered', 'undeclared'])
def test_managed_object_access_is_rejected_before_http(tmp_path, catalog, server, modality, fault):
    _, args, report, _ = _fixture(tmp_path, catalog, server['model'].base_url, modality,
        registration=fault != 'unregistered', binding=fault != 'undeclared')
    assert report['status'] != 'checked'
    assert not server['bodies'] and not Path(args['target']).exists()
