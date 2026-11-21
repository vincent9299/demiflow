"""Native object/media facts and byte-preserving behavior, without business imports."""
from demiflow.forge.project import initialize_project
import hashlib
import io
import json
from pathlib import Path

import pytest
from PIL import Image, ImageOps

from demiflow import data
from demiflow.collect.documents import SCHEMA_VERSION
from demiflow.collect.image_resize import resize_image
from demiflow.collect.wiki_syntax import parse_wikitext
from demiflow.execution.checked import CheckedExecution
from demiflow.execution.local_kernel import local_execution
from demiflow.forge.flow_check import FlowChecker
from demiflow.forge.platform import inspect_modules, LIMITS
from demiflow.workbench.pipeline_policy import DEFAULT_LIMITS
from demiflow.forge.project_bindings import resolve_flow_resources
from demiflow.workbench.projects import workspace as ws
from test_operator_foundation_models import load


@pytest.fixture(scope='module')
def base(tmp_path_factory):
    path = tmp_path_factory.mktemp('collect-contract') / 'workspace'
    initialize_project(path)
    for directory in ('flows', 'operators', 'tables', 'data'):
        (path / directory).mkdir()
    control = ws.read_control(path / 'workspace.json')
    expected = ws.content_digest(control)
    control['locations']['local'] = {'root': 'data', 'writable': True}
    ws.write_control(path / 'workspace.json', control, expected=expected)
    return path


@pytest.fixture
def root(base, tmp_path):
    import shutil
    path = tmp_path / 'workspace'
    shutil.copytree(base, path)
    return path


def picture(mode='RGBA', orientation=6):
    image = Image.new(mode, (37, 25), (50, 100, 150, 80) if mode == 'RGBA' else 80)
    exif = Image.Exif()
    exif[274] = orientation
    output = io.BytesIO()
    image.save(output, format='PNG', exif=exif)
    return output.getvalue()


@pytest.mark.parametrize('mode,orientation', [('RGBA', 1), ('RGBA', 6), ('L', 8), ('P', 3)])
def test_explicit_encoding_profile_preserves_exact_pixel_order(mode, orientation):
    raw = picture(mode, orientation)
    with Image.open(io.BytesIO(raw)) as source:
        source.load()
        oriented = ImageOps.exif_transpose(source)
        pic = oriented.convert('RGBA')
        background = Image.new('RGBA', pic.size, 'white')
        background.alpha_composite(pic)
        pic = background.convert('RGB')
        pic.thumbnail((17, 17))
        expected = io.BytesIO()
        pic.save(expected, format='JPEG', quality=88)
    actual = resize_image(raw, max_side=17, quality=88, profile='oriented_white_bicubic_v1')
    assert actual == expected.getvalue()


def document():
    return {'schema_version': SCHEMA_VERSION,
        'source': {k: 'value' for k in ('url', 'final_url', 'title', 'retrieved_at', 'content_type')},
        'parser': {'version': 'fixture'}, 'raw_ref': {'uri': 'file:///unused', 'sha256': '0' * 64},
        'blocks': [{'block_id': 'b0', 'position': 0, 'kind': 'paragraph', 'text': 'A {{lang|en|word}}', 'headings': []}]}


def inspected(root, family):
    raw = picture() if family == 'image' else json.dumps(document()).encode()
    path = root / 'data/object'
    path.write_bytes(raw)
    ref = {'uri': path.as_uri(), 'sha256': hashlib.sha256(raw).hexdigest()}
    if family == 'wiki':
        body = "data.from_items([{'text': 'A {{lang|en|word}} [[File:X|ignored]]'}]).parse_wikitext(column='text', output='syntax', max_chars=4096, max_bytes=65536)"
    else:
        body = 'data.from_items(' + repr([{'ref': ref}]) + ").read_object(reference='ref', output='raw', root=" + repr(str(root / 'data')) + ', max_bytes=65536)'
        if family == 'document':
            body += ".decode_document(column='raw', output='document', max_bytes=65536)"
        elif family == 'image':
            body += ".encode_image(column='raw', output='image', profile='oriented_white_bicubic_v1', max_bytes=65536, max_side=17, memory_bytes=268435456, timeout_s=5)"
    (root / 'flows/collect.py').write_text('from demiflow import data\ndef run():\n    return ' + body + '.materialize()\n')
    catalog = inspect_modules(['demiflow.data'], Path(data.__file__).resolve().parents[1], LIMITS)
    report = FlowChecker(root, catalog, {}, [], resource_resolver=lambda tree, refs, bindings:
        resolve_flow_resources(root, tree, refs, bindings)).inspect('flows/collect.py:run', {})
    return report, raw


@pytest.mark.parametrize('family', ['object', 'document', 'image', 'wiki'])
def test_checked_native_collect_calls_use_real_actors(root, monkeypatch, family):
    report, raw = inspected(root, family)
    assert report['status'] == 'checked', report['diagnostics']
    flow = load(monkeypatch, 'flows.collect', root / 'flows/collect.py')
    context = CheckedExecution(nodes=report['nodes'], edges=report['edges'], actions=report['actions'],
        operators=report['operators'], source_root=root,
        limits={**DEFAULT_LIMITS, 'memory_bytes': 1024**3}, record=lambda *a: None, check_stop=lambda: None)
    from demiflow.execution import RunContext
    # The implicit native context has a separate 256 MiB default. Bind the
    # same explicit finite budget as CheckedExecution, including image residency.
    with RunContext.start(root / 'native-run', limits={'memory_bytes': 1024**3}):
        with local_execution(workers=1, worker_mode='thread', partitions=1, memory_bytes=1024**3):
            with context.bind_context():
                result = flow.run()
            row, = result.take_all()
    if family == 'object':
        assert row['raw'] == raw
    elif family == 'document':
        assert row['document']['blocks'] == document()['blocks']
    elif family == 'image':
        assert row['image']['sha256'] == hashlib.sha256(resize_image(raw, max_side=17, profile='oriented_white_bicubic_v1')).hexdigest()
    else:
        assert {n['kind'] for n in row['syntax']['nodes']} >= {'Text', 'Template', 'Wikilink'}


def test_object_locations_must_be_registered_and_cannot_drift(root):
    report, _ = inspected(root, 'object')
    assert report['status'] == 'checked'
    source = root / 'flows/collect.py'
    source.write_text(source.read_text().replace(repr(str(root / 'data')), repr(str(root))))
    catalog = inspect_modules(['demiflow.data'], Path(data.__file__).resolve().parents[1], LIMITS)
    report = FlowChecker(root, catalog, {}, [], resource_resolver=lambda tree, refs, bindings:
        resolve_flow_resources(root, tree, refs, bindings)).inspect('flows/collect.py:run', {})
    assert report['status'] != 'checked'
    assert any('registered' in d['detail'] for d in report['diagnostics'])


def test_object_reader_relocation_error_retention_and_null_skip(tmp_path):
    from demiflow.objects import ReadLocalObject
    raw = b'document'
    digest = hashlib.sha256(raw).hexdigest()
    path = tmp_path / digest[:2] / digest
    path.parent.mkdir()
    path.write_bytes(raw)
    actor = ReadLocalObject(reference='ref', output='bytes', root=str(tmp_path), relocations=['/old-objects'],
        layout='sha256_prefix2', max_bytes=32, error_output='error', concurrency=1, queue_depth=None, label='read')
    ref = {'uri': 'file:///old-objects/' + digest[:2] + '/' + digest, 'sha256': digest}
    assert actor({'ref': ref})['bytes'] == raw
    assert actor({'ref': None}) == {'ref': None, 'bytes': None, 'error': None}
    wrong = actor({'ref': {**ref, 'uri': 'file:///old-objects/wrong'}})
    assert wrong['bytes'] is None and 'layout' in wrong['error']
    path.write_bytes(b'changed')
    assert 'SHA256' in actor({'ref': ref})['error']


def test_wiki_syntax_is_bounded_and_contains_no_template_policy():
    text = '{{infobox|x=visible}} [[File:X|text]] <ref>note</ref> &amp;'
    value = parse_wikitext(text)
    nodes = value['nodes']
    assert any(n['name'] == 'infobox' for n in nodes)
    assert any(n['name'] == 'File:X' for n in nodes)
    assert any(n['name'] == 'ref' for n in nodes)
    assert any(n['text'] == '&' for n in nodes)
    assert all(c > n['id'] for n in nodes for c in n['children'] + n['title'] + n['label'])
    for limits in ({'max_chars': 2}, {'max_nodes': 1}, {'max_depth': 1}, {'max_bytes': 1024}):
        with pytest.raises((ValueError, TypeError)):
            parse_wikitext('{{a|{{b|{{c|deep}}}}}}', **limits)


def test_image_fixed_residency_rejects_smaller_native_budget(root, monkeypatch):
    from demiflow.execution import RunContext, RecoveryContractError
    from demiflow.execution.run_journal import RunJournal

    report, _ = inspected(root, 'image')
    assert report['status'] == 'checked', report['diagnostics']
    flow = load(monkeypatch, 'flows.collect', root / 'flows/collect.py')
    directory = root / 'low-budget-run'
    with pytest.raises(RecoveryContractError, match='Stream fixed residency exceeds'):
        with RunContext.start(directory, limits={'memory_bytes': 128 * 1024**2}):
            with local_execution(workers=1, worker_mode='thread', partitions=1,
                                 memory_bytes=128 * 1024**2):
                flow.run()
    assert RunJournal(directory).execution_commits('action-1') == []
