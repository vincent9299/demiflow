import hashlib
import json

import pytest

from demiflow.workbench.projects import workspace as ws
from demiflow.forge.platform import content_digest
from test_forge_runs import base, root, custom, execute, rows
from test_collect_flow_contract import document, picture


@pytest.mark.parametrize('family', ['document', 'image', 'wiki'])
def test_sealed_collect_nodes_and_external_object_binding(root, family):
    _, workspace, _ = ws.load_workspace(root)
    ws.bind_platform(root, expected=workspace['platform']['digest'], extras=['images', 'wikitext'])
    _, workspace, _ = ws.load_workspace(root)
    ws.set_budgets(root, {'memory_bytes': 1024**3, 'cancel_grace_s': 20}, expected=content_digest(workspace))
    if family == 'wiki':
        expression = "data.from_items([{'text': '[[A|label]] {{lang|en|word}}'}]).parse_wikitext(column='text', output='syntax', max_chars=4096, max_bytes=65536)"
        columns = ['text']
    else:
        raw = picture() if family == 'image' else json.dumps(document()).encode()
        obj = root / 'data/object'
        obj.write_bytes(raw)
        ref = {'uri': obj.as_uri(), 'sha256': hashlib.sha256(raw).hexdigest()}
        expression = 'data.from_items(' + repr([{'ref': ref}]) + ").read_object(reference='ref', output='raw', root=" + repr(str(root / 'data')) + ', max_bytes=65536)'
        if family == 'image':
            expression += ".encode_image(column='raw', output='image', profile='oriented_white_bicubic_v1', max_side=17, max_bytes=65536, memory_bytes=268435456, timeout_s=5)"
            columns = ['image']
        else:
            expression += ".decode_document(column='raw', output='document', max_bytes=65536)"
            columns = ['raw']
    body = "def run(*, source, target):\n    result = " + expression + ".materialize()\n    return result.select_columns(" + repr(columns) + ").write_lance(target['uri'], mode='create', return_receipt=True)\n"
    prepared = custom(root, body)
    result = execute(root, prepared)
    assert result['status'] == 'succeeded', result
    assert result['outputs'][0]['receipt']['written_rows'] == 1
    if family == 'image':
        assert rows(root, result)[0]['image']['data_url'].startswith('data:image/jpeg;base64,')
    elif family == 'wiki':
        assert rows(root, result)[0]['text'] == '[[A|label]] {{lang|en|word}}'
