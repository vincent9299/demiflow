"""The sealed worker can count with a custom local encoding, fully offline."""
import pytest

from demiflow.workbench.runs import manager as runs
from demiflow.workbench.projects import workspace as ws
from test_forge_runs import base, root, custom, execute, rows
from test_map_prompt_async import server
from test_operator_foundation_models import model_files
from test_local_token_resource import declaration


def test_sealed_local_token_resource_and_model_execution(root, server):
    model_files(root)
    budget = declaration(root)
    text = (root / 'flows/model.py').read_text().replace('max_requests=2, options=', 'max_requests=2, token_budget=' + repr(budget) + ', options=')
    text = text.replace('data.read_lance(source, version=1)',
        "data.read_lance(source['uri'], version=source['version'], columns=['case_id']).limit(2).rename_columns({'case_id': 'item'}).materialize()")
    text = text.replace('write_lance(target,', "write_lance(target['uri'],")
    # A dependency outside the bound environment cannot silently participate.
    with pytest.raises(ValueError, match='dependencies.*snapshot'):
        custom(root, text.removeprefix('from demiflow import data\n'))
    _, workspace, _ = ws.load_workspace(root)
    ws.bind_platform(root, expected=workspace['platform']['digest'], extras=['llm', 'tokens'])
    prepared = custom(root, text.removeprefix('from demiflow import data\n'))
    directory = runs.directory(root, prepared['run_id'])
    sealed = directory / 'source/flows/bytes.bpe'
    assert sealed.read_bytes() == (root / 'flows/bytes.bpe').read_bytes()
    original = sealed.read_bytes()
    sealed.write_bytes(b'changed')
    with pytest.raises(ValueError, match='Sealed resource'):
        runs.verify_seal(directory, runs._load(root, prepared['run_id'])[1]['manifest'])
    sealed.write_bytes(original)
    (root / 'flows/bytes.bpe').write_bytes(b'changed')
    with pytest.raises(ValueError, match='stale|changed'):
        runs.start(root, prepared['run_id'], expected=prepared['digest'])
    (root / 'flows/bytes.bpe').write_bytes(original)
    result = execute(root, prepared)
    assert result['status'] == 'succeeded', result
    assert len(server['requests']) == len(rows(root, result)) == 2
    assert all(row['answer'] == 'ok' for row in rows(root, result))
