"""Static validation, DF-02 hygiene, and workspace.json registration tests for workflows/*.json."""
from __future__ import annotations

import json
import pytest

from demiflow.forge.pipeline_check import check_project
from demiflow.forge.project import initialize_project, load_project, register_flow, register_workflow, slim_project_manifest
from demiflow.forge.workflow import (
    WORKFLOW_FORMAT,
    discover_workflows,
    parse_from_ref,
    resolve_node_overrides,
    validate_workflow,
    validate_workflow_inputs,
)
from demiflow.workbench.conversations.context import _run_project_layout


def _make_project(root, name='demo_proj'):
    initialize_project(root, name=name)
    (root / 'flows').mkdir(exist_ok=True)
    (root / 'configs').mkdir(exist_ok=True)
    (root / 'data').mkdir(exist_ok=True)
    (root / 'flows' / 'step_a.py').write_text(
        'from demiflow import data\n'
        'def run(*, output_table, scale=1):\n'
        '    return data.from_items([{"id": 1, "val": scale}]).write_lance(output_table["uri"], mode=output_table["mode"], return_receipt=True)\n',
        encoding='utf-8',
    )
    (root / 'configs' / 'step_a.json').write_text(
        json.dumps({'output_table': {'kind': 'output', 'uri': 'data/a.lance'}, 'scale': 1}),
        encoding='utf-8',
    )
    (root / 'flows' / 'step_b.py').write_text(
        'from demiflow import data\n'
        'def run(*, input_table, output_table, tag="v1"):\n'
        '    return data.read_lance(input_table["uri"], version=input_table.get("version")).write_lance(output_table["uri"], mode=output_table["mode"], return_receipt=True)\n',
        encoding='utf-8',
    )
    # Create an initial dummy input table so step_b passes static registration.
    from demiflow import data
    data.from_items([{'id': 1, 'val': 1}]).write_lance(str(root / 'data' / 'seed.lance'), mode='create')
    (root / 'configs' / 'step_b.json').write_text(
        json.dumps({
            'input_table': {'uri': 'data/seed.lance', 'version': 1},
            'output_table': {'kind': 'output', 'uri': 'data/b.lance'},
            'tag': 'v1',
        }),
        encoding='utf-8',
    )
    register_flow(root, 'flows/step_a.py:run', params='configs/step_a.json')
    register_flow(root, 'flows/step_b.py:run', params='configs/step_b.json')
    return root


def test_parse_from_ref_supports_tables_and_dotted_model_files():
    assert parse_from_ref('gen.outputs.result') == {
        'node': 'gen', 'kind': 'output', 'name': 'result', 'field': None, 'expr': 'gen.outputs.result',
    }
    assert parse_from_ref('gen.outputs.result.version') == {
        'node': 'gen', 'kind': 'output', 'name': 'result', 'field': 'version', 'expr': 'gen.outputs.result.version',
    }
    assert parse_from_ref('train.models.default.manifest_digest') == {
        'node': 'train', 'kind': 'model_attr', 'model_key': 'default', 'attr': 'manifest_digest',
        'expr': 'train.models.default.manifest_digest',
    }
    assert parse_from_ref('train.models.default.files.adapter_model.safetensors.uri') == {
        'node': 'train', 'kind': 'model_file', 'model_key': 'default', 'step': None,
        'file_key': 'adapter_model.safetensors', 'field': 'uri',
        'expr': 'train.models.default.files.adapter_model.safetensors.uri',
    }
    assert parse_from_ref('train.models.fit_0.steps.1000.files.lora/weights.safetensors.sha256') == {
        'node': 'train', 'kind': 'model_file', 'model_key': 'fit_0', 'step': 1000,
        'file_key': 'lora/weights.safetensors', 'field': 'sha256',
        'expr': 'train.models.fit_0.steps.1000.files.lora/weights.safetensors.sha256',
    }
    assert parse_from_ref('bad') is None
    assert parse_from_ref('gen.outputs.result.unknown') is None
    assert parse_from_ref('train.models.default.steps.0.files.w.uri') is None
    assert parse_from_ref('train.models.default.files.../secret.uri') is None


def test_valid_workflow_passes_validation_registration_and_hygiene(tmp_path):
    root = _make_project(tmp_path / 'wf_clean')
    (root / 'workflows').mkdir()
    wf_doc = {
        'format': WORKFLOW_FORMAT,
        'name': '两步串行编排',
        'description': 'A 产出表供 B 消费',
        'inputs': {
            'scale': {'type': 'integer', 'default': 5, 'description': '缩放因子'},
        },
        'concurrency_groups': {'gpu': 1},
        'policy': {'max_parallel': 2, 'on_failure': 'fail_fast'},
        'nodes': [
            {
                'id': 'a',
                'title': '生成基础表',
                'entry': 'flows/step_a.py:run',
                'params': 'configs/step_a.json',
                'overrides': {'scale': {'$input': 'scale'}},
            },
            {
                'id': 'b',
                'title': '下游处理',
                'entry': 'flows/step_b.py:run',
                'params': 'configs/step_b.json',
                'overrides': {'input_table': {'$from': 'a.outputs.output_table'}, 'tag': 'from_a'},
                'concurrency_group': 'gpu',
            },
        ],
    }
    (root / 'workflows' / 'pipe.json').write_text(json.dumps(wf_doc, ensure_ascii=False, indent=2), encoding='utf-8')

    spec = validate_workflow(root, 'workflows/pipe.json')
    assert spec['status'] == 'passed', spec['diagnostics']
    assert spec['topo_order'] == ['a', 'b']
    assert spec['cross_project'] is False
    assert len(spec['edges']) == 1 and spec['edges'][0]['kind'] == 'data'

    # Register via Forge API & verify slim workspace.json preserves workflows cleanly.
    reg = register_workflow(root, 'workflows/pipe.json')
    assert reg['status'] == 'registered'
    _, loaded = load_project(root)
    assert loaded['workflows'] == ['workflows/pipe.json']
    assert slim_project_manifest(root)['status'] == 'unchanged'

    # manage_project_layout supports register_workflow and protects registered workflows from remove_path.
    reg2 = _run_project_layout(root, {'action': 'register_workflow', 'path': 'workflows/pipe.json'})
    assert reg2['status'] == 'unchanged'
    with pytest.raises(Exception):
        _run_project_layout(root, {'action': 'remove_path', 'paths': ['workflows/pipe.json']})

    # Both path-scoped and full-project forge check pass with zero diagnostics.
    path_rep = check_project(root, paths=['workflows/pipe.json'])
    assert path_rep['status'] == 'passed', path_rep
    full_rep = check_project(root)
    assert full_rep['status'] == 'passed', full_rep

    # discover_workflows reports registration status.
    listed = discover_workflows(root)
    assert len(listed) == 1 and listed[0]['registered'] is True and listed[0]['status'] == 'passed'


def test_workflow_static_diagnostics_catch_cycle_paths_and_missing_refs(tmp_path):
    root = _make_project(tmp_path / 'wf_bad')
    (root / 'workflows').mkdir()

    # 1. Cycle (WF-02) + forbidden absolute path (WF-07) + undeclared input (WF-03) + unregistered entry (WF-04)
    bad_doc = {
        'format': WORKFLOW_FORMAT,
        'name': '错误编排',
        'projects': {
            'leak': '/etc/passwd',
            'rel': '../other',
        },
        'inputs': {},
        'nodes': [
            {
                'id': 'n1',
                'entry': 'flows/step_a.py:run',
                'params': 'configs/step_a.json',
                'overrides': {'scale': {'$input': 'missing_in'}, 'x': {'$from': 'n2.outputs.out'}},
            },
            {
                'id': 'n2',
                'entry': 'flows/missing.py:run',
                'params': 'configs/missing.json',
                'after': ['n1'],
            },
        ],
    }
    (root / 'workflows' / 'broken.json').write_text(json.dumps(bad_doc), encoding='utf-8')
    spec = validate_workflow(root, 'workflows/broken.json')
    assert spec['status'] == 'failed'
    rules = {d['rule'] for d in spec['diagnostics']}
    assert {'WF-02', 'WF-03', 'WF-04', 'WF-07'} <= rules

    # Full project check also surfaces workflow errors.
    rep = check_project(root)
    assert rep['status'] == 'failed'
    assert any(d['path'] == 'workflows/broken.json' for d in rep['diagnostics'])


def test_validate_inputs_and_resolve_overrides():
    spec = {
        'inputs': {
            'limit': {'type': 'integer', 'default': 10, 'required': False},
            'tag': {'type': 'string', 'required': True},
        }
    }
    with pytest.raises(Exception):
        validate_workflow_inputs(spec, {})
    vals = validate_workflow_inputs(spec, {'tag': 'exp1'})
    assert vals == {'limit': 10, 'tag': 'exp1'}

    node = {
        'overrides': {
            'input_table': {'$from': 'up.outputs.result'},
            'input_ver': {'$from': 'up.outputs.result.version'},
            'weights': {'$from': 'trn.models.default.steps.1000.files.lora.safetensors.uri'},
            'digest': {'$from': 'trn.models.default.manifest_digest'},
            'limit': {'$input': 'limit'},
            'tag': {'$input': 'tag'},
        }
    }
    artifacts = {
        'up': {'outputs': {'result': {'uri': '/p/data/r.lance', 'version': 3, 'row_count': 42, 'schema_hash': 'abc'}}},
        'trn': {
            'models': {
                'default': {
                    'manifest_digest': 'a' * 64,
                    'final_step': 1000,
                    'steps': {
                        '1000': {
                            'step': 1000,
                            'model_format': 'lora',
                            'files': {'lora.safetensors': {'uri': '/m/obj/aa', 'sha256': 'b' * 64, 'size_bytes': 128}},
                        }
                    },
                    'files': {'lora.safetensors': {'uri': '/m/obj/aa', 'sha256': 'b' * 64, 'size_bytes': 128}},
                }
            }
        },
    }
    resolved, records = resolve_node_overrides(node, inputs_values=vals, upstream_artifacts=artifacts)
    assert resolved == {
        'input_table': {'uri': '/p/data/r.lance', 'version': 3},
        'input_ver': 3,
        'weights': '/m/obj/aa',
        'digest': 'a' * 64,
        'limit': 10,
        'tag': 'exp1',
    }
    assert len(records) == 6
