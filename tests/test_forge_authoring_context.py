"""Authoring evidence has a single, product-independent source."""
from hashlib import sha256
from importlib.resources import files
import json

import pytest

from demiflow.forge.api_reference import build_reference
from demiflow.forge.context import build_context, render_context
from demiflow.forge.schema import ForgeError
from demiflow.forge.bridge.agent_context import assemble
from demiflow.workbench.conversations.policy import Limits


@pytest.fixture(scope='module')
def reference():
    return build_reference()


def test_selected_topic_and_real_sdk_without_project(tmp_path, reference):
    (tmp_path / 'PIPELINE_SPEC.md').write_text('retired parent marker')
    target = tmp_path / 'new'
    overview = build_context(target, reference=reference)
    assert overview['status'] == 'ready' and not target.exists()
    assert overview['project']['state'] == 'uninitialized'
    assert overview['registered_flows']['total'] == 0
    assert all(item['topic'] == 'core' for item in overview['guidance'])
    result = build_context(target, paths=['prompts/score.yaml'], api='demiflow.data:Dataset.filter',
                           reference=reference)
    assert result['status'] == 'ready'
    assert [i['topic'] for i in result['guidance']] == ['core', 'prompt']
    assert any(r['id'] == 'DF-05' for r in result['rules'])
    assert result['rules_id'] == overview['rules_id']
    info, = result['sdk']['apis']
    native = next(i for i in reference['catalog']['objects'] if i['id'] == info['id'])
    assert info['signature'] == native['signature']['text']
    assert info['native_semantics'] == native['native_semantics']
    text = render_context(result)
    assert result['sdk']['sdk_revision'] in text and result['rules_id'] in text
    assert info['signature'] in text and '正反例' in text and 'retired parent marker' not in text
    with pytest.raises(ForgeError, match='Unknown SDK symbol'):
        build_context(target, api='demiflow.data:absent', reference=reference)


def test_context_and_workbench_read_same_project_and_protect_paths(tmp_path, reference):
    (tmp_path/'operators').mkdir()
    (tmp_path/'operators/secret.py').write_text('hidden marker')
    (tmp_path/'operators/public.py').write_text('def even(row): return row["id"] % 2 == 0\n')
    (tmp_path/'flows').mkdir()
    (tmp_path/'flows/test.py').write_text('from operators.public import even\n'
                                         'def run(rows): return rows.filter(even).count()\n')
    (tmp_path/'workspace.json').write_text(json.dumps(dict(
        format='demiflow_forge_workspace_v3', name='project', id='w'+'a'*32,
        entries={'flows/test.py:run':['params.json']}, platform={'path':'.forge/platform/'+ 'b'*64 + '.json', 'digest':'b'*64},
        locations={}, audit={'reports':[], 'change_scope':None})))
    (tmp_path/'README.md').write_text('HIDDEN README')
    (tmp_path/'PIPELINE_SPEC.md').write_text('retired rules')
    protected = ['operators/secret.py', 'README.md']
    result = build_context(tmp_path, reference=reference, exclude=protected)
    assert result['registered_flows']['total'] == 1
    assert result['role_files']['operators'] == {'total':1, 'items':['operators/public.py']}
    scoped = build_context(tmp_path, paths=['operators/public.py'], reference=reference, exclude=protected)
    assert scoped['targets']['known_consumers'] == [
        {'path':'operators/public.py','entry':'flows/test.py:run','evidence':'direct_source_reference',
         'source_sha256':sha256((tmp_path/'flows/test.py').read_bytes()).hexdigest()}]
    assert scoped['targets']['parameter_paths'] == ['params.json']
    source = next(i for i in scoped['sources'] if i['path']=='operators/public.py')
    assert source['sha256'] == sha256((tmp_path/'operators/public.py').read_bytes()).hexdigest()
    grant = dict(root=str(tmp_path), kind='pipeline', phase='implementation', writes=[], watch=[], sources=[],
                 non_goals=[], authorization='test', protected=protected)
    evidence, prompt = assemble(grant, 'read', Limits(), coding=reference)
    skill = files('demiflow.forge').joinpath('skills', 'demiforge', 'SKILL.md').read_text()
    assert skill in prompt and 'retired rules' not in prompt
    assert 'secret.py' not in prompt and 'HIDDEN README' not in prompt
    assert evidence['coding_guidance']['rules_id'] == result['rules_id']
    with pytest.raises(ForgeError, match='protected'):
        build_context(tmp_path, sources=['operators/secret.py'], reference=reference, exclude=protected)


def test_restricted_context_uses_only_the_turns_dependency_read_scope(tmp_path, reference, monkeypatch):
    from click.testing import CliRunner
    from demiflow.cli import main
    from demiflow.forge.bridge.common import digest

    root = tmp_path / 'project'
    root.mkdir()
    dependencies = tmp_path / 'references'
    allowed, private = dependencies / 'approved_lib', dependencies / 'private_lib'
    for package in (allowed, private):
        package.mkdir(parents=True)
        (package / '__init__.py').write_text('VALUE = 1\n')
    (root / 'pyproject.toml').write_text('[tool.demiflow.packages]\n' + ''.join(
        f'{pkg.name} = {{source = "local_ref", path = {json.dumps(str(pkg))}}}\n' for pkg in (allowed, private)))
    grant = dict(root=str(root), kind='pipeline', phase='implementation', writes=[], watch=[], sources=[],
                 non_goals=[], authorization='fixture')

    def assembled_version(**scope):
        manifest, _ = assemble(grant, 'inspect', Limits(), coding=reference, execution_mode='restricted', **scope)
        skill = next(row for row in manifest['sources'] if row['source_id'] == 'forge-skill')
        return skill['version']

    no_grants = build_context(root, reference=reference, allowed_roots=())
    assert no_grants['project_dependencies']['packages'] == {}
    assert assembled_version(allowed_roots=()) == digest(no_grants)
    inherited = build_context(root, reference=reference)
    assert set(inherited['project_dependencies']['packages']) == {allowed.name, private.name}
    assert assembled_version() == digest(inherited)
    scope = dict(allowed_roots=(dependencies,), denied_paths=(private,))
    scoped = build_context(root, reference=reference, **scope)
    assert set(scoped['project_dependencies']['packages']) == {'approved_lib'}
    assert assembled_version(**scope) == digest(scoped)

    config = tmp_path / 'runtime.json'
    config.write_text(json.dumps(dict(root=str(root), execution_mode='restricted',
                                      allowed_read_roots=[str(dependencies)], denied_paths=[str(private)])))
    monkeypatch.setenv('DEMIFLOW_FORGE_RUNTIME_CONFIG', str(config))
    result = CliRunner().invoke(main, ['forge', 'context', '--root', str(root), '--json'])
    assert result.exit_code == 0, result.output
    facts = json.loads(result.output)
    assert set(facts['project_dependencies']['packages']) == {'approved_lib'}
    assert 'denied_paths' not in facts['environment']
    assert 'allowed_read_roots' not in facts['environment']
    config.write_text(json.dumps(dict(root=str(root), execution_mode='restricted')))
    assert build_context(root, reference=reference)['project_dependencies']['packages'] == {}
    config.write_text(json.dumps(dict(root=str(tmp_path / 'another-project'), execution_mode='restricted')))
    with pytest.raises(ForgeError, match='another project.*list_registered_projects'):
        build_context(root, reference=reference)


def test_skill_and_guide_are_distinct_installable_resources():
    forge = files('demiflow.forge')
    skill = forge.joinpath('skills', 'demiforge', 'SKILL.md').read_text()
    guide = forge.joinpath('standards', 'pipeline-authoring.md').read_text()
    review = forge.joinpath('standards', 'pipeline-review.md').read_text()
    for command in ('context', 'init', 'add', 'check'):
        assert 'demiflow forge ' + command in skill
    for symbol in ('Dataset.fit', 'TrainingRecipe', 'PredictorSpec'):
        assert f'--api demiflow.data:{symbol}' in skill
    assert 'DF-05' in guide and 'DF-05' not in skill
    assert '正反例' in guide
    for doc in (skill, guide, review):
        assert '受管模型' in doc and '已提交' in doc and 'workflows/*.json' in doc


def test_optional_and_required_source_failures_are_distinct(tmp_path, reference):
    result = build_context(tmp_path, sources=['missing.md'], reference=reference)
    assert result['status'] == 'incomplete' and result['diagnostics'][0]['path'] == 'missing.md'
    (tmp_path/'workspace.json').write_text('broken')
    with pytest.raises(ForgeError):
        build_context(tmp_path, reference=reference)


def test_context_mismatched_sdk_cache_fails_closed(tmp_path, reference):
    result = build_context(tmp_path, reference={**reference, 'catalog_id':'stale'})
    assert result['status']=='incomplete' and not result['sdk']['apis']
    assert result['diagnostics'][0]['code'] == 'conflict'


def test_progressive_disclosure_topics_and_flow_titles(tmp_path, reference):
    (tmp_path / 'flows').mkdir()
    (tmp_path / 'flows/delivery.py').write_text(
        '"""评测题与参考图联合交付：出题、复用、定稿。"""\n'
        'from demiflow.data import from_items\n'
        'def run():\n'
        '    """评测题与参考图联合交付。"""\n'
        '    return from_items([{"id": 1}]).count()\n',
        encoding='utf-8',
    )
    (tmp_path / 'parameters').mkdir()
    (tmp_path / 'parameters/delivery.json').write_text(json.dumps({
        'format': 'demiflow_forge_flow_parameters_v2', 'arguments': {},
    }))
    (tmp_path / 'workspace.json').write_text(json.dumps(dict(
        format='demiflow_forge_workspace_v3', name='demo', id='w' + 'c' * 32,
        entries={'flows/delivery.py:run': ['parameters/delivery.json']},
        platform={'path': '.forge/platform/' + 'd' * 64 + '.json', 'digest': 'd' * 64},
        locations={}, audit={'reports': [], 'change_scope': None},
    )))
    overview = build_context(tmp_path, reference=reference)
    flow_item, = overview['registered_flows']['items']
    assert flow_item['title'] == '评测题与参考图联合交付'
    assert '出题、复用、定稿' in flow_item['summary']
    overview_text = render_context(overview)
    assert '评测题与参考图联合交付 | flows/delivery.py:run' in overview_text
    assert '## 当前版本 DemiFlow 原生 API' not in overview_text
    assert '--topic <sdk|rules|files|flow|operator|prompt|workflow>' in overview_text

    sdk_ctx = build_context(tmp_path, topic='sdk', reference=reference)
    sdk_text = render_context(sdk_ctx)
    assert '## 当前版本 DemiFlow 原生 API' in sdk_text
    assert 'demiflow.execution' in sdk_ctx['sdk']['symbol_index']
    assert 'RunContext' in sdk_ctx['sdk']['symbol_index']['demiflow.execution']
    assert 'local_execution' in sdk_ctx['sdk']['symbol_index']['demiflow.data']
    assert 'Dataset.fit' in sdk_ctx['sdk']['symbol_index']['demiflow.data']
    assert 'LocalObjectStore' in sdk_ctx['sdk']['symbol_index']['demiflow.objects']
    assert 'demiflow.execution' in sdk_text and 'RunContext' in sdk_text

    exec_ctx = build_context(tmp_path, api='demiflow.execution:RunContext', reference=reference)
    assert exec_ctx['sdk']['apis'][0]['id'] == 'demiflow.execution:RunContext'
    local_exec_ctx = build_context(tmp_path, api='demiflow.data:local_execution', reference=reference)
    assert local_exec_ctx['sdk']['apis'][0]['id'] == 'demiflow.data:local_execution'

    rules_text = render_context(build_context(tmp_path, topic='rules', reference=reference))
    assert '## Forge 开发规范' in rules_text and 'DF-05' in rules_text

    files_text = render_context(build_context(tmp_path, topic='files', reference=reference))
    assert 'flows/delivery.py' in files_text and '## 项目角色文件' in files_text

    with pytest.raises(ForgeError, match='Unknown context topic'):
        build_context(tmp_path, topic='unknown_topic', reference=reference)


def test_sdk_topic_dynamically_discovers_flow_apis_and_expands_project_used_symbols(tmp_path, reference):
    # 1. Even in an empty project, --topic sdk surfaces live-discovered @flow_api summaries
    #    and own methods of every @flow_api class (without inherited MaterializedDataset duplicates).
    empty_proj = tmp_path / 'empty_proj'
    empty_proj.mkdir()
    empty_ctx = build_context(empty_proj, topic='sdk', reference=reference)
    data_syms = empty_ctx['sdk']['symbol_index']['demiflow.data']
    assert 'ModelSelection.predict' in data_syms
    assert 'FitRef.select' in data_syms
    assert not any(s.startswith('MaterializedDataset.') for s in data_syms)

    summaries = {row['id']: row for row in empty_ctx['sdk'].get('flow_api_summaries', ())}
    for expected in (
        'demiflow.data:Dataset.fit',
        'demiflow.data:TrainingRecipe',
        'demiflow.data:ModelSelection.predict',
        'demiflow.data:Dataset.map_embeddings',
        'demiflow.data:read_queue',
    ):
        assert expected in summaries
        assert summaries[expected]['summary']
    assert not any('MaterializedDataset.' in sid for sid in summaries)
    empty_text = render_context(empty_ctx)
    assert '其余实时发现的原生流程 API 速览' in empty_text
    assert 'demiflow.data:ModelSelection.predict' in empty_text
    # Table-of-contents summary must precede long full API bodies so head-N reads see all native flow APIs.
    assert empty_text.index('其余实时发现的原生流程 API 速览') < empty_text.index('## 当前版本 DemiFlow 原生 API')

    # 2. In a project referencing non-essential public APIs across domains, --topic sdk
    #    dynamically expands their live docs (while respecting exclude boundaries).
    proj = tmp_path / 'used_proj'
    (proj / 'flows').mkdir(parents=True)
    (proj / 'flows/train.py').write_text(
        'from demiflow import data\n'
        'def run(ds):\n'
        '    recipe = data.TrainingRecipe\n'
        '    pred = data.PredictorSpec\n'
        '    return ds.fit(recipe=recipe).select(steps=(1,)).predict(on=ds)\n',
        encoding='utf-8',
    )
    (proj / 'flows/embed.py').write_text(
        'from demiflow.data import read_queue\n'
        'def run():\n'
        '    return read_queue().map_embeddings()\n',
        encoding='utf-8',
    )
    (proj / 'flows/secret.py').write_text(
        'from demiflow.objects import LocalObjectStore\n',
        encoding='utf-8',
    )
    sdk_ctx = build_context(proj, topic='sdk', reference=reference, exclude=('flows/secret.py',))
    expanded = {row['id']: row for row in sdk_ctx['sdk']['apis']}
    for expected in (
        'demiflow.data:TrainingRecipe',
        'demiflow.data:PredictorSpec',
        'demiflow.data:Dataset.fit',
        'demiflow.data:FitRef.select',
        'demiflow.data:ModelSelection.predict',
        'demiflow.data:read_queue',
        'demiflow.data:Dataset.map_embeddings',
    ):
        assert expected in expanded
        assert len(expanded[expected]['documentation']) >= 40
        assert 'source_excerpt' not in expanded[expected]
    # Excluded file must not leak its referenced symbols into expanded APIs.
    assert 'demiflow.objects:LocalObjectStore' not in expanded
    # Expanded symbols are omitted from flow_api_summaries to avoid duplicate bloat.
    summary_ids = {row['id'] for row in sdk_ctx['sdk'].get('flow_api_summaries', ())}
    assert not (summary_ids & set(expanded))
    rendered = render_context(sdk_ctx)
    assert 'context.model_dir' in rendered  # Live updated TrainingRecipe docstring surfaces automatically
    assert 'demiflow.data:Dataset.map_embeddings' in rendered
