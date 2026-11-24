"""Tests for Forge project-owned .env / .env.example specification and workbench launcher."""
from pathlib import Path

import pytest

from demiflow.forge.project import initialize_project, load_project
from demiflow.forge.project_env import (
    MAX_DOTENV_BYTES,
    declared_prompt_env_names,
    ensure_gitignore_env,
    load_project_env,
    parse_dotenv,
)
from demiflow.forge.project_rules import check_project_hygiene
from demiflow.forge.schema import ForgeError
from demiflow.workbench.conversations.context import _run_project_layout
from demiflow.workbench.runs import manager as runs


def _make_prompt_project(root: Path, *, api_key_env: str = 'DEMO_LLM_API_KEY', base_url_env: str = '') -> Path:
    initialize_project(root)
    (root / 'flows').mkdir(exist_ok=True)
    (root / 'prompts').mkdir(exist_ok=True)
    (root / 'configs').mkdir(exist_ok=True)
    (root / 'flows/demo.py').write_text(
        'from demiflow import data\n'
        'def run():\n'
        '    return data.range(1).count()\n',
        encoding='utf-8',
    )
    model_lines = [
        '  name: demo-model',
        '  transport: openai_compatible',
        f'  api_key_env: {api_key_env}',
    ]
    if base_url_env:
        model_lines.append(f'  base_url_env: {base_url_env}')
    else:
        model_lines.append('  base_url: https://example.invalid/v1')
    (root / 'prompts/demo.yaml').write_text(
        'schema_version: demiflow_prompt_v1\n'
        'model:\n' + '\n'.join(model_lines) + '\n'
        'messages:\n'
        '  - role: user\n'
        '    content: hello\n',
        encoding='utf-8',
    )
    (root / 'configs/demo.json').write_text('{}\n', encoding='utf-8')
    from demiflow.forge.project import register_flow
    register_flow(root, 'flows/demo.py:run', params='configs/demo.json')
    return root


def test_parse_dotenv_standard_forms():
    text = (
        '# comment\n'
        'PLAIN=abc123\n'
        'export EXPORTED="quoted value # kept"\n'
        "SINGLE='single quoted'\n"
        'INLINE=val123 # trailing comment\n'
        'EMPTY=\n'
        'invalid-key=skip\n'
        'NO_EQUALS_LINE\n'
    )
    parsed = parse_dotenv(text)
    assert parsed == {
        'PLAIN': 'abc123',
        'EXPORTED': 'quoted value # kept',
        'SINGLE': 'single quoted',
        'INLINE': 'val123',
    }
    parsed_with_empty = parse_dotenv(text, allow_empty=True)
    assert parsed_with_empty['EMPTY'] == ''
    assert 'invalid-key' not in parsed_with_empty


def test_load_project_env_guards(tmp_path):
    assert load_project_env(tmp_path) == {}
    (tmp_path / '.env').write_text('API_KEY=secret_val\n', encoding='utf-8')
    assert load_project_env(tmp_path) == {'API_KEY': 'secret_val'}

    # Oversized file rejected
    big_dir = tmp_path / 'big'
    big_dir.mkdir()
    (big_dir / '.env').write_bytes(b'A=' + b'x' * (MAX_DOTENV_BYTES + 10))
    with pytest.raises(ForgeError):
        load_project_env(big_dir)

    # Symlink rejected
    link_dir = tmp_path / 'link'
    link_dir.mkdir()
    secret_target = tmp_path / 'outside.env'
    secret_target.write_text('API_KEY=leaked\n', encoding='utf-8')
    (link_dir / '.env').symlink_to(secret_target)
    with pytest.raises(ForgeError, match='regular file'):
        load_project_env(link_dir)


def test_initialize_project_gitignore_only_on_new_project(tmp_path):
    fresh = tmp_path / 'fresh'
    res = initialize_project(fresh)
    assert '.gitignore' in res['created_paths']
    gi = (fresh / '.gitignore').read_text(encoding='utf-8')
    assert '.env' in gi and '.env.local' in gi

    # existing=True must not touch business files when registering an existing project
    existing_dir = tmp_path / 'existing'
    existing_dir.mkdir()
    (existing_dir / 'README.md').write_text('hello\n', encoding='utf-8')
    initialize_project(existing_dir, existing=True)
    assert not (existing_dir / '.gitignore').exists()


def test_hygiene_checks_env_example_and_secrets(tmp_path):
    root = _make_prompt_project(tmp_path / 'proj', api_key_env='CONCEPT_EVAL_API_KEY', base_url_env='CONCEPT_EVAL_BASE_URL')
    _, control = load_project(root)
    assert declared_prompt_env_names(root) == {'CONCEPT_EVAL_API_KEY', 'CONCEPT_EVAL_BASE_URL'}

    # 1. Missing .env.example flagged under DF-02
    _, diags = check_project_hygiene(root, control, checked_sources={'flows/demo.py'}, parameter_digests={})
    assert any(d['rule'] == 'DF-02' and d['path'] == '.env.example' and 'CONCEPT_EVAL_API_KEY' in d['detail'] for d in diags)

    # 2. Incomplete .env.example flags missing key
    (root / '.env.example').write_text('CONCEPT_EVAL_API_KEY=\n', encoding='utf-8')
    _, diags = check_project_hygiene(root, control, checked_sources={'flows/demo.py'}, parameter_digests={})
    assert any(d['rule'] == 'DF-02' and d['path'] == '.env.example' and 'CONCEPT_EVAL_BASE_URL' in d['detail'] for d in diags)

    # 3. Real secret / non-placeholder value in .env.example rejected
    (root / '.env.example').write_text(
        'CONCEPT_EVAL_API_KEY=sk-proj-abcdefghijklmnopqrstuvwxyz1234\n'
        'CONCEPT_EVAL_BASE_URL=\n',
        encoding='utf-8',
    )
    _, diags = check_project_hygiene(root, control, checked_sources={'flows/demo.py'}, parameter_digests={})
    assert any(d['rule'] == 'DF-02' and d['path'] == '.env.example' and '不应填写实际密钥值' in d['detail'] for d in diags)

    # 4. Clean empty-value .env.example passes even when local .env does NOT exist
    (root / '.env.example').write_text(
        '# template\nCONCEPT_EVAL_API_KEY=\nCONCEPT_EVAL_BASE_URL=\n',
        encoding='utf-8',
    )
    assert not (root / '.env').exists()
    _, diags = check_project_hygiene(root, control, checked_sources={'flows/demo.py'}, parameter_digests={})
    assert not any(d['path'] == '.env.example' for d in diags)

    # 5. Hardcoded secret in configs/*.json rejected
    (root / 'configs/demo.json').write_text(
        '{"api_key": "sk-proj-abcdefghijklmnopqrstuvwxyz1234"}\n',
        encoding='utf-8',
    )
    _, diags = check_project_hygiene(root, control, checked_sources={'flows/demo.py'}, parameter_digests={})
    assert any(d['rule'] == 'DF-02' and d['path'] == 'configs/demo.json' and '硬编码明文密钥' in d['detail'] for d in diags)


def test_manage_project_layout_sync_env_example(tmp_path):
    root = _make_prompt_project(tmp_path / 'proj', api_key_env='CONCEPT_EVAL_API_KEY')
    _, control = load_project(root)
    # Remove .gitignore to confirm sync_env_example ensures it too
    (root / '.gitignore').unlink()
    # Pre-populate .env.example with an accidental value to verify sync clears values safely
    (root / '.env.example').write_text('LEGACY_EXTRA=should-be-cleared\n', encoding='utf-8')

    result = _run_project_layout(root, {'action': 'sync_env_example', 'variables': ['CUSTOM_ENDPOINT_URL']})
    assert result['status'] == 'synced'
    assert result['path'] == '.env.example'
    assert result['variables'] == ['CONCEPT_EVAL_API_KEY', 'CUSTOM_ENDPOINT_URL', 'LEGACY_EXTRA']
    assert result['gitignore_updated'] is True

    content = (root / '.env.example').read_text(encoding='utf-8')
    assert 'CONCEPT_EVAL_API_KEY=\n' in content
    assert 'CUSTOM_ENDPOINT_URL=\n' in content
    assert 'LEGACY_EXTRA=\n' in content
    assert 'should-be-cleared' not in content
    assert '.env' in (root / '.gitignore').read_text(encoding='utf-8')

    # Passes hygiene cleanly (no .env.example or secret diagnostics)
    _, diags = check_project_hygiene(root, control, checked_sources={'flows/demo.py'}, parameter_digests={})
    assert not any(d['path'] == '.env.example' or '硬编码明文密钥' in d['detail'] for d in diags)


def test_worker_environment_loads_project_dotenv_with_whitelist_and_precedence(tmp_path, monkeypatch):
    root = tmp_path / 'proj'
    run_target = root / '.forge/runs/r1234567890abcdef'
    run_target.mkdir(parents=True)
    (root / '.env').write_text(
        'CONCEPT_EVAL_API_KEY=from-project-dotenv\n'
        'UNDECLARED_SECRET=must-not-leak\n',
        encoding='utf-8',
    )
    monkeypatch.setenv('CONCEPT_EVAL_API_KEY', 'from-host-environ')
    monkeypatch.setenv('FALLBACK_HOST_KEY', 'host-only-value')

    # Explicit project_root: project .env overrides host os.environ; host fallback works; undeclared keys excluded
    env = runs.worker_environment(
        tmp_path / 'detached_target',
        credential_names=['CONCEPT_EVAL_API_KEY', 'FALLBACK_HOST_KEY'],
        project_root=root,
    )
    assert env['CONCEPT_EVAL_API_KEY'] == 'from-project-dotenv'
    assert env['FALLBACK_HOST_KEY'] == 'host-only-value'
    assert 'UNDECLARED_SECRET' not in env

    # Inferred project_root from <root>/.forge/runs/<run_id> when project_root omitted
    env_inferred = runs.worker_environment(run_target, credential_names=['CONCEPT_EVAL_API_KEY'])
    assert env_inferred['CONCEPT_EVAL_API_KEY'] == 'from-project-dotenv'
    assert 'UNDECLARED_SECRET' not in env_inferred

    # Missing credential error points user to project .env and .env.example
    with pytest.raises(ForgeError) as exc_info:
        runs.worker_environment(run_target, credential_names=['MISSING_CRED_KEY'])
    msg = str(exc_info.value)
    assert 'Declared credential is unavailable or exceeds budget: MISSING_CRED_KEY' in msg
    assert '.env' in msg and '.env.example' in msg


def test_ensure_gitignore_env_idempotent(tmp_path):
    assert ensure_gitignore_env(tmp_path) is True
    assert ensure_gitignore_env(tmp_path) is False
    gi = (tmp_path / '.gitignore').read_text(encoding='utf-8')
    assert gi.splitlines().count('.env') == 1
    assert gi.splitlines().count('.env.local') == 1
