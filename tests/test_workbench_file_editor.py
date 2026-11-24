"""Tests for workbench visible project dotfiles, lightweight file editing, and .env initialization."""
import json
import os
import stat
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import pytest

from demiflow.forge.project import initialize_project
from demiflow.forge.schema import ForgeError
from demiflow.workbench.projects import documents
from demiflow.workbench import api as workbench_api


def _setup_project(root):
    initialize_project(root)
    (root / 'flows').mkdir(exist_ok=True)
    (root / 'configs').mkdir(exist_ok=True)
    (root / 'data').mkdir(exist_ok=True)
    (root / 'tables').mkdir(exist_ok=True)
    (root / 'flows/demo.py').write_text('x = 1\n', encoding='utf-8')
    (root / 'configs/demo.json').write_text('{"a": 1}\n', encoding='utf-8')
    (root / 'data/notes.txt').write_text('readonly data\n', encoding='utf-8')
    (root / 'tables/items.json').write_text('{}\n', encoding='utf-8')
    (root / '.env.example').write_text('DEMO_API_KEY=\n', encoding='utf-8')
    (root / '.secret_other').write_text('hidden\n', encoding='utf-8')
    (root / '.git').mkdir(exist_ok=True)
    (root / '.git/config').write_text('[core]\n', encoding='utf-8')
    return root


def test_listing_and_search_show_project_dotfiles_but_hide_internal_dirs(tmp_path):
    root = _setup_project(tmp_path / 'proj')
    page = documents.listing(root, '', 0, 50, all_files=True)
    names = {row['name']: row['kind'] for row in page['entries']}
    assert names['.env.example'] == 'text'
    assert names['.gitignore'] == 'text'
    assert '.forge' not in names
    assert '.git' not in names
    assert '.secret_other' not in names

    # Markdown-only browser (all_files=False) keeps dotfiles hidden
    md_page = documents.listing(root, '', 0, 50, all_files=False)
    md_names = {row['name'] for row in md_page['entries']}
    assert '.env.example' not in md_names

    # Search finds .env.example and .gitignore, never .git/config or .secret_other
    env_hits = documents.search_files(root, 'env')['entries']
    assert any(row['path'] == '.env.example' and row['kind'] == 'text' for row in env_hits)
    assert not any(row['path'].startswith('.git') or row['name'] == '.secret_other' for row in env_hits)


def test_save_file_cas_and_protection_boundaries(tmp_path):
    root = _setup_project(tmp_path / 'proj')
    diff = documents.file_diff(root, 'flows/demo.py')
    assert diff['writable'] is True
    assert diff['readonly_reason'] is None

    # Save with matching sha256 succeeds
    saved = documents.save_file(root, 'flows/demo.py', 'x = 2\n', expected_sha256=diff['sha256'])
    assert (root / 'flows/demo.py').read_text(encoding='utf-8') == 'x = 2\n'
    assert saved['sha256'] != diff['sha256']

    # Stale expected_sha256 raises conflict without overwriting
    with pytest.raises(ForgeError) as exc_conflict:
        documents.save_file(root, 'flows/demo.py', 'x = 3\n', expected_sha256=diff['sha256'])
    assert exc_conflict.value.code == 'conflict'
    assert (root / 'flows/demo.py').read_text(encoding='utf-8') == 'x = 2\n'

    # Protected files are read-only and rejected
    for readonly_path in ('workspace.json', 'tables/items.json', 'data/notes.txt'):
        ro_diff = documents.file_diff(root, readonly_path)
        assert ro_diff['writable'] is False
        assert ro_diff['readonly_reason']
        with pytest.raises(ForgeError) as exc_ro:
            documents.save_file(root, readonly_path, 'hacked\n')
        assert exc_ro.value.code == 'operation_rejected'

    # Hidden internal files & unknown dotfiles rejected
    for blocked in ('.git/config', '.forge/x.txt', '.secret_other'):
        with pytest.raises(ForgeError):
            documents.save_file(root, blocked, 'nope\n')


def test_init_env_file_and_edit_sets_private_permissions(tmp_path):
    root = _setup_project(tmp_path / 'proj')
    ex_diff = documents.file_diff(root, '.env.example')
    assert ex_diff['writable'] is True
    assert ex_diff['env_exists'] is False

    res = documents.init_env_file(root)
    assert res == {'path': '.env', 'created': True, 'already_exists': False}
    env_file = root / '.env'
    assert env_file.read_text(encoding='utf-8') == 'DEMO_API_KEY=\n'
    assert stat.S_IMODE(env_file.stat().st_mode) == 0o600

    # Second call does not overwrite existing .env
    env_diff = documents.file_diff(root, '.env')
    assert env_diff['writable'] is True
    documents.save_file(root, '.env', 'DEMO_API_KEY=real-local-val\n', expected_sha256=env_diff['sha256'])
    assert stat.S_IMODE(env_file.stat().st_mode) == 0o600

    again = documents.init_env_file(root)
    assert again == {'path': '.env', 'created': False, 'already_exists': True}
    assert env_file.read_text(encoding='utf-8') == 'DEMO_API_KEY=real-local-val\n'


def test_http_file_save_and_env_init_endpoints(tmp_path):
    from test_forge_public_api import request, service
    root = _setup_project(tmp_path / 'proj')
    with service(root, enable_config_write=True, workspace_config_write=True) as app:
        # 1. Init .env via POST /api/project-env-init
        code, init_res = request(app, '/api/project-env-init', {})
        assert code == 200 and init_res['created'] is True

        # 2. Read .env via GET /api/project-file-diff
        code, diff_res = request(app, '/api/project-file-diff?path=.env')
        assert code == 200 and diff_res['writable'] is True

        # 3. Save .env via POST /api/project-file-save
        code, save_res = request(app, '/api/project-file-save', {
            'path': '.env',
            'text': 'DEMO_API_KEY=updated-via-http\n',
            'expected_sha256': diff_res['sha256'],
        })
        assert code == 200 and save_res['path'] == '.env'
        assert (root / '.env').read_text(encoding='utf-8') == 'DEMO_API_KEY=updated-via-http\n'

        # 4. Saving workspace.json fails with 422 (operation_rejected)
        code, _ = request(app, '/api/project-file-save', {'path': 'workspace.json', 'text': '{}'})
        assert code == 422

    # 5. Read-only server disables editing and rejects save with 403
    with service(root, enable_config_write=False, workspace_config_write=False) as ro_app:
        code, ro_diff = request(ro_app, '/api/project-file-diff?path=.env')
        assert code == 200 and ro_diff['writable'] is False
        code, _ = request(ro_app, '/api/project-file-save', {'path': '.env', 'text': 'X=1\n'})
        assert code == 403
