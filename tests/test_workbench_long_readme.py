"""Read long original project documentation without truncating it or its evidence."""
import hashlib

import pytest
from playwright.sync_api import expect

from demiflow.workbench.projects import registry, workspace as ws
from test_forge_project_discovery import original, snapshot, cached_snapshot
from test_forge_markdown_reading import browser
from test_forge_public_api import service
from test_forge_web_redesign import opened


def long_readme(root):
    text = '# 原项目完整说明\n\n' + ('业务说明与版本历史。😀\n' * 3500)
    text += '\n[末尾配置](settings/small.json)\n\nEND-OF-ORIGINAL-README\n'
    assert 65536 < len(text.encode()) < ws.README_BYTES
    (root / 'README.md').write_text(text, encoding='utf-8')
    return text


def test_long_readme_preserves_text_and_tail_reference(original, tmp_path):
    text = long_readme(original)
    registry.add(tmp_path / 'registry.json', original)
    value = ws.workspace_context(original)
    assert value['readme'] == text
    project = value['project']
    assert not any(d['code'] == 'documentation_unreadable' for d in project['diagnostics'])
    assert any(c['path'] == 'settings/small.json' for c in project['configurations'])
    evidence = next(e for e in project['evidence'] if e['path'] == str(original / 'README.md'))
    assert evidence['sha256'] == hashlib.sha256(text.encode()).hexdigest()
    assert project['coverage']['candidate_code_executed'] is False


@pytest.mark.parametrize('fault', ['oversize', 'symlink'])
def test_readme_retains_byte_and_path_boundaries(original, tmp_path, fault):
    registry.add(tmp_path / 'registry.json', original)
    path = original / 'README.md'
    if fault == 'oversize':
        path.write_bytes(b'x' * (ws.README_BYTES + 1))
    else:
        target = tmp_path / 'outside.md'; target.write_text('MUST-NOT-BE-DISPLAYED')
        path.unlink(); path.symlink_to(target)
    value = ws.workspace_context(original)
    assert value['readme'].startswith('项目说明无法读取：')
    assert 'MUST-NOT-BE-DISPLAYED' not in value['readme']
    assert any(d['code'] == 'documentation_unreadable' for d in value['project']['diagnostics'])


def test_long_readme_pages_reconstruct_original_through_http(original, tmp_path, browser):
    text = long_readme(original)
    path = tmp_path / 'registry.json'; registry.add(path, original)
    with service(None, registry=path) as app, opened(browser, app) as (page, requests):
        reader = page.locator('#project-readme .forge-reader')
        expect(reader.locator('.reading-source-chunk').first).to_be_visible()
        portions = []
        while True:
            chunks = reader.locator('.reading-source-chunk').all_text_contents()
            assert len(chunks) <= 4
            assert sum(len(c.encode()) for c in chunks) <= 65536
            portions.append(''.join(chunks))
            next_page = reader.get_by_role('button', name='下一页原文', exact=True)
            if next_page.is_disabled():
                break
            next_page.click()
        assert len(portions) > 1 and ''.join(portions) == text
        assert not page.locator('#project-readme .error-box').count()
        assert not any(method == 'POST' for method, _ in requests)
    assert (original / 'README.md').read_text(encoding='utf-8') == text
