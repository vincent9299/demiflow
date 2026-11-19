"""Markdown navigation is read-only, scoped, bounded and retains document identity."""
import json
import os
from pathlib import Path
from urllib.parse import urlencode

import pytest
from playwright.sync_api import expect

from demiflow.forge.schema import ForgeError
from demiflow.workbench.projects import documents, registry
from test_forge_markdown_reading import browser
from test_forge_public_api import service, request
from test_forge_web_redesign import opened


@pytest.fixture
def docs(tmp_path, monkeypatch):
    monkeypatch.setenv('DEMIFLOW_FORGE_DOMAIN', str(tmp_path/'runtime'))
    root=tmp_path/'project';root.mkdir()
    (root/'README.md').write_text('# Project home\n[Guide](docs/guide.md)')
    (root/'docs').mkdir()
    (root/'docs/guide.md').write_text('# User guide\nSecond document. [Back](../README.md)')
    (root/'notes.MD').write_text('# Case insensitive extension')
    (root/'script.py').write_text('raise RuntimeError("must never execute")')
    (root/'.private').mkdir();(root/'.private/secret.md').write_text('private')
    path=tmp_path/'projects.json';registry.add(path, root, name='Documentation')
    return root,path


def test_document_pages_and_read_are_bounded_and_project_scoped(docs, tmp_path, monkeypatch):
    root,_=docs
    (tmp_path/'outside.md').write_text('outside')
    (root/'external.md').symlink_to(tmp_path/'outside.md')
    (root/'external').symlink_to(tmp_path, target_is_directory=True)
    entries=[];offset=0
    while True:
        page=documents.listing(root, offset=offset, limit=1)
        entries.extend(row['path'] for row in page['entries'])
        if page['next_offset'] is None:break
        offset=page['next_offset']
    assert entries == ['docs', 'notes.MD', 'README.md']
    assert documents.read(root,'docs/guide.md')['text'].startswith('# User guide')
    for path in ['../outside.md','/etc/passwd','.private/secret.md','external.md','external/outside.md','script.py','docs\\guide.md']:
        with pytest.raises(ForgeError):documents.read(root,path)
    (root/'huge.md').write_bytes(b'x'*(documents.DOCUMENT_BYTES+1))
    with pytest.raises(ForgeError, match='budget'):documents.read(root,'huge.md')
    monkeypatch.setattr(documents,'DIRECTORY_ENTRIES',2)
    with pytest.raises(ForgeError, match='budget'):documents.listing(root)


def test_document_http_auth_routes_and_limits(docs):
    root,path=docs
    with service(None,registry=path) as app:
        workspace=next(iter(registry.load_registry(path)['projects']))
        prefix='/api/workspaces/'+workspace+'/'
        status, listing=request(app,prefix+'project-documents')
        assert status==200 and any(row['name']=='README.md' for row in listing['entries'])
        status,value=request(app,prefix+'project-markdown?'+urlencode({'path':'docs/guide.md'}))
        assert status==200 and value['text']==(root/'docs/guide.md').read_text()
        assert request(app,prefix+'project-documents?limit=51')[0]==400
        assert request(app,prefix+'project-markdown?path=README.md',headers={'X-Forge-Token':'wrong'})[0]==403
        assert request(app,'/api/workspaces/w'+'0'*32+'/project-documents')[0] in (400,404)


def test_markdown_directory_switch_links_refresh_back_and_mobile(docs, browser):
    root,path=docs
    before={str(p.relative_to(root)):p.read_bytes() for p in root.rglob('*') if p.is_file()}
    with service(None,registry=path) as app,opened(browser,app) as (page,requests):
        expect(page.get_by_role('navigation',name='Markdown 文档目录')).to_be_visible()
        expect(page.get_by_role('searchbox',name='搜索文档')).to_be_hidden()
        page.get_by_role('button',name='搜索文档',exact=True).click()
        page.get_by_role('searchbox',name='搜索文档').fill('notes')
        expect(page.locator('.document-item:visible')).to_have_count(1)
        page.keyboard.press('Escape')
        page.locator('.document-folder > summary').filter(has_text='docs').click()
        page.locator('.document-item[data-document="docs/guide.md"]').click()
        expect(page.get_by_role('heading',name='User guide',exact=True)).to_be_visible()
        assert page.evaluate('ForgeView.decodeView(location.hash).document')=='docs/guide.md'
        expect(page.locator('.document-item[data-document="docs/guide.md"]')).to_have_attribute('aria-current','page')
        page.reload()
        expect(page.get_by_role('heading',name='User guide',exact=True)).to_be_visible()
        page.go_back()
        expect(page.get_by_role('heading',name='Project home',exact=True)).to_be_visible()
        # Relative Markdown references use the same main reading area.
        page.get_by_role('button',name='Guide',exact=True).click()
        expect(page.get_by_role('heading',name='User guide',exact=True)).to_be_visible()
        # The existing reader keeps unsupported parent-path references inert.
        expect(page.locator('.reading-unsupported')).to_contain_text('Back')
        page.locator('.document-item[data-document="README.md"]').click()
        expect(page.get_by_role('heading',name='Project home',exact=True)).to_be_visible()
        page.set_viewport_size({'width':390,'height':850})
        page.reload()
        expect(page.get_by_role('heading',name='Project home',exact=True)).to_be_visible()
        expect(page.locator('#document-directory')).to_be_hidden()
        page.get_by_role('button',name='文档目录',exact=True).click()
        expect(page.locator('#document-directory')).to_be_visible()
        page.locator('.document-item[data-document="notes.MD"]').click()
        expect(page.get_by_role('heading',name='Case insensitive extension',exact=True)).to_be_visible()
        assert page.evaluate('document.documentElement.scrollWidth<=innerWidth')
        assert not [(method,url) for method,url in requests if method=='POST']
        folder=os.environ.get('FORGE_NAVIGATION_EVIDENCE')
        if folder:
            for width in [1440,390]:
                page.set_viewport_size({'width':width,'height':900});page.reload()
                expect(page.get_by_role('heading',name='Case insensitive extension',exact=True)).to_be_visible()
                page.screenshot(path=str(Path(folder)/('documents-'+str(width)+'.png')),full_page=True)
    assert before=={str(p.relative_to(root)):p.read_bytes() for p in root.rglob('*') if p.is_file()}
