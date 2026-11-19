"""Real host assembly and version-init regression checks in an owned /tmp domain.

The browser route faults below only hold a request or fail one transport read;
every successful table/version/sample response comes from the real HTTP service.
"""
import hashlib
from pathlib import Path
import re
from urllib.parse import parse_qs, urlsplit
import urllib.error
import urllib.request

import pytest

from d19_fixtures import assert_tree_imports, owned_fixture
from test_forge_markdown_reading import browser
from test_forge_public_api import service
from test_forge_workspace import demo

from demiflow.workbench.api import ASSETS


def test_index_resources_are_served_by_real_host(demo):
    assert_tree_imports()
    names = re.findall(r'(?:href|src)="(/[^\"]+)"', (ASSETS / 'index.html').read_text())
    assert '/workbench-presentation.js' in names and '/workbench-graph.css' in names
    with service(demo[0]) as app:
        for name in names:
            with urllib.request.urlopen(app.origin + name) as response:
                assert response.read() == (ASSETS / name[1:]).read_bytes(), name
                assert response.headers['X-Content-Type-Options'] == 'nosniff'
                assert 'unsafe-' not in response.headers['Content-Security-Policy']
                assert response.headers['Content-Type'].startswith(
                    'text/javascript' if name.endswith('.js') else 'text/css')
    with service(demo[0], api_only=True) as app:
        for name in ('/workbench-presentation.js', '/workbench-graph.css'):
            with pytest.raises(urllib.error.HTTPError) as caught:
                urllib.request.urlopen(app.origin + name)
            assert caught.value.code == 404
