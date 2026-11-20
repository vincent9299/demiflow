"""Rechecks receive fresh controls; a resolver never freezes an old authority."""
import json

import pytest

from demiflow.forge.platform import content_digest
from demiflow.forge.schema import ForgeError
from demiflow.workbench import project_operations
from demiflow.workbench.projects import workspace as ws
from demiflow.workbench.runs import manager as runs
from demiflow.workbench.table_bindings import make_resolver
from demiflow.workbench.analysis import catalog_versions, source_query, media_source_export, web_sources
from test_forge_workspace import demo, isolated
from test_forge_analysis_media_source_export import declared, fixed
from _p12_bounded_test import bounded_test


def changing_resolver(root):
    resolver = make_resolver(ws.table_binding, runs.result_table); controls = []
    def resolve(candidate, control, selector):
        controls.append(control)
        result = resolver(candidate, control, selector)
        if len(controls) == 1:
            path = root / 'tables/candidates.json'
            previous = ws.read_control(path); current = json.loads(json.dumps(previous))
            current['ref']['lance_version'] -= 1
            ws.write_control(path, current, expected=content_digest(previous))
        return result
    return resolve, controls


def test_historical_version_rereads_alias_after_metadata(isolated):
    if bounded_test(test_historical_version_rereads_alias_after_metadata, isolated=isolated): return
    from demiflow.lance.registry import Catalog
    root, control, _ = ws.load_workspace(isolated)
    table, ref, uri = ws.table_binding(root, control, 'candidates')
    Catalog(root / 'data').register(ref)
    binding = ws.preview_binding(root, table, uri)['digest']
    resolver, controls = changing_resolver(root)
    with pytest.raises(ForgeError, match='while its historical version') as caught:
        catalog_versions.resolve_version(root, alias='candidates', expected_binding=binding,
            version=ref.lance_version, table_resolver=resolver)
    assert caught.value.code == 'conflict' and len(controls) == 2
    assert controls[0] is not controls[1]


def test_source_sample_cannot_return_rows_after_alias_rebind(isolated):
    if bounded_test(test_source_sample_cannot_return_rows_after_alias_rebind, isolated=isolated): return
    description = project_operations.describe_table(isolated, 'candidates')
    locator = dict(kind='workspace_table', alias='candidates',
        binding=description['binding']['digest'], columns=['case_id'])
    source = web_sources.resolve(isolated, locator)['source']
    resolver, controls = changing_resolver(isolated)
    with pytest.raises(ForgeError) as caught:
        source_query.read(isolated, source_locator=locator, source_snapshot=source['digest'],
            query=dict(kind='sample', columns=['case_id'], limit=1), table_resolver=resolver)
    assert caught.value.code == 'conflict' and len(controls) == 2
    assert controls[0] is not controls[1]


def test_media_recheck_observes_revoked_display_grant_even_when_table_binding_is_same(tmp_path):
    if bounded_test(test_media_recheck_observes_revoked_display_grant_even_when_table_binding_is_same, tmp_path=tmp_path): return
    root, _ = declared(tmp_path); locator, source = fixed(root)
    resolver = make_resolver(ws.table_binding, runs.result_table)
    selected = media_source_export._selection(root, source_locator=locator, source_snapshot=source['digest'],
        case_key='0', field='image_path', table_resolver=resolver)
    path = root / 'tables/rows.json'; control = ws.read_control(path)
    ws.write_control(path, {**control, 'display': {}}, expected=content_digest(control))
    assert project_operations.describe_table(root, 'rows')['binding']['digest'] == locator['binding']
    with pytest.raises(ForgeError, match='authorization changed') as caught:
        media_source_export._recheck(selected, table_resolver=resolver)
    assert caught.value.code == 'conflict'
