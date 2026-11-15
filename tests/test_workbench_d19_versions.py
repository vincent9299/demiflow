"""V02 -- the fixed historical-version chain is one identity end to end.

Success criterion (04-acceptance §2): a real Lance table with two registered
versions whose schema, rows and image bytes genuinely differ; selecting a
historical version drives sample / profile / Notebook source export / Case
membership / media through the SAME version binding, never the current alias
value. Required counterexamples: unregistered / deleted version, alias drift,
half a version pair, run-product re-versioning, and byte-for-byte old-locator
digest compatibility. The original tables control file hash must not move.

This is real acceptance, not a contract mock: every read goes through the
candidate tree's own HTTP service and native Lance worker. Assertions on the
new C4.1 wire shape (selected.binding/reference/alias_binding, version-paired
sample/profile/media, historical locator resolve/export) fail honestly until
the F lane lands; they are never satisfied by reading the current version.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from urllib.parse import urlencode

import pyarrow as pa
import pytest

from d19_fixtures import owned_fixture
from test_forge_markdown_reading import browser

from demiflow.workbench.projects import workspace as ws
from demiflow.workbench.analysis import web_sources
from demiflow.workbench.analysis.arrow_bridge import decoded_batches
from demiflow.workbench.analysis.materialize import input_chunks
from demiflow.workbench.worker import operation
from demiflow.forge.schema import ForgeError


def _sha(path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _get(fx, endpoint, **query):
    path = '/api/' + endpoint
    if query:
        path += '?' + urlencode(query)
    return fx.api(path)


@pytest.mark.parametrize('owned_fixture', [{}], indirect=True)
def test_ui_d19_fixed_version_chain(owned_fixture, browser):
    fx = owned_fixture
    root = fx.root
    vf = fx.version_facts
    alias = vf['alias']
    v1, v2 = vf['versions'][1], vf['versions'][2]
    alias_binding = vf['alias_binding']
    control_path = Path(vf['control_path'])

    # -- 0. the two registered versions are genuinely different facts ---------
    assert v1['schema_hash'] != v2['schema_hash']
    assert v1['row_count'] == 2 and v2['row_count'] == 3
    assert v1['rows'] != v2['rows']
    assert v1['columns'] != v2['columns']
    assert v1['image_bytes'] != v2['image_bytes']
    assert v1['binding'] != v2['binding']
    assert v1['binding'] != alias_binding
    assert v2['binding'] == alias_binding  # the alias is bound to the current v2
    control_baseline = _sha(control_path)
    assert control_baseline == vf['control_sha256']
    fx.record('v02-version-facts', dict(
        alias=alias, alias_binding=alias_binding,
        v1={k: v1[k] for k in ('binding', 'schema_hash', 'row_count', 'columns', 'image_sha256')},
        v2={k: v2[k] for k in ('binding', 'schema_hash', 'row_count', 'columns', 'image_sha256')},
        control_sha256=control_baseline))

    # -- 1. registered version list (real Catalog facts, works on baseline) ---
    code, listed = _get(fx, 'table-versions', alias=alias, expected_binding=alias_binding)
    assert code == 200, listed
    assert listed['total_registered'] == 2 and listed['truncated'] is False
    assert [item['lance_version'] for item in listed['versions']] == [1, 2]
    assert listed['versions'][0]['row_count'] == 2
    assert listed['versions'][1]['row_count'] == 3
    assert listed['binding']['digest'] == alias_binding

    # -- 2. selected version opens the NEW fixed binding (C4.1 §2, F02/F03) ---
    # Until F lands, selected only carries {version, metadata}; these assertions
    # fail honestly rather than passing on the current alias value.
    for version, facts in ((1, v1), (2, v2)):
        code, opened = _get(fx, 'table-versions', alias=alias,
                            expected_binding=alias_binding, version=version)
        assert code == 200, opened
        selected = opened['selected']
        assert selected['version'] == version
        assert selected['binding']['digest'] == facts['binding'], selected
        assert selected['alias_binding'] == alias_binding, selected
        assert selected['reference']['ref']['lance_version'] == version
        assert selected['metadata']['row_count'] == facts['row_count']
        assert list(selected['metadata']['fields']) == facts['columns']
        assert selected['metadata']['schema_hash'] == facts['schema_hash']
        # opening a historical version never moves the fixed alias binding
        assert opened['binding']['digest'] == alias_binding
    fx.record('v02-selected', dict(v1_binding=v1['binding'], v2_binding=v2['binding']))

    # -- 3. sample reads the selected version, not the current alias (F03) ----
    for version, facts in ((1, v1), (2, v2)):
        code, sampled = _get(fx, 'sample', alias=alias, expected_binding=alias_binding,
                             version=version, version_binding=facts['binding'], limit=100)
        assert code == 200, sampled
        assert sampled['binding']['digest'] == facts['binding'], sampled
        assert [row['case_id'] for row in sampled['rows']] == [r['case_id'] for r in facts['rows']]
        assert sampled['columns'] == facts['columns']
    # current alias (no version pair) stays byte-compatible with the bound v2
    code, current = _get(fx, 'sample', alias=alias, expected_binding=alias_binding, limit=100)
    assert code == 200 and current['binding']['digest'] == alias_binding
    assert [row['case_id'] for row in current['rows']] == [r['case_id'] for r in v2['rows']]

    # -- 4. profile binds the same selected version (C4.1 §3, F13) ------------
    code, profile = _get(fx, 'table-profile', alias=alias, expected_binding=alias_binding,
                         version=1, version_binding=v1['binding'],
                         columns=json.dumps(v1['columns']))
    assert code == 200, profile
    assert profile['binding'] == v1['binding'], profile
    assert profile['rows'] == v1['row_count']

    # -- 5. media reads the selected version's image bytes (F14/F16) ----------
    for version, facts in ((1, v1), (2, v2)):
        code, image = _get(fx, 'media', alias=alias, expected_binding=alias_binding,
                           version=version, version_binding=facts['binding'],
                           row_id=0, field='image_uri')
        assert code == 200, image
        assert image == facts['image_bytes'], (version, len(image))
    # a historical text row must never be paired with the other version's image
    code, wrong = _get(fx, 'media', alias=alias, expected_binding=alias_binding,
                       version=1, version_binding=v2['binding'], row_id=0, field='image_uri')
    assert code >= 400, wrong

    # -- 6. Notebook source export carries the historical locator (F07-F11) ---
    columns = v1['columns']
    historical = dict(kind='workspace_table', alias=alias, binding=v1['binding'],
                      columns=columns, version=1, alias_binding=alias_binding)
    resolved = web_sources.resolve(root, historical)
    source = resolved['source']
    assert source['binding'] == v1['binding'], source
    assert source['rows'] == v1['row_count']
    assert [col['name'] for col in source['schema']] == columns
    assert resolved['locator'] == historical  # never normalized to current
    output = fx.tmp_path / 'v02-export.bin'
    fd = os.open(output, os.O_RDWR | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        result = operation('analysis_export', root, source_locator=historical,
                           source_snapshot=source['digest'], output_fd=fd, _pass_fds=(fd,))
    finally:
        os.close(fd)
    config = dict(operation='materialize', input_id='i' + 'a' * 32, publish_id='v02-export',
                  **{key: result[key] for key in ('stream_sha256', 'schema_sha256', 'encoded_bytes', 'chunks')})
    decoded = pa.Table.from_batches(decoded_batches(input_chunks(output, config)))
    assert decoded.num_rows == v1['row_count']
    assert decoded.to_pylist() == [{k: row[k] for k in columns} for row in v1['rows']]
    assert result['source_snapshot'] == source['digest']

    # -- 7. Case membership / media verify on the SAME version (F10/F16) ------
    target = dict(source=source['digest'], case_key='0', variant=None, field=None)
    assert web_sources.verify(root, locator=historical, source=source, target=target) == {'verified': True}
    membership = ws.sample_table(root, alias, expected_binding=alias_binding,
                                 version=1, version_binding=v1['binding'],
                                 columns=[], row_id=0)
    assert membership['binding']['digest'] == v1['binding']
    assert membership['rows'][0]['case_id'] == v1['rows'][0]['case_id']
    # verifying the historical source against the current alias value must fail
    drifted = dict(historical, binding=alias_binding)
    with pytest.raises(ForgeError):
        web_sources.resolve(root, drifted)

    # -- 8. browser version switch drives the same fixed context (A06) --------
    with fx.open_page(browser) as (page, log):
        page.locator('.function-nav').get_by_role('button', name='数据', exact=True).click()
        page.get_by_label('当前数据表', exact=True).select_option(alias)
        page.locator('details.bound-versions > summary').click()
        # registered facts preview shows the two genuinely different versions
        versions_panel = page.locator('details.bound-versions')
        versions_panel.get_by_text('v1').first.wait_for()
        fx.shot(page, 'v02-versions-listed')
        # A06 selectTableVersion: choosing v1 replaces the fixed context and
        # re-reads sample with version/version_binding. The control is not yet
        # integrated; locate it by the frozen version affordance and fail honest.
        switch = page.get_by_role('button', name='固定到 v1')
        if switch.count() == 0:
            switch = page.get_by_role('button', name='v1', exact=True)
        assert switch.count() > 0, 'A06 selectTableVersion not integrated: no historical version switch affordance'
        switch.first.click()
        # wait for the real historical sample re-read to be issued (bounded)
        for _ in range(40):
            page.wait_for_timeout(100)
            if any('/sample' in r['url'] and 'version=1' in r['url'] for r in log['requests']):
                break
        fx.shot(page, 'v02-version-one-selected')
        version_requests = [r for r in log['requests']
                            if '/sample' in r['url'] and 'version=1' in r['url']]
        assert version_requests, 'A06 must re-read sample with version=1&version_binding'
        assert any('version_binding=' + v1['binding'] in r['url'] for r in version_requests), version_requests
        fx.record('v02-browser-requests', log['requests'])

    # -- 9. required counterexamples (real HTTP, fail closed) -----------------
    # unregistered / deleted version
    code, value = _get(fx, 'table-versions', alias=alias, expected_binding=alias_binding, version=999)
    assert code >= 400, value
    # half a version pair is rejected on every read branch
    for endpoint in ('sample', 'table-profile', 'media'):
        query = dict(alias=alias, expected_binding=alias_binding, version=1)
        if endpoint == 'table-profile':
            query['columns'] = json.dumps(columns)
        if endpoint == 'media':
            query.update(row_id=0, field='image_uri')
        code, value = _get(fx, endpoint, **query)
        assert code >= 400, (endpoint, value)
        query = dict(alias=alias, expected_binding=alias_binding, version_binding=v1['binding'])
        if endpoint == 'table-profile':
            query['columns'] = json.dumps(columns)
        if endpoint == 'media':
            query.update(row_id=0, field='image_uri')
        code, value = _get(fx, endpoint, **query)
        assert code >= 400, (endpoint, value)
    # a version_binding that does not match the opened version is a conflict
    code, value = _get(fx, 'sample', alias=alias, expected_binding=alias_binding,
                       version=1, version_binding=v2['binding'], limit=10)
    assert code >= 400, value
    # alias drift: re-bind the alias, the old expected_binding must not guess it.
    # This is a deliberate write counterexample; restore the exact original control
    # bytes afterwards so the read-chain hash invariant below stays meaningful.
    control_original = control_path.read_bytes()
    ws.attach_table(root, alias, relative_uri=alias + '.lance', version=1,
                    dataset_id=alias, schema_name=alias, schema_version='1')
    code, value = _get(fx, 'sample', alias=alias, expected_binding=alias_binding,
                       version=2, version_binding=v2['binding'], limit=10)
    assert code >= 400, value
    control_path.write_bytes(control_original)

    # run product is a single sealed version, never a re-versionable history
    run_alias = 'run:' + fx.run_facts['run_id'] + ':target'
    code, run_listed = _get(fx, 'table-versions', alias=run_alias)
    assert code == 200, run_listed
    assert run_listed['reason'] == 'run_output_sealed_version'
    assert run_listed['versions'] == [] and run_listed['total_registered'] == 0
    code, value = _get(fx, 'table-versions', alias=run_alias, version=2)
    assert code >= 400, value

    # -- 10. old locator / old URL stay byte-for-byte compatible --------------
    old_locator = dict(kind='workspace_table', alias=alias, binding=alias_binding, columns=columns)
    old_source = web_sources.resolve(root, old_locator)['source']
    assert old_source['binding'] == alias_binding
    assert old_source['rows'] == v2['row_count']
    code, plain = _get(fx, 'sample', alias=alias, expected_binding=alias_binding, limit=100)
    assert code == 200 and plain['binding']['digest'] == alias_binding
    assert 'version' not in plain and 'version_binding' not in plain

    # -- 11. the original control file never moved ----------------------------
    assert _sha(control_path) == control_baseline, 'tables control file hash changed during version reads'
    fx.record('v02-result', dict(
        status='asserted', control_sha256_before=control_baseline,
        control_sha256_after=_sha(control_path),
        v1_binding=v1['binding'], v2_binding=v2['binding'], alias_binding=alias_binding))