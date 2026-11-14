"""W022 · graph-source wire contract carries the two-key logical selection.

The GET_FIELDS['graph-source'] optional set is extended with logical_node_id and
occurrence_id (C09). They are OPTIONAL strings: a legacy request without them is
still accepted, an unknown key is still refused, and the two keys pass through the
real query parser as plain strings (never coerced to int/JSON). Other routes'
required/optional key sets are asserted unchanged so the extension stays narrow.

Pure transport-contract unit checks against the genuine api._fields / Handler._query
parsers; no server socket, browser or engine execution is involved.
"""
from types import SimpleNamespace
from urllib.parse import urlencode

import pytest

from demiflow.forge.schema import ForgeError
from demiflow.workbench import api

REQUIRED = {'graph_binding', 'node_id', 'kind'}
OPTIONAL = {'resource', 'logical_node_id', 'occurrence_id'}
BINDING = '{"workspace_id":"w","basis_kind":"source","entry":"flows/x.py:run"}'


def _query(**fields):
    url = SimpleNamespace(path='/api/graph-source', query=urlencode(fields))
    return api.Handler._query(None, url)


def test_graph_source_key_sets():
    assert api.GET_FIELDS['graph-source'] == (REQUIRED, OPTIONAL)
    # The two new keys are optional, never required (legacy clients stay valid).
    assert 'logical_node_id' not in REQUIRED and 'occurrence_id' not in REQUIRED
    assert {'logical_node_id', 'occurrence_id'} <= OPTIONAL


def test_required_only_request_accepted():
    query = _query(graph_binding=BINDING, node_id='n3', kind='implementation')
    assert query['node_id'] == 'n3' and query['kind'] == 'implementation'
    assert 'logical_node_id' not in query and 'occurrence_id' not in query


def test_two_key_selection_passes_through_as_strings():
    query = _query(graph_binding=BINDING, node_id='n3', kind='implementation',
                   logical_node_id='ln7', occurrence_id='occ2')
    # Plain strings: not in the integer-validation list nor the JSON-coercion list.
    assert query['logical_node_id'] == 'ln7'
    assert query['occurrence_id'] == 'occ2'
    assert type(query['logical_node_id']) is str and type(query['occurrence_id']) is str


def test_resource_still_optional_alongside_two_key():
    query = _query(graph_binding=BINDING, node_id='n3', kind='config',
                   resource='public', logical_node_id='ln7', occurrence_id='occ2')
    assert query['resource'] == 'public'


def test_unknown_key_refused():
    with pytest.raises(ForgeError) as exc:
        _query(graph_binding=BINDING, node_id='n3', kind='implementation', bogus='x')
    assert exc.value.code == 'invalid_request'


def test_missing_required_refused():
    with pytest.raises(ForgeError) as exc:
        _query(graph_binding=BINDING, node_id='n3')  # kind missing
    assert exc.value.code == 'invalid_request'


def test_fields_accepts_and_rejects_directly():
    spec = api.GET_FIELDS['graph-source']
    api._fields({'graph_binding': BINDING, 'node_id': 'n3', 'kind': 'implementation',
                 'logical_node_id': 'ln7', 'occurrence_id': 'occ2'}, spec)
    with pytest.raises(ForgeError):
        api._fields({'graph_binding': BINDING, 'node_id': 'n3', 'kind': 'implementation',
                     'occurrence_id': 'occ2', 'extra': '1'}, spec)


@pytest.mark.parametrize('name,required,optional', [
    ('graph', {'basis_kind'},
     {'entry', 'params', 'report_digest', 'run_id', 'attempt', 'node_id', 'node_offset'}),
    ('run-context', {'run_id'}, {'attempt'}),
    ('run-observations', {'run_id', 'attempt'}, {'node_id', 'action_id', 'cursor', 'limit'}),
    ('parameter-state', {'path'}, set()),
    ('run-snapshot', {'run_id'}, {'attempt', 'revision'}),
])
def test_other_routes_unchanged(name, required, optional):
    assert api.GET_FIELDS[name] == (required, optional)