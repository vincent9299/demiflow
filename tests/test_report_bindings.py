"""Report bindings describe the graph that is returned.

A report binding ties a report node to one node of the compiled graph. The
driver builds the identity bridge only when the bindings cover every node, and
the bridge builder refuses a binding whose occurrence or semantic identity is
not in the graph. Two things used to break that:

* graph adaptation folds the terminal into another node and re-derives the
  semantic identity of each node it marks, after the bindings were recorded;
* for exclude_keys the compiler reads the excluded side through a key-only
  source it builds itself, which had no binding, so a checked flow that used
  exclude_keys never got an identity bridge.
"""
import pytest

from demiflow.execution.seal.identity_bridge import build_identity_bridge

import compile_contract_corpus as corpus

REPORT_CASES = sorted(name for name in corpus.CASES if name.startswith('report/'))


@pytest.fixture(scope='module')
def context(tmp_path_factory):
    return corpus.Context(tmp_path_factory.mktemp('report-bindings'))


def _nodes(compiled):
    return {node.occurrence_id: node for node in compiled.graph.nodes}


def _bridge(compiled):
    graph = compiled.graph
    return build_identity_bridge(
        {'dag_id': graph.dag_id, 'nodes': [{'occurrence_id': node.occurrence_id, 'logical_node_id': node.semantic_id}
                                           for node in graph.nodes]},
        action_id=graph.action_id, report_digest='report-digest', bindings=[dict(item) for item in compiled.report_bindings])


@pytest.mark.parametrize('name', REPORT_CASES)
def test_every_binding_names_a_node_of_the_returned_graph(context, name):
    compiled = corpus.lower_case(name, context)[0]
    nodes = _nodes(compiled)
    assert compiled.report_bindings
    for binding in compiled.report_bindings:
        assert binding['occurrence_id'] in nodes, binding['report_node_id']
        assert nodes[binding['occurrence_id']].semantic_id == binding['logical_node_id'], binding['report_node_id']
    keys = [(binding['report_node_id'], binding['report_occurrence']) for binding in compiled.report_bindings]
    assert len(keys) == len(set(keys))
    assert _bridge(compiled)['entries']            # the bridge builder accepts them as they are


def test_exclude_keys_is_covered_node_for_node(context):
    compiled = corpus.lower_case('report/exclude_keys', context)[0]
    nodes = _nodes(compiled)
    assert {binding['occurrence_id'] for binding in compiled.report_bindings} == set(nodes)
    key_only = [node for node in nodes.values() if node.kernel.configuration.get('key_only')]
    assert len(key_only) == 1
    bound = [binding for binding in compiled.report_bindings if binding['occurrence_id'] == key_only[0].occurrence_id]
    assert len(bound) == 1 and bound[0]['report_node_id'].startswith('report-source-')
    relational = next(node for node in nodes.values() if node.kernel.kind == 'exclude_keys')
    assert relational.kernel.configuration['lance_key_source'] == key_only[0].occurrence_id
    assert len(_bridge(compiled)['entries']) == len(nodes)


@pytest.mark.parametrize('name', ['report/graph_broadcast', 'report/graph_target', 'report/graph_literal_prefix'])
def test_routed_graph_bindings_follow_the_adapter(context, name):
    compiled = corpus.lower_case(name, context)[0]
    nodes, graph = _nodes(compiled), compiled.graph
    terminal = nodes[graph.terminals[0]]
    assert 'graph_result' in terminal.kernel.configuration
    action_binding = [binding for binding in compiled.report_bindings if binding['report_node_id'] == graph.action_id]
    # The terminal was folded into the captured union; its binding names the node that is the terminal now.
    assert len(action_binding) == 1 and action_binding[0]['occurrence_id'] == terminal.occurrence_id
    assert action_binding[0]['logical_node_id'] == terminal.semantic_id
    marked = [node for node in nodes.values() if any(key.startswith('graph_') for key in node.kernel.configuration)]
    assert len(marked) >= 4
    for node in marked:
        for binding in compiled.report_bindings:
            if binding['occurrence_id'] == node.occurrence_id:
                assert binding['logical_node_id'] == node.semantic_id


def test_plans_the_adapter_does_not_touch_bind_in_node_order(context):
    compiled = corpus.lower_case('report/chain', context)[0]
    assert [binding['occurrence_id'] for binding in compiled.report_bindings] == [
        node.occurrence_id for node in compiled.graph.nodes]
    assert [binding['mode'] for binding in compiled.report_bindings].count('reuse') >= 1


def test_what_stays_uncovered_and_why(context):
    """A zero-row take runs none of the declared plan, so the report has nothing for the node that does run."""
    compiled = corpus.lower_case('report/take_zero', context)[0]
    nodes = _nodes(compiled)
    unbound = [node.kernel.kind for node in nodes.values()
               if node.occurrence_id not in {binding['occurrence_id'] for binding in compiled.report_bindings}]
    assert unbound == ['items']
    compiled = corpus.lower_case('report/chain_terminal_unbound', context)[0]
    assert len(compiled.report_bindings) == len(compiled.graph.nodes) - 1     # no observation for the action itself
