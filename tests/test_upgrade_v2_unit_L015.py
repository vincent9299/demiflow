"""L015 · run_local_chain — verify_preserve evidence (no production change).

The baseline implementation is kept byte-for-byte. These tests bind the card's
requirement structurally, on the real function AST: resources / stages /
partitions / program binding come from the INTERPRETED layout, the worker
derives checkpoint policy and stage budgets from that same layout, the driver
holds the only commit authority, and the failure/cancel terminal releases the
resource scope through the ResourceManager protocol.

Runtime behaviour (real managed worker, real commit cuts, real recovery) is
bound by the existing chain acceptance files, cited in the delivery receipt;
this file does not re-run them.
"""
import ast
import inspect
import textwrap

from demiflow.execution import chain


def _function(name):
    return ast.parse(textwrap.dedent(inspect.getsource(getattr(chain, name)))).body[0]


def _dotted(node):
    parts = []
    while isinstance(node, ast.Attribute):
        parts.append(node.attr)
        node = node.value
    if isinstance(node, ast.Name):
        parts.append(node.id)
    return ".".join(reversed(parts))


def _calls(node, name):
    return [item for item in ast.walk(node)
            if isinstance(item, ast.Call) and _dotted(item.func) == name]


def _assigned(node, name):
    for item in ast.walk(node):
        if isinstance(item, ast.Assign) and any(
                isinstance(target, ast.Name) and target.id == name for target in item.targets):
            return item.value
    raise AssertionError("no assignment to " + name)


def _dict_value(value):
    assert isinstance(value, ast.Dict), "not a dict literal"
    return {_dotted_key(key): val for key, val in zip(value.keys, value.values)}


def _dict_literal(node, name):
    return _dict_value(_assigned(node, name))


def _dotted_key(key):
    return key.value if isinstance(key, ast.Constant) else _dotted(key)


def _in_finally(node, call):
    for item in ast.walk(node):
        if isinstance(item, ast.Try):
            for statement in item.finalbody:
                if call in ast.walk(statement):
                    return True
    return False


def test_layout_is_interpreted_from_the_sealed_plan():
    fn = _function("run_local_chain")
    assert len(_calls(fn, "interpret_physical_plan")) == 1
    assert len(_calls(fn, "load_sealed_plan")) == 1
    assert len(_calls(fn, "load_execution_bindings")) == 1
    placement = ast.unparse(_assigned(fn, "placement"))
    assert "layout.workers" in placement
    assert "group_id == 'chain-group'" in placement


def test_worker_profile_comes_from_the_layout():
    fn = _function("run_local_chain")
    profile = ast.unparse(_assigned(fn, "sealed_profile"))
    assert profile == "ResourceProfile(**to_wire(placement.profile))"
    requests = _calls(fn, "WorkerRequest")
    assert len(requests) == 1
    assert ast.unparse(requests[0].args[2]) == "sealed_profile"


def test_stages_and_partitions_come_from_the_placement():
    fn = _function("run_local_chain")
    assignment = _dict_literal(fn, "assignment")
    assert ast.unparse(assignment["stages"]) == "list(placement.stage_ids)"
    assert ast.unparse(assignment["partitions"]) == "list(placement.partition_ids)"


def test_program_binding_comes_from_the_sealed_bindings():
    fn = _function("run_local_chain")
    assert ast.unparse(_assigned(fn, "bindings_ref")) == "bindings['ref']"
    launch = _dict_literal(fn, "launch_spec")
    execution_plan = _dict_value(launch["execution_plan"])
    assert ast.unparse(execution_plan["bindings_ref"]) == "bindings_ref"
    assert ast.unparse(execution_plan["action_id"]) == "action.action_id"
    # The worker re-derives placement/checkpoint/stage budgets from this ref.
    from demiflow.execution.worker import layout as worker_layout
    source = inspect.getsource(worker_layout.load_worker_layout)
    assert "interpret_physical_plan" in source
    assert "return placement, layout.checkpoint, stage_limits" in source
    assert 'stage_limits[binding["program_index"]] = dict(stage["budgets"])' in source


def test_assignment_channels_are_validated_against_the_layout():
    # The driver sends an empty channel set for the single-worker chain slice;
    # the worker refuses any mismatch against the interpreted layout, so this
    # is fail-closed rather than an unchecked literal.
    from demiflow.execution import plan_interpreter
    source = inspect.getsource(plan_interpreter.validate_worker_assignment)
    assert "_group_channel_ids(layout, placement)" in source
    assert "assignment input channels differ from the layout" in source
    assert "assignment output channels differ from the layout" in source


def test_driver_holds_the_only_commit_authority():
    fn = _function("run_local_chain")
    assert len(_calls(fn, "terminal.commit_cut")) == 1
    assert _calls(fn, "CommitCoordinator") == []
    assert len(_calls(fn, "terminal.finish")) == 1
    assert len(_calls(fn, "DriverStreamTerminal")) == 1


def test_failure_terminal_releases_the_resource_scope():
    fn = _function("run_local_chain")
    closes = _calls(fn, "manager.close")
    assert len(closes) == 1
    assert _in_finally(fn, closes[0]), "scope close must run on failure/cancel too"
    reconciles = _calls(fn, "manager.reconcile_once")
    assert any(_in_finally(fn, call) for call in reconciles)
    assert len(_calls(fn, "connector.close")) == 1


def test_restore_point_comes_from_the_terminal_journal():
    fn = _function("run_local_chain")
    assert ast.unparse(_assigned(fn, "restore")) == "terminal.restore_point()"
    assignment = _dict_literal(fn, "assignment")
    assert "restore" in ast.unparse(assignment["restore_point"])