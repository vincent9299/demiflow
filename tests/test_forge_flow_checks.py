"""Contract closure acceptance: source is inspected, never imported or run."""

import copy
import json
from pathlib import Path
import re

import pytest

from demiflow.data.flow_contract import field, scalar, transfer_fields
from demiflow.forge.flow_check import FlowChecker
from demiflow.forge.platform import discover_catalog


@pytest.fixture(scope="module")
def catalog():
    value = discover_catalog(["demiflow.data"])
    assert value["inspection_complete"], value["diagnostics"]
    return value


@pytest.fixture
def composition(tmp_path, catalog):
    for directory in ("flows", "operators"):
        (tmp_path / directory).mkdir()
    doc = Path(__file__).parents[1] / "docs/forge-stage2-contract-design-20261102.md"
    blocks = re.findall(r"```python\n(.*?)\n```", doc.read_text(), re.S)
    op = tmp_path / "operators/question_rules.py"
    op.write_text(blocks[0] + "\n")
    flow = tmp_path / "flows/questions.py"
    flow.write_text(blocks[1] + "\n")
    tables = {
        "candidates": {
            "uri": str(tmp_path / "candidates.lance"),
            "version": 3,
            "fields": {
                "case_id": field(scalar("str")),
                "question": field(scalar("str", nullable=True)),
                "image_uri": field(scalar("str", nullable=True)),
            },
        },
        "scores": {
            "uri": str(tmp_path / "scores.lance"),
            "version": 2,
            "fields": {
                "case_id": field(scalar("str")),
                "score": field(scalar("float")),
            },
        },
    }
    target = str(tmp_path / "selected.lance")
    args = {k: {"uri": v["uri"], "version": v["version"]} for k, v in tables.items()}
    args["target"] = {"uri": target}

    def inspect(entry="select_a", limits=None):
        return FlowChecker(tmp_path, catalog, tables, [target], limits=limits).inspect(
            "flows/questions.py:" + entry, args
        )

    return tmp_path, flow, op, tables, args, inspect


def test_complete_example_and_variants(composition):
    root, flow, op, tables, args, inspect = composition
    a, b = inspect(), inspect("select_b")
    assert a["status"] == b["status"] == "checked", (a["diagnostics"], b["diagnostics"])
    assert len(a["nodes"]) == 7 and len(a["edges"]) == 6
    assert a["digest"] != b["digest"]
    assert [
        n["parameters"]["fn_kwargs"]["minimum"]
        for n in a["nodes"]
        if n["operator"] and n["operator"]["name"] == "above_threshold"
    ] == [0.8]
    assert a["coverage"]["operator_behavior"] == "not_run"
    assert a["coverage"]["execution"] == "not_run"
    assert not Path(args["target"]["uri"]).exists()
    assert inspect()["digest"] == a["digest"]


@pytest.mark.parametrize('tail,category', [
    ('.run_stream()', 'unsupported'),
    ('.unknown_transform()', 'unknown'),
    ('.repartition(2)', 'invalid'),  # Supported transform still needs a terminal action.
    ('.limit(-1)', 'invalid'),
    (".write_lance(target['uri'], mode='append', return_receipt=True)", 'denied'),
])
def test_capability_diagnostic_categories(composition, tail, category):
    root, flow, op, tables, args, inspect_flow = composition
    flow.write_text("from demiflow import data\ndef select_a(*, candidates, scores, target):\n"
                    "    return data.read_lance(candidates['uri'], version=candidates['version'])" + tail + "\n")
    report = inspect_flow()
    assert report['diagnostics'][0]['category'] == category, report['diagnostics']


@pytest.mark.parametrize(
    "old,new,rule",
    [
        ('inputs={"text": "question"}', 'inputs={"question": "text"}', "OP-02"),
        (
            'fn_kwargs={"max_chars": 4096}',
            'fn_kwargs={"max_chars": 4096, "text": "oops"}',
            "OP-02",
        ),
        ('version=candidates["version"]', "version=None", "FL-05"),
        ('how="inner"', 'how="left"', "FL-06"),
        ("return_receipt=True", "return_receipt=False", "FL-09"),
        ('mode="create",', "", "FL-09"),
        (
            'output="normalized_question"',
            'output="normalized_question", outputs={"x": "y"}',
            "FL-06",
        ),
        (
            'columns=["case_id", "question", "image_uri"]',
            'columns=["missing"]',
            "FL-06",
        ),
        (
            "return selected.write_lance(",
            "return selected\n    selected.write_lance(",
            "FL-04",
        ),
        (
            "normalized = source.map(",
            "unused = source.limit(1)\n    normalized = source.map(",
            "FL-04",
        ),
    ],
)
def test_wiring_breakages(composition, old, new, rule):
    _, flow, _, _, _, inspect = composition
    text = flow.read_text()
    assert old in text
    flow.write_text(text.replace(old, new))
    result = inspect()
    assert result["status"] == "invalid", result["diagnostics"]
    assert result["diagnostics"][0]["rule"] == rule, result["diagnostics"]
    assert result["diagnostics"][0]["line"] is not None


@pytest.mark.parametrize(
    "old,new,rule,status",
    [
        ("text: str | None", "text: str", "FL-06", "invalid"),
        (
            "def keep_selected(row: SelectionRow) -> bool:",
            "def keep_selected(row: SelectionRow) -> dict[str, bool]:",
            "OP-03",
            "invalid",
        ),
        ('return row["selected"]', 'return {"selected": True}', "OP-03", "invalid"),
        (
            'return row["selected"]',
            'row["selected"] = True\n    return row["selected"]',
            "OP-07",
            "invalid",
        ),
        ('return row["selected"]', 'return eval("True")', "OP-06", "invalid"),
        (
            'return row["selected"]',
            'try:\n        return row["selected"]\n    except Exception:\n        return True',
            "OP-08",
            "invalid",
        ),
        (
            'return row["selected"]',
            'try:\n        return row["selected"]\n    finally:\n        return True',
            "OP-08",
            "invalid",
        ),
        ('return row["selected"]', "return mystery(row)", "OP-06", "incomplete"),
        (
            'return row["selected"]',
            "return row.unknown_method()",
            "OP-06",
            "incomplete",
        ),
        ("-> bool:", "-> Any:", "OP-03", "incomplete"),
    ],
)
def test_operator_breakages(composition, old, new, rule, status):
    _, _, op, _, _, inspect = composition
    text = op.read_text()
    assert old in text
    op.write_text(text.replace(old, new))
    result = inspect()
    assert result["status"] == status, result["diagnostics"]
    assert result["diagnostics"][0]["rule"] == rule, result["diagnostics"]


def test_helper_alias_preserves_effect_check(composition):
    root, _, op, _, _, inspect = composition
    (root / "operators/helper.py").write_text(
        'from demiflow import data as native\ndef helper(row: str) -> str:\n    native.read_lance("hidden.lance")\n    return row\n'
    )
    op.write_text(
        "from operators.helper import helper as harmless\n"
        + op.read_text().replace(
            'return " ".join(text.split())', "return harmless(text)"
        )
    )
    result = inspect()
    assert (
        result["status"] == "invalid" and result["diagnostics"][0]["rule"] == "OP-04"
    ), result


def test_no_module_default_decorator_or_class_execution(composition):
    root, flow, op, _, _, inspect = composition
    marker = root / "executed"
    flow.write_text(f'open({str(marker)!r}, "w").write("bad")\n' + flow.read_text())
    result = inspect()
    assert result["status"] == "invalid" and not marker.exists()


def test_broad_reraise_and_pure_actor(composition):
    _, _, op, _, _, inspect = composition
    op.write_text(
        op.read_text().replace(
            'return row["selected"]',
            'try:\n        return row["selected"]\n    except Exception:\n        raise',
        )
    )
    assert inspect()["status"] == "checked"


def test_unknown_semantics_and_graph_limits(composition, catalog):
    _, _, _, _, _, inspect = composition
    result = inspect(limits={"nodes": 2})
    assert (
        result["status"] == "incomplete" and result["diagnostics"][0]["rule"] == "FL-10"
    )


def test_missing_and_null_are_independent():
    left = {"id": field(scalar("str"))}
    right = {"id": field(scalar("str")), "value": field(scalar("float"))}
    joined = transfer_fields("join", left, {"on": "id", "how": "left"}, other=right)
    assert (
        joined["value"]["required"] is False
        and joined["value"]["type"]["nullable"] is False
    )
    result = {
        **scalar("record"),
        "fields": {"value": field(scalar("float"), required=False)},
    }
    with pytest.raises(ValueError, match="must exist"):
        transfer_fields(
            "map",
            left,
            {"inputs": {"x": "id"}, "outputs": {"value": "value"}},
            returns=result,
        )


def simple_flow(
    root, catalog, body, operator="", *, arguments=None, fields=None, targets=None
):
    from demiflow.forge.flow_check import FlowChecker

    (root / "flows").mkdir(exist_ok=True)
    (root / "operators").mkdir(exist_ok=True)
    (root / "operators/rules.py").write_text(operator)
    (root / "flows/simple.py").write_text(body)
    fields = fields or {"x": field(scalar("int"))}
    tables = {
        "source": {"uri": "/fixture/source.lance", "version": 1, "fields": fields}
    }
    return FlowChecker(
        root, catalog, tables, targets or ["/fixture/out.lance"]
    ).inspect("flows/simple.py:run", arguments or {})


def test_native_range_filter_count_declares_integer_ids_and_scalar_result(tmp_path, catalog):
    source = """from demiflow import data
from operators.rules import is_even

def run():
    return data.range(10).filter(is_even).count()
"""
    predicate = """from typing import TypedDict
class Row(TypedDict):
    id: int
def is_even(row: Row) -> bool:
    return row['id'] % 2 == 0
"""
    report = simple_flow(tmp_path, catalog, source, predicate)
    assert report['status'] == 'checked', report['diagnostics']
    assert [node['kind'] for node in report['nodes']] == ['range', 'filter', 'count']
    assert report['nodes'][0]['fields']['id'] == field(scalar('int'))
    assert report['nodes'][-1]['result'] == 'value'
    assert report['coverage']['execution'] == 'not_run'

    negative = simple_flow(tmp_path, catalog, source.replace('range(10)', 'range(-1)'), predicate)
    assert negative['status'] == 'invalid'
    assert 'non-negative integer' in negative['diagnostics'][0]['detail']


@pytest.mark.parametrize(
    "expression,operator,status,rule",
    [
        ("op", "def op(x: int) -> int:\n    return x + 1\n", "checked", None),
        (
            "op",
            "class op:\n    def __init__(self, increment=1):\n        self.increment = increment\n    def __call__(self, x: int) -> int:\n        return x + self.increment\n",
            "checked",
            None,
        ),
        (
            "op",
            "def helper(value: str) -> int:\n    return len(value)\ndef op(x: int) -> int:\n    return helper(x)\n",
            "invalid",
            "OP-03",
        ),
        (
            "op",
            "def op(x: int) -> int:\n    helper = abs\n    return helper(x)\n",
            "checked",
            None,
        ),
        (
            "op",
            "def op(x: int) -> int:\n    total = 0\n    for part in [x, 1]:\n        total += part\n    return total\n",
            "checked",
            None,
        ),
        ("op", "async def op(x: int) -> int:\n    return x\n", "invalid", "OP-02"),
        (
            "op",
            "class op:\n    def __call__(self, x: int) -> int:\n        return x\n    async def aclose(self) -> None:\n        pass\n",
            "invalid",
            "OP-07",
        ),
        (
            "op",
            'def op(x: int) -> int:\n    return __import__("os").getpid()\n',
            "invalid",
            "OP-06",
        ),
        (
            "op",
            "import math\ndef op(x: int) -> int:\n    return math.floor(x)\n",
            "checked",
            None,
        ),
        (
            "op",
            "import math\ndef op(x: int) -> int:\n    return math.factorial(x)\n",
            "incomplete",
            "OP-06",
        ),
        (
            "op",
            'class op:\n    def __init__(self):\n        self.file = open("/tmp/probe")\n    def __call__(self, x: int) -> int:\n        return x\n',
            "invalid",
            "OP-05",
        ),
        (
            "op",
            "def op(x: int) -> int:\n    while x > 0:\n        x -= 1\n    return x\n",
            "incomplete",
            "OP-06",
        ),
        (
            "op",
            "def op(x: int) -> int:\n    try:\n        return x\n    except ValueError:\n        raise\n",
            "checked",
            None,
        ),
    ],
)
def test_operator_subset(tmp_path, catalog, expression, operator, status, rule):
    flow = f'from demiflow import data\nfrom operators.rules import op\ndef run():\n    source = data.read_lance("/fixture/source.lance", version=1)\n    result = source.map({expression}, inputs={{"x":"x"}}, output="value")\n    return result.write_lance("/fixture/out.lance", mode="create", return_receipt=True)\n'
    result = simple_flow(tmp_path, catalog, flow, operator)
    assert result["status"] == status, result["diagnostics"]
    if rule:
        assert result["diagnostics"][0]["rule"] == rule, result


@pytest.mark.parametrize(
    "mutation",
    [
        'box = [row]\n    box[0].update({"x": 2})',
        'box = {"borrowed": row}\n    box["borrowed"].clear()',
        'values = row["items"]\n    values += [2]',
    ],
)
def test_borrowed_mutation_cannot_hide_in_containers(tmp_path, catalog, mutation):
    operator = (
        "from typing import TypedDict\nclass Row(TypedDict):\n    x: int\n    items: list[int]\ndef op(row: Row) -> bool:\n    "
        + mutation
        + "\n    return True\n"
    )
    fields = {
        "x": field(scalar("int")),
        "items": field({**scalar("list"), "item": scalar("int")}),
    }
    flow = 'from demiflow import data\nfrom operators.rules import op\ndef run():\n    result = data.read_lance("/fixture/source.lance", version=1).filter(op)\n    return result.write_lance("/fixture/out.lance",mode="create",return_receipt=True)\n'
    result = simple_flow(tmp_path, catalog, flow, operator, fields=fields)
    assert (
        result["status"] == "invalid" and result["diagnostics"][0]["rule"] == "OP-07"
    ), result


def test_config_branches_expansion_aliases_and_independent_actions(tmp_path, catalog):
    code = """from demiflow import data as native
from operators.rules import op as fn

def _part(source):
    alias = fn
    return source.map(alias, inputs={"x":"x"}, output="x")

def run(*, enabled, targets):
    read = native.read_lance
    source = read("/fixture/source.lance", version=1)
    if enabled:
        source = _part(source)
    for target in targets:
        result = _part(source)
        result.write_lance(target, mode="create", return_receipt=True)
"""
    result = simple_flow(
        tmp_path,
        catalog,
        code,
        "def op(x: int) -> int:\n    return x\n",
        arguments={
            "enabled": True,
            "targets": ["/fixture/a.lance", "/fixture/b.lance"],
        },
        targets=["/fixture/a.lance", "/fixture/b.lance"],
    )
    assert result["status"] == "checked", result["diagnostics"]
    assert (
        len(result["nodes"]) == 6
        and len(result["edges"]) == 5
        and len(set(n["id"] for n in result["nodes"])) == 6
    )
    repeated = simple_flow(
        tmp_path,
        catalog,
        code,
        "def op(x: int) -> int:\n    return x\n",
        arguments={
            "enabled": True,
            "targets": ["/fixture/a.lance", "/fixture/a.lance"],
        },
        targets=["/fixture/a.lance"],
    )
    assert (
        repeated["status"] == "invalid"
        and repeated["diagnostics"][0]["rule"] == "FL-09"
    )


@pytest.mark.parametrize(
    "probe",
    [
        "import operators.hidden\n",
        'def unused(x=open("probe", "w")):\n    pass\n',
        'class Hidden:\n    value = open("probe", "w")\n',
        'def unused(x: open("probe", "w")):\n    pass\n',
        '@open("probe", "w")\ndef unused():\n    pass\n',
    ],
)
def test_all_initialization_is_checked_without_execution(composition, probe):
    root, flow, op, _, _, inspect = composition
    (root / "operators/hidden.py").write_text('open("probe", "w")\n')
    flow.write_text(probe + flow.read_text())
    result = inspect()
    assert (
        result["status"] in {"invalid", "incomplete"} and not (root / "probe").exists()
    )


def test_package_initialization_and_real_definition_ownership(composition):
    root, flow, op, _, _, inspect = composition
    (root / "operators/__init__.py").write_text('open("probe", "w")\n')
    assert inspect()["status"] == "invalid"
    (root / "operators/__init__.py").unlink()
    (root / "flows/compute.py").write_text(op.read_text())
    op.write_text(
        "from flows.compute import normalize_question, above_threshold, keep_selected\n"
    )
    result = inspect()
    assert result["status"] == "checked", result
    assert any(o["source"] == "flows/compute.py" for o in result["operators"])


@pytest.mark.parametrize(
    "limit", ["files", "total_bytes", "ast_nodes", "file_nodes", "calls", "edges"]
)
def test_closure_budgets(composition, limit):
    *_, inspect = composition
    result = inspect(limits={limit: 1})
    assert result["status"] == "incomplete", result


def test_unknown_native_semantics_are_not_guessed(composition, catalog):
    root, _, _, tables, args, _ = composition
    for semantics in (
        {"status": "unknown"},
        {
            "format": "demiflow_api_flow_v1",
            "kind": "new_unhandled_kind",
            "datasets": ["self"],
            "callable": None,
            "effects": [],
            "execution": "lazy",
        },
    ):
        changed = copy.deepcopy(catalog)
        for entry in changed["objects"]:
            if entry["id"].endswith("Dataset.map"):
                entry["native_semantics"] = semantics
        result = FlowChecker(root, changed, tables, [args["target"]["uri"]]).inspect(
            "flows/questions.py:select_a", args
        )
        assert (
            result["status"] == "incomplete"
            and result["diagnostics"][0]["rule"] == "FL-02"
        )


def test_catalog_only_alias_does_not_grant_native_authority(composition, catalog):
    root, flow, _, tables, args, _ = composition
    changed = copy.deepcopy(catalog)
    original = next(x for x in changed["objects"] if x["id"].endswith("Dataset.map"))
    alias = copy.deepcopy(original)
    alias["id"] = alias["id"].removesuffix("map") + "transform"
    alias["defined_name"] = "transform"
    changed["objects"].append(alias)
    flow.write_text(flow.read_text().replace(".map(", ".transform("))
    result = FlowChecker(root, changed, tables, [args["target"]["uri"]]).inspect(
        "flows/questions.py:select_a", args
    )
    assert result["status"] == "incomplete", result["diagnostics"]
    assert result["diagnostics"][0]["rule"] == "FL-02"


def test_actor_helper_source_and_lifecycle_stream_boundary(tmp_path, catalog):
    operator = """from demiflow import data
class op:
    def __init__(self, offset=1):
        self.offset = offset
    def helper(self, x: int) -> int:
        return x + self.offset
    def __call__(self, x: int) -> int:
        return self.helper(x)
    async def aclose(self) -> None:
        pass
"""
    flow = """from demiflow import data
from operators.rules import op
def run():
    source = data.read_lance("/fixture/source.lance",version=1)
    result = source.map(op,inputs={"x":"x"},output="value",concurrency=1,queue_depth=1,callable_scope="worker",catch=(ValueError,)).materialize()
    return result.write_lance("/fixture/out.lance",mode="create",return_receipt=True)
"""
    good = simple_flow(tmp_path, catalog, flow, operator)
    assert good["status"] == "checked", good["diagnostics"]
    assert any("drop_and_count_declared_errors" in n["effects"] for n in good["nodes"])
    assert good["coverage"]["resource_runtime"] == "not_run"
    direct = simple_flow(tmp_path, catalog, flow.replace(".materialize()", ""), operator)
    assert direct["status"] == "checked", direct["diagnostics"]
    for old, new, rule in [
        ("concurrency=1", "concurrency=0", "OP-07"),
        ("catch=(ValueError,)", "catch=(BaseException,)", "FL-01"),
    ]:
        bad = simple_flow(tmp_path, catalog, flow.replace(old, new), operator)
        assert (
            bad["status"] == "invalid" and bad["diagnostics"][0]["rule"] == rule
        ), bad["diagnostics"]
    hidden = simple_flow(
        tmp_path,
        catalog,
        flow,
        operator.replace(
            "return x + self.offset", 'data.read_lance("hidden")\n        return x'
        ),
    )
    assert hidden["status"] == "invalid" and hidden["diagnostics"][0]["rule"] == "OP-04"


@pytest.mark.parametrize(
    "probe",
    [
        'VALUE: open("probe", "w") = 1\n',
        'def unused(*args: open("probe", "w")):\n    pass\n',
        'class Hook:\n    def __class_getitem__(cls, name):\n        open("probe", "w")\ndef unused(x: Hook[int]):\n    pass\n',
    ],
)
def test_annotation_initialization_protocols_blocked(composition, probe):
    root, flow, *_, inspect = composition
    flow.write_text(probe + flow.read_text())
    result = inspect()
    assert (
        result["status"] in {"invalid", "incomplete"}
        and result["diagnostics"][0]["rule"] == "OP-06"
    )
    assert not (root / "probe").exists()


def test_module_function_alias_preserves_definition(composition):
    _, flow, op, _, _, inspect = composition
    op.write_text(op.read_text() + "\nnormalizer = normalize_question\n")
    text = (
        flow.read_text()
        .replace("    normalize_question,", "    normalizer,")
        .replace(".map(\n        normalize_question,", ".map(\n        normalizer,")
    )
    assert text != flow.read_text()
    flow.write_text(text)
    result = inspect()
    assert result["status"] == "checked", result["diagnostics"]


def test_async_shape_and_checkpoint_effect_are_explicit(tmp_path, catalog):
    operator = 'from typing import TypedDict\nclass Row(TypedDict):\n    x: int\nasync def op(row: Row) -> Row:\n    return {"x": row["x"]}\n'
    flow = 'from demiflow import data\nfrom operators.rules import op\ndef run():\n    result = data.read_lance("/fixture/source.lance",version=1).map_async(op).materialize()\n    return result.write_lance("/fixture/out.lance",mode="create",return_receipt=True)\n'
    good = simple_flow(tmp_path, catalog, flow, operator)
    assert good["status"] == "checked", good["diagnostics"]
    bad = simple_flow(
        tmp_path,
        catalog,
        flow.replace("map_async(op)", 'map_async(op,checkpoint={"path":"hidden"})'),
        operator,
    )
    assert bad["status"] == "incomplete" and bad["diagnostics"][0]["rule"] == "FL-07"


def test_output_cannot_be_silently_relocated(composition):
    root,_,_,_,_,inspect=composition
    directory=root/'_demiflow';directory.mkdir()
    (directory/'lance_locations.json').write_text(json.dumps({'version':1,'tables':{'selected.lance':'other.lance'}}))
    result=inspect()
    assert result['status']=='invalid' and result['diagnostics'][0]['rule']=='FL-09',result
    assert 'physical target' in result['diagnostics'][0]['detail']
