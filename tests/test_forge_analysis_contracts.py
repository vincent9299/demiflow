"""Public document behavior and canonical-budget rejection before encoding."""
import copy
import hashlib
import json
import math
from unittest.mock import patch

import pytest

from demiflow.workbench.analysis import contracts as c
from demiflow.forge.schema import ForgeError


@pytest.mark.parametrize("value", [None, False, True, 0, -2**63, 2**63-1, -0.0, 1e-300,
    '\x00\x01\b\f\n\r\t"\\', "中文🙂é", [], {}, [False, {"引号\"": [None, False, "a\\b"]}]])
def test_canonical_budget_includes_escaping_unicode_and_containers(value):
    expected = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    assert c.encoded_size(value, maximum=len(expected)) == len(expected)
    assert c.encoded(value, maximum=len(expected)) == expected
    assert c.digest(value) == hashlib.sha256(expected).hexdigest()
    # Rejection must happen before final encoding, including one-byte edges.
    with patch.object(c.json, "dumps", side_effect=AssertionError("encoder must not run")):
        with pytest.raises(ForgeError, match="byte limit"):
            c.encoded(value, maximum=len(expected)-1)


def test_control_character_expansion_rejected_before_final_allocation():
    with patch.object(c.json, "dumps", side_effect=AssertionError("encoder must not run")):
        with pytest.raises(ForgeError) as error:
            c.encoded("\x01" * 65534)
    assert error.value.code == "resource_limit"


@pytest.mark.parametrize("value", [float("nan"), float("inf"), 2**63, object(), {1: "x"}, "\ud800"])
def test_non_json_or_nonportable_values_are_rejected(value):
    with pytest.raises(ForgeError):
        c.encoded(value)


def test_cycles_depth_and_duplicate_keys_are_rejected():
    cycle = []; cycle.append(cycle)
    with pytest.raises(ForgeError):
        c.encoded(cycle)
    value = "leaf"
    for _ in range(17):
        value = [value]
    with pytest.raises(ForgeError):
        c.encoded(value)
    for data in (b'{"a":1,"a":2}', b'{"x":NaN}', b'"\\ud800"'):
        with pytest.raises(ForgeError):
            c.decode(data)


def test_notebook_roundtrip_is_source_only_and_sql_semantics_are_explicit():
    document = {"title": "分析", "cells": [
        {"id": "python-1", "language": "python", "source": "x = 1\nprint(x)"},
        {"id": "sql-1", "language": "sql", "source": "select * from orders"},
        {"id": "notes", "language": "markdown", "source": "# 记录"}]}
    exported = c.export_notebook(document)
    assert exported["nbformat"] == 4 and exported["nbformat_minor"] == 5
    assert exported["cells"][1]["source"].startswith("from demiflow.workbench.analysis.client import sql")
    for item in exported["cells"]:
        if item["cell_type"] == "code":
            item["outputs"] = [{"output_type": "display_data", "data": {"text/html": "<script>bad()</script>"}, "metadata": {}}]
            item["execution_count"] = 99
    original = copy.deepcopy(exported)
    assert c.import_notebook(exported, title="分析") == document
    assert exported == original  # no mutation of the imported file/outputs
    edited = c.notebook_patch(document, {"put_cells": [{"id": "sql-1", "language": "sql", "source": "select count(*) from orders"}], "order": ["notes", "sql-1", "python-1"]})
    assert edited["cells"][1]["id"] == "sql-1" and document["cells"][1]["source"] == "select * from orders"
    with pytest.raises(ForgeError):
        c.notebook_patch(document, {"order": ["python-1"]})


@pytest.mark.parametrize("bad", [[], {}, 4, True, None])
def test_bad_cell_language_returns_contract_error_instead_of_type_error(bad):
    with pytest.raises(ForgeError):
        c.cell({"id": "one", "language": bad, "source": "x"})


def test_question_changes_have_independent_identities_and_typed_answers():
    question = {"id": "quality", "title": "可用吗", "type": "choice", "required": True,
                "options": [{"id": "yes", "label": "可用"}, {"id": "no", "label": "不可用"}]}
    changed = {**question, "title": "图像可用吗"}
    assert c.digest(question) != c.digest(changed)
    assert c.answer(question, "yes") == "yes"
    for value in (None, "unknown", [], True):
        with pytest.raises(ForgeError):
            c.answer(question, value)
    rating = {"id": "score", "title": "评分", "type": "rating", "required": False, "minimum": 1, "maximum": 5}
    for value in (True, 0, 6, 3.5):
        with pytest.raises(ForgeError):
            c.answer(rating, value)


def test_typed_filters_reject_raw_expressions_unknown_fields_and_type_confusion():
    schema = {"text": {"type": "utf8"}, "score": {"type": "float64"}, "count": {"type": "int64"}}
    valid = {"op": "and", "items": [{"op": "contains", "field": "text", "value": "quote'"},
            {"op": "range", "field": "score", "value": {"lower": 0, "upper": 1}}]}
    assert c.typed_filter(valid, schema) == valid
    for item in [{"op": "sql", "field": "text", "value": "1=1"},
                 {"op": [], "field": "text", "value": "x"},
                 {"op": "eq", "field": "missing", "value": 1},
                 {"op": "eq", "field": "count", "value": True},
                 {"op": "range", "field": "score", "value": {"lower": 2, "upper": 1}}]:
        with pytest.raises(ForgeError):
            c.typed_filter(item, schema)
