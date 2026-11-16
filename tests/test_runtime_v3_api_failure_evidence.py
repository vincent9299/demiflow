"""Failure reporting cannot discard the evidence behind a red API matrix."""
from types import SimpleNamespace

import pytest


def test_failed_matrix_exports_original_case_details_before_raising(monkeypatch):
    import runtime_v3_api_scenarios
    from runtime_v3_support import _s_api_matrix

    failure = {"type": "ModuleNotFoundError", "message": "missing-dependency"}
    report = {
        "api_count": 98,
        "api": {str(index): {} for index in range(98)},
        "failed": {"Dataset.parse_wikitext": {"failed": ["wiki-native"]}},
        "cases": [{"name": "wiki-native", "status": "failed", "error": failure,
                   "traceback": "the original worker traceback"}],
    }
    saved = []
    monkeypatch.setattr(runtime_v3_api_scenarios, "api_matrix", lambda *_: report)
    fixture = SimpleNamespace(provider="local", _save_evidence=lambda name, value: saved.append((name, value)))
    with pytest.raises(AssertionError, match="wiki-native"):
        _s_api_matrix(fixture, None)
    assert saved == [("api_matrix", report)]
    assert saved[0][1]["cases"][0]["error"] == failure
    assert saved[0][1]["cases"][0]["traceback"] == "the original worker traceback"
