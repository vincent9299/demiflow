from operators.normalize_text import normalize_text


def test_trim_and_uppercase_preserves_identity():
    assert normalize_text({"id": 7, "text": "  Snow Leopard  "}, uppercase=True) == {
        "id": 7,
        "text": "SNOW LEOPARD",
    }


def test_can_preserve_case_and_accept_empty_text():
    assert normalize_text({"id": 8, "text": " Mixed Case "}, uppercase=False) == {
        "id": 8,
        "text": "Mixed Case",
    }
    assert normalize_text({"id": 9, "text": "   "}, uppercase=True) == {"id": 9, "text": ""}
