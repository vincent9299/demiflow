"""Rich values retain types; active HTML and guessed media never reach the UI."""
from html.parser import HTMLParser

import pytest

from demiflow.workbench.analysis import outputs
from demiflow.forge.schema import ForgeError


def test_html_allowlist_keeps_readable_tables_and_blocks_all_active_attributes():
    source = '''<base href="https://bad.test"><style>body{display:none}</style>
    <div onclick="alert(1)" style="background:url(https://bad.test)">
    <h2>结果 &amp; 说明</h2><table><tr><td>1&lt;2</td></tr></table>
    <script>fetch('/api/projects')</script><img src="https://bad.test/x" onerror="alert(1)">
    <a href="javascript:alert(1)" target="_top">链接文字</a>
    <iframe srcdoc="evil"></iframe><svg><script>bad()</script></svg></div>'''
    result = outputs.html_preview(source)
    assert result["source"] == source and result["sandbox"] == "" and not result["active_content"]
    assert result["blocked_media"] is True
    assert "<table>" in result["preview"] and "结果 &amp; 说明" in result["preview"]

    class Inspect(HTMLParser):
        def handle_starttag(self, tag, attrs):
            if tag == "meta":
                assert dict(attrs).get("charset") == "utf-8" or dict(attrs).get("http-equiv") == "Content-Security-Policy"
            else:
                assert tag in outputs._TAGS and attrs == []

    Inspect().feed(result["preview"])
    assert "fetch(" not in result["preview"] and "bad.test" not in result["preview"]
    assert "script-src 'none'" in outputs.SANDBOX_CSP and "form-action 'none'" in outputs.SANDBOX_CSP


def test_html_limits_and_format_mismatch_are_explicit():
    with pytest.raises(ForgeError) as error:
        outputs.html_preview("<div>"*33)
    assert error.value.code == "resource_limit"
    with pytest.raises(ForgeError):
        outputs.html_preview("&"*(outputs.HTML_BYTES//4))
    invalid = outputs.field_preview("dialogue", "not a message list")
    assert invalid["compatible"] is False and invalid["value"] == "not a message list"
    invalid = outputs.field_preview("image", "/home/private/image.png")
    assert invalid["format"] == "json" and invalid["compatible"] is False


def test_messages_and_opaque_assets_have_bounded_typed_shapes():
    asset = {"artifact_id": "o"+"a"*32, "sha256": "a"*64, "mime": "image/png", "bytes": 1024, "width": 640, "height": 480}
    dialogue = [{"role": "user", "content": [{"format": "text", "value": "看这个"}, {"format": "image", "value": asset}]},
                {"role": "assistant", "content": [{"format": "text", "value": "回复"}]}]
    assert outputs.field_preview("dialogue", dialogue)["compatible"] is True
    for change in ({"uri": "/tmp/x"}, {"width": 8192, "height": 8192}, {"mime": "image/svg+xml"}):
        with pytest.raises(ForgeError):
            outputs.asset({**asset, **change}, "image")
    for role in ("admin", [], None):
        with pytest.raises(ForgeError):
            outputs.message({"role": role, "content": []})


@pytest.mark.parametrize("format_name,value", [("text", "text"), ("md", "# title"), ("code", "print(1)"),
    ("json", {"v": [None, False, 1]}), ("tag", ["good", "needs-review"]), ("html", "<p>ok</p>")])
def test_common_formats_preserve_the_source_value(format_name, value):
    result = outputs.field_preview(format_name, value)
    assert result.get("source", result.get("value")) == value
