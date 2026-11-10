"""Typed output descriptions and an HTML allowlist; never execute rich output."""
from __future__ import annotations

from html import escape
from html.parser import HTMLParser

from demiflow.workbench.analysis import contracts as c

HTML_BYTES = 256*1024
_TAGS = frozenset({"p", "br", "hr", "strong", "b", "em", "i", "u", "s", "code", "pre",
                   "ul", "ol", "li", "table", "thead", "tbody", "tfoot", "tr", "th", "td",
                   "blockquote", "span", "div", "h1", "h2", "h3", "h4", "h5", "h6",
                   "details", "summary", "a"})
_VOID = frozenset({"br", "hr"})
_DROP_CONTENT = frozenset({"script", "style", "iframe", "object", "template", "svg", "math",
                           "textarea", "noscript", "form", "audio", "video", "canvas"})
_ESCAPE_EXTRA = {"&": 4, "<": 3, ">": 3, '"': 5, "'": 5}
SANDBOX_CSP = "default-src 'none'; script-src 'none'; style-src 'none'; img-src 'none'; media-src 'none'; connect-src 'none'; form-action 'none'; base-uri 'none'"


class _HTML(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.parts = []
        self.stack = []
        self.blocked = []
        self.nodes = 0
        self.bytes = 0
        self.blocked_media = False

    def emit(self, value):
        self.bytes += len(value.encode("utf-8"))
        if self.bytes > HTML_BYTES:
            c.reject("HTML preview exceeds output byte limit", code="resource_limit")
        self.parts.append(value)

    def node(self):
        self.nodes += 1
        if self.nodes > 4096 or len(self.stack) + len(self.blocked) > 32:
            c.reject("HTML exceeds node/depth limit", code="resource_limit")

    def handle_starttag(self, tag, attrs):
        self.node()
        if tag in {"img", "audio", "video", "source", "picture"}:
            self.blocked_media = True
        if self.blocked or tag in _DROP_CONTENT:
            if tag in _DROP_CONTENT:
                if len(self.blocked) + len(self.stack) >= 32:
                    c.reject("HTML exceeds depth limit", code="resource_limit")
                self.blocked.append(tag)
            return
        if tag not in _TAGS:
            return
        # No URLs, user style/class/id, event attributes, forms or embedded
        # assets. Typed media is rendered by the separate authorized component.
        self.emit("<" + tag + ">")
        if tag not in _VOID:
            if len(self.stack) >= 32:
                c.reject("HTML exceeds depth limit", code="resource_limit")
            self.stack.append(tag)

    def handle_startendtag(self, tag, attrs):
        self.handle_starttag(tag, attrs)
        self.handle_endtag(tag)

    def handle_endtag(self, tag):
        self.node()
        if self.blocked:
            if tag in self.blocked:
                del self.blocked[len(self.blocked) - 1 - self.blocked[::-1].index(tag):]
            return
        if tag in self.stack:
            while self.stack:
                current = self.stack.pop()
                self.emit("</" + current + ">")
                if current == tag:
                    break

    def handle_data(self, value):
        self.node()
        if self.blocked:
            return
        # Count escaping before html.escape allocates. UTF-8 expansion is
        # exact, and only the five HTML metacharacters grow further.
        extra = sum(_ESCAPE_EXTRA.get(char, 0) for char in value)
        if self.bytes + len(value.encode("utf-8")) + extra > HTML_BYTES:
            c.reject("HTML preview exceeds output byte limit", code="resource_limit")
        self.emit(escape(value, quote=True))


def sanitized_html(source, *, _report=False):
    source = c.text(source, "HTML source", HTML_BYTES, empty=True)
    parser = _HTML()
    parser.feed(source)
    parser.close()
    while parser.stack:
        parser.emit("</" + parser.stack.pop() + ">")
    body = "".join(parser.parts)
    return (body, parser.blocked_media) if _report else body


def html_preview(source):
    """A trusted projection; the UI must still set sandbox='' on its iframe."""
    body, blocked_media = sanitized_html(source, _report=True)
    prefix = '<!doctype html><meta charset="utf-8"><meta http-equiv="Content-Security-Policy" content="' + escape(SANDBOX_CSP, quote=True) + '">'
    return dict(format="html", source=source, preview=prefix + body,
                sandbox="", referrer_policy="no-referrer", active_content=False, blocked_media=blocked_media,
                embedded_media="use typed authorized media fields")


def message(value, *, dialogue=False):
    items = c.sequence(value, "dialogue turns", 32) if dialogue else [value]
    for item in items:
        c.fields(item, {"role", "content"}, label="message")
        c.one_of(item["role"], {"system", "user", "assistant", "tool"}, "message role")
        if type(item["content"]) is str:
            c.text(item["content"],"message text",16384,empty=True)
            continue
        for part in c.sequence(item["content"], "message parts", 8):
            c.fields(part, {"format", "value"}, label="message part")
            c.one_of(part["format"], {"text", "image", "audio", "video"}, "message content format")
            if part["format"] == "text":
                c.text(part["value"], "message text", 16384, empty=True)
            else:
                asset(part["value"], part["format"])
    c.encoded(value, maximum=256*1024)
    return value


def asset(value, kind):
    """Validate an opaque descriptor; the serving route must authorize again."""
    c.one_of(kind, {"image", "audio", "video"}, "media format")
    c.fields(value, {"artifact_id", "sha256", "mime", "bytes"}, {"width", "height", "duration_s", "codec", "fps", "output_index"}, label="media reference")
    c.identity(value["artifact_id"], "media artifact ID")
    c.sha(value["sha256"], "media digest")
    c.text(value["mime"], "media MIME", 128)
    limits = {"image": 8*1024*1024, "audio": 16*1024*1024, "video": 64*1024*1024}
    c.integer(value["bytes"], "media bytes", limits[kind], 1)
    allowed_mime = {"image": {"image/png", "image/jpeg", "image/webp", "image/gif"},
                    "audio": {"audio/wav", "audio/mpeg", "audio/ogg", "audio/webm"},
                    "video": {"video/mp4", "video/webm", "video/ogg"}}
    c.one_of(value["mime"], allowed_mime[kind], "media MIME")
    if "output_index" in value:
        c.integer(value["output_index"], "media output index", 255)
    if "codec" in value:
        c.text(value["codec"], "media codec", 128)
    if "fps" in value:
        fps = value["fps"]
        if type(fps) not in {int, float} or not 0 < fps <= 30:
            c.reject("Video frame-rate limit exceeded", code="resource_limit")
    for name in ("width", "height"):
        if name in value:
            c.integer(value[name], name, 8192, 1)
    if kind == "image" and "width" in value and "height" in value and value["width"]*value["height"] > 4*1024*1024:
        c.reject("Image pixel limit exceeded", code="resource_limit")
    if kind == "video" and "width" in value and "height" in value and (value["width"] > 1920 or value["height"] > 1080):
        c.reject("Video dimensions exceed 1080p", code="resource_limit")
    if "duration_s" in value:
        duration = value["duration_s"]
        maximum = 600 if kind == "audio" else 300
        if type(duration) not in {int, float} or not 0 <= duration <= maximum:
            c.reject("Media duration limit exceeded", code="resource_limit")
    return value


def field_preview(format_name, value):
    """Type mismatch has an explicit text fallback, never guessed execution."""
    c.one_of(format_name, c.FORMATS, "field preview format")
    try:
        if value is None:
            return dict(format=format_name, value=None, compatible=True, null=True)
        if format_name == "html":
            return html_preview(value)
        if format_name in {"message", "dialogue"}:
            message(value, dialogue=format_name == "dialogue")
        elif format_name in {"image", "audio", "video"}:
            asset(value, format_name)
        elif format_name == "tag":
            for tag in c.sequence(value, "data tags", 64):
                c.text(tag, "data tag", 256)
        elif format_name == "json":
            c.encoded(value, maximum=256*1024, depth=8)
        else:
            c.text(value, "field preview", 256*1024, empty=True)
        return dict(format=format_name, value=value, compatible=True)
    except ValueError as exc:
        if getattr(exc, "code", None) == "resource_limit":
            raise
        # Preserve the value as native JSON for an accurately escaped raw view.
        c.encoded(value, maximum=256*1024, depth=8)
        return dict(format="json", requested_format=format_name, value=value,
                    compatible=False, reason="value_does_not_match_declared_format")


OUTPUT_MIME = "application/vnd.demiflow.output+json"
CASE_MIME = "application/vnd.demiflow.case-view+json"
TABLE_MIME = "application/vnd.demiflow.table+json"
MESSAGE_MIME = "application/vnd.demiflow.message+json"
DIALOGUE_MIME = "application/vnd.demiflow.dialogue+json"
MIME_FORMATS = {"text/plain": "text", "text/markdown": "md", "text/html": "html",
                "application/json": "json", "image/png": "image", "image/jpeg": "image",
                "image/webp": "image", "image/gif": "image",
                "audio/wav": "audio", "audio/mpeg": "audio", "audio/ogg": "audio", "audio/webm": "audio",
                "video/mp4": "video", "video/webm": "video", "video/ogg": "video",
                MESSAGE_MIME: "message", DIALOGUE_MIME: "dialogue", CASE_MIME: "case", TABLE_MIME: "table"}
DISPLAY_BYTES = 16*1024


def table_reference(value):
    """Only an immutable registered source, never a Python/SQL/file expression."""
    if type(value) is dict and "id" in value:
        # Existing client.TableHandle MIME; preserve producer compatibility.
        c.fields(value, {"id", "generation", "digest", "rows", "schema", "retention"}, label="table handle MIME")
        c.identity(value["id"]); c.integer(value["generation"], "table generation", 2**31-1, 1); c.sha(value["digest"])
        c.integer(value["rows"], "table rows", 2**63-1); c.sequence(value["schema"], "table schema", 256)
        c.one_of(value["retention"], {"ephemeral", "pinned", "external"}, "table retention")
        return value
    c.fields(value, {"version", "source_snapshot"}, {"columns", "label"}, label="table output reference")
    if type(value["version"]) is not int or value["version"] != 1:
        c.reject("Unsupported table reference version")
    c.sha(value["source_snapshot"], "table output snapshot")
    if "columns" in value:
        for column in c.sequence(value["columns"], "table output columns", 16):
            c.text(column, "table output column", 256)
    if "label" in value:
        c.text(value["label"], "table output label", 256)
    return value


def case_reference(value, source):
    # The existing versioned view schema stays canonical. Origin is owned by
    # the output reader; a producer cannot assert a saved view or annotation.
    c.case_view(value, source)
    return value


def _bounded_value(value):
    size = c.encoded_size(value, maximum=16*1024*1024, nodes=32768, depth=16)
    if size <= DISPLAY_BYTES:
        return dict(value=value, truncated=False)
    if type(value) is str:
        # A codepoint prefix cannot cut a multibyte character. The extra bound
        # includes JSON escaping; reduce before encoding the resulting view.
        text = value[:DISPLAY_BYTES//6]
        return dict(value=text, truncated=True, encoded_bytes=size,
                    reason="preview_prefix_only_use_original_artifact")
    return dict(truncated=True, encoded_bytes=size,
                reason="structured_value_exceeds_preview_budget_use_original_artifact")


def _browser_needs_exact_json(value):
    if type(value) is int:
        return abs(value) > 2**53-1
    if type(value) is list:
        return any(_browser_needs_exact_json(item) for item in value)
    if type(value) is dict:
        return any(_browser_needs_exact_json(item) for item in value.values())
    return False


def mime_representations(data, *, media_reference=None, source_resolver=None, table_resolver=None):
    """Project an already bounded MIME bundle; never parse code or fetch URLs.

    This runs in the media/output job, not the Web controller. Original bytes
    remain in the immutable output artifact. No giant base64 is echoed to UI.
    """
    if type(data) is not dict or len(data) > 16:
        c.reject("Invalid output MIME bundle")
    result = []
    for mime, value in data.items():
        c.text(mime, "output MIME", 128)
        format_name = MIME_FORMATS.get(mime)
        item = dict(mime=mime, format=format_name or "unsupported")
        try:
            if format_name in {"image", "audio", "video"}:
                if not callable(media_reference):
                    item.update(available=False, reason="authorized_media_reader_unavailable")
                else:
                    item.update(value=media_reference(mime, value), available=True)
            elif format_name in {"case", "table"}:
                if format_name == "table" and type(value) is dict and "id" in value:
                    table_reference(value)
                    if not callable(table_resolver) or table_resolver(value) != value:
                        c.reject("Table output handle is not registered for this execution generation", code="operation_rejected")
                else:
                    if not callable(source_resolver):
                        c.reject("Output source is not registered", code="operation_rejected")
                    source = source_resolver(value.get("source_snapshot") if type(value) is dict else None)
                    if format_name == "case":
                        case_reference(value, source)
                    else:
                        table_reference(value)
                        if source["digest"] != value["source_snapshot"] or any(column not in {item["name"] for item in source["schema"]} for column in value.get("columns", [])):
                            c.reject("Table output source/projection differs", code="conflict")
                c.encoded_size(value, maximum=32*1024)
                item.update(value=value, available=True)
            elif format_name is None:
                item.update(_bounded_value(value), available=False,
                            reason="unsupported_MIME_no_widgets_or_scripts")
            else:
                projection = _bounded_value(value)
                item.update(projection)
                # HTML source gets the same existing allowlist; even its
                # projection is never inserted into the application DOM.
                if projection["truncated"]:
                    item.update(available=False)
                else:
                    rendered = field_preview(format_name, value)
                    item.update(available=rendered.get("compatible", True),
                                format=rendered["format"])
                    if format_name == "json" and _browser_needs_exact_json(value):
                        # Browser JSON.parse rounds int64 above 2**53. Preserve
                        # exact typed JSON source, never send a rounded value.
                        item.pop("value", None)
                        item["raw_json"] = c.encoded(value, maximum=DISPLAY_BYTES, depth=8).decode("utf-8")
                        item["reason"] = "integer_outside_browser_exact_range_show_original_JSON"
                    if rendered["format"] == "html":
                        item["preview"] = rendered
                    if rendered.get("reason"):
                        item["reason"] = rendered["reason"]
        except ValueError as error:
            # A bad alternative cannot hide a valid text/plain representation.
            item.update(available=False, reason=getattr(error, "reason", getattr(error, "code", "invalid_representation")))
            if format_name not in {"image", "audio", "video"}:
                item.update(_bounded_value(value))
        result.append(item)
    order = {CASE_MIME: 0, TABLE_MIME: 1, "application/json": 2, "image/png": 3,
             "image/jpeg": 3, "image/webp": 3, "image/gif": 3, "text/markdown": 4,
             "text/html": 5, "text/plain": 6}
    result.sort(key=lambda item: order.get(item["mime"], 7))
    return result
