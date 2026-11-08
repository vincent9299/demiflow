"""Bounded wikitext syntax records; no template or lead-selection policy."""


def parse_wikitext(text, *, max_chars=262144, max_nodes=16384, max_depth=32, max_bytes=8 * 1024**2):
    import mwparserfromhell
    from mwparserfromhell.nodes import Text, Comment, HTMLEntity, Template, Wikilink, ExternalLink, Tag
    from ..data.runtime_contract import check_size
    if type(text) is not str or len(text) > max_chars:
        raise ValueError('Wikitext exceeds character budget')
    # The parser's own temporaries live within the enclosing worker RSS/time
    # guard. Record count/depth and retained output bytes have separate bounds.
    parsed = mwparserfromhell.parse(text)
    nodes, consumed = [], 0

    def code(value, depth):
        nonlocal consumed
        if depth > max_depth:
            raise ValueError('Wikitext syntax exceeds depth budget')
        result = []
        for node in value.nodes:
            if len(nodes) >= max_nodes:
                raise ValueError('Wikitext syntax exceeds node budget')
            item = {'id': len(nodes), 'kind': type(node).__name__, 'text': '', 'name': '',
                    'children': [], 'title': [], 'label': [], 'has_label': False, 'parameters': []}
            nodes.append(item)
            result.append(item['id'])
            if type(node) is Text:
                item['text'] = str(node)
            elif type(node) is HTMLEntity:
                item['text'] = node.normalize()
            elif type(node) is Template:
                item['name'] = str(node.name)
                if len(node.params) > max_nodes:
                    raise ValueError('Wikitext parameters exceed node budget')
                for p in node.params:
                    parameter = {'name': str(p.name), 'raw': str(p.value), 'value': []}
                    consumed += check_size(parameter, max_bytes)
                    if consumed > max_bytes:
                        raise ValueError('Wikitext syntax exceeds output byte budget')
                    parameter['value'] = code(p.value, depth + 1)
                    item['parameters'].append(parameter)
            elif type(node) is Wikilink:
                item['name'] = str(node.title)
                item['title'] = code(node.title, depth + 1)
                item['has_label'] = node.text is not None
                if node.text is not None:
                    item['label'] = code(node.text, depth + 1)
            elif type(node) is ExternalLink:
                item['has_label'] = node.title is not None
                if node.title is not None:
                    item['label'] = code(node.title, depth + 1)
            elif type(node) is Tag:
                item['name'] = str(node.tag)
                if node.contents is not None:
                    item['children'] = code(node.contents, depth + 1)
            elif type(node) is Comment:
                pass
            # Unknown syntax remains identifiable; policy may ignore or reject.
            # It never becomes an executable third-party object in a row.
            consumed += check_size(item, max_bytes)
            if consumed > max_bytes:
                raise ValueError('Wikitext syntax exceeds output byte budget')
        return result

    roots = code(parsed, 0)
    output = {'roots': roots, 'nodes': nodes}
    check_size(output, max_bytes)
    return output


class ParseWikitext:
    def __init__(self, **arguments):
        from .flow_contract import normalize_wiki
        self.arguments = normalize_wiki(arguments)

    def snapshot_state(self):
        # The parser and syntax-node counters are local to one input string.
        # The immutable limits are already part of the native plan identity.
        return {}

    def restore_state(self, state):
        if type(state) is not dict or state:
            raise ValueError('Wikitext parser has no mutable semantic state')

    def __call__(self, row):
        from .flow_contract import row_result
        a = self.arguments
        text = row[a['column']]
        if text is None:
            return row_result(row, a, None)
        try:
            value = parse_wikitext(text, **{k: a[k] for k in ('max_chars', 'max_nodes', 'max_depth', 'max_bytes')})
        except (ValueError, TypeError, RecursionError) as error:
            return row_result(row, a, None, error)
        return row_result(row, a, value)
