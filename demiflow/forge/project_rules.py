"""Packaged authoring rules and independent source-layout diagnostics."""
from hashlib import sha256
from pathlib import Path
import re

from .platform import content_digest
from .platform_environment import read_bounded
from .schema import ForgeError


ROLE_DIRS = ('flows', 'operators', 'prompts', 'configs', 'data_tools', 'notebooks')
TOPICS = ('flow', 'operator', 'prompt')
GUIDE_RESOURCE = 'standards/pipeline-authoring.md'


def _read_guide():
    path = Path(__file__).parent / GUIDE_RESOURCE
    raw = read_bounded(path, 64 * 1024)
    sections = re.split(r'^## (\w+)\s*$', raw.decode(), flags=re.MULTILINE)
    if tuple(sections[1::2]) != ('core', *TOPICS):
        raise ForgeError('Authoring guide has invalid topic headings')
    rules, notes = [], []
    for topic, body in zip(sections[1::2], sections[2::2]):
        items = re.split(r'^### ([^\n]+)$', body, flags=re.MULTILINE)
        for heading, text in zip(items[1::2], items[2::2]):
            source = f'{GUIDE_RESOURCE}#{topic}'
            if heading == '建议与示例':
                notes.append(dict(topic=topic, text=text.strip(), source=source))
            elif re.fullmatch(r'DF-\d{2}', heading):
                if any(rule['id'] == heading for rule in rules):
                    raise ForgeError('Duplicate authoring rule: ' + heading)
                rules.append(dict(topic=topic, id=heading, text=text.strip(), source=source))
            else:
                raise ForgeError('Unknown authoring guide heading: ' + heading)
    return rules, notes, sha256(raw).hexdigest()


_RULES, _NOTES, GUIDE_DIGEST = _read_guide()
RULES_ID = content_digest([(item['id'], item['text']) for item in _RULES])


def guidance(*, topics=()):
    selected = {'core', *topics}
    if selected.difference(('core', *TOPICS)):
        raise ForgeError('Unknown authoring topic', code='invalid_request')
    return ([dict(item) for item in _RULES if item['topic'] in selected],
            [dict(item) for item in _NOTES if item['topic'] in selected])


def diagnostic(rule, path, detail, *, line=None, status='invalid'):
    return dict(rule=rule, path=path, line=line, detail=detail, status=status)


def check_layout(*, changed, operators, prompts):
    """Check known roles, including actual operator/prompt use reported by the checker."""
    diagnostics, inspected = [], set()
    for path in changed:
        if Path(path).parts[0] in ROLE_DIRS:
            inspected.add(path)
    roles = {
        'operators': operators,
        'prompts': prompts,
    }
    for directory, paths in roles.items():
        for path, line in paths:
            inspected.add(path)
            if Path(path).parts[:1] != (directory,):
                diagnostics.append(diagnostic('DF-02', path,
                    f'此文件的角色要求放在 {directory}/。', line=line))
    return inspected, diagnostics
