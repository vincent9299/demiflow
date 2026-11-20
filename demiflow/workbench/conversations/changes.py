"""Bounded source previews supplement full-byte hashes, never truncate results."""
import difflib
from pathlib import Path

from demiflow.workbench.conversations.common import read_file
from demiflow.workbench.conversations.common import safe_path
from demiflow.workbench.conversations.common import snapshot

TEXT_SUFFIXES = {'.py', '.md', '.json', '.yaml', '.yml', '.toml', '.js', '.css', '.html', '.txt'}


def capture(root, paths, *, exclude=()):
    facts = snapshot(root, paths, exclude=exclude)
    remaining = 128 * 1024
    for name, fact in facts.items():
        if fact is None: continue
        if Path(name).suffix not in TEXT_SUFFIXES:
            fact['preview_unavailable'] = 'not_plain_source'
        elif fact['bytes'] > 16384 or fact['bytes'] > remaining:
            fact['preview_unavailable'] = 'preview_byte_budget'
        else:
            try:
                raw = read_file(safe_path(root, name), 16384)
                import hashlib
                if hashlib.sha256(raw).hexdigest() != fact['sha256']:
                    from demiflow.workbench.conversations.common import reject
                    reject('Source changed between hash and preview', 'conflict')
                value = raw.decode('utf-8')
                if len(value.splitlines()) > 512:
                    fact['preview_unavailable'] = 'preview_line_budget'
                else:
                    fact['preview'] = value; remaining -= len(raw)
            except UnicodeError:
                fact['preview_unavailable'] = 'not_utf8'
    return facts


def differences(before, after, writes):
    for name in sorted(before.keys() | after.keys()):
        old, new = before.get(name), after.get(name)
        if (old or {}).get('sha256') == (new or {}).get('sha256'): continue
        value = dict(path=name, before={k:v for k,v in (old or {}).items() if k!='preview'} if old else None,
            after={k:v for k,v in (new or {}).items() if k!='preview'} if new else None,
            within_scope=any(name==w or name.startswith(w+'/') for w in writes))
        if (old is None or 'preview' in old) and (new is None or 'preview' in new):
            # <=512 lines each side and <=32 KiB combined source input.
            chunks = list(difflib.unified_diff((old or {}).get('preview','').splitlines(keepends=True),
                          (new or {}).get('preview','').splitlines(keepends=True),fromfile='before/'+name,tofile='after/'+name))
            diff=''.join(chunks)
            if len(diff.encode())<=32768:value['diff']=diff
            else:value['diff_unavailable']='diff_byte_budget'
        else:value['diff_unavailable']='source_preview_unavailable; full hashes retained'
        yield value
