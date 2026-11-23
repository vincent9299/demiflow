"""Read-only Markdown navigation, one bounded directory or document per request."""
import difflib
import hashlib
import os
from pathlib import Path
import subprocess

from demiflow.forge.platform_environment import read_bounded
from demiflow.forge.schema import ForgeError
from demiflow.forge.io import no_symlinks
from demiflow.forge.project_io import relative_path

DIRECTORY_ENTRIES = 4096
DIRECTORY_BYTES = 512 * 1024
DOCUMENT_BYTES = 1024 * 1024
DIFF_LIMIT = 200
MARKDOWN = {'.md', '.markdown'}
TEXT = MARKDOWN | {'.py', '.js', '.ts', '.css', '.html', '.sh', '.json', '.yaml', '.yml', '.toml', '.txt', '.csv'}
SKIP = {'node_modules', '__pycache__', 'venv', 'env'}
CHANGE_SKIP = SKIP | {'runs', 'data', 'datasets', 'outputs', 'results'}


def _path(root, value, *, directory=False):
    if directory and value == '':
        return no_symlinks(Path(root).absolute())
    if not isinstance(value, str) or len(value.encode('utf-8')) > 1024 or any(ord(c) < 32 for c in value):
        raise ForgeError('Invalid document path', code='invalid_request')
    parts = value.split('/')
    if (len(parts) > 24 or any(part.startswith('.') or part in SKIP or
                              part.endswith('.lance') for part in parts)):
        raise ForgeError('Document path is outside the visible project directory', code='invalid_request')
    return relative_path(root, value)


def listing(root, path='', offset=0, limit=50, *, all_files=False):
    if type(offset) is not int or not 0 <= offset <= DIRECTORY_ENTRIES or type(limit) is not int or not 1 <= limit <= 50:
        raise ForgeError('Invalid document page', code='invalid_request')
    directory = _path(root, path, directory=True)
    rows, size = [], 0
    try:
        with os.scandir(directory) as entries:
            for index, entry in enumerate(entries):
                size += len(entry.name.encode('utf-8'))
                if index >= DIRECTORY_ENTRIES or size > DIRECTORY_BYTES:
                    raise ForgeError('Document directory exceeds listing budget', code='resource_limit')
                if entry.name.startswith('.') or entry.name in SKIP or entry.name.endswith('.lance') or entry.is_symlink():
                    continue
                item = '/'.join(filter(None, [path, entry.name]))
                if len(item.encode('utf-8')) > 1024 or len(item.split('/')) > 24 or '\\' in item:
                    continue
                kind = ('directory' if entry.is_dir(follow_symlinks=False) else
                        'markdown' if Path(entry.name).suffix.lower() in MARKDOWN and entry.is_file(follow_symlinks=False) else None)
                if all_files and kind is None and entry.is_file(follow_symlinks=False):
                    kind = 'text' if Path(entry.name).suffix.lower() in TEXT else 'file'
                if kind:
                    rows.append(dict(name=entry.name, path=item, kind=kind))
        rows.sort(key=lambda row: (row['kind'] != 'directory', row['name'].casefold(), row['name']))
        return dict(path=path, entries=rows[offset:offset+limit],
                    next_offset=offset+limit if len(rows) > offset+limit else None)
    except FileNotFoundError as exc:
        raise ForgeError('Document directory no longer exists', code='not_found') from exc
    except OSError as exc:
        raise ForgeError('Document directory cannot be read', code='invalid_request') from exc


def search_files(root, query, *, limit=50):
    """Bounded BFS filename/path search inside project root for sidebar tree filtering."""
    from collections import deque
    if not isinstance(query, str) or len(query.encode('utf-8')) > 256 or any(ord(c) < 32 for c in query):
        raise ForgeError('Invalid file search query', code='invalid_request')
    if type(limit) is not int or not 1 <= limit <= 100:
        raise ForgeError('Invalid search limit', code='invalid_request')
    needle = query.strip().casefold()
    if not needle:
        return dict(path='', query='', entries=[], next_offset=None, truncated=False)
    base = _path(root, '', directory=True)
    # Visit source/config dirs first via BFS; defer heavy run/output trees so shallow files match fast.
    queue = deque([('', base, 0)])
    deferred = deque()
    matches, visited, truncated = [], 0, False
    max_visited, max_depth = DIRECTORY_ENTRIES, 12
    while queue or deferred:
        rel_dir, cur_dir, depth = queue.popleft() if queue else deferred.popleft()
        try:
            with os.scandir(cur_dir) as entries:
                items = sorted(entries, key=lambda e: (not e.is_dir(follow_symlinks=False), e.name.casefold(), e.name))
        except OSError:
            continue
        for entry in items:
            visited += 1
            if visited > max_visited:
                truncated = True
                break
            name = entry.name
            if name.startswith('.') or name in SKIP or name.endswith('.lance') or entry.is_symlink():
                continue
            item = '/'.join(filter(None, [rel_dir, name]))
            if len(item.encode('utf-8')) > 1024 or len(item.split('/')) > 24 or '\\' in item:
                continue
            is_dir = entry.is_dir(follow_symlinks=False)
            if is_dir:
                if depth + 1 <= max_depth:
                    target_q = deferred if (depth == 0 and name in CHANGE_SKIP) else queue
                    target_q.append((item, Path(entry.path), depth + 1))
                continue
            if not entry.is_file(follow_symlinks=False):
                continue
            if needle in name.casefold() or needle in item.casefold():
                suffix = Path(name).suffix.lower()
                kind = 'markdown' if suffix in MARKDOWN else ('text' if suffix in TEXT else 'file')
                matches.append((0 if needle in name.casefold() else 1, item.casefold(), dict(name=name, path=item, kind=kind)))
                if len(matches) >= limit * 4:
                    truncated = True
                    break
        if truncated:
            break
    matches.sort(key=lambda row: (row[0], row[1]))
    results = [row[2] for row in matches[:limit]]
    if len(matches) > limit:
        truncated = True
    return dict(path='', query=query.strip(), entries=results, next_offset=None, truncated=truncated)


def read(root, path, *, project_text=False):
    target = _path(root, path)
    if project_text and target.suffix.lower() not in TEXT:
        raise ForgeError('File type cannot be previewed', code='invalid_request')
    if not project_text and target.suffix.lower() not in MARKDOWN:
        raise ForgeError('Only Markdown documents can be opened here', code='invalid_request')
    try:
        raw = read_bounded(target, DOCUMENT_BYTES)
        return dict(path=path, sha256=hashlib.sha256(raw).hexdigest(), text=raw.decode('utf-8'))
    except FileNotFoundError as exc:
        raise ForgeError('Document no longer exists', code='not_found') from exc
    except (OSError, UnicodeError) as exc:
        raise ForgeError('Document must be a readable UTF-8 file', code='invalid_request') from exc


def _git_run(root, args):
    env = {**os.environ, 'GIT_OPTIONAL_LOCKS': '0'}
    try:
        return subprocess.run(
            ['git', '-C', str(root), *args],
            capture_output=True, timeout=3, env=env, check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None


def _git_repo(root):
    base = no_symlinks(Path(root).absolute())
    top = _git_run(base, ['rev-parse', '--show-toplevel'])
    if not top or top.returncode != 0:
        return None
    toplevel = Path(top.stdout.decode('utf-8', 'replace').strip()).resolve()
    try:
        rel = base.resolve().relative_to(toplevel).as_posix()
    except ValueError:
        return None
    prefix = '' if rel in {'', '.'} else rel + '/'
    if prefix:
        tracked = _git_run(base, ['ls-files', '--', '.'])
        if not tracked or tracked.returncode != 0 or not tracked.stdout.strip():
            return None
    branch_proc = _git_run(base, ['rev-parse', '--abbrev-ref', 'HEAD'])
    branch = branch_proc.stdout.decode('utf-8', 'replace').strip() if branch_proc and branch_proc.returncode == 0 else ''
    return base, toplevel, prefix, branch


def _strip_prefix(git_path, prefix):
    if not prefix:
        return git_path
    return git_path[len(prefix):] if git_path.startswith(prefix) else None


def _visible_change_path(rel):
    if not rel or len(rel.encode('utf-8')) > 1024 or any(ord(c) < 32 for c in rel) or '\\' in rel:
        return False
    parts = rel.split('/')
    return (len(parts) <= 24 and not any(
        not p or p in {'.', '..'} or p.startswith('.') or p in CHANGE_SKIP or p.endswith('.lance')
        for p in parts
    ))


def git_changes(root):
    """Return bounded git working-tree changes scoped strictly inside the project root."""
    repo = _git_repo(root)
    if repo is None:
        return dict(is_git=False, branch='', changes=[], truncated=False)
    base, _, prefix, branch = repo
    status_proc = _git_run(base, ['status', '--porcelain=v1', '-z', '--untracked-files=all', '--', '.'])
    if not status_proc or status_proc.returncode != 0:
        return dict(is_git=True, branch=branch, changes=[], truncated=False)
    stats = {}
    numstat = _git_run(base, ['diff', '--numstat', 'HEAD', '--', '.'])
    if numstat and numstat.returncode == 0:
        for line in numstat.stdout.decode('utf-8', 'replace').splitlines():
            cols = line.split('\t', 2)
            if len(cols) == 3:
                rel = _strip_prefix(cols[2], prefix)
                if rel:
                    a = int(cols[0]) if cols[0].isdigit() else None
                    r = int(cols[1]) if cols[1].isdigit() else None
                    stats[rel] = (a, r)
    tokens = status_proc.stdout.split(b'\0')
    rows, seen, truncated = [], set(), False
    index = 0
    while index < len(tokens):
        entry = tokens[index]; index += 1
        if len(entry) < 4:
            continue
        code = entry[:2].decode('ascii', 'replace')
        raw_path = entry[3:].decode('utf-8', 'replace')
        if code[0] in {'R', 'C'} or code[1] in {'R', 'C'}:
            index += 1
        rel = _strip_prefix(raw_path, prefix)
        if not rel or rel in seen or not _visible_change_path(rel):
            continue
        seen.add(rel)
        if len(rows) >= DIFF_LIMIT:
            truncated = True
            break
        if code == '??' or 'A' in code:
            kind = 'A'
        elif 'D' in code:
            kind = 'D'
        else:
            kind = 'M'
        suffix = Path(rel).suffix.lower()
        preview = suffix in TEXT
        added, removed = stats.get(rel, (None, None))
        file_sha = ''
        if preview and kind != 'D':
            try:
                target = _path(base, rel)
                if target.is_file() and not target.is_symlink() and target.stat().st_size <= 256 * 1024:
                    raw_bytes = target.read_bytes()
                    file_sha = hashlib.sha256(raw_bytes).hexdigest()
                    if kind == 'A' and added is None:
                        added = len(raw_bytes.splitlines())
                        removed = 0
            except (ForgeError, OSError):
                pass
        rows.append(dict(path=rel, status=kind, added=added, removed=removed, preview=preview, sha256=file_sha))
    rows.sort(key=lambda item: item['path'].casefold())
    return dict(is_git=True, branch=branch, changes=rows, truncated=truncated)


def file_diff(root, path):
    """Read bounded current file text and git HEAD baseline for side/unified diff viewing."""
    target = _path(root, path)
    if target.suffix.lower() not in TEXT:
        raise ForgeError('File type cannot be previewed', code='invalid_request')
    modified, sha256, exists = '', '', False
    if target.exists():
        try:
            raw = read_bounded(target, DOCUMENT_BYTES)
            modified = raw.decode('utf-8')
            sha256 = hashlib.sha256(raw).hexdigest()
            exists = True
        except (OSError, UnicodeError) as exc:
            raise ForgeError('Document must be a readable UTF-8 file', code='invalid_request') from exc
    repo = _git_repo(root)
    if repo is None:
        if not exists:
            raise ForgeError('Document no longer exists', code='not_found')
        return dict(path=path, is_git=False, status='clean', sha256=sha256,
                    original=None, modified=modified, changed=False, added=0, removed=0)
    base, _, prefix, _ = repo
    git_rel = prefix + path
    head_proc = _git_run(base, ['show', 'HEAD:' + git_rel])
    has_head = bool(head_proc and head_proc.returncode == 0)
    original = None
    if has_head:
        if len(head_proc.stdout) > DOCUMENT_BYTES:
            raise ForgeError('Document baseline exceeds preview budget', code='resource_limit')
        try:
            original = head_proc.stdout.decode('utf-8')
        except UnicodeError:
            original = None
    if not exists and not has_head:
        raise ForgeError('Document no longer exists', code='not_found')
    if not exists and has_head:
        status = 'D'
    elif exists and not has_head:
        status = 'A'
    elif original is not None and original != modified:
        status = 'M'
    else:
        status = 'clean'
    added = removed = 0
    if status == 'A':
        added = len(modified.splitlines())
    elif status == 'D' and original is not None:
        removed = len(original.splitlines())
    elif status == 'M' and original is not None:
        for line in difflib.unified_diff(original.splitlines(), modified.splitlines(), n=0):
            if line.startswith('+') and not line.startswith('+++'):
                added += 1
            elif line.startswith('-') and not line.startswith('---'):
                removed += 1
    return dict(path=path, is_git=True, status=status, sha256=sha256,
                original=original if status in {'M', 'D', 'clean'} else '',
                modified=modified, changed=status != 'clean', added=added, removed=removed)
