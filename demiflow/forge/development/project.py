"""Versioned project context and bounded, content-based source snapshots."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import stat
import time

from .resources import TOOL_ROOT, configured_tool_root, implementation_files, resource_path

VERSION = "1.0.0-candidate.10"
PROJECT_LOCAL_STANDARDS = frozenset({"1.0.0-candidate.9", VERSION})
WORKSPACE_STANDARDS = frozenset({"1.0.0-candidate.7", "1.0.0-candidate.8", *PROJECT_LOCAL_STANDARDS})
STANDARDS = {
    f"1.0.0-candidate.{number}": resource_path("standards/" +
        ("pipeline-v1.md" if number == 1 else f"pipeline-v1.0.0-candidate.{number}.md"))
    for number in range(1, 11)
}
STANDARD = STANDARDS[VERSION]
SKIP = frozenset({".git", ".demiforge", "__pycache__", ".pytest_cache", ".venv",
                  "venv", "node_modules", "datasets", "runs", ".ipynb_checkpoints"})
MAX_FILE = 16 * 1024 * 1024
MAX_TOTAL = 256 * 1024 * 1024
MAX_FILES = 20000
PAGE_SIZE = 8000
REVIEW_RULES = tuple(f"DF-{i:02d}" for i in range(1, 8))


class ForgeError(ValueError):
    pass


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
                                     separators=(",", ":")).encode()).hexdigest()


def file_identity(info):
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)


def file_hash(path, *, limit=None, deadline=None):
    path = Path(path)
    no_symlinks(path)
    before = path.stat()
    if not stat.S_ISREG(before.st_mode):
        raise ForgeError(f"Expected regular file, not symlink: {path}")
    if limit is None:
        limit = MAX_TOTAL if path.suffix == ".ipynb" else MAX_FILE
    if before.st_size > limit:
        raise ForgeError(f"Source exceeds {limit} bytes: {path}")
    h = hashlib.sha256()
    size = 0
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, "rb") as stream:
        if file_identity(os.fstat(stream.fileno())) != file_identity(before):
            raise ForgeError(f"Source changed before hashing: {path}")
        while True:
            if deadline is not None and time.monotonic() > deadline:
                raise ForgeError("Source snapshot time budget exceeded")
            chunk = stream.read(min(1024 * 1024, before.st_size - size + 1))
            if not chunk:
                break
            size += len(chunk)
            if size > before.st_size:
                raise ForgeError(f"Source grew while hashing: {path}")
            h.update(chunk)
        if file_identity(os.fstat(stream.fileno())) != file_identity(before):
            raise ForgeError(f"Source changed while hashing: {path}")
    no_symlinks(path)
    if size != before.st_size or file_identity(path.stat()) != file_identity(before):
        raise ForgeError(f"Source changed while hashing: {path}")
    return h.hexdigest()


def safe_id(value):
    if not isinstance(value, str) or not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_-]{0,79}", value):
        raise ForgeError("ID must be 1-80 letters, digits, '-' or '_'")
    return value


def relative(value, *, modern=False):
    if not isinstance(value, str):
        raise ForgeError("Expected project-relative path string")
    if modern and len(value) > 4096:
        raise ForgeError('Source path length budget exceeded')
    p = PurePosixPath(value)
    if not value or p.is_absolute() or any(x in {"..", "."} for x in value.split("/")):
        raise ForgeError(f"Expected normalized project-relative path: {value}")
    if str(p) != value or any(x in (SKIP - {"runs", "datasets"} if modern else SKIP) for x in p.parts):
        raise ForgeError(f"Excluded or noncanonical source path: {value}")
    return value


def within(name, root):
    return name == root or name.startswith(root + "/")


def no_symlinks(path):
    for part in (path, *path.parents):
        if part.is_symlink():
            raise ForgeError(f"Symlink in source/state path: {part}")
    return path


def read_json(path):
    no_symlinks(path)
    if not path.is_file():
        raise ForgeError(f"Expected regular JSON file: {path}")
    if path.stat().st_size > MAX_TOTAL:
        raise ForgeError(f"JSON exceeds size limit: {path}")
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (UnicodeError, json.JSONDecodeError) as error:
        raise ForgeError(f"Invalid JSON: {path}: {error}") from error


def snapshot_limits(values=None):
    defaults = dict(total_bytes=MAX_TOTAL, file_bytes=MAX_FILE, notebook_bytes=MAX_TOTAL,
                    entries=MAX_FILES, seconds=120)
    ceilings = dict(total_bytes=16 * 1024**3, file_bytes=256 * 1024**2,
                    notebook_bytes=1024**3, entries=200000, seconds=600)
    if values is not None:
        if type(values) is not dict or set(values) - set(defaults):
            raise ForgeError("Unknown source snapshot budget")
        for key, value in values.items():
            if type(value) is not int or not 1 <= value <= ceilings[key]:
                raise ForgeError(f"Invalid source snapshot budget: {key}")
        defaults.update(values)
    return defaults


def snapshot(root, paths, *, limits=None, exact=None, subtrees=(), report=None,
             identities=None, expected_hashes=None, deadline=None):
    """Legacy pruning by default; modern callers supply precise source selectors."""
    result = {}
    total = enumerated = 0
    limits = snapshot_limits(limits)
    deadline = deadline or time.monotonic() + limits['seconds']
    modern = exact is not None
    exact = set(exact or ())
    subtrees = set(subtrees)
    unknown = set()
    runtime = {'runs', 'datasets'}
    protected = SKIP - runtime
    max_metadata = 16 * 1024**2
    metadata_bytes = 0

    def partial(label):
        nonlocal metadata_bytes
        if label in unknown:
            return
        metadata_bytes += len(label.encode())
        if len(unknown) >= min(20000, limits['entries']) or metadata_bytes > max_metadata:
            raise ForgeError('Source coverage metadata budget exceeded')
        unknown.add(label)

    selected_paths = {str(Path(name)) for name in exact | subtrees}
    if modern:
        if len(selected_paths) > min(20000, limits['entries']):
            raise ForgeError('Source selector entry budget exceeded')
        metadata_bytes = sum(len(name.encode()) for name in selected_paths)
        if metadata_bytes > max_metadata:
            raise ForgeError('Source selector metadata byte budget exceeded')
        for name in tuple(selected_paths):
            if time.monotonic() > deadline:
                raise ForgeError('Source snapshot time budget exceeded')
            for parent in Path(name).parents:
                label = str(parent)
                if label == '.' or label in selected_paths:
                    continue
                if len(selected_paths) >= min(20000, limits['entries']):
                    raise ForgeError('Source selector entry budget exceeded')
                metadata_bytes += len(label.encode())
                if metadata_bytes > max_metadata:
                    raise ForgeError('Source selector metadata byte budget exceeded')
                selected_paths.add(label)

    def selected(label):
        return str(Path(label)) in selected_paths

    def remember(path, identity):
        if identities is not None:
            if path in identities and identities[path] != identity:
                raise ForgeError(f'Source changed between snapshots: {path}')
            identities[path] = identity

    def visit(path, label, selective=False):
        nonlocal total, enumerated
        if time.monotonic() > deadline:
            raise ForgeError('Source snapshot time budget exceeded')
        if len(Path(label).parts) > 64:
            raise ForgeError('Source snapshot depth budget exceeded')
        if label in result or label + '/' in result:
            return
        if path.is_symlink():
            # Snapshot the link itself, without opening or traversing its target.
            no_symlinks(path.parent)
            before = file_identity(path.lstat())
            target = os.fsencode(os.readlink(path))
            if len(target) > 4096:
                raise ForgeError(f'Symlink target exceeds path budget: {path}')
            total += len(target)
            if total > limits['total_bytes']:
                raise ForgeError('Watched source bytes exceed declared snapshot budget')
            if not path.is_symlink() or file_identity(path.lstat()) != before:
                raise ForgeError(f'Symlink changed during snapshot: {path}')
            remember(path, ('symlink', before))
            result[label] = 'symlink:' + hashlib.sha256(target).hexdigest()
            if len(result) > limits['entries']:
                raise ForgeError('Too many source snapshot entries for declared budget')
            return
        no_symlinks(path)
        if not path.exists():
            result[label] = None
            remember(path, None)
        elif path.is_dir():
            before = file_identity(path.stat())
            result[label + '/'] = 'directory'
            remember(path, before)
            full = any(within(label, scope) for scope in subtrees)
            with os.scandir(path) as entries:
                for child in entries:
                    if modern:
                        if time.monotonic() > deadline:
                            raise ForgeError('Source snapshot time budget exceeded')
                        enumerated += 1
                        if enumerated > limits['entries']:
                            raise ForgeError('Source snapshot directory entry budget exceeded')
                        if child.name in protected:
                            continue
                        child_label = label + '/' + child.name
                        child_path = Path(child.path)
                        if child_path.is_symlink():
                            visit(child_path, child_label)
                            continue
                        no_symlinks(child_path)
                        if selective and not full and not selected(child_label):
                            partial(child_label)
                            continue
                        candidate = child.name in runtime and child.is_dir(follow_symlinks=False)
                        if candidate and not full and not any(within(child_label, scope) for scope in subtrees):
                            if selected(child_label):
                                visit(child_path, child_label, selective=True)
                            else:
                                partial(child_label)
                            continue
                        visit(child_path, child_label, selective=selective and not full)
                    elif child.name not in SKIP:
                        visit(Path(child.path), label + '/' + child.name)
            if file_identity(path.stat()) != before:
                raise ForgeError(f'Source directory changed during snapshot: {path}')
        else:
            if not path.is_file():
                raise ForgeError(f'Special file in watched sources: {path}')
            before = file_identity(path.stat())
            total += path.stat().st_size
            if total > limits['total_bytes']:
                raise ForgeError('Watched source bytes exceed declared snapshot budget')
            result[label] = file_hash(path, deadline=deadline,
                                     limit=limits['notebook_bytes'] if path.suffix == '.ipynb' else limits['file_bytes'])
            if file_identity(path.stat()) != before:
                raise ForgeError(f'Source changed during snapshot: {path}')
            remember(path, before)
            if expected_hashes is not None and label in expected_hashes and result[label] != expected_hashes[label]:
                raise ForgeError(f'Static source and snapshot differ: {label}')
        if len(result) > limits['entries']:
            raise ForgeError('Too many source snapshot entries for declared budget')

    for name in paths:
        visit(root / name, name)
    for name in sorted(exact):
        visit(root / name, name, selective=True)
    if report is not None:
        report.update(unverified_ranges=sorted(unknown), complete=not unknown)
    return dict(sorted(result.items()))

def changes(before, after):
    return sorted(k for k in before.keys() | after.keys() if before.get(k) != after.get(k))


class Project:
    def __init__(self, root):
        self.root = Path(root).absolute()
        no_symlinks(self.root)
        self.control = no_symlinks(self.root / ".demiforge")
        self.path = self.control / "project.json"
        self.config = read_json(self.path)
        self.validate()

    def validate(self):
        c = self.config
        multi = isinstance(c, dict) and c.get('schema') == 'demiforge-project-v3'
        registration_key = 'projects' if multi else 'pipelines'
        required = {"schema", "standard", "tool_root", "sources", "watch", "readonly", registration_key}
        if not isinstance(c, dict) or set(c) - (required | {"snapshot_limits"}) or required - set(c):
            raise ForgeError(f"project.json requires: {sorted(required)}")
        if c["schema"] not in {"demiforge-project-v1", "demiforge-project-v2", "demiforge-project-v3"} or c["standard"] not in STANDARDS:
            raise ForgeError(f"Unsupported project schema/standard; expected {VERSION}")
        if multi != (c['standard'] == VERSION):
            raise ForgeError('candidate.10 requires project-v3; historical standards retain their original schema')
        modern = c["schema"] in {"demiforge-project-v2", "demiforge-project-v3"}
        if modern != (c["standard"] in WORKSPACE_STANDARDS) or not modern and "snapshot_limits" in c:
            raise ForgeError("The workspace standard requires project-v2; legacy bindings retain project-v1")
        snapshot_limits(c.get("snapshot_limits"))
        modern_paths = c["standard"] in PROJECT_LOCAL_STANDARDS or (
            isinstance(c[registration_key], dict) and any(isinstance(p, dict) and
            p.get("standard", c["standard"]) in PROJECT_LOCAL_STANDARDS for p in c[registration_key].values()))
        try:
            self.tool_root = configured_tool_root(self.root, c["tool_root"])
        except ValueError as error:
            raise ForgeError(str(error)) from error
        if not isinstance(c["sources"], dict) or "project-rules" not in c["sources"]:
            raise ForgeError("sources must include project-rules (current project instructions)")
        for key, source in c["sources"].items():
            safe_id(key)
            if key in {"standard", "pipeline-readme", "pipeline-entry"} or (
                    multi and (key == 'project-readme' or key.startswith('flow-entry-'))):
                raise ForgeError(f"Reserved source ID: {key}")
            if not isinstance(source, dict) or set(source) - {"path", "lines"} or "path" not in source:
                raise ForgeError(f"Invalid context source: {key}")
            relative(source["path"], modern=modern_paths)
            if "lines" in source:
                span = source["lines"]
                if not isinstance(span, list) or len(span) != 2 or any(type(n) is not int for n in span) or not 1 <= span[0] <= span[1]:
                    raise ForgeError(f"Invalid inclusive line range: {key}")
        if not isinstance(c["watch"], list) or not c["watch"]:
            raise ForgeError("watch must list the complete relevant source roots")
        for name in c["watch"]:
            relative(name, modern=modern_paths)
        if not isinstance(c["readonly"], list):
            raise ForgeError("readonly must list platform source/contract paths")
        for name in c["readonly"]:
            if not isinstance(name, str) or not name:
                raise ForgeError("Invalid readonly path")
            path = self.root / name
            no_symlinks(path)
            if not path.exists():
                raise ForgeError(f"Missing readonly source: {path}")
            if path.resolve() in {self.root, self.control} or self.control.is_relative_to(path.resolve()):
                raise ForgeError("Readonly root cannot contain project/control state")
        if not isinstance(c[registration_key], dict) or not c[registration_key]:
            raise ForgeError("At least one pipeline registration is required")
        if multi and len(c[registration_key]) > 128:
            raise ForgeError('Project registration count exceeds budget')
        for name, p in c[registration_key].items():
            safe_id(name)
            if type(p) is not dict:
                raise ForgeError(f"Invalid pipeline registration: {name}")
            legacy = p.get("standard", c["standard"]) not in WORKSPACE_STANDARDS
            local = p.get("standard", c["standard"]) in PROJECT_LOCAL_STANDARDS
            required_pipeline = {"path", "flows" if multi else "entry", "verify"}
            if multi:
                required_pipeline.add('prompts')
            if modern and legacy:
                required_pipeline.add("standard")
            elif modern:
                required_pipeline.update({"operators", "notebooks"})
                if "standard" in p:
                    required_pipeline.add("standard")
            if type(p) is not dict or set(p) != required_pipeline or p.get("standard", c["standard"]) not in STANDARDS:
                raise ForgeError(f"Invalid pipeline registration: {name}")
            if (multi and p.get('standard', VERSION) != VERSION) or (not multi and p.get('standard') == VERSION):
                raise ForgeError('Multi-flow ownership requires a candidate.10 project-v3 registration')
            relative(p["path"], modern=local)
            watched = [p["path"]]
            if legacy:
                relative(p["entry"])
                if "/" in p["entry"] or not p["entry"].endswith("_pipeline.py"):
                    raise ForgeError("entry must be a local <prefix>_pipeline.py")
            else:
                entries = p['flows'] if multi else {'main': p['entry']}
                if not isinstance(entries, dict) or not 1 <= len(entries) <= 64:
                    raise ForgeError('Register 1..64 named flows per project')
                modules = set()
                for flow, entry in entries.items():
                    safe_id(flow)
                    if type(entry) is not dict or set(entry) != {"path", "symbol"}:
                        raise ForgeError("Entry requires project-relative path and symbol")
                    relative(entry["path"], modern=local)
                    if not entry["path"].endswith(".py") or not isinstance(entry["symbol"], str) or not entry["symbol"].isidentifier() or entry["symbol"].startswith('_'):
                        raise ForgeError("Entry must name a public function in a Python module")
                    if not local and not entry["path"].startswith("flows/"):
                        raise ForgeError("Historical standard requires an entry in flows/*.py; adopt candidate.9 for project-local ownership")
                    if entry['path'] in modules:
                        raise ForgeError('Each named flow owns a distinct entry module')
                    modules.add(entry['path'])
                    watched.append(entry["path"])
                for key, suffix in (("operators", None), ("notebooks", '.ipynb'), *((('prompts', None),) if multi else ())):
                    if type(p[key]) is not list or len(p[key]) > 64 or any(not isinstance(v, str) for v in p[key]) or len(set(p[key])) != len(p[key]):
                        raise ForgeError(f"Invalid registered {key}")
                    for path in p[key]:
                        relative(path, modern=local)
                        if key == 'operators' and not local and not path.startswith('operators/') or suffix and not path.endswith(suffix):
                            raise ForgeError(f"Invalid registered {key} path")
                        watched.append(path)
            if any(not any(within(path, w) for w in c["watch"]) for path in watched):
                raise ForgeError(f"Pipeline {name} is not fully watched")
            if not isinstance(p["verify"], list) or not p["verify"]:
                raise ForgeError("Register at least one trusted fixture verification command")
            ids = set()
            for command in p["verify"]:
                if type(command) is not dict or set(command) != {"id", "argv", "env", "timeout"}:
                    raise ForgeError("Verification requires id, argv, env, timeout")
                safe_id(command["id"])
                if command["id"] in ids:
                    raise ForgeError("Duplicate verification ID")
                ids.add(command["id"])
                if not isinstance(command["argv"], list) or not command["argv"] or any(not isinstance(a, str) or not a for a in command["argv"]):
                    raise ForgeError("Verification argv must be nonempty strings (no shell)")
                if not isinstance(command["env"], dict) or any(not isinstance(k, str) or not isinstance(v, str) for k, v in command["env"].items()):
                    raise ForgeError("Verification env must map strings to strings")
                if type(command["timeout"]) is not int or not 1 <= command["timeout"] <= 600:
                    raise ForgeError("Verification timeout must be 1..600 seconds")
        self.validate_ownership()

    def validate_ownership(self):
        """New bindings own their code; old bindings keep their recorded layout."""
        for name, pipeline in self.registrations.items():
            if self.standard(name) not in PROJECT_LOCAL_STANDARDS:
                continue
            owner = pipeline['path']
            entries = [e['path'] for e in self.entries(name).values()]
            roles = [*entries, *pipeline['operators'], *pipeline.get('prompts', []), *pipeline['notebooks']]
            no_symlinks(self.root / owner)
            for path in roles:
                if path == owner or not within(path, owner):
                    raise ForgeError(f'Pipeline {name} source must be inside its owner directory: {path}')
                target = no_symlinks(self.root / path)
                if any(target.is_relative_to((self.root / ro).resolve()) or
                       (self.root / ro).resolve().is_relative_to(target)
                       for ro in self.config['readonly']):
                    raise ForgeError(f'Owned source overlaps a readonly path: {path}')
            for role in ('operators', 'prompts'):
                for operator in pipeline.get(role, []):
                    if any(within(entry, operator) or within(operator, entry) for entry in entries):
                        raise ForgeError(f'Entry and {role} roles overlap: {operator}')
                    if (self.root / operator).is_file() and not operator.endswith(('.py',) if role == 'operators' else ('.yaml', '.yml', '.json')):
                        raise ForgeError(f'Invalid {role} source type: {operator}')
                    for other_name, other in self.registrations.items():
                        for other_role, other_operator in ((r, o) for r in ('operators', 'prompts') for o in other.get(r, [])):
                            if (other_name == name and other_role == role and other_operator == operator):
                                continue
                            if within(operator, other_operator) or within(other_operator, operator):
                                raise ForgeError(f'Operator ownership overlaps: {operator}, {other_operator}')
                        if other_name != name and any(within(e['path'], operator) for e in self.entries(other_name).values()):
                            raise ForgeError(f'Operator source contains another entry: {operator}')
            for other_name in self.registrations:
                if other_name != name and set(entries) & {e['path'] for e in self.entries(other_name).values()}:
                    raise ForgeError(f'Entry module has multiple owners: {name}, {other_name}')

    @property
    def registrations(self):
        return self.config['projects' if self.config['schema'] == 'demiforge-project-v3' else 'pipelines']

    def entries(self, name):
        p = self.pipeline(name)
        if 'flows' in p:
            return p['flows']
        entry = p['entry']
        return {'main': entry if isinstance(entry, dict) else
                dict(path=p['path'] + '/' + entry, symbol='run_pipeline')}

    def pipeline(self, name):
        try:
            return self.registrations[name]
        except KeyError as error:
            raise ForgeError(f"Unregistered pipeline: {name}") from error

    def binding(self):
        return binding(self.path, self.config)

    def source_defs(self, pipeline):
        p = self.pipeline(pipeline)
        if 'flows' in p:
            return {'standard': {'path': str(STANDARDS[self.standard(pipeline)])}, **self.config['sources'],
                    'project-readme': {'path': p['path'] + '/README.md', 'allow_missing': True},
                    **{'flow-entry-' + name: {'path': entry['path'], 'allow_missing': True}
                       for name, entry in p['flows'].items()}}
        return {"standard": {"path": str(STANDARDS[self.standard(pipeline)])}, **self.config["sources"],
                "pipeline-readme": {"path": p["path"] + "/README.md", "allow_missing": True},
                "pipeline-entry": {"path": self.entry_path(pipeline), "allow_missing": True}}

    def standard(self, name):
        return self.pipeline(name).get('standard', self.config['standard'])

    def entry_path(self, name):
        p = self.pipeline(name)
        if 'flows' in p:
            raise ForgeError('Select an explicit flow; a project has no default execution entry')
        return p['entry']['path'] if isinstance(p['entry'], dict) else p['path'] + '/' + p['entry']

    def source(self, definition, *, identities=None, deadline=None):
        path = no_symlinks(self.root / definition["path"])
        if identities is not None and path in identities:
            actual = file_identity(path.stat()) if path.exists() else None
            if actual != identities[path]:
                raise ForgeError(f"Context changed after snapshot: {path}")
        if not path.exists() and definition.get("allow_missing"):
            return f"MISSING: {definition['path']} (new pipeline; create then refresh context)\n"
        if not path.is_file():
            raise ForgeError(f"Expected regular context source: {path}")
        if path.suffix == ".ipynb":
            from .notebook import read_cells
            cells = read_cells(path, max_bytes=snapshot_limits(self.config.get('snapshot_limits'))['notebook_bytes'])
            text = "Notebook code context; saved outputs/metadata omitted here, full file remains snapshot-bound.\n"
            for index, cell in enumerate(cells, 1):
                if cell.get("cell_type") == "code":
                    text += f"\n## Cell {index}\n" + "".join(cell.get("source", [])) + "\n"
            if len(text.encode()) > 1024 * 1024:
                raise ForgeError(f"Notebook code context exceeds 1 MiB: {path}")
        else:
            if path.stat().st_size > 1024 * 1024:
                raise ForgeError(f"Context source exceeds 1 MiB: {path}")
            if identities is None:
                text = path.read_text(encoding="utf-8")
            else:
                from .coverage import bounded_bytes
                raw, identity = bounded_bytes(path, 1024 * 1024, expected=identities.get(path), deadline=deadline)
                text = raw.decode("utf-8")
                identities[path] = identity
        if identities is not None:
            actual = file_identity(path.stat())
            if path in identities and actual != identities[path]:
                raise ForgeError(f"Context changed during reading: {path}")
            identities[path] = actual
        if "lines" in definition:
            lines = text.splitlines(keepends=True)
            start, end = definition["lines"]
            if end > len(lines):
                raise ForgeError(f"Context line range exceeds file: {path}")
            text = "".join(lines[start-1:end])
        return text

    def sources(self, pipeline, *, identities=None, deadline=None):
        sources = {}
        for key, definition in self.source_defs(pipeline).items():
            text = self.source(definition, identities=identities, deadline=deadline)
            sources[key] = {"definition": definition, "sha256": digest(text),
                            "pages": max(1, (len(text) + PAGE_SIZE - 1) // PAGE_SIZE)}
        return sources

    def tree(self, pipeline=None, *, facts=None, report=None, identities=None, deadline=None):
        paths = sorted(set(self.config["watch"] + [s["path"] for s in self.config["sources"].values()]))
        if pipeline is None or self.standard(pipeline) not in PROJECT_LOCAL_STANDARDS:
            return snapshot(self.root, paths, limits=self.config.get('snapshot_limits'))
        if facts is None:
            return self.observe(pipeline).tree
        exact = self._source_selectors(pipeline, facts)
        subtrees = set(facts.subtrees)
        for path in self.config['watch']:
            if any(part in {'runs', 'datasets'} for part in Path(path).parts) and (self.root / path).is_dir():
                subtrees.add(path)
        return snapshot(self.root, paths, limits=self.config.get('snapshot_limits'), exact=exact,
                        subtrees=subtrees, report=report, identities=identities,
                        expected_hashes=facts.hashes, deadline=deadline)

    def _source_selectors(self, pipeline, facts=None):
        maximum = min(20000, snapshot_limits(self.config.get('snapshot_limits'))['entries'])
        if len(self.config['sources']) + 3 > maximum:
            raise ForgeError('Source selector entry budget exceeded')
        result = (set(facts.required) if facts is not None else set()) | {
            d['path'] for d in self.source_defs(pipeline).values() if not Path(d['path']).is_absolute()}
        if len(result) > maximum:
            raise ForgeError('Source selector entry budget exceeded')
        return result

    def readonly(self, pipeline=None, *, facts=None, report=None, identities=None, deadline=None):
        if pipeline is None or self.standard(pipeline) not in PROJECT_LOCAL_STANDARDS:
            return snapshot(self.root, self.config["readonly"], limits=self.config.get('snapshot_limits'))
        limits = snapshot_limits(self.config.get('snapshot_limits'))
        deadline = deadline or time.monotonic() + limits['seconds']
        maximum = min(20000, limits['entries'])
        roots, exact, metadata_bytes = {}, set(), 0
        # Index readonly roots inside the project. External roots cannot contain
        # a project-relative source (validate already forbids containing the project).
        if len(self.config['readonly']) > maximum:
            raise ForgeError('Readonly source selector entry budget exceeded')
        project_root = self.root.resolve()
        for name in self.config['readonly']:
            if time.monotonic() > deadline:
                raise ForgeError('Source snapshot time budget exceeded')
            path = (self.root / name).resolve()
            if not path.is_relative_to(project_root):
                continue
            label = path.relative_to(project_root)
            metadata_bytes += len(str(label).encode()) + len(name.encode())
            if metadata_bytes > 16 * 1024**2:
                raise ForgeError('Readonly source selector metadata byte budget exceeded')
            roots.setdefault(label, []).append(name)
        for source in self._source_selectors(pipeline, facts):
            path = Path(source)
            for parent in (path, *path.parents):
                if time.monotonic() > deadline:
                    raise ForgeError('Source snapshot time budget exceeded')
                for name in roots.get(parent, ()):
                    suffix = path.relative_to(parent).as_posix()
                    label = name if suffix == '.' else name + '/' + suffix
                    if label in exact:
                        continue
                    metadata_bytes += len(label.encode())
                    if len(exact) >= maximum or metadata_bytes > 16 * 1024**2:
                        raise ForgeError('Readonly source selector budget exceeded')
                    exact.add(label)
        subtrees = {p for p in self.config['readonly'] if any(part in {'runs', 'datasets'} for part in Path(p).parts)
                    and (self.root / p).is_dir()}
        return snapshot(self.root, self.config['readonly'], limits=self.config.get('snapshot_limits'), exact=exact,
                        subtrees=subtrees, report=report, identities=identities, deadline=deadline)

    def observe(self, pipeline):
        from .coverage import Observation, SourceFacts
        if self.standard(pipeline) not in PROJECT_LOCAL_STANDARDS:
            for path in [*self.config['watch'], *(s['path'] for s in self.config['sources'].values())]:
                relative(path)
            return Observation(self.tree(), self.readonly(), self.sources(pipeline))
        deadline = time.monotonic() + snapshot_limits(self.config.get('snapshot_limits'))['seconds']
        if len(self.config['sources']) + len(self.config['watch']) + len(self.config['readonly']) > snapshot_limits(self.config.get('snapshot_limits'))['entries']:
            raise ForgeError('Source declaration entry budget exceeded')
        facts = SourceFacts(self, deadline=deadline)
        tree_report, readonly_report, identities = {}, {}, {}
        tree = self.tree(pipeline, facts=facts, report=tree_report, identities=identities, deadline=deadline)
        readonly = self.readonly(pipeline, facts=facts, report=readonly_report, identities=identities, deadline=deadline)
        coverage = {**facts.summary(), 'default_pruning_candidates': ['datasets', 'runs'],
                    'protected_directories': sorted(SKIP - {'datasets', 'runs'}),
                    'tree': tree_report, 'readonly': readonly_report,
                    'complete': tree_report['complete'] and readonly_report['complete'] and not facts.problems}
        result = Observation(tree, readonly, self.sources(pipeline, identities=identities, deadline=deadline),
                             coverage=coverage, facts=facts, identities=identities)
        result.stable()
        return result


def binding(config_path, config, *, hash_file=None):
    """Tool/config identity only; this does not certify current project sources."""
    hash_file = hash_file or file_hash
    standards = {config["standard"], *(p.get("standard", config["standard"])
                                      for p in config['projects' if config['schema'] == 'demiforge-project-v3' else 'pipelines'].values())}
    files = implementation_files()
    for version in sorted(standards):
        path = STANDARDS[version]
        files["demiflow/forge/development/standards/" + path.name] = path
    return digest({"config": hash_file(config_path),
                   "tool": {name: hash_file(path) for name, path in sorted(files.items())}})
