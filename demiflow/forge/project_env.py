"""Project-owned .env / .env.example parsing and prompt credential discovery (stdlib-only)."""
from pathlib import Path
import re

from .platform_environment import read_bounded
from .schema import ForgeError


MAX_DOTENV_BYTES = 64 * 1024
_ENV_NAME_RE = re.compile(r'^[A-Z_][A-Z0-9_]{0,127}$')
_SECRET_LITERAL_RE = re.compile(r'(?:sk-proj-[A-Za-z0-9_-]{20,}|sk-[A-Za-z0-9]{20,}|ghp_[A-Za-z0-9]{30,}|AIza[0-9A-Za-z_-]{30,})')
_PLACEHOLDER_VALUE_RE = re.compile(r'^(?:<[^>]*>|\$\{[^}]*\}|YOUR_[A-Z0-9_]+|REPLACE_[A-Z0-9_]+|CHANGEME|TODO|xxx+)$', re.IGNORECASE)


def parse_dotenv(text: str, *, allow_empty: bool = False) -> dict[str, str]:
    """Parse standard KEY=VALUE dotenv text without executing shell syntax."""
    result: dict[str, str] = {}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith('#'):
            continue
        if line.startswith('export '):
            line = line[len('export '):].strip()
        if '=' not in line:
            continue
        key, raw_val = line.split('=', 1)
        key = key.strip()
        if not _ENV_NAME_RE.fullmatch(key):
            continue
        val = raw_val.strip()
        if len(val) >= 2 and val[0] == val[-1] and val[0] in ('"', "'"):
            val = val[1:-1]
        else:
            # Strip unquoted trailing inline comment (`VALUE # comment`)
            if ' #' in val:
                val = val.split(' #', 1)[0].rstrip()
        if any(ch in val for ch in ('\0', '\r', '\n')):
            raise ForgeError(f'Invalid control character in project environment variable: {key}', code='invalid_request')
        if val or allow_empty:
            result[key] = val
    return result


def load_project_env(root: Path, *, filename: str = '.env', allow_empty: bool = False) -> dict[str, str]:
    """Safely read project-owned <root>/<filename> (rejecting symlinks and oversized files)."""
    root = Path(root).absolute()
    path = root / filename
    if not path.exists() and not path.is_symlink():
        return {}
    if path.is_symlink() or not path.is_file():
        raise ForgeError(f'Project environment file must be a regular file: {filename}', code='invalid_request')
    raw = read_bounded(path, MAX_DOTENV_BYTES)
    try:
        text = raw.decode('utf-8')
    except UnicodeDecodeError as exc:
        raise ForgeError(f'Project environment file must be valid UTF-8: {filename}', code='invalid_request') from exc
    return parse_dotenv(text, allow_empty=allow_empty)


def declared_prompt_env_names(root: Path, *, exclude=()) -> set[str]:
    """Collect api_key_env / base_url_env variable names declared in project prompts/*.yaml."""
    import yaml

    root = Path(root).absolute()
    prompts_dir = root / 'prompts'
    if not prompts_dir.is_dir() or prompts_dir.is_symlink():
        return set()
    names: set[str] = set()
    files = sorted(prompts_dir.rglob('*'))
    for fp in files[:512]:
        if not fp.is_file() or fp.is_symlink() or fp.suffix.lower() not in ('.yaml', '.yml'):
            continue
        rel = fp.relative_to(root).as_posix()
        if any(Path(rel).is_relative_to(parent) for parent in exclude):
            continue
        try:
            raw = read_bounded(fp, 256 * 1024)
            doc = yaml.safe_load(raw.decode('utf-8', errors='ignore'))
        except Exception:
            continue
        _collect_env_fields(doc, names)
    return names


def _collect_env_fields(obj, out: set[str], depth: int = 0):
    if depth > 16:
        return
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k in ('api_key_env', 'base_url_env') and isinstance(v, str):
                cand = v.strip()
                if _ENV_NAME_RE.fullmatch(cand):
                    out.add(cand)
            else:
                _collect_env_fields(v, out, depth + 1)
    elif isinstance(obj, list):
        for item in obj:
            _collect_env_fields(item, out, depth + 1)


def is_placeholder_or_empty(value: str) -> bool:
    s = value.strip()
    return not s or bool(_PLACEHOLDER_VALUE_RE.fullmatch(s))


def find_hardcoded_secret_line(text: str) -> int | None:
    """Return 1-based line number of an obvious hardcoded API secret literal, or None."""
    for lineno, line in enumerate(text.splitlines(), 1):
        if _SECRET_LITERAL_RE.search(line):
            return lineno
    return None


def ensure_gitignore_env(root: Path) -> bool:
    """Ensure <root>/.gitignore ignores local .env files without clobbering existing rules."""
    root = Path(root).absolute()
    gi = root / '.gitignore'
    if gi.is_symlink():
        return False
    existing = ''
    if gi.is_file():
        try:
            existing = read_bounded(gi, 64 * 1024).decode('utf-8', errors='ignore')
        except Exception:
            return False
    lines = {line.strip() for line in existing.splitlines() if line.strip() and not line.strip().startswith('#')}
    needed = [entry for entry in ('.env', '.env.local') if entry not in lines]
    if not needed:
        return False
    prefix = '' if (not existing or existing.endswith('\n')) else '\n'
    header = '' if existing else '# Project local runtime environment (never commit real secrets)\n'
    gi.write_text(existing + prefix + header + '\n'.join(needed) + '\n', encoding='utf-8')
    return True
