"""One local location for per-table coordination, separate from business rows."""
from pathlib import Path
from urllib.parse import urlsplit


def control_directory(uri):
    value=str(uri)
    if urlsplit(value).scheme not in ('','file'):raise ValueError('Local table control requires a filesystem URI')
    table=Path(urlsplit(value).path if value.startswith('file:') else value)
    return table.parent/'_demiflow'/table.name


def table_lock_path(uri):
    directory=control_directory(uri)
    directory.mkdir(parents=True,exist_ok=True)
    return directory/'write.lock'
