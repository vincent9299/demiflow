"""Read-only process identity inspection for managed local services."""
from pathlib import Path

def command(pid):
    try:return Path(f'/proc/{pid}/cmdline').read_bytes().decode().strip('\0').split('\0')
    except FileNotFoundError:return []

def matching(fragment):
    return [int(p.name) for p in Path('/proc').iterdir() if p.name.isdigit() and fragment in ' '.join(command(int(p.name)))]
