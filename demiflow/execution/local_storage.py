"""Finite local-filesystem admission for SQLite WAL task stores."""
from pathlib import Path

_NETWORK_FS = {'nfs', 'nfs4', 'cifs', 'smb3', 'ceph', 'lustre', 'dpc', 'glusterfs', 'fuse.glusterfs', 'fuse.sshfs', '9p'}


def require_local_storage(path, *, mountinfo='/proc/self/mountinfo'):
    path = Path(path).resolve()
    if not Path(mountinfo).exists():
        raise ValueError('Cannot establish local filesystem; use a PostgreSQL task channel')
    matches = []
    with Path(mountinfo).open() as stream:description=stream.read(1024**2+1)
    if len(description)>1024**2 or description.count('\n')>8192:
        raise ValueError('Filesystem declaration exceeds its byte/count budget')
    for line in description.splitlines():
        left, right = line.split(' - ', 1)
        mount = Path(left.split()[4].replace('\\040', ' ').replace('\\134', '\\'))
        if path == mount or mount in path.parents:
            matches.append((len(mount.parts), right.split()[0]))
    if not matches or max(matches)[1] in _NETWORK_FS or max(matches)[1].startswith('fuse.'):
        raise ValueError('SQLite WAL queue requires local storage; use a PostgreSQL task channel')
    return path



def process_identity(pid):
    try:
        # /proc stat comm can contain spaces and parentheses.
        fields = Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()
        return None if fields[0] == 'Z' else fields[19]
    except FileNotFoundError:
        return None
