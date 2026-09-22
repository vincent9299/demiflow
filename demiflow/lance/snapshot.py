"""Local immutable Lance snapshots without copying Blob payloads.

Data files are immutable; hard links keep their bytes owned by both tables until
one is retired. Manifests and auxiliary metadata are copied, never shared. This
is a physical snapshot, not an alias or a runtime fallback to the source table.
"""
from pathlib import Path
import os
import shutil


def clone_local_snapshot(source, destination):
    import lance
    src, dst = Path(source.uri).resolve(), Path(destination).resolve()
    if dst == src or dst.is_relative_to(src) or src.is_relative_to(dst):
        raise ValueError('Snapshot must have an independent destination')
    if dst.exists():
        raise FileExistsError(dst)
    if not src.is_dir():
        raise ValueError('Only local Lance datasets are supported')
    dst.parent.mkdir(parents=True, exist_ok=True)
    def copy_file(a, b):
        relative = Path(a).relative_to(src)
        if Path(a).is_symlink():
            raise ValueError('Symlink in Lance snapshot')
        if relative.parts[0] in ('data', '_indices'):
            os.link(a, b)
            return b
        return shutil.copy2(a, b)
    try:
        shutil.copytree(src, dst, copy_function=copy_file)
        result = lance.dataset(str(dst), version=source.version)
        if result.count_rows() != source.count_rows() or result.schema != source.schema:
            raise ValueError('Snapshot schema/count mismatch')
        if lance.dataset(str(dst)).version != source.version:
            result.restore()
        return lance.dataset(str(dst))
    except BaseException:
        if dst.exists():
            shutil.rmtree(dst)
        raise
