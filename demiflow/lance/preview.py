"""Projected, pinned read-only inspection in an allocation-limited worker.

Batch and I/O admission precede native reads. Native decoding is additionally
subject to the caller worker's finite address-space limit, because Arrow byte
counts do not predict a variable-width decoder's allocations. No truncation is
used to turn an oversized decoded value into a successful read.
"""

from __future__ import annotations

import base64
import datetime
import json
import math
from pathlib import Path, PurePosixPath
import os
import stat
import resource

from .storage import open_lance_dataset, schema_hash, resolve_local_uri
from .refs import DatasetRef


def _guard():
    soft, _ = resource.getrlimit(resource.RLIMIT_AS)
    if soft == resource.RLIM_INFINITY or soft > 4 * 1024**3:
        raise RuntimeError(
            "Preview requires an isolated worker with a finite allocation limit"
        )


def _local_file(path, *, maximum=None):
    for parent in (path, *path.parents):
        if parent.is_symlink():
            raise ValueError('Symlink in native table location')
    info = path.stat()
    if not stat.S_ISREG(info.st_mode) or maximum is not None and info.st_size > maximum:
        raise ValueError('Native table file type or metadata budget exceeded')


def _local_layout(uri):
    """Validate the physical root; historical files are not enumerated."""
    root = Path(uri)
    if not root.is_absolute():
        raise ValueError('Preview requires an absolute local table path')
    for path in (root, root / '_versions', root / 'data', root / '_deletions', root / '_indices'):
        for parent in (path, *path.parents):
            if parent.is_symlink():
                raise ValueError('Symlink in native table location')
        if path.exists() and not path.is_dir():
            raise ValueError('Native table location must be a directory')
    if resolve_local_uri(root) != root:
        raise ValueError('Attach the physical table path; implicit relocation is not covered')
    return root


def _fragment_file(root, directory, value):
    path = PurePosixPath(value)
    if path.is_absolute() or '..' in path.parts or ':' in value or str(path) != value:
        raise ValueError('External fragment locations are not covered by local preview')
    _local_file(root / directory / path)


def _open(uri, version, *, maximum_files=65536, verify_fragments=True):
    root = _local_layout(uri)
    if version is not None:
        # Both public Lance manifest naming schemes; only the requested version.
        for name in (f'{version}.manifest', f'{2**64 - 1 - version:020d}.manifest'):
            path = root / '_versions' / name
            if path.exists() or path.is_symlink():
                _local_file(path, maximum=64 * 1024**2)
    ds = open_lance_dataset(
        str(root), version, index_cache_size_bytes=0, metadata_cache_size_bytes=1024**2
    )
    if not verify_fragments:
        return ds
    fragments = ds.get_fragments()
    if len(fragments) > 1024:
        raise ValueError("Native fragment metadata exceeds preview budget")
    files = 1  # The fixed version manifest plus its referenced data/sidecar files.
    for fragment in fragments:
        for data_file in fragment.data_files():
            files += 1
            if files > maximum_files:
                raise ValueError('Native table file count exceeds preview budget')
            path = PurePosixPath(data_file.path)
            if (
                path.is_absolute()
                or ".." in path.parts
                or ":" in data_file.path
                or getattr(data_file, "base_id", None) is not None
            ):
                raise ValueError(
                    "External fragment locations are not covered by local preview"
                )
            _fragment_file(root, 'data', data_file.path)
        deletion = fragment.metadata.deletion_file
        if deletion is not None:
            files += 1
            if files > maximum_files:
                raise ValueError('Native table file count exceeds preview budget')
            if deletion.base_id is not None or deletion.file_type not in {'array', 'bitmap'}:
                raise ValueError('External deletion locations are not covered')
            extension = 'arrow' if deletion.file_type == 'array' else 'bin'
            _fragment_file(root, '_deletions', f'{fragment.fragment_id}-{deletion.read_version}-{deletion.id}.{extension}')
        if fragment.metadata.overlays or fragment.metadata.row_id_meta is not None:
            raise ValueError(
                "Overlay/stable-row-id metadata requires additional preview semantics"
            )
    return ds


def snapshot_metadata(uri, version):
    """Fixed manifest facts only, never authorization to read fragment content.

    Used to verify native committed outputs in a separately bounded worker.
    Unlike Case preview, this does not enumerate/open all fragment paths.
    """
    _guard()
    if type(version) is not int or version < 1:
        raise ValueError('Snapshot metadata requires a fixed positive version')
    ds = _open(uri, version, verify_fragments=False)
    from .schema import schema_identity
    return {'version': ds.version, 'row_count': ds.count_rows(),
            'schema_hash': schema_hash(ds.schema), 'schema_identity': schema_identity(ds.schema)}


def describe_table(uri, version):
    _guard()
    if version is not None and (type(version) is not int or version < 1):
        raise ValueError(
            "Version must be a fixed positive integer or an explicit attach-time head"
        )
    ds = _open(uri, version)
    from ..data.flow_contract import arrow_type, field, scalar

    if len(ds.schema) > 2048:
        raise ValueError("Schema field budget exceeded")
    fields = {}
    for f in ds.schema:
        try:
            type_ = arrow_type(f.type, nullable=f.nullable)
        except ValueError:
            type_ = {
                **scalar("unsupported", nullable=f.nullable),
                "physical": str(f.type),
            }
        fields[f.name] = field(type_, evidence="metadata")
    return {
        "version": ds.version,
        "row_count": ds.count_rows(),
        "schema_hash": schema_hash(ds.schema),
        "fields": fields,
    }


def catalog_reference(root, relative_uri, version, *, dataset_id=None):
    _guard()
    from .registry import REGISTRY_RELATIVE, _sql_string, _ref_from_row

    path = Path(root) / REGISTRY_RELATIVE
    if not path.exists():
        return None
    ds = _open(path, None)
    expression = (
        f"relative_uri = {_sql_string(relative_uri)} AND lance_version = {int(version)}"
    )
    if dataset_id is not None:
        expression += " AND dataset_id = " + _sql_string(dataset_id)
    # At most two rows are needed to diagnose ambiguous/duplicate identities.
    rows = _rows(ds, None, offset=0, limit=2, filter=expression, maximum=64 * 1024)
    if len(rows) > 1:
        raise ValueError("Ambiguous catalog identity; select an explicit dataset_id")
    return _ref_from_row(rows[0]) if rows else None


def _json_value(value, depth=0):
    if depth > 16:
        raise ValueError("Case nesting exceeds budget")
    if isinstance(value, bytes):
        return {"type": "bytes", "base64": base64.b64encode(value).decode()}
    if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
        return value.isoformat()
    if isinstance(value, dict):
        return {k: _json_value(v, depth + 1) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(v, depth + 1) for v in value]
    if type(value) is float and not math.isfinite(value):
        return {"type": "float", "value": str(value)}
    return value


def _column(schema, name):
    """Resolve a literal column or a bounded struct path; never accept SQL."""
    import pyarrow as pa
    if type(name) is not str or not 1 <= len(name) <= 256:
        raise ValueError('Invalid preview column')
    parts = [name] if name in schema.names else name.split('.')
    if len(parts) > 8 or any(not part for part in parts):
        raise ValueError('Preview field path exceeds its bound')
    current = schema
    for i, part in enumerate(parts):
        try:
            value = current.field(part).type
        except (KeyError, IndexError):
            raise ValueError('Unknown preview column: ' + name) from None
        if i + 1 < len(parts):
            if not pa.types.is_struct(value):
                raise ValueError('Nested preview projection requires struct fields')
            current = value
    return '.'.join('`' + part.replace('`', '``') + '`' for part in parts)


def _rows(ds, columns, *, offset, limit, filter=None, maximum=256 * 1024):
    _guard()
    if (
        type(offset) is not int
        or offset < 0
        or type(limit) is not int
        or not 1 <= limit <= 100
    ):
        raise ValueError("Invalid preview page")
    names = list(ds.schema.names) if columns is None else columns
    if (
        not isinstance(names, list)
        or len(names) > 16
        or any(type(c) is not str for c in names) or len(set(names)) != len(names)
    ):
        raise ValueError("Preview requires up to 16 existing columns")
    rows = []
    used = 0
    scanner = ds.scanner(
        columns={name: _column(ds.schema, name) for name in names},
        offset=offset,
        limit=limit,
        filter=filter,
        batch_size=1,
        batch_size_bytes=min(maximum, 64 * 1024),
        batch_readahead=1,
        fragment_readahead=1,
        io_buffer_size=1024**2,
        with_row_id=True,
        scan_in_order=True,
        use_scalar_index=False,
    )
    reader = scanner.to_batches()
    try:
        for batch in reader:
            # Check native buffers before conversion to Python values, which can
            # require multiple copies. The worker allocation limit covers the
            # preceding native decode, including single values above this cap.
            if batch.nbytes > maximum or batch.get_total_buffer_size() > 4 * maximum:
                raise MemoryError("Case native buffers exceed read budget")
            for row in batch.to_pylist():
                if "_rowid" in row:
                    row["_rowid"] = str(row["_rowid"])
                row = _json_value(row)
                raw = json.dumps(row, ensure_ascii=False, allow_nan=False).encode()
                if len(raw) > maximum - used:
                    raise MemoryError(
                        "Case response exceeds byte budget; narrow the projection/page"
                    )
                used += len(raw)
                rows.append(row)
                if len(rows) > limit:
                    raise RuntimeError("Native preview exceeded its row limit")
    finally:
        close = getattr(reader, "close", None)
        if close:
            close()
    return rows


def sample_table(
    uri, ref, *, columns=None, offset=0, limit=20, row_id=None, match=None
):
    _guard()
    ref = DatasetRef.from_dict(ref)
    ds = _open(uri, ref.lance_version)
    if schema_hash(ds.schema) != ref.schema_hash or ds.count_rows() != ref.row_count:
        raise ValueError("Pinned table metadata does not match reference")
    expression = None
    if row_id is not None:
        if type(row_id) is not int or row_id < 0:
            raise ValueError("Invalid row locator")
        expression = f"_rowid = {row_id}"
    if match is not None:
        if (
            row_id is not None
            or not isinstance(match, dict)
            or set(match) != {"field", "value"}
        ):
            raise ValueError("Invalid comparison key")
        name = _column(ds.schema, match['field'])
        value = match["value"]
        if value is None:
            expression = name + " IS NULL"
        elif type(value) in (str, int, bool):
            from .registry import _sql_string

            literal = _sql_string(value) if type(value) is str else str(value).lower()
            expression = name + " = " + literal
        else:
            raise ValueError(
                "Comparison keys must be explicit string/integer/boolean/null values"
            )
    names = columns if columns is not None else list(ds.schema.names)[:16]
    rows = _rows(
        ds,
        names,
        offset=offset,
        limit=1 if row_id is not None else limit,
        filter=expression,
        maximum=1024**2 if row_id is not None else 256 * 1024,
    )
    return {
        "version": ref.lance_version,
        "schema_hash": ref.schema_hash,
        "row_count": ref.row_count,
        "offset": offset,
        "limit": limit,
        "columns": names,
        "rows": rows,
        "has_more": (
            len(rows) == limit if expression else offset + len(rows) < ref.row_count
        ),
        "match": match,
        "allocation_guard": "isolated address-space limit; native batch bytes are approximate",
    }


def image_preview(path):
    """Validate encoded size and image header before pixel allocation."""
    _guard()
    import io
    import os
    import stat
    from PIL import Image

    path = Path(path)
    if path.suffix.lower() not in {".png", ".jpg", ".jpeg", ".webp", ".gif"}:
        raise ValueError("Unsupported image format")
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor, "rb") as source:
        info = os.fstat(source.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_size > 8 * 1024**2:
            raise ValueError("Image encoded size exceeds 8 MiB")
        raw = source.read(8 * 1024**2 + 1)
        after = os.fstat(source.fileno())
        if (
            len(raw) > 8 * 1024**2
            or len(raw) != info.st_size
            or (after.st_size, after.st_mtime_ns, after.st_ctime_ns)
            != (info.st_size, info.st_mtime_ns, info.st_ctime_ns)
        ):
            raise ValueError("Image changed or exceeds budget")
    return image_bytes_preview(raw)


def image_bytes_preview(raw):
    """Decode an already verified bounded object, including extensionless hashes."""
    _guard()
    import io
    from PIL import Image
    if type(raw) is not bytes or len(raw) > 8 * 1024**2:
        raise ValueError('Image encoded size exceeds 8 MiB')
    image = Image.open(io.BytesIO(raw))
    if image.format not in {'PNG', 'JPEG', 'WEBP', 'GIF'}:
        image.close()
        raise ValueError('Unsupported image format')
    if image.width * image.height > 16_000_000 or image.width < 1 or image.height < 1:
        raise ValueError("Image pixel budget exceeded")
    with image:
        image.load()
        image.thumbnail((1000, 1000))
        output = io.BytesIO()
        image.convert("RGB").save(output, format="JPEG", quality=85)
        if output.tell() > 512 * 1024:
            raise ValueError("Thumbnail exceeds 512 KiB")
        return {
            "mime": "image/jpeg",
            "data": base64.b64encode(output.getvalue()).decode(),
            "width": image.width,
            "height": image.height,
            "media_identity": "external file; table version alone does not pin its contents",
        }
