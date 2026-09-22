"""Consolidate local, schema-identical datasets into one independent Lance table.

Immutable data/Blob files are hard-linked into the new table. A single manifest
owns every fragment; source paths are not retained as runtime dependencies.
This deliberately rejects layouts whose identities cannot be preserved safely.
"""
from pathlib import Path
import json
import os
import shutil
import uuid


def _field_ids(schema):
    def walk(fields,prefix=''):
        for f in fields:
            yield (prefix+f.name(),f.id())
            yield from walk(f.children(),prefix+f.name()+'.')
    return tuple(walk(schema.fields()))


def consolidate_local_tables(sources,destination):
    import lance
    sources=list(sources)
    if not sources:raise ValueError('At least one source table is required')
    target=Path(destination).resolve()
    if target.exists():raise FileExistsError(target)
    schema=sources[0].schema;lance_schema=sources[0].lance_schema
    identity=_field_ids(lance_schema)
    roots=[]
    for ds in sources:
        root=Path(ds.uri).resolve()
        if not root.is_dir() or target.is_relative_to(root) or root.is_relative_to(target):
            raise ValueError('Consolidation requires independent local table paths')
        if not ds.schema.equals(schema,check_metadata=True) or _field_ids(ds.lance_schema)!=identity:
            raise ValueError('Source schemas and field IDs must match')
        roots.append(root)
    if len(set(roots))!=len(roots):raise ValueError('Duplicate source table')
    attempt=target.with_name(target.name+'.attempt-'+uuid.uuid4().hex)
    attempt.mkdir(parents=True)
    fragments=[];deleted=[];expected=sum(ds.count_rows() for ds in sources)
    def link(a,b):
        if a.is_symlink():raise ValueError('Symlink in source table')
        b.parent.mkdir(parents=True,exist_ok=True)
        if b.exists():
            if not os.path.samefile(a,b):raise ValueError('Conflicting data file name: '+a.name)
        else:os.link(a,b)
    try:
        for root,ds in zip(roots,sources):
            for fragment in ds.get_fragments():
                meta=fragment.metadata.to_json();old_id=meta['id'];new_id=len(fragments)
                if any(meta.get(k) for k in ('overlays','row_id_meta','created_at_version_meta','last_updated_at_version_meta')):
                    raise ValueError('Stable row IDs, overlays or row version metadata require a rewrite')
                for f in meta['files']:
                    if f.get('base_id') is not None or Path(f['path']).is_absolute() or '..' in Path(f['path']).parts:
                        raise ValueError('External data files require explicit materialization')
                    source=root/'data'/f['path'];link(source,attempt/'data'/f['path'])
                    # Blob v2 payload files use a sibling directory named after
                    # the data file stem. Preserve these relative descriptors.
                    blob_dir=source.with_suffix('')
                    if blob_dir.exists():
                        for entry in blob_dir.rglob('*'):
                            if entry.is_symlink():raise ValueError('Symlink in Blob payload')
                            if entry.is_file():link(entry,attempt/'data'/entry.relative_to(root/'data'))
                deletion=meta.get('deletion_file')
                if deletion:
                    if deletion.get('base_id') is not None:raise ValueError('External deletion file')
                    extension={'array':'arrow','bitmap':'bin'}[deletion['file_type']]
                    suffix=f"-{deletion['read_version']}-{deletion['id']}.{extension}"
                    link(root/'_deletions'/f'{old_id}{suffix}',attempt/'_deletions'/f'{new_id}{suffix}')
                meta['id']=new_id
                if deletion:deleted.append(lance.FragmentMetadata.from_json(json.dumps(meta)))
                meta['deletion_file']=None
                fragments.append(lance.FragmentMetadata.from_json(json.dumps(meta)))
        ds=lance.LanceDataset.commit(str(attempt),lance.LanceOperation.Overwrite(lance_schema,fragments),read_version=0)
        if deleted:
            ds=lance.LanceDataset.commit(str(attempt),lance.LanceOperation.Delete(deleted,[], 'preserve source deletion masks'),read_version=ds.version)
        if ds.count_rows()!=expected or not ds.schema.equals(schema,check_metadata=True):
            raise ValueError('Consolidated schema/count mismatch')
        actual=ds.get_fragments()
        if [f.fragment_id for f in actual]!=list(range(len(fragments))):raise ValueError('Unexpected fragment identities')
        attempt.rename(target)
        return lance.dataset(str(target))
    except BaseException:
        if attempt.exists():shutil.rmtree(attempt)
        raise
