"""Managed key-only exclusion followed by bounded reads of pinned Lance rows."""
from contextlib import closing


def exclude_lance_rows(engine, source):
    import lance
    import pyarrow as pa
    from ..data.plan import LogicalPlan
    from ..data.sources import LanceSource
    from ..lance.arrow_batches import LANCE_FILE_ROWS
    from ..lance.model import LanceScanSpec
    from ..lance.storage import open_lance_dataset
    from .local_kernel import PlanInput

    query=source.left.source.query
    snapshot=open_lance_dataset(query.uri,query.version,query.storage_options)
    columns=list(query.columns or snapshot.schema.names)
    for name in (*source.on,*columns):
        if name not in snapshot.schema.names:raise KeyError(name)
    batch_rows=min(query.batch_size or 1024,8192)
    aliases=[engine.name('key') for _ in source.on]
    row_id,ordinal=engine.name('rowid'),engine.name('ordinal')
    schema=pa.schema([*(pa.field(alias,snapshot.schema.field(name).type)
        for name,alias in zip(source.on,aliases)),(row_id,pa.uint64()),(ordinal,pa.uint64())])
    narrow_path=engine.directory/(engine.name('lance_keys')+'.lance')
    scanned=0
    def key_batches():
        nonlocal scanned
        scanner=snapshot.scanner(columns=list(source.on),filter=query.filter,limit=query.limit,
            with_row_id=True,scan_in_order=True,batch_size=query.batch_size or 8192,
            batch_readahead=query.batch_readahead or 1,fragment_readahead=query.fragment_readahead or 1)
        with closing(scanner.to_batches()) as batches:
            for batch in batches:
                size=batch.num_rows
                yield pa.RecordBatch.from_arrays([*(batch.column(name) for name in source.on),
                    batch.column('_rowid'),pa.array(range(scanned,scanned+size),type=pa.uint64())],schema=schema)
                scanned+=size
    narrow=lance.write_dataset(pa.RecordBatchReader.from_batches(schema,key_batches()),str(narrow_path),
        mode='create',schema=schema,max_rows_per_file=LANCE_FILE_ROWS)
    engine.stats['stages'].append({'name':'lance_key_scan','source_uri':query.uri,
        'source_version':snapshot.version,'rows_output':scanned,'key_columns':list(source.on),
        'payload_columns_in_relation':0})
    left=engine.typed(PlanInput(LanceSource(LanceScanSpec(uri=str(narrow_path),version=narrow.version)),LogicalPlan()))
    left=engine.keyed(left,aliases)
    right=engine.input(source.right,source.right_on)
    lk,lv=left.keys[tuple(aliases)]
    rk,rv=right.keys[tuple(source.right_on)]
    fields={c.name:c.value for c in left.columns}
    # Native SQL is private to this platform node. Its only output is surviving
    # row IDs, ordered by the original filtered scan ordinal, not by body/key.
    result=engine.session.query(
        f'SELECT l.{fields[row_id]} AS selected_row_id FROM {left.view} l '
        f'LEFT ANTI JOIN {right.view} r ON l.{lk}=r.{rk} AND l.{lv} AND r.{rv} '
        f'ORDER BY l.{fields[ordinal]}',sources=engine.sources,views=engine.views,
        label='dataset_exclude_keys',_dataset_spec={'keys':engine.key_functions})
    engine.record(result,'keys_only_exclusion')
    hydrated={'name':'lance_payload_read','source_uri':query.uri,'source_version':snapshot.version,
        'rows_output':0,'batches':0,'max_batch_rows':0,'max_batch_bytes':0}
    try:
        with closing(lance.dataset(**result.source()).to_batches(batch_size=batch_rows,
                batch_readahead=1,fragment_readahead=1)) as batches:
            for batch in batches:
                # _take_rows uses Lance row IDs (not offsets or physical row
                # addresses), so deletion/compaction and stable IDs stay valid.
                table=snapshot._take_rows(batch.column('selected_row_id'),columns=columns)
                if table.num_rows!=batch.num_rows:
                    raise RuntimeError('Pinned Lance row lookup did not return every surviving row')
                hydrated['rows_output']+=table.num_rows;hydrated['batches']+=1
                hydrated['max_batch_rows']=max(hydrated['max_batch_rows'],table.num_rows)
                hydrated['max_batch_bytes']=max(hydrated['max_batch_bytes'],table.nbytes)
                yield from table.to_pylist()
    finally:
        engine.stats['stages'].append(hydrated)
