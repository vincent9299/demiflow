"""Bounded index publication; the caller owns one atomic SQLite transaction."""
import json

from .documents import canonical

# These bound the additional lookup/serialization buffers, not the caller's
# prepared input, returned receipts, SQLite cache or process RSS.
_ROWS = 256
_KEYS = 2048
_BYTES = 4 * 1024 * 1024
_PARAMETERS = 256


def _json_bound(value, budget):
    """Conservative UTF-8 JSON size without first encoding a large value."""
    if isinstance(value, str):return 6 * len(value) + 2
    if value is None or isinstance(value, bool):return 5
    if isinstance(value, int):return value.bit_length() // 3 + 3
    if isinstance(value, float):return 32
    if isinstance(value, (list, tuple, dict)):
        size=2
        values=(v for pair in value.items() for v in pair) if isinstance(value,dict) else iter(value)
        for child in values:
            size+=_json_bound(child,budget-size)+2
            if size>budget:break
        return size
    return budget+1  # Preserve canonical()'s own error on the ordinary path.


def _windows(prepared):
    window=[];keys=0;size=0
    for item in prepared:
        n=len(item.get('urls',()))+int(item['kind']=='document')
        weight=_json_bound(item,_BYTES)
        if window and (len(window)>=_ROWS or keys+n>_KEYS or size+weight>_BYTES):
            yield window;window=[];keys=0;size=0
        # No extra batch lookup buffer for unusually wide native metadata.
        # The ordinary one-row path keeps its existing validation/size contract.
        if n>_KEYS or weight>_BYTES:
            yield None,item
        else:
            window.append(item);keys+=n;size+=weight
    if window:yield window


def _lookup(db, table, key, keys, columns):
    """Private identifiers are constants; only bounded key values are bound."""
    ordered=sorted(keys)
    for offset in range(0,len(ordered),_PARAMETERS):
        chunk=ordered[offset:offset+_PARAMETERS]
        marks=','.join('?' for _ in chunk)
        yield from db.execute(f'SELECT {columns} FROM {table} WHERE {key} IN ({marks})',chunk)


def publish_window(db, items, ordinary):
    ids={item['identity'] for item in items if item['kind']=='document'}
    urls={url for item in items if item['kind']=='redirect' for url in item['urls']}
    # Check stored metadata sizes before retrieving them. The write transaction
    # prevents another publisher from changing the rows between these queries.
    sizes=sum(r[1] for r in _lookup(db,'documents','id',ids,'id,length(CAST(receipt AS BLOB))'))
    sizes+=sum(r[1]+r[2] for r in _lookup(db,'redirects','url',urls,
        'url,length(CAST(target_url AS BLOB)),length(CAST(receipt AS BLOB))'))
    if sizes>_BYTES:
        return [ordinary(db,item) for item in items]
    documents=dict(_lookup(db,'documents','id',ids,'id,receipt'))
    redirects={url:(target,receipt) for url,target,receipt in
        _lookup(db,'redirects','url',urls,'url,target_url,receipt')}
    new_documents={};new_urls=set();new_redirects={};output=[]
    for item in items:
        receipt=item['receipt'];kind=item['kind']
        if kind=='failure':output.append(receipt);continue
        if kind=='document':
            identity=item['identity']
            if identity not in documents:
                encoded=canonical(receipt)
                documents[identity]=encoded
                new_documents[identity]=(identity,item['observed'],item['parser'],encoded)
            new_urls.update((url,identity) for url in item['urls'])
            saved=documents[identity]
        else:
            if any(url in redirects and redirects[url][0]!=item['target'] for url in item['urls']):
                output.append({'url':receipt['url'],'status':'invalid_document',
                    'reason':'conflicting_redirect_target','attempts':[]})
                continue
            encoded=None
            for url in item['urls']:
                if url not in redirects:
                    if encoded is None:encoded=canonical(receipt)
                    redirects[url]=(item['target'],encoded)
                    new_redirects[url]=(url,item['target'],encoded)
            saved=redirects[receipt['url']][1]
        output.append(json.loads(saved))
    # Sorting changes physical insertion order only: logical first-wins and
    # conflicts above follow input order, including overlapping aliases.
    db.executemany('INSERT INTO documents VALUES (?, ?, ?, ?)',
        (new_documents[key] for key in sorted(new_documents)))
    db.executemany('INSERT OR IGNORE INTO urls VALUES (?, ?)',sorted(new_urls))
    db.executemany('INSERT INTO redirects VALUES (?, ?, ?)',
        (new_redirects[key] for key in sorted(new_redirects)))
    return output


def publish_prepared(db, prepared, ordinary):
    output=[]
    for window in _windows(prepared):
        if isinstance(window,tuple):output.append(ordinary(db,window[1]))
        elif len(window)==1:output.append(ordinary(db,window[0]))
        else:output.extend(publish_window(db,window,ordinary))
    return output
