"""Read-only query of a local SQLite database as a search source (no HTTP).

Options: ``database`` (file path), ``query_str`` (SQL with the named
parameters ``:query`` and/or ``:wildcard``; paging is appended as
``LIMIT :limit OFFSET :offset``), ``limit`` (rows per page) and
``result_type``: ``MainResult`` turns each row into a result with the row's
columns as fields (``url``, ``title``, ``content`` ...), ``KeyValue`` keeps the
row as a key/value table.
"""
import sqlite3
from pathlib import Path

engine_type = 'offline'
paging = True
categories = ['general']
about = {'website': 'https://www.sqlite.org/', 'results': 'SQL'}
database = None
query_str = None
limit = 10
result_type = 'MainResult'


def setup(settings):
    return result_type in ('MainResult', 'KeyValue') and type(limit) is int and 0 < limit <= 1000


def search(query, params):
    uri = Path(database).resolve().as_uri() + '?mode=ro'
    values = {'query': query, 'wildcard': '%' + query.replace(' ', '%') + '%',
              'limit': limit, 'offset': (params['pageno'] - 1) * limit}
    connection = sqlite3.connect(uri, uri=True)
    try:
        cursor = connection.execute(query_str + ' LIMIT :limit OFFSET :offset', values)
        columns = [column[0] for column in cursor.description]
        rows = [dict(zip(columns, map(str, row))) for row in cursor.fetchmany(limit)]
    finally:
        connection.close()
    if result_type == 'MainResult':
        return rows
    return [{'template': 'keyvalue.html', 'kvmap': row} for row in rows]
