"""Read-only query of a local SQLite database as a search source (no HTTP).

Options: ``database`` (file path), ``query_str`` (a ``SELECT`` with the named
parameters ``:query`` and/or ``:wildcard``; paging is appended as
``LIMIT :limit OFFSET :offset``), ``limit`` (rows per page) and
``result_type``: ``KeyValue`` (default) returns each row as a key/value table,
``MainResult`` turns the row's columns into result fields (``url``, ``title``,
``content`` ...; a column that is no result field is an error).
"""
import sqlite3
from pathlib import Path

from demiflow.collect.native_search.adapter import key_value, main_result

engine_type = 'offline'
paging = True
about = {'website': 'https://www.sqlite.org/', 'results': 'SQL'}
database = ''
query_str = ''
limit = 10
result_type = 'KeyValue'


def setup(settings):
    if 'query_str' not in settings:
        raise ValueError('query_str cannot be empty')
    if not settings['query_str'].lower().startswith('select '):
        raise ValueError('only SELECT query is supported')


def search(query, params):
    uri = Path(database).resolve().as_uri() + '?mode=ro'
    values = {'query': query, 'wildcard': '%' + query.replace(' ', '%') + '%',
              'limit': limit, 'offset': (params['pageno'] - 1) * limit}
    connection = sqlite3.connect(uri, uri=True)
    try:
        cursor = connection.execute(query_str + ' LIMIT :limit OFFSET :offset', values)
        columns = [column[0] for column in cursor.description]
        rows = [dict(zip(columns, map(str, row))) for row in cursor.fetchall()]
    finally:
        connection.close()
    if result_type == 'MainResult':
        return [main_result(row.pop('url', None), **row) for row in rows]
    return [key_value(row) for row in rows]
