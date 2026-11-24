"""Read-only query of a local SQLite database as a search source (no HTTP).

Options: ``database`` (file path), ``query_str`` (a ``SELECT`` with the named
parameters ``:query`` and/or ``:wildcard``; paging is appended as
``LIMIT :limit OFFSET :offset``), ``limit`` (rows per page) and
``result_type``: ``KeyValue`` (default) returns each row as a key/value table,
``MainResult`` turns the row's columns into result fields (``url``, ``title``,
``content`` ...; a column that is no result field is an error).

Rows are read from the database one at a time as the worker takes results, and
the worker stops at the session's result budget: more than ``max_results`` rows,
or more encoded bytes than a reply may have (``output_limit()``), end the search
as ``response_too_large`` one row past the budget, not after the page (a negative
``limit`` is no page size to SQLite, so the budget is the only bound then).

A value, or a stored row, longer than a reply may be is refused by SQLite before
it is read into memory (its length limit is set to ``output_limit()``), also for
a value the query builds. A value just inside the limit is read whole; a row the
query puts together from several columns counts against the budget once read.
"""
import sqlite3
from pathlib import Path

from demiflow.collect.native_search.adapter import ResponseTooLarge, key_value, main_result, output_limit

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
        longest = sqlite3.SQLITE_LIMIT_LENGTH  # a value or row longer than a reply may be is not read at all
        connection.setlimit(longest, min(output_limit(), connection.getlimit(longest)))
        cursor = connection.execute(query_str + ' LIMIT :limit OFFSET :offset', values)
        columns = [column[0] for column in cursor.description]
        for record in cursor:
            row = dict(zip(columns, map(str, record)))
            yield main_result(row.pop('url', None), **row) if result_type == 'MainResult' else key_value(row)
    except sqlite3.DataError as exc:
        if getattr(exc, 'sqlite_errorcode', None) != sqlite3.SQLITE_TOOBIG:
            raise
        raise ResponseTooLarge() from None
    finally:
        connection.close()
