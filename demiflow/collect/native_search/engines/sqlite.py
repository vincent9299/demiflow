"""Read-only query of a local SQLite database as a search source (no HTTP).

Options: ``database`` (file path), ``query_str`` (a ``SELECT`` with the named
parameters ``:query`` and/or ``:wildcard``; paging is appended as
``LIMIT :limit OFFSET :offset``), ``limit`` (rows per page) and
``result_type``: ``KeyValue`` (default) returns each row as a key/value table,
``MainResult`` turns the row's columns into result fields (``url``, ``title``,
``content`` ...; a column that is no result field is an error).

Two limits apply, and they are separate.

What the query returns is bounded by the session. Rows are handed to the worker
one at a time. SQLite adds up the text and blob bytes of each row before the row
leaves it; the row that would take the search past the bytes one reply may have
(``output_limit()``) is not handed over, however many columns it has, and the
search ends as ``response_too_large``. So no more text and blob bytes than one
reply may have are read into Python. The worker counts the encoded results the
same way and stops one row past ``max_results`` (a negative ``limit`` is no page
size to SQLite, so these budgets are the only bound then). In a UTF-16 database
text is counted at half its stored size, which is never more than its UTF-8 size,
and so is a long blob: up to three times the limit can be read there.

What the query reads on the way is not bounded by the reply: sort keys, values it
only measures or filters on and the schema may be longer than a reply. For this
work SQLite may handle single values and records of up to ``VALUE_LIMIT`` bytes
(of one reply, where a reply may be larger); a query that needs a longer one ends
as ``resource_limit`` before SQLite reads it into memory. Within that limit SQLite
holds the row it is computing and one more copy of a value while it measures it:
its own memory for one row can be that limit for every column of the query, not
the size of a reply. Sorting may use SQLite's temporary files.
"""
import sqlite3
from pathlib import Path

from demiflow.collect.native_search.adapter import ResponseTooLarge, SourceError, key_value, main_result, output_limit

engine_type = 'offline'
paging = True
about = {'website': 'https://www.sqlite.org/', 'results': 'SQL'}
database = ''
query_str = ''
limit = 10
result_type = 'KeyValue'
# Bytes of one value or record SQLite may handle while it runs a query, whatever the reply may have.
VALUE_LIMIT = 32 * 1024 * 1024
# Put after a query text: a comment left open at its end closes here, before anything that follows.
ENDING = '\n/**/'


class ValueTooLarge(SourceError):
    """The query needs a value or a record longer than SQLite may handle for one search."""
    status = 'resource_limit'


def setup(settings):
    if 'query_str' not in settings:
        raise ValueError('query_str cannot be empty')
    if not settings['query_str'].lower().startswith('select '):
        raise ValueError('only SELECT query is supported')


def _sum(terms):
    """``terms`` added up, grouped so that the expression stays shallow for any number of them."""
    while len(terms) > 1:
        terms = ['(' + ' + '.join(terms[i:i + 8]) + ')' for i in range(0, len(terms), 8)]
    return terms[0]


def _row_bytes(columns, taken_names, scale):
    """SQL for the text and blob bytes of ``columns`` (quoted names) of the current row, as reply bytes.

    Every value is measured once, one after the other, in one subquery of its own (``LIMIT -1 OFFSET 0`` keeps
    SQLite from merging it and measuring twice): SQLite then holds one copy of one value at a time besides
    the row itself. Only text and blobs count; a short value is checked for its type (numbers are short), a
    longer one is text or a blob. ``scale`` is 2 for UTF-16 text; a long blob is halved there as well.
    """
    prefix = 'demiflow_n'
    while any(name.lower().startswith(prefix) for name in taken_names):
        prefix += '_'
    sizes = [prefix + str(index) for index in range(len(columns))]
    measured = ', '.join(f'length(CAST({column} AS BLOB)) AS {size}' for column, size in zip(columns, sizes))
    counted = _sum([f"CASE WHEN {size} > {32 * scale} THEN {size} / {scale}"
                    f" WHEN typeof({column}) = 'text' THEN {size} / {scale}"
                    f" WHEN typeof({column}) = 'blob' THEN {size} ELSE 0 END" for column, size in zip(columns, sizes)])
    return f'(SELECT {counted} FROM (SELECT {measured} LIMIT -1 OFFSET 0))'


def _guarded(connection, values, scale):
    """The fields of the query's rows, and the statement that gives every row's text and blob bytes to
    ``demiflow_taken`` before the row leaves SQLite (the query runs inside it as it would by itself)."""
    # The column names the query gives, and the unique names SQLite gives the same columns of a subquery.
    names = [column[0] for column in connection.execute(query_str + ENDING + ' LIMIT 0', values).description]
    inner = [column[0] for column in
             connection.execute(f'SELECT * FROM ({query_str}{ENDING}) LIMIT 0', values).description]
    fields = list(dict.fromkeys(names))  # a name given twice is one field: in its first place, with its last column
    last = {name: '`' + inner[index].replace('`', '``') + '`' for index, name in enumerate(names)}
    columns = [last[name] for name in fields]
    first = f'CASE WHEN demiflow_taken({_row_bytes(columns, inner, scale)}) >= 0 THEN {columns[0]} END'
    return fields, (f"SELECT {', '.join([first, *columns[1:]])} "
                    f"FROM ({query_str} LIMIT :limit OFFSET :offset{ENDING})")


def search(query, params):
    uri = Path(database).resolve().as_uri() + '?mode=ro'
    values = {'query': query, 'wildcard': '%' + query.replace(' ', '%') + '%',
              'limit': limit, 'offset': (params['pageno'] - 1) * limit}
    room = [output_limit()]

    def taken(size):
        """Called by SQLite with the text and blob bytes of a row before the row leaves it."""
        room[0] -= size
        if room[0] < 0:
            raise ResponseTooLarge()
        return size

    connection = sqlite3.connect(uri, uri=True)
    try:
        # One snapshot for the whole search. Reading the encoding reads the schema, with no limit of ours yet:
        # nothing the database defines besides what the query uses can fail the query.
        connection.execute('BEGIN')
        scale = 2 if connection.execute('PRAGMA encoding').fetchone()[0].startswith('UTF-16') else 1
        longest = sqlite3.SQLITE_LIMIT_LENGTH
        needed = scale * max(output_limit(), len(values['wildcard'].encode()))  # a reply and the search text fit
        connection.setlimit(longest, min(max(VALUE_LIMIT, needed), connection.getlimit(longest)))
        connection.create_function('demiflow_taken', 1, taken)
        fields, statement = _guarded(connection, values, scale)
        for record in connection.execute(statement, values):
            row = dict(zip(fields, map(str, record)))
            yield main_result(row.pop('url', None), **row) if result_type == 'MainResult' else key_value(row)
    except sqlite3.DataError as exc:
        if getattr(exc, 'sqlite_errorcode', None) != sqlite3.SQLITE_TOOBIG:
            raise
        raise ValueTooLarge() from None
    except sqlite3.OperationalError:
        if room[0] >= 0:
            raise
        raise ResponseTooLarge() from None
    finally:
        connection.close()
