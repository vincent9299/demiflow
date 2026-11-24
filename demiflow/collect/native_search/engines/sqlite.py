"""Read-only query of a local SQLite database as a search source (no HTTP).

Options: ``database`` (file path), ``query_str`` (a ``SELECT`` with the named
parameters ``:query`` and/or ``:wildcard``; paging is appended as
``LIMIT :limit OFFSET :offset``; a statement that cannot stand as a subquery,
such as ``EXPLAIN``, is no query of a source), ``limit`` (rows per page) and
``result_type``: ``KeyValue`` (default) returns each row as a key/value table,
``MainResult`` turns the row's columns into result fields (``url``, ``title``,
``content`` ...; a column that is no result field is an error).

Two limits apply, and they are separate.

What the query returns is bounded by the session. Rows are handed to the worker
one at a time. SQLite adds up the text and blob bytes of each row before the row
leaves it; the row that would take the search past the bytes one reply may have
(``output_limit()``) is not handed over, however many columns it has, and the
search ends as ``response_too_large``. The worker counts the encoded results the
same way and stops one row past ``max_results`` (a negative ``limit`` is no page
size to SQLite, so these budgets are the only bound then).

How exact that count is depends on the encoding of the database. In a UTF-8
database (the default) it is exact: no more text and blob bytes than one reply
may have are read into Python. In a UTF-16 database text is counted at half its
stored size, which is never more than its UTF-8 size, and so is a long blob: text
of up to three times the limit, or a blob of twice the limit, can be read into
Python there before the worker's own count refuses it.

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


def _row_bytes(indexes, scale):
    """SQL for the text and blob bytes of the columns ``c<index>`` of the current row, as reply bytes.

    Every value is measured once, one after the other, in one subquery of its own (``LIMIT -1 OFFSET 0`` keeps
    SQLite from merging it and measuring twice): SQLite then holds one copy of one value at a time besides
    the row itself. Only text and blobs count; a short value is checked for its type (numbers are short), a
    longer one is text or a blob. ``scale`` is 2 for UTF-16 text; a long blob is halved there as well.
    """
    measured = ', '.join(f'length(CAST(c{index} AS BLOB)) AS n{index}' for index in indexes)
    counted = _sum([f"CASE WHEN n{index} > {32 * scale} THEN n{index} / {scale}"
                    f" WHEN typeof(c{index}) = 'text' THEN n{index} / {scale}"
                    f" WHEN typeof(c{index}) = 'blob' THEN n{index} ELSE 0 END" for index in indexes])
    return f'(SELECT {counted} FROM (SELECT {measured} LIMIT -1 OFFSET 0))'


def _statement(text):
    """``text`` without the semicolon that ends its statement and what follows it (SQLite tells where that is)."""
    end = text.find(';')
    while end >= 0 and not sqlite3.complete_statement(text[:end + 1]):
        end = text.find(';', end + 1)
    return text if end < 0 else text[:end]


def _guarded(connection, values, scale):
    """The fields of the query's rows, and the statement that gives every row's text and blob bytes to
    ``demiflow_taken`` before the row leaves SQLite (the query runs inside it as it would by itself)."""
    asked = query_str + ' LIMIT :limit OFFSET :offset'  # the text a search of this source has always run
    # Its column names, read from that very text with a page of no rows: nothing is added to it, nothing runs.
    probe = connection.execute(asked, {**values, 'limit': 0})
    names = [column[0] for column in probe.description]
    probe.close()
    fields = list(dict.fromkeys(names))  # a name given twice is one field: in its first place, with its last column
    last = {name: index for index, name in enumerate(names)}
    indexes = [last[name] for name in fields]
    # The query's columns get names of ours by position, so no name SQLite or the query chose is referred to.
    rows = 'demiflow_rows'
    while rows in asked.lower():
        rows += '_'
    first = f'CASE WHEN demiflow_taken({_row_bytes(indexes, scale)}) >= 0 THEN c{indexes[0]} END'
    return fields, (f"WITH {rows}({', '.join(f'c{index}' for index in range(len(names)))}) "
                    f"AS ({_statement(asked)}{ENDING}) "
                    f"SELECT {', '.join([first, *(f'c{index}' for index in indexes[1:])])} FROM {rows}")


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
        fields, statement = _guarded(connection, values, scale)
        connection.create_function('demiflow_taken', 1, taken)
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
