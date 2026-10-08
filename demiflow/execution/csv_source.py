"""Explicit Arrow CSV options for local Dataset readers and native joins."""
from pathlib import Path
import csv
import gzip


def csv_spec(source):
    import pyarrow as pa
    import pyarrow.csv as pacsv
    options = dict(source.options)
    if not options or set(options) - {'read_options', 'parse_options', 'convert_options'}:
        return None
    read = options.get('read_options', pacsv.ReadOptions())
    parse = options.get('parse_options', pacsv.ParseOptions())
    convert = options.get('convert_options', pacsv.ConvertOptions())
    if (len(source.paths) != 1 or read.column_names or read.autogenerate_column_names
            or read.skip_rows or read.skip_rows_after_names or read.encoding.lower() != 'utf8'
            or parse.invalid_row_handler or not parse.double_quote or parse.escape_char
            or not parse.ignore_empty_lines or convert.include_columns or convert.strings_can_be_null):
        return None
    path = Path(source.paths[0]).resolve()
    opener = gzip.open if path.suffix == '.gz' else open
    with opener(path, 'rt', newline='', encoding='utf-8-sig') as stream:
        names = next(csv.reader(stream, delimiter=parse.delimiter,
            quotechar=parse.quote_char or '"', quoting=csv.QUOTE_MINIMAL if parse.quote_char else csv.QUOTE_NONE))
    if len(names) != len(set(names)) or not names or any(not n for n in names):
        raise ValueError('CSV header must have unique nonempty column names')
    if set(convert.column_types) != set(names) or not all(pa.types.is_string(t) for t in convert.column_types.values()):
        return None
    stat = path.stat()
    return dict(format='csv', uri=str(path), columns=names,
                delimiter=parse.delimiter, quote=parse.quote_char or '\x00',
                newlines_in_values=parse.newlines_in_values,
                compression='gzip' if path.suffix == '.gz' else '',
                stat=(stat.st_size, stat.st_mtime_ns, stat.st_ino))


def assert_csv_unchanged(spec):
    stat = Path(spec['uri']).stat()
    if (stat.st_size, stat.st_mtime_ns, stat.st_ino) != tuple(spec['stat']):
        raise ValueError('CSV source changed while executing Dataset: ' + spec['uri'])


def arrow_csv_rows(source):
    import pyarrow.csv as pacsv
    for path in source.paths:
        with pacsv.open_csv(path, **dict(source.options)) as reader:
            for batch in reader:
                yield from batch.to_pylist()
