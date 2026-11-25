"""Rows a streaming action produced, as its caller receives them.

Row callables run from sealed copies in every worker mode, so a list that a
callback appends to never reaches the test. The rows travel through a file.
"""
import os
import pickle
import tempfile


def run_stream_rows(stream):
    """Run ``stream`` with ``run_stream()``; return its rows and statistics."""
    descriptor, name = tempfile.mkstemp(suffix='.rows')
    os.close(descriptor)

    def record(row):
        with open(name, 'ab') as out:
            pickle.dump(row, out)
        return row
    try:
        stats = stream.map(record).run_stream()
        rows = []
        with open(name, 'rb') as source:
            while True:
                try:
                    rows.append(pickle.load(source))
                except EOFError:
                    break
    finally:
        os.unlink(name)
    return rows, stats
