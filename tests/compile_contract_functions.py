"""Callables and passive declarations used by the compile corpus.

Keep this file stable. A function's identity includes the SHA-256 of its whole
source file and a class's identity includes the code of each method, so any
edit here changes the identities of every corpus case that uses them. Corpus
cases live in ``compile_contract_corpus.py`` and may change freely.
"""
from demiflow.data.datasink import Datasink
from demiflow.data.datasource import Datasource


def identity(row):
    return row


def keep(row):
    return True


def many(row):
    return [row, row]


def batch(rows):
    return rows


async def identity_async(row):
    return row


async def batch_async(rows):
    return rows


def fold(state, row):
    return state


def double(x):
    return x


def build_inputs(row, reading):
    return {}


def connection():
    raise RuntimeError('the compile corpus never opens a connection')


def rows_factory():
    return iter(())


def route(row):
    return 'first'


class Counter:
    def __init__(self):
        self.seen = 0

    def __call__(self, row):
        self.seen += 1
        return row


class Session:
    def search(self, *args, **kwargs):
        return []

    async def aclose(self):
        pass


class FixedDatasource(Datasource):
    def snapshot_identity(self):
        return {'fixture': 'fixed'}

    def get_read_tasks(self, parallelism, **read_args):
        raise RuntimeError('the compile corpus never reads a datasource')


class FixedSink(Datasink):
    def recovery_identity(self):
        return {'contract': 'compile-corpus.sink.v1', 'target': 'fixed'}

    def write(self, blocks, context):
        raise RuntimeError('the compile corpus never writes')
