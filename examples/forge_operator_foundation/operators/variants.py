from typing import Iterator, TypedDict


class Case(TypedDict):
    case_id: str
    text: str


class Variant(TypedDict):
    case_id: str
    text: str
    variant: str


def expand(row: Case) -> Iterator[Variant]:
    yield {'case_id': row['case_id'], 'text': row['text'], 'variant': 'A'}
    yield {'case_id': row['case_id'], 'text': row['text'], 'variant': 'B'}


class Label:
    def __init__(self, prefix: str):
        self.prefix = prefix
        self.ready = False

    async def astart(self) -> None:
        self.ready = True

    async def __call__(self, row: Variant) -> Variant:
        if not self.ready:
            raise ValueError('Label actor is not started')
        return {'case_id': row['case_id'], 'text': row['text'], 'variant': self.prefix + row['variant']}

    async def aclose(self) -> None:
        self.ready = False
