from demiflow import data
from operators.variants import expand, Label


def run(*, source, target, prefix):
    rows = data.read_lance(source['uri'], version=source['version'])
    variants = rows.flat_map(expand).map_async(Label(prefix), concurrency=1, queue_depth=1)
    result = variants.materialize()
    return result.write_lance(target['uri'], mode='create', return_receipt=True)
