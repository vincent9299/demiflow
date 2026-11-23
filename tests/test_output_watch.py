from pathlib import Path

import pytest

from demiflow.execution.journal.output_watch import OutputWatch


def test_output_accounting_ignores_history_and_siblings(tmp_path):
    root = tmp_path / 'table.lance'
    (root / 'data').mkdir(parents=True)
    (root / 'data/history.lance').write_bytes(b'h' * 100000)
    watch = OutputWatch([root])
    try:
        assert watch.poll() == 0
        (tmp_path / 'unrelated').write_bytes(b'u' * 10000)
        path = root / 'data/new.lance'
        path.write_bytes(b'n' * 123)
        assert watch.poll() == 123
        with path.open('ab') as output:
            output.write(b'n' * 10)
        assert watch.poll() == 133
        path.rename(root / 'data/renamed.lance')
        assert watch.poll() == 133
    finally:
        watch.close()


def test_bounded_cache_does_not_limit_lifetime_files(tmp_path):
    root = tmp_path / 'new.lance'
    watch = OutputWatch([root], max_files=1)
    try:
        (root / 'data').mkdir(parents=True)
        (root / 'data/one').write_bytes(b'a' * 17)
        assert watch.poll() == 17
        (root / 'data/two').write_bytes(b'b')
        assert watch.poll() == 18
        assert len(watch.identities) == 1
        with (root / 'data/one').open('ab') as output:
            output.write(b'c')
        # Evicted entries are counted conservatively, never silently omitted.
        assert watch.poll() == 36
        assert len(watch.identities) == 1
        (root / 'data/one').unlink()
        watch.poll()
        assert len(watch.identities) == 0
        (root / 'data/one').write_bytes(b'x' * 2)
        assert watch.poll() == 38
    finally:
        watch.close()
