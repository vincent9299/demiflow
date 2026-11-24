"""Record which demiflow modules a process loads. It changes no behaviour.

Active only when DEMIFLOW_IMPORT_LOG names a file and this directory is on PYTHONPATH,
so every Python process a test starts records too. A process started with -I or -E
ignores PYTHONPATH and is not recorded; `tests/verification_scope.py` allows for that.
The log is a set: one module name per line, appended once per process.
"""
import os
import sys

_LOG = os.environ.get('DEMIFLOW_IMPORT_LOG')

if _LOG:
    _seen = set()

    def _record(event, args):
        if event != 'import':
            return
        name = args[0]
        if name in _seen or not (name == 'demiflow' or name.startswith('demiflow.')):
            return
        _seen.add(name)
        try:
            with open(_LOG, 'a', encoding='utf-8') as handle:
                handle.write(name + '\n')
        except OSError:
            pass

    sys.addaudithook(_record)
