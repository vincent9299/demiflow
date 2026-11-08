"""Fixed private bootstrap, also usable before the matching wheel is installed.

The source root/operation are administrator-owned launch data, never HTTP input.
No environment PYTHONPATH, editable install, shell command or OCI fallback.
"""
import argparse
from pathlib import Path
import runpy
import sys
import os


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--code-root', required=True)
    parser.add_argument('--operation', choices=('wrapper','worker','kernel'), required=True)
    parser.add_argument('--context', required=True)
    parser.add_argument('--context-sha256', required=True)
    parser.add_argument('--control-fd', type=int)
    parser.add_argument('--release-fd', type=int)
    args, extra = parser.parse_known_args()
    root = Path(args.code_root)
    if not root.is_absolute() or root.is_symlink() or '..' in root.parts or not (root/'demiflow/__init__.py').is_file():
        raise ValueError('Fixed matching platform code root is required')
    sys.path.insert(0, str(root))
    from demiflow.workbench.analysis.job_context import JobContext
    context = JobContext.load(args.context, args.context_sha256).install()
    if context.value['code_root'] != str(root) or Path(sys.executable) != Path(context.value['python']):
        raise ValueError('Worker interpreter/source differs from admitted context')
    if args.operation == 'wrapper':
        if extra or args.control_fd is None or args.release_fd is not None:
            raise ValueError('Fixed wrapper control descriptor required')
        from demiflow.workbench.analysis.trusted_runner import run
        return run(context, args.context, args.context_sha256, args.control_fd)
    if args.control_fd is not None:
        raise ValueError('Only wrapper receives its parent control pipe')
    if args.release_fd is not None:
        if args.operation != 'worker' or os.read(args.release_fd,1)!=b'S':
            raise ValueError('Original worker release was not confirmed')
        os.close(args.release_fd)
    if args.operation == 'kernel':
        if context.value['role'] != 'kernel': raise ValueError('Kernel role required')
        # The app's source is imported before user's project modules. This is
        # provenance, not a security restriction on trusted Python code.
        sys.path.append(context.value['cwd'])
        sys.argv = ['ipykernel_launcher', *extra]
        runpy.run_module('ipykernel_launcher', run_name='__main__')
        return 0
    if extra:
        raise ValueError('Worker accepts no arbitrary arguments')
    role = context.value['role']; operation = context.value['config'].get('operation')
    module = ('kernel_worker' if role == 'kernel' else 'case_worker' if operation == 'case-page'
              else 'native_worker' if role == 'native' else 'media_worker')
    import importlib
    importlib.import_module('demiflow.workbench.analysis.'+module).main()
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
