"""Irreversible Linux guard for a disposable, trusted-code plan reader.

This is not a hostile Python sandbox. It denies filesystem mutation, network
access and child processes at the kernel boundary while construction imports
and reads local configuration. The parent owns time, memory and output limits.
Never install this in the Web process or a reusable reader.
"""
import ctypes
import errno
import os
import sys


def enter_readonly():
    if sys.platform != 'linux':
        raise RuntimeError('Read-only plan construction requires Linux seccomp')
    lib = ctypes.CDLL('libseccomp.so.2', use_errno=True)
    lib.seccomp_init.argtypes = [ctypes.c_uint32]
    lib.seccomp_init.restype = ctypes.c_void_p
    lib.seccomp_syscall_resolve_name.argtypes = [ctypes.c_char_p]
    lib.seccomp_syscall_resolve_name.restype = ctypes.c_int
    lib.seccomp_rule_add.argtypes = [ctypes.c_void_p, ctypes.c_uint32, ctypes.c_int, ctypes.c_uint]
    lib.seccomp_rule_add.restype = ctypes.c_int
    lib.seccomp_load.argtypes = [ctypes.c_void_p]
    lib.seccomp_load.restype = ctypes.c_int
    lib.seccomp_attr_set.argtypes = [ctypes.c_void_p, ctypes.c_int, ctypes.c_uint32]
    lib.seccomp_attr_set.restype = ctypes.c_int
    lib.seccomp_release.argtypes = [ctypes.c_void_p]

    class Comparison(ctypes.Structure):
        _fields_ = [('arg', ctypes.c_uint), ('op', ctypes.c_int),
                    ('datum_a', ctypes.c_uint64), ('datum_b', ctypes.c_uint64)]

    context = lib.seccomp_init(0x7fff0000)  # SCMP_ACT_ALLOW
    if not context:
        raise RuntimeError('Cannot allocate the read-only plan guard')

    def deny(name, *conditions, error=errno.EPERM):
        number = lib.seccomp_syscall_resolve_name(name.encode('ascii'))
        if number >= 0 and lib.seccomp_rule_add(context, 0x00050000 | error,
                number, len(conditions), *conditions) != 0:
            raise RuntimeError('Cannot install plan guard rule: ' + name)

    try:
        # Arrow/native imports may already own threads. Apply the restriction
        # atomically to all existing threads; future threads inherit it.
        if lib.seccomp_attr_set(context, 4, 1) != 0:  # SCMP_FLTATR_CTL_TSYNC
            raise RuntimeError('Cannot synchronize the read-only plan guard')
        for name in ('creat', 'truncate', 'ftruncate', 'truncate64', 'ftruncate64',
                     'unlink', 'unlinkat', 'rename', 'renameat', 'renameat2',
                     'mkdir', 'mkdirat', 'rmdir', 'link', 'linkat', 'symlink', 'symlinkat',
                     'mknod', 'mknodat', 'chmod', 'fchmod', 'fchmodat', 'fchmodat2',
                     'chown', 'fchown', 'lchown', 'fchownat', 'utime', 'utimes',
                     'futimesat', 'utimensat', 'setxattr', 'lsetxattr', 'fsetxattr',
                     'removexattr', 'lremovexattr', 'fremovexattr', 'fallocate',
                     'socket', 'socketpair', 'connect', 'bind', 'listen', 'accept', 'accept4',
                     'sendto', 'sendmsg', 'sendmmsg', 'socketcall',
                     'fork', 'vfork', 'execve', 'execveat', 'kill', 'tkill', 'tgkill',
                     'pidfd_send_signal', 'ptrace', 'process_vm_writev',
                     'mount', 'umount2', 'unshare', 'setns', 'bpf',
                     'io_uring_setup', 'io_uring_enter', 'io_uring_register'):
            deny(name)
        # Native Python/Arrow libraries may start threads, but cannot fork a
        # process. Returning ENOSYS lets glibc use the filterable clone ABI.
        deny('clone3', error=errno.ENOSYS)
        deny('clone', Comparison(0, 7, 0x00010000, 0))  # no CLONE_THREAD
        deny('openat2', error=errno.ENOSYS)  # indirect flags cannot be filtered
        for name, argument in (('open', 1), ('openat', 2)):
            for bit in (os.O_WRONLY, os.O_RDWR, os.O_CREAT, os.O_TRUNC, os.O_APPEND):
                deny(name, Comparison(argument, 7, bit, bit))
        if lib.seccomp_load(context) != 0:
            raise RuntimeError('Cannot activate read-only plan guard')
    finally:
        lib.seccomp_release(context)
