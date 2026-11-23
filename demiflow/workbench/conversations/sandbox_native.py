"""Linux x86-64 confinement primitives used only by the fixed child launcher.

Landlock ABI 1 mediates paths; an allowlist seccomp profile covers its missing
metadata/truncation and ambient-process interfaces. No privilege is restored.
"""
import ctypes
import ctypes.util
import errno
import os
import platform
import stat

LIBC = ctypes.CDLL(None, use_errno=True)
SECCOMP = ctypes.CDLL(ctypes.util.find_library('seccomp'), use_errno=True)
READ = (1 << 2) | (1 << 3)
EXEC = 1 << 0
WRITE = (1 << 1) | (1 << 4) | (1 << 5) | (1 << 7) | (1 << 8)
TRUNCATE = 1 << 14


def landlock_abi():
    """Return kernel Landlock ABI version (0 if unavailable)."""
    abi = LIBC.syscall(444, 0, 0, 1)
    return int(abi) if abi >= 1 else 0


def tree_usage(paths, maximum=8192):
    """Bounded trusted accounting without following directory/link aliases."""
    count, used = 0, 0
    pending = []
    try:
        for path in paths:
            try:
                info = os.stat(path, follow_symlinks=False)
            except (FileNotFoundError, NotADirectoryError):
                continue
            if stat.S_ISREG(info.st_mode):
                count += 1
                used += info.st_size
            elif stat.S_ISDIR(info.st_mode):
                try:
                    pending.append(os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC))
                except (FileNotFoundError, NotADirectoryError):
                    continue
            else:
                raise RuntimeError('Sandbox writable scope contains an unsupported inode')
        while pending:
            descriptor = pending.pop()
            try:
                try:
                    with os.scandir(descriptor) as entries:
                        for entry in entries:
                            count += 1
                            if count > maximum:
                                raise RuntimeError('Sandbox writable file count exceeded budget')
                            try:
                                info = entry.stat(follow_symlinks=False)
                            except (FileNotFoundError, NotADirectoryError):
                                continue
                            if stat.S_ISDIR(info.st_mode):
                                try:
                                    pending.append(os.open(entry.name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                                                           dir_fd=descriptor))
                                except (FileNotFoundError, NotADirectoryError):
                                    continue
                            elif stat.S_ISREG(info.st_mode):
                                used += info.st_size
                            else:
                                raise RuntimeError('Sandbox writable scope contains an unsupported inode')
                except (FileNotFoundError, NotADirectoryError):
                    continue
            finally:
                os.close(descriptor)
        return used, count
    finally:
        for descriptor in pending:
            os.close(descriptor)


def checked(result, label):
    if result < 0:
        raise OSError(ctypes.get_errno(), label)
    return result


def prctl(option, *args):
    return checked(LIBC.prctl(option, *args), 'prctl failed')


def landlock(read_paths, write_paths, runtime_paths, identities):
    if platform.machine() != 'x86_64':
        raise RuntimeError('The development sandbox requires the qualified x86-64 ABI')
    abi = checked(LIBC.syscall(444, 0, 0, 1), 'Landlock unavailable')
    if abi < 1:
        raise RuntimeError('Landlock ABI 1 is required')

    class Rules(ctypes.Structure):
        _fields_ = [('handled', ctypes.c_uint64)]

    class PathRule(ctypes.Structure):
        _pack_ = 1
        _fields_ = [('access', ctypes.c_uint64), ('fd', ctypes.c_int32)]

    # Adaptively enable Landlock ABI 3 TRUNCATE mediation on Linux >= 6.2 so
    # writable project directories support native truncate/ftruncate while
    # read-only paths still deny truncation in the kernel. On ABI 1-2 kernels
    # (e.g. Linux 5.15), truncate remains denied globally by seccomp.
    handled = ((1 << 13) - 1) | (TRUNCATE if abi >= 3 else 0)
    write_rights = READ | WRITE | (TRUNCATE if abi >= 3 else 0)
    rules = Rules(handled)
    descriptor = checked(LIBC.syscall(444, ctypes.byref(rules), ctypes.sizeof(rules), 0), 'Landlock ruleset')
    try:
        for paths, rights in ((read_paths, READ), (runtime_paths, READ | EXEC),
                              (write_paths, write_rights)):
            for path in paths:
                access = rights
                if not os.path.isdir(path):
                    access &= EXEC | (1 << 1) | (1 << 2) | (TRUNCATE if abi >= 3 else 0)
                fd = os.open(path, os.O_PATH | os.O_CLOEXEC | os.O_NOFOLLOW)
                try:
                    info = os.fstat(fd)
                    if [info.st_dev, info.st_ino] != identities[path]:
                        raise RuntimeError('Sandbox scope identity changed before confinement')
                    attr = PathRule(access, fd)
                    checked(LIBC.syscall(445, descriptor, 1, ctypes.byref(attr), 0), 'Landlock path rule')
                finally:
                    os.close(fd)
        prctl(38, 1, 0, 0, 0)  # no_new_privs
        checked(LIBC.syscall(446, descriptor, 0), 'Landlock restrict_self')
    finally:
        os.close(descriptor)


def drop_capabilities():
    # NOROOT and NO_SETUID_FIXUP are locked, as is ambient capability raising.
    # KEEP_CAPS remains off. UID 0 is mapped to the invoking host UID; this does
    # not claim to create a dedicated host account or retain host capabilities.
    prctl(28, 1 | 2 | 4 | 8 | 32 | 64 | 128, 0, 0, 0)
    for number in range(64):
        if LIBC.prctl(24, number, 0, 0, 0) < 0 and ctypes.get_errno() != errno.EINVAL:
            checked(-1, 'Capability bounding-set drop')
    prctl(47, 4, 0, 0, 0)  # PR_CAP_AMBIENT_CLEAR_ALL

    class Header(ctypes.Structure):
        _fields_ = [('version', ctypes.c_uint32), ('pid', ctypes.c_int)]

    class Caps(ctypes.Structure):
        _fields_ = [('effective', ctypes.c_uint32), ('permitted', ctypes.c_uint32),
                    ('inheritable', ctypes.c_uint32)]

    header = Header(0x20080522, 0)
    caps = (Caps * 2)()
    checked(LIBC.capset(ctypes.byref(header), caps), 'Capability clear')
    prctl(38, 1, 0, 0, 0)


def seccomp(*, direct_network=False):
    sec = SECCOMP
    sec.seccomp_init.argtypes = [ctypes.c_uint32]
    sec.seccomp_init.restype = ctypes.c_void_p
    sec.seccomp_syscall_resolve_name.argtypes = [ctypes.c_char_p]
    sec.seccomp_syscall_resolve_name.restype = ctypes.c_int
    sec.seccomp_load.argtypes = [ctypes.c_void_p]
    sec.seccomp_release.argtypes = [ctypes.c_void_p]

    class Compare(ctypes.Structure):
        _fields_ = [('arg', ctypes.c_uint), ('op', ctypes.c_uint),
                    ('a', ctypes.c_uint64), ('b', ctypes.c_uint64)]

    sec.seccomp_rule_add_array.argtypes = [ctypes.c_void_p, ctypes.c_uint32, ctypes.c_int,
                                         ctypes.c_uint, ctypes.POINTER(Compare)]
    # libseccomp rejects all non-native architectures. x32 syscall numbers are
    # not in this allowlist. Unknown/new interfaces fail with ENOSYS.
    context = sec.seccomp_init(0x00050000 | errno.ENOSYS)
    if not context:
        raise RuntimeError('seccomp context unavailable')

    def rule(name, conditions=(), action=0x7fff0000):
        number = sec.seccomp_syscall_resolve_name(name.encode())
        if number < 0:
            raise RuntimeError('Unrecognized required seccomp syscall: ' + name)
        comparisons = (Compare * len(conditions))(*(Compare(*args) for args in conditions))
        result = sec.seccomp_rule_add_array(context, action, number, len(conditions), comparisons)
        if result:
            raise RuntimeError('Cannot install seccomp rule: ' + name)

    allowed = '''read write readv writev pread64 pwrite64 preadv pwritev preadv2 pwritev2
        close close_range lseek fstat stat lstat newfstatat statx access faccessat faccessat2
        readlink readlinkat getdents getdents64 chdir fchdir getcwd umask fsync fdatasync sync_file_range
        mkdir mkdirat rmdir unlink unlinkat rename renameat renameat2 link linkat
        mmap mprotect munmap mremap brk madvise mincore msync
        rt_sigaction rt_sigprocmask rt_sigreturn rt_sigpending rt_sigtimedwait rt_sigsuspend
        sigaltstack kill tkill tgkill signalfd signalfd4
        fork vfork execve execveat exit exit_group wait4 waitid
        getpid getppid gettid getuid geteuid getgid getegid getgroups
        setsid getsid setpgid getpgid getpgrp
        futex set_robust_list get_robust_list set_tid_address rseq arch_prctl
        sched_yield sched_getaffinity sched_setaffinity sched_getparam sched_getscheduler
        sched_get_priority_max sched_get_priority_min sched_rr_get_interval
        nanosleep clock_nanosleep clock_gettime clock_getres gettimeofday time times
        getrusage getrlimit prlimit64 sysinfo uname getrandom
        pipe pipe2 dup dup2 dup3 fcntl flock
        select pselect6 poll ppoll epoll_create epoll_create1 epoll_ctl epoll_wait epoll_pwait epoll_pwait2
        eventfd eventfd2 timerfd_create timerfd_settime timerfd_gettime
        bind listen accept accept4 connect getsockopt setsockopt getsockname getpeername shutdown
        sendto recvfrom sendmsg recvmsg sendmmsg recvmmsg
        socketpair restart_syscall membarrier memfd_create
        statfs fstatfs copy_file_range sendfile splice tee vmsplice'''.split()
    if landlock_abi() >= 3:
        allowed.extend(('truncate', 'ftruncate'))
    try:
        for name in allowed:
            if name != 'socketpair':
                rule(name)
        # No pathname Unix sockets or raw/IP packet sockets.
        rule('socket', ((0, 4, 2, 0), (1, 7, 15, 1)))  # AF_INET + SOCK_STREAM
        if direct_network:
            rule('socket', ((0, 4, 2, 0), (1, 7, 15, 2)))   # AF_INET + SOCK_DGRAM (DNS)
            rule('socket', ((0, 4, 10, 0), (1, 7, 15, 1)))  # AF_INET6 + SOCK_STREAM
            rule('socket', ((0, 4, 10, 0), (1, 7, 15, 2)))  # AF_INET6 + SOCK_DGRAM (DNS)
            rule('socket', ((0, 4, 16, 0), (1, 7, 15, 3)))  # AF_NETLINK + SOCK_RAW (getaddrinfo/interfaces)
            rule('socket', ((0, 4, 16, 0), (1, 7, 15, 2)))  # AF_NETLINK + SOCK_DGRAM
        rule('socketpair', ((0, 4, 1, 0), (1, 7, 15, 1)))
        # Namespace creation flags are excluded, including through clone.
        namespace_flags = 0x00020000 | 0x02000000 | 0x04000000 | 0x08000000 | 0x10000000 | 0x20000000 | 0x40000000
        rule('clone', ((0, 7, namespace_flags, 0),))
        for name, position in (('open', 1), ('openat', 2)):
            # Landlock ABI 1 lets O_RDONLY|O_TRUNC through its write check.
            # Use disjoint allowed conditions: a broad allow would subsume a
            # conditional errno rule in libseccomp's rule optimizer.
            rule(name, ((position, 7, os.O_TRUNC, 0),))
            rule(name, ((position, 7, os.O_ACCMODE, os.O_WRONLY),))
            rule(name, ((position, 7, os.O_ACCMODE, os.O_RDWR),))
        for option in (1, 2, 3, 15, 16, 21, 39, 0x53564d41):
            rule('prctl', ((0, 4, option, 0),))
        for request in (0x5401, 0x5413, 0x541b, 0x5421, 0x5450, 0x5451):
            # FIONCLEX/FIOCLEX only change inheritance of already owned FDs;
            # CPython uses FIOCLEX when opening its script entrypoint.
            rule('ioctl', ((1, 4, request, 0),))
        checked(sec.seccomp_load(context), 'seccomp_load')
    finally:
        sec.seccomp_release(context)
