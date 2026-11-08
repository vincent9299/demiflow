"""Identity of one known workbench child; no scheduling or run records.

Notebook, data readers and development agents retain their own lifecycles.
An identity is evidence to recheck, not permission to signal a process.
"""


def _read(path, maximum):
    with open(path, "rb") as stream:
        raw = stream.read(maximum + 1)
    if len(raw) > maximum:
        raise RuntimeError("Process observation byte budget exceeded")
    return raw


def process_identity(pid):
    """Return the original pid/start/boot shape, or None for an absent process."""
    if type(pid) is not int or not 1 <= pid < 2**31:
        return None
    try:
        fields = _read(f"/proc/{pid}/stat", 32768).decode().rsplit(")", 1)[1].split()
        if fields[0] == "Z":
            return None
        return dict(pid=pid, start=fields[19],
                    boot=_read("/proc/sys/kernel/random/boot_id", 128).decode().strip())
    except (OSError, IndexError, UnicodeError):
        return None
