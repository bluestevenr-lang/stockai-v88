"""Small flock adapter: native POSIX locking, portalocker on Windows."""
import errno
import os

LOCK_EX, LOCK_NB, LOCK_UN = 2, 4, 8
if os.name != 'nt':
    from fcntl import flock, LOCK_EX, LOCK_NB, LOCK_UN
else:
    import portalocker

    def flock(file, operation):
        if operation == LOCK_UN:
            portalocker.unlock(file)
            return
        if operation not in (LOCK_EX, LOCK_EX | LOCK_NB):
            raise ValueError('Only exclusive V88 locks are supported')
        flags = portalocker.LOCK_EX
        if operation & LOCK_NB:
            flags |= portalocker.LOCK_NB
        try:
            portalocker.lock(file, flags)
        except portalocker.exceptions.AlreadyLocked as exc:
            raise BlockingIOError(errno.EAGAIN, 'V88 file is locked') from exc
