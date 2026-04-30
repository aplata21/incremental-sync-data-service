"""Filesystem primitives that the commit phase relies on for crash safety.

The whole crash-consistency story depends on three properties:

1. Atomic same-filesystem rename. ``os.replace`` is atomic on POSIX and on
   the same volume on Windows (NTFS). We never rename across filesystems,
   so this is the single primitive we build on.

2. Durable rename. After a rename, fsync the *parent directory* so the
   directory entry change reaches disk. Without this, a crash between
   ``os.replace`` and the next ``fsync`` can roll the rename back. POSIX
   only; on Windows it is a no-op (Windows does not expose dir fsync).

3. Durable file content. Writers ``fsync`` the file fd before closing the
   tempfile. Otherwise a crash can leave the rename point at a zero-length
   or torn file on POSIX filesystems with delayed allocation.
"""

from __future__ import annotations

import os
import sys
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path


def ensure_dir(path: Path) -> None:
    """``mkdir -p`` semantics. Idempotent."""
    path.mkdir(parents=True, exist_ok=True)


def fsync_file(fd: int) -> None:
    """fsync a file descriptor; tolerate platforms that don't support it."""
    try:
        os.fsync(fd)
    except OSError:
        # Some filesystems (tmpfs in CI, certain network mounts) refuse fsync.
        # That's fine for tests; production targets ext4/xfs/apfs and will succeed.
        pass


def fsync_dir(path: Path) -> None:
    """fsync the directory entry so a rename inside it is durable.

    On POSIX, opening a directory and calling fsync flushes the directory
    metadata. On Windows, you cannot open a directory for read this way, so
    this becomes a no-op. Windows still provides atomic renames via
    ``os.replace``; only the post-crash durability guarantee is weaker, which
    is acceptable for a local prototype.
    """
    if sys.platform.startswith("win"):
        return
    try:
        fd = os.open(str(path), os.O_RDONLY)
    except OSError:
        return
    try:
        fsync_file(fd)
    finally:
        os.close(fd)


def write_bytes_atomic(target: Path, data: bytes) -> None:
    """Write ``data`` to ``target`` via tempfile + fsync + atomic rename.

    The temp file is created in ``target.parent`` so the rename stays on the
    same filesystem. The parent directory is fsynced after the rename so the
    new directory entry is durable.
    """
    ensure_dir(target.parent)
    fd, tmp_str = tempfile.mkstemp(
        prefix=f".{target.name}.",
        suffix=".tmp",
        dir=str(target.parent),
    )
    tmp = Path(tmp_str)
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
            f.flush()
            fsync_file(f.fileno())
        os.replace(tmp, target)
        fsync_dir(target.parent)
    except Exception:
        # best-effort cleanup; never mask the real exception
        try:
            tmp.unlink(missing_ok=True)
        except OSError:
            pass
        raise


def atomic_rename(src: Path, dst: Path) -> None:
    """Atomic same-filesystem rename + parent dir fsync.

    Used by the commit phase to swing already-staged files into place.
    """
    ensure_dir(dst.parent)
    os.replace(src, dst)
    fsync_dir(dst.parent)


@contextmanager
def staged_writer(target: Path) -> Iterator[Path]:
    """Yield a temp path next to ``target``; caller writes to it; on a
    successful exit we fsync + rename. On exception the temp is deleted.

    Used when a writer wants streaming I/O instead of materializing a single
    bytes blob in memory (large lake partitions).
    """
    ensure_dir(target.parent)
    fd, tmp_str = tempfile.mkstemp(
        prefix=f".{target.name}.",
        suffix=".tmp",
        dir=str(target.parent),
    )
    os.close(fd)  # caller will reopen with whatever mode they want
    tmp = Path(tmp_str)
    try:
        yield tmp
        # caller wrote to tmp; ensure durable, then rename
        with open(tmp, "rb+") as f:
            fsync_file(f.fileno())
        os.replace(tmp, target)
        fsync_dir(target.parent)
    except Exception:
        try:
            tmp.unlink(missing_ok=True)
        except OSError:
            pass
        raise
