"""
DiskSpaceManager — Layer 1.

The only component that performs real file I/O. It is page-oriented:
it does not know what records, indexes, or queries are. Pages are
fixed-size byte strings; the file is grown one page at a time using seek().
"""

import os
from models import PageResult, WriteResult


class DiskSpaceManager:
    def __init__(self, config: dict):
        self.config = config
        self.page_size: int = int(config.get("page_size", 4096))

        # Where all storage files (*.bin, wal.log, master.rec) live. Honour
        # the config's data_dir; a relative path is anchored to the project
        # root (parent of this package) so it stays independent of cwd, and a
        # absolute path is used as-is. The directory is created if missing.
        _root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        _configured = str(config.get("data_dir", "data"))
        self.data_dir: str = (
            _configured if os.path.isabs(_configured)
            else os.path.join(_root, _configured)
        )
        os.makedirs(self.data_dir, exist_ok=True)

        # I/O counters
        self.read_count: int = 0
        self.write_count: int = 0

        # In-memory bookkeeping. Authoritative state still lives on disk
        # (file size on disk == num_pages * page_size); these caches just
        # save us a stat() per allocation.
        self.file_pages: dict[str, int] = {}
        # Per-file free list of page ids that were deallocated and may be reused.
        self.free_lists: dict[str, list[int]] = {}

        self.log_write = self._default_log_write

        # ── WAL log file (owned by the RecoveryManager, never buffered) ──
        # The DiskSpaceManager performs the real file I/O for it (spec 6.1):
        # a dedicated append-only file, separate from data files, with an
        # explicit fsync the RecoveryManager can force on commit.
        self.wal_path: str = os.path.join(self.data_dir, "wal.log")
        self._wal_handle = None

        self._scan_existing_files()

    # ------------------------------------------------------------------ #
    # housekeeping
    # ------------------------------------------------------------------ #
    def _default_log_write(self, file_id: str, page_id: int, data: bytes) -> None:
        return None

    # ------------------------------------------------------------------ #
    # WAL log I/O — a dedicated append-only file outside the buffer pool
    # ------------------------------------------------------------------ #
    def log_append(self, line: str) -> None:
        """Append one already-serialised log record line to wal.log.

        The handle is flushed on every call so records reach the OS before a
        crash; os._exit(1) skips Python's buffers, so we must not rely on them.
        """
        if self._wal_handle is None:
            self._wal_handle = open(self.wal_path, "a")
        self._wal_handle.write(line + "\n")
        self._wal_handle.flush()

    def log_fsync(self) -> None:
        """Force wal.log to stable storage (WAL #2 durability on commit)."""
        if self._wal_handle is not None:
            self._wal_handle.flush()
            os.fsync(self._wal_handle.fileno())

    def log_read_lines(self) -> list:
        """Return wal.log as a list of raw lines (empty if it does not exist)."""
        if not os.path.exists(self.wal_path):
            return []
        with open(self.wal_path, "r") as f:
            return f.read().splitlines()





    def _scan_existing_files(self) -> None:
        """Recover page-count metadata from any .bin files left behind by a previous run."""
        for fname in os.listdir(self.data_dir):
            if not fname.endswith(".bin"):
                continue
            file_id = fname[:-4]
            size = os.path.getsize(os.path.join(self.data_dir, fname))
            self.file_pages[file_id] = size // self.page_size
            self.free_lists.setdefault(file_id, [])

    def valid_file_id(self, file_id: str) -> bool:
        """Reject anything that isn't a safe single-segment name (no path
        traversal / separators), keeping data files confined to data_dir.
        Public so the Buffer Manager can expose it to upper layers."""
        return (
            isinstance(file_id, str)
            and file_id not in ("", ".", "..")
            and os.path.basename(file_id) == file_id
            and os.sep not in file_id
            and (os.altsep is None or os.altsep not in file_id)
        )

    def _path(self, file_id: str) -> str:
        return os.path.join(self.data_dir, file_id + ".bin")

    def _page_error(self, file_id: str, page_id: int) -> str:
        if type(page_id) is not int:
            return f"invalid page id {page_id!r}"
        if page_id < 0:
            return f"invalid page id {page_id}"
        if not self.file_exists(file_id):
            return f"file not found: {file_id}"
        if page_id >= self.num_pages(file_id):
            return f"page {page_id} does not exist in file {file_id}"
        return ""

    # ------------------------------------------------------------------ #
    # file lifecycle
    # ------------------------------------------------------------------ #
    def file_exists(self, file_id: str) -> bool:
        if not self.valid_file_id(file_id):
            return False
        return os.path.exists(self._path(file_id))

    def create_file(self, file_id: str) -> None:
        if not self.valid_file_id(file_id):
            raise ValueError(f"invalid file id: {file_id!r}")
        if not self.file_exists(file_id):
            open(self._path(file_id), "wb").close()
            self.file_pages[file_id] = 0
            self.free_lists[file_id] = []

    def num_pages(self, file_id: str) -> int:
        return self.file_pages.get(file_id, 0)

    # ------------------------------------------------------------------ #
    # page allocation
    # ------------------------------------------------------------------ #
    def allocate_page(self, file_id: str) -> int:
        """Reserve a fresh page id without touching the disk.

        Allocation is purely a bookkeeping op: we bump the in-memory page
        count and let the caller do the real write through the buffer.
        Zero-filling the page on disk here would cost an unnecessary I/O
        on top of the buffer's eventual write-back. POSIX seek-past-EOF
        will extend the file automatically when the buffer flushes the
        first real write to this page.
        """
        if file_id not in self.file_pages:
            self.create_file(file_id)

        # Reuse a previously deallocated page when possible.
        free = self.free_lists.get(file_id)
        if free:
            return free.pop()

        page_id = self.file_pages[file_id]
        self.file_pages[file_id] = page_id + 1
        return page_id

    def deallocate_page(self, file_id: str, page_id: int) -> None:
        error = self._page_error(file_id, page_id)
        if error:
            raise ValueError(error)
        free = self.free_lists.setdefault(file_id, [])
        if page_id not in free:
            free.append(page_id)

    # ------------------------------------------------------------------ #
    # page I/O — every read/write counts toward the I/O statistics
    # ------------------------------------------------------------------ #
    def read_page(self, file_id: str, page_id: int) -> PageResult:
        if not self.valid_file_id(file_id):
            return PageResult(
                data=b"", io_performed=False, page_id=page_id, file_id=file_id,
                status="failure", error=f"invalid file id: {file_id!r}",
            )
        error = self._page_error(file_id, page_id)
        if error:
            return PageResult(
                data=b"", io_performed=False, page_id=page_id, file_id=file_id,
                status="failure", error=error,
            )
        with open(self._path(file_id), "rb") as f:
            f.seek(page_id * self.page_size)
            data = f.read(self.page_size)
        if len(data) < self.page_size:
            # Partial page (shouldn't happen if we always allocate full pages,
            # but be defensive against external truncation).
            data = data + b"\x00" * (self.page_size - len(data))
        self.read_count += 1
        return PageResult(
            data=data, io_performed=True, page_id=page_id, file_id=file_id,
            status="success",
        )

    def write_page(self, file_id: str, page_id: int, data: bytes) -> WriteResult:
        if not self.valid_file_id(file_id):
            return WriteResult(
                success=False, page_id=page_id, status="failure",
                error=f"invalid file id: {file_id!r}",
            )
        if type(page_id) is not int or page_id < 0:
            return WriteResult(
                success=False, page_id=page_id, status="failure",
                error=f"invalid page id {page_id!r}",
            )
        if len(data) != self.page_size:
            return WriteResult(
                success=False, page_id=page_id, status="failure",
                error=f"data size {len(data)} != page size {self.page_size}",
            )
        if not self.file_exists(file_id):
            self.create_file(file_id)
        if page_id > self.num_pages(file_id):
            return WriteResult(
                success=False, page_id=page_id, status="failure",
                error=f"page {page_id} cannot be written before page {self.num_pages(file_id)}",
            )

        # NOTE: we deliberately do NOT read the previous page contents here.
        # WriteResult.old_data is unused by any caller, and reading it would
        # count a phantom disk read on every write — inflating Disk I/O reads
        # and structurally tying reads to writes. A write is exactly 1 write.
        self._raw_write(file_id, page_id, data)
        return WriteResult(
            success=True, page_id=page_id, old_data=b"", new_data=data,
            status="success",
        )

    def _raw_write(self, file_id: str, page_id: int, data: bytes) -> None:
        """Internal: do the actual seek+write and bookkeeping (one I/O)."""
        path = self._path(file_id)
        with open(path, "r+b") as f:
            f.seek(page_id * self.page_size)
            f.write(data)
        if page_id >= self.file_pages.get(file_id, 0):
            self.file_pages[file_id] = page_id + 1
        self.write_count += 1
        self.log_write(file_id, page_id, data)

    # ------------------------------------------------------------------ #
    # statistics
    # ------------------------------------------------------------------ #
    def get_io_stats(self) -> dict:
        return {"reads": self.read_count, "writes": self.write_count}

    def reset_stats(self) -> None:
        self.read_count = 0
        self.write_count = 0
