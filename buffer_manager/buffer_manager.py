"""
BufferManager — Layer 2.

In-memory page cache between FileIndexManager and DiskSpaceManager.
On a hit no disk I/O happens. On a miss we evict (LRU or MRU), write
back if dirty, then read the requested page from disk.

Layer 3 must always go through this layer; it must never call
DiskSpaceManager directly.
"""

from collections import OrderedDict
from typing import Optional

from models import BufferResult, PageResult


class BufferManager:
    def __init__(self, config: dict, disk, recovery=None):
        self.config = config
        self.disk = disk
        # RecoveryManager (cross-cutting): logs every page write and enforces
        # WAL #1 before any dirty page is allowed to reach disk. None means the
        # engine is running without recovery (plain Project-3 behaviour).
        self.recovery = recovery
        self.pool_size: int = int(config.get("buffer_pool_size", 16))
        if self.pool_size < 1:
            raise ValueError("buffer_pool_size must be at least 1")
        self.policy: str = str(config.get("replacement_policy", "LRU")).upper()
        if self.policy not in ("LRU", "MRU"):
            self.policy = "LRU"

        # Frame storage. OrderedDict gives us O(1) move-to-end / popitem from
        # either side, which is exactly what LRU/MRU need.
        # key = (file_id, page_id)  ->  page bytes
        self.pool: "OrderedDict[tuple[str, int], bytes]" = OrderedDict()
        self.dirty: dict[tuple[str, int], bool] = {}

        # Statistics — exposed via get_stats(); reset by `stats reset`.
        self.requests: int = 0
        self.hits: int = 0
        self.misses: int = 0
        self.evictions: int = 0
        self.dirty_writebacks: int = 0
        # Logical request counters split by direction. These count every
        # call to get_page / write_page regardless of whether the page
        # was already cached. EXPLAIN uses the deltas to report
        # "Actual I/O" — i.e., the work the operation logically did,
        # independent of how much the buffer happened to absorb.
        self.read_requests: int = 0
        self.write_requests: int = 0

    # ------------------------------------------------------------------ #
    # eviction
    # ------------------------------------------------------------------ #
    def _victim_key(self) -> tuple[str, int]:
        """Choose one frame according to the active replacement policy."""
        if self.policy == "MRU":
            # Most-recently-used = the tail of the OrderedDict.
            return next(reversed(self.pool))
        # LRU = the head of the OrderedDict.
        return next(iter(self.pool))

    def _make_room(self) -> tuple[Optional[int], bool, str]:
        """Free a frame if the pool is full.

        Returns (evicted_page_id, was_dirty, error). If a dirty writeback
        fails, the page is kept in the pool and error is non-empty.
        """
        if len(self.pool) < self.pool_size:
            return None, False, ""

        evicted_key = self._victim_key()
        evicted_data = self.pool[evicted_key]
        was_dirty = self.dirty.get(evicted_key, False)
        if was_dirty:
            self._wal_before_flush(evicted_data)        # WAL #1
            result = self.disk.write_page(evicted_key[0], evicted_key[1], evicted_data)
            if not result.success:
                return evicted_key[1], True, result.error
            if self.recovery is not None:
                self.recovery.page_flushed(evicted_key[0], evicted_key[1])
            self.dirty_writebacks += 1

        self.pool.pop(evicted_key)
        self.dirty.pop(evicted_key, None)
        self.evictions += 1
        return evicted_key[1], was_dirty, ""

    def _validate_write(self, file_id: str, page_id: int, data: bytes) -> str:
        if hasattr(self.disk, "valid_file_id") and not self.disk.valid_file_id(file_id):
            return f"invalid file id: {file_id!r}"
        if type(page_id) is not int or page_id < 0:
            return f"invalid page id {page_id!r}"
        if len(data) != self.disk.page_size:
            return f"data size {len(data)} != page size {self.disk.page_size}"
        if not self.disk.file_exists(file_id):
            if page_id != 0:
                return f"page {page_id} cannot be written before page 0"
            return ""
        if page_id > self.disk.num_pages(file_id):
            return f"page {page_id} cannot be written before page {self.disk.num_pages(file_id)}"
        return ""

    # ------------------------------------------------------------------ #
    # read path
    # ------------------------------------------------------------------ #
    def get_page(self, file_id: str, page_id: int) -> BufferResult:
        self.requests += 1
        self.read_requests += 1
        key = (file_id, page_id)

        if key in self.pool:
            # HIT
            self.hits += 1
            data = self.pool[key]
            self.pool.move_to_end(key)  # mark as recently used
            return BufferResult(
                page=PageResult(data=data, io_performed=False,
                                page_id=page_id, file_id=file_id),
                cache_hit=True,
            )

        # MISS — read first, so an invalid request does not evict a valid page.
        self.misses += 1
        page_result = self.disk.read_page(file_id, page_id)
        if page_result.status != "success":
            return BufferResult(
                page=page_result, cache_hit=False,
                status="failure", error=page_result.error,
            )

        evicted_id, was_dirty, error = self._make_room()
        if error:
            return BufferResult(
                page=PageResult(data=b"", io_performed=False,
                                page_id=page_id, file_id=file_id,
                                status="failure", error=error),
                cache_hit=False,
                evicted_page_id=evicted_id, dirty_writeback=was_dirty,
                status="failure", error=error,
            )
        self.pool[key] = page_result.data
        self.dirty[key] = False
        return BufferResult(
            page=page_result, cache_hit=False,
            evicted_page_id=evicted_id, dirty_writeback=was_dirty,
        )

    # ------------------------------------------------------------------ #
    # write path
    # ------------------------------------------------------------------ #
    def write_page(self, file_id: str, page_id: int, data: bytes) -> BufferResult:
        """Stage a write in the buffer pool. Disk write is deferred to flush/eviction."""
        self.requests += 1
        self.write_requests += 1
        key = (file_id, page_id)
        error = self._validate_write(file_id, page_id, data)
        if error:
            return BufferResult(
                page=PageResult(data=b"", io_performed=False,
                                page_id=page_id, file_id=file_id,
                                status="failure", error=error),
                cache_hit=key in self.pool,
                status="failure", error=error,
            )

        # WAL: log the change (capturing the before-image) and receive the
        # page bytes with the new pageLSN stamped into the header. A log record
        # is now guaranteed to precede this page ever reaching disk.
        if self.recovery is not None:
            before = self._current_bytes(file_id, page_id)
            data = self.recovery.log_page_update(file_id, page_id, before, data)

        if key in self.pool:
            self.hits += 1
            self.pool[key] = data
            self.pool.move_to_end(key)
            self.dirty[key] = True
            return BufferResult(
                page=PageResult(data=data, io_performed=False,
                                page_id=page_id, file_id=file_id),
                cache_hit=True,
            )

        self.misses += 1
        evicted_id, was_dirty, error = self._make_room()
        if error:
            return BufferResult(
                page=PageResult(data=b"", io_performed=False,
                                page_id=page_id, file_id=file_id,
                                status="failure", error=error),
                cache_hit=False,
                evicted_page_id=evicted_id, dirty_writeback=was_dirty,
                status="failure", error=error,
            )
        self.pool[key] = data
        self.dirty[key] = True
        return BufferResult(
            page=PageResult(data=data, io_performed=False,
                            page_id=page_id, file_id=file_id),
            cache_hit=False,
            evicted_page_id=evicted_id, dirty_writeback=was_dirty,
        )

    # ------------------------------------------------------------------ #
    # page allocation — proxies through to the disk and primes the cache
    # ------------------------------------------------------------------ #
    def allocate_page(self, file_id: str) -> int:
        page_id = self.disk.allocate_page(file_id)
        key = (file_id, page_id)
        # If we'd overflow the pool, evict before priming.
        if key not in self.pool and len(self.pool) >= self.pool_size:
            _, _, error = self._make_room()
            if error:
                raise RuntimeError(error)
        self.pool[key] = b"\x00" * self.disk.page_size
        # disk.allocate_page no longer writes to disk, so the in-memory
        # zero-page is the only copy. Mark it dirty so it actually gets
        # persisted (even if the caller never overwrites it).
        self.dirty[key] = True
        return page_id

    def num_pages(self, file_id: str) -> int:
        return self.disk.num_pages(file_id)

    def file_exists(self, file_id: str) -> bool:
        return self.disk.file_exists(file_id)

    def create_file(self, file_id: str) -> None:
        self.disk.create_file(file_id)

    def valid_file_id(self, file_id: str) -> bool:
        # Passthrough so Layer 3 validates file ids via the adjacent layer
        # instead of reaching through to DiskSpaceManager directly.
        return self.disk.valid_file_id(file_id)

    # ------------------------------------------------------------------ #
    # flush — write every dirty frame back to disk
    # ------------------------------------------------------------------ #
    def flush(self) -> None:
        for key, data in list(self.pool.items()):
            if self.dirty.get(key, False):
                self._wal_before_flush(data)            # WAL #1
                result = self.disk.write_page(key[0], key[1], data)
                if result.success:
                    self.dirty[key] = False
                    if self.recovery is not None:
                        self.recovery.page_flushed(key[0], key[1])
                else:
                    raise RuntimeError(result.error)

    # ------------------------------------------------------------------ #
    # WAL helpers
    # ------------------------------------------------------------------ #
    def _wal_before_flush(self, data: bytes) -> None:
        """WAL #1: a dirty page with pageLSN L must not hit disk until every
        log record up to L is durable."""
        if self.recovery is not None:
            self.recovery.flush_log_up_to(self.recovery.page_lsn_of(data))

    def _current_bytes(self, file_id: str, page_id: int):
        """Return the page's current bytes (the before-image for logging):
        from the pool if cached, else from disk, else None for a brand-new page."""
        key = (file_id, page_id)
        if key in self.pool:
            return self.pool[key]
        if self.disk.file_exists(file_id) and page_id < self.disk.num_pages(file_id):
            pr = self.disk.read_page(file_id, page_id)
            if pr.status == "success":
                return pr.data
        return None

    # ------------------------------------------------------------------ #
    # stats
    # ------------------------------------------------------------------ #
    def get_stats(self) -> dict:
        hit_rate = (self.hits / self.requests * 100.0) if self.requests else 0.0
        return {
            "requests": self.requests,
            "hits": self.hits,
            "misses": self.misses,
            "evictions": self.evictions,
            "dirty_writebacks": self.dirty_writebacks,
            "hit_rate": hit_rate,
        }

    def reset_stats(self) -> None:
        self.requests = 0
        self.hits = 0
        self.misses = 0
        self.evictions = 0
        self.dirty_writebacks = 0
        self.read_requests = 0
        self.write_requests = 0
