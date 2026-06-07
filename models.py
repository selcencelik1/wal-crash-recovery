"""
models.py — Result dataclasses used for inter-layer communication.

Every function call between layers must return one of these objects.
No raw bytes / ints may cross a layer boundary.
"""

from dataclasses import dataclass, field
from typing import Optional, Any, List


@dataclass
class PageResult:
    """Returned by DiskSpaceManager (and BufferManager.get_page().page) when a page is fetched."""
    data: bytes                     # raw page bytes (page_size long)
    io_performed: bool              # True iff a real disk read/write actually happened
    page_id: int = -1               # which page within the file
    file_id: str = ""               # which file (relation / index)
    status: str = "success"         # "success" | "failure"
    error: str = ""                 # human-readable error if status == "failure"


@dataclass
class BufferResult:
    """Returned by BufferManager for get_page / write_page."""
    page: PageResult                # the fetched / cached page
    cache_hit: bool                 # True if served from the buffer pool
    evicted_page_id: Optional[int] = None   # id of the page kicked out, if any
    dirty_writeback: bool = False   # True if the evicted page was dirty and got flushed
    status: str = "success"
    error: str = ""


@dataclass
class RecordResult:
    """Returned by FileIndexManager for search / range / insert / delete operations."""
    records: List[Any] = field(default_factory=list)   # matching records (each a list of field values)
    pages_accessed: int = 0         # how many data pages this op touched
    index_nodes_visited: int = 0    # 0 for heap_scan; >0 for hash / b+tree
    records_scanned: int = 0        # how many candidate records we inspected
    estimated_io: int = 0           # planner's I/O estimate (used by EXPLAIN)
    strategy: str = ""              # which access strategy was used
    status: str = "success"         # "success" | "failure"
    error: str = ""                 # diagnostic message on failure


@dataclass
class WriteResult:
    """Returned by DiskSpaceManager.write_page."""
    success: bool
    page_id: int = -1
    old_data: bytes = b""           # not populated (kept for the Result-object contract; no caller reads it)
    new_data: bytes = b""           # what we just wrote
    status: str = "success"
    error: str = ""
