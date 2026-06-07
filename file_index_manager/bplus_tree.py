"""
On-disk B+ tree on the primary key.

Every node lives in exactly one page of <type>_idx.bin and is fetched
through the BufferManager — there are no in-memory tree structures, so
the tree survives restarts intact (the catalog persists the root page).

Node layouts (header is the standard 32-byte page header):

    Internal (PAGE_TYPE_BPLUS_INTERNAL):
        record_count = N (key count)
        body = c_0 k_1 c_1 k_2 c_2 ... k_N c_N
               (N+1 child page-ids of 4 bytes, interleaved with N keys)

    Leaf (PAGE_TYPE_BPLUS_LEAF):
        record_count = N
        next_page    = page-id of next leaf in linked list (-1 = last)
        body         = N entries of (key, data_page, data_slot)
                       data_page/slot are 4-byte signed ints

Keys are 4 bytes for int primary keys and 32 bytes (null-padded ASCII)
for str primary keys, decided at type-creation time.

Insert / Search / Range / Delete are all implemented; insert handles
node splits including a root split. Delete does not rebalance — the
spec doesn't require it and lazy-delete is correct (just less compact).
"""

import bisect
import struct
from typing import List, Tuple

from models import RecordResult
from .page import (
    HEADER_SIZE, NULL_PAGE,
    PAGE_TYPE_BPLUS_INTERNAL, PAGE_TYPE_BPLUS_LEAF,
    pack_header, unpack_header, empty_page, STR_FIELD_SIZE,
)
from . import heap_scan


def index_file_name(type_name: str) -> str:
    return f"{type_name}_idx"


# Cap fanout so a single page never grows past page_size — the per-key
# ceiling is also bounded below by sane fanout.
HARD_FANOUT_CAP = 50


class BPlusTree:
    def __init__(self, buffer, page_size: int, meta: dict):
        self.buffer = buffer
        self.page_size = page_size
        self.meta = meta
        self.fname = index_file_name(meta["type_name"])

        pk_idx = meta["primary_key_order"] - 1
        self.key_type: str = meta["fields"][pk_idx][1]
        self.key_size: int = 4 if self.key_type == "int" else STR_FIELD_SIZE

        body = page_size - HEADER_SIZE
        # Internal: 4 bytes left-most child + N * (key + 4 child)  ≤ body
        max_internal = (body - 4) // (self.key_size + 4)
        # Leaf: N * (key + 8)  ≤ body
        max_leaf = body // (self.key_size + 8)
        self.max_keys = max(4, min(HARD_FANOUT_CAP, max_internal, max_leaf))

    # ----------------------------------------------------------------- #
    # key encoding
    # ----------------------------------------------------------------- #
    def _enc_key(self, k) -> bytes:
        if self.key_type == "int":
            return struct.pack("<i", int(k))
        s = str(k).encode("ascii", "replace")[:STR_FIELD_SIZE]
        return s.ljust(STR_FIELD_SIZE, b"\x00")

    def _dec_key(self, b: bytes):
        if self.key_type == "int":
            return struct.unpack("<i", b[:4])[0]
        return b[:STR_FIELD_SIZE].rstrip(b"\x00").decode("ascii", "replace")

    # ----------------------------------------------------------------- #
    # node serialization
    # ----------------------------------------------------------------- #
    def _write_leaf(self, page_id: int, keys: List, values: List[Tuple[int, int]],
                    next_page: int) -> bytes:
        page = bytearray(self.page_size)
        page[:HEADER_SIZE] = pack_header(
            PAGE_TYPE_BPLUS_LEAF, page_id, len(keys), b"\x00" * 10, next_page,
        )
        off = HEADER_SIZE
        for k, (dp, ds) in zip(keys, values):
            page[off:off + self.key_size] = self._enc_key(k)
            off += self.key_size
            page[off:off + 8] = struct.pack("<ii", dp, ds)
            off += 8
        return bytes(page)

    def _read_leaf(self, page: bytes) -> Tuple[List, List[Tuple[int, int]], int]:
        hdr = unpack_header(page)
        n = hdr["record_count"]
        off = HEADER_SIZE
        keys, values = [], []
        for _ in range(n):
            keys.append(self._dec_key(page[off:off + self.key_size]))
            off += self.key_size
            dp, ds = struct.unpack("<ii", page[off:off + 8])
            values.append((dp, ds))
            off += 8
        return keys, values, hdr["next_page"]

    def _write_internal(self, page_id: int, keys: List,
                        children: List[int]) -> bytes:
        page = bytearray(self.page_size)
        page[:HEADER_SIZE] = pack_header(
            PAGE_TYPE_BPLUS_INTERNAL, page_id, len(keys), b"\x00" * 10, NULL_PAGE,
        )
        off = HEADER_SIZE
        page[off:off + 4] = struct.pack("<i", children[0])
        off += 4
        for k, c in zip(keys, children[1:]):
            page[off:off + self.key_size] = self._enc_key(k)
            off += self.key_size
            page[off:off + 4] = struct.pack("<i", c)
            off += 4
        return bytes(page)

    def _read_internal(self, page: bytes) -> Tuple[List, List[int]]:
        hdr = unpack_header(page)
        n = hdr["record_count"]
        off = HEADER_SIZE
        children = [struct.unpack("<i", page[off:off + 4])[0]]
        off += 4
        keys = []
        for _ in range(n):
            keys.append(self._dec_key(page[off:off + self.key_size]))
            off += self.key_size
            children.append(struct.unpack("<i", page[off:off + 4])[0])
            off += 4
        return keys, children

    # ----------------------------------------------------------------- #
    # page allocation helpers
    # ----------------------------------------------------------------- #
    def _alloc_node(self, node_type: int) -> int:
        if not self.buffer.file_exists(self.fname):
            self.buffer.create_file(self.fname)
        pid = self.buffer.allocate_page(self.fname)
        self._write_node(pid, empty_page(self.page_size, node_type, pid))
        return pid

    def _get_node(self, page_id: int) -> bytes:
        result = self.buffer.get_page(self.fname, page_id)
        if result.status != "success":
            raise RuntimeError(result.error)
        return result.page.data

    def _write_node(self, page_id: int, data: bytes) -> None:
        result = self.buffer.write_page(self.fname, page_id, data)
        if result.status != "success":
            raise RuntimeError(result.error)

    # ----------------------------------------------------------------- #
    # public API
    # ----------------------------------------------------------------- #
    def build_empty(self) -> int:
        """Create an empty leaf to act as the root. Returns its page id."""
        return self._alloc_node(PAGE_TYPE_BPLUS_LEAF)

    def search(self, root_page: int, key) -> Tuple[int, int, int]:
        """Locate `key`. Returns (data_page, data_slot, nodes_visited),
        or (-1, -1, nodes_visited) if not found.
        """
        visited = 0
        if root_page == NULL_PAGE:
            return -1, -1, 0
        pid = root_page
        while True:
            page = self._get_node(pid)
            visited += 1
            hdr = unpack_header(page)
            if hdr["page_type"] == PAGE_TYPE_BPLUS_LEAF:
                keys, values, _ = self._read_leaf(page)
                idx = bisect.bisect_left(keys, key)
                if idx < len(keys) and keys[idx] == key:
                    dp, ds = values[idx]
                    return dp, ds, visited
                return -1, -1, visited
            keys, children = self._read_internal(page)
            child_idx = bisect.bisect_right(keys, key)
            pid = children[child_idx]

    def search_record(self, root_page: int, key) -> RecordResult:
        dp, ds, visited = self.search(root_page, key)
        if dp < 0:
            return RecordResult(
                records=[], pages_accessed=0,
                index_nodes_visited=visited,
                strategy="bplus_tree",
                status="failure", error="not found",
            )
        values, pa = heap_scan.read_record_at(self.buffer, self.meta, dp, ds)
        if values is None:
            return RecordResult(
                records=[], pages_accessed=pa,
                index_nodes_visited=visited,
                strategy="bplus_tree",
                status="failure", error="not found",
            )
        return RecordResult(
            records=[values], pages_accessed=pa,
            index_nodes_visited=visited, records_scanned=1,
            strategy="bplus_tree",
        )

    # ---------------- insertion ---------------- #
    def insert(self, root_page: int, key, data_page: int, data_slot: int) -> Tuple[int, int]:
        """Insert key → (data_page, data_slot). Returns (new_root, nodes_visited).

        Caller is responsible for rejecting duplicate keys *before* calling this
        (use search() first); a duplicate here would silently corrupt the index.
        """
        visited = [0]
        if root_page == NULL_PAGE:
            root_page = self.build_empty()
            page = self._write_leaf(root_page, [key], [(data_page, data_slot)], NULL_PAGE)
            self._write_node(root_page, page)
            return root_page, 1
        promotion = self._insert_recursive(root_page, key, data_page, data_slot, visited)
        if promotion is None:
            return root_page, visited[0]
        # Root split → build a new internal root.
        promo_key, promo_child = promotion
        new_root = self._alloc_node(PAGE_TYPE_BPLUS_INTERNAL)
        page = self._write_internal(new_root, [promo_key], [root_page, promo_child])
        self._write_node(new_root, page)
        return new_root, visited[0] + 1

    def _insert_recursive(self, page_id: int, key, dp: int, ds: int, visited):
        page = self._get_node(page_id)
        visited[0] += 1
        hdr = unpack_header(page)

        if hdr["page_type"] == PAGE_TYPE_BPLUS_LEAF:
            keys, values, next_page = self._read_leaf(page)
            pos = bisect.bisect_left(keys, key)
            keys.insert(pos, key)
            values.insert(pos, (dp, ds))
            if len(keys) <= self.max_keys:
                self._write_node(page_id, self._write_leaf(page_id, keys, values, next_page))
                return None
            return self._split_leaf(page_id, keys, values, next_page)

        # internal
        keys, children = self._read_internal(page)
        child_idx = bisect.bisect_right(keys, key)
        promotion = self._insert_recursive(children[child_idx], key, dp, ds, visited)
        if promotion is None:
            return None
        promo_key, promo_child = promotion
        # The promoted key separates the old child from the new one.
        keys.insert(child_idx, promo_key)
        children.insert(child_idx + 1, promo_child)
        if len(keys) <= self.max_keys:
            self._write_node(page_id, self._write_internal(page_id, keys, children))
            return None
        return self._split_internal(page_id, keys, children)

    def _split_leaf(self, page_id: int, keys: List, values: List,
                    next_page: int):
        mid = (len(keys) + 1) // 2
        left_keys, right_keys = keys[:mid], keys[mid:]
        left_vals, right_vals = values[:mid], values[mid:]
        new_pid = self._alloc_node(PAGE_TYPE_BPLUS_LEAF)
        # Old leaf keeps the lower half and links to the new leaf;
        # the new leaf takes over the original next_page link.
        self._write_node(page_id, self._write_leaf(page_id, left_keys, left_vals, new_pid))
        self._write_node(new_pid, self._write_leaf(new_pid, right_keys, right_vals, next_page))
        # B+ tree: the separator is a *copy* of the smallest key on the right.
        return right_keys[0], new_pid

    def _split_internal(self, page_id: int, keys: List, children: List):
        mid = len(keys) // 2
        promote_key = keys[mid]
        left_keys = keys[:mid]
        right_keys = keys[mid + 1:]
        left_children = children[:mid + 1]
        right_children = children[mid + 1:]
        new_pid = self._alloc_node(PAGE_TYPE_BPLUS_INTERNAL)
        self._write_node(page_id, self._write_internal(page_id, left_keys, left_children))
        self._write_node(new_pid, self._write_internal(new_pid, right_keys, right_children))
        return promote_key, new_pid

    # ---------------- deletion (lazy) ---------------- #
    def delete(self, root_page: int, key) -> Tuple[bool, int]:
        """Remove `key` from the tree. Returns (was_present, nodes_visited).
        Does not rebalance; an empty leaf simply stays in the linked list.
        """
        visited = 0
        if root_page == NULL_PAGE:
            return False, 0
        pid = root_page
        while True:
            page = self._get_node(pid)
            visited += 1
            hdr = unpack_header(page)
            if hdr["page_type"] == PAGE_TYPE_BPLUS_LEAF:
                keys, values, next_page = self._read_leaf(page)
                idx = bisect.bisect_left(keys, key)
                if idx >= len(keys) or keys[idx] != key:
                    return False, visited
                del keys[idx]
                del values[idx]
                self._write_node(pid, self._write_leaf(pid, keys, values, next_page))
                return True, visited
            keys, children = self._read_internal(page)
            child_idx = bisect.bisect_right(keys, key)
            pid = children[child_idx]

    # ---------------- range scan ---------------- #
    def range_search(self, root_page: int, low, high) -> Tuple[List, int, int]:
        """Return (pointer-list, leaves_scanned, nodes_visited).

        pointer-list is a list of (data_page, data_slot) tuples for every
        key k in [low, high]. Caller fetches the actual records from the heap.
        """
        visited = 0
        results: List[Tuple[int, int]] = []
        if root_page == NULL_PAGE:
            return results, 0, 0

        # Descend to the leaf containing `low`.
        pid = root_page
        while True:
            page = self._get_node(pid)
            visited += 1
            hdr = unpack_header(page)
            if hdr["page_type"] == PAGE_TYPE_BPLUS_LEAF:
                break
            keys, children = self._read_internal(page)
            child_idx = bisect.bisect_right(keys, low)
            pid = children[child_idx]

        # Walk the leaf linked list, collecting matches until we exceed `high`.
        leaves_scanned = 0
        while pid != NULL_PAGE:
            page = self._get_node(pid)
            leaves_scanned += 1
            keys, values, next_page = self._read_leaf(page)
            done = False
            for k, v in zip(keys, values):
                if k < low:
                    continue
                if k > high:
                    done = True
                    break
                results.append(v)
            if done:
                break
            pid = next_page
        return results, leaves_scanned, visited
