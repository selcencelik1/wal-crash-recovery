"""
Static hash index on the primary key.

Layout:
    File: <type>_idx.bin
    Pages 0..NUM_BUCKETS-1 are the primary bucket pages, allocated when
    the type is created. Overflow chains use the page header's next_page
    field; overflow pages are allocated on demand at the file's tail.

    Each bucket page is a slotted page (PAGE_TYPE_HASH_BUCKET) holding up
    to MAX_SLOTS entries:
        key_slot    = STR_FIELD_SIZE bytes (key always serialized as a
                      32-byte fixed string — int keys are str(value).encode)
        page_id     = 4 bytes (data page in <type>.bin)
        slot_id     = 4 bytes (slot within that data page)
    -> entry size = 32 + 4 + 4 = 40 bytes; 10 entries per bucket page = 400
       bytes of body, well below the 4064-byte page body.

Equality lookups go through the index. Range search falls back to
heap_scan (per the spec).
"""

import struct
from typing import Optional, Tuple

from models import RecordResult
from .page import (
    PAGE_TYPE_HASH_BUCKET, MAX_SLOTS, NULL_PAGE,
    HEADER_SIZE, STR_FIELD_SIZE,
    empty_page, get_slot_bitmap, is_slot_occupied, find_free_slot,
    write_record_into_page, clear_slot_in_page, read_record_from_page,
    set_next_page, unpack_header,
)
from . import heap_scan


HASH_ENTRY_FMT = "<32sii"     # key, page_id, slot_id
HASH_ENTRY_SIZE = struct.calcsize(HASH_ENTRY_FMT)   # 40

DEFAULT_NUM_BUCKETS = 16


def index_file_name(type_name: str) -> str:
    return f"{type_name}_idx"


def _get_page(buffer, file_id: str, page_id: int) -> bytes:
    result = buffer.get_page(file_id, page_id)
    if result.status != "success":
        raise RuntimeError(result.error)
    return result.page.data


def _write_page(buffer, file_id: str, page_id: int, data: bytes) -> None:
    result = buffer.write_page(file_id, page_id, data)
    if result.status != "success":
        raise RuntimeError(result.error)


# ---------------------------------------------------------------- #
# key handling — we encode every key as a 32-byte ASCII string so
# the page format stays uniform regardless of key type
# ---------------------------------------------------------------- #
def encode_key(value, key_type: str) -> bytes:
    if key_type == "int":
        s = str(int(value))
    else:
        s = str(value)
    s = s.encode("ascii", "replace")[:STR_FIELD_SIZE]
    return s.ljust(STR_FIELD_SIZE, b"\x00")


def decode_key(blob: bytes, key_type: str):
    raw = blob[:STR_FIELD_SIZE].rstrip(b"\x00").decode("ascii", "replace")
    if key_type == "int":
        try:
            return int(raw)
        except ValueError:
            return None
    return raw


def hash_key(key, num_buckets: int) -> int:
    if isinstance(key, int):
        return key % num_buckets
    return sum(ord(c) for c in str(key)) % num_buckets


def encode_entry(key_blob: bytes, page_id: int, slot_id: int) -> bytes:
    return struct.pack(HASH_ENTRY_FMT, key_blob, page_id, slot_id)


def decode_entry(blob: bytes) -> Tuple[bytes, int, int]:
    key, pid, sid = struct.unpack(HASH_ENTRY_FMT, blob[:HASH_ENTRY_SIZE])
    return key, pid, sid


# ---------------------------------------------------------------- #
# build / lifecycle
# ---------------------------------------------------------------- #
def build_empty(buffer, page_size: int, type_name: str,
                num_buckets: int = DEFAULT_NUM_BUCKETS) -> int:
    """Allocate `num_buckets` empty bucket pages. Returns the bucket count."""
    fname = index_file_name(type_name)
    if not buffer.file_exists(fname):
        buffer.create_file(fname)
    for _ in range(num_buckets):
        pid = buffer.allocate_page(fname)
        page = empty_page(page_size, PAGE_TYPE_HASH_BUCKET, pid)
        _write_page(buffer, fname, pid, page)
    return num_buckets


# ---------------------------------------------------------------- #
# insertion
# ---------------------------------------------------------------- #
def insert(buffer, page_size: int, meta: dict, key_value,
           data_page: int, data_slot: int) -> int:
    """Insert (key, data_page, data_slot) into the hash index.

    Returns the number of index nodes (pages) visited.
    """
    fname = index_file_name(meta["type_name"])
    num_buckets = meta["hash_num_buckets"]
    pk_idx = meta["primary_key_order"] - 1
    key_type = meta["fields"][pk_idx][1]
    key_blob = encode_key(key_value, key_type)
    entry = encode_entry(key_blob, data_page, data_slot)

    bucket_pid = hash_key(_to_native(key_value, key_type), num_buckets)
    visited = 0
    pid = bucket_pid
    last_pid = pid

    while True:
        page = _get_page(buffer, fname, pid)
        visited += 1
        bitmap = get_slot_bitmap(page)
        slot = find_free_slot(bitmap, MAX_SLOTS)
        if slot != -1:
            new_page = write_record_into_page(page, slot, entry, HASH_ENTRY_SIZE)
            _write_page(buffer, fname, pid, new_page)
            return visited
        # Bucket page full → follow / create overflow chain.
        hdr = unpack_header(page)
        if hdr["next_page"] != NULL_PAGE:
            last_pid = pid
            pid = hdr["next_page"]
            continue
        # Allocate a new overflow page, link it in.
        new_pid = buffer.allocate_page(fname)
        new_page = empty_page(page_size, PAGE_TYPE_HASH_BUCKET, new_pid)
        new_page = write_record_into_page(new_page, 0, entry, HASH_ENTRY_SIZE)
        _write_page(buffer, fname, new_pid, new_page)

        linked = set_next_page(page, new_pid)
        _write_page(buffer, fname, pid, linked)
        return visited + 1


# ---------------------------------------------------------------- #
# lookup helpers
# ---------------------------------------------------------------- #
def lookup_pointer(buffer, meta: dict, key_value):
    """Resolve a key to (data_page, data_slot, index_nodes_visited).

    Returns (None, None, n) if not found.
    """
    fname = index_file_name(meta["type_name"])
    num_buckets = meta["hash_num_buckets"]
    pk_idx = meta["primary_key_order"] - 1
    key_type = meta["fields"][pk_idx][1]
    needle = encode_key(key_value, key_type)

    bucket_pid = hash_key(_to_native(key_value, key_type), num_buckets)
    visited = 0
    pid = bucket_pid
    while pid != NULL_PAGE:
        try:
            page = _get_page(buffer, fname, pid)
        except RuntimeError:
            return None, None, visited
        visited += 1
        bitmap = get_slot_bitmap(page)
        for slot in range(MAX_SLOTS):
            if not is_slot_occupied(bitmap, slot):
                continue
            entry = read_record_from_page(page, slot, HASH_ENTRY_SIZE)
            key, dp, ds = decode_entry(entry)
            if key == needle:
                return dp, ds, visited
        hdr = unpack_header(page)
        pid = hdr["next_page"]
    return None, None, visited


def search_by_pk(buffer, meta: dict, pk_value) -> RecordResult:
    dp, ds, visited = lookup_pointer(buffer, meta, pk_value)
    if dp is None:
        return RecordResult(
            records=[], pages_accessed=0,
            index_nodes_visited=visited,
            strategy="hash_index",
            status="failure", error="not found",
        )
    values, pa = heap_scan.read_record_at(buffer, meta, dp, ds)
    if values is None:
        return RecordResult(
            records=[], pages_accessed=pa,
            index_nodes_visited=visited,
            strategy="hash_index",
            status="failure", error="not found",
        )
    return RecordResult(
        records=[values],
        pages_accessed=pa,
        index_nodes_visited=visited,
        records_scanned=1,
        strategy="hash_index",
    )


# ---------------------------------------------------------------- #
# deletion
# ---------------------------------------------------------------- #
def delete_key(buffer, meta: dict, key_value) -> Tuple[bool, int]:
    """Remove the entry for `key_value`. Returns (deleted, nodes_visited)."""
    fname = index_file_name(meta["type_name"])
    num_buckets = meta["hash_num_buckets"]
    pk_idx = meta["primary_key_order"] - 1
    key_type = meta["fields"][pk_idx][1]
    needle = encode_key(key_value, key_type)

    bucket_pid = hash_key(_to_native(key_value, key_type), num_buckets)
    visited = 0
    pid = bucket_pid
    while pid != NULL_PAGE:
        try:
            page = _get_page(buffer, fname, pid)
        except RuntimeError:
            return False, visited
        visited += 1
        bitmap = get_slot_bitmap(page)
        for slot in range(MAX_SLOTS):
            if not is_slot_occupied(bitmap, slot):
                continue
            entry = read_record_from_page(page, slot, HASH_ENTRY_SIZE)
            key, _, _ = decode_entry(entry)
            if key == needle:
                new_page = clear_slot_in_page(page, slot, HASH_ENTRY_SIZE)
                _write_page(buffer, fname, pid, new_page)
                return True, visited
        hdr = unpack_header(page)
        pid = hdr["next_page"]
    return False, visited


# ---------------------------------------------------------------- #
# helper
# ---------------------------------------------------------------- #
def _to_native(value, key_type: str):
    if key_type == "int":
        try:
            return int(value)
        except (TypeError, ValueError):
            return 0
    return str(value)
