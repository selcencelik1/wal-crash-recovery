"""
Heap scan — the baseline access strategy. No index; we walk every data
page of a relation and check each occupied slot.

A heap also exists *underneath* the indexes: when hash_index or bplus_tree
is the active strategy, the index just records (page_id, slot_id) pointers
into the same heap. Inserts/deletes go through these helpers either way.
"""

from typing import Optional, Tuple, List

from models import RecordResult
from .page import (
    PAGE_TYPE_DATA, MAX_SLOTS, empty_page,
    get_slot_bitmap, find_free_slot, is_slot_occupied,
    write_record_into_page, clear_slot_in_page,
    read_record_from_page,
    encode_record, decode_record, record_size_for,
)


def data_file_name(type_name: str) -> str:
    return type_name


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
# raw heap helpers — used by heap_scan AND by the indexes
# ---------------------------------------------------------------- #
def insert_into_heap(buffer, page_size: int, meta: dict,
                     record_values: list,
                     max_records: int = MAX_SLOTS) -> Tuple[int, int, int]:
    """Place a record on the first page with a free slot.

    `max_records` is the configured per-page record cap
    (config.max_records_per_page); it is clamped to the physical slot
    ceiling. Returns (page_id, slot_id, pages_accessed).
    """
    type_name = meta["type_name"]
    field_types = [t for _, t in meta["fields"]]
    rec_size = record_size_for(field_types)
    blob = encode_record(record_values, field_types)
    cap = max(1, min(MAX_SLOTS, max_records))

    if not buffer.file_exists(type_name):
        buffer.create_file(type_name)

    pages_accessed = 0
    num_pages = buffer.num_pages(type_name)
    for pid in range(num_pages):
        page = _get_page(buffer, type_name, pid)
        pages_accessed += 1
        bitmap = get_slot_bitmap(page)
        slot = find_free_slot(bitmap, cap)
        if slot != -1:
            new_page = write_record_into_page(page, slot, blob, rec_size)
            _write_page(buffer, type_name, pid, new_page)
            return pid, slot, pages_accessed

    # All existing pages are full → allocate a fresh one.
    new_pid = buffer.allocate_page(type_name)
    pages_accessed += 1
    blank = empty_page(page_size, PAGE_TYPE_DATA, new_pid)
    new_page = write_record_into_page(blank, 0, blob, rec_size)
    _write_page(buffer, type_name, new_pid, new_page)
    meta["num_data_pages"] = max(meta.get("num_data_pages", 0), new_pid + 1)
    return new_pid, 0, pages_accessed


def read_record_at(buffer, meta: dict, page_id: int, slot_id: int):
    """Read the record at a known (page, slot). Returns (values, pages_accessed)."""
    type_name = meta["type_name"]
    field_types = [t for _, t in meta["fields"]]
    rec_size = record_size_for(field_types)
    page = _get_page(buffer, type_name, page_id)
    bitmap = get_slot_bitmap(page)
    if not is_slot_occupied(bitmap, slot_id):
        return None, 1
    blob = read_record_from_page(page, slot_id, rec_size)
    return decode_record(blob, field_types), 1


def delete_at(buffer, meta: dict, page_id: int, slot_id: int) -> bool:
    type_name = meta["type_name"]
    field_types = [t for _, t in meta["fields"]]
    rec_size = record_size_for(field_types)
    page = _get_page(buffer, type_name, page_id)
    bitmap = get_slot_bitmap(page)
    if not is_slot_occupied(bitmap, slot_id):
        return False
    new_page = clear_slot_in_page(page, slot_id, rec_size)
    _write_page(buffer, type_name, page_id, new_page)
    return True


# ---------------------------------------------------------------- #
# heap_scan strategy proper
# ---------------------------------------------------------------- #
def search_by_pk(buffer, meta: dict, pk_value) -> RecordResult:
    """Equality search by primary key via full sequential scan."""
    type_name = meta["type_name"]
    field_types = [t for _, t in meta["fields"]]
    rec_size = record_size_for(field_types)
    pk_idx = meta["primary_key_order"] - 1
    pk_type = field_types[pk_idx]
    needle = _coerce(pk_value, pk_type)

    pages_accessed = 0
    records_scanned = 0
    if not buffer.file_exists(type_name):
        return RecordResult(strategy="heap_scan", status="failure",
                            error=f"type {type_name} has no data file")

    num_pages = buffer.num_pages(type_name)
    for pid in range(num_pages):
        pages_accessed += 1
        try:
            page = _get_page(buffer, type_name, pid)
        except RuntimeError as e:
            return RecordResult(
                pages_accessed=pages_accessed,
                records_scanned=records_scanned,
                strategy="heap_scan",
                status="failure",
                error=str(e),
            )
        bitmap = get_slot_bitmap(page)
        for slot in range(MAX_SLOTS):
            if not is_slot_occupied(bitmap, slot):
                continue
            blob = read_record_from_page(page, slot, rec_size)
            values = decode_record(blob, field_types)
            records_scanned += 1
            if values[pk_idx] == needle:
                return RecordResult(
                    records=[values],
                    pages_accessed=pages_accessed,
                    records_scanned=records_scanned,
                    strategy="heap_scan",
                )
    return RecordResult(
        records=[],
        pages_accessed=pages_accessed,
        records_scanned=records_scanned,
        strategy="heap_scan",
        status="failure",
        error="not found",
    )


def find_location_by_pk(buffer, meta: dict, pk_value):
    """Locate the (page, slot) of a primary key. Returns (page_id, slot_id, pages_accessed) or (None, None, n)."""
    type_name = meta["type_name"]
    field_types = [t for _, t in meta["fields"]]
    rec_size = record_size_for(field_types)
    pk_idx = meta["primary_key_order"] - 1
    pk_type = field_types[pk_idx]
    needle = _coerce(pk_value, pk_type)

    pages_accessed = 0
    if not buffer.file_exists(type_name):
        return None, None, 0
    num_pages = buffer.num_pages(type_name)
    for pid in range(num_pages):
        try:
            page = _get_page(buffer, type_name, pid)
        except RuntimeError:
            return None, None, pages_accessed
        pages_accessed += 1
        bitmap = get_slot_bitmap(page)
        for slot in range(MAX_SLOTS):
            if not is_slot_occupied(bitmap, slot):
                continue
            blob = read_record_from_page(page, slot, rec_size)
            values = decode_record(blob, field_types)
            if values[pk_idx] == needle:
                return pid, slot, pages_accessed
    return None, None, pages_accessed


def range_search(buffer, meta: dict, field_name: str, low: int, high: int) -> RecordResult:
    """Range search on an integer field via full scan."""
    type_name = meta["type_name"]
    field_types = [t for _, t in meta["fields"]]
    rec_size = record_size_for(field_types)
    fnames = [n for n, _ in meta["fields"]]

    if field_name not in fnames:
        return RecordResult(strategy="heap_scan", status="failure",
                            error=f"unknown field {field_name}")
    fidx = fnames.index(field_name)
    if field_types[fidx] != "int":
        return RecordResult(strategy="heap_scan", status="failure",
                            error="range_search requires an integer field")

    try:
        lo, hi = int(low), int(high)
    except (TypeError, ValueError):
        return RecordResult(strategy="heap_scan", status="failure",
                            error="range bounds must be integers")

    matches: List[list] = []
    pages_accessed = 0
    records_scanned = 0
    if not buffer.file_exists(type_name):
        return RecordResult(strategy="heap_scan")

    num_pages = buffer.num_pages(type_name)
    for pid in range(num_pages):
        pages_accessed += 1
        try:
            page = _get_page(buffer, type_name, pid)
        except RuntimeError as e:
            return RecordResult(
                records=matches,
                pages_accessed=pages_accessed,
                records_scanned=records_scanned,
                strategy="heap_scan",
                status="failure",
                error=str(e),
            )
        bitmap = get_slot_bitmap(page)
        for slot in range(MAX_SLOTS):
            if not is_slot_occupied(bitmap, slot):
                continue
            blob = read_record_from_page(page, slot, rec_size)
            values = decode_record(blob, field_types)
            records_scanned += 1
            if lo <= values[fidx] <= hi:
                matches.append(values)
    return RecordResult(
        records=matches,
        pages_accessed=pages_accessed,
        records_scanned=records_scanned,
        strategy="heap_scan",
    )


def _coerce(value, field_type: str):
    if field_type == "int":
        try:
            return int(value)
        except (TypeError, ValueError):
            return None
    return str(value)
