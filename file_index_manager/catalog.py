"""
System Catalog.

Stored as binary pages in __catalog__.bin so it survives restarts and
— per the instructor's clarification on the forum — every access
**must** go through the BufferManager. We do not hold a parsed copy
of the catalog in memory; each lookup re-scans the catalog pages, and
the BufferManager's pool is the only cache we rely on.

The catalog itself uses the same slotted-page format as data pages;
each slot holds one type definition. When the first catalog page fills
up we link a new one via the page header's next_page field, so the
catalog can grow indefinitely.

Catalog entry layout (212 bytes, fixed):

    16   type_name           (null-padded ASCII)
     4   num_fields
     4   primary_key_order   (1-indexed; 0 = no PK)
     4   num_data_pages      (cached count of pages in <type>.bin)
     4   num_records
     4   bplus_root_page     (root page in <type>_idx.bin; -1 if unused)
     4   hash_num_buckets    (0 if unused)
     4   reserved
    12 * 28 = 336 bytes  field descriptors (24 name + 1 type + 3 pad each)

Types may declare 6..MAX_FIELDS fields (spec: "at least 6 fields"). The
entry stays fixed length (44 + 12*28 = 380 bytes); 10 entries per catalog
page = 3800 bytes, well within the 4064-byte page body.

Field type byte: 0 = int, 1 = str.
"""

import struct
from typing import Iterator, Optional, Tuple

from .page import (
    HEADER_SIZE, MAX_SLOTS, NULL_PAGE, PAGE_TYPE_CATALOG,
    empty_page, get_slot_bitmap, is_slot_occupied,
    write_record_into_page, clear_slot_in_page, read_record_from_page,
    pack_header, unpack_header, set_next_page,
)

CATALOG_FILE = "__catalog__"
CATALOG_FIRST_PAGE = 0
MAX_TYPES_PER_PAGE = MAX_SLOTS  # 10 types per catalog page

TYPE_NAME_LEN = 16
FIELD_NAME_LEN = 24
MIN_FIELDS = 6          # spec: "at least 6 fields per type"
MAX_FIELDS = 12         # documented cap so the catalog entry stays fixed-size
FIELD_TYPE_INT = 0
FIELD_TYPE_STR = 1

ENTRY_HEADER_FMT = "<16siiiiiii"   # 16 + 4*7 = 44 bytes
ENTRY_HEADER_SIZE = struct.calcsize(ENTRY_HEADER_FMT)
FIELD_DESC_FMT = "<24sB3s"          # 28 bytes
FIELD_DESC_SIZE = struct.calcsize(FIELD_DESC_FMT)
CATALOG_ENTRY_SIZE = ENTRY_HEADER_SIZE + MAX_FIELDS * FIELD_DESC_SIZE  # 212


# ---------------------------------------------------------------- #
# encoding / decoding catalog entries
# ---------------------------------------------------------------- #
def encode_entry(meta: dict) -> bytes:
    name = meta["type_name"].encode("ascii", "replace")[:TYPE_NAME_LEN].ljust(TYPE_NAME_LEN, b"\x00")
    parts = [struct.pack(
        ENTRY_HEADER_FMT,
        name,
        meta["num_fields"],
        meta["primary_key_order"],
        meta.get("num_data_pages", 0),
        meta.get("num_records", 0),
        meta.get("bplus_root_page", -1),
        meta.get("hash_num_buckets", 0),
        0,
    )]
    fields = meta["fields"]
    for i in range(MAX_FIELDS):
        if i < len(fields):
            fname, ftype = fields[i]
            fb = fname.encode("ascii", "replace")[:FIELD_NAME_LEN].ljust(FIELD_NAME_LEN, b"\x00")
            tcode = FIELD_TYPE_INT if ftype == "int" else FIELD_TYPE_STR
            parts.append(struct.pack(FIELD_DESC_FMT, fb, tcode, b"\x00" * 3))
        else:
            parts.append(b"\x00" * FIELD_DESC_SIZE)
    blob = b"".join(parts)
    assert len(blob) == CATALOG_ENTRY_SIZE
    return blob


def decode_entry(blob: bytes) -> dict:
    h = struct.unpack(ENTRY_HEADER_FMT, blob[:ENTRY_HEADER_SIZE])
    num_fields = min(max(h[1], 0), MAX_FIELDS)
    type_name = h[0].rstrip(b"\x00").decode("ascii", "replace")
    meta = {
        "type_name": type_name,
        "num_fields": num_fields,
        "primary_key_order": h[2],
        "num_data_pages": h[3],
        "num_records": h[4],
        "bplus_root_page": h[5],
        "hash_num_buckets": h[6],
        "fields": [],
    }
    off = ENTRY_HEADER_SIZE
    for i in range(num_fields):
        fb, tcode, _ = struct.unpack(FIELD_DESC_FMT, blob[off:off + FIELD_DESC_SIZE])
        off += FIELD_DESC_SIZE
        fname = fb.rstrip(b"\x00").decode("ascii", "replace")
        ftype = "int" if tcode == FIELD_TYPE_INT else "str"
        meta["fields"].append((fname, ftype))
    return meta


# ---------------------------------------------------------------- #
# Catalog facade — all reads/writes go through the buffer manager.
# No parsed-entry cache lives in this object.
# ---------------------------------------------------------------- #
class Catalog:
    def __init__(self, buffer, page_size: int):
        self.buffer = buffer
        self.page_size = page_size
        self._ensure_first_page()

    # -------- bootstrap --------
    def _ensure_first_page(self) -> None:
        """Make sure __catalog__.bin exists and has a page 0 to start the chain."""
        if not self.buffer.file_exists(CATALOG_FILE):
            self.buffer.create_file(CATALOG_FILE)
        if self.buffer.num_pages(CATALOG_FILE) == 0:
            page_id = self.buffer.allocate_page(CATALOG_FILE)
            blank = empty_page(self.page_size, PAGE_TYPE_CATALOG, page_id)
            self._write_page(CATALOG_FILE, page_id, blank)

    # ---------------------------------------------------------------- #
    # page chain traversal — every yield is one buffer access
    # ---------------------------------------------------------------- #
    def _iter_pages(self) -> Iterator[Tuple[int, bytes]]:
        """Walk the catalog page chain, yielding (page_id, page_bytes).

        Every visit goes through the buffer manager (a hit if the page
        is already in the pool, a miss otherwise — the manager handles it).
        Stops on next_page == NULL_PAGE.
        """
        if self.buffer.num_pages(CATALOG_FILE) == 0:
            return
        pid = CATALOG_FIRST_PAGE
        seen = set()
        while pid != NULL_PAGE and pid not in seen:
            seen.add(pid)
            page = self._get_page(CATALOG_FILE, pid)
            yield pid, page
            hdr = unpack_header(page)
            pid = hdr["next_page"]

    def _scan_for_type(self, type_name: str) -> Tuple[Optional[int], Optional[int], Optional[dict]]:
        """Find a type by name. Returns (page_id, slot_idx, decoded meta)."""
        for pid, page in self._iter_pages():
            bitmap = get_slot_bitmap(page)
            for slot in range(MAX_TYPES_PER_PAGE):
                if not is_slot_occupied(bitmap, slot):
                    continue
                blob = read_record_from_page(page, slot, CATALOG_ENTRY_SIZE)
                meta = decode_entry(blob)
                if meta["type_name"] == type_name:
                    return pid, slot, meta
        return None, None, None

    def _last_page_id(self) -> int:
        """Return the page id of the last catalog page (the one whose next_page is NULL)."""
        last = CATALOG_FIRST_PAGE
        for pid, _ in self._iter_pages():
            last = pid
        return last

    # ---------------------------------------------------------------- #
    # public API — every call re-scans through the buffer
    # ---------------------------------------------------------------- #
    def has_type(self, type_name: str) -> bool:
        pid, _, _ = self._scan_for_type(type_name)
        return pid is not None

    def get(self, type_name: str) -> Optional[dict]:
        _, _, meta = self._scan_for_type(type_name)
        return meta

    def all_types(self) -> list:
        names = []
        for _, page in self._iter_pages():
            bitmap = get_slot_bitmap(page)
            for slot in range(MAX_TYPES_PER_PAGE):
                if not is_slot_occupied(bitmap, slot):
                    continue
                blob = read_record_from_page(page, slot, CATALOG_ENTRY_SIZE)
                names.append(decode_entry(blob)["type_name"])
        return names

    def add_type(self, meta: dict) -> bool:
        # Reject duplicates — the FileIndexManager already gates this,
        # but defending the catalog layer keeps the invariant local.
        if self.has_type(meta["type_name"]):
            return False

        # Look for a free slot in any existing catalog page.
        for pid, page in self._iter_pages():
            bitmap = get_slot_bitmap(page)
            for slot in range(MAX_TYPES_PER_PAGE):
                if bitmap[slot] == 0:
                    new_page = write_record_into_page(
                        page, slot, encode_entry(meta), CATALOG_ENTRY_SIZE,
                    )
                    self._write_page(CATALOG_FILE, pid, new_page)
                    return True

        # Every existing catalog page is full — extend the chain.
        last_pid = self._last_page_id()
        new_pid = self.buffer.allocate_page(CATALOG_FILE)
        new_page = empty_page(self.page_size, PAGE_TYPE_CATALOG, new_pid)
        new_page = write_record_into_page(
            new_page, 0, encode_entry(meta), CATALOG_ENTRY_SIZE,
        )
        self._write_page(CATALOG_FILE, new_pid, new_page)

        # Link the new page from the previous tail.
        last_page = self._get_page(CATALOG_FILE, last_pid)
        self._write_page(CATALOG_FILE, last_pid, set_next_page(last_page, new_pid))
        return True

    def update_type(self, meta: dict) -> None:
        pid, slot, _ = self._scan_for_type(meta["type_name"])
        if pid is None:
            return
        page = self._get_page(CATALOG_FILE, pid)
        new_page = write_record_into_page(
            page, slot, encode_entry(meta), CATALOG_ENTRY_SIZE,
        )
        self._write_page(CATALOG_FILE, pid, new_page)

    def remove_type(self, type_name: str) -> None:
        pid, slot, _ = self._scan_for_type(type_name)
        if pid is None:
            return
        page = self._get_page(CATALOG_FILE, pid)
        new_page = clear_slot_in_page(page, slot, CATALOG_ENTRY_SIZE)
        self._write_page(CATALOG_FILE, pid, new_page)

    def _get_page(self, file_id: str, page_id: int) -> bytes:
        result = self.buffer.get_page(file_id, page_id)
        if result.status != "success":
            raise RuntimeError(result.error)
        return result.page.data

    def _write_page(self, file_id: str, page_id: int, data: bytes) -> None:
        result = self.buffer.write_page(file_id, page_id, data)
        if result.status != "success":
            raise RuntimeError(result.error)
