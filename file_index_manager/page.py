"""
Slotted-page primitives.

Layout (header is 32 bytes regardless of page type):

    offset  size  field
    ------  ----  -----
       0      1   page_type      (0=DATA, 1=BPLUS_INTERNAL, 2=BPLUS_LEAF,
                                  3=HASH_BUCKET, 4=CATALOG)
       1      4   page_id        (this page's id within its file, signed)
       5      4   record_count   (occupied entries; for index pages this
                                  doubles as "key count")
       9     10   slot_bitmap    (one byte per slot, 0=free, 1=occupied;
                                  used by DATA / CATALOG / HASH_BUCKET pages
                                  that have at most MAX_SLOTS entries)
      19      4   next_page      (for chained pages: next data page,
                                  next leaf, next hash overflow; -1 = NONE)
      23      9   reserved       (padding to a clean 32-byte header)

Body is page_size - 32 bytes. Records are fixed length per relation
(int = 4 bytes, str = 32 bytes null-padded), packed slot-by-slot starting
at offset 32.
"""

import struct

# ---------- page-type constants ----------
PAGE_TYPE_DATA = 0
PAGE_TYPE_BPLUS_INTERNAL = 1
PAGE_TYPE_BPLUS_LEAF = 2
PAGE_TYPE_HASH_BUCKET = 3
PAGE_TYPE_CATALOG = 4

# ---------- header geometry ----------
HEADER_SIZE = 32
HEADER_FORMAT = "<BiI10si9s"   # pack-format for the 32-byte header

# ---------- record geometry ----------
INT_FIELD_SIZE = 4              # 4-byte signed int (struct 'i')
STR_FIELD_SIZE = 32             # null-padded fixed string

NULL_PAGE = -1                  # sentinel for "no next page"
MAX_SLOTS = 10                  # spec cap: up to 10 records per data page
                                # (also reused as the slotted cap for
                                # catalog and hash-bucket pages)


# ============================================================ #
# header pack / unpack
# ============================================================ #
def pack_header(page_type: int, page_id: int, record_count: int,
                slot_bitmap: bytes, next_page: int = NULL_PAGE) -> bytes:
    """Build the 32-byte header for a page."""
    if len(slot_bitmap) < 10:
        slot_bitmap = slot_bitmap.ljust(10, b"\x00")
    return struct.pack(
        HEADER_FORMAT,
        page_type & 0xFF,
        page_id,
        record_count,
        slot_bitmap[:10],
        next_page,
        b"\x00" * 9,
    )


def unpack_header(page_bytes: bytes) -> dict:
    page_type, page_id, record_count, slot_bitmap, next_page, _ = \
        struct.unpack(HEADER_FORMAT, page_bytes[:HEADER_SIZE])
    return {
        "page_type": page_type,
        "page_id": page_id,
        "record_count": record_count,
        "slot_bitmap": slot_bitmap,
        "next_page": next_page,
    }


def empty_page(page_size: int, page_type: int, page_id: int,
               next_page: int = NULL_PAGE) -> bytes:
    """Allocate an empty page of the given type."""
    header = pack_header(page_type, page_id, 0, b"\x00" * 10, next_page)
    return header + b"\x00" * (page_size - HEADER_SIZE)


# ============================================================ #
# field encoding (int / str) — used for records, hash keys, b+tree keys
# ============================================================ #
def encode_int(v: int) -> bytes:
    return struct.pack("<i", int(v))


def decode_int(b: bytes) -> int:
    return struct.unpack("<i", b[:INT_FIELD_SIZE])[0]


def encode_str(v: str) -> bytes:
    s = str(v).encode("ascii", errors="replace")
    if len(s) > STR_FIELD_SIZE:
        s = s[:STR_FIELD_SIZE]
    return s.ljust(STR_FIELD_SIZE, b"\x00")


def decode_str(b: bytes) -> str:
    return b[:STR_FIELD_SIZE].rstrip(b"\x00").decode("ascii", errors="replace")


def field_size(field_type: str) -> int:
    return INT_FIELD_SIZE if field_type == "int" else STR_FIELD_SIZE


def record_size_for(field_types: list) -> int:
    return sum(field_size(t) for t in field_types)


def encode_record(values: list, field_types: list) -> bytes:
    parts = []
    for v, t in zip(values, field_types):
        parts.append(encode_int(v) if t == "int" else encode_str(v))
    return b"".join(parts)


def decode_record(blob: bytes, field_types: list) -> list:
    out = []
    off = 0
    for t in field_types:
        if t == "int":
            out.append(decode_int(blob[off:off + INT_FIELD_SIZE]))
            off += INT_FIELD_SIZE
        else:
            out.append(decode_str(blob[off:off + STR_FIELD_SIZE]))
            off += STR_FIELD_SIZE
    return out


# ============================================================ #
# slotted-page record helpers (used by DATA and CATALOG pages)
# ============================================================ #
def slot_offset(slot_idx: int, record_size: int) -> int:
    return HEADER_SIZE + slot_idx * record_size


def get_slot_bitmap(page_bytes: bytes) -> bytearray:
    return bytearray(page_bytes[9:19])


def is_slot_occupied(bitmap: bytes, slot_idx: int) -> bool:
    return bitmap[slot_idx] != 0


def find_free_slot(bitmap: bytes, max_slots: int = MAX_SLOTS) -> int:
    for i in range(max_slots):
        if bitmap[i] == 0:
            return i
    return -1


def write_record_into_page(page_bytes: bytes, slot_idx: int,
                           record_blob: bytes, record_size: int) -> bytes:
    """Return a new page bytes object with `record_blob` placed at `slot_idx`.

    Updates the slot bitmap and the record_count in the header.
    """
    page = bytearray(page_bytes)
    off = slot_offset(slot_idx, record_size)
    page[off:off + record_size] = record_blob

    bitmap = get_slot_bitmap(page)
    was_occupied = bitmap[slot_idx] != 0
    bitmap[slot_idx] = 1
    page[9:19] = bytes(bitmap)

    if not was_occupied:
        rc = struct.unpack("<I", page[5:9])[0] + 1
        page[5:9] = struct.pack("<I", rc)
    return bytes(page)


def clear_slot_in_page(page_bytes: bytes, slot_idx: int,
                       record_size: int) -> bytes:
    """Return a new page bytes with slot_idx marked free and zeroed."""
    page = bytearray(page_bytes)
    bitmap = get_slot_bitmap(page)
    if bitmap[slot_idx] == 0:
        return bytes(page)
    bitmap[slot_idx] = 0
    page[9:19] = bytes(bitmap)

    off = slot_offset(slot_idx, record_size)
    page[off:off + record_size] = b"\x00" * record_size

    rc = struct.unpack("<I", page[5:9])[0]
    if rc > 0:
        page[5:9] = struct.pack("<I", rc - 1)
    return bytes(page)


def read_record_from_page(page_bytes: bytes, slot_idx: int,
                          record_size: int) -> bytes:
    off = slot_offset(slot_idx, record_size)
    return bytes(page_bytes[off:off + record_size])


def set_next_page(page_bytes: bytes, next_page: int) -> bytes:
    page = bytearray(page_bytes)
    page[19:23] = struct.pack("<i", next_page)
    return bytes(page)
