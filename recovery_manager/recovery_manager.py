"""
RecoveryManager — Write-Ahead Logging + a simplified ARIES crash recovery.

Design note (why logging lives at the BufferManager boundary)
-------------------------------------------------------------
Project 3's engine is strictly layered and *every* page mutation — heap,
catalog, hash and B+tree index pages alike — funnels through a single call:
``BufferManager.write_page(file_id, page_id, data)``.  That makes the buffer
the one complete, leak-proof interception point for WAL.  Rather than sprinkle
``log_update`` calls across heap_scan / catalog / hash_index / bplus_tree, the
BufferManager hands every write to ``log_page_update`` here, which assigns the
LSN, stamps it into the page header (pageLSN), and records the before/after
image.  The contract the spec asks for is honoured: a log record is written
*before* the page can ever reach disk, the page carries its pageLSN, and the
two WAL invariants below hold.

Owns:
  - wal.log     (append-only log file, written through the DiskSpaceManager,
                 never cached in the buffer pool)
  - master.rec  (one-line file storing the begin_checkpoint LSN of the most
                 recent complete checkpoint)

WAL invariants enforced:
  WAL #1 (atomicity): BufferManager calls flush_log_up_to(pageLSN) before it
          writes any dirty page to disk, guaranteeing flushedLSN >= pageLSN.
  WAL #2 (durability): commit() flushes the log and os.fsync's it before
          returning.

Recovery (three phases, run automatically on construction, before any input):
  Analysis -> Redo ("repeat history") -> Undo (roll back the losers).
"""

import csv
import io
import os
from typing import Dict, List, Optional, Tuple


# ── Log record field names (column order in wal.log) ────────────────────────
_FIELDS = ["lsn", "prev_lsn", "xid", "type", "page_id",
           "offset", "before", "after"]

# page_id in wal.log is stored as "<file_id>:<page_id>" so it survives restarts.
_PAGE_SEP = ":"

# Sentinel for "no previous LSN in this transaction chain".
_NO_LSN = -1

# Sentinel xid for checkpoint records (they belong to no transaction).
_SYS_XID = -1


# ── pageLSN placement inside the 32-byte page header ────────────────────────
# page.py reserves bytes 23..31 ("reserved" field) which no other code reads,
# so we stash the 8-byte pageLSN there.  Crucially we must NOT use byte 0
# (page_type) or bytes 1..4 (page_id) — those carry real header data.
HEADER_SIZE      = 32
PAGE_LSN_OFFSET  = 23
PAGE_LSN_SIZE    = 8   # bytes (fits in the 9 reserved header bytes)


def read_page_lsn(data: bytes) -> int:
    return int.from_bytes(
        data[PAGE_LSN_OFFSET:PAGE_LSN_OFFSET + PAGE_LSN_SIZE], "little",
        signed=False,
    )


def write_page_lsn(data: bytearray, lsn: int) -> None:
    data[PAGE_LSN_OFFSET:PAGE_LSN_OFFSET + PAGE_LSN_SIZE] = \
        int(lsn).to_bytes(PAGE_LSN_SIZE, "little")


def _encode_bytes(b: bytes) -> str:
    """Hex-encode bytes so they survive a CSV round-trip."""
    return b.hex() if b else ""


def _decode_bytes(s: str) -> bytes:
    return bytes.fromhex(s) if s else b""


def _diff_range(a: bytes, b: bytes) -> Tuple[Optional[int], Optional[int]]:
    """Return [lo, hi) bounding the bytes that differ between a and b."""
    n = min(len(a), len(b))
    lo = 0
    while lo < n and a[lo] == b[lo]:
        lo += 1
    if lo == n and len(a) == len(b):
        return None, None
    hi = max(len(a), len(b))
    m = min(len(a), len(b))
    while hi > lo and hi <= m and a[hi - 1] == b[hi - 1]:
        hi -= 1
    return lo, hi


class RecoveryManager:
    # ------------------------------------------------------------------
    # Construction + startup recovery
    # ------------------------------------------------------------------
    def __init__(self, config: dict, disk):
        self.config = config
        self.disk = disk
        self.page_size: int = int(config.get("page_size", 4096))

        self._master_path = os.path.join(disk.data_dir, "master.rec")

        self._checkpoint_interval: int = int(config.get("checkpoint_interval", 50))
        self._log_buffer_size:     int = int(config.get("log_buffer_size", 8))

        # In-memory log buffer (records not yet pushed to wal.log).
        self._log_buffer: List[dict] = []

        # LSN / XID counters recovered from the on-disk log (never reset).
        existing = self._read_all_log_records()
        self._next_lsn: int = (existing[-1]["lsn"] + 1) if existing else 0
        self._flushed_lsn: int = (existing[-1]["lsn"]) if existing else _NO_LSN
        max_xid = max((r["xid"] for r in existing if r["xid"] >= 0), default=-1)
        self._next_xid: int = max_xid + 1

        # Transaction Table:  xid -> {"status", "lastLSN"}
        self.tx_table: Dict[int, dict] = {}
        # Dirty Page Table:   (file_id, page_id) -> recLSN
        self.dirty_page_table: Dict[Tuple[str, int], int] = {}

        # Operations since the last checkpoint (drives fuzzy checkpointing).
        self._ops_since_ckpt: int = 0

        # Set by the QueryProcessor for the duration of one tx_op so the
        # BufferManager's writes are attributed to the right transaction.
        self.current_xid: Optional[int] = None

        # Injected after construction (unused by recovery itself, which talks
        # to the disk directly, but handy for runtime aborts).
        self.buffer = None

        # ── Three-phase recovery, before any input is processed. ──
        self._run_recovery()

    # ==================================================================
    # Transaction control  (called by the QueryProcessor)
    # ==================================================================
    def begin_transaction(self) -> int:
        xid = self._next_xid
        self._next_xid += 1
        self.tx_table[xid] = {"status": "active", "lastLSN": _NO_LSN}
        self._maybe_checkpoint()
        return xid

    def commit(self, xid: int) -> None:
        """WAL #2: the commit record is flushed and fsync'd before we return,
        then an end record closes the transaction out."""
        self._emit(xid, "commit")
        self._flush_log()
        self.disk.log_fsync()                      # force log to stable storage
        if xid in self.tx_table:
            self.tx_table[xid]["status"] = "committed"
        self._emit(xid, "end")
        self._flush_log()
        self.tx_table.pop(xid, None)

    # ==================================================================
    # Update logging  (called by the BufferManager for every page write)
    # ==================================================================
    def log_page_update(self, file_id: str, page_id: int,
                        before: Optional[bytes], after: bytes) -> bytes:
        """Log a page modification and return the page bytes with the new
        pageLSN stamped in.  Writes that happen outside any transaction
        (e.g. the one-time empty-catalog bootstrap) are not logged — they are
        recreated deterministically on a fresh start.
        """
        if self.current_xid is None:
            return after

        xid = self.current_xid
        lsn = self._next_lsn
        self._next_lsn += 1

        stamped = bytearray(after)
        write_page_lsn(stamped, lsn)
        stamped = bytes(stamped)

        before_full = bytes(before) if before is not None else b"\x00" * len(stamped)
        if len(before_full) != len(stamped):
            before_full = before_full.ljust(len(stamped), b"\x00")[:len(stamped)]

        lo, hi = _diff_range(before_full, stamped)
        if lo is None:
            return stamped
        # We log the contiguous [lo, hi) window that brackets the change, but
        # Undo restores only the bytes that actually differ (see _phase_undo):
        # that way undoing a loser's insert in a high slot never clobbers a
        # committed change (e.g. a delete) that lives in a lower slot of the
        # same page but happens to fall inside the same window.
        self._emit_with_lsn(
            lsn, xid, "update",
            page_id=f"{file_id}{_PAGE_SEP}{page_id}",
            offset=lo,
            before=_encode_bytes(before_full[lo:hi]),
            after=_encode_bytes(stamped[lo:hi]),
        )

        key = (file_id, page_id)
        self.dirty_page_table.setdefault(key, lsn)
        self._maybe_checkpoint()
        return stamped

    # ==================================================================
    # WAL #1 helpers  (called by the BufferManager around dirty-page writes)
    # ==================================================================
    @staticmethod
    def page_lsn_of(data: bytes) -> int:
        return read_page_lsn(data)

    def flush_log_up_to(self, lsn: int) -> None:
        """Ensure every log record with LSN <= lsn is on disk."""
        if lsn <= self._flushed_lsn:
            return
        self._flush_log()

    def page_flushed(self, file_id: str, page_id: int) -> None:
        """A page just became clean on disk — drop it from the Dirty Page Table."""
        self.dirty_page_table.pop((file_id, page_id), None)

    # ==================================================================
    # Fuzzy checkpointing
    # ==================================================================
    def _maybe_checkpoint(self) -> None:
        self._ops_since_ckpt += 1
        if self._ops_since_ckpt >= self._checkpoint_interval:
            self._checkpoint()
            self._ops_since_ckpt = 0

    def _checkpoint(self) -> None:
        begin_lsn = self._emit(_SYS_XID, "begin_chkpt")
        # The end_checkpoint record carries snapshots of both tables, packed
        # into the spare before/after fields.
        self._emit(_SYS_XID, "end_chkpt",
                   before=self._serialise_tx_table(),
                   after=self._serialise_dpt())
        self._flush_log()
        self._write_master(begin_lsn)              # master points at a *complete* ckpt

    def _serialise_tx_table(self) -> str:
        return "|".join(
            f"{xid}:{e['status']}:{e['lastLSN']}"
            for xid, e in self.tx_table.items()
        )

    def _serialise_dpt(self) -> str:
        return "|".join(
            f"{fid}{_PAGE_SEP}{pid}:{rec_lsn}"
            for (fid, pid), rec_lsn in self.dirty_page_table.items()
        )

    @staticmethod
    def _deserialise_tx_table(s: str) -> Dict[int, dict]:
        out: Dict[int, dict] = {}
        if not s:
            return out
        for part in s.split("|"):
            xid_s, status, last_s = part.split(":")
            out[int(xid_s)] = {"status": status, "lastLSN": int(last_s)}
        return out

    @staticmethod
    def _deserialise_dpt(s: str) -> Dict[Tuple[str, int], int]:
        out: Dict[Tuple[str, int], int] = {}
        if not s:
            return out
        for part in s.split("|"):
            segs = part.rsplit(":", 2)          # file_id : page_id : recLSN
            if len(segs) == 3:
                fid, pid_s, rec_s = segs
                out[(fid, int(pid_s))] = int(rec_s)
        return out

    # ==================================================================
    # Three-phase recovery
    # ==================================================================
    def _run_recovery(self) -> None:
        records = self._read_all_log_records()
        if not records:
            return

        begin_ckpt_lsn = self._read_master()
        self._phase_analysis(records, begin_ckpt_lsn)
        self._phase_redo(records)
        self._phase_undo(records)

        # Any transaction that committed but crashed before writing its end
        # record gets one now.
        for xid, entry in list(self.tx_table.items()):
            if entry["status"] == "committed":
                self._emit(xid, "end")
                self.tx_table.pop(xid, None)
        self._flush_log()

    # ── Phase 1 — Analysis ────────────────────────────────────────────
    def _phase_analysis(self, records: List[dict], begin_ckpt_lsn: int) -> None:
        tx_snap = dpt_snap = ""
        scan_from = _NO_LSN          # process everything if there is no checkpoint

        if begin_ckpt_lsn >= 0:
            for idx, rec in enumerate(records):
                if rec["lsn"] == begin_ckpt_lsn and rec["type"] == "begin_chkpt":
                    for r2 in records[idx + 1:]:
                        if r2["type"] == "end_chkpt":
                            tx_snap, dpt_snap = r2.get("before", ""), r2.get("after", "")
                            scan_from = r2["lsn"]
                            break
                    break

        self.tx_table = self._deserialise_tx_table(tx_snap)
        self.dirty_page_table = self._deserialise_dpt(dpt_snap)

        for rec in records:
            if rec["lsn"] <= scan_from:
                continue
            rtype, xid = rec["type"], rec["xid"]

            if rtype == "update":
                if xid not in self.tx_table:
                    self.tx_table[xid] = {"status": "active", "lastLSN": _NO_LSN}
                self.tx_table[xid]["status"] = "active"
                self.tx_table[xid]["lastLSN"] = rec["lsn"]
                key = self._parse_page_key(rec.get("page_id", ""))
                if key is not None:
                    self.dirty_page_table.setdefault(key, rec["lsn"])
            elif rtype == "commit":
                if xid in self.tx_table:
                    self.tx_table[xid]["status"] = "committed"
                    self.tx_table[xid]["lastLSN"] = rec["lsn"]
            elif rtype == "end":
                self.tx_table.pop(xid, None)

    # ── Phase 2 — Redo ("repeat history") ─────────────────────────────
    def _phase_redo(self, records: List[dict]) -> None:
        if not self.dirty_page_table:
            return
        min_rec_lsn = min(self.dirty_page_table.values())

        for rec in records:
            if rec["type"] != "update" or rec["lsn"] < min_rec_lsn:
                continue
            key = self._parse_page_key(rec.get("page_id", ""))
            if key is None or key not in self.dirty_page_table:
                continue
            if rec["lsn"] < self.dirty_page_table[key]:
                continue

            file_id, page_id = key
            exists = self.disk.file_exists(file_id) and \
                page_id < self.disk.num_pages(file_id)
            if exists:
                pr = self.disk.read_page(file_id, page_id)
                if pr.status != "success":
                    continue
                data = bytearray(pr.data)
                if read_page_lsn(data) >= rec["lsn"]:
                    continue                      # already on disk — idempotent skip
            else:
                # The page never reached disk before the crash; recreate it
                # (zero-filled) so we can replay the change onto it.
                self._ensure_page_exists(file_id, page_id)
                pr = self.disk.read_page(file_id, page_id)
                if pr.status != "success":
                    continue
                data = bytearray(pr.data)

            after = _decode_bytes(rec.get("after", ""))
            off = int(rec.get("offset", 0))
            data[off:off + len(after)] = after
            self.disk.write_page(file_id, page_id, bytes(data))

    # ── Phase 3 — Undo (roll back the losers) ─────────────────────────
    def _phase_undo(self, records: List[dict]) -> None:
        losers = {xid for xid, e in self.tx_table.items()
                  if e["status"] == "active"}
        if not losers:
            return

        lsn_index = {r["lsn"]: r for r in records}
        to_undo: Dict[int, int] = {}
        for xid in losers:
            last = self.tx_table[xid]["lastLSN"]
            if last != _NO_LSN:
                to_undo[xid] = last

        while to_undo:
            # Globally pick the largest LSN still to be undone.
            xid = max(to_undo, key=lambda x: to_undo[x])
            lsn = to_undo[xid]
            rec = lsn_index.get(lsn)

            if rec is not None and rec["type"] == "update":
                key = self._parse_page_key(rec.get("page_id", ""))
                if key is not None:
                    file_id, page_id = key
                    if self.disk.file_exists(file_id) and \
                            page_id < self.disk.num_pages(file_id):
                        pr = self.disk.read_page(file_id, page_id)
                        if pr.status == "success":
                            before = _decode_bytes(rec.get("before", ""))
                            after = _decode_bytes(rec.get("after", ""))
                            off = int(rec.get("offset", 0))
                            data = bytearray(pr.data)
                            # Restore ONLY the bytes this record changed; leave
                            # the rest of the window untouched so we don't undo
                            # a later committed change that shares the window.
                            for i in range(len(before)):
                                if i >= len(after) or before[i] != after[i]:
                                    data[off + i] = before[i]
                            self.disk.write_page(file_id, page_id, bytes(data))

            prev = rec["prev_lsn"] if rec is not None else _NO_LSN
            if prev == _NO_LSN:
                self._emit(xid, "end")            # chain exhausted
                self.tx_table.pop(xid, None)
                del to_undo[xid]
            else:
                to_undo[xid] = prev

    def _ensure_page_exists(self, file_id: str, page_id: int) -> None:
        if not self.disk.file_exists(file_id):
            self.disk.create_file(file_id)
        while self.disk.num_pages(file_id) <= page_id:
            pid = self.disk.num_pages(file_id)
            self.disk.write_page(file_id, pid, b"\x00" * self.page_size)

    # ==================================================================
    # Log buffer / file I/O  (the log is written through the DiskSpaceManager)
    # ==================================================================
    def _emit(self, xid: int, rtype: str, **kwargs) -> int:
        lsn = self._next_lsn
        self._next_lsn += 1
        return self._emit_with_lsn(lsn, xid, rtype, **kwargs)

    def _emit_with_lsn(self, lsn: int, xid: int, rtype: str, **kwargs) -> int:
        prev = _NO_LSN
        if xid >= 0 and xid in self.tx_table:
            prev = self.tx_table[xid]["lastLSN"]

        self._log_buffer.append({
            "lsn":      lsn,
            "prev_lsn": prev,
            "xid":      xid,
            "type":     rtype,
            "page_id":  kwargs.get("page_id", ""),
            "offset":   kwargs.get("offset", ""),
            "before":   kwargs.get("before", ""),
            "after":    kwargs.get("after", ""),
        })
        if xid >= 0 and xid in self.tx_table:
            self.tx_table[xid]["lastLSN"] = lsn

        if len(self._log_buffer) >= self._log_buffer_size:
            self._flush_log()
        return lsn

    def _flush_log(self) -> None:
        """Push the in-memory log buffer to wal.log via the DiskSpaceManager.

        log_append flushes the OS file handle on every call, so records survive
        an os._exit(1) crash even though Python's destructors never run.
        """
        if not self._log_buffer:
            return
        for rec in self._log_buffer:
            self.disk.log_append(self._format_line(rec))
        self._flushed_lsn = self._log_buffer[-1]["lsn"]
        self._log_buffer.clear()

    # public alias used by archive.py on normal shutdown
    def flush_log(self) -> None:
        self._flush_log()

    @staticmethod
    def _format_line(rec: dict) -> str:
        buf = io.StringIO()
        csv.writer(buf).writerow([rec[f] for f in _FIELDS])
        return buf.getvalue().rstrip("\r\n")

    def _read_all_log_records(self) -> List[dict]:
        records: List[dict] = []
        for line in self.disk.log_read_lines():
            if not line:
                continue
            try:
                row = next(csv.reader([line]))
            except StopIteration:
                continue
            if len(row) < len(_FIELDS):
                continue
            rec = dict(zip(_FIELDS, row))
            try:
                rec["lsn"] = int(rec["lsn"])
                rec["prev_lsn"] = int(rec["prev_lsn"]) if rec["prev_lsn"] else _NO_LSN
                rec["xid"] = int(rec["xid"])
                if rec["offset"] != "":
                    rec["offset"] = int(rec["offset"])
            except (ValueError, KeyError):
                continue
            records.append(rec)
        return records

    # ==================================================================
    # Master record (the begin_checkpoint LSN of the latest complete ckpt)
    # ==================================================================
    def _write_master(self, lsn: int) -> None:
        with open(self._master_path, "w") as f:
            f.write(str(lsn))
            f.flush()
            os.fsync(f.fileno())

    def _read_master(self) -> int:
        if not os.path.exists(self._master_path):
            return _NO_LSN
        try:
            with open(self._master_path) as f:
                return int(f.read().strip())
        except (ValueError, OSError):
            return _NO_LSN

    # ==================================================================
    # Utility
    # ==================================================================
    @staticmethod
    def _parse_page_key(page_id_str: str) -> Optional[Tuple[str, int]]:
        if not page_id_str:
            return None
        idx = page_id_str.rfind(_PAGE_SEP)
        if idx < 0:
            return None
        try:
            return page_id_str[:idx], int(page_id_str[idx + 1:])
        except ValueError:
            return None
