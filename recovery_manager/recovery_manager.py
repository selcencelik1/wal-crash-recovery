"""
RecoveryManager — Write-Ahead Logging + ARIES crash recovery.

Owns:
  - wal.log  (append-only log file, never in buffer pool)
  - master   (one-line file storing the LSN of the last begin_checkpoint)

WAL invariants enforced:
  WAL #1  flush_log_up_to(lsn)  called by BufferManager before every
          dirty-page write.  Guarantees flushedLSN >= pageLSN.
  WAL #2  commit() fsyncs the log before returning.

Recovery (three-phase ARIES):
  Analysis → Redo → Undo
  Runs automatically on construction, before any input is processed.
"""

import csv
import io
import os
import time
from typing import Dict, List, Optional, Tuple


# ── Log record field names ──────────────────────────────────────────────────
_FIELDS = ["lsn", "prev_lsn", "xid", "type", "page_id",
           "offset", "before", "after"]

# page_id field in wal.log uses "<file_id>:<page_id>" so it survives restarts.
_PAGE_SEP = ":"

# Sentinel for "no previous LSN in this transaction chain".
_NO_LSN = -1


def _encode_bytes(b: bytes) -> str:
    """Hex-encode bytes so they survive CSV round-trips."""
    return b.hex() if b else ""


def _decode_bytes(s: str) -> bytes:
    return bytes.fromhex(s) if s else b""


# ── pageLSN layout inside a page header ────────────────────────────────────
# We reserve the first 8 bytes of every data page for the pageLSN (int64 LE).
PAGE_LSN_OFFSET = 0
PAGE_LSN_SIZE   = 8   # bytes

def read_page_lsn(data: bytes) -> int:
    return int.from_bytes(data[PAGE_LSN_OFFSET:PAGE_LSN_OFFSET + PAGE_LSN_SIZE],
                          "little", signed=False)

def write_page_lsn(data: bytearray, lsn: int) -> None:
    data[PAGE_LSN_OFFSET:PAGE_LSN_OFFSET + PAGE_LSN_SIZE] = \
        lsn.to_bytes(PAGE_LSN_SIZE, "little")


class RecoveryManager:
    # ------------------------------------------------------------------
    # Construction + startup recovery
    # ------------------------------------------------------------------

    def __init__(self, config: dict, disk):
        self.config  = config
        self.disk    = disk

        base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self._wal_path    = os.path.join(base, "wal.log")
        self._master_path = os.path.join(base, "master.rec")

        self._checkpoint_interval: int = int(config.get("checkpoint_interval", 50))
        self._log_buffer_size:     int = int(config.get("log_buffer_size", 8))

        # ── in-memory log buffer ──────────────────────────────────────
        self._log_buffer: List[dict] = []

        # ── LSN counter — read from wal.log so it never resets ────────
        self._next_lsn: int = self._recover_next_lsn()

        # ── flushedLSN: highest LSN definitely on disk ────────────────
        self._flushed_lsn: int = self._read_flushed_lsn()

        # ── Transaction Table  {xid -> {"status", "lastLSN"}} ─────────
        self.tx_table: Dict[int, dict] = {}

        # ── Dirty Page Table  {(file_id, page_id) -> recLSN} ──────────
        self.dirty_page_table: Dict[Tuple[str, int], int] = {}

        # ── XID generator ────────────────────────────────────────────
        self._next_xid: int = self._recover_next_xid()

        # ── operations since last checkpoint ─────────────────────────
        self._ops_since_ckpt: int = 0

        # ── reference to BufferManager (set later by archive.py) ─────
        self.buffer = None   # injected after construction

        # ── Three-phase recovery ──────────────────────────────────────
        self._run_recovery()

    # ------------------------------------------------------------------
    # XID management
    # ------------------------------------------------------------------

    def begin_transaction(self) -> int:
        xid = self._next_xid
        self._next_xid += 1
        self.tx_table[xid] = {"status": "active", "lastLSN": _NO_LSN}
        self._maybe_checkpoint()
        return xid

    def commit(self, xid: int) -> None:
        """WAL #2: commit record + fsync before returning."""
        lsn = self._append(xid, "commit")
        self.flush_log_up_to(lsn)           # fsync via _flush_log
        self._fsync_log()
        if xid in self.tx_table:
            self.tx_table[xid]["status"] = "committed"
        # end record — also flushed immediately
        end_lsn = self._append(xid, "end")
        self.flush_log_up_to(end_lsn)
        self.tx_table.pop(xid, None)

    def abort(self, xid: int) -> None:
        """Roll back a single transaction (used during Undo phase)."""
        self._undo_transaction(xid)

    # ------------------------------------------------------------------
    # Logging an update (called by FileIndexManager)
    # ------------------------------------------------------------------

    def log_update(self, xid: int, file_id: str, page_id: int,
                   offset: int, before: bytes, after: bytes) -> int:
        """Write an update record and return the new LSN."""
        lsn = self._append(xid, "update",
                           page_id=f"{file_id}{_PAGE_SEP}{page_id}",
                           offset=offset,
                           before=_encode_bytes(before),
                           after=_encode_bytes(after))
        # Update Dirty Page Table
        key = (file_id, page_id)
        if key not in self.dirty_page_table:
            self.dirty_page_table[key] = lsn
        self._maybe_checkpoint()
        return lsn

    # ------------------------------------------------------------------
    # WAL #1 — called by BufferManager before flushing a dirty page
    # ------------------------------------------------------------------

    def flush_log_up_to(self, lsn: int) -> None:
        """Ensure every log record with LSN <= lsn is on disk."""
        if lsn <= self._flushed_lsn:
            return
        # Flush the in-memory buffer to disk.
        self._flush_log()

    # ------------------------------------------------------------------
    # Called by BufferManager when a page becomes clean on disk
    # ------------------------------------------------------------------

    def page_flushed(self, file_id: str, page_id: int) -> None:
        """Remove page from Dirty Page Table when it lands on disk."""
        self.dirty_page_table.pop((file_id, page_id), None)

    # ------------------------------------------------------------------
    # Fuzzy checkpointing
    # ------------------------------------------------------------------

    def _maybe_checkpoint(self) -> None:
        self._ops_since_ckpt += 1
        if self._ops_since_ckpt >= self._checkpoint_interval:
            self._checkpoint()
            self._ops_since_ckpt = 0

    def _checkpoint(self) -> None:
        """Write begin_checkpoint + end_checkpoint records."""
        # begin_checkpoint
        begin_lsn = self._append(-1, "begin_chkpt")
        self._write_master(begin_lsn)

        # end_checkpoint carries snapshots of both tables
        # We serialise them as pipe-delimited strings inside the "before"
        # and "after" fields so no extra CSV columns are needed.
        tx_snap  = self._serialise_tx_table()
        dpt_snap = self._serialise_dpt()
        self._append(-1, "end_chkpt",
                     before=tx_snap,
                     after=dpt_snap)
        self.flush_log_up_to(begin_lsn)

    def _serialise_tx_table(self) -> str:
        parts = []
        for xid, entry in self.tx_table.items():
            parts.append(f"{xid}:{entry['status']}:{entry['lastLSN']}")
        return "|".join(parts)

    def _serialise_dpt(self) -> str:
        parts = []
        for (fid, pid), rec_lsn in self.dirty_page_table.items():
            parts.append(f"{fid}{_PAGE_SEP}{pid}:{rec_lsn}")
        return "|".join(parts)

    @staticmethod
    def _deserialise_tx_table(s: str) -> Dict[int, dict]:
        result = {}
        if not s:
            return result
        for part in s.split("|"):
            xid_s, status, last_s = part.split(":")
            result[int(xid_s)] = {"status": status, "lastLSN": int(last_s)}
        return result

    @staticmethod
    def _deserialise_dpt(s: str) -> Dict[Tuple[str, int], int]:
        result = {}
        if not s:
            return result
        for part in s.split("|"):
            # format: file_id:page_id:recLSN
            # split on last two colons
            segs = part.rsplit(":", 2)
            if len(segs) == 3:
                fid, pid_s, rec_s = segs
                result[(fid, int(pid_s))] = int(rec_s)
        return result

    # ------------------------------------------------------------------
    # Three-phase recovery
    # ------------------------------------------------------------------

    def _run_recovery(self) -> None:
        all_records = self._read_all_log_records()
        if not all_records:
            return

        begin_ckpt_lsn = self._read_master()
        self._phase_analysis(all_records, begin_ckpt_lsn)
        self._phase_redo(all_records)
        self._phase_undo(all_records)

        # Write end records for transactions that committed but lacked one.
        for xid, entry in list(self.tx_table.items()):
            if entry["status"] == "committed":
                self._append(xid, "end")
                self.tx_table.pop(xid)
        self._flush_log()

    # ── Analysis ──────────────────────────────────────────────────────

    def _phase_analysis(self, records: List[dict], begin_ckpt_lsn: int) -> None:
        # Find the end_checkpoint record that follows begin_ckpt_lsn.
        tx_snap = dpt_snap = ""
        scan_from = 0

        if begin_ckpt_lsn >= 0:
            for rec in records:
                if rec["lsn"] == begin_ckpt_lsn and rec["type"] == "begin_chkpt":
                    # The very next end_chkpt record carries the snapshots.
                    idx = records.index(rec)
                    for r2 in records[idx + 1:]:
                        if r2["type"] == "end_chkpt":
                            tx_snap  = r2.get("before", "")
                            dpt_snap = r2.get("after",  "")
                            scan_from = r2["lsn"]
                            break
                    break

        # Restore tables from checkpoint snapshot.
        self.tx_table        = self._deserialise_tx_table(tx_snap)
        self.dirty_page_table = self._deserialise_dpt(dpt_snap)

        # Replay records after the checkpoint.
        for rec in records:
            if rec["lsn"] <= scan_from:
                continue
            rtype = rec["type"]
            xid   = rec["xid"]

            if rtype == "update":
                if xid not in self.tx_table:
                    self.tx_table[xid] = {"status": "active", "lastLSN": _NO_LSN}
                self.tx_table[xid]["status"]  = "active"
                self.tx_table[xid]["lastLSN"] = rec["lsn"]
                page_key = self._parse_page_key(rec.get("page_id", ""))
                if page_key and page_key not in self.dirty_page_table:
                    self.dirty_page_table[page_key] = rec["lsn"]

            elif rtype == "commit":
                if xid in self.tx_table:
                    self.tx_table[xid]["status"]  = "committed"
                    self.tx_table[xid]["lastLSN"] = rec["lsn"]

            elif rtype == "end":
                self.tx_table.pop(xid, None)

    # ── Redo ──────────────────────────────────────────────────────────

    def _phase_redo(self, records: List[dict]) -> None:
        if not self.dirty_page_table:
            return
        min_rec_lsn = min(self.dirty_page_table.values())

        for rec in records:
            if rec["lsn"] < min_rec_lsn:
                continue
            if rec["type"] != "update":
                continue

            page_key = self._parse_page_key(rec.get("page_id", ""))
            if page_key is None:
                continue
            file_id, page_id = page_key

            # Skip if page not in DPT
            if page_key not in self.dirty_page_table:
                continue
            # Skip if record LSN < recLSN
            if rec["lsn"] < self.dirty_page_table[page_key]:
                continue
            # Skip if page on disk already reflects this update
            if not self.disk.file_exists(file_id):
                continue
            if page_id >= self.disk.num_pages(file_id):
                continue
            page_result = self.disk.read_page(file_id, page_id)
            if page_result.status != "success":
                continue
            page_lsn = read_page_lsn(page_result.data)
            if page_lsn >= rec["lsn"]:
                continue

            # Apply after-image
            after = _decode_bytes(rec.get("after", ""))
            offset = int(rec.get("offset", 0))
            new_data = bytearray(page_result.data)
            new_data[offset:offset + len(after)] = after
            write_page_lsn(new_data, rec["lsn"])
            self.disk.write_page(file_id, page_id, bytes(new_data))

    # ── Undo ──────────────────────────────────────────────────────────

    def _phase_undo(self, records: List[dict]) -> None:
        # Losers = transactions still active after Analysis
        losers = {xid for xid, e in self.tx_table.items()
                  if e["status"] == "active"}
        if not losers:
            return

        # Build index: lsn -> record
        lsn_index = {r["lsn"]: r for r in records}

        # toUndo set: maps xid -> current LSN to undo
        to_undo: Dict[int, int] = {}
        for xid in losers:
            last = self.tx_table[xid]["lastLSN"]
            if last != _NO_LSN:
                to_undo[xid] = last

        while to_undo:
            # Pick the loser with the largest LSN to undo next.
            xid = max(to_undo, key=lambda x: to_undo[x])
            lsn = to_undo[xid]
            rec = lsn_index.get(lsn)

            if rec is None or rec["type"] != "update":
                # Nothing to undo at this LSN; follow prev_lsn.
                prev = rec["prev_lsn"] if rec else _NO_LSN
                if prev == _NO_LSN:
                    self._append(xid, "end")
                    del to_undo[xid]
                    self.tx_table.pop(xid, None)
                else:
                    to_undo[xid] = prev
                continue

            # Apply before-image
            page_key = self._parse_page_key(rec.get("page_id", ""))
            if page_key:
                file_id, page_id = page_key
                if self.disk.file_exists(file_id) and \
                        page_id < self.disk.num_pages(file_id):
                    page_result = self.disk.read_page(file_id, page_id)
                    if page_result.status == "success":
                        before = _decode_bytes(rec.get("before", ""))
                        offset = int(rec.get("offset", 0))
                        new_data = bytearray(page_result.data)
                        new_data[offset:offset + len(before)] = before
                        # Use the undo LSN as the new pageLSN
                        write_page_lsn(new_data, lsn)
                        self.disk.write_page(file_id, page_id, bytes(new_data))

            prev = rec.get("prev_lsn", _NO_LSN)
            if isinstance(prev, str):
                prev = int(prev) if prev else _NO_LSN

            if prev == _NO_LSN:
                self._append(xid, "end")
                del to_undo[xid]
                self.tx_table.pop(xid, None)
            else:
                to_undo[xid] = prev

    # ------------------------------------------------------------------
    # Private helpers — log I/O
    # ------------------------------------------------------------------

    def _append(self, xid: int, rtype: str, **kwargs) -> int:
        lsn = self._next_lsn
        self._next_lsn += 1

        prev = _NO_LSN
        if xid >= 0 and xid in self.tx_table:
            prev = self.tx_table[xid]["lastLSN"]

        record = {
            "lsn":      lsn,
            "prev_lsn": prev,
            "xid":      xid,
            "type":     rtype,
            "page_id":  kwargs.get("page_id", ""),
            "offset":   kwargs.get("offset", ""),
            "before":   kwargs.get("before", ""),
            "after":    kwargs.get("after",  ""),
        }
        self._log_buffer.append(record)

        if xid >= 0 and xid in self.tx_table:
            self.tx_table[xid]["lastLSN"] = lsn

        # Auto-flush when buffer is full.
        if len(self._log_buffer) >= self._log_buffer_size:
            self._flush_log()

        return lsn

    def _flush_log(self) -> None:
        if not self._log_buffer:
            return
        needs_header = not os.path.exists(self._wal_path) or \
                       os.path.getsize(self._wal_path) == 0
        with open(self._wal_path, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=_FIELDS,
                                    extrasaction="ignore")
            if needs_header:
                writer.writeheader()
            for rec in self._log_buffer:
                writer.writerow(rec)
        # Update flushedLSN to the last record we just wrote.
        if self._log_buffer:
            self._flushed_lsn = self._log_buffer[-1]["lsn"]
        self._log_buffer.clear()

    def _fsync_log(self) -> None:
        self._flush_log()
        if os.path.exists(self._wal_path):
            with open(self._wal_path, "a") as f:
                os.fsync(f.fileno())

    def _read_all_log_records(self) -> List[dict]:
        if not os.path.exists(self._wal_path):
            return []
        records = []
        try:
            with open(self._wal_path, "r", newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    row["lsn"]      = int(row["lsn"])
                    row["prev_lsn"] = int(row["prev_lsn"]) if row["prev_lsn"] else _NO_LSN
                    row["xid"]      = int(row["xid"])
                    if row["offset"]:
                        row["offset"] = int(row["offset"])
                    records.append(row)
        except Exception:
            pass
        return records

    def _recover_next_lsn(self) -> int:
        records = self._read_all_log_records()
        if records:
            return records[-1]["lsn"] + 1
        return 0

    def _read_flushed_lsn(self) -> int:
        records = self._read_all_log_records()
        if records:
            return records[-1]["lsn"]
        return -1

    def _recover_next_xid(self) -> int:
        records = self._read_all_log_records()
        max_xid = -1
        for r in records:
            if r["xid"] >= 0:
                max_xid = max(max_xid, r["xid"])
        return max_xid + 1

    # ------------------------------------------------------------------
    # Master record (stores begin_checkpoint LSN)
    # ------------------------------------------------------------------

    def _write_master(self, lsn: int) -> None:
        with open(self._master_path, "w") as f:
            f.write(str(lsn))

    def _read_master(self) -> int:
        if not os.path.exists(self._master_path):
            return -1
        try:
            with open(self._master_path) as f:
                return int(f.read().strip())
        except (ValueError, OSError):
            return -1

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_page_key(page_id_str: str) -> Optional[Tuple[str, int]]:
        if not page_id_str:
            return None
        # Format: "file_id:page_id"
        idx = page_id_str.rfind(_PAGE_SEP)
        if idx < 0:
            return None
        try:
            return page_id_str[:idx], int(page_id_str[idx + 1:])
        except ValueError:
            return None

    def _undo_transaction(self, xid: int) -> None:
        """Undo a single active transaction (runtime abort, not recovery)."""
        records = self._read_all_log_records()
        lsn_index = {r["lsn"]: r for r in records}
        if xid not in self.tx_table:
            return
        cur = self.tx_table[xid]["lastLSN"]
        while cur != _NO_LSN:
            rec = lsn_index.get(cur)
            if rec is None:
                break
            if rec["type"] == "update":
                page_key = self._parse_page_key(rec.get("page_id", ""))
                if page_key and self.buffer:
                    file_id, page_id = page_key
                    buf_result = self.buffer.get_page(file_id, page_id)
                    if buf_result.status == "success":
                        before = _decode_bytes(rec.get("before", ""))
                        offset = int(rec.get("offset", 0))
                        new_data = bytearray(buf_result.page.data)
                        new_data[offset:offset + len(before)] = before
                        write_page_lsn(new_data, cur)
                        self.buffer.write_page(file_id, page_id, bytes(new_data))
            prev = rec.get("prev_lsn", _NO_LSN)
            cur = int(prev) if prev and prev != _NO_LSN else _NO_LSN
        self._append(xid, "end")
        self.tx_table.pop(xid, None)
        self._flush_log()
