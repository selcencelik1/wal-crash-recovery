"""
QueryProcessor — Layer 4.

Top-level layer. Parses each input line into a command, dispatches to
FileIndexManager, and is responsible for the three output artefacts:

    * output.txt        — query results (overwrites at session start)
    * stats_output.txt  — snapshot written each `stats` command
    * log.csv           — append-only audit log; survives restarts

It does not touch disk directly for data — it only reads counters from
DiskSpaceManager and BufferManager to assemble explain/stats output.
"""

import os
import time
from typing import List

from models import RecordResult


class QueryProcessor:
    def __init__(self, config: dict, file_idx, buffer, disk):
        self.config = config
        self.file_idx = file_idx
        self.buffer = buffer
        self.disk = disk

        # Output destinations live next to archive.py, independent of cwd
        # and independent of any config-provided paths.
        archive_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.output_path = os.path.join(archive_dir, "output.txt")
        self.stats_path = os.path.join(archive_dir, "stats_output.txt")
        self.log_path = os.path.join(archive_dir, "log.csv")

        # output.txt is a fresh artifact per run (the spec example shows
        # the queries-of-this-run accumulated in it). log.csv is *not*
        # cleared — it must survive restarts.
        with open(self.output_path, "w") as f:
            pass

        # Cumulative counters for the `stats` snapshot. These reset on
        # `stats reset` along with the lower-layer counters.
        self.records_scanned = 0
        self.records_returned = 0
        self.pages_scanned = 0
        self.index_nodes_visited = 0

    # ================================================================ #
    # public entry — called by archive.main once per non-blank input line
    # ================================================================ #
    def process(self, line: str) -> None:
        line = line.strip()
        if not line:
            return
        tokens = line.split()
        op = tokens[0].lower()

        try:
            if op == "create":
                self._handle_create(tokens, line)
            elif op == "delete":
                self._handle_delete(tokens, line)
            elif op == "search":
                self._handle_search(tokens, line)
            elif op == "range_search":
                self._handle_range_search(tokens, line)
            elif op == "explain":
                self._handle_explain(tokens, line)
            elif op == "stats":
                self._handle_stats(tokens, line)
            else:
                self._log(line, "failure")
        except Exception as e:
            # Never crash the system on a malformed command.
            self._log(line, "failure")

    # ================================================================ #
    # CREATE — both `create type` and `create record`
    # ================================================================ #
    def _handle_create(self, tokens: List[str], line: str) -> None:
        if len(tokens) < 2:
            self._log(line, "failure")
            return
        sub = tokens[1].lower()
        if sub == "type":
            self._create_type(tokens, line)
        elif sub == "record":
            self._create_record(tokens, line)
        else:
            self._log(line, "failure")

    def _create_type(self, tokens: List[str], line: str) -> None:
        # create type <name> <num_fields> <pk_order> <f1_name> <f1_type> ...
        if len(tokens) < 5:
            self._log(line, "failure")
            return
        type_name = tokens[2]
        try:
            num_fields = int(tokens[3])
            pk_order = int(tokens[4])
        except ValueError:
            self._log(line, "failure")
            return
        field_tokens = tokens[5:]
        if len(field_tokens) != num_fields * 2:
            self._log(line, "failure")
            return
        fields = []
        for i in range(num_fields):
            fname = field_tokens[i * 2]
            ftype = field_tokens[i * 2 + 1]
            fields.append((fname, ftype))

        result = self.file_idx.create_type(type_name, fields, pk_order)
        self._accumulate(result)
        self._log(line, "success" if result.status == "success" else "failure")

    def _create_record(self, tokens: List[str], line: str) -> None:
        # create record <type> <v1> <v2> ...
        if len(tokens) < 3:
            self._log(line, "failure")
            return
        type_name = tokens[2]
        values = tokens[3:]
        result = self.file_idx.create_record(type_name, values)
        self._accumulate(result)
        self._log(line, "success" if result.status == "success" else "failure")

    # ================================================================ #
    # DELETE
    # ================================================================ #
    def _handle_delete(self, tokens: List[str], line: str) -> None:
        if len(tokens) != 4 or tokens[1].lower() != "record":
            self._log(line, "failure")
            return
        type_name = tokens[2]
        pk_value = tokens[3]
        result = self.file_idx.delete_record(type_name, pk_value)
        self._accumulate(result)
        self._log(line, "success" if result.status == "success" else "failure")

    # ================================================================ #
    # SEARCH (equality)
    # ================================================================ #
    def _handle_search(self, tokens: List[str], line: str,
                       suppress_output: bool = False,
                       meta=None,
                       log_operation: bool = True) -> RecordResult:
        if len(tokens) != 4 or tokens[1].lower() != "record":
            if log_operation:
                self._log(line, "failure")
            return RecordResult(status="failure", error="bad search syntax")
        type_name = tokens[2]
        pk_value = tokens[3]
        result = self.file_idx.search_record(type_name, pk_value, meta=meta)
        self._accumulate(result)
        if result.status == "success" and result.records and not suppress_output:
            for rec in result.records:
                self._write_output(self._format_record(rec))
        if log_operation:
            self._log(line, "success" if result.status == "success" else "failure")
        return result

    # ================================================================ #
    # RANGE SEARCH
    # ================================================================ #
    def _handle_range_search(self, tokens: List[str], line: str,
                             suppress_output: bool = False,
                             meta=None,
                             log_operation: bool = True) -> RecordResult:
        # range_search <type> <field> <low> <high>
        if len(tokens) != 5:
            if log_operation:
                self._log(line, "failure")
            return RecordResult(status="failure", error="bad range_search syntax")
        type_name = tokens[1]
        field_name = tokens[2]
        low, high = tokens[3], tokens[4]
        result = self.file_idx.range_search(type_name, field_name, low, high, meta=meta)
        self._accumulate(result)
        if result.status == "success" and not suppress_output:
            for rec in result.records:
                self._write_output(self._format_record(rec))
        # Range search with zero matches is still a success.
        if log_operation:
            self._log(line, "success" if result.status == "success" else "failure")
        return result

    # ================================================================ #
    # EXPLAIN <any DML>
    # ================================================================ #
    def _handle_explain(self, tokens: List[str], line: str) -> None:
        if len(tokens) < 2:
            self._log(line, "failure")
            return
        # The DML command starts at tokens[1].
        inner_tokens = tokens[1:]
        inner_line = " ".join(inner_tokens)
        op = inner_tokens[0].lower()

        # Planning phase — figure out estimated strategy + I/O.
        plan_strategy, plan_io = self._estimate(inner_tokens)

        # Pre-fetch the type's catalog meta *before* we snapshot, so
        # the catalog buffer access (which always happens — the catalog
        # is itself a paged file) doesn't show up in this operation's
        # "Actual I/O". The catalog access still goes through the buffer
        # manager (cumulative stats include it); we just isolate the
        # data + index work for the explain block.
        pre_meta = self._prefetch_meta(inner_tokens)

        # Snapshot counters *before* the operation so we can report
        # the work this single command did, not cumulative totals.
        # "Actual I/O" reads/writes are the number of logical page
        # I/Os the operation performed (i.e. how many times it asked
        # the buffer for a read or a write) — independent of whether
        # those requests were absorbed by the cache. "Buffer Hits /
        # Misses" then break down how the cache served them.
        read_req_before  = self.buffer.read_requests
        write_req_before = self.buffer.write_requests
        hits_before      = self.buffer.hits
        misses_before    = self.buffer.misses

        # Execute (without writing the bare result to output.txt; we
        # will hand-format the RESULT block ourselves).
        result = self._execute_inner(
            inner_tokens, inner_line, suppress_output=True, pre_meta=pre_meta,
        )

        actual_reads  = self.buffer.read_requests  - read_req_before
        actual_writes = self.buffer.write_requests - write_req_before
        buf_hits      = self.buffer.hits           - hits_before
        buf_misses    = self.buffer.misses         - misses_before

        if result is not None and result.error == "bad explain syntax":
            self._log(line, "failure")
            return

        # Use the strategy actually exercised, if the executor reported one.
        actual_strategy = (
            result.strategy if result is not None and result.strategy
            else plan_strategy
        )

        # Compose the explain block (single-space layout matching the
        # reference output, not the column-padded form).
        block = []
        block.append("--- PLAN ---")
        block.append(f"Query: {inner_line}")
        block.append(f"Strategy: {actual_strategy}")
        block.append(f"Estimated I/O: {plan_io}")
        block.append("--- RESULT ---")
        if result is not None and result.records:
            for rec in result.records:
                block.append(self._format_record(rec))
        block.append("--- STATS ---")
        block.append(f"Actual I/O: {actual_reads} reads, {actual_writes} writes")
        block.append(f"Buffer Hits: {buf_hits}")
        block.append(f"Buffer Misses: {buf_misses}")
        # Pages Scanned == total page operations (logical reads + writes).
        block.append(f"Pages Scanned: {actual_reads + actual_writes}")

        self._write_output("\n".join(block))
        status = "success" if result is not None and result.status == "success" else "failure"
        self._log(line, status)

    def _estimate(self, inner_tokens: List[str]):
        op = inner_tokens[0].lower()
        if op == "create" and len(inner_tokens) >= 3 and inner_tokens[1].lower() == "record":
            return self.file_idx.estimate_io("insert", inner_tokens[2])
        if op == "delete" and len(inner_tokens) >= 3 and inner_tokens[1].lower() == "record":
            return self.file_idx.estimate_io("delete", inner_tokens[2])
        if op == "search" and len(inner_tokens) >= 3 and inner_tokens[1].lower() == "record":
            return self.file_idx.estimate_io("search", inner_tokens[2])
        if op == "range_search" and len(inner_tokens) >= 3:
            return self.file_idx.estimate_io("range", inner_tokens[1], inner_tokens[2])
        return self.file_idx.index_strategy, 1

    def _execute_inner(self, tokens: List[str], line: str,
                       suppress_output: bool, pre_meta=None):
        op = tokens[0].lower()
        if op == "search":
            return self._handle_search(
                tokens, line, suppress_output=suppress_output, meta=pre_meta,
                log_operation=False,
            )
        if op == "range_search":
            return self._handle_range_search(
                tokens, line, suppress_output=suppress_output, meta=pre_meta,
                log_operation=False,
            )
        if op == "create" and len(tokens) >= 3 and tokens[1].lower() == "record":
            result = self.file_idx.create_record(tokens[2], tokens[3:], meta=pre_meta)
            self._accumulate(result)
            return result
        if op == "delete" and len(tokens) == 4 and tokens[1].lower() == "record":
            result = self.file_idx.delete_record(tokens[2], tokens[3], meta=pre_meta)
            self._accumulate(result)
            return result
        return RecordResult(status="failure", error="bad explain syntax")

    def _prefetch_meta(self, inner_tokens: List[str]):
        """Resolve the type-name argument of a DML command and pull its
        catalog entry into the buffer. Used by EXPLAIN so the catalog
        lookup happens *outside* the snapshot window.
        """
        op = inner_tokens[0].lower() if inner_tokens else ""
        type_name = None
        if op in ("create", "delete", "search") and len(inner_tokens) >= 3 \
                and inner_tokens[1].lower() == "record":
            type_name = inner_tokens[2]
        elif op == "range_search" and len(inner_tokens) >= 2:
            type_name = inner_tokens[1]
        if not type_name:
            return None
        return self.file_idx.get_meta(type_name)

    # ================================================================ #
    # STATS  /  STATS RESET
    # ================================================================ #
    def _handle_stats(self, tokens: List[str], line: str) -> None:
        if len(tokens) == 2 and tokens[1].lower() == "reset":
            self.disk.reset_stats()
            self.buffer.reset_stats()
            self.records_scanned = 0
            self.records_returned = 0
            self.pages_scanned = 0
            self.index_nodes_visited = 0
            self._log(line, "success")
            return
        if len(tokens) != 1:
            self._log(line, "failure")
            return

        # Experiment stats should represent total workload disk I/O, including
        # dirty pages that are still buffered when the stats command runs.
        self.buffer.flush()
        io = self.disk.get_io_stats()
        bs = self.buffer.get_stats()
        strategy = self.file_idx.index_strategy

        # stats_output.txt is overwritten each `stats` (snapshot, not log).
        lines = []
        lines.append("=== STATISTICS ===")
        lines.append(f"Disk I/O: {io['reads']} reads, {io['writes']} writes")
        lines.append(
            f"Buffer Pool: {bs['requests']} requests, {bs['hits']} hits, "
            f"{bs['misses']} misses ({bs['hit_rate']:.1f}% hit rate)"
        )
        lines.append(
            f"Evictions: {bs['evictions']} ({bs['dirty_writebacks']} dirty writebacks)"
        )
        nodes_visited = self.index_nodes_visited if strategy != "heap_scan" else 0
        lines.append(f"Index: {strategy}, {nodes_visited} nodes visited")
        lines.append(
            f"Records: {self.records_scanned} scanned, {self.records_returned} returned"
        )
        with open(self.stats_path, "w") as f:
            f.write("\n".join(lines) + "\n")
        self._log(line, "success")

    # ================================================================ #
    # output / logging helpers
    # ================================================================ #
    @staticmethod
    def _format_record(record: List) -> str:
        return " ".join(str(v) for v in record)

    def _write_output(self, text: str) -> None:
        with open(self.output_path, "a") as f:
            f.write(text + "\n")

    def _log(self, op: str, status: str) -> None:
        # log.csv is append-only, persistent across runs.
        ts = int(time.time())
        # Operation string contains spaces but no commas (alphanumeric per spec),
        # so a 3-column CSV is unambiguous.
        with open(self.log_path, "a") as f:
            f.write(f"{ts},{op},{status}\n")

    def _accumulate(self, result: RecordResult) -> None:
        self.records_scanned += result.records_scanned
        self.records_returned += len(result.records)
        self.pages_scanned += result.pages_accessed
        self.index_nodes_visited += result.index_nodes_visited
