"""
FileIndexManager — Layer 3.

Knows about records, relations (types), pages, and indexes.
Every page access goes through the BufferManager.

Public surface (used by QueryProcessor):

    create_type(type_name, fields, pk_order)              -> RecordResult
    create_record(type_name, values)                       -> RecordResult
    delete_record(type_name, pk_value)                     -> RecordResult
    search_record(type_name, pk_value)                     -> RecordResult
    range_search(type_name, field_name, low, high)         -> RecordResult
    estimate_io(operation, type_name, *args)               -> int
    list_types() / get_meta(type_name)
"""

from typing import List, Optional, Tuple

from models import RecordResult
from . import heap_scan, hash_index, bplus_tree
from .catalog import Catalog, FIELD_NAME_LEN, MAX_FIELDS, MIN_FIELDS, TYPE_NAME_LEN
from .bplus_tree import BPlusTree
from .page import STR_FIELD_SIZE


class FileIndexManager:
    def __init__(self, config: dict, buffer):
        self.config = config
        self.buffer = buffer
        self.page_size: int = int(config.get("page_size", 4096))
        # Slot bitmap is physically 10 bytes; spec says "up to 10". Clamp so
        # the configured cap actually governs data-page packing.
        self.max_records_per_page: int = max(
            1, min(10, int(config.get("max_records_per_page", 10)))
        )
        self.index_strategy: str = str(config.get("index_strategy", "heap_scan"))
        if self.index_strategy not in ("heap_scan", "hash_index", "bplus_tree"):
            self.index_strategy = "heap_scan"
        self.hash_buckets: int = int(config.get("hash_num_buckets", 16))

        self.catalog = Catalog(buffer, self.page_size)

    # ================================================================ #
    # introspection
    # ================================================================ #
    def list_types(self) -> List[str]:
        return self.catalog.all_types()

    def get_meta(self, type_name: str):
        return self.catalog.get(type_name)

    # ================================================================ #
    # DDL
    # ================================================================ #
    def create_type(self, type_name: str, fields: List[Tuple[str, str]],
                    pk_order: int) -> RecordResult:
        error = self._validate_type_definition(type_name, fields)
        if error:
            return RecordResult(
                strategy=self.index_strategy,
                status="failure",
                error=error,
            )
        if self.catalog.has_type(type_name):
            return RecordResult(
                strategy=self.index_strategy,
                status="failure",
                error=f"type '{type_name}' already exists",
            )
        if pk_order < 1 or pk_order > len(fields):
            return RecordResult(
                strategy=self.index_strategy,
                status="failure",
                error=f"invalid primary_key_order {pk_order}",
            )
        meta = {
            "type_name": type_name,
            "num_fields": len(fields),
            "primary_key_order": pk_order,
            "num_data_pages": 0,
            "num_records": 0,
            "bplus_root_page": -1,
            "hash_num_buckets": 0,
            "fields": fields,
        }

        try:
            # Create the (still empty) heap file for the relation.
            self.buffer.create_file(type_name)

            # Set up the chosen index.
            if self.index_strategy == "hash_index":
                num_buckets = hash_index.build_empty(
                    self.buffer, self.page_size, type_name, self.hash_buckets,
                )
                meta["hash_num_buckets"] = num_buckets
            elif self.index_strategy == "bplus_tree":
                tree = BPlusTree(self.buffer, self.page_size, meta)
                root_pid = tree.build_empty()
                meta["bplus_root_page"] = root_pid

            if not self.catalog.add_type(meta):
                return RecordResult(
                    strategy=self.index_strategy,
                    status="failure",
                    error="catalog full or duplicate",
                )
        except RuntimeError as e:
            return RecordResult(
                strategy=self.index_strategy,
                status="failure",
                error=str(e),
            )
        return RecordResult(strategy=self.index_strategy)

    # ================================================================ #
    # DML — INSERT
    # ================================================================ #
    def create_record(self, type_name: str, raw_values: List[str],
                      meta: Optional[dict] = None) -> RecordResult:
        if meta is None:
            meta = self.catalog.get(type_name)
        if meta is None:
            return RecordResult(
                strategy=self.index_strategy, status="failure",
                error=f"type '{type_name}' does not exist",
            )
        if len(raw_values) != meta["num_fields"]:
            return RecordResult(
                strategy=self.index_strategy, status="failure",
                error=f"expected {meta['num_fields']} values, got {len(raw_values)}",
            )

        # Coerce raw string tokens into the catalog-declared field types.
        try:
            values = self._coerce_values(meta, raw_values)
        except ValueError as e:
            return RecordResult(
                strategy=self.index_strategy, status="failure",
                error=str(e),
            )

        pk_idx = meta["primary_key_order"] - 1
        pk_value = values[pk_idx]

        try:
            # Reject duplicate primary keys *before* mutating anything.
            dup_check = self._lookup_pointer(meta, pk_value)
            if dup_check[0] is not None:
                return RecordResult(
                    strategy=self.index_strategy, status="failure",
                    error=f"duplicate primary key {pk_value}",
                    pages_accessed=dup_check[2],
                    index_nodes_visited=dup_check[3],
                )

            # Heap insert.
            page_id, slot_id, pa = heap_scan.insert_into_heap(
                self.buffer, self.page_size, meta, values,
                self.max_records_per_page,
            )

            # Index maintenance.
            nodes_visited = 0
            if self.index_strategy == "hash_index":
                nodes_visited = hash_index.insert(
                    self.buffer, self.page_size, meta, pk_value, page_id, slot_id,
                )
            elif self.index_strategy == "bplus_tree":
                tree = BPlusTree(self.buffer, self.page_size, meta)
                new_root, nodes_visited = tree.insert(
                    meta["bplus_root_page"], pk_value, page_id, slot_id,
                )
                meta["bplus_root_page"] = new_root

            meta["num_records"] += 1
            meta["num_data_pages"] = max(
                meta["num_data_pages"], self.buffer.num_pages(type_name),
            )
            self.catalog.update_type(meta)
        except RuntimeError as e:
            return RecordResult(
                strategy=self.index_strategy,
                status="failure",
                error=str(e),
            )

        return RecordResult(
            pages_accessed=pa,
            index_nodes_visited=nodes_visited,
            strategy=self.index_strategy,
        )

    # ================================================================ #
    # DML — DELETE
    # ================================================================ #
    def delete_record(self, type_name: str, pk_raw,
                      meta: Optional[dict] = None) -> RecordResult:
        if meta is None:
            meta = self.catalog.get(type_name)
        if meta is None:
            return RecordResult(
                strategy=self.index_strategy, status="failure",
                error=f"type '{type_name}' does not exist",
            )
        pk_idx = meta["primary_key_order"] - 1
        pk_type = meta["fields"][pk_idx][1]
        pk_value = self._coerce_one(pk_raw, pk_type, "primary key")
        if pk_value is None:
            return RecordResult(
                strategy=self.index_strategy, status="failure",
                error=f"invalid primary key value {pk_raw!r}",
            )

        try:
            # Locate (page, slot) using the active strategy.
            dp, ds, pa, nv = self._lookup_pointer(meta, pk_value)
        except RuntimeError as e:
            return RecordResult(
                strategy=self.index_strategy,
                status="failure",
                error=str(e),
            )
        if dp is None:
            return RecordResult(
                pages_accessed=pa, index_nodes_visited=nv,
                strategy=self.index_strategy,
                status="failure", error="not found",
            )

        try:
            # Remove from index, then from heap.
            if self.index_strategy == "hash_index":
                ok, extra_nv = hash_index.delete_key(self.buffer, meta, pk_value)
                nv += extra_nv
                if not ok:
                    return RecordResult(
                        pages_accessed=pa, index_nodes_visited=nv,
                        strategy=self.index_strategy,
                        status="failure", error="index entry missing",
                    )
            elif self.index_strategy == "bplus_tree":
                tree = BPlusTree(self.buffer, self.page_size, meta)
                ok, extra_nv = tree.delete(meta["bplus_root_page"], pk_value)
                nv += extra_nv
                if not ok:
                    return RecordResult(
                        pages_accessed=pa, index_nodes_visited=nv,
                        strategy=self.index_strategy,
                        status="failure", error="index entry missing",
                    )

            if not heap_scan.delete_at(self.buffer, meta, dp, ds):
                return RecordResult(
                    pages_accessed=pa + 1, index_nodes_visited=nv,
                    strategy=self.index_strategy,
                    status="failure", error="heap delete failed",
                )

            meta["num_records"] = max(0, meta["num_records"] - 1)
            self.catalog.update_type(meta)
        except RuntimeError as e:
            return RecordResult(
                pages_accessed=pa,
                index_nodes_visited=nv,
                strategy=self.index_strategy,
                status="failure",
                error=str(e),
            )
        return RecordResult(
            pages_accessed=pa + 1, index_nodes_visited=nv,
            strategy=self.index_strategy,
        )

    # ================================================================ #
    # DML — SEARCH (equality on primary key)
    # ================================================================ #
    def search_record(self, type_name: str, pk_raw,
                      meta: Optional[dict] = None) -> RecordResult:
        if meta is None:
            meta = self.catalog.get(type_name)
        if meta is None:
            return RecordResult(
                strategy=self.index_strategy, status="failure",
                error=f"type '{type_name}' does not exist",
            )
        pk_idx = meta["primary_key_order"] - 1
        pk_type = meta["fields"][pk_idx][1]
        pk_value = self._coerce_one(pk_raw, pk_type, "primary key")
        if pk_value is None:
            return RecordResult(
                strategy=self.index_strategy, status="failure",
                error=f"invalid primary key value {pk_raw!r}",
            )

        try:
            if self.index_strategy == "hash_index":
                return hash_index.search_by_pk(self.buffer, meta, pk_value)
            if self.index_strategy == "bplus_tree":
                tree = BPlusTree(self.buffer, self.page_size, meta)
                return tree.search_record(meta["bplus_root_page"], pk_value)
            return heap_scan.search_by_pk(self.buffer, meta, pk_value)
        except RuntimeError as e:
            return RecordResult(
                strategy=self.index_strategy,
                status="failure",
                error=str(e),
            )

    # ================================================================ #
    # DML — RANGE SEARCH
    # ================================================================ #
    def range_search(self, type_name: str, field_name: str,
                     low_raw, high_raw,
                     meta: Optional[dict] = None) -> RecordResult:
        if meta is None:
            meta = self.catalog.get(type_name)
        if meta is None:
            return RecordResult(
                strategy=self.index_strategy, status="failure",
                error=f"type '{type_name}' does not exist",
            )
        try:
            low, high = int(low_raw), int(high_raw)
        except (TypeError, ValueError):
            return RecordResult(
                strategy=self.index_strategy, status="failure",
                error="range bounds must be integers",
            )

        # Field exists and is integer?
        fnames = [n for n, _ in meta["fields"]]
        if field_name not in fnames:
            return RecordResult(
                strategy=self.index_strategy, status="failure",
                error=f"unknown field '{field_name}'",
            )
        fidx = fnames.index(field_name)
        if meta["fields"][fidx][1] != "int":
            return RecordResult(
                strategy=self.index_strategy, status="failure",
                error="range_search requires an integer field",
            )
        pk_idx = meta["primary_key_order"] - 1

        try:
            # Hash → falls back to heap_scan (per spec).
            if self.index_strategy == "hash_index":
                res = heap_scan.range_search(self.buffer, meta, field_name, low, high)
                res.strategy = "heap_scan"  # report the *actual* strategy used
                res.records = self._order_range(res.records, fidx, pk_idx)
                return res

            # B+ tree only helps if the range field is the primary key.
            if self.index_strategy == "bplus_tree" and pk_idx == fidx:
                tree = BPlusTree(self.buffer, self.page_size, meta)
                ptrs, leaves, nodes_visited = tree.range_search(
                    meta["bplus_root_page"], low, high,
                )
                records = []
                pa = 0
                for dp, ds in ptrs:
                    values, page_count = heap_scan.read_record_at(self.buffer, meta, dp, ds)
                    pa += page_count
                    if values is not None:
                        records.append(values)
                return RecordResult(
                    records=self._order_range(records, fidx, pk_idx),
                    pages_accessed=pa + leaves,
                    index_nodes_visited=nodes_visited,
                    records_scanned=len(ptrs),
                    strategy="bplus_tree",
                )

            res = heap_scan.range_search(self.buffer, meta, field_name, low, high)
            res.records = self._order_range(res.records, fidx, pk_idx)
            return res
        except RuntimeError as e:
            return RecordResult(
                strategy=self.index_strategy,
                status="failure",
                error=str(e),
            )

    # ================================================================ #
    # planner — used by EXPLAIN
    # ================================================================ #
    def estimate_io(self, op: str, type_name: str, *args) -> Tuple[str, int]:
        meta = self.catalog.get(type_name)
        if meta is None:
            return self.index_strategy, 1
        npages = max(1, meta.get("num_data_pages", 1))

        if op in ("search", "delete"):
            if self.index_strategy == "heap_scan":
                return "heap_scan", npages
            if self.index_strategy == "hash_index":
                return "hash_index", 2          # 1 bucket + 1 data page
            if self.index_strategy == "bplus_tree":
                return "bplus_tree", 3          # ~tree height + 1 data page
        if op == "range":
            if self.index_strategy == "heap_scan":
                return "heap_scan", npages
            if self.index_strategy == "hash_index":
                return "heap_scan", npages      # falls back
            if self.index_strategy == "bplus_tree":
                field_name = args[0] if args else ""
                fnames = [n for n, _ in meta["fields"]]
                pk_idx = meta["primary_key_order"] - 1
                if field_name in fnames and fnames.index(field_name) == pk_idx:
                    return "bplus_tree", 3 + max(1, npages // 2)
                return "heap_scan", npages
        if op == "insert":
            return self.index_strategy, 2
        return self.index_strategy, 1

    # ================================================================ #
    # internal helpers
    # ================================================================ #
    @staticmethod
    def _order_range(records: List, fidx: int, pk_idx: int) -> List:
        """Spec §9 ordering: non-decreasing by the searched field, ties broken
        by primary key ascending. Applies regardless of index strategy."""
        return sorted(records, key=lambda r: (r[fidx], r[pk_idx]))

    def _coerce_values(self, meta: dict, raw_values: List[str]) -> List:
        out = []
        for (fname, ftype), raw in zip(meta["fields"], raw_values):
            v = self._coerce_one(raw, ftype, fname)
            if v is None:
                raise ValueError(f"field '{fname}' expected {ftype}, got {raw!r}")
            out.append(v)
        return out

    def _coerce_one(self, raw, ftype: str, label: str):
        if ftype == "int":
            try:
                return int(raw)
            except (TypeError, ValueError):
                return None
        value = str(raw)
        if len(value.encode("ascii", "replace")) > STR_FIELD_SIZE:
            return None
        return value

    def _validate_type_definition(self, type_name: str,
                                  fields: List[Tuple[str, str]]) -> str:
        if not self._valid_identifier(type_name, TYPE_NAME_LEN):
            return f"invalid type name {type_name!r}"
        if len(fields) < MIN_FIELDS or len(fields) > MAX_FIELDS:
            return f"types must have between {MIN_FIELDS} and {MAX_FIELDS} fields"
        seen = set()
        for fname, ftype in fields:
            if not self._valid_identifier(fname, FIELD_NAME_LEN):
                return f"invalid field name {fname!r}"
            if fname in seen:
                return f"duplicate field name {fname!r}"
            seen.add(fname)
            if ftype not in ("int", "str"):
                return f"unsupported field type {ftype}"
        return ""

    def _valid_identifier(self, value: str, max_bytes: int) -> bool:
        if not isinstance(value, str) or not value:
            return False
        if len(value.encode("ascii", "replace")) > max_bytes:
            return False
        return self.buffer.valid_file_id(value)

    def _lookup_pointer(self, meta: dict, pk_value):
        """Locate (page_id, slot_id) of a pk via the active strategy.

        Returns (page_id, slot_id, pages_accessed, index_nodes_visited).
        """
        if self.index_strategy == "hash_index":
            dp, ds, nv = hash_index.lookup_pointer(self.buffer, meta, pk_value)
            return dp, ds, 0, nv
        if self.index_strategy == "bplus_tree":
            tree = BPlusTree(self.buffer, self.page_size, meta)
            dp, ds, nv = tree.search(meta["bplus_root_page"], pk_value)
            if dp < 0:
                return None, None, 0, nv
            return dp, ds, 0, nv
        # heap_scan
        dp, ds, pa = heap_scan.find_location_by_pk(self.buffer, meta, pk_value)
        return dp, ds, pa, 0
