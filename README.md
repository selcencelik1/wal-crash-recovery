# CmpE321 - Project 4 - WAL Crash Recovery
https://github.com/selcencelik1/wal-crash-recovery.git
## Contributors
  - Mert Ozustun, 2022400192
  - Selcen Celik, 2022400219

## Overview

A modular DBMS engine with write-ahead logging and ARIES-style crash
recovery (Analysis, Redo, Undo).

| Layer | Module | Responsibility |
|-------|--------|----------------|
| 1 | `disk_space_manager/` | Raw file and page I/O |
| 2 | `buffer_manager/` | Page pool, LRU/MRU eviction, dirty writeback |
| 3 | `file_index_manager/` | Records, relations, system catalog, indexes |
| 4 | `query_processor/` | Command parsing, output / stats / log |
| — | `recovery_manager/` | WAL logging, checkpoints, crash recovery |

## How to Run

```bash
python3 archive.py <config.json> <input_file>
```

`config.json` configures the run; the input file holds one command per line.
Recovery runs automatically on startup against whatever is on disk, so a
crash run is replayed by re-running the engine on the same `data_dir`:

```bash
python3 archive.py test_cases/case_1/config.json test_cases/case_1/input_a.txt
.
.
.
python3 archive.py test_cases/case_1/config.json test_cases/case_1/verify.txt
```

Outputs are written next to `archive.py`:

- `output.txt` — query results (overwritten each run)
- `stats_output.txt` — statistics snapshot, written on each `stats` command
- `log.csv` — append-only audit log (survives restarts)

Storage files (`*.bin`, `wal.log`, `master.rec`) are written to `data_dir`
(default: `data/`). Start a measured run from a clean state:

```bash
rm -rf data output.txt stats_output.txt
```

## Configuration (`config.json`)

```json
{
  "page_size": 4096,
  "max_records_per_page": 10,
  "buffer_pool_size": 16,
  "replacement_policy": "LRU",
  "index_strategy": "bplus_tree",
  "checkpoint_interval": 50,
  "log_buffer_size": 8,
  "data_dir": "data"
}
```

- `replacement_policy`: `LRU` or `MRU`
- `index_strategy`: `heap_scan`, `hash_index`, or `bplus_tree`


## Repository Layout

```
archive.py              entry point (builds the layers, runs recovery + input)
config.json             run configuration
models.py               inter-layer Result dataclasses
run.txt                 example run commands
disk_space_manager/     Layer 1
buffer_manager/         Layer 2
file_index_manager/     Layer 3
query_processor/        Layer 4
recovery_manager/       WAL + crash recovery
test_cases/             crash-recovery test cases
```
