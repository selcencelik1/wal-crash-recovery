# CmpE321 - Project 3 - Modular DBMS Engine

## Contributors
  - Mert Ozustun, 2022400192
  - Selcen Celik, 2022400219

## Overview

| Layer | Module | Responsibility |
|-------|--------|----------------|
| 1 | `disk_space_manager/` | Raw file and page I/O |
| 2 | `buffer_manager/` | Page pool, LRU/MRU eviction, dirty writeback |
| 3 | `file_index_manager/` | Records, relations, system catalog, indexes |
| 4 | `query_processor/` | Command parsing, output / stats / log |

## How to Run

```bash
python3 archive.py config.json input.txt
```

`config.json` configures the run; `input.txt` holds one command per line.
Outputs are written next to `archive.py`:

- `output.txt` — query results (overwritten each run)
- `stats_output.txt` — statistics snapshot, written on each `stats` command
- `log.csv` — append-only audit log (survives restarts)

Start each measured run from a clean state:

```bash
rm -f ./*.bin output.txt stats_output.txt
```

## Configuration (`config.json`)

```json
{
  "page_size": 4096,
  "max_records_per_page": 10,
  "buffer_pool_size": 128,
  "replacement_policy": "LRU",
  "index_strategy": "bplus_tree"
}
```

- `replacement_policy`: `LRU` or `MRU`
- `index_strategy`: `heap_scan`, `hash_index`, or `bplus_tree`


## Repository Layout

```
archive.py              entry point (builds the four layers, runs input)
config.json             run configuration
input.txt               sample command script
record.txt              experiment commands and run instructions
workload_generator.py   sequential/random workload generator
models.py               inter-layer Result dataclasses
disk_space_manager/     Layer 1
buffer_manager/         Layer 2
file_index_manager/     Layer 3
query_processor/         Layer 4
report.tex / report.pdf project report
ai_usage.md             AI usage disclosure
```
