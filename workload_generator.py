"""
workload_generator.py

Produces valid input lines for archive.py to stdout. Four modes:

    sequential  insert N records, then Q full-table scans
    random      insert N records, then Q random equality searches
    range       insert N records, then Q random integer range queries
    mixed       insert N records, then Q mixed search/insert/delete ops

Type contract: exactly 6 fields, of which at least 2 are integers.
The primary key is the 1st field (an integer).
"""

import argparse
import random
import string
import sys


TYPE_NAME = "thing"
FIELDS = [
    ("id",     "int"),   # primary key
    ("name",   "str"),
    ("score",  "int"),
    ("city",   "str"),
    ("age",    "int"),
    ("note",   "str"),
]
PK_ORDER = 1


def rand_str(min_len: int = 4, max_len: int = 10) -> str:
    n = random.randint(min_len, max_len)
    return "".join(random.choices(string.ascii_letters + string.digits, k=n))


def emit_create_type():
    parts = ["create", "type", TYPE_NAME, str(len(FIELDS)), str(PK_ORDER)]
    for fn, ft in FIELDS:
        parts.append(fn)
        parts.append(ft)
    print(" ".join(parts))


def emit_create_record(pk: int) -> list:
    """Return [tokens, key] so the caller can keep track of inserted PKs."""
    values = []
    for fn, ft in FIELDS:
        if fn == "id":
            values.append(str(pk))
        elif ft == "int":
            values.append(str(random.randint(0, 100_000)))
        else:
            values.append(rand_str())
    print("create record " + TYPE_NAME + " " + " ".join(values))
    return values


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", required=True,
                    choices=["sequential", "random", "range", "mixed"])
    ap.add_argument("--records", type=int, required=True)
    ap.add_argument("--queries", type=int, required=True)
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()
    if args.records < 0:
        ap.error("--records must be non-negative")
    if args.queries < 0:
        ap.error("--queries must be non-negative")
    if args.mode == "random" and args.queries > 0 and args.records == 0:
        ap.error("--mode random requires --records > 0 when --queries > 0")
    random.seed(args.seed)

    emit_create_type()
    inserted_pks = list(range(1, args.records + 1))
    random.shuffle(inserted_pks)
    for pk in inserted_pks:
        emit_create_record(pk)

    if args.mode == "sequential":
        # The closest workload to a "full-table scan" we have is a
        # range_search across the entire primary-key space.
        for _ in range(args.queries):
            print(f"range_search {TYPE_NAME} id 1 {max(1, args.records)}")

    elif args.mode == "random":
        for _ in range(args.queries):
            pk = random.choice(inserted_pks)
            print(f"search record {TYPE_NAME} {pk}")

    elif args.mode == "range":
        for _ in range(args.queries):
            lo = random.randint(1, max(1, args.records))
            hi = random.randint(lo, max(1, args.records))
            print(f"range_search {TYPE_NAME} id {lo} {hi}")

    elif args.mode == "mixed":
        next_pk = args.records + 1
        live = list(inserted_pks)
        for _ in range(args.queries):
            r = random.random()
            if r < 0.5 and live:
                pk = random.choice(live)
                print(f"search record {TYPE_NAME} {pk}")
            elif r < 0.75:
                emit_create_record(next_pk)
                live.append(next_pk)
                next_pk += 1
            elif live:
                pk = random.choice(live)
                live.remove(pk)
                print(f"delete record {TYPE_NAME} {pk}")


if __name__ == "__main__":
    main()
