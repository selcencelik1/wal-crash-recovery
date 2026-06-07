import sys, json

from disk_space_manager import DiskSpaceManager
from buffer_manager import BufferManager
from file_index_manager import FileIndexManager
from query_processor import QueryProcessor
from recovery_manager import RecoveryManager


def main():
    config_path = sys.argv[1]
    input_path  = sys.argv[2]
    with open(config_path) as cf:
        config = json.load(cf)

    # Build layers bottom-up. Each layer receives the one below it; the
    # RecoveryManager is a cross-cutting concern wired into several of them.
    disk = DiskSpaceManager(config)

    # Constructing the RecoveryManager runs the three-phase ARIES recovery
    # (Analysis -> Redo -> Undo) against whatever is on disk, BEFORE any input
    # is processed. It talks to the disk directly, bypassing the buffer pool.
    recovery = RecoveryManager(config, disk)

    buffer   = BufferManager(config, disk, recovery)
    recovery.buffer = buffer
    file_idx = FileIndexManager(config, buffer)
    qp       = QueryProcessor(config, file_idx, buffer, disk, recovery)

    with open(input_path) as f:
        for line in f:
            line = line.strip()
            if line:
                qp.process(line)

    # Normal shutdown: persist dirty pages (each obeys WAL #1 on the way out)
    # and the tail of the log buffer.
    buffer.flush()
    recovery.flush_log()


if __name__ == "__main__":
    main()
