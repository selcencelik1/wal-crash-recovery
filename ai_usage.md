# AI Usage

Mert Ozustun (2022400192) and Selcen Celik (2022400219)

We used AI on this project, mostly Claude in the terminal (Claude Code), and Gemini a
little for a second opinion on some recovery design choices. We want to be honest about
it since the spec allows it, and we can explain every file ourselves if asked.

We wrote the code ourselves and used Claude here and there when we got stuck, mostly to
explain something or help fix a part we already had a draft of. The design was ours, decided
on paper, the three phase recovery, putting the pageLSN
in the reserved bytes of the page header so we never touch a real field, the `wal.log` record
format, the `master.rec` pointing at the last complete checkpoint, the fuzzy checkpoint every
`checkpoint_interval` ops. Claude also added comments through the code while we wrote it, the
WAL invariants, where the pageLSN lives, why undo only restores the bytes that differ. We kept
the ones that were true and fixed or removed the rest, and reading them was part of how we made
sure we understand the file.

Undo was the hardest part. We log a window of the page that brackets a change, but on undo we
must restore only the bytes that actually differ, otherwise rolling back a loser's insert would
clobber a committed change sharing the same window. We asked Claude to explain it, then fixed our
own logging and undo with that understanding. The other thing we asked about was durability under
a hard `os._exit(1)` crash, which is why we flush the log on every append instead of relying on
cleanup at exit.

Debugging and testing was where the help mattered most. Our output did not match the expected
output on some test cases, and the recovered state looked off after a crash, we worked through it
with Claude and found bugs in the undo window and in which transactions we treated as losers (a
commit that crashed before its end record has to be finished, not rolled back). We tested by running
a workload on the `test_cases`, simulating the crash, re-running the engine on the same `data_dir` so
recovery runs on startup, and comparing the output and `verify.txt` against what we expected, plus
checking `wal.log` and the prev_lsn chains by hand.

In summary, the code is a mix of our work and Claude's help, done back and forth. The design and
the analysis are ours, and we used AI as a helper, not a replacement.
