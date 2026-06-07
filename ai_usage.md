# AI Usage

Mert Ozustun (2022400192) and Selcen Celik (2022400219)

We used AI on this project, mostly Claude in the terminal (Claude Code). We
also used Gemini a little, mainly to talk through some of our design
decisions and get a second opinion before we committed to them. We want to
be honest about it since the spec says honest disclosure is fine, and anyway
we can explain every file ourselves if asked.

One thing Claude did that helped us a lot was add comments through the code
while we were writing it. The header layout comment in `page.py`, the notes
on why allocate does not zero fill, the catalog entry breakdown, things like
that. We kept the comments that were actually true for our design and fixed
or removed the ones that were not, and going over them was part of how we
made sure we understand every file we are handing in.

We did the design on paper first. The four layer split, the 32 byte header,
the 4 byte int and 32 byte string, the 380 byte catalog entry, the 50 key
fanout cap, all of that was our decision. The code itself we wrote together
with Claude. It was back and forth: we would say what a function had to do
and the format we wanted, Claude would help write or fix a part, we would
read it, run it, and adjust it until we were happy. So the implementation is
a mix of our work and Claude's help, not one or the other. Where the help
mattered most was catching the dumb mistakes with the `struct` format
strings and the slot offset math, which is the kind of thing where it is
easy to be off by one.

The B+ tree was the hardest part for us. We understood the idea but kept
getting the node split wrong, especially the difference between a leaf split
(you copy the separator key up) and an internal split (you push it up and it
leaves that node). We asked Claude to explain that clearly and then
fixed our own split functions with that understanding. The lazy delete was
our call, we just checked it was allowed by the spec. The hash index and the
overflow chaining we mostly wrote ourselves since it is simpler.

For the buffer manager we asked how to get O(1) LRU and MRU out of an
`OrderedDict`, since we were not sure popitem worked from both ends. After
that the eviction, the dirty map and the deferred write back were ours.

The most useful thing was debugging and testing. Our disk I/O numbers
looked wrong for a while, reads were always glued to writes no matter the
workload. We worked through it with Claude and found we were reading the old
page before every write, which counted a fake read each time. We removed
that so a write is just a write now. We had a few bugs like this where the
behaviour looked right but a counter was off, and talking it through with
Claude helped us find them faster than staring at it alone. For testing we
ran the engine by hand on different configs and workloads, checked the
output and the stats against what we expected, tried the spec sample and the
edge cases (duplicate keys, delete then search, a bad range field,
persistence after a restart), and when something failed we used Claude to
help track down why before fixing it ourselves.

The experiments we ran by hand with the commands in `record.txt`, and the
numbers in the report are the real output from those runs, not made up.
Claude helped with the LaTeX tables and tightened some sentences, but the
reasoning in the analysis (why MRU wins under sequential flooding, why a
wide range query does not really help the B+ tree) is ours and written in
our own words.

We did not paste anything in without reading it. We dropped suggestions we
did not need, picked all the sizes ourselves, and rewrote a lot of the
report so it sounds like us. Short version: the code is a mix of our work
and Claude's help, done together back and forth. The design and the analysis
are ours. We can walk through any file in this submission and explain why it
does what it does.
