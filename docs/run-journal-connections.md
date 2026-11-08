# RunJournal connection lifetime

The native `RunContext` opens a `RunJournal.connection_scope()` after claiming
the attempt. It closes the scope before releasing the runtime lease, including
entry failure, cancellation and exceptional exit. Native iterators retain the
same context through callback thread switches and drain before the scope exits.
Short control operations and independent observers still close their connections
after each operation unless their caller explicitly owns a connection scope.

The scope holds at most two connections: one writer and one read-only reader.
Each has a separate reentrant lock. This separation is necessary because Lance
can request Arrow batches on a callback thread while the publishing thread holds
`execution_guard`; that callback must still be able to poll cancellation. Locks
serialize access to each connection, with the existing one-second write and
five-second read timeout also bounding Python connection-lock waits. A journal
inherited across `fork` rejects access before acquiring an inherited lock; the
child must construct a fresh journal. Connections are never inherited as owners.

The writer initializes schema and connection settings once when it is opened.
Every operation still runs its own `BEGIN IMMEDIATE` and commit or rollback.
Every ownership/attempt check and every cumulative budget reservation remains
inside the same transaction. Publication keeps its existing fencing transaction
across sink inspection, publication and receipt recording. Connection reuse does
not batch acknowledged facts, reduce sync frequency or change the journal mode.
`synchronous=FULL` remains in force. Failed operations evict and close their
connection; a later operation opens and initializes a fresh handle.

Readers use `mode=ro` on a separate connection, never the uncommitted writer
connection. Multi-query execution-commit reads use one explicit snapshot for
status, preparation and receipts. Whole-history reads reuse that same snapshot
instead of opening one connection per commit. Every snapshot ends before the
connection returns to the scope, so a later control poll sees later committed
cancellation or ownership state. No business/control value is cached in Python.
Independent read methods do not initialize schema, claim a run or repair data.
The legacy `inspect()` method retains its historical schema initialization;
use `execution_view()` for independent observation.

The additional retained resources are bounded by two handles, two 2 MiB SQLite
page-cache targets and 64 cached statements per connection. These are SQLite
cache settings, not a total RSS guarantee. Existing row/document/cardinality,
scratch, output, control-store and executor RSS limits still apply. There is no
new queue, thread, background writer or second recovery authority. A slow sink
still holds the required writer fence; resource and cancellation readers remain
independent. The underlying filesystem must support SQLite's existing locking
and synchronization requirements. This change does not enable WAL or certify
host power-loss recovery on a particular filesystem.

Tests cover thread callbacks under the publication fence, exact concurrent
budget reservations, fresh cancellation/attempt reads, rollback after SQLite
capacity failure, connection cleanup and fork rejection. The task evidence also
contains alternating baseline/candidate benchmarks, process-loss windows,
syscall failure injection and fixed HTTP/image replay. Real provider quality and
physical power-loss behavior are outside those tests.

References: [Python connection thread and transaction behavior](https://docs.python.org/3.11/library/sqlite3.html),
[SQLite isolation](https://sqlite.org/isolation.html), and
[SQLite synchronization settings](https://sqlite.org/pragma.html#pragma_synchronous).
