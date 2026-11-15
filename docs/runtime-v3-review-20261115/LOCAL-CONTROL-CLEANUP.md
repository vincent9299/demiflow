# Local control cleanup follows original physical exit proof

A full 98-entry / 168-case API execution passed every business assertion on
`202cfe4`, but fixture teardown failed: baseline 59 FDs, final 75. The original
`+8` assertion remains unchanged. Evidence: `/tmp/demiflow-api-final-202cfe4.xml`
(69 passed, one teardown error), with raw and formal JSON beside it.

A subsequent real API probe counted `/proc/self/fd` after each case. Failures
whose exception tracebacks retained the driver kept control sockets alive:
prepend rejection +4, receipt SHA rejection +2, wiki row rejection +3. Native
stream write also retained 3 until unrelated garbage collection. Final probe
was 25 FDs from 9 before cases; the sockets were real original Local connector
endpoints, not a second journal or fixture SQLite pool. The probe additionally
replaced a helper module function for observation, making its SQL-source code
pin invalid; those two probe failures are not evidence of a SQL product defect.

`LocalResourceProvider.ensure_absent` now closes only that allocation's original
control connector in its two existing confirmed-exit branches. Resource release
still requires the same original physical evidence. Unconfirmed exit neither
closes the connector nor refunds resources. A driver socket close error is
logged and cannot erase already-confirmed physical exit or mask the original
business failure. Ray already performed equivalent control closure.

Red: `/tmp/demiflow-control-red.xml` (three failures, unconfirmed-exit guard
passed). Green: `/tmp/demiflow-control-green.xml` (five passed, 28.36 s). Tests
retain exceptions and execution objects strongly through three real failures,
observe connector cleanup before each new finite phase, and run the real native
stream terminal. No garbage collection, threshold increase or test skip is used.
