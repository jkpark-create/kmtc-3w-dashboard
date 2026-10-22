# Remote query batching recovery, 2026-10-22

The Integrated producer had two independent fixed 300-document loops. The T/S
backlog loop repeated latest physical movement, discharge evidence, and onward
leg queries for each container batch. With 15,422 candidates, that means 52
batches and 156 movement-evidence statements. The equipment type repair queried
four tables by document batches: 14 Booking keys and 17,853 B/L keys required
122 statements on a cold cache. The 300 value is an implementation constant,
not a business scope rule. No source-history date cutoff was added.

The installed T/S reader selects the pinned daily EQ056I container universe
inside the ICC source predicate. It retrieves latest movement, prior loading,
and actual discharge evidence across that universe, then reads onward legs
once. It retains all-container movement history checks across bookings, ties,
B/L-first shipment identity, the original classification, and the full inventory
reconciliation. It does not pass a huge client container bind list to the
remote movement queries. Onward booking lists are split into IN expressions of
at most 900 keys inside one statement.

Equipment repair preserves legacy 300-document cache files as read-only inputs.
All uncached keys for a table are queried in one statement with bounded IN
expressions, grouped by the same document/size/type columns. Completed bulk
results are cached atomically. Missing source documents remain absent. A resume
does not re-query completed tables. Both readers log completed query duration
and row counts; T/S connection recovery remains bounded and serial.

Live verification artifacts are retained outside OneDrive under
`%LOCALAPPDATA%/KMTC/3w-dashboard-recovery/ts-query-repair-20261022/`:

- `live-query-source-set.log` and `live-ts-backlog.json`: source date 20261022;
  extraction 10:11:51 through 10:15:04 KST (193.1 seconds), zero database-link
  retries, three movement-evidence statements. All 15,627 registered containers
  reconciled to 14,979 waiting, 642 review and six excluded; waiting TEU 21,776.
- `equipment-live-equivalence.json`: four actual cold-cache queries rather than
  122. All document equipment groups and counts equal the previous 122-file
  cache after order-independent comparison. Group order is not a source key.
  The compared scope is the captured 20261021 manifest, not a current publication.
- `equipment-cache-equivalence.json`: the warm-cache path performs zero remote
  queries and preserves all prior document groups and counts.
- `legacy-sample.log` and `legacy-comparison-stop.json`: the first legacy
  movement query against 300 containers did not complete during more than six
  minutes of comparison-process runtime. Only that verified comparison process
  was stopped at 10:23 KST to release the redundant database read. Full live
  old/new sample equality was not established; no elapsed full-job speedup is
  claimed from that incomplete comparison.

Validation: 37 Python tests and six existing Node equipment-repair tests passed.
This includes source-date pinning, all-key coverage, Oracle IN expression bounds,
missing-versus-zero behavior, partial legacy cache recovery, and zero-query
resume. The installed source files are in the separate Integrated Dashboard
project, which currently has no Git metadata. The reproducible source and test
patch is `docs/recovery/2026-10-22-integrated-ts-retry.patch` in this repository.
Applying that patch to the backed-up Windows CRLF sources was checked with
`git apply --check --ignore-space-change` in an isolated temporary directory.

The 20261022 daily producer and the registered 3W consumer continue separately.
The serving source was still 20261019 at the 10:21 KST baseline verification.
Query recovery is not proof of a current production release or authenticated
main/Sales Target browser readback. Sales Target's strict unknown-CM1 gate has
not been bypassed, and no unknown source amounts were changed to zero.
