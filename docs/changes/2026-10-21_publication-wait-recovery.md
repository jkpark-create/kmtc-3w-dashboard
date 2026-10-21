# Keep the daily consumer waiting for a verified publication

The 3W task used to stop after five minutes when Integrated held its publication
lock. A completed October 19 aggregation also failed its completion receipt when
the producer started another refresh. The consumer must read one independently
verified immutable generation instead of requiring the producer to remain idle
throughout the aggregation.

`published_source_package.py` copies only published, hash-checked source members
to the consumer runtime. The source date, manifest SHA-256, production success
receipt, published index and every member must agree before it writes a pin.
Subsequent source reads use that immutable package. A corrupt, partial or older
generation cannot satisfy the requested date.

`wait_integrated_publication.py` now honors the bounded wait in the immutable
consumer branch as well as the legacy readiness branch. It retries pending
publication and transient transport failures without holding either output
lock. Permission failures remain terminal. The installed machine-local batch
defaults to 43,200 seconds and permits an explicit shorter override. It remains
outside Git, as required by the operations contract.

Validation: ten package-integrity/readiness tests passed. Nineteen existing
Drive, salesperson detection and Q3 target checks passed against the installed
runtime and its dependency path. Drive verification matched 2,393 previously
published files by ID, size and MD5; this verifies the saved release, not today's
unsynced staging.

The stalled pre-fix producer tree rooted at PID 15068 was verified and terminated
through Windows elevation at 2026-10-21 18:05 KST. Its publication gate was then
opened exclusively and closed successfully. The Integrated registered task was
restarted, and a notification-free October 21 consumer invocation is waiting for
the new production receipt. Publication completion must be checked separately.

The Sales Target deferral was traced to two independent issues: the batch
explicitly skipped the builder, and the legacy builder invocation omitted its
required verified monthly source option. Read-only verification against the
existing October 19 snapshot confirmed all July-December monthly inputs with
no blocking reasons and no basis reconciliation pending.

The installed batch now calls `refresh_sales_target_daily.py` after the main
pipeline. It reads the latest official directory and the company-owned 2025
actual basis through `--monthly-target-from-existing-sources`, pins the exact
daily snapshot, builds into a separate directory and runs the immutable-release
verifier before activating local staging. The previous local generation is
retained. Legacy workbook mutation and the inline legacy builder remain disabled.

`publish_verified_daily_runtime.py` checks that main and Sales Target dates
match, uploads the new immutable dependencies, verifies them remotely, then
updates and reads back the Sales Target pointer. It preserves historical owner
audits and publishes today's customer-code quality checks separately. A failed
generation or verification cannot publish a new pointer. Publication resume
also checks the dated Sales Target release before skipping aggregation.

Thirty-five relevant tests passed, including seven new build/activation and
publication-order regression checks. Production publication and final browser
readback are still pending the October 21 producer generation.
