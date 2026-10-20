# Sales Target WOS-3 CM1 recovery

From July the Integrated source separates booking performance from physical B/L
performance. The Sales Target screen is fixed to WOS-3 booking rows, so filtering
out B/L rows made canonical `norm_cm1` zero. The builder already calculated the
original booking cohort's `wos_norm_cm1_num` but omitted it from drill JSON.

The builder now serializes `wos_norm_cm1`. Pivot, shipper totals, booking details
and the additional CSV view columns use the original WOS lifting/profit fields.
Physical B/L fields and the existing raw CSV columns retain their source values.
Legacy chunks without the new field retain the previous normal-CM1 fallback.
Valid zero values render as zero in Pivot. The prior-year target notice now
explains that only some target components lack a basis, and retains reasons in
its tooltip. Quarter links with `month=ALL` correctly clear the default month.

Recovery uses `scripts/repair_sales_target_wos_cm1.py` against a downloaded release
and the same-date source parquet. It rejects duplicate booking keys, different
source dates, different shipper/POL/POD lineage, and different WOS normal lifting.
It repairs only modern WOS-3 booking rows, writes new immutable chunks and metadata,
and leaves the old release intact. Publication must upload all dependencies,
verify remote sizes/checksums, then update and read back the release pointer.

The 2026-10-12 release recovery covers 97,511 booking rows in 780 chunks, retaining
the 2026-10-20 roster update and every target. CN_SHA / Q3 / all months / C+D gives
CM1 8,011,798 and CM1/TEU 1,530. Q2 remains 5,219,116 on the current source release;
July is 1,198,535. Browser checks include Pivot metric changes and cell drilldown.

Freshness is a separate issue: the machine-local daily batch deliberately has
`SKIP_SALES_TARGET_BUILD=1`, and the latest daily follow-up exited with code 3
while waiting for the Integrated publication gate. The 2026-10-19 pipeline also
stopped before publication with code 80 when that gate was unavailable. This
CM1 recovery retains the truthful 2026-10-12 actuals date. It does not certify a
current-date daily refresh or remove the verified-monthly-target source guard.

Validation: Python compilation; 18 tests covering Drive runtime guards, new-owner
detection, Q3 target preservation and source-bound CM1 recovery; existing JS target
tests; new JS WOS profit/lifting and canonical B/L separation tests; rendered
desktop verification against downloaded Drive release data. Runtime JSON remains
on Drive and in LocalAppData, outside Git.
