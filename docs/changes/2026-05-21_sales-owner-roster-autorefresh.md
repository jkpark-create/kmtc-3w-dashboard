# Sales Owner Roster Auto Refresh

## Background
- Sales Target & Progress owner rows were previously sourced from an older contact-point workbook.
- The authoritative source is now KMTC Global Network:
  `https://docs.google.com/spreadsheets/d/1aGn2YyvKRx35mOsHLQAMaas6sa81LTOf4pNPugIUajg/edit`
- Sales owner matching should use the Global Network `ICC ID` first, then active `salesman.csv` customer-owner mapping.

## Decision
- `scripts/update_target_workbook_from_current_customer_owners.py` now defaults to the latest output dataset, or accepts `--dataset-id YYYYMMDD --as-of YYYYMMDD`.
- `run_daily.bat` refreshes the Target Workbook owner roster from Global Network before rebuilding `dist/sales-target`.
- The daily Sales Target build therefore reads a refreshed `Summary_All` each run.

## Verification
- Dry-run completed for dataset `20260521`.
- Owner rows: 240.
- Unmatched owner rows: 44.
- Existing known corrections are preserved:
  - `Alex Wu -> WUXIAOCHEN`
  - `Leo Wang -> WHZL`
  - `William Jiang -> LJJIANG`
  - `Crystal Zhou -> ZHOUXIAORU`
  - `Papada -> PAPHADA`
  - `Winnie Sia -> WINNIESIA`
  - `Manoj Arya -> ARYA`

