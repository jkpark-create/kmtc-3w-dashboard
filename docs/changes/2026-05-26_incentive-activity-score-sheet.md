# 2026-05-26 Incentive Activity Score Sheet Update

## Background

The 2026 Q1 incentive evaluation workbook needed one additional activity metric:
`출항 3주 전 고수익 비중`.

After the first workbook update, some country tabs had formulas in header or spacer
rows where item labels or blank cells were expected. The new metric columns also did
not consistently inherit the same formatting as the existing activity metric columns.

## Decision And Logic

- Activity items are now:
  - `출항 3주 전 부킹률 증대 (BSA 대비)` / 10 points
  - `출항 3주 전 부킹의 실선적률` / 5 points
  - `출항 3주 전 고수익 비중` / 5 points
- Performance items remain:
  - `CM1달성률` / 20 points
  - `물량 달성률` / 20 points
  - `고수익 달성률` / 10 points
  - `SME달성률` / 10 points
- Country-tab totals use the 80-point raw score converted to a 100-point basis.
- Team/summary tabs import the added metric and use the same 80-to-100 conversion.

## Changed Files And Sheets

- Google Sheets workbook:
  - `2026년 1분기 인센티브 평가(취합용)`
  - All country tabs from `JP` through `MY`
  - `점수표 (취합)`
  - `입력 가이드`
- Google Sheets report workbook:
  - `2026년 1분기 인센티브 결과 보고서`
  - `점수표 (취합)`
  - `활동실적비중`

## Verification

- Confirmed on `JP` that monthly blocks show the new `출항 3주 전 고수익 비중 / 점수`
  headers, spacer rows are blank, and data-row formulas remain only where needed.
- Confirmed `JP!F4:I5` formatting parity between the existing activity metric and
  the new metric columns.
- Confirmed on `XMN` that the same header, blank spacer, and data-formula pattern is
  applied after the final country-tab batch update.

## Deployment And Commit

- Google Sheets changes were applied directly to the live shared workbooks.
- Git commit records the workbook change rationale and verification trail.

## Follow Up

- Recheck visually in the browser after a sheet refresh if Google Sheets still shows
  stale formatting from the local browser cache.
