# 2026-04-23 대시보드 도착지 기준 J열 전환

## 배경/문제

- Booking Snapshot의 AD열 `D_group`은 기존 도착지 그룹핑 컬럼인데, 국가 단위 도착지 표시를 위해 이 컬럼 값을 바꾸면 산출물 호환성과 기존 해석이 깨질 수 있다.

## 원인/판단

- 대시보드 도착지 `dest`가 필요한 것은 국가 코드 기준이며, 원천 컬럼은 J열 `DLY_CTR_CD`다.
- AD열 `D_group`은 `MY/SG`, `AE` 그룹핑 의미를 유지하는 편이 맞다.

## 결정/계산 로직

- `D_group`은 기존 그룹핑 로직으로 복원한다.
- 대시보드 JSON의 `dest`는 매번 `DLY_CTR_CD`에서 재생성한다.
- 캐시나 중간 산출물에 `dest`가 이미 있어도 `DLY_CTR_CD`를 우선해 덮어쓴다.

## 변경 파일

- `daily_3w_dashboard.py`
- `DEVELOPMENT.md`
- `docs/changes/2026-04-23_multi-select-country-destination.md`
- `docs/changes/2026-04-23_dashboard-dest-source-dly-ctr.md`

## 검증 결과

- `.venv/bin/python -m py_compile daily_3w_dashboard.py` 통과
- `output/booking_snapshot_result_20260423.csv`와 `output/_cache_20260423.parquet`의 `D_group`을 J열 `DLY_CTR_CD` 기준 그룹핑 값으로 재계산
- `D_group` 검증 결과 불일치 0건
- `SKIP_GDRIVE_UPLOAD=1`로 `dashboard_summary_20260423.json` 및 `dist/data.json` 재생성
- `dist/data.json`에서 `dest:"MY/SG"` 0건, `dest:"MY"`/`dest:"SG"` 등 원본 국가 코드 유지 확인

## 배포/커밋

- 미배포

## 후속 확인사항

- 운영 환경 업로드가 필요하면 `SKIP_GDRIVE_UPLOAD` 없이 정기 실행 또는 업로드 단계를 실행한다.
