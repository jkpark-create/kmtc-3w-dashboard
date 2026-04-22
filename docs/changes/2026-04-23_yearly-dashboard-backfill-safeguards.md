# 2026-04-23 연간 대시보드 백필 안전장치

## 배경/문제

- 연간/과거 데이터셋을 생성할 때 View 2 실제 선적 데이터가 대상 연도와 맞지 않으면 빈 데이터나 잘못된 실선적 지표가 생성될 수 있었다.
- Tableau 임시 워크북 publish 후 대기 시간이 고정되어 있어 환경별 조정이 어려웠다.
- 연간 백필용 원천 CSV와 Google Docs 변환 HTML 같은 로컬 산출물이 Git 추적 대상으로 보일 수 있었다.

## 원인/판단

- 연간 데이터셋에서는 다운로드/캐시/필터 조건이 최신 일일 데이터보다 더 쉽게 어긋날 수 있으므로, 생성 직전에 대상 연도 데이터 존재 여부를 확인하는 방어 로직이 필요하다.
- `1_YYYY.csv`, `2_YYYY.csv`, `generated_docs/`는 재생성 가능한 로컬 파일이므로 저장소에는 포함하지 않는 것이 맞다.

## 결정/계산 로직

- View 2 `Date_vsl`에 대상 `DATASET_YEAR`가 없으면 연간 대시보드 생성을 중단한다.
- 연간 데이터셋에서 집계 대상 booking row가 없으면 summary JSON 생성을 중단한다.
- Tableau publish 대기 시간은 `TABLEAU_PUBLISH_WAIT_SECONDS` 환경변수로 조정 가능하게 했다.
- View 2 날짜 필터 컬럼 식별자를 현재 Tableau workbook 기준으로 갱신했다.
- `1_*.csv`, `2_*.csv`, `generated_docs/`를 `.gitignore`에 추가했다.

## 변경 파일

- `daily_3w_dashboard.py`
- `.gitignore`
- `docs/changes/2026-04-23_yearly-dashboard-backfill-safeguards.md`

## 검증 결과

- `python -m py_compile daily_3w_dashboard.py` 통과

## 배포/커밋

- Main repo 반영 예정

## 후속 확인사항

- 다음 연간 백필 실행 시 `DASHBOARD_YEAR`, `DASHBOARD_DATASET_ID`, `DASHBOARD_INPUT_SUFFIX` 조합과 View 2 대상 연도 검사를 로그에서 확인한다.
