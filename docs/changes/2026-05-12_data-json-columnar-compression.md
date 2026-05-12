# 2026-05-12 data.json columnar compression

## 배경/문제

- 일일 배포에서 `dist/data.json`이 105.8MB까지 증가해 GitHub의 단일 파일 100MB 제한으로 push가 실패했다.

## 원인/판단

- `monthly`, `weekly`, `shipper`, `bsa` 레코드를 객체 배열로 저장하면서 동일한 필드명이 수십만 행에 반복되었다.
- 특히 `shipper`와 `bsa` 섹션의 반복 필드명이 파일 크기 증가의 주 원인이었다.

## 결정/계산 로직

- JSON을 `columns-v1` 포맷으로 저장한다.
- 각 섹션은 컬럼 목록 `c`와 행 배열 `r`로 구성하고, 행 끝의 빈 값은 잘라 저장한다.
- 웹 화면은 로딩 직후 `columns-v1` 데이터를 기존 객체 배열 형태로 복원해 기존 집계/필터 로직을 그대로 사용한다.
- 생성된 JSON이 95MB 이상이면 파이프라인을 중단해 GitHub push 실패 커밋이 생기지 않도록 한다.

## 변경 파일

- `daily_3w_dashboard.py`
- `dist/index.html`
- `run_daily.bat`
- `DEVELOPMENT.md`

## 검증 결과

- 2026-05-12 기준 `dist/data.json`: 105,772,883 bytes -> 47,375,497 bytes
- `dist/data.json`의 `monthly`, `weekly`, `shipper`, `bsa` 행 수가 기존과 동일하게 복원되는 것을 확인했다.

## 배포/커밋

- push 실패로 남아 있던 `dist`의 미배포 로컬 커밋은 큰 blob을 포함하지 않도록 재정리해야 한다.

## 후속 확인사항

- 데이터 증가 추세가 계속되면 섹션/월별 파일 분리 또는 문자열 dictionary encoding을 추가 검토한다.
