# Sales Target Pipeline / Skills Documentation

## 배경/문제

- `Sales Target & Progress` 화면은 `index.json`, `manifest.json`, `data/*.json`, `base2025.json`, Target Workbook, BSA 배분, Q3 저조구간 스크립트가 연결되어 있어 단일 `DEVELOPMENT.md`만으로 추적하기 어려웠다.
- 커밋 후보로 남아 있는 신규 Q3 운영 스크립트와 `dist/sales-target/base2025.json` 변경의 역할을 분리해 둘 필요가 있었다.
- 특히 로직과 계산식, BSA 배분, 2025 기준선 재계산, Q3 목표 산식이 나중에 검산 가능해야 했다.

## 원인/판단

- `dist/`는 별도 GitHub Pages repo라 상위 repo에서 `dist` 포인터가 dirty로 보일 수 있다.
- Q3 저조구간 관련 스크립트는 Google Sheets 쓰기 작업을 포함하므로 preview/apply 절차와 커밋 범위를 분리해야 한다.
- `base2025.json`은 단순 데이터 파일처럼 보이지만 화면 필터 연동 Target 재계산의 핵심 계약이다.

## 결정/계산 로직

- `docs/sales-target/`를 Sales Target 전용 문서 루트로 만들었다.
- `data-pipeline.md`에 데이터 원천, JSON 스키마, BSA 배분, Runtime 흐름을 정리했다.
- `calculation-spec.md`에 Booking/Lifting/High-Profit, Target/GAP, 2025 base, 화주 BSA, Q3 산식을 공식으로 정리했다.
- `skills/` 아래에 반복 작업을 실행 단위로 분리했다.
  - JSON 재생성
  - 2025 base/BSA 점검
  - Q3 저조구간 분석
  - 배포/커밋 순서

## 변경 파일

- `docs/sales-target/README.md`
- `docs/sales-target/data-pipeline.md`
- `docs/sales-target/calculation-spec.md`
- `docs/sales-target/development-progress.md`
- `docs/sales-target/skills/README.md`
- `docs/sales-target/skills/build-sales-target-json.md`
- `docs/sales-target/skills/maintain-2025-base-and-bsa.md`
- `docs/sales-target/skills/analyze-q3-laggards.md`
- `docs/sales-target/skills/publish-and-commit.md`
- `docs/changes/2026-06-16_sales-target-pipeline-skills-docs.md`
- `DEVELOPMENT.md`

## 검증 결과

- 온라인 `Sales Target & Progress` 페이지의 현재 구조를 확인했다.
- 로컬 `dist/sales-target/index.json`, `manifest.json`, `base2025.json`의 format/data_date/chunk 수를 확인했다.
- 주요 계산식은 `scripts/build_sales_target_drill_data.py`, `dist/sales-target/app.js`, Q3 관련 스크립트의 현재 로직 기준으로 문서화했다.

## 배포/커밋

- 문서 추가만 수행했다.
- 실제 Git commit/push는 수행하지 않았다.
- 권장 커밋 단위는 `docs/sales-target/development-progress.md`와 `skills/publish-and-commit.md`에 분리해 기록했다.

## 후속 확인사항

- `laggard_q3_compute.py`의 snapshot/BSA 경로가 20260610으로 고정되어 있어 반복 운영 전 최신 날짜 인자화가 필요하다.
- `dist/sales-target/base2025.json` 변경은 `dist` 내부 repo에서 먼저 커밋한 뒤 상위 repo `dist` 포인터를 커밋해야 한다.

