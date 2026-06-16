# Sales Target & Progress 문서 묶음

이 폴더는 `https://jkpark-create.github.io/kmtc-3w-dashboard-web/sales-target/` 화면의 개발 진행, 데이터 파이프라인, 계산식, 운영 작업 단위를 분리해서 기록한다.

## 문서 구성

| 파일 | 용도 |
| --- | --- |
| `data-pipeline.md` | `daily_3w_dashboard.py` 산출물부터 `dist/sales-target/*.json` 배포까지의 데이터 흐름 |
| `calculation-spec.md` | KPI, Target, GAP, 2025 base, BSA 배분, Q3 저조구간 계산식 |
| `development-progress.md` | 현재 개발 진행 상태, 남은 커밋 후보, 검증/배포 체크리스트 |
| `skills/` | 반복 작업을 skill 단위 절차로 쪼갠 실행 가이드 |

## 현재 배포 기준

로컬 `dist/sales-target` 기준:

| 항목 | 값 |
| --- | --- |
| 화면 | `dist/sales-target/index.html` |
| 앱 로직 | `dist/sales-target/app.js` |
| Summary 데이터 | `dist/sales-target/index.json` |
| 상세 chunk manifest | `dist/sales-target/manifest.json` |
| 2025 기준선 | `dist/sales-target/base2025.json` |
| 최신 data_date | `20260615` |
| 최신 generated_at | `2026-06-15T11:16:21` |
| chunk 수 | `964` |
| BKG row 수 | `259,599` |

## 핵심 원칙

- `index.json`은 Target Workbook `Summary_All`을 기준으로 목표/분기 성과 요약을 담는다.
- `manifest.json`과 `data/*.json`은 화면에서 필요한 상세 BKG를 lazy-load 하기 위한 청크 구조다.
- `base2025.json`은 도착국가/도착포트/등급/고수익 필터가 걸릴 때 2025 기준선과 Target을 같은 기준으로 다시 자르는 보조 데이터다.
- BSA는 영업사원 원천 필드가 없으므로 2025 Normal LST 비중으로 영업사원과 화주에 배분한다. 2025 이력이 전혀 없는 신규 lane만 2026 활동량 fallback을 쓴다.
- `dist/`는 별도 GitHub Pages 저장소다. 상위 repo에는 `dist` 포인터만 남는다.

