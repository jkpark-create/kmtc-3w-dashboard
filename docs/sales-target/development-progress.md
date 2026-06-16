# Sales Target 개발 진행 및 커밋 정리

## 1. 현재 상태

확인일: 2026-06-16

온라인 화면:

- `https://jkpark-create.github.io/kmtc-3w-dashboard-web/sales-target/`
- 화면 제목: `Sales Target & Progress`
- 상단 KPI: 3W Before Booking Rate, Actual Lifting Rate, High-Profit Rate
- 주요 view: Target 요약, Drill, Pivot

로컬 배포 데이터:

- `dist/sales-target/index.json`: `data_date=20260615`, `rows=205`
- `dist/sales-target/manifest.json`: `data_date=20260615`, `chunk_count=964`, `bkg_rows=259599`
- `dist/sales-target/base2025.json`: 33개 tab, 약 3.8MB

Git 상태:

- 상위 repo branch: `codex/obt-exception-monitor-guide-auth`
- 상위 repo에서 `dist` 포인터가 dirty 상태로 보임
- `dist/` 내부 repo branch: `master`
- `dist/` 내부 변경: `sales-target/base2025.json`
- 상위 repo 신규 스크립트:
  - `scripts/fill_hwaju.py`
  - `scripts/improve_targets_tab.py`
  - `scripts/laggard_q3_fix_salesman.py`
  - `scripts/laggard_q3_rebuild_filtered.py`
  - `scripts/lifting_landscape.py`
  - `scripts/nsa_hp_compute.py`
  - `scripts/rebuild_block3.py`
  - `scripts/set_q3_targets.py`

## 2. 개발 진행 요약

### 2.1 Sales Target 화면 본체

완료된 구조:

- `index.json`으로 선적지/영업사원별 Target 요약을 즉시 렌더링
- `manifest.json`으로 월/영업사원별 상세 chunk를 lazy-load
- 도착국가/도착포트/등급/고수익 필터가 걸리면 상세 chunk와 `base2025.json`을 사용해 실제 기준으로 Target/Perform/GAP 재계산
- Drill 화면에서 화주별 BSA 목표를 2025 화주 Normal LST 비중으로 배분
- 이탈화주를 0-활동 행으로 표시하여 "목표는 있는데 2026 활동이 없는 화주"를 노출
- Pivot에서 BKG_NO 단위 자유 집계 및 CSV export 제공

핵심 구현 파일:

- `scripts/build_sales_target_drill_data.py`
- `dist/sales-target/app.js`
- `dist/sales-target/index.html`
- `dist/sales-target/guide.html`

### 2.2 2025 기준선 / BSA 배분 보강

완료된 로직:

- 2025 Normal LST 기준으로 route BSA를 영업사원에게 배분
- 도착/등급/고수익 필터에 맞춰 2025 base를 다시 잘라 Target을 `filtered_base + delta`로 재계산
- 2025 화주별 Normal LST로 영업사원 route BSA를 화주에게 재배분
- 2025 이력이 없는 신규 lane은 2026 FST 활동량 fallback으로 BSA가 사라지지 않도록 보정

현재 `dist/sales-target/base2025.json` 변경은 이 기준선/배분 데이터 갱신에 해당한다.

### 2.3 Q3 저조구간/영업사원 분석

현재 신규 스크립트들은 Google Sheets의 `①②③ 저조구간·영업사원` 및 `개선대상 화주` 탭을 만들거나 보정하기 위한 운영 도구다.

| 스크립트 | 역할 |
| --- | --- |
| `laggard_q3_compute.py` | 원본 저조구간 후보 데이터 생성. `output/laggard_q3_data.json` 작성 |
| `laggard_q3_build_sheet.py` | 저조구간 분석용 Google Sheet 최초 생성 |
| `laggard_q3_fix_salesman.py` | 영업사원별 목표/실적/GAP을 segment-sliced 기준으로 보정 |
| `laggard_q3_rebuild_filtered.py` | negative GAP 중심으로 ①②③ 블록 재구성, 주간BSA 컬럼 추가 |
| `nsa_hp_compute.py` | 중국발 -> NSA 고수익비중 블록 산출 |
| `lifting_landscape.py` | 실선적률 저조 후보 구간 랭킹 |
| `rebuild_block3.py` | ③ 실선적률 블록만 새 저조구간으로 교체 |
| `fill_hwaju.py` | 전체화주수/3주전화주수 컬럼 보강 |
| `set_q3_targets.py` | 3분기 목표 열 산정 및 입력 |
| `improve_targets_tab.py` | 개선대상 화주 탭 생성/갱신 |

정리된 공식은 `calculation-spec.md`와 `skills/analyze-q3-laggards.md`에 있다.

## 3. 커밋 후보 분리

### Commit A: 문서/skills 정리

목적: 이번 요청의 문서화 작업.

포함 후보:

```text
docs/sales-target/README.md
docs/sales-target/data-pipeline.md
docs/sales-target/calculation-spec.md
docs/sales-target/development-progress.md
docs/sales-target/skills/*.md
docs/changes/2026-06-16_sales-target-pipeline-skills-docs.md
DEVELOPMENT.md
```

권장 메시지:

```text
Document sales target pipeline and Q3 target workflows
```

### Commit B: Q3 저조구간 운영 스크립트

목적: Google Sheets 보정/개선대상 화주 추출 스크립트 보관.

포함 후보:

```text
scripts/fill_hwaju.py
scripts/improve_targets_tab.py
scripts/laggard_q3_fix_salesman.py
scripts/laggard_q3_rebuild_filtered.py
scripts/lifting_landscape.py
scripts/nsa_hp_compute.py
scripts/rebuild_block3.py
scripts/set_q3_targets.py
```

권장 메시지:

```text
Add Q3 laggard sales target maintenance scripts
```

커밋 전 확인:

```bash
python scripts/lifting_landscape.py
python scripts/nsa_hp_compute.py
python scripts/rebuild_block3.py
python scripts/set_q3_targets.py
python scripts/improve_targets_tab.py
```

Google Sheets에 쓰는 작업은 `--apply` 없이 preview를 먼저 확인한다.

### Commit C: dist 배포 데이터

목적: GitHub Pages용 `sales-target` 데이터 갱신.

`dist/` 내부 repo에서 먼저 처리한다.

```bash
git -C dist status --short
git -C dist add sales-target/base2025.json
git -C dist commit -m "Refresh sales target 2025 base"
git -C dist push
```

그 다음 상위 repo에서 `dist` 포인터를 커밋한다.

```bash
git add dist
git commit -m "Update dist pointer for sales target 2025 base"
```

주의:

- `dist` 내부 commit 없이 상위 repo의 `dist` 포인터만 커밋하면 `-dirty` 상태가 남는다.
- 대용량 원천 파일(`output/*.csv`, `output/*.parquet`, `output/*.xlsx`)은 일반적으로 커밋하지 않는다.

## 4. 검증 체크리스트

### 4.1 JSON 생성 검증

```bash
python scripts/build_sales_target_drill_data.py --snapshot output/booking_snapshot_result_20260615.csv --as-of 20260615
node -e "for (const f of ['dist/sales-target/index.json','dist/sales-target/manifest.json','dist/sales-target/base2025.json']) { const j=require('./'+f); console.log(f, j._format, j.data_date); }"
```

확인 항목:

- `index.json._format == sales-target-index-v1`
- `manifest.json._format == sales-target-manifest-v1`
- `base2025.json._format == sales-target-base2025-v1`
- `manifest.chunk_count > 0`
- `manifest.bkg_rows > 0`

### 4.2 화면 검증

권장:

```bash
python -m http.server 8080 -d dist
```

브라우저:

```text
http://localhost:8080/sales-target/
```

확인 항목:

- 로그인 gate 이후 화면 로딩
- KPI 카드 Target/Perform/GAP 표시
- 도착국가/도착포트 필터 후 Target 요약이 재계산됨
- Drill에서 화주 BSA 합계가 route allocated BSA와 맞음
- Pivot 셀 클릭 시 BKG_NO 상세가 표시됨

### 4.3 Q3 스크립트 검증

Preview:

```bash
python scripts/laggard_q3_fix_salesman.py
python scripts/laggard_q3_rebuild_filtered.py
python scripts/fill_hwaju.py
python scripts/set_q3_targets.py
python scripts/improve_targets_tab.py
```

쓰기:

```bash
python scripts/laggard_q3_rebuild_filtered.py --apply
python scripts/fill_hwaju.py --apply
python scripts/set_q3_targets.py --apply
python scripts/improve_targets_tab.py --apply
```

쓰기 전에는 preview 출력의 목표/실적/GAP 단위가 `calculation-spec.md`와 일치하는지 확인한다.

## 5. 남은 확인사항

- `laggard_q3_compute.py`는 `SNAP/BSA`가 20260610으로 고정되어 있다. 최신 재실행이 필요하면 20260615 또는 인자화가 필요하다.
- 신규 Q3 스크립트들은 Google Sheets ID를 코드 상수로 갖는다. 장기 운영용이면 `.env` 또는 config 파일로 분리하는 편이 좋다.
- Q3 ③ 실선적률은 최종적으로 `WOS-3 LST / WOS-3 FST`로 정리되어 있으나, 과거 후보 추출 파일에는 `WOS-3 LST / BSA`도 남아 있다. 후속 작업에서 문구/컬럼명을 계속 같은 기준으로 맞춰야 한다.
- `dist/sales-target/base2025.json` 변경은 내부 `dist` repo에 먼저 커밋해야 상위 repo `dist` 포인터가 clean하게 정리된다.

