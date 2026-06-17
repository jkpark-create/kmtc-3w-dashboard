# Sales Target 데이터 파이프라인

## 1. 전체 흐름

```text
Tableau Booking Snapshot / BSA
  -> daily_3w_dashboard.py
  -> output/booking_snapshot_result_YYYYMMDD.csv
  -> output/BSA_raw_monthly3W_YYYYMMDD.csv
  -> Target Workbook / Summary_All
  -> scripts/build_sales_target_drill_data.py
  -> dist/sales-target/index.json
  -> dist/sales-target/manifest.json
  -> dist/sales-target/data/*.json
  -> dist/sales-target/base2025.json
  -> GitHub Pages /sales-target/
```

`Sales Target & Progress`는 메인 `-3W Booking Dashboard`의 일일 부킹 스냅샷을 재사용한다. 차이는 Target Workbook의 목표율과 2025 기준선을 결합해, 선적지 -> 영업사원 -> 화주 -> BKG_NO 단계로 목표 대비 실적을 보여준다는 점이다.

## 2. 입력 데이터

### 2.1 Booking Snapshot

기본 입력은 `output/booking_snapshot_result_YYYYMMDD.csv`이다. 최신 파일은 `scripts/build_sales_target_drill_data.py`가 자동 선택하거나 `--snapshot`으로 지정한다.

주요 필드:

| 필드 | 용도 |
| --- | --- |
| `BKG_NO` | 상세 Drill / Pivot의 최소 식별자 |
| `BKG_SHPR_CST_NO`, `BKG_SHPR_CST_ENM` | 화주 기준 집계 |
| `POR_CTR_CD`, `POR_PLC_CD` | 선적국가/선적포트 및 target tab 매핑 |
| `POD_CTR_CD`, `POD_PORT_CD` | 화면의 도착국가/도착포트 필터 |
| `DLY_CTR_CD`, `DLY_PLC_CD` | BSA 배분 및 구간 계산 기준 |
| `FST_TEU`, `LST_TEU`, `CM1` | 부킹/실선적/수익성 지표 |
| `LST_Status` | Normal 실선적 여부 |
| `YYYYMM` | Actual departure 기준 445 calendar 월 |
| `Lead_time (BKG_Sche)` | WOS 단계, 특히 WOS-3 판정 |
| `Salesman_POR` | 영업사원. 필요 시 `salesman.csv` 현재 owner로 remap |
| `grade`, `고/저`, `고수익태그` | 등급/구간별 고수익/화주 고수익 필터 |

### 2.2 BSA CSV

`output/BSA_raw_monthly3W_YYYYMMDD.csv`를 읽고 아래 컬럼으로 표준화한다.

| 원천 컬럼 | 표준 컬럼 | 용도 |
| --- | --- | --- |
| `POR_Country` | `origin` | 선적국가 |
| `POR_PORT` | `ori_port` | 선적포트 |
| `DLY_Country` | `dest` | 도착국가 |
| `DLY_PORT` | `dst_port` | 도착포트 |
| `TEU_BSA (Actual)` | `route_bsa` | route 단위 BSA |
| `team` 또는 `Sales Team` | `team` | OBT 필터 |

Sales Target 화면은 `team == OBT`, `route_bsa > 0`, `tab != UNKNOWN`만 사용한다.

### 2.3 Target Workbook

`scripts/build_sales_target_drill_data.py`는 Google Sheets API로 Target Workbook `Summary_All!A1:Z`를 읽는다.

`Summary_All` 데이터 시작 행은 5행이며 주요 컬럼은 다음과 같다.

| 범위 | 내용 |
| --- | --- |
| A:B | `Tab`, `Name` |
| C:D | 2025 share, 2025 booking base |
| E:J | Booking Q1/Q2 target, perform/progress, gap |
| K:P | Lifting Q1/Q2 target, perform/progress, gap |
| Q:V | High-Profit Q1/Q2 target, perform/progress, gap |
| W:Y | A/C total, WOS-3 A/C, WOS-3 A/C ratio |
| Z:AB | Booking/Lifting/High-Profit Q3 target |
| AC | Row type (`TOTAL` / `SALES`) |

`Name`이 `Team Total` 또는 `Total`이면 `row_type = TOTAL`, 그 외는 `SALES`로 저장한다.

### 2.4 2025 Cache

`output/_cache_2025.parquet`는 다음 용도로 쓰인다.

- `w3_2025_teu`: 2025 WOS-3 FST_TEU 기준 물량
- `base2025.num`: 도착/등급/고수익 필터별 2025 KPI numerator
- `base2025.bsa`: 2025 BSA 배분값
- `base2025.shpr`: 2025 화주별 Normal LST, 화주 BSA 목표 배분 기준
- `base2025.smeta`: 이탈화주 표시용 화주명/등급

캐시가 없으면 일부 2025 컬럼과 필터 연동 Target 재계산이 제한된다.

## 3. Target tab 매핑

`tab_key(origin, ori_port)`가 화면의 선적지 단위를 만든다.

| 조건 | tab |
| --- | --- |
| CN + NKG 권역 포트 | `CN_NKG` |
| CN + SHK/DCB | `CN_SHK_DCB` |
| CN + 기타 포트 | `CN_<PORT>` |
| VN + SGN/CMP | `VN_SGN_CMP` |
| VN + HPH | `VN_HPH` |
| ID + JKT | `JKT` |
| ID + SUB | `SUB` |
| ID + 기타 | `ID-IDO` |
| MY + PKG/PKW | `PKG+PKW` |
| MY + PEN/PGU | `PEN` 또는 `PGU` |
| 기타 | 원본 origin |

팀 분류는 `POR_CTR_CD not in (KR, JP)` 그리고 `DLY_CTR_CD != KR`이면 OBT다.

## 4. 산출 JSON

### 4.1 `index.json`

목적: 첫 화면 Target 요약과 KPI 카드의 기본 목표/성과를 빠르게 렌더링한다.

주요 구조:

```json
{
  "_format": "sales-target-index-v1",
  "generated_at": "2026-06-15T11:16:21",
  "data_date": "20260615",
  "workbook_id": "...",
  "workbook_url": "https://docs.google.com/spreadsheets/d/.../edit",
  "rows": [],
  "origins": [],
  "months": []
}
```

`rows[]`는 `TOTAL`과 `SALES` 행을 모두 포함한다.

주요 row 필드:

| 필드 | 의미 |
| --- | --- |
| `tab` | 선적지 tab |
| `name` | `Team Total` 또는 영업사원명 |
| `row_type` | `TOTAL` / `SALES` |
| `share_2025` | 2025 Normal LST 기준 영업사원 비중 |
| `booking_base_2025` | 2025 WOS-3 Booking / BSA |
| `w3_2025_teu` | 2025 WOS-3 FST_TEU |
| `kpi.booking/lifting/high_profit.q1/q2/q3` | target, perform/progress, gap. Q3 target comes from the workbook; progress/gap are filled from generated Q3 booking months when available. |
| `accounts` | 화주 total, WOS-3 화주, 비중 |
| `month_progress` | 월별 live progress. 메인 대시보드 overlay와 Sales Target 화면 정합성 유지 |

### 4.2 `manifest.json`

목적: 화면 필터 옵션과 lazy-load 청크 목록을 제공한다.

주요 필드:

| 필드 | 의미 |
| --- | --- |
| `origins` | 사용 가능한 tab 목록 |
| `months` | 사용 가능한 `YYYYMM` |
| `salespeople_by_origin` | tab별 영업사원 옵션 |
| `dest_countries` | 도착국가 필터 옵션 |
| `dest_ports_by_country` | 도착국가별 도착포트 옵션 |
| `chunks` | `{origin, salesman, yyyymm, file, rows, shippers}` |

2026-06-15 기준 `chunk_count = 964`, `bkg_rows = 259,599`이다.

### 4.3 `data/<origin>__<salesman>__<YYYYMM>.json`

목적: 영업사원/월 상세 Drill과 Pivot을 위한 BKG_NO 단위 데이터.

구조:

```json
{
  "origin": "CN_SHA",
  "salesman": "WENJIE",
  "yyyymm": "202605",
  "totals": {},
  "shippers": [],
  "bsa_allocations": [],
  "bookings": []
}
```

`bookings[]`는 BKG_NO별 route, 화주, 상태, TEU, CM1, WOS, grade, high-profit flag를 가진다. `shippers[]`는 동일 chunk 안에서 화주별 사전 집계이며, 화면에서는 다시 필터 적용 후 재집계할 수 있다.

### 4.4 `base2025.json`

목적: 필터 연동 2025 기준선과 Target 재계산.

구조:

```json
{
  "_format": "sales-target-base2025-v1",
  "generated_at": "2026-06-15T11:16:21",
  "data_date": "20260615",
  "base": {
    "CN_SHA": {
      "WENJIE": {
        "num": [],
        "bsa": [],
        "shpr": [],
        "smeta": {}
      }
    }
  }
}
```

`num[]`:

| 필드 | 의미 |
| --- | --- |
| `dlyc`, `dly` | 도착국가/도착포트 |
| `g` | grade 첫 글자 |
| `hi` | 구간별 고수익 여부 |
| `nlst` | 2025 Normal LST, 전체 lead-time |
| `w3f` | 2025 WOS-3 FST_TEU |
| `w3l` | 2025 WOS-3 Normal LST_TEU |
| `w3h` | 2025 WOS-3 구간별 고수익 FST_TEU |

`bsa[]`: 도착국가/포트별 영업사원 배분 BSA.  
`shpr[]`: 도착국가/포트/화주별 2025 Normal LST.  
`smeta`: 화주명/등급 메타데이터.

## 5. BSA 배분

BSA 원천은 영업사원 차원이 없다. 따라서 `build_allocated_bsa()`가 route BSA를 영업사원에 배분한다.

기본 배분키:

```text
team, tab, dest, dst_port
```

배분 기준:

```text
basis_lst(sp, route) = 2025 Normal LST_TEU(sp, route)
basis_total(route) = Σ basis_lst(sp, route)
allocated_bsa(sp, route, month)
  = route_bsa(route, month) * basis_lst(sp, route) / basis_total(route)
```

신규 lane fallback:

```text
if basis_total(route) <= 0:
  act_lst(sp, route) = 2026 해당월 FST_TEU 활동량
  allocated_bsa = route_bsa * act_lst(sp, route) / Σ act_lst(route)
```

이 fallback은 2025 이력이 전혀 없어 BSA가 누락되는 것을 막기 위한 예외다.

## 6. 화면 Runtime 흐름

`dist/sales-target/app.js` 초기화:

```text
Promise.all([
  loadJson('index.json'),
  loadJson('manifest.json'),
  loadJson('base2025.json').catch(() => null)
])
```

렌더링:

| View | 데이터 |
| --- | --- |
| KPI cards | 기본은 `index.json`, 상세 필터가 있으면 chunk + BSA allocation 재계산 |
| 1. Target 요약 | 기본은 `index.json`, 도착/등급/고수익/월 필터가 있으면 상세 chunk 기반 재계산 |
| 2. Drill | 선택된 origin/sales/month chunk lazy-load |
| 3. Pivot | 선택 범위 chunk를 모두 로드한 뒤 브라우저에서 집계 |

필터 연동:

- 선적국가/선적포트/영업사원/도착국가/도착포트는 cascade 옵션을 가진다.
- 도착국가/도착포트/등급/고수익 필터는 `base2025.json`의 2025 기준선도 같은 조건으로 다시 자른다.
- 분기/월/WOS 필터는 2025 annual base를 자르지 않는다.

## 7. 빌드 명령

일반 재생성:

```bash
python scripts/build_sales_target_drill_data.py
```

명시 실행:

```bash
python scripts/build_sales_target_drill_data.py \
  --workbook 1YxZkwvoMaQXIEw07qUDZtCPDFZBf8GOZyr5knkxnLxo \
  --snapshot output/booking_snapshot_result_20260615.csv \
  --as-of 20260615 \
  --out dist/sales-target
```

주요 전제:

- `.gdrive-mcp/credentials.json`, `.gdrive-mcp/token.json` 사용 가능
- `output/booking_snapshot_result_YYYYMMDD.csv` 존재
- `output/BSA_raw_monthly3W_YYYYMMDD.csv` 존재
- `output/_cache_2025.parquet` 존재 또는 생성 가능
- Target Workbook `Summary_All` 접근 가능

## 8. 배포와 커밋 흐름

`dist/`는 별도 Git 저장소다.

```bash
git -C dist status --short
git -C dist add sales-target/index.json sales-target/manifest.json sales-target/base2025.json sales-target/data
git -C dist commit -m "Auto update sales-target drill data (YYYY-MM-DD)"
git -C dist push
```

상위 repo는 `dist` 포인터와 코드/문서 변경을 별도 커밋한다.

```bash
git status --short
git add dist scripts/build_sales_target_drill_data.py docs/sales-target docs/changes
git commit -m "Document sales target pipeline and Q3 target workflows"
git push
```

자동 배포가 아닌 수동 커밋 시에는 `output/`의 대용량 원천 파일은 커밋하지 않는다.
