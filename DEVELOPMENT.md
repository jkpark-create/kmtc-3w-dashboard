# -3W Booking Dashboard - Development Documentation

## 1. 시스템 개요

**목적**: 3주전(WOS-3) 기준 부킹 현황, 소석률, 수익성 분석 대시보드
**배포**: GitHub Pages 정적 사이트 (https://jkpark-create.github.io/kmtc-3w-dashboard-web/)
**데이터 소스**: Tableau Server (tableau.ekmtc.com) → Google Drive → GitHub Pages
**자동화**: Windows Task Scheduler (평일 09:03 / 매일 09:00)

---

## 2. 프로젝트 구조

```
-3W bkg dashboard/
  daily_3w_dashboard.py   # 데이터 파이프라인 (Tableau→처리→GDrive→GitHub)
  send_notification.py    # 일일 실행 결과 이메일 알림
  run_daily.bat           # Windows 배치 실행 스크립트
  .env                    # 환경변수 (TABLEAU_PASS 등)
  dist/                   # GitHub Pages 배포 (별도 git repo)
    index.html            # 정적 대시보드 (JS/HTML)
    data.json             # 대시보드 데이터 (매일 갱신)
    guide.html            # 사용 가이드
  output/                 # 일일 처리 결과 (최신 2개 유지)
    grade_latest.csv      # Tableau 분기별 화주 등급 (자동 다운로드/캐시)
  logs/                   # 실행 로그
  docs/changes/           # 개발건별 변경 기록 (NotebookLM/후속 개발 참고용)
```

---

## 3. 데이터 파이프라인 (daily_3w_dashboard.py)

### 3.1 Phase 1: Tableau 다운로드

**1.csv (View 1 - Booking_schedule)**: 부킹 상세 정보
- 소스: `temp_bkg_snapshot_v2` 워크북 (필터 범위: 2025-12-28 ~ 금주+4주)
- 포함: BKG_SHPR_CST_NO, Booking_date, Booking_schedule, Cancel_date, FST_TEU 등
- 인코딩: UTF-16, Tab-separated

**2.csv (View 2 - Date_vsl)**: 실선적 기준 데이터
- 소스: 동일 워크북의 View 2
- 포함: BKG_NO, Booking_status, CM1_Booking, LST_TEU, LST_Route, Date_vsl 등
- Base 데이터 (모든 부킹 포함)

**BSA raw (월간회의3주전)**: BSA(Booking Space Allocation) 데이터
- 소스: `Q_17363223877520/BSArawBKGpattern` 뷰
- 다운로드 방식: `Sales Team` 파라미터별 4회 다운로드 (OBT/EST/IST/JBT)
- 팀 필드: Tableau CSV의 `Sales Team`을 canonical `team`으로 사용. `Sales Team`이 없는 과거 파일만 `classify_team`으로 fallback
- 포함: Sales Team, POR_Country, POR_PORT, DLY_Country, DLY_PORT, YYYYMM, WW, TEU_BSA

### 3.2 Phase 2: Booking Snapshot 처리

**Grade 데이터 (화주 등급)**:
- 소스: Tableau `Q_17363223877520/grade` 뷰에서 분기별 자동 다운로드
- 분기 매핑: Q1=01월, Q2=04월, Q3=07월, Q4=10월 (YYYYMM 파라미터)
- 캐시: `output/grade_latest.csv` (동일 분기면 재다운로드 안 함)
- 분류: AB → A+B, CD → C+D, 미매칭 → '' (빈값, 미분류)
- Fallback: 다운로드 실패 시 기존 parquet 캐시에서 grade 로드

**기본 로직**: 2.csv(Base) + 1.csv(Supplement) 병합
- 2.csv의 모든 BKG_NO를 base로 사용
- 1.csv에서 부킹일자, 화주정보, POL/POD 등 보충
- Fallback: 1.csv에 없는 BKG는 2.csv의 Date_vsl 사용

**주요 계산 컬럼**:

| 컬럼 | 계산 로직 |
|------|-----------|
| week_start_date | Date_vsl 기준 일요일 (한국어 날짜 형식: "2026년 04월 05일") |
| Lead_time(Booking) | Booking_schedule - Booking_date → 1W/2W/3W/4W |
| Lead_time(Actual) | week_start_date(일요일) - Booking_date → WOS/WOS-1/WOS-2/WOS-3 |
| Lead_time(BKG_Sche) | week_start(BKG_Sche) - Booking_date → WOS/WOS-1/WOS-2/WOS-3 |
| grade | 화주코드(BKG_SHPR_CST_NO) → Tableau grade 뷰 참조 → A+B / C+D / '' (미분류) |
| CM1/TEU | CM1 / LST_TEU (Normal, CM1!=0 건만) |
| YYYYMM | 445 calendar 기준 (week_start_date → YYYYMM 매핑) |
| 고/저 | 루트별(POR_PORT+DLY_PORT) 화주 CM1/TEU vs 루트 평균 비교 |
| 고수익태그 | 화주+선적지별 최신월 기준 CM1/TEU vs 선적지 평균 (1~4순위 룩업) |

**-3W 필터 (캔슬 제외 조건)**:
- 조건1: Cancel 상태 + Cancel_date - Booking_date <= 3일 (즉시 캔슬)
- 조건2: Cancel 상태 + Actual_Departure - Booking_date >= 21일 + Cancel_date - Booking_date <= 7일

**집계 전 데이터 범위 정리**:
- `LST_Status`가 빈 값인 행은 제외한다.
- 위 -3W 필터에서 제외로 판정된 Cancel 행은 모든 집계에서 제외한다.
- 대시보드 표시 범위는 현재 연도 1월 이후 `YYYYMM`만 남긴다. 예: 2026년 실행 시 `YYYYMM < 202601`은 제외한다.
- 2.csv가 Normal/Confirm 위주로 내려오는 경우를 대비해, 1.csv에는 있으나 2.csv에는 없는 Cancel 건을 복원한다. 이때 이전 snapshot에서 Actual_Departure_schedule을 찾은 Cancel 건만 복원하며, LST_TEU와 CM1은 0/빈값으로 둔다.

### 3.3 Phase 3: JSON 생성 및 업로드

**data.json 구조**:

```json
{
  "data_date": "20260415",
  "wpm": {"202601": 4, "202602": 4, ...},  // 445 기준 월별 주수
  "months": ["202601", "202602", ...],
  "monthly": [...],   // 루트×월 집계 (17K rows, 6.1MB)
  "weekly": [...],    // 루트×주 집계 (38K rows, 14.7MB)
  "shipper": [...],   // 루트×주×화주 집계 (159K rows, ~45MB)
  "bsa": [...]        // BSA 루트×주 (50K rows, 6.1MB)
}
```

**집계 그룹키**:

| 데이터셋 | 그룹키 |
|---------|--------|
| monthly | team, origin, ori_port, dest, dst_port, YYYYMM |
| weekly | monthly + week_start_date |
| shipper | weekly + BKG_SHPR_CST_NO, BKG_SHPR_CST_ENM, Salesman_POR, 고수익태그, grade |
| bsa | team, origin, POR_PORT, dest, DLY_PORT, YYYYMM, WW |

**프론트엔드 파생 집계**:

| 화면 | 기준 데이터 | 파생 그룹키 | 비고 |
|------|-------------|-------------|------|
| Tab2 영업사원별 실적 | shipper | Salesman_POR | 화주수는 BKG_SHPR_CST_NO distinct |
| Tab2 영업사원×도착국가 | shipper | Salesman_POR + dest | 도착포트(dst_port)가 아닌 도착국가/그룹(dest) 기준 |

- `영업사원×도착국가`는 `dist/index.html`의 `buildSalesDestRows(month)`에서 생성한다.
- 글로벌 필터(팀/선적지/선적포트/도착국가/도착포트/월/주차/화주구분/등급/영업사원)는 `filterShipper(month)`를 통해 먼저 적용된다.
- 매트릭스와 상세 테이블은 같은 파생 rows를 공유한다. 매트릭스는 WOS-3 BKG 규모를 빠르게 보기 위한 요약이고, 상세 테이블은 실선적률/캔슬률/구간별고수익/CM1 확인용이다.
- BSA는 영업사원 단위로 배분되지 않으므로 `영업사원×도착국가`에는 소석률을 표시하지 않는다. 소석률은 지역별/도착국가별 전체 관점에서만 해석한다.

**집계 메트릭** (monthly/weekly 공통):

| 메트릭 | 의미 |
|--------|------|
| fst | 전체 BKG (FST_TEU 기준) |
| norm_lst | 실선적 — 전체 Normal (LST_TEU 기준, 소석률 계산용) |
| hi_fst | 고수익화주 BKG (`고수익태그` 기준, FST_TEU) |
| hi_norm_lst | 고수익화주 실선적 (`고수익태그` 기준, LST_TEU) |
| w3_fst | WOS-3 BKG |
| w3_norm_lst | WOS-3 실선적 (LST_TEU 기준) |
| w3_canc_fst | WOS-3 캔슬 |
| w3_hi_fst | WOS-3 고수익화주 BKG (`고수익태그` 기준) |
| w3_hi_norm_lst | WOS-3 고수익화주 실선적 (LST_TEU 기준) |
| w3_route_hi_fst | WOS-3 구간별 고수익 BKG (`고/저` 기준) |
| w3_ab_fst / w3_cd_fst | WOS-3 A+B / C+D 등급 BKG |
| w2_fst, w1_fst, wos_fst | WOS-2, WOS-1, WOS 각 단계 BKG |
| cm1_norm | CM1 합계 (Normal + CM1!=0) |
| lst_norm | LST_TEU 합계 (Normal + CM1!=0, CM1/TEU 계산용) |

**KPI 생성 로직 상세**:

대시보드의 KPI 카드와 대부분의 표/차트는 `daily_3w_dashboard.py`에서 만든 집계 필드를 그대로 합산한다.

| 화면 KPI | JSON 필드 | 생성 공식 | 원천/조건 |
|----------|-----------|-----------|-----------|
| 전체 BKG | `fst` | `FST_TEU` | 1.csv Booking_schedule 뷰의 FST_TEU. 비어 있으면 LST_TEU로 fallback |
| 실선적(Normal) | `norm_lst` | `LST_TEU * is_normal` | 2.csv Date_vsl 뷰의 LST_TEU 중 `LST_Status == "Normal"` |
| 3주전 BKG | `w3_fst` | `FST_TEU * (Lead_time (BKG_Sche) == "WOS-3")` | Booking_schedule 기준 WOS-3 판정 |
| 3주전 실선적(Normal) | `w3_norm_lst` | `LST_TEU * (Lead_time (BKG_Sche) == "WOS-3") * is_normal` | WOS-3이면서 Normal인 실제 선적 TEU |

`is_normal`은 `LST_Status == "Normal"`이면 1, 아니면 0이다. 따라서 전체 BKG와 3주전 BKG는 상태별로 남아 있는 대상 건의 `FST_TEU`를 합산하고, 실선적 계열은 Normal 건의 `LST_TEU`만 합산한다.

**WOS-3 판정 기준**:

`Lead_time (BKG_Sche)`는 Booking_schedule이 속한 주의 시작일(일요일)과 Booking_date의 차이로 계산한다.

```text
week_start (BKG_Sche) = Booking_schedule 날짜가 속한 주의 일요일
diff = week_start (BKG_Sche) - Booking_date

diff < 1   → Week of Sailing (WOS)
diff <= 7  → WOS-1
diff <= 14 → WOS-2
diff > 14  → WOS-3
```

주의: `YYYYMM`과 `week_start_date`는 Actual_Departure_schedule(Date_vsl) 기준 445 Calendar로 정해진다. 반면 WOS 단계는 Booking_schedule 기준으로 계산한다. 따라서 “2026년 5월 3주전 BKG”는 Actual 출항 주차가 2026년 5월에 속하면서, Booking_schedule 기준으로 WOS-3인 부킹의 `FST_TEU` 합계다.

**프론트엔드 KPI 합산**:

Tab 1 KPI는 `dist/index.html`에서 다음 필드를 합산한다.

```javascript
totalBkg = profitSum(fd, 'fst')
shipped  = profitSum(fd, 'norm_lst')
w3Bkg    = profitSum(fd, 'w3_fst')
w3Ship   = profitSum(fd, 'w3_norm_lst')
```

`fd`는 월 전체 선택 시 `filterMonthly(month)`, 주차 선택 시 `filterWeekly(month)` 결과다. 팀/선적지/선적포트/도착지/도착포트/월/주차 필터가 적용된다. `화주구분` 필터는 `profitSum()`에서 고수익/저수익 필드로 수치를 조정한다. 단, Tab 1의 monthly/weekly 집계에는 화주/영업사원 차원이 없으므로 `등급`과 `영업사원` 필터는 shipper 기반 화면에서 주로 반영된다.

**소석률 계산 기준**:
- 소석률 = norm_lst(전체 Normal 실선적) / BSA
- norm_lst는 Lead_time 무관하게 **모든 Normal 부킹**의 LST_TEU 합계
- 고수익화주부킹비중 = w3_hi_fst / w3_fst
- WOS 단계별 실선적률 = LST_TEU / FST_TEU. 최종 선적 TEU가 최초 부킹 TEU보다 커지면 100%를 넘을 수 있음

**BSA 집계**: teu_bsa=0인 레코드는 JSON 생성 전 제거 (0값 필드 누락 방지)

**shipper 메트릭**: monthly/weekly와 동일하되 AB/CD 컬럼 제외

**JSON 최적화**:
- 0값 키 제거 (현재 약 81MB, 약 190만 개 0 엔트리 삭제)
- 소수점 → 정수 반올림
- LST_TEU 기준 실선적 컬럼은 `*_lst` 접미사를 사용

---

## 4. 445 Calendar

KMTC 내부 445 패턴 주차-월 매핑:

```
패턴: [4, 4, 5, 4, 4, 5, 4, 4, 5, 4, 4, 5] (총 52주)
2026년 시작일: 2026-01-04 (일요일)

1월: WW1~4 (4주),  2월: WW5~8 (4주),   3월: WW9~13 (5주)
4월: WW14~17 (4주), 5월: WW18~21 (4주),  6월: WW22~26 (5주)
7월: WW27~30 (4주), 8월: WW31~34 (4주),  9월: WW35~39 (5주)
10월: WW40~43 (4주), 11월: WW44~47 (4주), 12월: WW48~52 (5주)
```

week_start_date → YYYYMM 매핑 함수 (`_build_445_map`):
- 각 연도의 첫 일요일부터 시작
- 2025: 2025-01-05, 2026: 2026-01-04, 2027: 2027-01-03

---

## 5. 팀 분류 및 BSA 팀 필드

Booking Snapshot의 `team`은 POR_Country(선적지)와 DLY_Country(도착지) 기반 `classify_team`으로 계산한다:

| 조건 | 팀 |
|------|-----|
| 선적지 != KR,JP AND 도착지 != KR | OBT (해외수출) |
| 선적지 == KR AND 도착지 != JP | EST (한국수출) |
| 선적지 != JP AND 도착지 == KR | IST (한국수입) |
| 그 외 (JP 관련) | JBT (일본) |

BSA는 Tableau 원천 뷰에 추가된 `Sales Team` 필드를 기준으로 `team`을 연결한다. 국가 기반 재분류는 `Sales Team` 컬럼이 없는 과거 BSA 파일을 읽을 때만 fallback으로 사용한다.

---

## 6. 도착지 그룹 매핑 (DEST_GROUP_MAP)

BKG와 BSA 모두 동일 매핑 적용:

| 원본 국가 | 그룹 |
|-----------|------|
| MY, SG | MY/SG |
| AE, SA, KW, QA, OM, BH, IQ, JO, EG | AE |
| 기타 | 원본 코드 유지 |

---

## 7. 대시보드 필터 체계 (dist/index.html)

### 7.1 글로벌 필터 (모든 탭에 적용)

| 필터 | ID | BKG 필드 | BSA 필드 |
|------|-----|---------|---------|
| 팀 | fTeam | team | team |
| 선적지 | fOri | origin | origin |
| 선적포트 | fOriP | ori_port | POR_PORT |
| 도착지 | fDst | dest | dest |
| 도착포트 | fDstP | dst_port | DLY_PORT |
| 보기 | fView | (dest/origin 전환) | (dest/origin 전환) |
| 월 | fMonth | YYYYMM | YYYYMM |
| 주차 | fWeek | week_start_date | WW (weekToWW 변환) |
| 화주구분 | fProfit | 고수익태그 | N/A |
| 등급 | fGrade | grade | N/A |
| 영업사원 | fSales | Salesman_POR | N/A |

### 7.2 Cascading 필터 순서

```
팀 → 선적지 → 선적포트 → 도착지 → 도착포트
(상위 필터 변경 시 하위 옵션 자동 갱신, 선택값 ALL로 리셋)
```

### 7.3 데이터 함수

| 함수 | 용도 | 주차 처리 |
|------|------|----------|
| filterMonthly(month) | 월간 집계 데이터 | 월 필터만 |
| filterWeekly(month) | 주간 집계 데이터 | 월+주차 필터 |
| filterShipper(month) | 화주별 데이터 | 월+주차+영업사원+화주구분+등급 |
| filterBSA(month, week) | BSA 데이터 | 월+WW (week=null→전체, undefined→gv사용) |
| buildSalesDestRows(month) | 영업사원×도착국가 파생 데이터 | `filterShipper` 결과를 Salesman_POR+dest로 재집계 |
| renderSalesDestMatrix(rows, month) | 영업사원×도착국가 매트릭스 | 상위 도착국가 12개 + 기타, 셀 색상은 WOS-3 BKG 규모 |
| renderSalesDestDetail(rows, sourceRows, month) | 영업사원별 도착국가 상세 | 조합별 화주수/BKG/실선적/캔슬/수익성 |

### 7.4 주차 선택 시 데이터 흐름

```
주차 미선택: filterMonthly(month) → 해당 월 전체 합계
주차 선택:   filterWeekly(month)  → 해당 주 실데이터 (평균 아님)
BSA:        filterBSA(month)     → weekToWW 변환 → WW 필터
```

**Tab 1 바차트/요약테이블의 과거 월 비교**:
- 선택 월: 해당 주 실데이터
- 과거 월: filterMonthly(m) / wpm (주평균, "주평균" 라벨 표기)

### 7.5 BSA WW 변환 (weekToWW)

```javascript
// week_start_date ("2026년 05월 03일") → WW 번호 ("18")
start = 2026-01-04 (1월 첫째 일요일)
diff = (target_date - start) / 7일
WW = diff + 1
```

---

## 8. 대시보드 탭 구조

### Tab 1: 소석률 현황
- KPI 카드: 전체BKG, 실선적, 3주전BKG, 3주전실선적, BSA, 소석률, 실선적률, CM1/TEU
- 월별 바차트: 실선적 vs BSA + 소석률/3주전BKG/BSA 라인
- 도착지별 소석률 수평 바
- Image-8 요약 테이블 (최근 3개월 × 도착지별)

### Tab 2: 부킹 트렌드 (Sub-tabs)
- **지역별**: 3주전 BKG + BSA/소석률 테이블, 주차별 차트, 구간별 고수익 비중 + 고수익화주 비중 추이
- **화주별**: 화주별 BKG/실선적/CM1/영업사원 테이블. 화주명은 줄바꿈 없이 한 줄로 표시, 영업사원은 3명 이상일 때 앞 2명 + `...`로 표시
- **영업사원별**: 영업사원별 BKG/실선적/화주수/CM1 + 영업사원×도착국가 분석
  - `영업사원별 실적`: Salesman_POR 기준 WOS-3 BKG, 실선적, 실선적률, 구간별 고수익, CM1/TEU
  - `영업사원별 BKG vs 실선적률`: 상위 영업사원 BKG/실선적 수평 막대
  - `Top 영업사원 도착국가 구성`: 상위 12명 영업사원의 WOS-3 BKG를 도착국가별 stacked bar로 표시. 상위 도착국가 8개 외는 `기타`로 묶음
  - `영업사원×도착국가 WOS-3 BKG 매트릭스`: 행=영업사원, 열=도착국가, 셀=WOS-3 BKG TEU. 색상은 셀 값/최대 셀 값 비율로 진하게 표시
  - `영업사원별 도착국가 상세`: Salesman_POR+dest 조합별 화주수, BKG, 실선적, 실선적률, 캔슬, 캔슬률, 구간별고수익, 구간별고수익%, CM1, CM1/TEU
  - 목적: 영업사원별 물량이 어느 도착국가에 집중되어 있는지, 큰 물량 조합의 실선적/캔슬/수익성 리스크가 있는지 확인
  - 주의: 도착국가 기준(`dest`)이며 도착포트(`dst_port`) 기준이 아님. BSA/소석률은 영업사원별 배분 근거가 없어 표시하지 않음
- **AB vs CD**: A+B/C+D 등급별 BKG/실선적률 비교 + Analysis Guide

### Tab 3: 전환 퍼널
- 3주전BKG → 캔슬제외 → 실선적 퍼널
- 도착지별 실선적률 추이, 캔슬률 차트/추이
- 화주별 캔슬 Top 15

### Tab 4: 수익성
- CM1/TEU 바, 월별 CM1/TEU 추이
- 화주 CM1/TEU Top/Bottom 10
- 수익 집중도 (누적 CM1 %)

### Tab 5: 부킹 패턴
- 지역별 WOS 분포 테이블 + 100% stacked bar
- 화주별 WOS 분포 테이블
- 고수익 vs 저수익 리드타임 비교

---

## 9. 자동화 스케줄

### Windows Task Scheduler 등록 작업

| 작업명 | 스케줄 | 실행 대상 | 설명 |
|--------|--------|-----------|------|
| **3W_BKG_Dashboard** | 평일(월~금) 09:03 | `cmd /c run_daily.bat` | 주중 대시보드 자동 업데이트 (주력 작업) |
| **3W_BKG_Dashboard_Daily** | 매일 09:00 | `run_daily.bat` | 일일 백업 작업 |
| **OBT_Raw_Automation** | 매일 09:00 | `cmd /c obt raw automation\run.bat` | Tableau OBT Raw → Google Sheets 동기화 |
| **RFQ_GDrive_Sync** | 매일 08:00 | `rfq_tool\gdrive_sync.py` | Google Drive 비딩 파일 동기화 |

### run_daily.bat 실행 흐름

```
1. 캐시 정리 (output/_cache_*, dashboard_summary_*)
2. python daily_3w_dashboard.py
   Phase 1: Tableau 다운로드 (1.csv, 2.csv, BSA, grade)
   Phase 2: Booking Snapshot 처리 → output/booking_snapshot_result_YYYYMMDD.xlsx
   Phase 3: JSON 생성 → dist/data.json, Google Drive 업로드
3. dist/ git push (data.json 변경 시만)
4. python send_notification.py (이메일 알림, 성공/실패 무관 항상 실행)
```

### 파일 정리
- output/: 각 유형별 최신 2개만 유지 (자동 삭제)
- logs/: `run_YYYYMMDD.log` 일별 로그 저장

---

## 10. 외부 연동

### Tableau Server
- URL: https://tableau.ekmtc.com
- 계정: obt / .env TABLEAU_PASS
- Playwright(headless Chrome)로 로그인 후 CSV 다운로드
- 인증 방식: 브라우저 세션 로그인 후 CSV URL로 이동하여 다운로드 이벤트를 받는다. 서버 인증 토큰을 직접 저장하지 않고, 실행 시마다 Playwright 세션에서 로그인한다.
- Booking Snapshot:
  - 원본 워크북 `bookingsnapshot`을 Tableau REST API로 내려받아 Booking_schedule 날짜 필터를 수정한다.
  - 수정된 임시 워크북 `temp_bkg_snapshot_v2`를 Tableau 프로젝트에 publish한다.
  - View `1`, `2`를 각각 CSV로 다운로드한다.
- BSA:
  - 소스 뷰는 `Q_17363223877520/BSArawBKGpattern`.
  - `Sales Team` 파라미터로 OBT/EST/IST/JBT를 각각 다운로드한다.
  - CSV에 포함된 `Sales Team` 컬럼을 대시보드의 canonical `team`으로 사용한다.
- Grade:
  - 소스 뷰는 `Q_17363223877520/grade`.
  - 현재 분기 시작월(Q1=01, Q2=04, Q3=07, Q4=10)을 `YYYYMM` 파라미터로 다운로드한다.
  - 같은 분기의 `output/grade_latest.csv`가 있으면 재다운로드하지 않는다.
- 주의: Tableau 화면과 CSV export 사이에 서버 캐시/시점 차이가 생길 수 있다. BSA 값 검산 시에는 같은 CSV export 방식으로 재다운로드한 파일과 비교한다.

### Google Drive
- 폴더 ID: 1JIxg6Y-_gRfI1HueXZ1Q9j4-Z5bxvNgv
- 인증: .gdrive-mcp/credentials.json + token.json (OAuth2 refresh)
- 업로드 파일: _cache_*.parquet, BSA_*.csv, dashboard_summary.json
- 인증 흐름:
  - `.gdrive-mcp/credentials.json`에서 OAuth client 정보를 읽는다.
  - `.gdrive-mcp/token.json`의 refresh token으로 실행 시 access token을 갱신한다.
  - Drive REST API로 파일 존재 여부를 조회한 뒤 있으면 update, 없으면 create한다.
- 업로드 대상:
  - `_cache_YYYYMMDD.parquet`: 처리된 booking snapshot 캐시. 같은 날 재집계/재업로드 시 최신 파일로 덮어쓴다.
  - `BSA_raw_monthly3W_YYYYMMDD.csv`: Tableau에서 받은 최신 BSA raw.
  - `dashboard_summary.json`: 웹 대시보드가 과거 날짜 선택/Drive 조회 시 사용하는 고정 이름 최신 요약.
  - `dashboard_summary_YYYYMMDD.json`: 날짜별 히스토리 조회용 요약 파일.
- `upload_to_gdrive()`는 원격 업로드 전에 `dist/data.json`도 같은 summary JSON으로 복사한다. 따라서 Google Drive와 GitHub Pages가 같은 집계 결과를 바라보게 된다.
- output 폴더는 유형별 최신 2개 파일만 유지하도록 오래된 파일을 정리한다.

### Gmail 알림
- 인증: .gmail-mcp/credentials.json + gcp-oauth.keys.json
- 발송: 일일 실행 결과 HTML 이메일 (jkpark@ekmtc.com)
- `run_daily.bat`는 성공/실패와 관계없이 마지막에 `send_notification.py`를 호출한다.
- 알림 메일은 `logs/run_YYYYMMDD.log`를 읽어 성공 여부, 소요 시간, 단계별 상태, 에러 메시지, 주요 처리 결과를 HTML로 요약한다.
- 발송 계정과 수신 계정은 기본적으로 `jkpark@ekmtc.com`이다.

### GitHub Pages
- dist/ repo: kmtc-3w-dashboard-web (master branch)
- main repo: kmtc-3w-dashboard (master branch)
- `dist/`는 별도 Git 저장소이며 GitHub Pages 정적 사이트의 실제 배포 대상이다.
- `daily_3w_dashboard.py`는 summary JSON을 `dist/data.json`으로 복사한다.
- `run_daily.bat`는 `dist/data.json` 변경이 있을 때만 `dist` 저장소에서 commit/push한다.
- main repo는 자동화 코드, 운영 문서, 변경 기록, `dist` 서브모듈/포인터 상태를 관리한다.
- 배포 확인 순서:
  1. `dist/data.json`의 `data_date`와 핵심 KPI 값 확인
  2. `git -C dist status`가 clean인지 확인
  3. `dist` push 후 GitHub Pages URL에서 새 데이터 반영 확인
  4. main repo에서 코드/문서/`dist` 포인터 변경을 commit/push

---

## 11. 문서 운영 및 지식화

### 문서별 역할

- `dist/guide.html`: 웹 사용자 가이드. 현재 배포 UI에서 보이는 탭, 필터, 지표 의미, 100% 초과 가능 사유처럼 사용자 판단에 필요한 설명만 유지한다.
- `DEVELOPMENT.md`: 현재 운영 기준의 기술 문서. 데이터 소스, 집계 로직, 메트릭 정의, 자동화/배포 흐름, 알려진 제한사항을 최신 상태로 유지한다.
- `docs/changes/`: 개발건별 작업 기록. 향후 기능 추가/수정 시 이력과 의사결정 맥락을 잃지 않도록 한 건당 한 파일로 남긴다.

### 사용자 가이드 작성 기준

- 각 탭 설명은 단순 기능 목록이 아니라 다음 네 가지를 포함한다.
  - `무엇을 보는 화면인가`: 사용자가 답할 수 있는 업무 질문
  - `지표 의미`: 계산 기준, 분모/분자, 기준 데이터셋
  - `해석 방법`: 높거나 낮을 때의 의미, 같이 봐야 할 보조 지표
  - `다음 행동`: 이상치 발견 시 이어서 볼 탭/필터/관리 후보
- 혼동 가능성이 큰 지표는 반드시 구분해서 설명한다.
  - 소석률 = 실선적(Normal) / BSA
  - 3주전BKG/BSA = WOS-3 BKG / BSA
  - 실선적률 = WOS-3 실선적 / WOS-3 BKG
  - 고수익화주 = 선적지 기준 화주 태그
  - 구간별고수익 = POR+DLY 루트 기준 부킹 분류
- 필터 설명에는 기본값뿐 아니라 적용 범위와 해석상 주의점을 포함한다.
- 신규 화면을 추가할 때는 `dist/guide.html`, `DEVELOPMENT.md`, `docs/changes/`를 함께 갱신한다.

### 개발건별 기록 규칙

- 파일명: `YYYY-MM-DD_short-title.md`
- 필수 항목: 배경/문제, 결정/로직, 변경 파일, 검증 결과, 배포/커밋, 후속 확인사항
- 사용자용 설명이 필요한 변경은 `dist/guide.html`도 함께 갱신한다.
- 현재 로직의 기준값이나 예외 사유가 바뀌면 `DEVELOPMENT.md`의 메트릭 정의를 먼저 갱신하고, 변경건 문서에는 변경 이유와 검증 결과를 기록한다.

### NotebookLM 활용 권장 방식

- NotebookLM은 소스 문서 기반 질의/요약 보조 레이어로 사용한다.
- 원본(source of truth)은 Git에 버전 관리되는 `DEVELOPMENT.md`, `dist/guide.html`, `docs/changes/`로 둔다.
- 주요 배포 후 위 문서들을 NotebookLM 소스로 추가하거나 갱신해서 "왜 이 로직이 들어갔는지", "어느 파일을 봐야 하는지"를 빠르게 질의하는 방식이 적합하다.
- NotebookLM 답변은 소스 기반 요약으로 활용하되, 실제 수정/배포 판단은 Git 문서와 코드 diff를 기준으로 한다.

---

## 12. 알려진 제한사항

1. **BSA 데이터 시점**: Tableau CSV export가 인터랙티브 화면과 다른 시점의 캐시를 반환할 수 있음
2. **data.json 크기**: 현재 ~81MB (shipper 주차별 데이터 포함). GitHub 100MB 제한 근접
3. **445 Calendar 하드코딩**: 2025~2027년 시작일이 코드에 고정. 2028년 이후 추가 필요
4. **Grade 분기 갱신**: Tableau grade 뷰에서 분기별 자동 다운로드. 뷰 구조 변경 시 컬럼 매칭 로직 수정 필요
5. **영업사원별 BSA 미배분**: BSA는 루트/도착국가/포트 기준 선복 데이터이며 영업사원별 배분 필드가 없음. 영업사원×도착국가 분석에서는 소석률을 계산하지 않음

---

## 13. 변경 이력

| 변경사항 | 커밋 | 설명 |
|---------|------|------|
| BSA 팀 필드 연결 | c9a72f8 / fbc3721 | BSA raw의 `Sales Team`을 canonical `team`으로 사용, BSA 재다운로드 및 data.json 배포 |
| Grade 자동 다운로드 | ef411a5 | `booking snapshot.xlsx` 의존 제거 → Tableau 분기별 다운로드 |
| Grade 기본값 변경 | a083517 | 미분류 화주: 'C+D' → '' (빈값) |
| 소석률 계산 수정 | 0b194c0 | norm_lst = 전체 Normal (기존: WOS-3 Normal만) |
| 고수익태그 수정 | a083517 | 전역 최신월 → 화주+선적지별 최신월 기준 |
| BSA 집계 수정 | ee0ecba | teu_bsa 필드 누락 처리 + 0값 레코드 제거 |
| 고수익화주 집계 수정 | eda7ca5 / 201c99d | hi/w3_hi 메트릭을 `고수익태그` 기준으로 계산, 구간별 고수익은 w3_route_hi_fst로 분리 |
| 문서 운영 구조 추가 | docs | 사용자 가이드/개발 문서/개발건별 변경 기록의 역할 분리 |
| 445 Calendar 분리 | ef411a5 | 항상 코드에서 445 맵 생성 (template 의존 제거) |
| 영업사원×도착국가 분석 | acbb2b4 | Tab2 영업사원별에 도착국가 구성 차트, 매트릭스, 상세 테이블 추가 |
