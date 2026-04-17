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
- 다운로드 방식: Sales+Team 파라미터별 4회 다운로드 (OBT/EST/IST/JBT)
- 팀 분류: POR_Country/DLY_Country 기반 classify_team으로 재분류 (중복 방지)
- 포함: POR_Country, POR_PORT, DLY_Country, DLY_PORT, YYYYMM, WW, TEU_BSA

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

**집계 메트릭** (monthly/weekly 공통):

| 메트릭 | 의미 |
|--------|------|
| fst | 전체 BKG (FST_TEU 기준) |
| norm_lst | 실선적 — 전체 Normal (LST_TEU 기준, 소석률 계산용) |
| w3_fst | WOS-3 BKG |
| w3_norm_lst | WOS-3 실선적 (LST_TEU 기준) |
| w3_canc_fst | WOS-3 캔슬 |
| w3_hi_fst | WOS-3 고수익화주 BKG |
| w3_hi_norm_lst | WOS-3 고수익화주 실선적 |
| w3_ab_fst / w3_cd_fst | WOS-3 A+B / C+D 등급 BKG |
| w2_fst, w1_fst, wos_fst | WOS-2, WOS-1, WOS 각 단계 BKG |
| cm1_norm | CM1 합계 (Normal + CM1!=0) |
| lst_norm | LST_TEU 합계 (Normal + CM1!=0, CM1/TEU 계산용) |

**소석률 계산 기준**:
- 소석률 = norm_lst(전체 Normal 실선적) / BSA
- norm_lst는 Lead_time 무관하게 **모든 Normal 부킹**의 LST_TEU 합계
- 3주전 실선적률 = w3_norm_lst / w3_fst

**BSA 집계**: teu_bsa=0인 레코드는 JSON 생성 전 제거 (0값 필드 누락 방지)

**shipper 메트릭**: monthly/weekly와 동일하되 AB/CD 컬럼 제외

**JSON 최적화**:
- 0값 키 제거 (71MB, 약 190만 개 0 엔트리 삭제)
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

## 5. 팀 분류 (classify_team)

POR_Country(선적지)와 DLY_Country(도착지) 기반:

| 조건 | 팀 |
|------|-----|
| 선적지 != KR,JP AND 도착지 != KR | OBT (해외수출) |
| 선적지 == KR AND 도착지 != JP | EST (한국수출) |
| 선적지 != JP AND 도착지 == KR | IST (한국수입) |
| 그 외 (JP 관련) | JBT (일본) |

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
- **지역별**: 3주전 BKG + BSA/소석률 테이블, 주차별 차트, 고수익 비중 추이
- **화주별**: 화주별 BKG/실선적/CM1/영업사원 테이블
- **영업사원별**: 영업사원별 BKG/실선적/화주수/CM1
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

### Google Drive
- 폴더 ID: 1JIxg6Y-_gRfI1HueXZ1Q9j4-Z5bxvNgv
- 인증: .gdrive-mcp/credentials.json + token.json (OAuth2 refresh)
- 업로드 파일: _cache_*.parquet, BSA_*.csv, dashboard_summary.json

### Gmail 알림
- 인증: .gmail-mcp/credentials.json + gcp-oauth.keys.json
- 발송: 일일 실행 결과 HTML 이메일 (jkpark@ekmtc.com)

### GitHub Pages
- dist/ repo: kmtc-3w-dashboard-web (master branch)
- main repo: kmtc-3w-dashboard (master branch)

---

## 11. 알려진 제한사항

1. **BSA 데이터 시점**: Tableau CSV export가 인터랙티브 화면과 다른 시점의 캐시를 반환할 수 있음
2. **data.json 크기**: 현재 ~71MB (shipper 주차별 데이터 포함). GitHub 100MB 제한 근접
3. **445 Calendar 하드코딩**: 2025~2027년 시작일이 코드에 고정. 2028년 이후 추가 필요
4. **Grade 분기 갱신**: Tableau grade 뷰에서 분기별 자동 다운로드. 뷰 구조 변경 시 컬럼 매칭 로직 수정 필요

---

## 12. 변경 이력 (2026-04-16)

| 변경사항 | 커밋 | 설명 |
|---------|------|------|
| Grade 자동 다운로드 | ef411a5 | `booking snapshot.xlsx` 의존 제거 → Tableau 분기별 다운로드 |
| Grade 기본값 변경 | a083517 | 미분류 화주: 'C+D' → '' (빈값) |
| 소석률 계산 수정 | 0b194c0 | norm_lst = 전체 Normal (기존: WOS-3 Normal만) |
| 고수익태그 수정 | a083517 | 전역 최신월 → 화주+선적지별 최신월 기준 |
| BSA 집계 수정 | ee0ecba | teu_bsa 필드 누락 처리 + 0값 레코드 제거 |
| 445 Calendar 분리 | ef411a5 | 항상 코드에서 445 맵 생성 (template 의존 제거) |
