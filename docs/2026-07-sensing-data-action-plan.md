# 2026-07 Sensing Data And Action Plan

기준일: 2026-06-30  
실행 기간: 2026-07-01 ~ 2026-07-31  
목적: 기존 자동화와 대시보드를 "확인용"에서 "센싱 후 액션으로 이어지는 업무 루틴"으로 전환한다.

## 1. 방향성

이번 한 달 동안 가져갈 센싱 데이터는 많을수록 좋은 것이 아니라, 아래 세 가지 질문에 답할 수 있어야 한다.

1. 어디에서 실적 또는 운영 리스크가 발생하고 있는가?
2. 그 원인이 물량, 선복, 운임, 수익성, 징수, 데이터 품질 중 무엇인가?
3. 누가, 무엇을, 언제까지 조치해야 하는가?

따라서 센싱 데이터는 다음 구조로 정리한다.

| 구분 | 역할 | 예시 |
| --- | --- | --- |
| 상태 센싱 | 현재 상태가 정상인지 확인 | BSA 대비 Booking, BL 소석률, CM1/TEU |
| 선행 센싱 | 앞으로 부족하거나 위험해질 항목을 미리 확인 | WOS-3 부킹, 최근 3일 pickup, RFQ pipeline |
| 예외 센싱 | 기준에서 벗어난 항목을 찾아 조치 대상으로 전환 | 저운임, EFC/LSS Gap, Booking-BL Gap |
| 실행 센싱 | 조치 후 개선 여부를 추적 | Action status, pickup 변화, Gap 해소 여부 |

## 2. 7월 필수 센싱 데이터

### 2.1 BSA And Booking Load

| 항목 | 가져갈 데이터 | 기준 단위 | 핵심 지표 | 센싱 목적 | 후속 액션 |
| --- | --- | --- | --- | --- | --- |
| BSA | BSA TEU | YYYYWW, Route, POR, DLY, Vessel 가능 시 | `bsa_teu` | 기준 선복 확인 | 부족/초과 판단의 기준값으로 사용 |
| Booking | Current Booking TEU | YYYYWW, Route, POR, DLY, Vessel, BKG_NO | `booking_teu` | 현재 부킹 소석 수준 확인 | 부족 구간 영업 follow-up |
| Booking Load Factor | Booking TEU / BSA TEU | Route-week, country-week | `booking_load_factor` | BSA 대비 부킹 부족/초과 감지 | 80% 미만 TOP 리스트 작성 |
| Target Gap | 목표 소석률까지 필요한 TEU | Route-week | `target_gap_teu_80`, `target_gap_teu_95` | 필요한 추가 물량 수치화 | 담당 영업과 대체 화주 후보 확인 |
| Over TEU | Booking TEU - BSA TEU | Route-week | `over_teu` | 과부킹 또는 선복 조정 필요 감지 | CM1/TEU와 함께 선적 우선순위 검토 |

우선순위: 필수  
주의점: Salesperson별 BSA 배정 기준이 없으면 영업사원별 BSA 활용률은 표시하지 않는다. 대신 영업사원별 WOS-3 BKG, Normal LST, Cancel, CM1/TEU를 본다.

### 2.2 WOS-3 Booking Pace

| 항목 | 가져갈 데이터 | 기준 단위 | 핵심 지표 | 센싱 목적 | 후속 액션 |
| --- | --- | --- | --- | --- | --- |
| WOS-3 FST TEU | WOS-3 시점 최초 부킹 TEU | YYYYWW, Route, POR, DLY, Salesman, Shipper | `wos3_fst_teu` | 출항 3주 전 초기 부킹력 확인 | 부족 구간 조기 영업 액션 |
| WOS-3 BSA Ratio | WOS-3 FST TEU / BSA TEU | Route-week | `wos3_bsa_ratio` | 선행 소석 위험 감지 | 낮은 구간 Action Monitor 등록 |
| WOS-3 Shipment Rate | WOS-3 부킹 중 Normal LST 전환율 | Route-week, shipper | `wos3_shipment_rate` | 초기 부킹의 실제 선적 전환력 확인 | 전환율 낮은 화주/구간 확인 |
| Recent Pickup | 최근 3일 WOS-3 TEU 증감 | Daily snapshot, route, shipper | `recent_3d_wos3_pickup_teu` | 조치 후 개선 여부 확인 | NO_PICKUP 대상 재확인 |
| Cancel Signal | WOS-3 이후 취소/감소 | BKG_NO, route, shipper | cancel TEU, cancel rate | 부킹 품질과 캔슬 위험 확인 | 반복 취소 화주 또는 구간 관리 |

우선순위: 필수  
주의점: WOS-3/BSA Ratio는 100%를 초과할 수 있으므로 cap을 걸지 않는다.

### 2.3 BL And Lifting Conversion

| 항목 | 가져갈 데이터 | 기준 단위 | 핵심 지표 | 센싱 목적 | 후속 액션 |
| --- | --- | --- | --- | --- | --- |
| BL TEU | BL 또는 컨테이너 기준 실제 TEU | BL_NO, BKG_NO, Route, YYYYWW | `bl_teu` | 실제 선적/BL 반영 수준 확인 | Booking은 있는데 BL 없는 항목 확인 |
| BL Load Factor | BL TEU / BSA TEU | Route-week | `bl_load_factor` | 실제 소석률 확인 | 운영/문서 진행 지연 확인 |
| Booking-BL Gap | Booking TEU - BL TEU | Route-week, vessel | `booking_bl_gap_teu` | 부킹과 실제 반영 차이 감지 | BL 발행 지연, 취소, 매핑 오류 구분 |
| BL Status | BL 상태값 | BL_NO, BKG_NO | issued, draft, void 등 | 유효 BL 정의 | 상태 코드 기준 확정 필요 |

우선순위: 필수  
주의점: BL row를 Booking row에 직접 join하면 Booking TEU와 CM1이 중복될 수 있다. BL은 먼저 BL_NO/BKG_NO 또는 lane 단위로 집계 후 연결한다.

### 2.4 CM1 And Profitability

| 항목 | 가져갈 데이터 | 기준 단위 | 핵심 지표 | 센싱 목적 | 후속 액션 |
| --- | --- | --- | --- | --- | --- |
| Booking CM1 | Booking 기준 CM1 | BKG_NO, Route, shipper | `booking_cm1` | 현재 예약 기준 수익성 확인 | 저수익 물량 식별 |
| Booking CM1/TEU | Booking CM1 / Booking TEU | Route, shipper, salesman | `cm1_teu_booking` | 물량 품질 확인 | 추가 유치 대상/제한 대상 구분 |
| BL CM1/TEU | BL CM1 / BL TEU | BL, route | `cm1_teu_bl` | 실제 선적 기준 수익성 확인 | 사후 수익성 검토 |
| Route High Profit Share | Route 기준 high-profit subset 비중 | WOS-3 route, shipper | `wos3_route_high_profit_share` | 좋은 물량이 들어오는지 확인 | 저수익 대체 화주 후보 검토 |
| Low Profit With Over TEU | 초과 부킹 중 낮은 CM1/TEU | Route-week, shipper | over TEU + CM1/TEU | 과부킹 품질 검토 | 선적 우선순위 또는 rate 재검토 |

우선순위: 필수  
주의점: High-profit shipper와 Route-high-profit은 다른 개념이다. 고객 자체의 high-profit tag와 route 기준 수익성 판단을 분리한다.

### 2.5 Rate And Low-Freight Risk

| 항목 | 가져갈 데이터 | 기준 단위 | 핵심 지표 | 센싱 목적 | 후속 액션 |
| --- | --- | --- | --- | --- | --- |
| Current Rate | 현재 등록 운임 | Route, POL/POD, container type, customer | current rate | 현재 운임 수준 확인 | 저운임 후보 추출 |
| Previous Rate | 직전 운임 | 동일 route/customer 기준 | rate change | 운임 하락 감지 | 하락폭 큰 건 검토 |
| Market/Reference Rate | 비교 기준 운임 | Route, container type | reference gap | 시장 대비 낮은 운임 확인 | 재검토 또는 승인 필요 건 정리 |
| Low-Freight Flag | 저운임 판단 결과 | Route, customer, contract/RFQ | low-rate flag | 리스크 대상 선별 | Weekly Rate Watch TOP 리스트 |
| Expiry/Validity | 운임 유효기간 | Contract/RFQ | days to expiry | 만료 전 조치 | 갱신 대상 선별 |

우선순위: 필수  
주의점: 저운임 판단은 단순히 낮은 rate만 보지 말고 CM1/TEU, volume, strategic account 여부를 함께 본다.

### 2.6 EFC/LSS Collection Gap

| 항목 | 가져갈 데이터 | 기준 단위 | 핵심 지표 | 센싱 목적 | 후속 액션 |
| --- | --- | --- | --- | --- | --- |
| Expected Charge | 기대 징수액 | BKG_NO, BL_NO, route, charge code | expected EFC/LSS | 받아야 할 금액 기준 | 미징수 판단 기준 |
| Actual Charge | 실제 징수액 | BKG_NO, BL_NO, invoice/charge | actual EFC/LSS | 실제 반영 확인 | 차액 확인 |
| Gap Amount | 기대 징수액 - 실제 징수액 | BKG_NO, BL_NO, customer | EFC/LSS gap | 미징수/과소징수 감지 | 담당자 follow-up |
| Gap Reason | 차이 사유 | charge code, customer, route | reason code | 반복 원인 확인 | 기준/시스템/담당자별 개선 |
| Follow-up Status | 조치 상태 | item-level | open/done/hold | 회수 진행 추적 | 주간회의 공유 |

우선순위: 필수  
주의점: 기대 징수 기준이 route, customer, charge code별로 다를 수 있으므로 기준표가 필요하다.

### 2.7 RFQ And Bidding Pipeline

| 항목 | 가져갈 데이터 | 기준 단위 | 핵심 지표 | 센싱 목적 | 후속 액션 |
| --- | --- | --- | --- | --- | --- |
| RFQ List | 진행 중 RFQ/비딩 건 | RFQ, customer, route | RFQ count, volume | 향후 물량 기회 확인 | 중요 RFQ follow-up |
| Bid Rate | 제출 운임 | RFQ, route, customer | bid rate | 수주 가능성과 수익성 확인 | rate 재검토 |
| Expected Volume | 예상 물량 | RFQ, route | expected TEU | BSA 부족 구간 보완 가능성 확인 | 부족 구간과 연결 |
| Win/Loss | 수주 여부 | RFQ | win rate | 비딩 성과 확인 | 패턴 분석 |
| Effective Date | 적용 시작일 | RFQ/contract | start/end date | 실제 booking 연결 시점 확인 | 7월 이후 pipeline 관리 |

우선순위: 7월에는 보조, 8월 Integrated Dashboard 확장 시 강화  
주의점: RFQ는 현재 실적보다 선행 지표에 가깝다. BSA 부족 구간의 대체 물량 후보로 연결하는 방식이 가장 실용적이다.

### 2.8 OBT Raw And Source Freshness

| 항목 | 가져갈 데이터 | 기준 단위 | 핵심 지표 | 센싱 목적 | 후속 액션 |
| --- | --- | --- | --- | --- | --- |
| Source Updated At | 원천 데이터 갱신 시각 | Source-level | max updated at | 데이터 신뢰도 확인 | 오래된 source는 경고 표시 |
| Row Count | 데이터 건수 | Source/date | row count | 누락 또는 과다 적재 확인 | 이상 시 재다운로드 |
| Key Null Check | 주요 key 누락 | BKG_NO, BL_NO, route, week | null count | join 실패 예방 | 원천 정리 요청 |
| Duplicate Check | 중복 key | BKG_NO, BL_NO, route-week | duplicate count | TEU/CM1 과대 집계 방지 | 집계 로직 점검 |
| Reconciliation | dashboard total 비교 | source vs output | TEU diff, amount diff | 기존 보고와 차이 확인 | 차이 원인 기록 |

우선순위: 필수  
주의점: Tableau extract는 갱신 전이면 최신 상태가 아닐 수 있다. 주간회의 자료에는 기준일과 source freshness를 같이 표시한다.

## 3. 액션으로 연결할 센싱 룰 초안

| 센싱 조건 | 우선순위 | 액션 유형 | 담당 연결 | 회의 표시 |
| --- | --- | --- | --- | --- |
| BSA 대비 Booking Load Factor 80% 미만 | High | 추가 부킹 확보 | Route 담당 영업, 해당 country 담당 | BSA 부족 TOP |
| WOS-3 BSA Ratio 낮음 | High | 조기 영업 액션 | 영업사원, 대상 화주 | WOS-3 부족 TOP |
| WOS-3 최근 3일 pickup 없음 | High | 재접촉 또는 대체 화주 확인 | 영업사원 | NO_PICKUP 리스트 |
| Booking은 충분하나 BL Load Factor 낮음 | Medium | BL 진행/선적 확인 | 운영/문서/영업 | Booking-BL Gap |
| Booking-BL Gap이 큰 vessel/route | Medium | BL lag, cancel, mapping 분리 | 운영/데이터 담당 | Gap TOP |
| Over TEU 발생 + CM1/TEU 낮음 | High | 선적 우선순위/운임 재검토 | 영업/운임 담당 | 수익성 리스크 |
| Low-freight flag 발생 | High | 운임 재검토 | 운임/RFQ 담당 | 저운임 TOP |
| EFC/LSS Gap 발생 | High | 징수 follow-up | 담당자/정산 | 징수 Gap TOP |
| RFQ 예상 물량이 BSA 부족 구간과 맞음 | Medium | pipeline 연결 | RFQ/영업 담당 | 기회 후보 |
| Source freshness 지연 | Medium | 데이터 재확인 | 데이터 담당 | 자료 기준일 경고 |

## 4. Action List 필드

대시보드에서 뽑아야 할 최종 결과는 화면이 아니라 follow-up 리스트다. 최소 필드는 아래와 같이 둔다.

| 필드 | 설명 |
| --- | --- |
| 기준일 | 데이터 산출 기준일 |
| Period | YYYYWW 또는 YYYYMM |
| Sensing Type | BSA, WOS-3, BL, Rate, CM1, EFC/LSS, RFQ, Data Quality |
| Priority | High, Medium, Low |
| Route | 항로 |
| POR | 출발지 |
| DLY | 도착지 |
| Vessel/Voyage | 가능 시 |
| Customer/Shipper | 가능 시 |
| Salesman/Owner | 담당자 |
| Metric Value | 문제 판단에 사용한 수치 |
| Threshold | 기준값 |
| Gap | 부족 TEU, 금액 차이, rate gap 등 |
| Suggested Action | 권장 조치 |
| Due Date | 조치 기한 |
| Status | Open, In Progress, Done, Hold |
| Memo | 확인 내용 |

## 5. 주간회의용 TOP 리스트

7월에는 아래 5개 블록만 고정으로 가져가도 충분하다.

### 5.1 이번 주 조치 필요 TOP 10

선정 기준:

1. Priority High
2. Gap 규모가 큰 순
3. 출항 또는 마감까지 남은 시간이 짧은 순
4. CM1/TEU 또는 징수 금액 영향이 큰 순

표시 필드:

| Rank | Type | Route | Week | Issue | Metric | Owner | Action | Due |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |

### 5.2 BSA/WOS-3 저조 구간

표시 필드:

| Route | DLY | Week | BSA TEU | Booking TEU | WOS-3 TEU | Gap TEU | Owner |
| --- | --- | --- | --- | --- | --- | --- | --- |

### 5.3 저운임/수익성 리스크

표시 필드:

| Route | Customer | Current Rate | Reference | Rate Gap | CM1/TEU | Volume | Action |
| --- | --- | --- | --- | --- | --- | --- | --- |

### 5.4 Booking-BL Gap

표시 필드:

| Route | Vessel | Week | Booking TEU | BL TEU | Gap TEU | Suspected Reason | Owner |
| --- | --- | --- | --- | --- | --- | --- | --- |

### 5.5 EFC/LSS Gap

표시 필드:

| Customer | BL/BKG | Route | Expected | Actual | Gap | Charge | Owner | Status |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |

## 6. 7월 실행 순서

| 기한 | 산출물 | 해야 할 일 |
| --- | --- | --- |
| 2026-07-05 | 업무 플로우 맵 | 데이터 source, 판단 지표, 보고/조치 흐름을 1장으로 정리 |
| 2026-07-12 | 센싱 데이터 정의표 | 위 필수 센싱 데이터 중 실제로 가져올 source와 컬럼 확정 |
| 2026-07-19 | 액션 룰 및 follow-up 리스트 | threshold, priority, owner, due date 기준 확정 |
| 2026-07-26 | 회의용 TOP 리스트 | 주간회의/월간회의에 넣을 TOP 리스트 포맷 적용 |
| 2026-07-31 | 실행 결과 공유 | 도움이 된 지표, 보고용에 머문 지표, 8월 확장 과제 정리 |

## 7. 7월에 먼저 고정할 것

가장 먼저 고정해야 할 것은 아래 10개다.

1. BSA 기준 source와 Revised/Proforma 기준
2. Booking TEU 기준과 cancel/void 제외 기준
3. BL 유효 상태값 기준
4. WOS-3 계산 기준
5. Route, POR, DLY, Vessel, Week join key
6. CM1/TEU를 Booking 기준으로 볼지 BL 기준으로 볼지
7. 저운임 판단 기준
8. EFC/LSS 기대 징수 기준표
9. 담당자 매핑 기준
10. Action status 기준

## 8. 8월 Integrated Dashboard 확장 후보

7월에는 센싱과 액션 루틴을 먼저 검증하고, 8월에는 아래 구조로 확장한다.

| 영역 | 확장 방향 |
| --- | --- |
| Control Room | BSA, Booking, BL, WOS-3, CM1, Rate, EFC/LSS를 한 화면에서 연결 |
| Action Monitor | 센싱 결과를 자동으로 follow-up queue로 생성 |
| Owner View | 담당자별 open action, overdue, recovered 현황 |
| Route View | route/vessel/port 단위 drilldown |
| Profit Guardrail | 물량 확대 대상과 수익성 주의 대상을 분리 |
| Source Health | 데이터 갱신, 누락, 중복, reconciliation 상태 표시 |

