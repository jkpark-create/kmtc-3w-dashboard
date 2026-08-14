# 2026-08-14 통합 대시보드 추출 기준 전환

## 적용 범위

- 2026-06 실적까지는 기존 Tableau 기준을 유지한다.
- 2026-07 실적부터 BSA, Booking, B/L의 대상 주차·대상 모선은 통합 대시보드의 `Single(TEAM) / individual` 계약을 사용한다.
- WOS-3 계산은 기존 `Booking_schedule`, `week_start (BKG_Sche)`, `Lead_time (BKG_Sche)`를 그대로 사용한다.

## 기준

- BSA: LOCAL first-vessel, 유효 Original round, 확정 SPOT의 bound별 Original 대체, skip 및 국내 T/S 중복 제외.
- Booking/B/L: 첫 모선의 첫 BSA-bearing load segment를 우선하고, 없으면 최초의 후속 유효 BSA-bearing vessel을 사용한다. LCL/비표준 화물, skip, voyage `999999`는 제외하고 Booking과 실제 B/L은 분리한다.
- 통합 원천에만 있는 부킹은 WOS 일정이 없는 실적 전용 행으로 보강한다. 통합 범위에서 제외된 기존 7월 이후 행은 B/L 실적에 기여하지 않는다.

## 연계 반영

- 변경된 BSA CSV를 메인 대시보드, 영업사원별 BSA 배분, Sales Target, OBT Action Monitor가 공통 사용한다.
- 실적 전용 행에도 화주·영업사원·POR/DLY를 유지하여 영업사원별 집계가 통합 실적 총량과 보존되게 한다.

## 검증

- 통합 스냅샷 `20260814` 기준 2026-07 이후 Booking `421,333.851 TEU`, B/L `308,515.0 TEU`가 어댑터 적용 후 정확히 보존되었다.
- 기존 행의 `Booking_schedule` 변경 건수는 0건이다.
- 2026-07 BSA `172,372 TEU`, 2026-08 BSA `200,774 TEU`가 통합 대시보드 KPI와 일치한다.
