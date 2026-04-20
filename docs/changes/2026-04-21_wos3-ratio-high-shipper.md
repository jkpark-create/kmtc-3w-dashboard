# 2026-04-21 WOS-3 소석률/고수익화주 집계 점검

## 배경/문제

- 지역별, 구간별, 화주별 화면에서 `3주전 부킹 소석률`과 `3주전 고수익화주 소석률`이 100%를 초과하는 사례가 확인되었다.
- 화주명 컬럼은 긴 화주명이 잘려 전체 확인이 어려운 상태였다.
- 사용자 요청 기준은 값을 강제로 100%로 제한하는 것이 아니라, 실제로 100% 초과가 맞는 계산인지와 로직 적용이 정상인지 확인하는 것이다.

## 원인/판단

- `WOS-3 BKG/BSA`는 WOS-3 부킹 TEU가 BSA보다 크면 100%를 초과할 수 있다. 특히 국가/구간처럼 BSA 분모가 작은 집계에서 초과가 더 쉽게 발생한다.
- WOS 단계별 실선적률은 `LST_TEU / FST_TEU` 기준이므로 최종 실선적 TEU가 최초 부킹 TEU보다 커진 경우 100%를 초과할 수 있다.
- `w3_hi_fst / w3_fst`처럼 부분집합을 전체 WOS-3 BKG로 나누는 비중 지표는 정상 로직에서는 100%를 초과하지 않아야 한다.
- 기존 집계에는 고수익화주 지표가 `고수익태그`가 아니라 구간별 `고/저` 분류를 일부 사용하던 문제가 있었다.

## 결정/계산 로직

- `hi_fst`, `hi_norm_lst`, `w3_hi_fst`, `w3_hi_norm_lst`는 `고수익태그 == 고수익화주` 기준으로 계산한다.
- 구간별 고수익 부킹은 별도 필드 `w3_route_hi_fst`로 분리한다.
- 퍼센트 계산은 실제값을 그대로 표시한다. 100% 초과를 숨기거나 100%로 cap하지 않는다.
- 웹 가이드에는 `BKG/BSA`와 `LST_TEU/FST_TEU` 지표가 100%를 초과할 수 있는 사유를 명시한다.
- 화주명 컬럼은 줄바꿈 없이 한 줄로 표시하고, 다수 영업사원명은 앞 2명 + `...`로 축약한다.

## 변경 파일

- `daily_3w_dashboard.py`
- `DEVELOPMENT.md`
- `dist/index.html`
- `dist/guide.html`
- `dist/data.json`

## 검증 결과

- `w3_hi_fst / w3_fst` 초과 사례: 월별/주별/화주별 0건
- `w3_route_hi_fst / w3_fst` 초과 사례: 월별/주별/화주별 0건
- `w3_norm_lst / w3_fst`, `w3_hi_norm_lst / w3_hi_fst`, `WOS-3 BKG/BSA`는 실제 데이터 기준 100% 초과 사례가 존재하며, 원인은 각각 최종 LST_TEU 증가와 BSA 대비 WOS-3 부킹 초과이다.
- `dist/data.json`은 `data_date=20260420` 기준으로 재생성했다.

## 배포/커밋

- main repo: `eda7ca5 Fix WOS-3 high shipper aggregation`
- web repo: `201c99d Fix WOS-3 ratio logic and guide`
- web repo: `9e274a1 Update guide UI documentation`
- web repo: `82fc604 Show shipper names on one line`

## 후속 확인사항

- `data.json` 크기가 약 81MB로 GitHub 100MB 제한에 가까우므로 shipper 주차 데이터 축소 또는 압축/분할 전략을 검토한다.
- 2028년 이후 445 Calendar 시작일 추가가 필요하다.
