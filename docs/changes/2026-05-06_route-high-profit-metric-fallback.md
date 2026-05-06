# 2026-05-06 구간별 고수익 표시 보정

## 배경/문제

- Tab 2의 `구간별고수익%`가 일부 화면에서 모두 100%로 표시됐다.
- 실제 기준은 `POR_PLC_CD + DLY_PLC_CD` 루트별 평균 CM1/TEU 대비 화주-루트 CM1/TEU가 높거나 낮은지로 구분해야 한다.

## 원인/판단

- 생성 스크립트의 `고/저` 산정은 포트-포트 루트 평균 기준으로 계산되고 있었다.
- JSON 압축 과정에서 값이 0인 `w3_route_hi_fst` 필드는 행에서 생략된다.
- 프론트의 `metricVal()`이 행에 `w3_route_hi_fst`가 없으면 구버전 fallback인 `w3_hi_fst`를 사용해, 0이어야 할 구간별고수익을 고수익화주 BKG로 표시했다.

## 결정/계산 로직

- 메트릭 fallback 여부를 행 단위가 아니라 데이터셋 전체/스키마 단위로 판단한다.
- 데이터셋에 `w3_route_hi_fst`가 존재하면 개별 행의 필드 누락은 0으로 취급한다.
- 다음 생성분부터 `metric_keys`를 summary JSON에 포함해 압축된 0값과 구버전 필드 부재를 구분한다.

## 변경 파일

- `dist/index.html`
- `daily_3w_dashboard.py`

## 검증 결과

- `python -m py_compile daily_3w_dashboard.py` 통과
- `dist/index.html` 인라인 스크립트 syntax 확인 통과
- `OBT / SHA / 202603 / 고수익화주` 샘플 기준 기존 표시 로직은 `9,334 / 9,334 = 100.0%`, 수정 후는 `3,988 / 9,334 = 42.7%`로 보정됨을 확인

## 배포/커밋

- GitHub Pages 배포 완료: `kmtc-3w-dashboard-web` `9d4a5c9`
- Main repo 커밋 예정

## 후속 확인사항

- 공개 URL에서 브라우저 캐시 새로고침 후 `구간별고수익%`가 100%로 고정되지 않는지 확인한다.
