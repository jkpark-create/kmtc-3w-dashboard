# 2026-05-06 화주별 구간수익 필터 합계 기준 보정

## 배경

- Tab 2 화주별 화면에서 `구간수익=구간별고수익`을 선택하면 BKG와 실선적까지 구간별 고수익 subset으로 축소되어, `구간별고수익%`가 100%로 고정되는 것처럼 보였다.
- 사용 의도는 필터로 해당 구간수익을 가진 화주를 선별하되, BKG/실선적은 그 화주의 전체 WOS-3 부킹과 Normal LST를 보여주는 것이다.

## 변경

- `filterShipper(month)`는 구간수익 필터가 켜지면 먼저 `w3_route_hi_fst` 기준으로 해당 고/저 구간수익 물량이 있는 화주를 찾고, 이후 그 화주의 전체 행을 다시 사용한다.
- 화주별/영업사원별 BKG와 실선적 컬럼은 `w3_fst`, `w3_norm_lst` 전체값을 유지한다.
- `구간별고수익` 컬럼은 `w3_route_hi_fst`를 사용한다.
- CM1/TEU는 구간수익 필터에 맞춰 계산한다.
  - 고수익: `w3_route_hi_cm1_norm / w3_route_hi_norm_lst`
  - 저수익: `(w3_cm1_norm - w3_route_hi_cm1_norm) / (w3_norm_lst - w3_route_hi_norm_lst)`
  - 전체: `w3_cm1_norm / w3_norm_lst`
- `shipper` JSON에는 용량을 줄이기 위해 `w3_route_hi_norm_lst`, `w3_route_hi_cm1_norm`을 각각 `rhn`, `rhc` 축약 키로 저장한다.

## 수정 파일

- `daily_3w_dashboard.py`
- `dist/index.html`
- `dist/guide.html`
- `DEVELOPMENT.md`

## 검증

- `python -m py_compile daily_3w_dashboard.py`
- `node` inline script syntax check
- `SKIP_DOWNLOAD=1`, `SKIP_GDRIVE_UPLOAD=1` 로 최신 데이터 재생성
- `dist/data.json` 크기 100MiB 미만 확인
- `shipper` 데이터에 `rhn`/`rhc` 축약 키 포함 확인

## 배포

- Web repo: `a095306` (`kmtc-3w-dashboard-web/master`)
- Main repo: `2c7e120` (`kmtc-3w-dashboard/codex/obt-exception-monitor-guide-auth`)
