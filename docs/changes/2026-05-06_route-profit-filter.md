# 2026-05-06 구간수익 필터 기준 전환

## 배경/문제

- 글로벌 `화주구분` 필터가 `고수익태그` 기준의 고수익화주/저수익화주를 바라보고 있었다.
- Tab 1과 구간별 고수익 지표를 루트 평균 CM1/TEU 기준으로 전환했기 때문에, 필터도 같은 `구간별고수익/구간별저수익` 기준을 사용해야 한다.

## 원인/판단

- `profitSum()`은 `w3_hi_fst`, `hi_fst` 등 고수익화주 메트릭을 사용했다.
- `filterShipper()`도 `고수익태그 == 고수익화주/저수익화주`로 화주 rows를 필터링했다.
- 월/주 집계에서 route-high 기준으로 전체 BKG, 실선적, CM1/TEU, WOS 단계별 BKG를 나눠 계산할 보조 메트릭이 부족했다.

## 결정/계산 로직

- 필터 라벨을 `구간수익`으로 바꾸고 옵션을 `구간별고수익`, `구간별저수익`으로 변경한다.
- `profitSum()`은 `route_hi_*` 메트릭을 우선 사용한다.
- 월/주 집계에는 전체 구간별 고수익 BKG/실선적/CM1과 WOS 단계별 route-high BKG를 추가한다.
- 화주 집계에는 파일 크기를 줄이기 위해 route-high 여부를 짧은 `rh` 플래그로 보관한다.

## 변경 파일

- `daily_3w_dashboard.py`
- `dist/index.html`
- `dist/guide.html`
- `dist/data.json`
- `DEVELOPMENT.md`

## 검증 결과

- `python -m py_compile daily_3w_dashboard.py` 통과
- `dist/index.html`, `dist/guide.html` 인라인 스크립트 syntax 확인 통과
- `SKIP_DOWNLOAD=1`, `SKIP_GDRIVE_UPLOAD=1`로 `dist/data.json` 재생성
- `dist/data.json` 크기: 104,726,559 bytes, 99.88 MiB
- 2026년 5월 WOS-3 기준: 전체 `135,709`, 구간별고수익 `63,501`, 구간별저수익 `72,208`

## 배포/커밋

- GitHub Pages 배포 완료: `kmtc-3w-dashboard-web` `fcfb0db`
- Main repo 커밋 완료

## 후속 확인사항

- 공개 URL에서 필터 라벨이 `구간수익`으로 표시되는지 확인한다.
- `구간별고수익` 선택 시 구간별고수익 비중/수치가 route-high subset 기준으로 표시되는지 확인한다.
