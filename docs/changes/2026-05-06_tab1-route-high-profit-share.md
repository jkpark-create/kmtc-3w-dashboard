# 2026-05-06 Tab 1 구간별 고수익 기준 전환

## 배경/문제

- Tab 1 소석률 현황의 보라 점선과 하단 월간 실적 표가 `고수익화주` 기준으로 표시되고 있었다.
- 사용 기준은 화주 태그가 아니라 `POR_PLC_CD + DLY_PLC_CD` 루트 평균 CM1/TEU 대비 고/저로 분류한 `구간별고수익`이어야 한다.

## 원인/판단

- 기존 월별 차트는 `w3_hi_fst / w3_fst`를 `고수익화주부킹비중`으로 표시했다.
- 하단 월간 실적 표의 우측 블록도 `w3_hi_fst`, `w3_hi_norm_lst`, `w3_hi_cm1_norm`을 사용했다.
- 구간별 고수익 BKG는 `w3_route_hi_fst`가 있었지만, 실선적과 CM1 메트릭은 별도 생성되지 않았다.

## 결정/계산 로직

- Tab 1 보라 점선은 `w3_route_hi_fst / w3_fst`로 표시한다.
- 하단 월간 실적 표의 우측 블록은 `3주전 BKG (구간별고수익)`으로 이름을 바꾸고 다음 필드를 사용한다.
- `BKG`: `w3_route_hi_fst`
- `실선적`: `w3_route_hi_norm_lst`
- `CM1`: `w3_route_hi_cm1_norm`
- `CM1/TEU`: `w3_route_hi_cm1_norm / w3_route_hi_norm_lst`

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
- 2026년 5월 전체 기준 `w3_route_hi_fst / w3_fst = 63,501 / 135,709 = 46.8%` 확인
- 기존 고수익화주 기준은 `59,739 / 135,709 = 44.0%`로, 새 표시가 구간별 고수익 기준으로 분리됐음을 확인

## 배포/커밋

- GitHub Pages 배포 완료: `kmtc-3w-dashboard-web` `57ee07e`
- Main repo 커밋 완료

## 후속 확인사항

- 공개 URL에서 Tab 1 월별 차트 legend가 `구간별고수익비중`으로 표시되는지 확인한다.
- 하단 월간 실적 표의 우측 블록 제목과 수치가 `구간별고수익` 기준인지 확인한다.
