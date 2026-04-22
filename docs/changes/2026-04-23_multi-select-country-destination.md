# 2026-04-23 선적/도착 필터 멀티셀렉트 및 도착지 국가 단위 분리

## 배경/문제

- 선적지와 도착지 국가/포트를 하나씩만 선택할 수 있어 복수 루트 비교가 번거로웠다.
- 도착지가 `MY/SG`, `AE`처럼 그룹으로 표시되어 국가 단위 확인이 어려웠다.

## 원인/판단

- `dist/index.html`의 글로벌 필터가 단일 `<select>` 기반으로 구현되어 있었다.
- JSON 생성 단계에서 BKG/BSA의 `dest`가 일부 국가 그룹 매핑을 거쳐 생성되고 있었다.

## 결정/계산 로직

- `fOri`, `fOriP`, `fDst`, `fDstP`를 체크박스형 멀티셀렉트로 변경했다.
- 선택값이 없으면 전체로 처리하고, 선택값이 있으면 해당 값 중 하나와 매칭되는 row만 포함한다.
- BKG/BSA 모두 `dest`를 원본 도착 국가 코드 단위로 유지한다.
- `D_group`은 기존 산출물 호환을 위해 유지하되 값은 도착 국가 코드로 저장한다.

## 변경 파일

- `daily_3w_dashboard.py`
- `dist/index.html`
- `dist/guide.html`
- `dist/data.json`
- `DEVELOPMENT.md`
- `docs/changes/2026-04-23_multi-select-country-destination.md`

## 검증 결과

- `python -m py_compile daily_3w_dashboard.py` 통과
- `dist/index.html`, `dist/guide.html` 스크립트 파싱 통과
- `SKIP_DOWNLOAD=1`, `SKIP_GDRIVE_UPLOAD=1`로 `dist/data.json` 재생성
- `dist/data.json`에서 `MY/SG` 미존재, `MY`/`SG` 국가 단위 존재 확인
- Playwright smoke test로 멀티셀렉트 4개 렌더링 및 도착지 `MY`, `SG` 동시 선택 확인

## 배포/커밋

- GitHub Pages 배포 완료: `kmtc-3w-dashboard-web` `32b1dd6`
- Main repo 반영 완료: `kmtc-3w-dashboard` `f9612d8`
- 공개 URL 검증 완료: `index.html` 멀티셀렉트 코드, `guide.html` 필터 설명, `data.json` 국가 단위 도착지 확인

## 후속 확인사항

- 운영자가 브라우저에서 캐시 새로고침 후 도착지 필터의 국가 단위 표시를 한 번 더 확인한다.
