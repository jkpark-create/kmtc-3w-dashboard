# 2026-04-23 멀티셀렉트 필터 성능 개선

## 배경/문제

- 선적지/선적포트/도착지/도착포트 필터를 멀티셀렉트로 바꾼 뒤, 체크박스 선택 시 화면 반응이 느려졌다.
- 2025 연간 데이터처럼 `shipper` row가 많은 데이터셋에서는 필터 변경 한 번에도 반복 집계 비용이 커진다.

## 원인/판단

- 체크박스 변경마다 멀티셀렉트 메뉴 DOM을 통째로 다시 만들고 있었다.
- 대시보드 렌더링 중 `filterMonthly`, `filterWeekly`, `filterShipper`, `filterBSA`가 같은 조건으로 여러 번 반복 실행됐다.
- 선택값 매칭이 배열 `includes` 기반이라 복수 선택 시 불필요한 선형 탐색이 생겼다.

## 결정/계산 로직

- 멀티셀렉트 체크 변경 시 즉시 메뉴 전체를 재생성하지 않고 버튼 라벨만 갱신한다.
- 멀티셀렉트 변경 이벤트는 짧게 debounce해서 cascade+render를 한 번만 실행한다.
- 렌더 1회 안에서 필터 결과를 메모이즈해 반복 계산을 줄인다.
- 복수 선택 매칭은 `Set` 기반으로 변경했다.

## 변경 파일

- `dist/index.html`
- `docs/changes/2026-04-23_multi-select-performance.md`

## 검증 결과

- `dist/index.html` 스크립트 파싱 통과
- Playwright smoke test로 멀티셀렉트 4개 렌더링 및 도착지 `MY`, `SG` 동시 선택 확인

## 배포/커밋

- GitHub Pages 배포 완료: `kmtc-3w-dashboard-web` `b716faa`
- 공개 URL 검증 완료: `memoizedFilter`, `scheduleCascadeRender`, `selectedSet` 반영 확인

## 후속 확인사항

- 운영 화면에서 연간 데이터셋 선택 후 필터 체크/해제 체감 속도를 확인한다.
