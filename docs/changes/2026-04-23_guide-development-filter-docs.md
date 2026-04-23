# 2026-04-23 가이드/개발 문서 필터 설명 보강

## 배경

- 루트 필터가 검색 가능한 멀티셀렉트와 적용 버튼 방식으로 바뀌면서, 사용자가 언제 실제 필터가 반영되는지 이해할 수 있는 설명이 필요했다.
- 개발 문서에도 `_pending`/`_selected` 상태 분리와 `source: 'multi-select'` 이벤트 처리 기준을 남겨야 후속 유지보수 때 검색 input 이벤트를 필터 적용 이벤트로 오인하지 않는다.

## 변경

- `dist/guide.html` 필터 섹션에 검색, 체크박스 선택, `적용`, `전체`, `닫기` 동작을 한국어/영어로 추가했다.
- `DEVELOPMENT.md`의 글로벌 필터 설명에 멀티셀렉트 상태 관리와 적용 이벤트 기준을 추가했다.
- `DEVELOPMENT.md` 변경 이력에 루트 멀티셀렉트 적용/검색 배포 커밋을 추가했다.
- Google Docs 업데이트용 `generated_docs/DEVELOPMENT_google_docs_formatted.html`을 최신 `DEVELOPMENT.md` 기준으로 재생성했다.

## 검증

- `dist/guide.html` 인라인 스크립트 문법 확인.
- Google Docs용 HTML에 `검색 가능한 체크박스형 멀티셀렉트`, `_pending`, `source: 'multi-select'` 설명 반영 확인.

## 배포/문서

- 사용자 가이드는 GitHub Pages 배포 대상 `dist/guide.html`로 반영한다.
- 개발 문서는 Git 원본 `DEVELOPMENT.md`와 Google Docs 문서 `-3W Booking Dashboard - Development Documentation`에 함께 반영한다.
