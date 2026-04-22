# Development Documentation: KPI and Integration Details

## 배경/문제

- 전체 BKG, 실선적, 3주전 BKG, 3주전 실선적 같은 핵심 KPI가 어떤 원천 컬럼과 조건으로 생성되는지 문서에서 더 자세히 설명할 필요가 있었다.
- Tableau, Google Drive, Gmail, GitHub Pages 연동도 운영자가 장애나 값 차이를 확인할 때 참고할 수 있도록 흐름별 설명이 필요했다.

## 결정/로직

- `DEVELOPMENT.md`의 Phase 2/3 설명에 집계 전 제외 조건, KPI 생성 공식, WOS-3 판정 기준, 프론트엔드 KPI 합산 방식을 추가했다.
- 외부 연동 섹션에 Tableau 다운로드, Google Drive 업로드, Gmail 알림, GitHub Pages 배포 흐름을 구체화했다.

## 변경 파일

- `DEVELOPMENT.md`
- `generated_docs/DEVELOPMENT_google_docs_formatted.html` (로컬 생성물)
- Google Docs 문서 `-3W Booking Dashboard - Development Documentation`

## 검증 결과

- 문서 diff 검토
- Google Docs용 HTML 재생성
- Google Docs 문서 업데이트

## 배포/커밋

- 이 변경 기록은 문서 업데이트와 함께 main repo에 커밋한다.

## 후속 확인사항

- KPI 정의나 외부 연동 방식이 바뀌면 `DEVELOPMENT.md`, Google Docs 문서, 관련 변경 기록을 함께 갱신한다.
