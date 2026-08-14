# 2026-08-14 원천데이터 추출·완료 기준 도움말 보강

## 배경

- 메인 도움말의 원천데이터 범위 설명이 짧아, 사용자가 Tableau 원천별 추출 범위와 최종 완료 상태를 구분하기 어려웠다.
- 기존 도움말의 `2.csv: 전체` 설명은 현재 코드의 YYYYMM 필터와 일치하지 않았다.
- Google Drive 검증 후 로컬 감사 파일을 정리하는 정상 동작이 파일 누락으로 오해될 수 있었다.

## 반영 내용

- `dist/guide.html`에 `원천데이터 추출·완료 기준` 한·영 섹션을 추가했다.
- 1.csv, 2.csv, BSA raw, Grade, salesman.csv, Sales Target Google Sheet의 실제 추출 범위와 역할을 문서화했다.
- 2.csv base + 1.csv supplement 병합, 중복 제거, 날짜/TEU fallback, Cancel 제외 조건을 정리했다.
- 동일 YYYYMMDD 강제, Google Drive 파일 ID·중복명·크기·MD5 검증, 검증 후 로컬 정리 순서를 완료 기준으로 명시했다.
- Tableau 재시도 경고와 최종 실패를 구분하는 방법, 미매칭 영업사원 감사 건수, 445 캘린더 유지보수 범위를 추가했다.
- 기존 자동 업데이트 표의 2.csv 범위를 현재 코드 기준으로 수정했다.

## 변경 파일

- `dist/guide.html`
- `daily_3w_dashboard.py` (분할 방식 주석 정정)
- `docs/changes/2026-08-14_source-extraction-help.md`

## 검증 결과

- HTML 인라인 스크립트 문법 검사 통과.
- Playwright에서 한국어/영어 목차 이동과 언어 전환 확인.
- 1440×900 데스크톱 및 390×844 모바일 화면 확인. 모바일 표는 카드 내부 가로 스크롤로 처리하고 문서 전체의 가로 넘침은 제거했다.
- 관련 Python 컴파일 및 운영 회귀 테스트 통과.
- Google Drive 저장 매니페스트의 원격 파일 ID·크기·MD5 검증 통과.
