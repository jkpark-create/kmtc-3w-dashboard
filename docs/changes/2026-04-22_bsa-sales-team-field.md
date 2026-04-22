# BSA Sales Team Field

## 배경/문제

- Tableau BSA raw 뷰에 `Sales Team` 필드가 추가되었고, 화면 기준 BSA 값은 이 팀 필드를 기준으로 확인된다.
- 기존 자동화는 BSA 다운로드 후 POR_Country/DLY_Country 기반 `classify_team`으로 팀을 재분류해 Tableau 화면과 일부 값이 달라질 수 있었다.
- `OBT / CN / NBO / 202605 / WW19` 기준 Tableau 화면은 1,408 TEU였고, stale 로컬 파일은 1,326 TEU로 확인되었다.

## 결정/로직

- BSA raw는 Tableau CSV의 `Sales Team`을 canonical `team`으로 사용한다.
- `Sales Team` 컬럼이 없는 과거 BSA 파일만 `classify_team`으로 fallback한다.
- 다운로드 URL은 이 뷰에서 실제 동작하는 `Sales Team=...` 파라미터 방식을 유지한다.

## 변경 파일

- `daily_3w_dashboard.py`
- `DEVELOPMENT.md`
- `dist/data.json`

## 검증 결과

- BSA 재다운로드 행 수: OBT 27,519 / EST 7,477 / IST 4,146 / JBT 11,582
- 최종 JSON BSA rows: 26,952
- `OBT / CN / NBO / 202605 / WW19` raw 및 JSON BSA: 1,408 TEU
- `python -m py_compile daily_3w_dashboard.py` 통과

## 배포/커밋

- main repo: `c9a72f8 Use Tableau Sales Team for BSA data`
- web repo: `fbc3721 Refresh BSA data (2026-04-22)`

## 후속 확인사항

- Tableau BSA 뷰에서 팀 필드명 또는 URL 파라미터명이 바뀌면 `normalize_bsa_team`과 다운로드 URL 생성부를 함께 확인한다.
