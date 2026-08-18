# 2026-08-18 미반영 변경 동기화

## 배경/문제

- Tableau 임시 워크북 발행 요청이 연결 확인 단계에서 10분 타임아웃된 뒤 최대 30분 동안 생성 여부를 반복 조회했다.
- 메인 대시보드의 기본 월/주차 변경이 배포 저장소의 화면과 가이드에만 작업 중인 상태였다.
- OBT Exception Monitor 파비콘은 배포 저장소에는 있었지만 루트 소스 저장소에는 반영되지 않았다.

## 결정/계산 로직

- 이미 서버에 게시된 데이터 원본 연결을 포함하는 임시 워크북은 REST 발행 시 `skipConnectionCheck=true`를 사용한다.
- 발행 API가 200/201 이외의 응답을 반환하면 구체적인 HTTP 오류를 즉시 발생시켜 불필요한 워크북 탐색 대기를 하지 않는다.
- 메인 대시보드 최초 진입 기본값은 오늘 날짜가 속한 달과 주차 전체로 통일한다. 현재 달 데이터가 없을 때만 가장 최신 데이터 달을 사용한다.
- OBT 파비콘과 HTML 참조를 루트 소스 및 배포 저장소 양쪽에 유지한다.

## 변경 파일

- `daily_3w_dashboard.py`
- `scripts/test/test_tableau_publish.py`
- `dist/index.html`
- `dist/guide.html`
- `scripts/test/verify_main_default_filters.cjs`
- `obt-exception-monitor/index.html`
- `obt-exception-monitor/favicon.svg`

## 검증 기준

- Tableau 발행 요청에 `overwrite=true`와 `skipConnectionCheck=true`가 함께 전달된다.
- 발행 API의 403 등 명시적 오류는 워크북 탐색 루프 없이 즉시 실패한다.
- 메인 대시보드 코드와 한/영 가이드가 현재 달/전체 주차 기본값을 동일하게 설명한다.
- 루트와 배포 저장소의 OBT 파비콘 SHA-256이 일치한다.

## 검증 결과

- Python 컴파일 통과.
- Google Drive 동기화 안전성, 신규 영업사원, Q3 Target, Tableau 발행, View 1 청크 테스트 19개 통과.
- 메인 대시보드 기본 필터와 한/영 가이드 정적 검증 통과.
- Google Drive 런타임 파일 1,437개가 저장된 manifest 기준 ID, 크기, MD5 일치.
- OBT 파비콘 SHA-256: `6B2EE4AD9D440662AB7ADC82A8435419C7E630654B323489018E17FB122E2B66` (루트/배포 동일).
