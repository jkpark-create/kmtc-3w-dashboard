# OBT Action Monitor

## 배경

- OBT Exception Monitor에서 추출된 영업사원/화주 action 후보가 실제 3W Booking TEU 개선으로 이어지는지 같은 화면에서 확인할 필요가 있었다.
- 활동량 수기 입력 없이, 기존 -3W booking 데이터와 `history.json` 이력을 이용해 자동 모니터링하는 방향으로 구성했다.
- 선적지 현황은 국가/포트 단위로 묶어 확인하고, 기존 -3W Dashboard의 멀티셀렉트 경험과 동일하게 복수 선택이 필요했다.

## 변경

- `obt-exception-monitor/index.html`
  - 기존 화면을 `Exception View` 탭으로 감싸고 `Action Monitor` 탭을 추가했다.
  - `Action Monitor`에 KPI, 선적지별 요약, 영업사원별 개선 현황, 자동 Action 후보 상세 테이블을 추가했다.
  - 선적국가/선적포트/도착국가/도착포트 필터를 단일 select에서 체크박스형 멀티셀렉트로 변경했다.

- `obt-exception-monitor/app.js`
  - route 필터를 다중 선택 배열로 처리하도록 필터링/cascade 로직을 변경했다.
  - 멀티셀렉트 검색, 임시 선택, 적용/닫기, 전체 초기화 동작을 추가했다.
  - `Action Monitor` 자동 후보를 현재 shipper exception에서 생성하고 action type을 자동 분류한다.
  - 화주 단위 3W 이력이 있으면 `recent 3W pickup`을 화주 기준으로 계산하고, 없으면 route 기준 이력으로 fallback한다.
  - 같은 요일 이전 snapshot의 동일 리드타임 3W 평균을 이용해 화주-구간별 `요일기대`, `요일 Gap`, `평소 대비`를 계산한다.
  - Custom 선택에서는 단일 W+1~W+3 주차일 때만 요일 기준/리드타임 트렌드를 적용하고, 월 전체/전체 주차/과거 주차/W+4 이상은 기간 비교 기준으로 표시한다.
  - `Exception View`의 트렌드 셀은 route-level 리드타임/속도 판단을 유지하되, route 기준 같은 요일 3W benchmark가 있을 때 보조 신호로 함께 표시한다.
  - `Improving`, `No Pickup`, `Open`, `Recovered` 상태를 자동 산정한다.

- `obt-exception-monitor/guide.html`
  - 가이드 순서를 데이터 범위 → 대응 시점/Custom → 비교 기준 → KPI/계산 상세 → Exception View → Action Monitor → 실행 흐름 순서로 읽히게 정리했다.
  - Action Monitor 탭의 목적, 자동 후보 단위, `관리 3W Gap`, `요일 Gap`, `평소 대비`, `최근 3W Pickup`, `No Pickup` 계산식을 상세 추가했다.
  - 활동량 수기 입력 대신 booking TEU pickup/no-pickup 신호로 실행 결과를 모니터링하는 해석 기준을 추가했다.

- `obt-exception-monitor/build_history.py`
  - 기존 route pace history 외에 차주~3주뒤 화주 단위 compact snapshot을 추가했다.
  - 정적 로딩 부담을 줄이기 위해 화주 이력은 Action Monitor 운영 구간만 저장한다.

- `dist/obt-exception-monitor/*`
  - 배포용 정적 파일과 `history.json`을 동일하게 갱신했다.

## 검증

- `node --check obt-exception-monitor/app.js` 통과.
- `node --check dist/obt-exception-monitor/app.js` 통과.
- `python -m py_compile obt-exception-monitor/build_history.py` 통과.
- `python obt-exception-monitor/build_history.py`로 11개 snapshot 재생성.
- 로컬 서버 `http://localhost:8765/obt-exception-monitor/`에서 Chrome headless CDP 스모크 테스트 통과.
  - `data.json` 로딩 확인.
  - `Action Monitor` 탭 표시 확인.
  - 자동 action 후보 KPI/상세 테이블 렌더링 확인.
  - 선적국가 멀티셀렉트 선택/적용 확인.
  - `요일 Gap`, `평소 대비`, 화주-구간 상세의 `요일기대` 렌더링 확인.
  - Custom 전체/과거/W+4 이상에서는 요일 기준과 리드타임 트렌드가 기간 비교 안내로 전환되는지 확인.
