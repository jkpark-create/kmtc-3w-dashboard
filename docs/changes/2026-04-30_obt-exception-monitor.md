# OBT Exception Monitor

## 배경/문제

- OBT 적재 데이터 기준으로 문제가 있거나 개선이 필요한 구간만 빠르게 확인할 별도 화면이 필요했다.
- 영업사원이 제한된 시간 안에서 조치할 수 있도록 구간, 화주, 영업사원 단위의 우선순위와 판단 사유를 압축해야 했다.
- 차주, 차차주, 3주뒤는 보는 기준이 달라야 하므로 비교 기준도 각 리드타임 시점에 맞춰야 했다.

## 결정/로직

- 신규 화면은 `obt-exception-monitor/`에 구성했다.
- 접속 기본 대응값은 `3주뒤(W+3)`로 설정하고, 대상 날짜와 WW 주차를 화면에 함께 표시한다.
- 비교 기준은 동일 리드타임 기준으로 맞춘다.
  - W+3는 `w3_norm_lst` 또는 `w3_fst`를 기준으로 비교한다.
  - W+2/W+1은 현재 원천에 WOS-2/WOS-1 필드가 없어 리드타임 성숙도 보정값으로 비교한다.
- 중요구간은 도착포트별 물량/BSA/영향 TEU 분포 기준을 사용해 작은 구간의 노이즈를 줄인다.
- 리스크 유형은 6개 상위 그룹으로 묶고, 세부 태그는 칩과 마우스오버 설명으로 유지한다.
- 영업사원별 카드는 상태, 영향 TEU, 관리 Gap, 트렌드 지연, 회복/재확보 후보, 대표 구간을 표시한다.
- 한글/영문 전환은 토글 스위치로 적용하고, 가이드도 같은 언어 설정을 사용한다.
- 기존 `-3W Booking Dashboard`의 Google OAuth 방식을 참조해 동일한 회사 도메인 로그인 게이트를 추가했다.
  - OAuth client와 허용 도메인(`ekmtc.com`)은 기존 대시보드 값을 사용한다.
  - 기존 로그인 세션(`gtoken`, `guser`)을 공유한다.
  - 직접 `/obt-exception-monitor/`로 진입하면 루트 대시보드 OAuth callback을 거쳐 다시 해당 화면으로 돌아오도록 구성한다.

## 변경 파일

- `.gitignore`
- `obt-exception-monitor/index.html`
- `obt-exception-monitor/styles.css`
- `obt-exception-monitor/app.js`
- `obt-exception-monitor/auth.js`
- `obt-exception-monitor/guide.html`
- `obt-exception-monitor/build_history.py`
- `obt-exception-monitor/DEPLOYMENT.md`
- `dist/index.html`
- `dist/obt-exception-monitor/*`

## 검증 결과

- `node --check obt-exception-monitor/app.js` 통과.
- 로컬 서버 `http://localhost:8765/obt-exception-monitor/`에서 Chrome headless CDP 스모크 테스트 통과.
- Pages 배포 구조와 같은 `http://localhost:8765/dist/obt-exception-monitor/` 경로에서 Chrome headless CDP 스모크 테스트 통과.
- 확인 항목:
  - 기본 대응값 `w3`
  - 선택 옵션 `3주뒤 · 05/17 (WW20)`
  - 구간별 부제 `3주뒤 05/17 (WW20) ...`
  - 언어 전환 후 `Route Exceptions` 표시
  - `guide.html` 이동 및 영문 가이드 표시
  - 런타임 에러 없음

## 배포/커밋

- 기존 Pages 배포 저장소 `kmtc-3w-dashboard-web`에 `/obt-exception-monitor/` 경로로 배포한다.
- 루트 대시보드 헤더에 `OBT Monitor` 링크를 추가한다.
- 누적 속도 데이터 `obt-exception-monitor/history.json`은 소스 repo 커밋 대상에서 제외하고, Pages 배포 repo에만 포함한다.

## 후속 확인사항

- OAuth redirect는 기존 루트 callback을 사용하므로 신규 redirect URI 추가는 필요 없다.
- 현재 방식은 기존 대시보드와 같은 클라이언트 측 로그인 게이트다. 정적 파일 자체의 서버 측 보호가 필요하면 Cloudflare Access, Google Cloud IAP, Enterprise private/internal Pages 중 하나를 추가 검토한다.
